//! Producer-side schema resolution and framing choice.
//!
//! Every encoder in this crate — [`ConfluentSchemaEncoder`], [`AvroSchemaEncoder`],
//! [`JsonSchemaEncoder`], [`ProtobufSchemaEncoder`] — has to answer the same two
//! questions before it can frame a payload:
//!
//! 1. **Which identifier does this subject resolve to?** — [`SchemaResolution`]
//! 2. **Which framing carries it?** — [`Framing`]
//!
//! Both are configured on the builder and both default to what a first-time
//! user expects: register the schema, frame it with the 4-byte ID.
//!
//! The resolution itself is cached per subject with the same bounded,
//! coalescing map the registry and codec caches use, so N tasks racing on a
//! cold subject issue exactly one round-trip.
//!
//! [`ConfluentSchemaEncoder`]: crate::ConfluentSchemaEncoder
//! [`AvroSchemaEncoder`]: crate::AvroSchemaEncoder
//! [`JsonSchemaEncoder`]: crate::JsonSchemaEncoder
//! [`ProtobufSchemaEncoder`]: crate::ProtobufSchemaEncoder

use crate::error::{Result, SchemaRegError, error_code};
use crate::traits::SchemaRegistryClient;
use crate::types::{SchemaGuid, SchemaId, SchemaKey, SchemaReference, SchemaType};

/// Default bound on the number of `subject → identifier` mappings an encoder
/// keeps in memory.
///
/// Subjects are derived from the topic set a producer writes to, so this is
/// normally bounded by application configuration rather than by traffic. The
/// bound exists for the one case where it is not: a
/// [`SubjectNameStrategy::Custom`](crate::SubjectNameStrategy::Custom) that
/// derives subjects from message content.
///
/// Eviction is cheap here — every resolution mode is idempotent, so a
/// re-resolved subject costs one extra round-trip and returns the same answer.
pub const DEFAULT_MAX_SUBJECT_CACHE_ENTRIES: usize = 1000;

// ── SchemaResolution ──────────────────────────────────────────────────────

/// How an encoder turns a subject into the identifier it frames with.
///
/// This is the single most consequential producer setting, and the one most
/// often wrong in production: the default writes to the registry.
///
/// | Mode | Round-trip on a cold subject | Registry permission | Schema on the wire |
/// |---|---|---|---|
/// | [`AutoRegister`] *(default)* | `POST /subjects/{s}/versions` | `Subject:Write` | the encoder's own |
/// | [`LookupOnly`] | `POST /subjects/{s}` | `Subject:Read` | the encoder's own |
/// | [`UseLatestVersion`] | `GET /subjects/{s}/versions/latest` | `Subject:Read` | the encoder's own |
///
/// The last column is not a typo: **no mode changes what is serialised.** The
/// encoder always serialises with the schema it was built with. What changes is
/// which identifier the frame carries, and whether a mismatch is silently
/// papered over (`AutoRegister` creates a new version) or reported
/// (`LookupOnly` fails).
///
/// This mirrors the Confluent Java serdes' `auto.register.schemas` and
/// `use.latest.version` properties, including their defaults.
///
/// [`AutoRegister`]: Self::AutoRegister
/// [`LookupOnly`]: Self::LookupOnly
/// [`UseLatestVersion`]: Self::UseLatestVersion
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SchemaResolution {
    /// Register the encoder's schema under the subject and use the ID the
    /// registry assigns. **Default**, matching `auto.register.schemas=true`.
    ///
    /// Registration is idempotent: re-registering identical content returns the
    /// existing ID rather than creating a version. The risk is the case where
    /// the content is *not* identical — a field added in a local branch, a
    /// namespace typo — where this silently creates a new production version
    /// from a producer process. Prefer [`LookupOnly`](Self::LookupOnly)
    /// wherever schemas are owned by CI or a migration step.
    #[default]
    AutoRegister,

    /// Look the schema up without registering it, and fail if it is not there.
    ///
    /// Needs only read access. The failure is a not-found error
    /// ([`is_not_found`](SchemaRegError::is_not_found) is `true`,
    /// [`is_retryable`](SchemaRegError::is_retryable) is `false`), so a producer
    /// that has drifted from the registry stops at startup instead of writing
    /// records nobody registered a schema for.
    ///
    /// Requires the backend to implement
    /// [`lookup_schema`](SchemaRegistryClient::lookup_schema).
    LookupOnly,

    /// Frame with the identifier of the subject's **latest registered version**,
    /// whatever that currently is.
    ///
    /// Matches `use.latest.version=true`. Use it when schemas are evolved
    /// centrally and producers should follow rather than lead — for instance
    /// when the registry adds a field with a default that consumers already
    /// expect.
    ///
    /// The payload is still serialised with the encoder's own schema. Avro
    /// resolution on the consumer side makes that work for a compatible
    /// evolution and *only* for a compatible evolution: if the latest version is
    /// not backward-compatible with what this encoder writes, consumers get
    /// garbage. Keep the subject's compatibility level enforcing that, which is
    /// the same contract the Java serde relies on.
    ///
    /// Unlike the other two modes this is resolved once per subject and then
    /// cached; call [`invalidate_subject`] on the encoder to pick up a newer
    /// version without restarting.
    ///
    /// [`invalidate_subject`]: crate::AvroSchemaEncoder::invalidate_subject
    UseLatestVersion,
}

impl SchemaResolution {
    /// Whether this mode writes to the registry.
    #[must_use]
    pub fn writes_to_registry(self) -> bool {
        matches!(self, Self::AutoRegister)
    }
}

// ── Framing ───────────────────────────────────────────────────────────────

/// Which Confluent wire-format version an encoder emits.
///
/// The producer chooses; consumers accept either, because
/// [`decode_wire_format`](crate::decode_wire_format) reports a
/// [`SchemaKey`] rather than committing to one.
///
/// This selects the *identifier*, not its *placement*. Placement is the choice
/// of method: `encode` puts the prefix in front of the payload, while
/// [`encode_with_header`] returns the same prefix as a Kafka header value
/// alongside an unprefixed payload.
///
/// [`encode_with_header`]: crate::AvroSchemaEncoder::encode_with_header
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Framing {
    /// Wire format v0: `0x00` + a 4-byte registry-assigned [`SchemaId`].
    ///
    /// **Default.** Understood by every Confluent Platform release, Karapace,
    /// Redpanda, and Apicurio's compatibility API.
    #[default]
    SchemaId,

    /// Wire format v1: `0x01` + a 16-byte [`SchemaGuid`].
    ///
    /// Requires a registry that reports schema GUIDs — Confluent Platform 8 and
    /// newer. Because a GUID is a fingerprint of the schema rather than a
    /// per-registry counter, records framed this way stay readable after a
    /// cluster migration or a cross-region replication that would otherwise
    /// need every prefix rewritten.
    ///
    /// Building on [`SchemaResolution::AutoRegister`] costs one extra
    /// round-trip the first time a subject is seen: registration reports only
    /// the numeric ID, so the GUID is fetched with it. The other two modes get
    /// both identifiers in the response they already make.
    SchemaGuid,
}

impl Framing {
    /// Pick the identifier this framing needs out of what the registry reported.
    fn select(self, resolved: &ResolvedIdentity, subject: &str) -> Result<SchemaKey> {
        match self {
            Self::SchemaId => resolved.id.map(SchemaKey::Id).ok_or_else(|| {
                SchemaRegError::invalid_state(format!(
                    "the registry reported no numeric schema ID for subject '{subject}', \
                     which wire format v0 framing requires"
                ))
            }),
            Self::SchemaGuid => resolved.guid.map(SchemaKey::Guid).ok_or_else(|| {
                SchemaRegError::not_supported(format!(
                    "the registry reported no schema GUID for subject '{subject}'. \
                     Wire format v1 framing needs Confluent Platform 8 or newer; \
                     use Framing::SchemaId against an older registry"
                ))
            }),
        }
    }
}

// ── Resolution ────────────────────────────────────────────────────────────

/// Whatever identifiers the registry reported for a subject.
#[derive(Debug, Clone, Copy, Default)]
struct ResolvedIdentity {
    id: Option<SchemaId>,
    guid: Option<SchemaGuid>,
}

/// Error handed to coalesced waiters when the leader resolution is cancelled
/// (its task was aborted, or the subject was invalidated mid-flight).
pub(crate) fn subject_resolution_cancelled(subject: &String) -> SchemaRegError {
    SchemaRegError::invalid_state(format!(
        "schema resolution cancelled before completion for subject '{subject}'"
    ))
}

/// Resolve `subject` to the [`SchemaKey`] an encoder should frame with.
///
/// Callers wrap this in their own bounded, coalescing cache; it performs the
/// round-trips unconditionally.
pub(crate) async fn resolve_schema_key<C: SchemaRegistryClient>(
    registry: &C,
    resolution: SchemaResolution,
    framing: Framing,
    subject: &str,
    schema: &str,
    schema_type: SchemaType,
    references: &[SchemaReference],
) -> Result<SchemaKey> {
    let mut resolved = match resolution {
        SchemaResolution::AutoRegister => ResolvedIdentity {
            id: Some(
                registry
                    .register_schema(subject, schema, schema_type, references)
                    .await?,
            ),
            guid: None,
        },
        SchemaResolution::LookupOnly => {
            match registry
                .lookup_schema(subject, schema, schema_type, references)
                .await?
            {
                Some(found) => ResolvedIdentity {
                    id: found.id,
                    guid: found.guid,
                },
                None => {
                    return Err(SchemaRegError::api(
                        error_code::SCHEMA_NOT_FOUND,
                        format!(
                            "subject '{subject}' has no registered version matching this \
                             encoder's schema, and SchemaResolution::LookupOnly forbids \
                             creating one. Register the schema out of band, or switch to \
                             SchemaResolution::AutoRegister"
                        ),
                    ));
                }
            }
        }
        SchemaResolution::UseLatestVersion => {
            let latest = registry.get_latest_schema(subject).await?;
            ResolvedIdentity {
                id: latest.id,
                guid: latest.guid,
            }
        }
    };

    // v1 framing after an auto-registration: the registration response carries
    // only the numeric ID, so fetch the schema it names to learn the GUID. One
    // extra round-trip per subject, and served from cache when the encoder sits
    // behind `CachedSchemaRegistry`.
    if framing == Framing::SchemaGuid
        && resolved.guid.is_none()
        && let Some(id) = resolved.id
    {
        resolved.guid = registry.get_schema_by_id(id).await?.guid;
    }

    framing.select(&resolved, subject)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::types::Schema;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    const SCHEMA: &str = r#"{"type":"string"}"#;
    const GUID: SchemaGuid = SchemaGuid::from_bytes([7u8; 16]);

    #[derive(Default)]
    struct Mock {
        registers: AtomicU32,
        lookups: AtomicU32,
        latests: AtomicU32,
        by_id: AtomicU32,
        /// When false, `lookup_schema` answers `Ok(None)`.
        registered: bool,
        /// When true, responses carry a GUID as Confluent Platform 8 does.
        reports_guid: bool,
    }

    impl Mock {
        fn schema(&self, id: u32) -> Arc<Schema> {
            let mut s = Schema::new(id, SchemaType::Avro, SCHEMA);
            if self.reports_guid {
                s.guid = Some(GUID);
            }
            Arc::new(s)
        }
    }

    impl SchemaRegistryClient for Mock {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            self.by_id.fetch_add(1, Ordering::SeqCst);
            Ok(self.schema(id.as_u32()))
        }
        async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
            self.latests.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(
                Arc::unwrap_or_clone(self.schema(9)).with_subject(subject, 3i32),
            ))
        }
        async fn get_schema_by_version(
            &self,
            _: &str,
            _: crate::types::SchemaVersion,
        ) -> Result<Arc<Schema>> {
            unreachable!("not part of resolution")
        }
        async fn register_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<SchemaId> {
            self.registers.fetch_add(1, Ordering::SeqCst);
            Ok(SchemaId::new(1))
        }
        async fn lookup_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<Option<Arc<Schema>>> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            Ok(self.registered.then(|| self.schema(5)))
        }
    }

    async fn resolve(mock: &Mock, r: SchemaResolution, f: Framing) -> Result<SchemaKey> {
        resolve_schema_key(mock, r, f, "orders-value", SCHEMA, SchemaType::Avro, &[]).await
    }

    #[tokio::test]
    async fn auto_register_is_the_default_and_registers_once() {
        let mock = Mock::default();
        let key = resolve(&mock, SchemaResolution::default(), Framing::default())
            .await
            .unwrap();
        assert_eq!(key, SchemaId::new(1));
        assert_eq!(mock.registers.load(Ordering::SeqCst), 1);
        assert_eq!(mock.lookups.load(Ordering::SeqCst), 0);
        assert!(SchemaResolution::default().writes_to_registry());
    }

    #[tokio::test]
    async fn lookup_only_never_writes() {
        let mock = Mock {
            registered: true,
            ..Mock::default()
        };
        let key = resolve(&mock, SchemaResolution::LookupOnly, Framing::SchemaId)
            .await
            .unwrap();
        assert_eq!(key, SchemaId::new(5));
        assert_eq!(mock.registers.load(Ordering::SeqCst), 0);
        assert!(!SchemaResolution::LookupOnly.writes_to_registry());
    }

    /// The whole point of `LookupOnly`: an unregistered schema must stop the
    /// producer, with an error a retry loop will not spin on.
    #[tokio::test]
    async fn lookup_only_fails_not_found_when_unregistered() {
        let mock = Mock::default();
        let err = resolve(&mock, SchemaResolution::LookupOnly, Framing::SchemaId)
            .await
            .unwrap_err();
        assert!(err.is_not_found(), "{err}");
        assert!(!err.is_retryable(), "{err}");
        assert_eq!(mock.registers.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn use_latest_version_reads_the_subject_head() {
        let mock = Mock::default();
        let key = resolve(&mock, SchemaResolution::UseLatestVersion, Framing::SchemaId)
            .await
            .unwrap();
        assert_eq!(key, SchemaId::new(9));
        assert_eq!(mock.latests.load(Ordering::SeqCst), 1);
        assert_eq!(mock.registers.load(Ordering::SeqCst), 0);
    }

    /// Registration reports only the numeric ID, so v1 framing has to ask for
    /// the GUID separately — exactly once.
    #[tokio::test]
    async fn guid_framing_after_auto_register_fetches_the_guid() {
        let mock = Mock {
            reports_guid: true,
            ..Mock::default()
        };
        let key = resolve(&mock, SchemaResolution::AutoRegister, Framing::SchemaGuid)
            .await
            .unwrap();
        assert_eq!(key, SchemaKey::Guid(GUID));
        assert_eq!(mock.by_id.load(Ordering::SeqCst), 1);
    }

    /// A lookup already carries the GUID, so no follow-up is needed.
    #[tokio::test]
    async fn guid_framing_after_lookup_needs_no_extra_call() {
        let mock = Mock {
            registered: true,
            reports_guid: true,
            ..Mock::default()
        };
        let key = resolve(&mock, SchemaResolution::LookupOnly, Framing::SchemaGuid)
            .await
            .unwrap();
        assert_eq!(key, SchemaKey::Guid(GUID));
        assert_eq!(mock.by_id.load(Ordering::SeqCst), 0);
    }

    /// A pre-Platform-8 registry reports no GUID. That must be a clear
    /// `NotSupported`, not a frame built from a fabricated identifier.
    #[tokio::test]
    async fn guid_framing_against_a_registry_without_guids_is_not_supported() {
        let mock = Mock::default();
        let err = resolve(&mock, SchemaResolution::AutoRegister, Framing::SchemaGuid)
            .await
            .unwrap_err();
        assert!(err.is_not_supported(), "{err}");
        assert!(!err.is_retryable(), "{err}");
    }
}
