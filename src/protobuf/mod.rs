//! Protobuf serialisation + Confluent wire-format framing, with the
//! message-index path derived automatically from the message descriptor.
//!
//! # Feature requirement
//!
//! Gated behind the **`protobuf`** Cargo feature:
//!
//! ```toml
//! [dependencies]
//! schemreg = { version = "0.6", features = ["protobuf"] }
//! ```
//!
//! # Why this module exists
//!
//! Framing a Protobuf payload for Confluent Schema Registry needs one thing
//! Avro and JSON Schema do not: the **message-index path**, identifying which
//! message type inside the registered `.proto` file was serialised.
//!
//! Getting that path wrong is not a clean failure. The consumer slices the
//! payload at the wrong offset and hands the Protobuf runtime bytes that are
//! *almost* a valid message. Requiring callers to hand-write
//! `&[1, 0]` next to every encode is a standing invitation to that bug —
//! especially since the correct value changes when someone reorders messages in
//! the `.proto` file, with nothing at the call site to notice.
//!
//! [`ProtobufSchemaEncoder`] derives the path from the compiled descriptor
//! instead, so it is correct by construction and stays correct when the
//! `.proto` changes.
//!
//! # How the path is derived
//!
//! `prost-reflect` exposes each message's location within its
//! `FileDescriptorProto` as a field path. In that encoding, field 4 is
//! `message_type` (top-level messages) and field 3 is `nested_type`:
//!
//! | Message | Descriptor path | Confluent index |
//! |---|---|---|
//! | first top-level | `[4, 0]` | `[0]` |
//! | third top-level | `[4, 2]` | `[2]` |
//! | first nested in the second | `[4, 1, 3, 0]` | `[1, 0]` |
//!
//! The Confluent index is every second element starting at position 1 — see
//! [`message_index_path`].
//!
//! # Example
//!
//! ```rust,ignore
//! use schemreg::protobuf::{ProtobufSchemaDecoder, ProtobufSchemaEncoder};
//! use schemreg::EncodeTarget;
//!
//! // `Order` derives prost::Message + prost_reflect::ReflectMessage
//! // (e.g. via prost-build with `file_descriptor_set_path`).
//! let encoder = ProtobufSchemaEncoder::builder()
//!     .registry(registry.clone())
//!     .schema(PROTO_SOURCE)            // the .proto text, registered as-is
//!     .descriptor(Order::default().descriptor())
//!     .build()?;
//!
//! let framed = encoder.encode(&order, "orders", EncodeTarget::Value).await?;
//!
//! let decoder = ProtobufSchemaDecoder::new(registry);
//! let decoded: Order = decoder.decode(framed).await?;
//! ```

use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use prost_reflect::MessageDescriptor;

use crate::cache_inner::InMemoryCache;
use crate::error::{Result, SchemaRegError};
use crate::resolver::{
    DEFAULT_MAX_SUBJECT_CACHE_ENTRIES, Framing, SchemaResolution, resolve_schema_key,
    subject_resolution_cancelled,
};
use crate::subject::SubjectNameStrategy;
use crate::traits::SchemaRegistryClient;
use crate::types::{EncodeTarget, SchemaId, SchemaKey, SchemaReference, SchemaType};
use crate::wire::{HeaderFramed, decode_protobuf_message_indexes, decode_wire_format_bytes};

/// Field number of `FileDescriptorProto.message_type`.
const FILE_MESSAGE_TYPE_FIELD: i32 = 4;
/// Field number of `DescriptorProto.nested_type`.
const NESTED_TYPE_FIELD: i32 = 3;

/// Derive the Confluent message-index path for `descriptor`.
///
/// Returns the sequence Confluent's serde writes between the 5-byte header and
/// the Protobuf payload: the position of the message among its file's top-level
/// messages, followed by one position per level of nesting.
///
/// # Errors
///
/// Returns a configuration error if the descriptor's path is not a well-formed
/// `message_type` / `nested_type` chain — which would mean the descriptor
/// describes something other than a message (an enum, a service) and the caller
/// has passed the wrong thing.
///
/// # Example
///
/// ```rust,ignore
/// let path = message_index_path(&Order::default().descriptor())?;
/// assert_eq!(path, vec![0]); // first top-level message
/// ```
pub fn message_index_path(descriptor: &MessageDescriptor) -> Result<Vec<u32>> {
    message_index_path_from(descriptor.path(), descriptor.full_name())
}

/// The whole of [`message_index_path`], over a raw descriptor path.
///
/// Split out so the tests can drive malformed shapes — an enum's path, an odd
/// length — that `prost-reflect` will not construct. A test that re-implemented
/// this validation would pass while the real function was broken.
fn message_index_path_from(path: &[i32], full_name: &str) -> Result<Vec<u32>> {
    // A message path alternates (field number, index): [4, i] then [3, j]...
    if path.len() < 2 || !path.len().is_multiple_of(2) {
        return Err(SchemaRegError::config(format!(
            "descriptor for '{full_name}' has an unexpected path {path:?}; expected an \
             alternating message_type/nested_type chain"
        )));
    }
    if path[0] != FILE_MESSAGE_TYPE_FIELD {
        return Err(SchemaRegError::config(format!(
            "descriptor for '{full_name}' does not start at FileDescriptorProto.message_type \
             (field {FILE_MESSAGE_TYPE_FIELD}); got field {}",
            path[0]
        )));
    }
    for (level, chunk) in path.as_chunks::<2>().0.iter().enumerate().skip(1) {
        if chunk[0] != NESTED_TYPE_FIELD {
            return Err(SchemaRegError::config(format!(
                "descriptor for '{full_name}' has a non-nested_type segment at level {level}: \
                 expected field {NESTED_TYPE_FIELD}, got {}",
                chunk[0]
            )));
        }
    }

    path.iter()
        .skip(1)
        .step_by(2)
        .map(|&position| {
            u32::try_from(position).map_err(|_| {
                SchemaRegError::config(format!(
                    "descriptor for '{full_name}' has a negative position {position} in its path"
                ))
            })
        })
        .collect()
}

// ── Encoder ───────────────────────────────────────────────────────────────

/// Cached subject-resolution entry.
struct EncoderEntry {
    key: SchemaKey,
}

/// Serialises a [`prost::Message`] to Confluent-framed Protobuf bytes.
///
/// On the first call for a given subject the encoder registers the `.proto`
/// source with the registry and caches the assigned schema ID. Subsequent
/// encodes hit only the in-memory cache; concurrent cold calls for the same
/// subject coalesce behind a single registration.
///
/// The message-index path is computed once at build time from the descriptor,
/// so it costs nothing per message and cannot drift from the `.proto`.
pub struct ProtobufSchemaEncoder<C> {
    registry: C,
    schema_str: String,
    message_indexes: Vec<u32>,
    /// Fully-qualified message name, used by the record-name subject strategies.
    full_name: String,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    resolution: SchemaResolution,
    framing: Framing,
    cache: InMemoryCache<String, EncoderEntry>,
}

impl<C> std::fmt::Debug for ProtobufSchemaEncoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufSchemaEncoder")
            .field("full_name", &self.full_name)
            .field("message_indexes", &self.message_indexes)
            .field("strategy", &self.strategy)
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .field("cached_subjects", &self.cache.len())
            .finish_non_exhaustive()
    }
}

impl<C: SchemaRegistryClient> ProtobufSchemaEncoder<C> {
    /// Create a builder for `ProtobufSchemaEncoder`.
    pub fn builder() -> ProtobufSchemaEncoderBuilder<C> {
        ProtobufSchemaEncoderBuilder::new()
    }

    /// The message-index path this encoder frames with.
    ///
    /// Derived from the descriptor unless overridden with
    /// [`message_indexes`](ProtobufSchemaEncoderBuilder::message_indexes).
    #[must_use]
    pub fn message_indexes(&self) -> &[u32] {
        &self.message_indexes
    }

    /// Return the cached identifier for `subject`, if it has been resolved.
    #[must_use]
    pub fn cached_schema_key(&self, subject: &str) -> Option<SchemaKey> {
        self.cache.get(&subject.to_string()).map(|e| e.key)
    }

    /// Return the cached schema ID for `subject`, if it has been resolved
    /// **and** framed as a numeric ID (`None` under [`Framing::SchemaGuid`]).
    #[must_use]
    pub fn cached_schema_id(&self, subject: &str) -> Option<SchemaId> {
        self.cached_schema_key(subject).and_then(SchemaKey::as_id)
    }

    /// Number of `subject → identifier` mappings currently cached.
    pub fn cached_subject_count(&self) -> usize {
        self.cache.len()
    }

    /// Forget the cached identifier for `subject`, forcing the next encode to
    /// resolve it again — the way to pick up a newer version under
    /// [`SchemaResolution::UseLatestVersion`] without a restart.
    pub fn invalidate_subject(&self, subject: &str) {
        self.cache.invalidate(&subject.to_string());
    }

    async fn resolve_subject(&self, subject: &str) -> Result<Arc<EncoderEntry>> {
        self.cache
            .get_or_fetch(subject.to_string(), || async {
                let key = resolve_schema_key(
                    &self.registry,
                    self.resolution,
                    self.framing,
                    subject,
                    &self.schema_str,
                    SchemaType::Protobuf,
                    &self.references,
                )
                .await?;
                Ok(Arc::new(EncoderEntry { key }))
            })
            .await
    }

    /// Serialise `message` to Confluent-framed Protobuf bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the subject cannot be resolved (registry or
    /// configuration error). Protobuf serialisation itself is infallible for a
    /// well-formed `prost` message.
    pub async fn encode<M: Message>(
        &self,
        message: &M,
        topic: &str,
        target: EncodeTarget,
    ) -> Result<Bytes> {
        let subject = self
            .strategy
            .subject_name(topic, Some(&self.full_name), target)?;
        let entry = self.resolve_subject(&subject).await?;
        let body = message.encode_to_vec();
        Ok(crate::wire::encode_protobuf_wire_format(
            entry.key,
            &self.message_indexes,
            &body,
        ))
    }

    /// Serialise `message` with the identifier **and** the message-index array
    /// in a Kafka header instead of in the payload prefix.
    ///
    /// The header value carries the magic byte, the identifier, and the
    /// message-index path; the payload is bare Protobuf. Write both, or a
    /// consumer can recover neither the schema nor the message type.
    ///
    /// # Errors
    ///
    /// As [`encode`](Self::encode).
    pub async fn encode_with_header<M: Message>(
        &self,
        message: &M,
        topic: &str,
        target: EncodeTarget,
    ) -> Result<HeaderFramed> {
        let subject = self
            .strategy
            .subject_name(topic, Some(&self.full_name), target)?;
        let entry = self.resolve_subject(&subject).await?;
        Ok(HeaderFramed::new(
            target,
            entry.key,
            Some(&self.message_indexes),
            Bytes::from(message.encode_to_vec()),
        ))
    }
}

/// Builder for [`ProtobufSchemaEncoder`].
pub struct ProtobufSchemaEncoderBuilder<C> {
    registry: Option<C>,
    schema: Option<String>,
    descriptor: Option<MessageDescriptor>,
    message_indexes: Option<Vec<u32>>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    resolution: SchemaResolution,
    framing: Framing,
    max_subject_cache_entries: usize,
}

impl<C> std::fmt::Debug for ProtobufSchemaEncoderBuilder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufSchemaEncoderBuilder")
            .field("registry", &self.registry.is_some())
            .field("schema_set", &self.schema.is_some())
            .field(
                "descriptor",
                &self.descriptor.as_ref().map(|d| d.full_name().to_string()),
            )
            .field("message_indexes", &self.message_indexes)
            .field("strategy", &self.strategy)
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .finish()
    }
}

impl<C: SchemaRegistryClient> ProtobufSchemaEncoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            schema: None,
            descriptor: None,
            message_indexes: None,
            strategy: SubjectNameStrategy::TopicName,
            references: Vec::new(),
            resolution: SchemaResolution::default(),
            framing: Framing::default(),
            max_subject_cache_entries: DEFAULT_MAX_SUBJECT_CACHE_ENTRIES,
        }
    }

    /// Choose how a subject resolves to an identifier
    /// (default: [`SchemaResolution::AutoRegister`]).
    pub fn resolution(mut self, resolution: SchemaResolution) -> Self {
        self.resolution = resolution;
        self
    }

    /// Choose the wire-format version (default: [`Framing::SchemaId`], v0).
    pub fn framing(mut self, framing: Framing) -> Self {
        self.framing = framing;
        self
    }

    /// Set the schema registry client (required).
    pub fn registry(mut self, registry: C) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the `.proto` source text registered with the registry (required).
    ///
    /// This is the schema *as the registry stores it* — the file content, not a
    /// descriptor. Consumers in other languages resolve it by ID.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the descriptor of the message type being encoded (required).
    ///
    /// The message-index path and the fully-qualified record name are both
    /// derived from it. With `prost-reflect`, obtain one via
    /// `MyMessage::default().descriptor()`.
    pub fn descriptor(mut self, descriptor: MessageDescriptor) -> Self {
        self.descriptor = Some(descriptor);
        self
    }

    /// Override the derived message-index path.
    ///
    /// Rarely needed — the derived value is correct whenever the descriptor
    /// comes from the same `.proto` that was registered. Use this only when the
    /// registered schema's message ordering differs from the compiled
    /// descriptor's, which is itself a situation worth fixing at the source.
    pub fn message_indexes(mut self, indexes: Vec<u32>) -> Self {
        self.message_indexes = Some(indexes);
        self
    }

    /// Set the subject name strategy (default: `TopicName`).
    pub fn strategy(mut self, strategy: SubjectNameStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set schema references (default: empty).
    ///
    /// Protobuf schemas that `import` another `.proto` need one reference per
    /// import, naming the subject the imported file is registered under.
    pub fn references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Bound the `subject → schema ID` cache (default:
    /// [`DEFAULT_MAX_SUBJECT_CACHE_ENTRIES`]).
    pub fn max_subject_cache_entries(mut self, max_entries: usize) -> Self {
        self.max_subject_cache_entries = max_entries;
        self
    }

    /// Build the encoder.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `registry`, `schema`, or `descriptor`
    /// was not set, or if the message-index path cannot be derived from the
    /// descriptor.
    pub fn build(self) -> Result<ProtobufSchemaEncoder<C>> {
        let registry = self
            .registry
            .ok_or_else(|| SchemaRegError::config("ProtobufSchemaEncoder: registry must be set"))?;
        let schema_str = self
            .schema
            .ok_or_else(|| SchemaRegError::config("ProtobufSchemaEncoder: schema must be set"))?;
        let descriptor = self.descriptor.ok_or_else(|| {
            SchemaRegError::config(
                "ProtobufSchemaEncoder: descriptor must be set — it is what makes the \
                 message-index path correct by construction",
            )
        })?;

        let message_indexes = match self.message_indexes {
            Some(explicit) => explicit,
            None => message_index_path(&descriptor)?,
        };

        Ok(ProtobufSchemaEncoder {
            registry,
            schema_str,
            message_indexes,
            full_name: descriptor.full_name().to_string(),
            strategy: self.strategy,
            references: self.references,
            resolution: self.resolution,
            framing: self.framing,
            cache: InMemoryCache::new(
                Some(self.max_subject_cache_entries.max(1)),
                subject_resolution_cancelled,
            ),
        })
    }
}

// ── Decoder ───────────────────────────────────────────────────────────────

/// A Confluent-framed Protobuf message, unframed.
#[derive(Debug, Clone)]
pub struct UnframedProtobuf {
    /// The schema identifier the wire prefix named — an ID (v0) or a GUID (v1).
    pub key: SchemaKey,
    /// Message-index path identifying which message type was serialised.
    pub message_indexes: Vec<u32>,
    /// The Protobuf payload, header and message-index stripped.
    pub payload: Bytes,
}

/// Strips Confluent framing from a Protobuf message and decodes it with `prost`.
///
/// # Verifying the message type
///
/// A Protobuf payload does not identify its own type — the message-index path
/// does. Decoding `Invoice` bytes as an `Order` usually *succeeds*, silently,
/// producing a struct full of defaults and unknown fields.
///
/// [`with_expected_descriptor`](Self::with_expected_descriptor) closes that
/// hole: the decoder checks the wire message-index against the expected type
/// and rejects a mismatch rather than handing back a plausible-looking wrong
/// answer.
pub struct ProtobufSchemaDecoder<C> {
    registry: C,
    expected_indexes: Option<Vec<u32>>,
    expected_name: Option<String>,
}

impl<C> std::fmt::Debug for ProtobufSchemaDecoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufSchemaDecoder")
            .field("expected_indexes", &self.expected_indexes)
            .field("expected_name", &self.expected_name)
            .finish_non_exhaustive()
    }
}

impl<C: SchemaRegistryClient> ProtobufSchemaDecoder<C> {
    /// Create a decoder backed by the given registry client.
    ///
    /// The registry is used by [`schema_for`](Self::schema_for) to resolve the
    /// `.proto` source behind a message. Plain [`decode`](Self::decode) needs no
    /// registry round-trip: the message type is supplied by the caller and the
    /// framing is self-describing.
    pub fn new(registry: C) -> Self {
        Self {
            registry,
            expected_indexes: None,
            expected_name: None,
        }
    }

    /// Reject messages whose message-index path does not match `descriptor`.
    ///
    /// Turns a silent mis-decode into a loud [`SchemaRegError::WireFormat`].
    ///
    /// # Errors
    ///
    /// Returns a configuration error if the path cannot be derived.
    pub fn with_expected_descriptor(mut self, descriptor: &MessageDescriptor) -> Result<Self> {
        self.expected_indexes = Some(message_index_path(descriptor)?);
        self.expected_name = Some(descriptor.full_name().to_string());
        Ok(self)
    }

    /// Strip the Confluent header and message-index without decoding.
    ///
    /// Useful for routing: inspect [`message_indexes`](UnframedProtobuf::message_indexes)
    /// to decide which concrete type to decode into.
    ///
    /// # Errors
    ///
    /// Returns a wire-format error if the header or the message-index array is
    /// malformed.
    pub fn unframe(&self, data: &Bytes) -> Result<UnframedProtobuf> {
        let (key, after_prefix) = decode_wire_format_bytes(data)?;
        let (message_indexes, offset) = decode_protobuf_message_indexes(&after_prefix)?;

        if let Some(expected) = &self.expected_indexes
            && message_indexes != *expected
        {
            return Err(SchemaRegError::wire_format(format!(
                "Protobuf message-index {message_indexes:?} does not match the expected \
                 type{} with index {expected:?} — the payload is a different message type",
                self.expected_name
                    .as_deref()
                    .map(|n| format!(" '{n}'"))
                    .unwrap_or_default()
            )));
        }

        Ok(UnframedProtobuf {
            key,
            message_indexes,
            payload: after_prefix.slice(offset..),
        })
    }

    /// Decode a Confluent-framed Protobuf message into `M`.
    ///
    /// # Errors
    ///
    /// Returns an error if the framing is invalid, the message-index does not
    /// match a configured expected descriptor, or the payload is not a valid
    /// encoding of `M`.
    pub async fn decode<M: Message + Default>(&self, data: Bytes) -> Result<M> {
        let unframed = self.unframe(&data)?;
        M::decode(unframed.payload).map_err(|e| {
            SchemaRegError::wire_format(format!("Protobuf deserialization failed: {e}"))
        })
    }

    /// Fetch the registered `.proto` source behind a framed message.
    ///
    /// # Errors
    ///
    /// Returns an error if the framing is invalid or the registry lookup fails.
    pub async fn schema_for(&self, data: &Bytes) -> Result<Arc<crate::types::Schema>> {
        let unframed = self.unframe(data)?;
        self.registry.get_schema_by_key(unframed.key).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use prost_reflect::DescriptorPool;
    use prost_reflect::prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};

    /// Build a descriptor pool for a synthetic `.proto` with this shape:
    ///
    /// ```text
    /// package test;
    /// message Order   { }              // top-level 0        → [0]
    /// message Invoice {                // top-level 1        → [1]
    ///   message Line  { }              //   nested 0         → [1, 0]
    ///   message Tax   {                //   nested 1         → [1, 1]
    ///     message Rate { }             //     nested 0       → [1, 1, 0]
    ///   }
    /// }
    /// message Refund  { }              // top-level 2        → [2]
    /// ```
    ///
    /// Building the pool programmatically keeps this test free of a
    /// `prost-build` step while still exercising real `prost-reflect`
    /// descriptors rather than hand-written path arrays.
    fn test_pool() -> DescriptorPool {
        fn msg(name: &str, nested: Vec<DescriptorProto>) -> DescriptorProto {
            DescriptorProto {
                name: Some(name.to_string()),
                nested_type: nested,
                ..Default::default()
            }
        }

        let file = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                msg("Order", vec![]),
                msg(
                    "Invoice",
                    vec![msg("Line", vec![]), msg("Tax", vec![msg("Rate", vec![])])],
                ),
                msg("Refund", vec![]),
            ],
            ..Default::default()
        };

        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .expect("the synthetic descriptor set is well-formed")
    }

    fn index_for(pool: &DescriptorPool, name: &str) -> Vec<u32> {
        let Some(descriptor) = pool.get_message_by_name(name) else {
            unreachable!("{name} must exist in the pool")
        };
        message_index_path(&descriptor).expect("path derivation must succeed")
    }

    #[test]
    fn first_top_level_message_derives_the_default_index() {
        assert_eq!(index_for(&test_pool(), "test.Order"), vec![0]);
    }

    #[test]
    fn later_top_level_messages_derive_their_position() {
        let pool = test_pool();
        assert_eq!(index_for(&pool, "test.Invoice"), vec![1]);
        assert_eq!(index_for(&pool, "test.Refund"), vec![2]);
    }

    #[test]
    fn nested_messages_derive_a_multi_segment_path() {
        let pool = test_pool();
        assert_eq!(index_for(&pool, "test.Invoice.Line"), vec![1, 0]);
        assert_eq!(index_for(&pool, "test.Invoice.Tax"), vec![1, 1]);
        assert_eq!(index_for(&pool, "test.Invoice.Tax.Rate"), vec![1, 1, 0]);
    }

    /// Every derived path must survive the wire codec unchanged — this is the
    /// join between descriptor derivation and the Confluent framing.
    #[test]
    fn derived_paths_round_trip_through_the_wire_codec() {
        use crate::wire::{
            decode_protobuf_message_indexes, decode_wire_format, encode_protobuf_wire_format,
        };

        let pool = test_pool();
        for name in [
            "test.Order",
            "test.Invoice",
            "test.Refund",
            "test.Invoice.Line",
            "test.Invoice.Tax",
            "test.Invoice.Tax.Rate",
        ] {
            let indexes = index_for(&pool, name);
            let framed = encode_protobuf_wire_format(1u32, &indexes, b"body");
            let (_, after) = decode_wire_format(&framed).unwrap();
            let (decoded, offset) = decode_protobuf_message_indexes(after).unwrap();
            assert_eq!(decoded, indexes, "{name}");
            assert_eq!(&after[offset..], b"body", "{name}");
        }
    }

    /// The first top-level message must produce the single-`0x00` optimised
    /// framing — this is what makes the common case byte-identical to Java's.
    #[test]
    fn first_message_produces_the_optimised_single_byte_framing() {
        use crate::wire::encode_protobuf_wire_format;

        let indexes = index_for(&test_pool(), "test.Order");
        let framed = encode_protobuf_wire_format(7u32, &indexes, b"x");
        assert_eq!(
            &framed[..],
            &[0x00, 0, 0, 0, 7, 0x00, b'x'][..],
            "path [0] must collapse to one byte"
        );
    }

    /// A descriptor for a *nested* type is still valid — but an enum descriptor
    /// would have a path starting at field 5, which must be rejected rather
    /// than silently producing a plausible-looking wrong index.
    #[test]
    fn a_non_message_path_is_rejected() {
        // Field 5 is FileDescriptorProto.enum_type, not message_type.
        let err = message_index_path_from(&[5, 0], "test.Colour")
            .expect_err("an enum path must be rejected");
        assert!(err.is_config_error(), "{err}");
    }

    #[test]
    fn a_malformed_path_is_rejected() {
        for bad in [vec![], vec![4], vec![4, 0, 3]] {
            assert!(
                message_index_path_from(&bad, "test.X").is_err(),
                "{bad:?} must be rejected"
            );
        }
        // A second segment that is not nested_type (field 3).
        assert!(message_index_path_from(&[4, 0, 2, 1], "test.X").is_err());
        // A negative position cannot be a descriptor index.
        assert!(message_index_path_from(&[4, -1], "test.X").is_err());
    }

    /// The raw-path entry point must agree with the descriptor one, or the
    /// tests above would be validating a different function than production
    /// uses.
    #[test]
    fn the_raw_path_entry_point_matches_the_descriptor_one() {
        let pool = test_pool();
        for name in ["test.Order", "test.Invoice.Tax.Rate"] {
            let Some(descriptor) = pool.get_message_by_name(name) else {
                unreachable!("{name} must exist in the pool")
            };
            assert_eq!(
                message_index_path(&descriptor).ok(),
                message_index_path_from(descriptor.path(), descriptor.full_name()).ok(),
                "{name}"
            );
        }
    }
}
