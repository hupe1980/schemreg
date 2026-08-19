//! Async trait interfaces for schema registry backends, caches, and codecs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Result, SchemaRegError};
use crate::types::{
    CompatibilityLevel, EncodeTarget, Schema, SchemaGuid, SchemaId, SchemaKey, SchemaReference,
    SchemaType, SchemaVersion,
};
use crate::wire::HeaderFramed;

// ── The method list ───────────────────────────────────────────────────────
//
// `SchemaRegistryClient` has to be mirrored four times: the object-safe
// `DynSchemaRegistryClient`, the blanket impl that bridges the two, and
// forwarding impls for `&T` and `Arc<T>`. Writing those out by hand is ~40
// lines per method, four near-identical copies that must stay in lockstep —
// exactly the shape where adding a method silently forgets one copy, and a
// generic caller then reaches a `NotSupported` default instead of the real
// implementation.
//
// So the signatures are declared once, here, and every mirror is generated
// from them. Adding a registry operation means adding one line to this list
// plus the documented declaration in the trait itself.

macro_rules! with_registry_methods {
    ($emit:ident) => {
        $emit! {
            get_schema_by_id(id: SchemaId) -> Arc<Schema>;
            get_schema_by_guid(guid: SchemaGuid) -> Arc<Schema>;
            get_schema_by_key(key: SchemaKey) -> Arc<Schema>;
            get_latest_schema(subject: &'a str) -> Arc<Schema>;
            get_schema_by_version(subject: &'a str, version: SchemaVersion) -> Arc<Schema>;
            lookup_schema(
                subject: &'a str,
                schema: &'a str,
                schema_type: SchemaType,
                references: &'a [SchemaReference],
            ) -> Option<Arc<Schema>>;
            register_schema(
                subject: &'a str,
                schema: &'a str,
                schema_type: SchemaType,
                references: &'a [SchemaReference],
            ) -> SchemaId;
            check_compatibility(
                subject: &'a str,
                schema: &'a str,
                schema_type: SchemaType,
                references: &'a [SchemaReference],
            ) -> bool;
            delete_subject(subject: &'a str, permanent: bool) -> Vec<SchemaVersion>;
            delete_version(
                subject: &'a str,
                version: SchemaVersion,
                permanent: bool,
            ) -> SchemaVersion;
            get_subjects() -> Vec<String>;
            get_versions(subject: &'a str) -> Vec<SchemaVersion>;
            get_compatibility(subject: &'a str) -> CompatibilityLevel;
            set_compatibility(subject: &'a str, level: CompatibilityLevel) -> ();
            health_check() -> ();
        }
    };
}

// ── SchemaRegistryClient ──────────────────────────────────────────────────

/// Async client interface for a schema registry.
///
/// Four methods are required; every other method defaults to
/// [`SchemaRegError::not_supported`] so a minimal backend stays small. That
/// variant is never [`is_retryable`](SchemaRegError::is_retryable), so a
/// caller's retry loop can tell "this backend cannot do that" apart from
/// "the backend is down".
///
/// Methods use RPITIT (`-> impl Future + Send`) for zero-cost monomorphized
/// dispatch; concrete impls may write plain `async fn` and the compiler checks
/// `Send`-ness. For trait objects use [`DynSchemaRegistryClient`], which every
/// implementor gets automatically.
///
/// # Implementing a custom backend
///
/// ```rust
/// use std::sync::Arc;
/// use schemreg::{Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion};
/// use schemreg::error::{Result, SchemaRegError};
///
/// struct InMemoryRegistry;
///
/// impl SchemaRegistryClient for InMemoryRegistry {
///     async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
///         Err(SchemaRegError::invalid_state(format!("schema {id} not found")))
///     }
///     async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
///         Err(SchemaRegError::invalid_state(format!("subject {subject} not found")))
///     }
///     async fn get_schema_by_version(&self, subject: &str, version: SchemaVersion) -> Result<Arc<Schema>> {
///         Err(SchemaRegError::invalid_state(format!("{subject}@{version} not found")))
///     }
///     async fn register_schema(
///         &self, _subject: &str, _schema: &str,
///         _schema_type: SchemaType, _references: &[SchemaReference],
///     ) -> Result<SchemaId> {
///         Ok(SchemaId::from(1u32))
///     }
/// }
///
/// // Composes transparently with the cache.
/// use schemreg::CachedSchemaRegistry;
/// let cached = Arc::new(CachedSchemaRegistry::new(InMemoryRegistry));
/// ```
pub trait SchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its registry-assigned ID (wire format v0).
    ///
    /// Schema IDs are immutable — a given ID always maps to the same schema —
    /// which is what makes [`CachedSchemaRegistry`](crate::CachedSchemaRegistry)
    /// able to cache the result forever.
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_;

    /// Retrieve the latest schema registered under the given subject.
    ///
    /// Never cached: a newer version can be registered at any moment.
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a;

    /// Retrieve a specific version of a schema under a subject.
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a;

    /// Register a schema under the given subject, returning its ID.
    ///
    /// Idempotent: re-registering identical content returns the existing ID
    /// rather than creating a new version. Pass `&[]` for `references` when
    /// there are no dependencies.
    ///
    /// Requires write access. A consumer, or a producer in an environment where
    /// schemas are registered by CI rather than by the application, should call
    /// [`lookup_schema`](Self::lookup_schema) instead.
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<SchemaId>> + Send + 'a;

    /// Retrieve a schema by its registry-independent GUID (wire format v1).
    ///
    /// Requires Confluent Platform 8 or newer. Default: `Err(NotSupported)`.
    fn get_schema_by_guid(
        &self,
        _guid: SchemaGuid,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_ {
        async {
            Err(SchemaRegError::not_supported(
                "get_schema_by_guid is not supported by this registry",
            ))
        }
    }

    /// Retrieve a schema by whichever identifier a record's framing carried.
    ///
    /// Dispatches to [`get_schema_by_id`](Self::get_schema_by_id) or
    /// [`get_schema_by_guid`](Self::get_schema_by_guid). This is the call a
    /// consumer wants: [`decode_wire_format`](crate::decode_wire_format)
    /// returns a [`SchemaKey`] precisely because the producer, not the
    /// consumer, chooses which wire format version to emit.
    fn get_schema_by_key(
        &self,
        key: SchemaKey,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_ {
        async move {
            match key {
                SchemaKey::Id(id) => self.get_schema_by_id(id).await,
                SchemaKey::Guid(guid) => self.get_schema_by_guid(guid).await,
            }
        }
    }

    /// Look up an already-registered schema **without registering it**.
    ///
    /// Returns `Ok(None)` when the subject exists but has no version with this
    /// content, and when the subject does not exist at all. Errors are reserved
    /// for transport, auth, and malformed-schema failures.
    ///
    /// This is the read-only counterpart to
    /// [`register_schema`](Self::register_schema), and the right call for any
    /// client that holds read-only credentials: it needs only `Subject:Read`,
    /// whereas `register_schema` needs `Subject:Write` and will happily create
    /// a version in production if the local schema has drifted.
    ///
    /// Default: `Err(NotSupported)`.
    fn lookup_schema<'a>(
        &'a self,
        _subject: &'a str,
        _schema: &'a str,
        _schema_type: SchemaType,
        _references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<Option<Arc<Schema>>>> + Send + 'a {
        async {
            Err(SchemaRegError::not_supported(
                "lookup_schema is not supported by this registry",
            ))
        }
    }

    /// Check whether `schema` is compatible with the latest version registered
    /// under `subject`, per the subject's configured compatibility level.
    ///
    /// Default: `Err(NotSupported)`.
    fn check_compatibility<'a>(
        &'a self,
        _subject: &'a str,
        _schema: &'a str,
        _schema_type: SchemaType,
        _references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        async {
            Err(SchemaRegError::not_supported(
                "check_compatibility is not supported by this registry",
            ))
        }
    }

    /// Delete a subject and all its versions, returning the deleted version
    /// numbers.
    ///
    /// Confluent-compatible registries model deletion in two stages: a *soft*
    /// delete hides the subject but keeps its IDs resolvable, and a *permanent*
    /// delete removes it for good. `permanent = true` requires the subject to
    /// have been soft-deleted first — see
    /// [`ConfluentSchemaRegistry::delete_subject`](crate::ConfluentSchemaRegistry::delete_subject).
    ///
    /// Default: `Err(NotSupported)`.
    fn delete_subject<'a>(
        &'a self,
        _subject: &'a str,
        _permanent: bool,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        async {
            Err(SchemaRegError::not_supported(
                "delete_subject is not supported by this registry",
            ))
        }
    }

    /// Delete a single version under a subject, returning the deleted version.
    ///
    /// Two-stage in the same way as [`delete_subject`](Self::delete_subject).
    /// Default: `Err(NotSupported)`.
    fn delete_version<'a>(
        &'a self,
        _subject: &'a str,
        _version: SchemaVersion,
        _permanent: bool,
    ) -> impl Future<Output = Result<SchemaVersion>> + Send + 'a {
        async {
            Err(SchemaRegError::not_supported(
                "delete_version is not supported by this registry",
            ))
        }
    }

    /// List all subjects currently registered in the registry.
    /// Default: `Err(NotSupported)`.
    fn get_subjects(&self) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        async {
            Err(SchemaRegError::not_supported(
                "get_subjects is not supported by this registry",
            ))
        }
    }

    /// List all version numbers registered under `subject`.
    /// Default: `Err(NotSupported)`.
    fn get_versions<'a>(
        &'a self,
        _subject: &'a str,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        async {
            Err(SchemaRegError::not_supported(
                "get_versions is not supported by this registry",
            ))
        }
    }

    /// Get the effective compatibility level for a subject.
    /// Default: `Err(NotSupported)`.
    fn get_compatibility<'a>(
        &'a self,
        _subject: &'a str,
    ) -> impl Future<Output = Result<CompatibilityLevel>> + Send + 'a {
        async {
            Err(SchemaRegError::not_supported(
                "get_compatibility is not supported by this registry",
            ))
        }
    }

    /// Set the compatibility level for a subject.
    /// Default: `Err(NotSupported)`.
    fn set_compatibility<'a>(
        &'a self,
        _subject: &'a str,
        _level: CompatibilityLevel,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        async {
            Err(SchemaRegError::not_supported(
                "set_compatibility is not supported by this registry",
            ))
        }
    }

    /// Probe the registry for connectivity.
    ///
    /// Returns `Ok(())` when the registry is reachable and the configured
    /// credentials are accepted. Designed for readiness probes and startup
    /// preflight checks. Default: `Err(NotSupported)`.
    fn health_check(&self) -> impl Future<Output = Result<()>> + Send + '_ {
        async {
            Err(SchemaRegError::not_supported(
                "health_check is not supported by this registry",
            ))
        }
    }
}

// ── Generated: forwarding impls for `&T` and `Arc<T>` ─────────────────────
//
// Without these, `CachedSchemaRegistry<&T>` and `Arc<ConfluentSchemaRegistry>`
// would not satisfy the trait. Every method must be forwarded explicitly:
// inheriting the defaults would route past the concrete implementation and
// answer `NotSupported` for operations the backend actually supports.

macro_rules! emit_deref_forwarding {
    ($( $name:ident ( $($arg:ident : $ty:ty),* $(,)? ) -> $ret:ty; )*) => {
        impl<T: SchemaRegistryClient + ?Sized> SchemaRegistryClient for &T {
            $(
                fn $name<'a>(&'a self $(, $arg: $ty)*)
                    -> impl Future<Output = Result<$ret>> + Send + 'a
                {
                    T::$name(self $(, $arg)*)
                }
            )*
        }

        impl<T: SchemaRegistryClient + ?Sized> SchemaRegistryClient for Arc<T> {
            $(
                fn $name<'a>(&'a self $(, $arg: $ty)*)
                    -> impl Future<Output = Result<$ret>> + Send + 'a
                {
                    T::$name(self $(, $arg)*)
                }
            )*
        }
    };
}

with_registry_methods!(emit_deref_forwarding);

// ── DynSchemaRegistryClient ───────────────────────────────────────────────

macro_rules! emit_dyn_trait {
    ($( $name:ident ( $($arg:ident : $ty:ty),* $(,)? ) -> $ret:ty; )*) => {
        /// Object-safe mirror of [`SchemaRegistryClient`].
        ///
        /// `SchemaRegistryClient` returns `impl Future`, so it cannot be used as
        /// `dyn SchemaRegistryClient`. This trait declares the same operations
        /// with `Pin<Box<dyn Future>>` returns, which can:
        ///
        /// ```rust
        /// use std::sync::Arc;
        /// use schemreg::DynSchemaRegistryClient;
        ///
        /// struct AppState {
        ///     registry: Arc<dyn DynSchemaRegistryClient>,
        /// }
        /// ```
        ///
        /// A blanket impl covers every [`SchemaRegistryClient`], so any concrete
        /// client coerces straight to `Arc<dyn DynSchemaRegistryClient>`.
        ///
        /// Erasure is a **two-way door**: `dyn DynSchemaRegistryClient` also
        /// implements [`SchemaRegistryClient`], so an erased client goes back
        /// into generic code — including
        /// [`CachedSchemaRegistry`](crate::CachedSchemaRegistry):
        ///
        /// ```rust
        /// use std::sync::Arc;
        /// use schemreg::{CachedSchemaRegistry, DynSchemaRegistryClient};
        /// # use schemreg::{Result, Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion};
        /// # struct MyRegistry;
        /// # impl SchemaRegistryClient for MyRegistry {
        /// #     async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> { unimplemented!() }
        /// #     async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> { unimplemented!() }
        /// #     async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> { unimplemented!() }
        /// #     async fn register_schema(&self, _: &str, _: &str, _: SchemaType, _: &[SchemaReference]) -> Result<SchemaId> { unimplemented!() }
        /// # }
        /// let erased: Arc<dyn DynSchemaRegistryClient> = Arc::new(MyRegistry);
        /// let cached = CachedSchemaRegistry::new(erased);
        /// ```
        ///
        /// # Method-resolution ambiguity
        ///
        /// Both traits expose identically named methods, and most concrete types
        /// implement both. With **both imported into one scope**,
        /// `client.get_schema_by_id(id)` is ambiguous and the compiler says so.
        /// Import only the one you need, or disambiguate:
        ///
        /// ```rust,ignore
        /// SchemaRegistryClient::get_schema_by_id(&client, id).await
        /// DynSchemaRegistryClient::get_schema_by_id(&client, id).await
        /// ```
        pub trait DynSchemaRegistryClient: Send + Sync {
            $(
                #[doc = concat!(
                    "Object-safe form of [`SchemaRegistryClient::",
                    stringify!($name), "`]."
                )]
                fn $name<'a>(&'a self $(, $arg: $ty)*)
                    -> Pin<Box<dyn Future<Output = Result<$ret>> + Send + 'a>>;
            )*
        }

        /// Blanket: every [`SchemaRegistryClient`] is a
        /// [`DynSchemaRegistryClient`] via `Box::pin`.
        impl<T: SchemaRegistryClient> DynSchemaRegistryClient for T {
            $(
                fn $name<'a>(&'a self $(, $arg: $ty)*)
                    -> Pin<Box<dyn Future<Output = Result<$ret>> + Send + 'a>>
                {
                    Box::pin(SchemaRegistryClient::$name(self $(, $arg)*))
                }
            )*
        }

        // Closes the loop, so type erasure is not a one-way door.
        //
        // No coherence conflict with the blanket impl above: that `T` is
        // implicitly `Sized`, which excludes `dyn DynSchemaRegistryClient`.
        impl SchemaRegistryClient for dyn DynSchemaRegistryClient + '_ {
            $(
                fn $name<'a>(&'a self $(, $arg: $ty)*)
                    -> impl Future<Output = Result<$ret>> + Send + 'a
                {
                    DynSchemaRegistryClient::$name(self $(, $arg)*)
                }
            )*
        }
    };
}

with_registry_methods!(emit_dyn_trait);

// Static assertion: `Arc<dyn DynSchemaRegistryClient>` must be Send + Sync.
const _: () = {
    fn _assert_dyn_is_send_sync()
    where
        dyn DynSchemaRegistryClient: Send + Sync,
    {
    }
};

// ── AnySchemaCache ────────────────────────────────────────────────────────

/// Shared cache-management interface implemented by both
/// [`CachedSchemaRegistry`](crate::CachedSchemaRegistry) and
/// [`CachedGlueSchemaRegistry`](crate::glue::CachedGlueSchemaRegistry).
pub trait AnySchemaCache: Send + Sync {
    /// Identifier type used by this cache (schema ID or schema version ID).
    type Id: Copy + Send + Sync;
    /// Number of entries currently held in the cache.
    fn cache_len(&self) -> usize;
    /// Returns `true` when the cache contains no entries.
    fn cache_is_empty(&self) -> bool;
    /// Clear all cached entries and cancel in-flight cache repopulation.
    fn clear_cache(&self);
    /// Invalidate a specific cache entry.
    fn invalidate(&self, id: Self::Id);
    /// Invalidate all cache entries.
    fn invalidate_all(&self);
    /// Pre-warm the cache for a set of immutable IDs.
    fn warm_cache<'a>(
        &'a self,
        ids: &'a [Self::Id],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

// ── PayloadEncoder / PayloadDecoder ───────────────────────────────────────

/// Adds schema framing to an **already-serialized** payload.
///
/// This is the framing layer, not the serialization layer: the payload arrives
/// as `Bytes` that the caller has already encoded as Avro, Protobuf, or JSON,
/// and the implementation resolves the subject, obtains a schema identifier,
/// and prepends the wire prefix.
///
/// For serialization *and* framing in one step, use the format-specific codecs
/// ([`AvroSchemaEncoder`](crate::avro::AvroSchemaEncoder),
/// [`JsonSchemaEncoder`](crate::json::JsonSchemaEncoder),
/// [`ProtobufSchemaEncoder`](crate::protobuf::ProtobufSchemaEncoder)) — they
/// take a typed value rather than bytes and so cannot implement this trait.
///
/// Object-safe: share as `Arc<dyn PayloadEncoder>`.
pub trait PayloadEncoder: Send + Sync {
    /// Frame `payload`, returning wire-formatted bytes.
    ///
    /// `topic` is the target topic. `record_name` is required for the
    /// `RecordName` and `TopicRecordName` subject strategies and ignored by
    /// `TopicName`. `target` selects the key or value subject.
    fn encode(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;

    /// Frame `payload` with the identifier in a Kafka record header instead of
    /// in the payload prefix — the placement Confluent Platform 8 introduced.
    ///
    /// The returned [`HeaderFramed`] carries the header name, the header value,
    /// and an **unprefixed** payload. Write all three: a consumer that never
    /// sees the header has nothing to look the schema up by.
    ///
    /// Defaults to [`SchemaRegError::not_supported`] so that an encoder which
    /// only knows prefix framing stays a one-method implementation. That variant
    /// is never [`is_retryable`](SchemaRegError::is_retryable), so a caller can
    /// tell "this encoder cannot do that" from "the registry is down".
    fn encode_with_header(
        &self,
        _payload: Bytes,
        _topic: &str,
        _record_name: Option<&str>,
        _target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<HeaderFramed>> + Send + '_>> {
        Box::pin(async {
            Err(SchemaRegError::not_supported(
                "header placement is not supported by this encoder",
            ))
        })
    }
}

/// Strips schema framing, returning the payload bytes underneath.
///
/// The consumer-side counterpart to [`PayloadEncoder`]: it removes the wire
/// prefix (and, for Protobuf, the message-index array) without deserializing
/// the payload.
///
/// Object-safe: share as `Arc<dyn PayloadDecoder>`.
pub trait PayloadDecoder: Send + Sync {
    /// Unframe a wire-formatted payload, returning the raw inner bytes.
    fn decode(
        &self,
        payload: Bytes,
        topic: &str,
        target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    struct Minimal;

    impl SchemaRegistryClient for Minimal {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            Ok(Arc::new(Schema::new(id, SchemaType::Avro, "\"string\"")))
        }
        async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
            Err(SchemaRegError::not_supported("test"))
        }
        async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
            Err(SchemaRegError::not_supported("test"))
        }
        async fn register_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<SchemaId> {
            Ok(SchemaId::new(1))
        }
    }

    /// Every optional method must answer `NotSupported`, and `NotSupported`
    /// must never look retryable — that is the whole contract a caller's retry
    /// loop leans on.
    #[tokio::test]
    async fn unimplemented_methods_are_not_supported_and_not_retryable() {
        let err = SchemaRegistryClient::lookup_schema(&Minimal, "s", "{}", SchemaType::Avro, &[])
            .await
            .unwrap_err();
        assert!(err.is_not_supported(), "{err}");
        assert!(!err.is_retryable(), "{err}");

        use SchemaRegistryClient as Client;
        for err in [
            Client::health_check(&Minimal).await.unwrap_err(),
            Client::get_subjects(&Minimal).await.unwrap_err(),
            Client::get_versions(&Minimal, "s").await.unwrap_err(),
            Client::delete_subject(&Minimal, "s", false)
                .await
                .unwrap_err(),
            Client::delete_version(&Minimal, "s", SchemaVersion::new(1), false)
                .await
                .unwrap_err(),
            Client::get_schema_by_guid(&Minimal, SchemaGuid::from_bytes([0; 16]))
                .await
                .unwrap_err(),
            Client::get_compatibility(&Minimal, "s").await.unwrap_err(),
            Client::set_compatibility(&Minimal, "s", CompatibilityLevel::Full)
                .await
                .unwrap_err(),
        ] {
            assert!(err.is_not_supported(), "{err}");
        }
    }

    /// The generated forwarding impls must reach the concrete implementation,
    /// not the trait defaults. If a method were missing from the macro list,
    /// `&Minimal` would silently answer `NotSupported` here.
    #[tokio::test]
    async fn reference_and_arc_wrappers_forward_to_the_concrete_impl() {
        async fn fetch<C: SchemaRegistryClient>(c: C) -> Result<Arc<Schema>> {
            c.get_schema_by_id(SchemaId::new(7)).await
        }

        let expected = Some(Some(SchemaId::new(7)));
        assert_eq!(fetch(Minimal).await.ok().map(|s| s.id), expected);
        assert_eq!(fetch(&Minimal).await.ok().map(|s| s.id), expected);
        assert_eq!(fetch(Arc::new(Minimal)).await.ok().map(|s| s.id), expected);
    }

    /// Type erasure must round-trip: erased into `dyn`, then back into a
    /// generic position.
    #[tokio::test]
    async fn erasure_is_a_two_way_door() {
        let erased: Arc<dyn DynSchemaRegistryClient> = Arc::new(Minimal);
        let schema = DynSchemaRegistryClient::get_schema_by_id(&*erased, SchemaId::new(3))
            .await
            .expect("erased lookup");
        assert_eq!(schema.id, Some(SchemaId::new(3)));

        async fn generic<C: SchemaRegistryClient>(c: C) -> Result<Option<SchemaId>> {
            c.get_schema_by_id(SchemaId::new(4)).await.map(|s| s.id)
        }
        assert_eq!(generic(erased).await.ok(), Some(Some(SchemaId::new(4))));
    }
}
