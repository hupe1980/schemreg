//! Async trait interfaces for schema registry backends, caches, and codecs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;
use crate::types::{Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion};

/// Async client interface for a schema registry.
///
/// Implement this trait to integrate with any schema registry backend.
/// When the `confluent` feature is enabled, [`ConfluentSchemaRegistry`](crate::confluent::ConfluentSchemaRegistry)
/// provides a ready-made HTTP implementation for the Confluent Schema
/// Registry (and compatible registries such as Karapace and Apicurio).
///
/// All methods use `async fn` (RPITIT), allowing zero-cost monomorphization at
/// generic call sites. Object-safe erased wrappers are used internally where
/// dynamic dispatch is needed (e.g. [`WireFormatDecoder`](crate::WireFormatDecoder)).
pub trait SchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its globally unique ID.
    ///
    /// Schema IDs are immutable — a given ID always maps to the same schema.
    /// The returned `Arc<Schema>` allows callers to hold a zero-copy reference
    /// into the cache without cloning the schema bytes.
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_;

    /// Retrieve the latest schema registered under the given subject.
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Schema>> + Send + 'a;

    /// Retrieve a specific version of a schema under a subject.
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> impl Future<Output = Result<Schema>> + Send + 'a;

    /// Register a schema under the given subject.
    ///
    /// If the same schema is already registered, the existing ID is returned
    /// (the operation is idempotent). Pass `&[]` for `references` when the
    /// schema has no dependencies.
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<SchemaId>> + Send + 'a;

    /// Check whether a schema is compatible with the latest version registered
    /// under `subject`.
    ///
    /// Returns `true` when the schema is compatible according to the subject's
    /// configured compatibility level, `false` otherwise.
    ///
    /// Implementations that do not support this operation return
    /// `Err(SchemaRegError::registry("check_compatibility: not implemented"))`.
    fn check_compatibility<'a>(
        &'a self,
        _subject: &'a str,
        _schema: &'a str,
        _schema_type: SchemaType,
        _references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        std::future::ready(Err(crate::error::SchemaRegError::not_supported(
            "check_compatibility is not supported by this registry",
        )))
    }

    /// Delete a subject and all its registered versions.
    ///
    /// Returns the list of deleted version numbers. Set `permanent` to `true`
    /// to perform a hard delete (bypasses the soft-delete stage).
    ///
    /// Implementations that do not support this operation return
    /// `Err(SchemaRegError::registry("delete_subject: not implemented"))`.
    fn delete_subject<'a>(
        &'a self,
        _subject: &'a str,
        _permanent: bool,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        std::future::ready(Err(crate::error::SchemaRegError::not_supported(
            "delete_subject is not supported by this registry",
        )))
    }

    /// List all subjects currently registered in the registry.
    ///
    /// Implementations that do not support this operation return
    /// `Err(SchemaRegError::registry("get_subjects: not implemented"))`.
    fn get_subjects(&self) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        std::future::ready(Err(crate::error::SchemaRegError::not_supported(
            "get_subjects is not supported by this registry",
        )))
    }

    /// List all version numbers registered under `subject`.
    ///
    /// Implementations that do not support this operation return
    /// `Err(SchemaRegError::registry("get_versions: not implemented"))`.
    fn get_versions<'a>(
        &'a self,
        _subject: &'a str,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        std::future::ready(Err(crate::error::SchemaRegError::not_supported(
            "get_versions is not supported by this registry",
        )))
    }
}

// Blanket forward implementation so that `&T` and `Arc<T>` can be used
// wherever a `SchemaRegistryClient` is expected.
impl<T: SchemaRegistryClient + ?Sized> SchemaRegistryClient for &T {
    fn get_schema_by_id(
        &self,
        id: crate::types::SchemaId,
    ) -> impl Future<Output = crate::error::Result<Arc<crate::types::Schema>>> + Send + '_ {
        T::get_schema_by_id(self, id)
    }
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = crate::error::Result<crate::types::Schema>> + Send + 'a {
        T::get_latest_schema(self, subject)
    }
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: crate::types::SchemaVersion,
    ) -> impl Future<Output = crate::error::Result<crate::types::Schema>> + Send + 'a {
        T::get_schema_by_version(self, subject, version)
    }
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: crate::types::SchemaType,
        references: &'a [crate::types::SchemaReference],
    ) -> impl Future<Output = crate::error::Result<crate::types::SchemaId>> + Send + 'a {
        T::register_schema(self, subject, schema, schema_type, references)
    }
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: crate::types::SchemaType,
        references: &'a [crate::types::SchemaReference],
    ) -> impl Future<Output = crate::error::Result<bool>> + Send + 'a {
        T::check_compatibility(self, subject, schema, schema_type, references)
    }
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> impl Future<Output = crate::error::Result<Vec<crate::types::SchemaVersion>>> + Send + 'a
    {
        T::delete_subject(self, subject, permanent)
    }
    fn get_subjects(&self) -> impl Future<Output = crate::error::Result<Vec<String>>> + Send + '_ {
        T::get_subjects(self)
    }
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = crate::error::Result<Vec<crate::types::SchemaVersion>>> + Send + 'a
    {
        T::get_versions(self, subject)
    }
}

impl<T: SchemaRegistryClient + ?Sized> SchemaRegistryClient for std::sync::Arc<T> {
    fn get_schema_by_id(
        &self,
        id: crate::types::SchemaId,
    ) -> impl Future<Output = crate::error::Result<Arc<crate::types::Schema>>> + Send + '_ {
        T::get_schema_by_id(self, id)
    }
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = crate::error::Result<crate::types::Schema>> + Send + 'a {
        T::get_latest_schema(self, subject)
    }
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: crate::types::SchemaVersion,
    ) -> impl Future<Output = crate::error::Result<crate::types::Schema>> + Send + 'a {
        T::get_schema_by_version(self, subject, version)
    }
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: crate::types::SchemaType,
        references: &'a [crate::types::SchemaReference],
    ) -> impl Future<Output = crate::error::Result<crate::types::SchemaId>> + Send + 'a {
        T::register_schema(self, subject, schema, schema_type, references)
    }
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: crate::types::SchemaType,
        references: &'a [crate::types::SchemaReference],
    ) -> impl Future<Output = crate::error::Result<bool>> + Send + 'a {
        T::check_compatibility(self, subject, schema, schema_type, references)
    }
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> impl Future<Output = crate::error::Result<Vec<crate::types::SchemaVersion>>> + Send + 'a
    {
        T::delete_subject(self, subject, permanent)
    }
    fn get_subjects(&self) -> impl Future<Output = crate::error::Result<Vec<String>>> + Send + '_ {
        T::get_subjects(self)
    }
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = crate::error::Result<Vec<crate::types::SchemaVersion>>> + Send + 'a
    {
        T::get_versions(self, subject)
    }
}

/// Shared cache-management interface implemented by schema cache wrappers.
///
/// This trait allows generic orchestration over both
/// [`CachedSchemaRegistry`](crate::CachedSchemaRegistry) and
/// [`glue::CachedGlueSchemaRegistry`](crate::glue::CachedGlueSchemaRegistry)
/// for cache lifecycle operations (invalidate, clear, prewarm), without
/// coupling to a specific registry provider.
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

/// Pluggable schema encoder for producer payloads.
///
/// Implement this trait to encode raw bytes into wire-framed bytes before
/// sending. The default implementation ([`ConfluentSchemaEncoder`](crate::confluent::ConfluentSchemaEncoder))
/// registers schemas with a Confluent-compatible registry and applies the
/// 5-byte Confluent wire format header.
///
/// # Object Safety
///
/// The trait is object-safe. Use `Arc<dyn SchemaEncoder>` to share an encoder
/// across tasks or store it in a struct field.
///
/// # Example
///
/// ```rust,ignore
/// use std::pin::Pin;
/// use std::future::Future;
/// use bytes::Bytes;
/// use schemreg::SchemaEncoder;
/// use schemreg::error::Result;
///
/// struct NoopEncoder;
///
/// impl SchemaEncoder for NoopEncoder {
///     fn encode(
///         &self,
///         payload: Bytes,
///         _topic: &str,
///         _record_name: Option<&str>,
///         _is_key: bool,
///     ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>> {
///         Box::pin(async move { Ok(payload) })
///     }
/// }
/// ```
pub trait SchemaEncoder: Send + Sync {
    /// Encode raw bytes, returning wire-framed bytes.
    ///
    /// `payload` contains the raw (pre-serialized) bytes to frame.
    /// `topic` is the target topic name. `record_name` is the schema record
    /// name (used by [`SubjectNameStrategy::RecordName`](crate::SubjectNameStrategy::RecordName)
    /// and [`SubjectNameStrategy::TopicRecordName`](crate::SubjectNameStrategy::TopicRecordName);
    /// pass `None` for the `TopicName` strategy). `is_key` distinguishes
    /// key vs value subjects.
    fn encode(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        is_key: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;
}

/// Object-safe async trait for consumer-side schema decoding.
///
/// A [`SchemaDecoder`] receives a raw (possibly wire-framed) [`Bytes`] payload,
/// strips any framing, and returns the decoded payload.
///
/// # Example — custom decoder
///
/// ```rust,ignore
/// use std::pin::Pin;
/// use std::future::Future;
/// use bytes::Bytes;
/// use schemreg::SchemaDecoder;
/// use schemreg::error::Result;
///
/// struct StripPrefixDecoder;
///
/// impl SchemaDecoder for StripPrefixDecoder {
///     fn decode(
///         &self,
///         payload: Bytes,
///         _topic: &str,
///         _is_key: bool,
///     ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>> {
///         Box::pin(async move {
///             // Strip a 4-byte proprietary header.
///             Ok(payload.slice(4..))
///         })
///     }
/// }
/// ```
pub trait SchemaDecoder: Send + Sync {
    /// Decode a wire-framed payload, returning the raw inner bytes.
    ///
    /// `payload` is the raw bytes (key or value).
    /// `topic` is the source topic name. `is_key` is `true` for key
    /// payloads and `false` for value payloads.
    fn decode(
        &self,
        payload: Bytes,
        topic: &str,
        is_key: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;
}

// ── DynSchemaRegistryClient ───────────────────────────────────────────────

/// Object-safe variant of [`SchemaRegistryClient`].
///
/// Because [`SchemaRegistryClient`] uses `impl Future` return types (RPITIT)
/// it cannot be used as `dyn SchemaRegistryClient`. This trait provides the
/// same interface with [`Pin<Box<dyn Future>>`] return types, enabling you to
/// hold and pass schema registry clients as trait objects:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use schemreg::DynSchemaRegistryClient;
///
/// fn use_registry(client: Arc<dyn DynSchemaRegistryClient>) {
///     // store in structs, pass across async boundaries, etc.
/// }
/// ```
///
/// A blanket implementation is provided for every type that implements
/// [`SchemaRegistryClient`], so no extra `impl` is needed.
pub trait DynSchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its globally unique ID.
    fn get_schema_by_id<'a>(
        &'a self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>>;

    /// Retrieve the latest schema registered under the given subject.
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + 'a>>;

    /// Retrieve a specific version of a schema under a subject.
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + 'a>>;

    /// Register a schema under the given subject.
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + 'a>>;

    /// Check whether a schema is compatible with the latest registered version.
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;

    /// Delete a subject and all its registered versions.
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>>;

    /// List all subjects currently registered.
    fn get_subjects<'a>(&'a self)
    -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>>;

    /// List all version numbers registered under `subject`.
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>>;
}

/// Blanket implementation: any [`SchemaRegistryClient`] is automatically a
/// [`DynSchemaRegistryClient`].
impl<T: SchemaRegistryClient> DynSchemaRegistryClient for T {
    fn get_schema_by_id<'a>(
        &'a self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::get_schema_by_id(self, id))
    }
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::get_latest_schema(self, subject))
    }
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::get_schema_by_version(
            self, subject, version,
        ))
    }
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::register_schema(
            self,
            subject,
            schema,
            schema_type,
            references,
        ))
    }
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::check_compatibility(
            self,
            subject,
            schema,
            schema_type,
            references,
        ))
    }
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::delete_subject(
            self, subject, permanent,
        ))
    }
    fn get_subjects<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::get_subjects(self))
    }
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>> {
        Box::pin(SchemaRegistryClient::get_versions(self, subject))
    }
}
