//! Async trait interfaces for schema registry backends, caches, and codecs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Result, SchemaRegError};
use crate::types::{
    CompatibilityLevel, EncodeTarget, Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion,
};

// ── SchemaRegistryClient ──────────────────────────────────────────────────

/// Async client interface for a schema registry.
///
/// Implement this trait to integrate with any schema registry backend.
/// When the `confluent` feature is enabled, [`ConfluentSchemaRegistry`](crate::confluent::ConfluentSchemaRegistry)
/// provides a ready-made HTTP implementation for the Confluent Schema
/// Registry (and compatible registries such as Karapace and Apicurio).
///
/// All methods use RPITIT (`-> impl Future + Send`) to allow zero-cost
/// monomorphization at generic call sites; concrete impl blocks may use
/// `async fn` syntax and the compiler verifies `Send`-ness automatically.
///
/// For object-safe trait objects, use [`DynSchemaRegistryClient`] which
/// provides the same interface with `Pin<Box<dyn Future>>` return types. A
/// blanket implementation is automatically provided for every
/// `SchemaRegistryClient`.
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
/// ```
///
/// # Wrapping with a cache
///
/// ```rust
/// use std::sync::Arc;
/// use schemreg::CachedSchemaRegistry;
/// # use schemreg::{Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion};
/// # use schemreg::error::{Result, SchemaRegError};
/// # struct InMemoryRegistry;
/// # impl SchemaRegistryClient for InMemoryRegistry {
/// #     async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> { Err(SchemaRegError::invalid_state("")) }
/// #     async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> { Err(SchemaRegError::invalid_state("")) }
/// #     async fn get_schema_by_version(&self, subject: &str, version: SchemaVersion) -> Result<Arc<Schema>> { Err(SchemaRegError::invalid_state("")) }
/// #     async fn register_schema(&self, _: &str, _: &str, _: SchemaType, _: &[SchemaReference]) -> Result<SchemaId> { Ok(SchemaId::from(1u32)) }
/// # }
///
/// let cached = Arc::new(CachedSchemaRegistry::new(InMemoryRegistry));
/// ```
pub trait SchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its globally unique ID.
    ///
    /// Schema IDs are immutable — a given ID always maps to the same schema.
    /// Returns `Arc<Schema>` for zero-copy sharing across tasks.
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_;

    /// Retrieve the latest schema registered under the given subject.
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

    /// Register a schema under the given subject.
    ///
    /// Idempotent: returns the existing ID if already registered.
    /// Pass `&[]` for `references` when there are no dependencies.
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<SchemaId>> + Send + 'a;

    /// Check whether `schema` is compatible with the latest version registered
    /// under `subject`, per the subject's configured compatibility level.
    ///
    /// Returns `true` when compatible, `false` otherwise.
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

    /// Convenience alias for [`check_compatibility`](Self::check_compatibility)
    /// with an empty references slice.
    ///
    /// Equivalent to `check_compatibility(subject, schema, schema_type, &[])`.
    fn check_compatible<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        self.check_compatibility(subject, schema, schema_type, &[])
    }

    /// Delete a subject and all its registered versions.
    ///
    /// Returns deleted version numbers. Set `permanent = true` to hard-delete,
    /// bypassing the soft-delete stage.
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

    /// Probe the registry for connectivity.
    ///
    /// Returns `Ok(())` when the registry is reachable. Designed for
    /// Kubernetes readiness probes and startup preflight checks.
    /// Default: `Err(NotSupported)`.
    fn health_check(&self) -> impl Future<Output = Result<()>> + Send + '_ {
        async {
            Err(SchemaRegError::not_supported(
                "health_check is not supported by this registry",
            ))
        }
    }

    /// Set the compatibility level for a subject.
    ///
    /// Uses `PUT /config/{subject}` on Confluent-compatible registries.
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

    /// Get the current compatibility level for a subject.
    ///
    /// Uses `GET /config/{subject}` on Confluent-compatible registries.
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
}

// ── Blanket impls: &T and Arc<T> ─────────────────────────────────────────
//
// Enables CachedSchemaRegistry<&T>, generic fn<C: SchemaRegistryClient> etc.
// Every method explicitly delegates to the inner T so that concrete provider
// implementations — not the default stubs — are reached.

impl<T: SchemaRegistryClient + ?Sized> SchemaRegistryClient for &T {
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_ {
        (**self).get_schema_by_id(id)
    }
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a {
        (**self).get_latest_schema(subject)
    }
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a {
        (**self).get_schema_by_version(subject, version)
    }
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<SchemaId>> + Send + 'a {
        (**self).register_schema(subject, schema, schema_type, references)
    }
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        (**self).check_compatibility(subject, schema, schema_type, references)
    }
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        (**self).delete_subject(subject, permanent)
    }
    fn get_subjects(&self) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        (**self).get_subjects()
    }
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        (**self).get_versions(subject)
    }
    fn health_check(&self) -> impl Future<Output = Result<()>> + Send + '_ {
        (**self).health_check()
    }
    fn set_compatibility<'a>(
        &'a self,
        subject: &'a str,
        level: CompatibilityLevel,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).set_compatibility(subject, level)
    }
    fn get_compatibility<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<CompatibilityLevel>> + Send + 'a {
        (**self).get_compatibility(subject)
    }
}

impl<T: SchemaRegistryClient + ?Sized> SchemaRegistryClient for Arc<T> {
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_ {
        (**self).get_schema_by_id(id)
    }
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a {
        (**self).get_latest_schema(subject)
    }
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a {
        (**self).get_schema_by_version(subject, version)
    }
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<SchemaId>> + Send + 'a {
        (**self).register_schema(subject, schema, schema_type, references)
    }
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        (**self).check_compatibility(subject, schema, schema_type, references)
    }
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        (**self).delete_subject(subject, permanent)
    }
    fn get_subjects(&self) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        (**self).get_subjects()
    }
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        (**self).get_versions(subject)
    }
    fn health_check(&self) -> impl Future<Output = Result<()>> + Send + '_ {
        (**self).health_check()
    }
    fn set_compatibility<'a>(
        &'a self,
        subject: &'a str,
        level: CompatibilityLevel,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).set_compatibility(subject, level)
    }
    fn get_compatibility<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<CompatibilityLevel>> + Send + 'a {
        (**self).get_compatibility(subject)
    }
}

// ── DynSchemaRegistryClient ───────────────────────────────────────────────

/// Object-safe variant of [`SchemaRegistryClient`].
///
/// Because `SchemaRegistryClient` uses RPITIT (`-> impl Future`) it cannot be
/// used as `dyn SchemaRegistryClient`. This trait provides the same interface
/// with `Pin<Box<dyn Future>>` return types for trait-object usage:
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
/// A blanket implementation is provided for every type that implements
/// [`SchemaRegistryClient`], so no extra `impl` is needed — any concrete client
/// coerces straight to `Arc<dyn DynSchemaRegistryClient>`.
///
/// The two traits compose in both directions: `dyn DynSchemaRegistryClient`
/// also implements [`SchemaRegistryClient`], so a type-erased client can be
/// handed back to generic code — including
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
/// implement both. With **both traits imported into the same scope**,
/// `client.get_schema_by_id(id)` is ambiguous and the compiler will say so.
/// Either import only the one you need, or disambiguate explicitly:
///
/// ```rust,ignore
/// SchemaRegistryClient::get_schema_by_id(&client, id).await
/// DynSchemaRegistryClient::get_schema_by_id(&client, id).await
/// ```
pub trait DynSchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its globally unique ID. See
    /// [`SchemaRegistryClient::get_schema_by_id`].
    fn get_schema_by_id<'a>(
        &'a self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>>;
    /// Retrieve the latest schema under a subject. See
    /// [`SchemaRegistryClient::get_latest_schema`].
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>>;
    /// Retrieve a specific version under a subject. See
    /// [`SchemaRegistryClient::get_schema_by_version`].
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>>;
    /// Register a schema under a subject. See
    /// [`SchemaRegistryClient::register_schema`].
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + 'a>>;
    /// Test schema compatibility. See
    /// [`SchemaRegistryClient::check_compatibility`].
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;
    /// Test compatibility with no references. See
    /// [`SchemaRegistryClient::check_compatible`].
    fn check_compatible<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;
    /// Delete a subject and all its versions. See
    /// [`SchemaRegistryClient::delete_subject`].
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>>;
    /// List all subjects. See [`SchemaRegistryClient::get_subjects`].
    fn get_subjects<'a>(&'a self)
    -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>>;
    /// List all versions under a subject. See
    /// [`SchemaRegistryClient::get_versions`].
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>>;
    /// Probe the registry for connectivity. See
    /// [`SchemaRegistryClient::health_check`].
    fn health_check<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    /// Set the compatibility level for a subject. See
    /// [`SchemaRegistryClient::set_compatibility`].
    fn set_compatibility<'a>(
        &'a self,
        subject: &'a str,
        level: CompatibilityLevel,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    /// Get the compatibility level for a subject. See
    /// [`SchemaRegistryClient::get_compatibility`].
    fn get_compatibility<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<CompatibilityLevel>> + Send + 'a>>;
}

// Static assertion: Arc<dyn DynSchemaRegistryClient> must be Send + Sync.
const _: () = {
    fn _assert_dyn_is_send_sync()
    where
        dyn DynSchemaRegistryClient: Send + Sync,
    {
    }
};

/// Blanket: every [`SchemaRegistryClient`] is automatically a
/// [`DynSchemaRegistryClient`] via `Box::pin` wrapping.
impl<T: SchemaRegistryClient> DynSchemaRegistryClient for T {
    fn get_schema_by_id<'a>(
        &'a self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>> {
        Box::pin(self.get_schema_by_id(id))
    }
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>> {
        Box::pin(self.get_latest_schema(subject))
    }
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send + 'a>> {
        Box::pin(self.get_schema_by_version(subject, version))
    }
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + 'a>> {
        Box::pin(self.register_schema(subject, schema, schema_type, references))
    }
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(self.check_compatibility(subject, schema, schema_type, references))
    }
    fn check_compatible<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(self.check_compatible(subject, schema, schema_type))
    }
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>> {
        Box::pin(self.delete_subject(subject, permanent))
    }
    fn get_subjects<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        Box::pin(self.get_subjects())
    }
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a>> {
        Box::pin(self.get_versions(subject))
    }
    fn health_check<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(self.health_check())
    }
    fn set_compatibility<'a>(
        &'a self,
        subject: &'a str,
        level: CompatibilityLevel,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(self.set_compatibility(subject, level))
    }
    fn get_compatibility<'a>(
        &'a self,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<CompatibilityLevel>> + Send + 'a>> {
        Box::pin(self.get_compatibility(subject))
    }
}

// ── dyn DynSchemaRegistryClient: SchemaRegistryClient ─────────────────────
//
// Closes the loop between the two traits. Without this, type-erasing a client
// into `Arc<dyn DynSchemaRegistryClient>` is a one-way door: the erased value
// can no longer be passed to anything generic over `SchemaRegistryClient`,
// including `CachedSchemaRegistry` and `ConfluentSchemaEncoder`.
//
// No coherence conflict with the blanket `impl<T: SchemaRegistryClient>
// DynSchemaRegistryClient for T`: that `T` is implicitly `Sized`, which
// excludes `dyn DynSchemaRegistryClient`.

impl SchemaRegistryClient for dyn DynSchemaRegistryClient + '_ {
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + '_ {
        DynSchemaRegistryClient::get_schema_by_id(self, id)
    }
    fn get_latest_schema<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a {
        DynSchemaRegistryClient::get_latest_schema(self, subject)
    }
    fn get_schema_by_version<'a>(
        &'a self,
        subject: &'a str,
        version: SchemaVersion,
    ) -> impl Future<Output = Result<Arc<Schema>>> + Send + 'a {
        DynSchemaRegistryClient::get_schema_by_version(self, subject, version)
    }
    fn register_schema<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<SchemaId>> + Send + 'a {
        DynSchemaRegistryClient::register_schema(self, subject, schema, schema_type, references)
    }
    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        DynSchemaRegistryClient::check_compatibility(self, subject, schema, schema_type, references)
    }
    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        DynSchemaRegistryClient::delete_subject(self, subject, permanent)
    }
    fn get_subjects(&self) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        DynSchemaRegistryClient::get_subjects(self)
    }
    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        DynSchemaRegistryClient::get_versions(self, subject)
    }
    fn health_check(&self) -> impl Future<Output = Result<()>> + Send + '_ {
        DynSchemaRegistryClient::health_check(self)
    }
    fn set_compatibility<'a>(
        &'a self,
        subject: &'a str,
        level: CompatibilityLevel,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        DynSchemaRegistryClient::set_compatibility(self, subject, level)
    }
    fn get_compatibility<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<CompatibilityLevel>> + Send + 'a {
        DynSchemaRegistryClient::get_compatibility(self, subject)
    }
}

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

// ── SchemaEncoder ─────────────────────────────────────────────────────────

/// Pluggable schema encoder for producer payloads.
///
/// Encode raw bytes into wire-framed bytes before sending to a broker.
/// Object-safe: use `Arc<dyn SchemaEncoder>` to share across tasks.
pub trait SchemaEncoder: Send + Sync {
    /// Encode `payload`, returning wire-framed bytes.
    ///
    /// `topic` is the target topic. `record_name` is required for `RecordName`
    /// and `TopicRecordName` strategies; `None` for `TopicName`. `target`
    /// distinguishes key from value subjects.
    fn encode(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;
}

// ── SchemaDecoder ─────────────────────────────────────────────────────────

/// Object-safe async trait for consumer-side schema decoding.
///
/// Strips wire-format framing from a [`Bytes`] payload, returning raw bytes.
/// Object-safe: use `Arc<dyn SchemaDecoder>` to share across tasks.
pub trait SchemaDecoder: Send + Sync {
    /// Decode a wire-framed payload, returning the raw inner bytes.
    fn decode(
        &self,
        payload: Bytes,
        topic: &str,
        target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;
}
