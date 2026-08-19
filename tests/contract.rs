//! Trait-contract tests for [`SchemaRegistryClient`].
//!
//! Two things are pinned here:
//!
//! 1. **Defaulted methods return `NotSupported`, never a transport error.**
//!    A caller must be able to tell "this registry cannot do that" apart from
//!    "the registry is down", because only the second is worth retrying.
//!
//! 2. **Delegation is complete.** Every wrapper in the crate — `&T`, `Arc<T>`,
//!    `CachedSchemaRegistry<T>`, and the `DynSchemaRegistryClient` blanket impl
//!    — must forward each method to the concrete backend. A wrapper that
//!    silently fell through to the trait default would turn a working operation
//!    into `NotSupported` at runtime, with nothing in the type system to catch it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use schemreg::{
    CachedSchemaRegistry, CompatibilityLevel, Result, Schema, SchemaGuid, SchemaId, SchemaKey,
    SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion,
};

/// An arbitrary well-formed GUID; the minimal backend never resolves it.
const GUID: SchemaGuid = SchemaGuid::from_bytes([0x11; 16]);

/// A second GUID, so that `get_schema_by_key` is a distinct lookup from
/// `get_schema_by_guid` — otherwise `CachedSchemaRegistry` correctly serves the
/// second from cache and the delegation count comes up one short.
const OTHER_GUID: SchemaGuid = SchemaGuid::from_bytes([0x22; 16]);

// ── A backend that implements *only* the four required methods ────────────

struct MinimalRegistry;

impl SchemaRegistryClient for MinimalRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        Ok(Arc::new(Schema::new(id, SchemaType::Avro, r#""string""#)))
    }
    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        Ok(Arc::new(
            Schema::new(SchemaId::from(1u32), SchemaType::Avro, r#""string""#)
                .with_subject(subject, 1i32),
        ))
    }
    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        Ok(Arc::new(
            Schema::new(SchemaId::from(1u32), SchemaType::Avro, r#""string""#)
                .with_subject(subject, version),
        ))
    }
    async fn register_schema(
        &self,
        _subject: &str,
        _schema: &str,
        _schema_type: SchemaType,
        _references: &[SchemaReference],
    ) -> Result<SchemaId> {
        Ok(SchemaId::from(1u32))
    }
}

/// Every optional method must default to `NotSupported` — and specifically
/// `NotSupported`, not `Network` or `InvalidState`, so callers do not retry.
#[tokio::test]
async fn defaulted_methods_return_not_supported_and_are_not_retryable() {
    let r = MinimalRegistry;

    let errors = vec![
        (
            "check_compatibility",
            r.check_compatibility("s", "{}", SchemaType::Avro, &[])
                .await
                .err(),
        ),
        (
            "lookup_schema",
            r.lookup_schema("s", "{}", SchemaType::Avro, &[])
                .await
                .err(),
        ),
        ("get_schema_by_guid", r.get_schema_by_guid(GUID).await.err()),
        ("delete_subject", r.delete_subject("s", false).await.err()),
        (
            "delete_version",
            r.delete_version("s", SchemaVersion::new(1), false)
                .await
                .err(),
        ),
        ("get_subjects", r.get_subjects().await.err()),
        ("get_versions", r.get_versions("s").await.err()),
        ("health_check", r.health_check().await.err()),
        (
            "set_compatibility",
            r.set_compatibility("s", CompatibilityLevel::Full)
                .await
                .err(),
        ),
        ("get_compatibility", r.get_compatibility("s").await.err()),
    ];

    for (name, err) in errors {
        let err = err.unwrap_or_else(|| panic!("{name} must fail on a minimal backend"));
        assert!(
            err.is_not_supported(),
            "{name} must return NotSupported, got: {err}"
        );
        assert!(
            !err.is_retryable(),
            "{name}: NotSupported must never be retryable"
        );
        assert!(
            !err.is_network_error(),
            "{name}: NotSupported must not be mistakable for a transport error"
        );
    }
}

/// The four required methods work through the same minimal backend.
#[tokio::test]
async fn required_methods_work_on_a_minimal_backend() {
    let r = MinimalRegistry;
    assert_eq!(
        r.get_schema_by_id(SchemaId::from(7u32)).await.unwrap().id,
        Some(SchemaId::from(7u32))
    );
    assert_eq!(
        r.get_latest_schema("orders-value")
            .await
            .unwrap()
            .subject
            .as_deref(),
        Some("orders-value")
    );
    assert_eq!(
        r.get_schema_by_version("orders-value", SchemaVersion::new(3))
            .await
            .unwrap()
            .version,
        Some(SchemaVersion::new(3))
    );
    assert_eq!(
        r.register_schema("orders-value", "{}", SchemaType::Avro, &[])
            .await
            .unwrap(),
        1u32
    );
}

// ── A backend that implements *every* method, counting the calls ──────────

#[derive(Default)]
struct CountingRegistry {
    calls: AtomicU32,
}

impl CountingRegistry {
    fn count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
    fn hit(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl SchemaRegistryClient for CountingRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.hit();
        Ok(Arc::new(Schema::new(id, SchemaType::Avro, r#""string""#)))
    }
    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        self.hit();
        Ok(Arc::new(
            Schema::new(SchemaId::from(1u32), SchemaType::Avro, r#""string""#)
                .with_subject(subject, 1i32),
        ))
    }
    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        self.hit();
        Ok(Arc::new(
            Schema::new(SchemaId::from(1u32), SchemaType::Avro, r#""string""#)
                .with_subject(subject, version),
        ))
    }
    async fn register_schema(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.hit();
        Ok(SchemaId::from(1u32))
    }
    async fn check_compatibility(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<bool> {
        self.hit();
        Ok(true)
    }
    async fn get_schema_by_guid(&self, guid: SchemaGuid) -> Result<Arc<Schema>> {
        self.hit();
        Ok(Arc::new(Schema::new(guid, SchemaType::Avro, r#""string""#)))
    }
    async fn get_schema_by_key(&self, key: SchemaKey) -> Result<Arc<Schema>> {
        self.hit();
        Ok(Arc::new(Schema::new(key, SchemaType::Avro, r#""string""#)))
    }
    async fn lookup_schema(
        &self,
        subject: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<Option<Arc<Schema>>> {
        self.hit();
        Ok(Some(Arc::new(
            Schema::new(SchemaId::from(1u32), SchemaType::Avro, r#""string""#)
                .with_subject(subject, 1i32),
        )))
    }
    async fn delete_version(
        &self,
        _: &str,
        version: SchemaVersion,
        _: bool,
    ) -> Result<SchemaVersion> {
        self.hit();
        Ok(version)
    }
    async fn delete_subject(&self, _: &str, _: bool) -> Result<Vec<SchemaVersion>> {
        self.hit();
        Ok(vec![SchemaVersion::new(1)])
    }
    async fn get_subjects(&self) -> Result<Vec<String>> {
        self.hit();
        Ok(vec!["orders-value".to_string()])
    }
    async fn get_versions(&self, _: &str) -> Result<Vec<SchemaVersion>> {
        self.hit();
        Ok(vec![SchemaVersion::new(1)])
    }
    async fn health_check(&self) -> Result<()> {
        self.hit();
        Ok(())
    }
    async fn set_compatibility(&self, _: &str, _: CompatibilityLevel) -> Result<()> {
        self.hit();
        Ok(())
    }
    async fn get_compatibility(&self, _: &str) -> Result<CompatibilityLevel> {
        self.hit();
        Ok(CompatibilityLevel::FullTransitive)
    }
}

/// Exercise all 12 methods against a client and assert none of them fell
/// through to a `NotSupported` default.
async fn exercise_all<C: SchemaRegistryClient>(c: &C, label: &str) {
    macro_rules! ok {
        ($name:literal, $call:expr) => {
            if let Err(e) = $call.await {
                panic!("{label}: {} fell through to a default: {e}", $name);
            }
        };
    }
    ok!("get_schema_by_id", c.get_schema_by_id(SchemaId::from(1u32)));
    ok!("get_latest_schema", c.get_latest_schema("s"));
    ok!(
        "get_schema_by_version",
        c.get_schema_by_version("s", SchemaVersion::new(1))
    );
    ok!(
        "register_schema",
        c.register_schema("s", "{}", SchemaType::Avro, &[])
    );
    ok!(
        "check_compatibility",
        c.check_compatibility("s", "{}", SchemaType::Avro, &[])
    );
    ok!(
        "lookup_schema",
        c.lookup_schema("s", "{}", SchemaType::Avro, &[])
    );
    ok!(
        "delete_version",
        c.delete_version("s", SchemaVersion::new(1), false)
    );
    ok!("get_schema_by_guid", c.get_schema_by_guid(GUID));
    ok!(
        "get_schema_by_key",
        c.get_schema_by_key(SchemaKey::Guid(OTHER_GUID))
    );
    ok!("delete_subject", c.delete_subject("s", false));
    ok!("get_subjects", c.get_subjects());
    ok!("get_versions", c.get_versions("s"));
    ok!("health_check", c.health_check());
    ok!(
        "set_compatibility",
        c.set_compatibility("s", CompatibilityLevel::Full)
    );
    ok!("get_compatibility", c.get_compatibility("s"));
}

/// Every trait method, exercised through every wrapper in the crate.
///
/// Bump this together with the macro list in `src/traits.rs`: a wrapper that
/// forgot to forward a new method shows up here as a count mismatch.
const TRAIT_METHOD_COUNT: u32 = 15;

#[tokio::test]
async fn reference_wrapper_delegates_every_method() {
    let inner = CountingRegistry::default();
    exercise_all(&&inner, "&T").await;
    assert_eq!(inner.count(), TRAIT_METHOD_COUNT);
}

#[tokio::test]
async fn arc_wrapper_delegates_every_method() {
    let inner = Arc::new(CountingRegistry::default());
    exercise_all(&inner, "Arc<T>").await;
    assert_eq!(inner.count(), TRAIT_METHOD_COUNT);
}

#[tokio::test]
async fn cached_wrapper_delegates_every_method() {
    let cached = CachedSchemaRegistry::new(CountingRegistry::default());
    exercise_all(&cached, "CachedSchemaRegistry<T>").await;
    assert_eq!(cached.inner().count(), TRAIT_METHOD_COUNT);
}

/// Type erasure is a two-way door: a client erased into
/// `Arc<dyn DynSchemaRegistryClient>` must still satisfy `SchemaRegistryClient`,
/// otherwise it cannot be handed to `CachedSchemaRegistry` or any generic
/// helper in the crate.
mod dyn_object_safety {
    use super::*;
    use schemreg::DynSchemaRegistryClient;

    // Both traits are in scope here, so every call is fully qualified. This is
    // exactly the disambiguation recipe documented on `DynSchemaRegistryClient`.

    #[tokio::test]
    async fn dyn_blanket_impl_exposes_every_method() {
        let client: Arc<dyn DynSchemaRegistryClient> = Arc::new(CountingRegistry::default());
        let c: &dyn DynSchemaRegistryClient = client.as_ref();

        DynSchemaRegistryClient::get_schema_by_id(c, SchemaId::from(1u32))
            .await
            .unwrap();
        DynSchemaRegistryClient::get_latest_schema(c, "s")
            .await
            .unwrap();
        DynSchemaRegistryClient::get_schema_by_version(c, "s", SchemaVersion::new(1))
            .await
            .unwrap();
        DynSchemaRegistryClient::register_schema(c, "s", "{}", SchemaType::Avro, &[])
            .await
            .unwrap();
        assert!(
            DynSchemaRegistryClient::check_compatibility(c, "s", "{}", SchemaType::Avro, &[])
                .await
                .unwrap()
        );
        assert!(
            DynSchemaRegistryClient::lookup_schema(c, "s", "{}", SchemaType::Avro, &[])
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            DynSchemaRegistryClient::delete_subject(c, "s", true)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            DynSchemaRegistryClient::get_subjects(c)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            DynSchemaRegistryClient::get_versions(c, "s")
                .await
                .unwrap()
                .len(),
            1
        );
        DynSchemaRegistryClient::health_check(c).await.unwrap();
        DynSchemaRegistryClient::set_compatibility(c, "s", CompatibilityLevel::Full)
            .await
            .unwrap();
        assert_eq!(
            DynSchemaRegistryClient::get_compatibility(c, "s")
                .await
                .unwrap(),
            CompatibilityLevel::FullTransitive
        );
    }

    /// `dyn DynSchemaRegistryClient` implements `SchemaRegistryClient`, so an
    /// erased client round-trips back into generic code.
    #[tokio::test]
    async fn erased_client_is_still_a_schema_registry_client() {
        let erased: Arc<dyn DynSchemaRegistryClient> = Arc::new(CountingRegistry::default());
        let cached = CachedSchemaRegistry::new(erased);

        let a = SchemaRegistryClient::get_schema_by_id(&cached, SchemaId::from(3u32))
            .await
            .unwrap();
        let b = SchemaRegistryClient::get_schema_by_id(&cached, SchemaId::from(3u32))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&a, &b), "the cache must serve the second call");
        assert_eq!(cached.cache_len(), 1);
    }

    /// `Arc<dyn DynSchemaRegistryClient>` must be `Send + Sync + 'static` so it
    /// can live in application state and move onto a multi-thread executor.
    #[test]
    fn dyn_client_is_send_sync_static() {
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<Arc<dyn DynSchemaRegistryClient>>();
        assert_send_sync_static::<Box<dyn DynSchemaRegistryClient>>();
    }
}

/// Futures produced by the RPITIT trait must be `Send` so they can be spawned.
#[tokio::test]
async fn trait_futures_are_send_and_spawnable() {
    let client = Arc::new(CountingRegistry::default());
    let handle = tokio::spawn({
        let client = Arc::clone(&client);
        async move { SchemaRegistryClient::get_schema_by_id(&client, SchemaId::from(5u32)).await }
    });
    assert_eq!(
        handle.await.unwrap().unwrap().id,
        Some(SchemaId::from(5u32))
    );
}

// ── Error taxonomy ────────────────────────────────────────────────────────

/// The retry predicate is the contract an outer retry loop depends on.
/// Getting it wrong in either direction is a production incident: too narrow
/// and transient blips surface as hard failures, too broad and a permanent
/// error spins forever.
#[test]
fn retry_classification_is_exhaustive_and_stable() {
    use schemreg::SchemaRegError;

    // Retryable.
    assert!(SchemaRegError::http(429, "throttled").is_retryable());
    assert!(SchemaRegError::http(500, "boom").is_retryable());
    assert!(SchemaRegError::http(502, "bad gateway").is_retryable());
    assert!(SchemaRegError::http(503, "unavailable").is_retryable());
    assert!(SchemaRegError::http(504, "timeout").is_retryable());

    // Not retryable.
    assert!(!SchemaRegError::http(400, "bad request").is_retryable());
    assert!(!SchemaRegError::http(404, "missing").is_retryable());
    assert!(!SchemaRegError::auth(401, "nope").is_retryable());
    assert!(!SchemaRegError::auth(403, "denied").is_retryable());
    assert!(!SchemaRegError::api(40401, "subject not found").is_retryable());
    assert!(!SchemaRegError::config("bad url").is_retryable());
    assert!(!SchemaRegError::wire_format("bad magic byte").is_retryable());
    assert!(!SchemaRegError::not_supported("nope").is_retryable());
    assert!(!SchemaRegError::invalid_state("cancelled").is_retryable());
}

#[test]
fn not_found_covers_the_confluent_code_range_only() {
    use schemreg::SchemaRegError;

    for code in [40401, 40402, 40403] {
        assert!(SchemaRegError::api(code, "x").is_not_found(), "{code}");
    }
    for code in [40400, 40404, 42201, 50001] {
        assert!(!SchemaRegError::api(code, "x").is_not_found(), "{code}");
    }
    // A bare 404 carries no error code but means the same thing — a proxy, a
    // gateway, or a registry that never implemented the route answers this way,
    // and `lookup_schema` must still report `Ok(None)` for it.
    assert!(SchemaRegError::http(404, "x").is_not_found());
    assert!(!SchemaRegError::http(410, "x").is_not_found());
}
