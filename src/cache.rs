//! In-memory caching wrapper around any [`SchemaRegistryClient`].
//!
//! Schema IDs are immutable in a Confluent-compatible registry — once assigned,
//! a schema ID always maps to the same schema definition. [`CachedSchemaRegistry`]
//! exploits this by caching `get_schema_by_id` responses indefinitely.
//!
//! `get_latest_schema` and `get_schema_by_version` always forward to the inner
//! client (since newer versions may be registered at any time) but populate the
//! ID-keyed cache so that subsequent `get_schema_by_id` calls are served locally.
//!
//! Concurrent cold misses for the same schema ID are *coalesced*: only one
//! outgoing request is made; all other callers wait for the result. This
//! prevents thundering-herd spikes on cold-start.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::cache_inner::InMemoryCache;
use crate::error::{Result, SchemaRegError};
use crate::traits::{AnySchemaCache, SchemaRegistryClient};
use crate::types::{
    CompatibilityLevel, Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion,
};

fn schema_lookup_cancelled_error(id: &SchemaId) -> SchemaRegError {
    SchemaRegError::invalid_state(format!(
        "schema lookup cancelled before completion for id {id}"
    ))
}

/// Error returned when [`CachedSchemaRegistry::warm_cache`] fails to load one
/// or more schema IDs.
///
/// Successfully fetched IDs **are** cached; only the failures are listed here.
/// Callers can inspect [`failures`](WarmCacheError::failures) to decide
/// whether to retry individual IDs or abort.
#[derive(Debug, Clone)]
pub struct WarmCacheError {
    /// The IDs that could not be fetched, along with the per-ID error.
    pub failures: Vec<(SchemaId, SchemaRegError)>,
}

impl fmt::Display for WarmCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "warm_cache failed for {} schema ID(s):",
            self.failures.len()
        )?;
        for (id, e) in &self.failures {
            write!(f, " id {id}: {e};")?;
        }
        Ok(())
    }
}

impl std::error::Error for WarmCacheError {}

/// Caching wrapper around any [`SchemaRegistryClient`].
///
/// Caches schema-ID-to-schema lookups in memory. Because a schema ID is
/// immutable in the registry (it always maps to the same schema), cached
/// entries never expire.
///
/// Methods whose results may change over time
/// ([`get_latest_schema`](SchemaRegistryClient::get_latest_schema))
/// always hit the inner client but still populate the ID cache so that
/// subsequent [`get_schema_by_id`](SchemaRegistryClient::get_schema_by_id)
/// calls are served from cache.
///
/// # Example
///
/// ```rust,ignore
/// use schemreg::{CachedSchemaRegistry};
/// #[cfg(feature = "confluent")]
/// use schemreg::ConfluentSchemaRegistry;
///
/// let client = ConfluentSchemaRegistry::new("http://localhost:8081").unwrap();
/// let cached = CachedSchemaRegistry::new(client);
///
/// // First call fetches from the registry:
/// let schema = cached.get_schema_by_id(1).await.unwrap();
///
/// // Second call is served from cache:
/// let same = cached.get_schema_by_id(1).await.unwrap();
/// ```
pub struct CachedSchemaRegistry<C> {
    inner: C,
    cache: InMemoryCache<SchemaId, Schema>,
}

/// Default maximum number of cached schema entries, matching the Java client default.
pub const DEFAULT_MAX_CACHE_ENTRIES: usize = 1000;

/// Maximum number of concurrent in-flight fetches issued by `warm_cache`.
///
/// Bounded so that pre-warming several thousand schema IDs on startup cannot
/// exhaust the HTTP connection pool or trip the registry's rate limiter.
pub const WARM_CACHE_CONCURRENCY: usize = 16;

impl<C: SchemaRegistryClient> CachedSchemaRegistry<C> {
    /// Wrap the given client with an in-memory cache.
    ///
    /// Defaults to a maximum of 1,000 entries, evicting the oldest entry on
    /// each insert once the limit is reached.
    pub fn new(inner: C) -> Self {
        Self::with_max_entries(inner, DEFAULT_MAX_CACHE_ENTRIES)
    }

    /// Wrap the given client with a bounded in-memory cache.
    ///
    /// When the cache reaches `max_entries`, the oldest inserted schema ID is evicted.
    pub fn with_max_entries(inner: C, max_entries: usize) -> Self {
        let max_entries = max_entries.max(1);
        Self {
            inner,
            cache: InMemoryCache::new(Some(max_entries), schema_lookup_cancelled_error),
        }
    }

    /// Returns a reference to the inner (uncached) client.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Number of schemas currently in the cache.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` if the cache contains no schemas.
    pub fn cache_is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear the schema cache.
    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    /// Remove a single schema ID from the cache.
    pub fn invalidate(&self, schema_id: impl Into<SchemaId>) {
        self.cache.invalidate(&schema_id.into());
    }

    /// Remove all cached schemas.
    pub fn invalidate_all(&self) {
        self.cache.clear();
    }

    /// Pre-fetch a set of schema IDs into the cache with bounded concurrency.
    ///
    /// Duplicate IDs are deduplicated automatically. At most
    /// [`WARM_CACHE_CONCURRENCY`] fetches are in flight at any moment; a new
    /// fetch starts as soon as any earlier one finishes, so one slow schema does
    /// not stall the rest (there is no per-batch barrier).
    ///
    /// **All IDs are attempted regardless of individual failures.** IDs that
    /// loaded successfully stay in the cache; the full set of failures is
    /// returned as a [`WarmCacheError`] so the caller can decide whether a
    /// partial warm is acceptable.
    ///
    /// # Errors
    ///
    /// Returns [`WarmCacheError`] if one or more IDs could not be fetched.
    pub async fn warm_cache(
        &self,
        schema_ids: impl IntoIterator<Item = impl Into<SchemaId>>,
    ) -> std::result::Result<(), WarmCacheError> {
        use futures::StreamExt as _;

        let unique: HashSet<SchemaId> = schema_ids.into_iter().map(Into::into).collect();
        if unique.is_empty() {
            return Ok(());
        }

        let failures: Vec<(SchemaId, SchemaRegError)> = futures::stream::iter(unique)
            .map(|id| async move {
                let result = self
                    .cache
                    .get_or_fetch(id, || self.inner.get_schema_by_id(id))
                    .await;
                result.err().map(|e| (id, e))
            })
            .buffer_unordered(WARM_CACHE_CONCURRENCY)
            .filter_map(std::future::ready)
            .collect()
            .await;

        if failures.is_empty() {
            Ok(())
        } else {
            Err(WarmCacheError { failures })
        }
    }

    /// Invalidate all cached schemas whose `subject` field matches `subject`.
    ///
    /// Performs an O(n) scan of the cache.
    pub fn invalidate_subject(&self, subject: &str) {
        let ids: Vec<SchemaId> = self
            .cache
            .keys_matching(|s: &Schema| s.subject.as_deref() == Some(subject));
        for id in ids {
            self.cache.invalidate(&id);
        }
    }

    // ── Inherent duplicates for trait-free callers ────────────────────────

    /// Retrieve a schema by its globally unique ID.
    pub async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.cache
            .get_or_fetch(id, || self.inner.get_schema_by_id(id))
            .await
    }

    /// Retrieve the latest schema registered under the given subject.
    pub async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        let generation = self.cache.generation();
        let schema = self.inner.get_latest_schema(subject).await?;
        self.cache
            .insert_if_current(schema.id, Arc::clone(&schema), generation);
        Ok(schema)
    }

    /// Retrieve a specific version of a schema under a subject.
    pub async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        let generation = self.cache.generation();
        let schema = self.inner.get_schema_by_version(subject, version).await?;
        self.cache
            .insert_if_current(schema.id, Arc::clone(&schema), generation);
        Ok(schema)
    }

    /// Register a schema under the given subject.
    pub async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.inner
            .register_schema(subject, schema, schema_type, references)
            .await
    }
}

impl<C> fmt::Debug for CachedSchemaRegistry<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedSchemaRegistry")
            .field("cache_len", &self.cache.len())
            .field("cache", &self.cache)
            .finish()
    }
}

impl<C: SchemaRegistryClient> SchemaRegistryClient for CachedSchemaRegistry<C> {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.get_schema_by_id(id).await
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        self.get_latest_schema(subject).await
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        self.get_schema_by_version(subject, version).await
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.register_schema(subject, schema, schema_type, references)
            .await
    }

    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl Future<Output = Result<bool>> + Send + 'a {
        self.inner
            .check_compatibility(subject, schema, schema_type, references)
    }

    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        self.inner.delete_subject(subject, permanent)
    }

    fn get_subjects(&self) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        self.inner.get_subjects()
    }

    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        self.inner.get_versions(subject)
    }

    fn health_check(&self) -> impl Future<Output = Result<()>> + Send + '_ {
        self.inner.health_check()
    }

    fn set_compatibility<'a>(
        &'a self,
        subject: &'a str,
        level: CompatibilityLevel,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.inner.set_compatibility(subject, level)
    }

    fn get_compatibility<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl Future<Output = Result<CompatibilityLevel>> + Send + 'a {
        self.inner.get_compatibility(subject)
    }
}

impl<C: SchemaRegistryClient> AnySchemaCache for CachedSchemaRegistry<C> {
    type Id = SchemaId;

    fn cache_len(&self) -> usize {
        Self::cache_len(self)
    }

    fn cache_is_empty(&self) -> bool {
        Self::cache_is_empty(self)
    }

    fn clear_cache(&self) {
        Self::clear_cache(self)
    }

    fn invalidate(&self, id: Self::Id) {
        Self::invalidate(self, id)
    }

    fn invalidate_all(&self) {
        Self::invalidate_all(self)
    }

    fn warm_cache<'a>(
        &'a self,
        ids: &'a [Self::Id],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            Self::warm_cache(self, ids.iter().copied())
                .await
                .map_err(|e| SchemaRegError::invalid_state(e.to_string()))
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
    use tokio::sync::{Notify, Semaphore};

    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        match result {
            Ok(v) => v,
            Err(e) => unreachable!("expected Ok(..), got Err({e})"),
        }
    }

    fn join_ok<T>(result: std::result::Result<T, tokio::task::JoinError>) -> T {
        match result {
            Ok(v) => v,
            Err(e) => unreachable!("spawned task failed: {e}"),
        }
    }

    struct MockRegistry {
        get_by_id_calls: AtomicU32,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self {
                get_by_id_calls: AtomicU32::new(0),
            }
        }

        fn get_by_id_call_count(&self) -> u32 {
            self.get_by_id_calls.load(AtomicOrdering::SeqCst)
        }
    }

    impl SchemaRegistryClient for MockRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            self.get_by_id_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Arc::new(Schema::new(
                id,
                crate::types::SchemaType::Avro,
                r#"{"type":"string"}"#,
            )))
        }

        async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
            Ok(Arc::new(
                Schema::new(
                    SchemaId::from(100u32),
                    crate::types::SchemaType::Avro,
                    r#"{"type":"string"}"#,
                )
                .with_subject(subject, 1i32),
            ))
        }

        async fn get_schema_by_version(
            &self,
            subject: &str,
            version: SchemaVersion,
        ) -> Result<Arc<Schema>> {
            Ok(Arc::new(
                Schema::new(
                    SchemaId::from(100u32),
                    crate::types::SchemaType::Avro,
                    r#"{"type":"string"}"#,
                )
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
            Ok(SchemaId::from(42u32))
        }
    }

    struct BlockingMockRegistry {
        get_by_id_calls: AtomicU32,
        get_latest_calls: AtomicU32,
        get_by_version_calls: AtomicU32,
        started: Notify,
        release: Semaphore,
        waiting_calls: AtomicU32,
    }

    impl BlockingMockRegistry {
        fn new() -> Self {
            Self {
                get_by_id_calls: AtomicU32::new(0),
                get_latest_calls: AtomicU32::new(0),
                get_by_version_calls: AtomicU32::new(0),
                started: Notify::new(),
                release: Semaphore::new(0),
                waiting_calls: AtomicU32::new(0),
            }
        }

        fn get_by_id_call_count(&self) -> u32 {
            self.get_by_id_calls.load(AtomicOrdering::SeqCst)
        }

        fn get_latest_call_count(&self) -> u32 {
            self.get_latest_calls.load(AtomicOrdering::SeqCst)
        }

        fn get_by_version_call_count(&self) -> u32 {
            self.get_by_version_calls.load(AtomicOrdering::SeqCst)
        }

        async fn wait_started(&self) {
            self.started.notified().await;
        }

        fn release(&self) {
            let waiting = self.waiting_calls.swap(0, AtomicOrdering::SeqCst);
            self.release.add_permits(waiting as usize);
        }
    }

    impl SchemaRegistryClient for BlockingMockRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            self.get_by_id_calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.started.notify_waiters();
            self.waiting_calls.fetch_add(1, AtomicOrdering::SeqCst);
            let _ = self
                .release
                .acquire()
                .await
                .expect("blocking registry release permit");
            Ok(Arc::new(Schema::new(
                id,
                crate::types::SchemaType::Avro,
                r#"{"type":"string"}"#,
            )))
        }

        async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
            self.get_latest_calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.started.notify_waiters();
            self.waiting_calls.fetch_add(1, AtomicOrdering::SeqCst);
            let _ = self
                .release
                .acquire()
                .await
                .expect("blocking registry release permit");
            Ok(Arc::new(
                Schema::new(
                    SchemaId::from(100u32),
                    crate::types::SchemaType::Avro,
                    r#"{"type":"string"}"#,
                )
                .with_subject(subject, 1i32),
            ))
        }

        async fn get_schema_by_version(
            &self,
            subject: &str,
            version: SchemaVersion,
        ) -> Result<Arc<Schema>> {
            self.get_by_version_calls
                .fetch_add(1, AtomicOrdering::SeqCst);
            self.started.notify_waiters();
            self.waiting_calls.fetch_add(1, AtomicOrdering::SeqCst);
            let _ = self
                .release
                .acquire()
                .await
                .expect("blocking registry release permit");
            Ok(Arc::new(
                Schema::new(
                    SchemaId::from(100u32),
                    crate::types::SchemaType::Avro,
                    r#"{"type":"string"}"#,
                )
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
            Ok(SchemaId::from(42u32))
        }
    }

    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        let s1 = ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);
        assert_eq!(cached.cache_len(), 1);

        let s2 = ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);
        assert_eq!(s1, s2);
    }

    #[tokio::test]
    async fn test_cache_different_ids() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        ok(cached.get_schema_by_id(SchemaId::from(2u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
        assert_eq!(cached.cache_len(), 2);

        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        ok(cached.get_schema_by_id(SchemaId::from(2u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        assert_eq!(cached.cache_len(), 1);

        cached.clear_cache();
        assert_eq!(cached.cache_len(), 0);

        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
    }

    #[tokio::test]
    async fn test_cache_invalidate_single_entry() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        ok(cached.get_schema_by_id(SchemaId::from(2u32)).await);

        cached.invalidate(1u32);
        assert_eq!(cached.cache_len(), 1);

        ok(cached.get_schema_by_id(SchemaId::from(2u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 3);
    }

    #[tokio::test]
    async fn test_cache_warm_cache_deduplicates_ids() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        ok(cached.warm_cache([1u32, 2u32, 1u32, 2u32, 3u32]).await);

        assert_eq!(cached.inner().get_by_id_call_count(), 3);
        assert_eq!(cached.cache_len(), 3);

        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        ok(cached.get_schema_by_id(SchemaId::from(2u32)).await);
        ok(cached.get_schema_by_id(SchemaId::from(3u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 3);
    }

    #[tokio::test]
    async fn test_cache_coalesces_concurrent_misses() {
        let cached = Arc::new(CachedSchemaRegistry::new(BlockingMockRegistry::new()));

        let first = {
            let c = cached.clone();
            tokio::spawn(async move { ok(c.get_schema_by_id(SchemaId::from(7u32)).await) })
        };
        cached.inner().wait_started().await;

        let second = {
            let c = cached.clone();
            tokio::spawn(async move { ok(c.get_schema_by_id(SchemaId::from(7u32)).await) })
        };
        tokio::task::yield_now().await;
        cached.inner().release();

        let s1 = join_ok(first.await);
        let s2 = join_ok(second.await);
        assert_eq!(s1, s2);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);
    }

    #[tokio::test]
    async fn test_cache_coalescer_cleans_up_when_leader_is_cancelled() {
        let cached = Arc::new(CachedSchemaRegistry::new(BlockingMockRegistry::new()));

        let first = {
            let c = cached.clone();
            tokio::spawn(async move { ok(c.get_schema_by_id(SchemaId::from(9u32)).await) })
        };
        cached.inner().wait_started().await;
        first.abort();
        tokio::task::yield_now().await;

        let second = {
            let c = cached.clone();
            tokio::spawn(async move { ok(c.get_schema_by_id(SchemaId::from(9u32)).await) })
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cached.inner().wait_started(),
        )
        .await
        .expect("second lookup did not reach inner registry");
        cached.inner().release();

        let schema = tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .expect("second lookup timed out")
            .expect("second task failed");
        assert_eq!(schema.id, 9u32);
    }

    #[tokio::test]
    async fn test_cache_get_latest_populates_id_cache() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        let schema = ok(cached.get_latest_schema("test-value").await);
        assert_eq!(cached.cache_len(), 1);

        let by_id = ok(cached.get_schema_by_id(schema.id).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 0);
        assert_eq!(by_id.id, schema.id);
    }

    #[tokio::test]
    async fn test_cache_get_by_version_populates_id_cache() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        let schema = ok(cached
            .get_schema_by_version("test-value", SchemaVersion::new(1))
            .await);
        assert_eq!(cached.cache_len(), 1);

        let by_id = ok(cached.get_schema_by_id(schema.id).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 0);
        assert_eq!(by_id.id, schema.id);
    }

    #[tokio::test]
    async fn test_cache_register_forwards() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        let id = ok(cached
            .register_schema("test-value", "{}", crate::types::SchemaType::Avro, &[])
            .await);
        assert_eq!(id, SchemaId::from(42u32));
    }

    #[tokio::test]
    async fn test_cache_with_max_entries_evicts_oldest_entry() {
        let cached = CachedSchemaRegistry::with_max_entries(MockRegistry::new(), 1);
        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        ok(cached.get_schema_by_id(SchemaId::from(2u32)).await);
        assert_eq!(cached.cache_len(), 1);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);

        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 3);
    }

    #[tokio::test]
    async fn test_cache_with_max_entries() {
        let cached = CachedSchemaRegistry::with_max_entries(MockRegistry::new(), 100);
        assert_eq!(cached.cache_len(), 0);
        ok(cached.get_schema_by_id(SchemaId::from(1u32)).await);
        assert_eq!(cached.cache_len(), 1);
    }

    #[tokio::test]
    async fn test_any_schema_cache_trait() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        let generic: &dyn AnySchemaCache<Id = SchemaId> = &cached;
        ok(generic
            .warm_cache(&[
                SchemaId::from(11u32),
                SchemaId::from(12u32),
                SchemaId::from(11u32),
            ])
            .await);
        assert_eq!(generic.cache_len(), 2);
        assert!(!generic.cache_is_empty());
        generic.invalidate(SchemaId::from(11u32));
        assert_eq!(generic.cache_len(), 1);
        generic.invalidate_all();
        assert!(generic.cache_is_empty());
    }

    #[tokio::test]
    async fn test_invalidate_does_not_repopulate_from_inflight_fetch() {
        let cached = Arc::new(CachedSchemaRegistry::new(BlockingMockRegistry::new()));

        let first = {
            let c = cached.clone();
            tokio::spawn(async move { ok(c.get_schema_by_id(SchemaId::from(7u32)).await) })
        };
        cached.inner().wait_started().await;
        cached.invalidate(7u32);
        {
            let c = cached.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                c.inner().release();
            });
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), first)
            .await
            .expect("in-flight fetch timed out")
            .expect("in-flight task failed");
        assert_eq!(cached.cache_len(), 0);

        let second = {
            let c = cached.clone();
            tokio::spawn(async move { ok(c.get_schema_by_id(SchemaId::from(7u32)).await) })
        };
        cached.inner().wait_started().await;
        cached.inner().release();
        let _ = join_ok(second.await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
    }

    #[tokio::test]
    async fn test_invalidate_drops_inflight_get_latest_cache_population() {
        let cached = Arc::new(CachedSchemaRegistry::new(BlockingMockRegistry::new()));

        let latest = {
            let c = cached.clone();
            tokio::spawn(async move { ok(c.get_latest_schema("test-value").await) })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while cached.inner().get_latest_call_count() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("latest lookup did not start");
        cached.invalidate(100u32);
        cached.inner().release();

        let _ = join_ok(latest.await);
        assert_eq!(cached.cache_len(), 0);

        ok(cached.get_schema_by_id(SchemaId::from(100u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);
    }

    #[tokio::test]
    async fn test_invalidate_drops_inflight_get_by_version_cache_population() {
        let cached = Arc::new(CachedSchemaRegistry::new(BlockingMockRegistry::new()));

        let by_version = {
            let c = cached.clone();
            tokio::spawn(async move {
                ok(c.get_schema_by_version("test-value", SchemaVersion::new(1))
                    .await)
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while cached.inner().get_by_version_call_count() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("version lookup did not start");
        cached.invalidate(100u32);
        cached.inner().release();

        let _ = join_ok(by_version.await);
        assert_eq!(cached.cache_len(), 0);

        ok(cached.get_schema_by_id(SchemaId::from(100u32)).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CachedSchemaRegistry<MockRegistry>>();
    }

    #[test]
    fn test_debug() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        let dbg = format!("{cached:?}");
        assert!(dbg.contains("cache_len"));
    }
}
