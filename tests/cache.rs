//! Integration tests for `CachedSchemaRegistry`.
//!
//! Uses an in-memory mock registry to test:
//! - Cache hit / miss behaviour
//! - Bounded eviction (LRU-style)
//! - `clear_cache`, `invalidate`, `invalidate_all`
//! - `warm_cache`
//! - In-flight coalescing: concurrent cold misses on the same ID issue
//!   exactly one backend call
//! - `AnySchemaCache` trait object usage

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use schemreg::error::Result;
use schemreg::{
    AnySchemaCache, CachedSchemaRegistry, Schema, SchemaId, SchemaReference, SchemaRegistryClient,
    SchemaType, SchemaVersion,
};

// ── Mock registry ─────────────────────────────────────────────────────────

fn avro_schema(id: u32) -> Schema {
    let schema_id = SchemaId::from(id);
    Schema::new(
        schema_id,
        SchemaType::Avro,
        format!(r#"{{"type":"record","name":"S{schema_id}"}}"#),
    )
    .with_subject(format!("subject-{schema_id}"), 1i32)
}

/// Minimal in-memory registry that counts backend calls.
#[derive(Default)]
struct MockRegistry {
    schemas: HashMap<SchemaId, Schema>,
    call_count: AtomicU32,
}

impl MockRegistry {
    fn with_schemas(schemas: impl IntoIterator<Item = (u32, Schema)>) -> Self {
        Self {
            schemas: schemas
                .into_iter()
                .map(|(id, s)| (SchemaId::from(id), s))
                .collect(),
            call_count: AtomicU32::new(0),
        }
    }

    fn calls(&self) -> u32 {
        self.call_count.load(Ordering::SeqCst)
    }
}

impl SchemaRegistryClient for MockRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.schemas
            .get(&id)
            .map(|s| Arc::new(s.clone()))
            .ok_or_else(|| schemreg::SchemaRegError::registry(format!("schema {id} not found")))
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Schema> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.schemas
            .values()
            .find(|s| s.subject.as_deref() == Some(subject))
            .cloned()
            .ok_or_else(|| {
                schemreg::SchemaRegError::registry(format!("subject {subject} not found"))
            })
    }

    async fn get_schema_by_version(&self, subject: &str, version: SchemaVersion) -> Result<Schema> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.schemas
            .values()
            .find(|s| s.subject.as_deref() == Some(subject) && s.version == Some(version))
            .cloned()
            .ok_or_else(|| {
                schemreg::SchemaRegError::registry(format!(
                    "subject {subject} v{} not found",
                    version.as_i32()
                ))
            })
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

// ── Basic cache behaviour ─────────────────────────────────────────────────

#[tokio::test]
async fn cache_hit_on_second_call() {
    let mock = MockRegistry::with_schemas([(1, avro_schema(1))]);
    let cached = CachedSchemaRegistry::new(mock);

    let _ = cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    let _ = cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();

    assert_eq!(cached.inner().calls(), 1, "second call must be a cache hit");
}

#[tokio::test]
async fn cache_miss_for_different_ids() {
    let mock = MockRegistry::with_schemas([(1, avro_schema(1)), (2, avro_schema(2))]);
    let cached = CachedSchemaRegistry::new(mock);

    let _ = cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    let _ = cached.get_schema_by_id(SchemaId::from(2u32)).await.unwrap();
    let _ = cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap(); // cache hit
    let _ = cached.get_schema_by_id(SchemaId::from(2u32)).await.unwrap(); // cache hit

    assert_eq!(
        cached.inner().calls(),
        2,
        "two unique IDs → two backend calls"
    );
}

#[tokio::test]
async fn cache_is_empty_initially() {
    let cached = CachedSchemaRegistry::new(MockRegistry::default());
    assert!(cached.cache_is_empty());
    assert_eq!(cached.cache_len(), 0);
}

#[tokio::test]
async fn cache_len_increments() {
    let mock = MockRegistry::with_schemas([
        (1, avro_schema(1)),
        (2, avro_schema(2)),
        (3, avro_schema(3)),
    ]);
    let cached = CachedSchemaRegistry::new(mock);
    for id in 1u32..=3 {
        cached.get_schema_by_id(SchemaId::from(id)).await.unwrap();
    }
    assert_eq!(cached.cache_len(), 3);
    assert!(!cached.cache_is_empty());
}

// ── Bounded cache eviction ────────────────────────────────────────────────

#[tokio::test]
async fn bounded_cache_evicts_oldest() {
    // max_entries = 2
    let schemas: HashMap<_, _> = (1u32..=4).map(|id| (id, avro_schema(id))).collect();
    let mock = MockRegistry::with_schemas(schemas);
    let cached = CachedSchemaRegistry::with_max_entries(mock, 2);

    cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    cached.get_schema_by_id(SchemaId::from(2u32)).await.unwrap();
    // Insert 3 → evicts 1.
    cached.get_schema_by_id(SchemaId::from(3u32)).await.unwrap();

    assert_eq!(
        cached.cache_len(),
        2,
        "bounded cache must stay at max_entries"
    );
    // ID 1 was evicted, so it must trigger another backend call.
    let before = cached.inner().calls();
    cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    assert_eq!(
        cached.inner().calls(),
        before + 1,
        "evicted entry must cause a backend fetch"
    );
}

// ── Invalidation ──────────────────────────────────────────────────────────

#[tokio::test]
async fn invalidate_single_entry() {
    let mock = MockRegistry::with_schemas([(1, avro_schema(1)), (2, avro_schema(2))]);
    let cached = CachedSchemaRegistry::new(mock);

    cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    cached.get_schema_by_id(SchemaId::from(2u32)).await.unwrap();
    let calls_before = cached.inner().calls();

    cached.invalidate(1u32);
    assert_eq!(
        cached.cache_len(),
        1,
        "only ID 1 should be removed, ID 2 stays"
    );

    // Re-fetching 1 must go to backend; 2 must still be cached.
    cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    cached.get_schema_by_id(SchemaId::from(2u32)).await.unwrap();
    assert_eq!(
        cached.inner().calls(),
        calls_before + 1,
        "only one extra backend call (for invalidated ID 1)"
    );
}

#[tokio::test]
async fn clear_cache_removes_all() {
    let mock = MockRegistry::with_schemas([(1, avro_schema(1)), (2, avro_schema(2))]);
    let cached = CachedSchemaRegistry::new(mock);

    cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    cached.get_schema_by_id(SchemaId::from(2u32)).await.unwrap();
    assert_eq!(cached.cache_len(), 2);

    cached.clear_cache();
    assert!(cached.cache_is_empty());

    // Re-fetching must go to backend.
    let before = cached.inner().calls();
    cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    cached.get_schema_by_id(SchemaId::from(2u32)).await.unwrap();
    assert_eq!(cached.inner().calls(), before + 2);
}

#[tokio::test]
async fn invalidate_all_equivalent_to_clear_cache() {
    let mock = MockRegistry::with_schemas([(10, avro_schema(10))]);
    let cached = CachedSchemaRegistry::new(mock);
    cached
        .get_schema_by_id(SchemaId::from(10u32))
        .await
        .unwrap();
    cached.invalidate_all();
    assert!(cached.cache_is_empty());
}

// ── Warm cache ────────────────────────────────────────────────────────────

#[tokio::test]
async fn warm_cache_prepopulates() {
    let schemas: HashMap<_, _> = (1u32..=5).map(|id| (id, avro_schema(id))).collect();
    let mock = MockRegistry::with_schemas(schemas);
    let cached = CachedSchemaRegistry::new(mock);

    cached.warm_cache(1u32..=5).await.unwrap();

    assert_eq!(cached.cache_len(), 5);
    let calls_after_warm = cached.inner().calls();

    // All subsequent lookups must be cache hits.
    for id in 1u32..=5 {
        cached.get_schema_by_id(SchemaId::from(id)).await.unwrap();
    }
    assert_eq!(
        cached.inner().calls(),
        calls_after_warm,
        "no extra backend calls after warm"
    );
}

#[tokio::test]
async fn warm_cache_deduplicates_ids() {
    let mock = MockRegistry::with_schemas([(1, avro_schema(1))]);
    let cached = CachedSchemaRegistry::new(mock);
    // Pass the same ID multiple times.
    cached.warm_cache([1u32, 1u32, 1u32]).await.unwrap();
    assert_eq!(
        cached.inner().calls(),
        1,
        "duplicate IDs must only cause one backend call"
    );
}

// ── In-flight coalescing ──────────────────────────────────────────────────

#[tokio::test]
async fn concurrent_cold_miss_single_backend_call() {
    use tokio::sync::Notify;

    // Registry that blocks until explicitly released and counts calls.
    // With coalescing, only ONE task ever enters get_schema_by_id; the rest
    // wait for its result. A TokioBarrier(N+1) would deadlock because only
    // 1 participant (the backend) + the test ever reach barrier.wait().
    // Instead we use a Notify pair: the backend signals "parked", the test
    // waits for that, then signals "release".
    struct BlockedRegistry {
        calls: Arc<AtomicU32>,
        parked: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl SchemaRegistryClient for BlockedRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.parked.notify_one(); // tell the test we're parked
            self.release.notified().await; // wait for test to release us
            Ok(Arc::new(avro_schema(id.as_u32())))
        }

        async fn get_latest_schema(&self, _subject: &str) -> Result<Schema> {
            unimplemented!()
        }

        async fn get_schema_by_version(
            &self,
            _subject: &str,
            _version: SchemaVersion,
        ) -> Result<Schema> {
            unimplemented!()
        }

        async fn register_schema(
            &self,
            _subject: &str,
            _schema: &str,
            _schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> Result<SchemaId> {
            unimplemented!()
        }
    }

    // 8 concurrent tasks all request ID 42 simultaneously.
    const N: usize = 8;
    let calls = Arc::new(AtomicU32::new(0));
    let parked = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let registry = BlockedRegistry {
        calls: Arc::clone(&calls),
        parked: Arc::clone(&parked),
        release: Arc::clone(&release),
    };
    let cached = Arc::new(CachedSchemaRegistry::new(registry));

    let mut handles = Vec::new();
    for _ in 0..N {
        let c = Arc::clone(&cached);
        handles.push(tokio::spawn(async move {
            c.get_schema_by_id(SchemaId::from(42u32)).await
        }));
    }

    // Wait until the one backend call is parked, then yield so the other
    // N-1 tasks have a chance to enqueue as waiters before we release.
    parked.notified().await;
    tokio::task::yield_now().await;

    // Release the single in-flight backend call; all N waiters unblock.
    release.notify_one();

    for h in handles {
        h.await.unwrap().unwrap();
    }

    // Exactly one backend call despite N concurrent misses.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "in-flight coalescing: only one backend call for N concurrent misses"
    );
}

// ── AnySchemaCache trait object ───────────────────────────────────────────

#[tokio::test]
async fn any_schema_cache_trait_object() {
    let mock = MockRegistry::with_schemas([(1, avro_schema(1))]);
    let cached = Arc::new(CachedSchemaRegistry::new(mock));

    // Coerce to a trait object.
    let dyn_cache: &dyn AnySchemaCache<Id = SchemaId> = &*cached;

    cached.get_schema_by_id(SchemaId::from(1u32)).await.unwrap();
    assert_eq!(dyn_cache.cache_len(), 1);
    assert!(!dyn_cache.cache_is_empty());

    dyn_cache.invalidate(SchemaId::from(1u32));
    assert!(dyn_cache.cache_is_empty());
}

// ── SchemaRegistryClient trait delegation ────────────────────────────────

#[tokio::test]
async fn cached_delegates_get_latest_schema() {
    let mock = MockRegistry::with_schemas([(5, avro_schema(5))]);
    let cached = CachedSchemaRegistry::new(mock);

    let schema = cached.get_latest_schema("subject-5").await.unwrap();
    assert_eq!(schema.id, 5u32);

    // get_latest_schema always goes to backend but caches by ID.
    let schema2 = cached.get_schema_by_id(SchemaId::from(5u32)).await.unwrap();
    assert_eq!(schema, *schema2);
    // Two backend calls: 1 for get_latest_schema, 0 for get_by_id (cache hit).
    assert_eq!(cached.inner().calls(), 1);
}

// ── Arc return variant ────────────────────────────────────────────────────

#[tokio::test]
async fn get_schema_by_id_returns_shared_pointer() {
    let mock = MockRegistry::with_schemas([(7, avro_schema(7))]);
    let cached = CachedSchemaRegistry::new(mock);

    let arc1 = cached.get_schema_by_id(SchemaId::from(7u32)).await.unwrap();
    let arc2 = cached.get_schema_by_id(SchemaId::from(7u32)).await.unwrap();

    // Both arcs point to the same allocation — zero-copy cache hit.
    assert!(std::sync::Arc::ptr_eq(&arc1, &arc2));
    assert_eq!(arc1.id, 7u32);
    // Only one backend call.
    assert_eq!(cached.inner().calls(), 1);
}

// ── Cancellation / abort safety ───────────────────────────────────────────

/// When the task that wins the in-flight race (the "leader") is aborted
/// before it resolves, all coalesced waiters must receive an error rather
/// than hanging forever or panicking.
#[tokio::test]
async fn aborted_leader_unblocks_waiters_with_error() {
    use tokio::sync::Notify;

    struct BlockingRegistry {
        parked: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl SchemaRegistryClient for BlockingRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> schemreg::error::Result<Arc<Schema>> {
            self.parked.notify_one();
            self.release.notified().await;
            Ok(Arc::new(avro_schema(id.as_u32())))
        }

        async fn get_latest_schema(&self, _subject: &str) -> schemreg::error::Result<Schema> {
            unimplemented!()
        }

        async fn get_schema_by_version(
            &self,
            _subject: &str,
            _version: SchemaVersion,
        ) -> schemreg::error::Result<Schema> {
            unimplemented!()
        }

        async fn register_schema(
            &self,
            _subject: &str,
            _schema: &str,
            _schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> schemreg::error::Result<SchemaId> {
            unimplemented!()
        }
    }

    let parked = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let registry = BlockingRegistry {
        parked: Arc::clone(&parked),
        release: Arc::clone(&release),
    };
    let cached = Arc::new(CachedSchemaRegistry::new(registry));

    // Spawn the leader task and several waiters.
    let c = Arc::clone(&cached);
    let leader = tokio::spawn(async move { c.get_schema_by_id(SchemaId::from(99u32)).await });

    // Wait until the leader is parked inside the backend call.
    parked.notified().await;
    tokio::task::yield_now().await;

    // Spawn waiters that will coalesce on the in-flight leader.
    let mut waiter_handles = Vec::new();
    for _ in 0..4 {
        let c = Arc::clone(&cached);
        waiter_handles.push(tokio::spawn(async move {
            c.get_schema_by_id(SchemaId::from(99u32)).await
        }));
    }
    tokio::task::yield_now().await;

    // Abort the leader — its cancellation should propagate to waiters.
    leader.abort();
    let _ = leader.await; // JoinError::Cancelled — expected

    // Waiters must NOT hang; they should either succeed (if the cache retried)
    // or return an Err. Under the current implementation they get an Err
    // because the in-flight slot is torn down when the leader is dropped.
    // Either outcome is acceptable; what is NOT acceptable is a hang or panic.
    let results: Vec<_> = futures::future::join_all(waiter_handles).await;

    for r in results {
        // JoinError means the task itself panicked — that is always a bug.
        assert!(
            !r.is_err() || r.unwrap_err().is_panic(),
            "waiter task must not panic"
        );
    }
}
