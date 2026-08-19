//! Generic bounded, coalescing in-memory cache.
//!
//! Every cache in the crate is this one type: the registry schema caches, the
//! Glue version cache, the codecs' parsed-schema and compiled-validator caches,
//! and the producers' `subject → schema ID` maps. Sharing it means the
//! cancellation and invalidation-race guarantees are established once instead
//! of re-derived (and mis-derived) per call site.
//!
//! # Access patterns
//!
//! 1. [`get_or_fetch`](InMemoryCache::get_or_fetch) coalesces concurrent cold
//!    misses so that N callers racing for one key issue exactly one backend
//!    request. The first caller is the *leader*; the rest wait on a
//!    `tokio::sync::oneshot` and are woken with the shared result. If the
//!    leader's future is dropped — its task was aborted, or the caller timed
//!    out — a drop guard wakes every waiter with an error rather than leaving
//!    them parked forever.
//!
//! 2. [`insert_if_current`](InMemoryCache::insert_if_current) stores a value
//!    fetched outside the coalescer. `get_latest_schema` uses it: the call
//!    always hits the backend (a newer version may exist at any moment) but the
//!    schema it returns is worth caching under its immutable ID.
//!
//! # Invalidation races
//!
//! An `invalidate()` that lands *while* a fetch for that key is in flight must
//! not be undone when the fetch completes — otherwise the caller who explicitly
//! dropped a stale entry silently gets it back.
//!
//! The two paths guard against that differently, and deliberately:
//!
//! - `get_or_fetch` compares the **in-flight token**. `invalidate` removes the
//!   key's in-flight entry, so a leader whose token is no longer registered
//!   knows it was invalidated and skips the insert. This is exact and *per
//!   key*: invalidating one key never affects a concurrent fetch of another.
//!
//! - `insert_if_current` has no in-flight entry to compare against — nothing
//!   registered the key before the fetch began — so it falls back to a global
//!   invalidation counter sampled before the fetch and re-checked **inside**
//!   the write lock. That is conservative: an unrelated `invalidate()` can make
//!   it skip an insert it could have kept. Skipping costs one future cache
//!   miss on a path that already always hits the backend, which is the right
//!   side to err on.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tracing::debug;

use crate::error::{Result, SchemaRegError};

/// A fetch in progress, and everyone waiting on it.
struct InFlight<V> {
    /// Identifies *this* leader. A leader whose token is no longer the
    /// registered one has been superseded by an invalidation.
    token: u64,
    waiters: Vec<oneshot::Sender<Result<Arc<V>>>>,
}

/// The stored entries plus their insertion order, under one lock.
///
/// Keeping these together is what makes eviction correct: the queue and the map
/// can never disagree about which keys exist, because nothing can observe them
/// between two separate lock acquisitions.
struct Store<K, V> {
    entries: HashMap<K, Arc<V>>,
    /// Insertion order, oldest first. Only populated when bounded.
    order: VecDeque<K>,
}

/// Generic bounded, coalescing in-memory cache storing `Arc<V>` keyed by `K`.
///
/// # Eviction
///
/// FIFO — the oldest *inserted* entry is dropped on overflow, not the least
/// recently used. Tracking recency would mean taking a write lock on every
/// read, which is the wrong trade for a cache whose working set (the schemas a
/// service actually uses) is normally far below the bound and where a miss
/// costs one idempotent round-trip.
///
/// # Type parameters
///
/// - `K`: cache key. `Clone` rather than `Copy` so `String`-keyed caches
///   (subject → schema ID, on the producer side) can share this implementation.
/// - `V`: cached value, stored as `Arc<V>` so a hit is a refcount bump.
pub(crate) struct InMemoryCache<K, V> {
    store: RwLock<Store<K, V>>,
    /// `None` means unbounded; every construction site in this crate bounds it.
    max_entries: Option<usize>,
    next_token: AtomicU64,
    /// Bumped by every `invalidate` / `clear`. Only `insert_if_current` reads
    /// it — see the module docs on why `get_or_fetch` does not.
    invalidation_generation: AtomicU64,
    in_flight: Mutex<HashMap<K, InFlight<V>>>,
    /// Builds the error handed to waiters when a leader is cancelled.
    make_cancelled_error: fn(&K) -> SchemaRegError,
}

impl<K, V> InMemoryCache<K, V>
where
    K: Hash + Eq + Clone + fmt::Debug + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub(crate) fn new(
        max_entries: Option<usize>,
        make_cancelled_error: fn(&K) -> SchemaRegError,
    ) -> Self {
        let capacity = max_entries.unwrap_or(0);
        Self {
            store: RwLock::new(Store {
                entries: HashMap::with_capacity(capacity),
                order: VecDeque::with_capacity(capacity),
            }),
            max_entries,
            next_token: AtomicU64::new(0),
            invalidation_generation: AtomicU64::new(0),
            in_flight: Mutex::new(HashMap::new()),
            make_cancelled_error,
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────

    pub(crate) fn len(&self) -> usize {
        self.store.read().entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.store.read().entries.is_empty()
    }

    /// Current invalidation generation, for pairing with `insert_if_current`.
    pub(crate) fn generation(&self) -> u64 {
        self.invalidation_generation.load(Ordering::SeqCst)
    }

    /// Look up `key` without fetching. Used by the encoders' `cached_schema_id`
    /// observability accessors, which must never trigger a registration.
    #[cfg_attr(
        not(any(
            feature = "confluent",
            feature = "avro",
            feature = "json",
            feature = "protobuf"
        )),
        allow(dead_code)
    )]
    pub(crate) fn get(&self, key: &K) -> Option<Arc<V>> {
        self.store.read().entries.get(key).map(Arc::clone)
    }

    // ── Invalidation ──────────────────────────────────────────────────────

    /// Drop `key` and cancel any in-flight fetch for it.
    pub(crate) fn invalidate(&self, key: &K) {
        self.invalidation_generation.fetch_add(1, Ordering::SeqCst);

        let waiters = self
            .in_flight
            .lock()
            .remove(key)
            .map(|e| e.waiters)
            .unwrap_or_default();

        {
            let mut store = self.store.write();
            if store.entries.remove(key).is_some() {
                store.order.retain(|cached| cached != key);
            }
        }

        self.notify_cancelled(key, waiters);
    }

    /// Drop every entry whose value satisfies `predicate`, and cancel the
    /// matching in-flight fetches.
    ///
    /// One pass and one generation bump, rather than one `invalidate` call per
    /// match — which would be quadratic in the cache size.
    pub(crate) fn invalidate_matching<P>(&self, predicate: P)
    where
        P: Fn(&V) -> bool,
    {
        self.invalidation_generation.fetch_add(1, Ordering::SeqCst);

        let removed: Vec<K> = {
            let mut store = self.store.write();
            let Store { entries, order } = &mut *store;

            let doomed: Vec<K> = entries
                .iter()
                .filter(|(_, v)| predicate(v.as_ref()))
                .map(|(k, _)| k.clone())
                .collect();
            if doomed.is_empty() {
                return;
            }
            for key in &doomed {
                entries.remove(key);
            }
            order.retain(|k| entries.contains_key(k));
            doomed
        };

        let mut cancelled = Vec::new();
        {
            let mut in_flight = self.in_flight.lock();
            for key in &removed {
                if let Some(entry) = in_flight.remove(key) {
                    cancelled.push((key.clone(), entry.waiters));
                }
            }
        }
        for (key, waiters) in cancelled {
            self.notify_cancelled(&key, waiters);
        }
    }

    /// Drop every entry and cancel every in-flight fetch.
    pub(crate) fn clear(&self) {
        self.invalidation_generation.fetch_add(1, Ordering::SeqCst);

        let cancelled: Vec<(K, InFlight<V>)> = self.in_flight.lock().drain().collect();
        {
            let mut store = self.store.write();
            store.entries.clear();
            store.order.clear();
        }

        for (key, entry) in cancelled {
            self.notify_cancelled(&key, entry.waiters);
        }
    }

    fn notify_cancelled(&self, key: &K, waiters: Vec<oneshot::Sender<Result<Arc<V>>>>) {
        if waiters.is_empty() {
            return;
        }
        let err = (self.make_cancelled_error)(key);
        for waiter in waiters {
            let _ = waiter.send(Err(err.clone()));
        }
    }

    // ── Insertion ─────────────────────────────────────────────────────────

    /// Insert `value` unless the invalidation generation moved since
    /// `observed_generation` was sampled.
    ///
    /// The generation is re-checked **inside** the write lock, which is what
    /// closes the window between "fetch completed" and "value stored".
    pub(crate) fn insert_if_current(&self, key: K, value: Arc<V>, observed_generation: u64) {
        let mut store = self.store.write();

        if self.invalidation_generation.load(Ordering::SeqCst) != observed_generation {
            debug!(
                ?key,
                "fetch completed after an invalidation; skipping cache insert"
            );
            return;
        }
        insert_locked(&mut store, self.max_entries, key, value);
    }

    // ── Coalescing fetch ──────────────────────────────────────────────────

    /// Return the cached value for `key`, or call `fetch` exactly once across
    /// all concurrent callers and cache the result.
    ///
    /// Errors are propagated to every waiter but never cached, so a transient
    /// backend failure does not become sticky.
    pub(crate) async fn get_or_fetch<F, Fut>(&self, key: K, fetch: F) -> Result<Arc<V>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<V>>>,
    {
        // Fast path: a read lock and a refcount bump.
        if let Some(v) = self.store.read().entries.get(&key) {
            return Ok(Arc::clone(v));
        }

        // Slow path: join an in-flight fetch, or become its leader.
        //
        // The lock guard must not survive this block: it is not `Send`, and
        // holding it across the `.await` below would make every future built
        // from this method non-`Send`.
        enum Role<V> {
            Waiter(oneshot::Receiver<Result<Arc<V>>>),
            Leader(u64),
        }

        let role = {
            let mut in_flight = self.in_flight.lock();

            // Re-check: another task may have inserted between the read above
            // and this lock.
            if let Some(v) = self.store.read().entries.get(&key) {
                return Ok(Arc::clone(v));
            }

            match in_flight.get_mut(&key) {
                Some(entry) => {
                    let (tx, rx) = oneshot::channel();
                    entry.waiters.push(tx);
                    Role::Waiter(rx)
                }
                None => {
                    let token = self.next_token.fetch_add(1, Ordering::SeqCst) + 1;
                    in_flight.insert(
                        key.clone(),
                        InFlight {
                            token,
                            waiters: Vec::new(),
                        },
                    );
                    Role::Leader(token)
                }
            }
        };

        let leader_token = match role {
            // The leader, or a cancellation, wakes us.
            Role::Waiter(rx) => {
                return rx.await.map_err(|_| (self.make_cancelled_error)(&key))?;
            }
            Role::Leader(token) => token,
        };

        // Leader path. The guard fires if this future is dropped before the
        // fetch completes, so waiters get an error instead of hanging.
        let mut guard = LeaderGuard {
            cache: self,
            key: key.clone(),
            token: leader_token,
            completed: false,
        };

        let result = fetch().await;

        // Claim the in-flight slot. Failing to claim it means `invalidate` or
        // `clear` removed it while the fetch was running: the result is still
        // returned to this caller, but must not be stored.
        let waiters = self.take_in_flight(&key, leader_token);
        guard.completed = true;

        let outcome = match result {
            Ok(value) => {
                let stored = match waiters {
                    Some(_) => {
                        let mut store = self.store.write();
                        debug!(?key, "cache miss — fetched from backend");
                        // Another writer may have won the race; prefer the
                        // stored value so all callers share one allocation.
                        match store.entries.get(&key) {
                            Some(existing) => Arc::clone(existing),
                            None => {
                                insert_locked(
                                    &mut store,
                                    self.max_entries,
                                    key.clone(),
                                    Arc::clone(&value),
                                );
                                value
                            }
                        }
                    }
                    None => {
                        debug!(
                            ?key,
                            "fetch completed after an invalidation; skipping cache insert"
                        );
                        value
                    }
                };
                Ok(stored)
            }
            Err(e) => Err(e),
        };

        if let Some(waiters) = waiters {
            for waiter in waiters {
                let _ = waiter.send(outcome.as_ref().map(Arc::clone).map_err(Clone::clone));
            }
        }
        outcome
    }

    /// Remove and return this leader's waiters, or `None` if the slot was taken
    /// over (invalidated) while the fetch was running.
    fn take_in_flight(&self, key: &K, token: u64) -> Option<Vec<oneshot::Sender<Result<Arc<V>>>>> {
        let mut in_flight = self.in_flight.lock();
        match in_flight.get(key) {
            Some(entry) if entry.token == token => in_flight.remove(key).map(|entry| entry.waiters),
            _ => None,
        }
    }
}

/// Insert into a locked store, evicting the oldest entry when at capacity.
fn insert_locked<K, V>(store: &mut Store<K, V>, max_entries: Option<usize>, key: K, value: Arc<V>)
where
    K: Hash + Eq + Clone,
{
    if store.entries.insert(key.clone(), value).is_some() {
        // Replacing an existing entry keeps its original queue position, so a
        // refreshed value does not jump ahead of older ones.
        return;
    }
    let Some(max) = max_entries else {
        return;
    };
    store.order.push_back(key);
    // `while`, not `if`: shrinking the bound is not possible today, but a
    // single `if` would silently leave the cache over capacity if it ever were.
    while store.entries.len() > max {
        match store.order.pop_front() {
            Some(evicted) => {
                store.entries.remove(&evicted);
            }
            None => break,
        }
    }
}

/// Wakes waiters if a leader's future is dropped before it completes.
struct LeaderGuard<'a, K, V>
where
    K: Hash + Eq + Clone + fmt::Debug + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    cache: &'a InMemoryCache<K, V>,
    key: K,
    token: u64,
    completed: bool,
}

impl<K, V> Drop for LeaderGuard<'_, K, V>
where
    K: Hash + Eq + Clone + fmt::Debug + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(waiters) = self.cache.take_in_flight(&self.key, self.token) {
            self.cache.notify_cancelled(&self.key, waiters);
        }
    }
}

impl<K, V> fmt::Debug for InMemoryCache<K, V>
where
    K: fmt::Debug + Hash + Eq + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryCache")
            .field("len", &self.store.read().entries.len())
            .field("max_entries", &self.max_entries)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn cancelled(key: &u32) -> SchemaRegError {
        SchemaRegError::invalid_state(format!("cancelled {key}"))
    }

    fn cache(max: usize) -> InMemoryCache<u32, u32> {
        InMemoryCache::new(Some(max), cancelled)
    }

    #[tokio::test]
    async fn fifo_eviction_stays_within_the_bound() {
        let c = cache(2);
        for k in 1..=3u32 {
            c.get_or_fetch(k, || async move { Ok(Arc::new(k)) })
                .await
                .unwrap();
        }
        assert_eq!(c.len(), 2);
        assert!(c.get(&1).is_none(), "the oldest entry must be evicted");
        assert!(c.get(&2).is_some());
        assert!(c.get(&3).is_some());
    }

    /// Re-inserting an existing key must not enqueue it twice — otherwise the
    /// order queue grows without bound and eviction starts popping keys that
    /// are no longer present.
    #[tokio::test]
    async fn refreshing_a_key_does_not_duplicate_its_queue_slot() {
        let c = cache(2);
        for _ in 0..50 {
            c.insert_if_current(1, Arc::new(1), c.generation());
        }
        c.insert_if_current(2, Arc::new(2), c.generation());
        assert_eq!(c.len(), 2);
        c.insert_if_current(3, Arc::new(3), c.generation());
        assert_eq!(c.len(), 2, "the bound must hold after repeated refreshes");
        assert!(c.get(&3).is_some());
    }

    /// Invalidation is per key. A global generation counter here would let a
    /// stream of `invalidate()` calls stop the cache from ever storing anything,
    /// because each one would make an unrelated in-flight fetch decline to
    /// cache its result.
    #[tokio::test]
    async fn invalidating_one_key_does_not_block_caching_another() {
        let c = Arc::new(cache(8));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let fetching = {
            let c = Arc::clone(&c);
            tokio::spawn(async move {
                c.get_or_fetch(1, || async {
                    let _ = release_rx.await;
                    Ok(Arc::new(100))
                })
                .await
            })
        };

        // Let the leader register itself, then churn an unrelated key.
        tokio::task::yield_now().await;
        for k in 2..20u32 {
            c.invalidate(&k);
        }
        let _ = release_tx.send(());

        assert_eq!(*fetching.await.unwrap().unwrap(), 100);
        assert!(
            c.get(&1).is_some(),
            "key 1 must be cached despite unrelated invalidations"
        );
    }

    #[tokio::test]
    async fn invalidating_the_fetched_key_does_discard_the_result() {
        let c = Arc::new(cache(8));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let fetching = {
            let c = Arc::clone(&c);
            tokio::spawn(async move {
                c.get_or_fetch(1, || async {
                    let _ = release_rx.await;
                    Ok(Arc::new(100))
                })
                .await
            })
        };

        tokio::task::yield_now().await;
        c.invalidate(&1);
        let _ = release_tx.send(());

        // The caller still gets its value…
        assert_eq!(*fetching.await.unwrap().unwrap(), 100);
        // …but the invalidation is not undone.
        assert!(c.get(&1).is_none());
    }

    #[tokio::test]
    async fn errors_are_propagated_but_never_cached() {
        let c = cache(4);
        let err = c
            .get_or_fetch(1, || async { Err(SchemaRegError::wire_format("boom")) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boom"));
        assert_eq!(c.len(), 0);
        assert!(c.get(&1).is_none());
    }

    #[tokio::test]
    async fn invalidate_matching_removes_only_matching_entries() {
        let c = cache(16);
        for k in 1..=6u32 {
            c.insert_if_current(k, Arc::new(k * 10), c.generation());
        }
        c.invalidate_matching(|v| *v % 20 == 0);
        assert_eq!(c.len(), 3);
        for k in [2u32, 4, 6] {
            assert!(c.get(&k).is_none(), "{k} should have been removed");
        }
        for k in [1u32, 3, 5] {
            assert!(c.get(&k).is_some(), "{k} should have survived");
        }
    }

    /// After `invalidate_matching`, the order queue must still line up with the
    /// entry map, or eviction later pops phantom keys and the bound drifts.
    #[tokio::test]
    async fn invalidate_matching_keeps_the_order_queue_consistent() {
        let c = cache(4);
        for k in 1..=4u32 {
            c.insert_if_current(k, Arc::new(k), c.generation());
        }
        c.invalidate_matching(|v| *v <= 2);
        assert_eq!(c.len(), 2);

        for k in 5..=6u32 {
            c.insert_if_current(k, Arc::new(k), c.generation());
        }
        assert_eq!(c.len(), 4);
        assert!(c.get(&5).is_some());
        assert!(c.get(&6).is_some());
    }

    #[tokio::test]
    async fn a_cancelled_leader_wakes_its_waiters() {
        let c = Arc::new(cache(4));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();

        let leader = {
            let c = Arc::clone(&c);
            tokio::spawn(async move {
                c.get_or_fetch(1, || async {
                    let _ = started_tx.send(());
                    // Never completes; the task is aborted below.
                    std::future::pending::<()>().await;
                    Ok(Arc::new(1))
                })
                .await
            })
        };
        started_rx.await.unwrap();

        let waiter = {
            let c = Arc::clone(&c);
            tokio::spawn(async move { c.get_or_fetch(1, || async { Ok(Arc::new(2)) }).await })
        };
        tokio::task::yield_now().await;

        leader.abort();
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter must not hang")
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("cancelled"), "{err}");
    }

    #[tokio::test]
    async fn clear_wakes_waiters_and_empties_the_cache() {
        let c = Arc::new(cache(4));
        c.insert_if_current(9, Arc::new(9), c.generation());
        assert_eq!(c.len(), 1);
        c.clear();
        assert!(c.is_empty());
    }
}
