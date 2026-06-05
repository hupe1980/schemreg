//! Generic in-memory coalescing cache used by both [`CachedSchemaRegistry`](crate::CachedSchemaRegistry)
//! and [`CachedGlueSchemaRegistry`](crate::glue::CachedGlueSchemaRegistry).
//!
//! # Design
//!
//! The cache provides two main access patterns:
//!
//! 1. **[`get_or_fetch`]**: coalesces concurrent cold misses so only one
//!    outgoing request is made for a given key. All other callers wait on a
//!    `tokio::sync::oneshot` channel and receive the result once the leader
//!    completes. Leader cancellation (task abort) is handled via a drop guard
//!    that notifies all waiters with an error instead of leaving them hanging.
//!
//! 2. **[`insert_if_current`]**: inserts a pre-fetched value only if the
//!    invalidation generation has not advanced since the fetch began. Used by
//!    `get_latest_schema` and `get_schema_by_version` which always hit the
//!    backend but cache the resulting schema ID for subsequent lookups.
//!
//! # TOCTOU-free invalidation
//!
//! The generation counter is re-checked **inside** the `cache.write()` lock in
//! both `get_or_fetch` and `insert_if_current`. This closes the window where
//! an `invalidate()` call races with a concurrent fetch completing.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tracing::debug;

use crate::error::{Result, SchemaRegError};

// ── Internal types ────────────────────────────────────────────────────────

struct InFlightEntry<V> {
    token: u64,
    waiters: Vec<oneshot::Sender<Result<Arc<V>>>>,
}

// ── InMemoryCache ─────────────────────────────────────────────────────────

/// Generic coalescing in-memory cache.
///
/// Stores `Arc<V>` values keyed by `K`. Concurrent cold misses for the same
/// key are coalesced: only the first caller ("leader") fetches from the
/// backend; all other callers ("waiters") receive the result via oneshot
/// channels once the leader completes.
///
/// # Type parameters
/// - `K`: cache key. Must be `Hash + Eq + Copy + Send + Sync + 'static`.
/// - `V`: cached value (stored as `Arc<V>`). Must be `Send + Sync + 'static`.
pub(crate) struct InMemoryCache<K, V> {
    entries: RwLock<HashMap<K, Arc<V>>>,
    insertion_order: RwLock<VecDeque<K>>,
    max_entries: Option<usize>,
    in_flight_token: AtomicU64,
    /// Monotonic counter bumped on every `invalidate` / `clear` call.
    /// Re-checked inside `cache.write()` to close the TOCTOU window between
    /// a fetch completing and the subsequent cache insertion.
    invalidation_generation: AtomicU64,
    in_flight: Mutex<HashMap<K, InFlightEntry<V>>>,
    /// Factory for the "lookup cancelled" error reported to waiters when the
    /// leader task is aborted before completing.
    make_cancelled_error: fn(K) -> SchemaRegError,
}

impl<K, V> InMemoryCache<K, V>
where
    K: Hash + Eq + Copy + fmt::Debug + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Create a new cache.
    ///
    /// - `max_entries`: if `Some(n)`, the cache evicts the oldest entry when
    ///   it reaches `n` entries. `None` means unbounded (caller's responsibility).
    /// - `make_cancelled_error`: factory for the error sent to coalesced waiters
    ///   when the leader task is aborted before completion.
    pub(crate) fn new(
        max_entries: Option<usize>,
        make_cancelled_error: fn(K) -> SchemaRegError,
    ) -> Self {
        let capacity = max_entries.unwrap_or(0);
        Self {
            entries: RwLock::new(HashMap::with_capacity(capacity)),
            insertion_order: RwLock::new(VecDeque::with_capacity(capacity)),
            max_entries,
            in_flight_token: AtomicU64::new(0),
            invalidation_generation: AtomicU64::new(0),
            in_flight: Mutex::new(HashMap::new()),
            make_cancelled_error,
        }
    }

    // ── Public accessors ──────────────────────────────────────────────────

    /// Number of entries currently held in the cache.
    pub(crate) fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns `true` when the cache contains no entries.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Current invalidation generation counter value.
    pub(crate) fn generation(&self) -> u64 {
        self.invalidation_generation.load(Ordering::SeqCst)
    }

    // ── Invalidation ──────────────────────────────────────────────────────

    /// Remove a single entry from the cache and cancel any in-flight waiters
    /// for that key.
    pub(crate) fn invalidate(&self, key: K) {
        self.invalidation_generation.fetch_add(1, Ordering::SeqCst);

        let waiters = self
            .in_flight
            .lock()
            .remove(&key)
            .map(|e| e.waiters)
            .unwrap_or_default();

        self.entries.write().remove(&key);
        self.insertion_order.write().retain(|cached| *cached != key);

        let err = (self.make_cancelled_error)(key);
        for waiter in waiters {
            let _ = waiter.send(Err(err.clone()));
        }
    }

    /// Remove all entries from the cache and cancel all in-flight waiters.
    pub(crate) fn clear(&self) {
        self.invalidation_generation.fetch_add(1, Ordering::SeqCst);

        let cancelled: Vec<(K, InFlightEntry<V>)> = self.in_flight.lock().drain().collect();
        self.entries.write().clear();
        self.insertion_order.write().clear();

        for (key, entry) in cancelled {
            let err = (self.make_cancelled_error)(key);
            for waiter in entry.waiters {
                let _ = waiter.send(Err(err.clone()));
            }
        }
    }

    /// Return all keys whose values satisfy `predicate`.
    pub(crate) fn keys_matching<P>(&self, predicate: P) -> Vec<K>
    where
        P: Fn(&V) -> bool,
    {
        self.entries
            .read()
            .iter()
            .filter(|(_, v)| predicate(v.as_ref()))
            .map(|(k, _)| *k)
            .collect()
    }

    // ── Insertion ─────────────────────────────────────────────────────────

    /// Insert `value` for `key` only if the invalidation generation has not
    /// advanced since `observed_generation` was sampled.
    ///
    /// Must NOT hold `cache.write()` when called (acquires it internally).
    /// The generation is re-checked **inside** the write lock to close the
    /// TOCTOU window.
    pub(crate) fn insert_if_current(&self, key: K, value: Arc<V>, observed_generation: u64) {
        let mut entries = self.entries.write();

        // Re-check inside the write lock to close the TOCTOU window.
        if self.invalidation_generation.load(Ordering::SeqCst) != observed_generation {
            debug!(
                ?key,
                "fetch completed after invalidation; skipping cache insert"
            );
            return;
        }

        // Update existing entry if present.
        if let Some(existing) = entries.get_mut(&key) {
            *existing = value;
            return;
        }

        // New entry: evict oldest if at capacity.
        if let Some(max_entries) = self.max_entries {
            let mut insertion_order = self.insertion_order.write();
            if entries.len() >= max_entries
                && let Some(evicted) = insertion_order.pop_front()
            {
                entries.remove(&evicted);
            }
            insertion_order.push_back(key);
        }
        entries.insert(key, value);
    }

    // ── Coalescing fetch ──────────────────────────────────────────────────

    /// Fetch `key` from the cache, or call `fetch` if it is not present.
    ///
    /// Concurrent callers for the same key are coalesced: only the first
    /// ("leader") calls `fetch`; the rest wait on a oneshot channel. If the
    /// leader's future is dropped (task abort), all waiters receive a
    /// "cancelled" error immediately.
    ///
    /// The `fetch` closure is called with no arguments and returns a future
    /// that resolves to `Result<Arc<V>>`. The result is cached (subject to
    /// the generation check) and broadcast to all waiters.
    pub(crate) async fn get_or_fetch<F, Fut>(&self, key: K, fetch: F) -> Result<Arc<V>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<V>>>,
    {
        // Fast path: read lock only.
        if let Some(v) = self.entries.read().get(&key) {
            return Ok(Arc::clone(v));
        }

        // Slow path: check for in-flight entry or become the leader.
        let (waiter_rx, leader_token) = {
            let mut in_flight = self.in_flight.lock();

            // Double-check inside the lock (another task may have inserted
            // between the read-lock check and acquiring in_flight lock).
            if let Some(v) = self.entries.read().get(&key) {
                return Ok(Arc::clone(v));
            }

            if let Some(entry) = in_flight.get_mut(&key) {
                // Become a waiter.
                let (tx, rx) = oneshot::channel();
                entry.waiters.push(tx);
                (Some(rx), None)
            } else {
                // Become the leader.
                let token = self.in_flight_token.fetch_add(1, Ordering::SeqCst) + 1;
                in_flight.insert(
                    key,
                    InFlightEntry {
                        token,
                        waiters: Vec::new(),
                    },
                );
                (None, Some(token))
            }
        };

        // Waiter path: wait for the leader.
        if let Some(rx) = waiter_rx {
            return rx.await.map_err(|_| (self.make_cancelled_error)(key))?;
        }

        // Leader path.
        let Some(leader_token) = leader_token else {
            return Err((self.make_cancelled_error)(key));
        };

        // Drop guard: if this future is dropped (task abort) before we mark
        // `completed`, notify all waiters with a cancellation error.
        struct FetchGuard<'a, K, V>
        where
            K: Hash + Eq + Copy + fmt::Debug + Send + Sync + 'static,
            V: Send + Sync + 'static,
        {
            cache: &'a InMemoryCache<K, V>,
            key: K,
            token: u64,
            completed: bool,
        }

        impl<K, V> Drop for FetchGuard<'_, K, V>
        where
            K: Hash + Eq + Copy + fmt::Debug + Send + Sync + 'static,
            V: Send + Sync + 'static,
        {
            fn drop(&mut self) {
                if self.completed {
                    return;
                }
                let waiters = {
                    let mut in_flight = self.cache.in_flight.lock();
                    if matches!(in_flight.get(&self.key), Some(e) if e.token == self.token) {
                        in_flight
                            .remove(&self.key)
                            .map(|e| e.waiters)
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    }
                };
                let err = (self.cache.make_cancelled_error)(self.key);
                for waiter in waiters {
                    let _ = waiter.send(Err(err.clone()));
                }
            }
        }

        let mut guard = FetchGuard {
            cache: self,
            key,
            token: leader_token,
            completed: false,
        };

        // Snapshot the generation before hitting the backend. Any concurrent
        // `invalidate()` that completes AFTER this point but BEFORE the write
        // lock is acquired will be detected by the generation re-check inside
        // `insert_if_current`.
        let gen_before = self.invalidation_generation.load(Ordering::SeqCst);

        let result = fetch().await;

        // Determine the arc result: on success, try to cache; on error, propagate.
        let arc_result: Result<Arc<V>> = match result {
            Ok(ref value) => {
                let should_insert = {
                    let in_flight = self.in_flight.lock();
                    matches!(in_flight.get(&key), Some(e) if e.token == leader_token)
                };

                if should_insert {
                    let mut entries = self.entries.write();
                    debug!(?key, "cache miss — fetched from backend");

                    // Re-check the generation inside the write lock.
                    if self.invalidation_generation.load(Ordering::SeqCst) != gen_before {
                        debug!(
                            ?key,
                            "fetch completed after invalidation; skipping cache insert"
                        );
                        Ok(Arc::clone(value))
                    } else if let Some(existing) = entries.get(&key) {
                        Ok(Arc::clone(existing))
                    } else {
                        if let Some(max_entries) = self.max_entries {
                            let mut insertion_order = self.insertion_order.write();
                            if entries.len() >= max_entries
                                && let Some(evicted) = insertion_order.pop_front()
                            {
                                entries.remove(&evicted);
                            }
                            insertion_order.push_back(key);
                        }
                        let arc = Arc::clone(value);
                        entries.insert(key, Arc::clone(&arc));
                        Ok(arc)
                    }
                } else {
                    debug!(
                        ?key,
                        "fetch completed after invalidation; skipping cache insert"
                    );
                    Ok(Arc::clone(value))
                }
            }
            Err(e) => Err(e),
        };

        // Notify all waiters.
        let waiters = {
            let mut in_flight = self.in_flight.lock();
            if matches!(in_flight.get(&key), Some(e) if e.token == leader_token) {
                in_flight
                    .remove(&key)
                    .map(|e| e.waiters)
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        };

        for waiter in waiters {
            let _ = waiter.send(arc_result.as_ref().map(Arc::clone).map_err(Clone::clone));
        }
        guard.completed = true;

        arc_result
    }
}

impl<K, V> fmt::Debug for InMemoryCache<K, V>
where
    K: fmt::Debug + Hash + Eq + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryCache")
            .field("len", &self.entries.read().len())
            .field("max_entries", &self.max_entries)
            .finish()
    }
}
