+++
title = "Caching"
description = "How schemreg caches schemas: immutable-identifier caching, request coalescing, cancellation safety, invalidation races, and bounded memory on every message-driven path."
weight = 6
+++

Every cache in the crate is the same type. The registry schema caches, the Glue
version cache, the codecs' parsed-schema and compiled-validator caches, and the
producers' subject maps all share one implementation, so the cancellation and
invalidation-race guarantees are established once instead of re-derived — and
mis-derived — per call site.

## What is cached, and for how long

| Operation | Cached | Why |
|---|---|---|
| `get_schema_by_id` | **Forever** | A registry never reassigns an ID |
| `get_schema_by_guid` | **Forever** | A GUID is a fingerprint of the schema itself |
| `get_latest_schema` | **Never** | A newer version can be registered at any moment |
| `get_schema_by_version` | **Never**, but populates the identifier caches | Same reason; the schema it returns is still worth indexing |

There is no TTL, because there is nothing for one to protect against. An
identifier-addressed lookup is immutable by construction; a subject-addressed
one always reaches the backend.

```rust,ignore
use schemreg::CachedSchemaRegistry;

let cached = CachedSchemaRegistry::with_max_entries(registry, 512);
```

## One round-trip fills both indexes

A schema reachable by both an ID and a GUID is one schema. Fetching it by either
identifier indexes it under the other, so a later record framed the other way is
served locally:

```rust,ignore
let by_id   = cached.get_schema_by_id(SchemaId::new(4)).await?;   // one HTTP request
let by_guid = cached.get_schema_by_guid(guid).await?;             // no request
assert!(Arc::ptr_eq(&by_id, &by_guid));
```

The two maps hold the same `Arc<Schema>`, so the duplicate entry costs one
pointer.

## Guarantees

| Property | Guarantee |
|---|---|
| Bound | Default 1 000 entries per cache; the oldest **inserted** entry is evicted on overflow (FIFO) |
| Coalescing | N concurrent cold misses for one key ⇒ exactly one backend request |
| Cancellation | If the leading task is aborted, every waiter is woken with an error — never a hang |
| Errors | Propagated to every waiter, never cached, so a transient failure does not become sticky |
| Invalidation race | A fetch completing after an `invalidate()` for **that key** is discarded, not resurrected — and invalidating one key never discards another key's in-flight result |
| Deletion | `delete_subject` / `delete_version` drop what was cached under that subject, but only on success |

Eviction is FIFO, not LRU: tracking recency means a write lock on every *read*,
which is the wrong trade for a cache whose working set normally sits far below
the bound and where a miss costs one idempotent round-trip.

## Pre-warming

```rust,ignore
// 16 concurrent fetches, no per-batch barrier: a new fetch starts as soon as
// any earlier one finishes, so one slow schema does not stall the rest.
if let Err(e) = cached.warm_cache([1u32, 2, 3]).await {
    for (id, err) in &e.failures {
        eprintln!("warm failed for {id}: {err}");
    }
}
```

Every ID is attempted regardless of individual failures, and the ones that
loaded stay cached — a partial warm is usually fine, and the caller is the only
one who can decide.

## Invalidation

```rust,ignore
cached.invalidate(2u32);                     // one ID
cached.invalidate_guid(guid);                // one GUID
cached.invalidate_subject("orders-value");   // everything carrying that subject
cached.clear_cache();                        // everything
```

`invalidate_subject` is one O(n) scan. Schemas fetched by bare identifier carry
no subject, so only entries populated through `get_latest_schema` or
`get_schema_by_version` can match — which is correct for a soft delete and
unavoidable for a permanent one, since nothing local records which IDs belonged
to the subject.

## Memory

No cache in the crate is unbounded, and none sits on a message-driven path
without a bound. That is a deliberate property rather than a default: a consumer
reading a compacted topic can encounter an unbounded number of distinct schema
IDs, and an unbounded map there is a slow memory leak that only shows up in
production.

Cloning a cached schema is O(1) regardless of its size — `Schema` holds
`Arc<str>` for the schema text and the subject, and clients return
`Arc<Schema>`. A 64 KiB schema costs the same cache hit as a 64-byte one; see
[Performance](@/docs/performance.md).

## Observability

```rust,ignore
cached.cache_len();        // schemas cached by ID
cached.guid_cache_len();   // schemas cached by GUID
cached.cache_is_empty();
```

`cache_len` deliberately does not count GUID-keyed entries: a schema reachable
by both identifiers is one schema, and counting it twice would make the number
useless as an occupancy signal.
