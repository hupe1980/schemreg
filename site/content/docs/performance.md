+++
title = "Performance"
description = "Measured numbers for wire-format decoding, cache hits, Protobuf index handling, and request coalescing — plus how to size the caches."
weight = 10
+++

Measured on an Apple M-series laptop with `cargo bench --features glue`. Numbers
are medians; reproduce with `cargo bench`.

## Decode is O(1) in payload size

| Payload | `decode_wire_format` | `decode_wire_format_bytes` |
|---|---|---|
| 64 B | 1.60 ns | 5.61 ns |
| 1 KiB | 1.62 ns | 5.58 ns |
| 64 KiB | 1.61 ns | 5.71 ns |

Flat across three orders of magnitude — nothing is copied. The `Bytes` variant
costs ~4 ns more for the refcount bump and is likewise size-independent.

`encode` does scale (28 ns → 755 ns from 64 B to 64 KiB): that is the one
unavoidable `memcpy` of the payload into the framed buffer.

## Cache hits are O(1) in schema size

| Schema text | Cache hit |
|---|---|
| 64 B | 14.28 ns |
| 4 KiB | 14.31 ns |
| 64 KiB | 14.28 ns |

This is the `Arc<str>` design paying off. `Schema` holds `Arc<str>` for the
schema text and the subject, and clients return `Arc<Schema>`, so serving a
64 KiB schema costs one atomic increment — identical to a 64-byte one.

A regression here shows up as this benchmark scaling with schema size, which
means a `String` clone has crept back in.

## Detection is ~1 ns

| Input | Time |
|---|---|
| Confluent frame | 1.35 ns |
| Unknown bytes | 1.05 ns |
| Truncated header | 1.08 ns |

It reads at most 18 bytes and returns `Copy` values.

## Protobuf index handling

| Path | Encode | Decode index |
|---|---|---|
| `[0]` (optimised) | 39.7 ns | 16.7 ns |
| `[2]` | 42.2 ns | 18.3 ns |
| `[1, 0]` | 44.0 ns | 19.3 ns |
| `[2, 1, 4, 0]` | 49.6 ns | 20.7 ns |

## Coalescing scales sub-linearly

Tasks racing for one uncached schema ID, including `tokio::spawn` overhead:

| Concurrent tasks | Total | Per task |
|---|---|---|
| 1 | 8.06 µs | 8.06 µs |
| 8 | 8.57 µs | 1.07 µs |
| 64 | 21.2 µs | 0.33 µs |
| 256 | 84.9 µs | 0.33 µs |

Per-task cost *falls* as concurrency rises and then flattens — the contended
section is a `HashMap` lookup plus a channel push, and one backend call is
amortised across every waiter. Contention is not the bottleneck; the network
call the coalescer avoids is three to four orders of magnitude larger.

---

## Allocation profile

Cache-hit decode path, from raw `Bytes` to `DecodedMessage`:

| Step | Allocations |
|---|---|
| `detect_wire_format` | 0 |
| `data.slice(offset..)` | 0 — refcount bump |
| `get_schema_by_key` via `dyn` | 1 — `Box::pin` for the erased future |
| Cache hit | 0 |
| Protobuf index parse *(Protobuf only)* | 1 — `Vec<u32>` |
| **Total** | **1** (Avro/JSON), **2** (Protobuf) |

The one allocation is the erased-future box, and only callers who chose
`Arc<dyn DynSchemaRegistryClient>` pay it. Generic callers monomorphize with
zero boxing.

---

## Sizing the caches

Every cache defaults to 1 000 entries with FIFO eviction. At a 2 KiB schema that
is ~2.1 MiB.

`CachedSchemaRegistry` keeps two of them — one keyed by schema ID, one by schema
GUID — because a record may name either and the mapping between them is not
known locally. Both hold the same `Arc<Schema>`, so a schema reachable by both
identifiers costs one pointer extra, not a second copy of the text.

| Entries | Approximate footprint |
|---|---|
| 1 000 (default) | ~2.1 MiB |
| 10 000 | ~21 MiB |
| 100 000 | ~212 MiB |

Raise it with `with_max_entries` when the schema-ID cardinality of your *stream*
genuinely exceeds the default — a compacted topic replayed from the beginning, or
a multi-tenant topic where each tenant registers its own schema.

FIFO rather than LRU is deliberate. Tracking recency means taking a write lock
on every *read*, which is the wrong trade for a cache whose working set — the
schemas a service actually uses — normally sits far below the bound, and where a
miss costs one idempotent round-trip. FIFO is O(1) on a `VecDeque` and needs
only the write path.

### Producer-side caches

The `subject → schema ID` maps inside each encoder are bounded separately
(`max_subject_cache_entries`, default 1 000). They are keyed by subject, so they
are normally bounded by your topic set rather than by traffic. The exception is a
`SubjectNameStrategy::Custom` that derives subjects from message *content* —
size that one deliberately.

---

## Tuning concurrency

- **`warm_cache`** runs at most 16 fetches concurrently with no per-batch
  barrier, so one slow schema does not stall the others. Failures are collected,
  not fatal: successful IDs stay cached.
- **Apicurio subject lookups cost two requests** — version metadata, then
  content — because Registry v3 removed the headers that carried a version's
  identity. `get_schema_by_id` is one request, and it is the one on the
  consumer hot path.
- **`max_concurrent_requests`** caps in-flight requests per client. Coalescing
  already collapses concurrent misses for the *same* ID; this bounds the other
  case — a cold start fanning out to thousands of *distinct* IDs, where each miss
  opens a socket.
- **`pool_max_idle_per_host`** controls connection reuse.

---

## Deliberate trade-offs

Three optimisations are available and declined, because each buys less than it
costs against a network round-trip:

| Not done | Would save | Costs |
|---|---|---|
| `SmallVec` for Protobuf index paths | one allocation per message | a dependency, for a stack-vs-heap difference visible only under a profiler |
| A generic `WireFormatDecoder<C>` | the erased-future box | a type parameter on every struct holding a decoder |
| A faster hasher | a few ns per lookup | `SchemaId` is a `u32` and `SchemaGuid` is 16 bytes; SipHash over either is noise |
