# Changelog

All notable changes to `schemreg` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The crate is pre-1.0: **minor versions may contain breaking changes**, and every
one is listed under a `Breaking` heading with the migration it requires.

---

## [0.4.0] — 2026-08-04

A correctness and hardening release. It contains a **critical wire-format fix**
that changes the bytes produced for Protobuf messages, and several deliberate
breaking changes taken while the pre-1.0 window is open.

### 🔴 Fixed — Critical

- **Confluent Protobuf message-index framing was wire-incompatible in both
  directions.** The element count was written as a plain unsigned LEB-128
  varint, but the Confluent serde ZigZag-encodes it
  (`org.apache.kafka.common.utils.ByteUtils.writeVarint`), and the mandated
  single-`0x00` encoding of the common path `[0]` was neither emitted nor
  recognised.

  Consequences before this fix:

  | Direction | Result |
  |---|---|
  | `schemreg` → Java/Python/Go/.NET | consumer reads `size = -1` and throws; the message is undeliverable |
  | Java → `schemreg`, path `[0]` | index reported as `[]` instead of `[0]` |
  | Java → `schemreg`, path `[1]` | count misread as 2, a payload byte consumed as an index — **silent data corruption** |

  Now verified against fixtures produced by the official `confluent-kafka-python`
  serializers (`conformance/`), covering every message-index shape:
  `[0]`, `[1]`, `[2]`, `[1,0]`, `[1,1]`, `[1,1,0]`.

  **Migration:** none for correct code. Bytes emitted by 0.3.0 were not accepted
  by any conforming consumer, so no working integration regresses.

### 🟠 Fixed — High

- **Every AWS Glue SDK error was reported as a retryable network error.**
  `EntityNotFoundException` and `AccessDeniedException` both satisfied
  `is_retryable() == true`, so a caller's retry loop spun forever on a permanent
  failure, and `is_not_found()` could never return `true` for Glue. SDK errors
  are now classified by service code into `Api` / `Auth` / `Http` / `Network`
  with correct retry semantics.

- **Path traversal was reachable** via `ConfluentSchemaRegistry::delete_subject`
  and *every* Apicurio artifact operation — none of them called
  `validate_subject`. Because the percent-encoder deliberately preserves `.`,
  a `..` segment survived encoding, so `DELETE /subjects/..` could be collapsed
  by a proxy into `DELETE /subjects`. Now validated at every path-building site,
  with the guard extended to reject percent-encoded traversal that a
  double-decoding intermediary would recover.

### 🟡 Fixed — Medium

- **Avro and JSON decoder caches were unbounded and non-coalescing.** A
  message-driven `HashMap` with no eviction, and N concurrent cold decodes
  compiled the same schema N times. Both now use the crate's bounded,
  coalescing `InMemoryCache`.
- **`Retry-After` was honoured only on 429**, never on 503 — the status servers
  actually return during rolling restarts.
- **`is_retryable()` disagreed with the internal retry policy** (`429 | 503`
  versus `429 | 5xx`), so a 502 was retried internally but reported to callers
  as permanent.
- **Apicurio `get_subjects` / `get_versions` silently truncated at 500** with no
  error and no warning. Both now paginate.
- **`SchemaDecoder` had no implementors** anywhere in the crate.
- **Type erasure was a one-way door**: `Arc<dyn DynSchemaRegistryClient>` could
  not be passed to anything generic over `SchemaRegistryClient`.
- **`cargo deny` evaluated only the default (empty) feature set**, so the entire
  AWS SDK and `jsonschema` dependency trees were never scanned. Enabling
  `all-features` surfaced two unlicensed crates and four duplicate-version
  violations.
- Patched **RUSTSEC-2026-0185** (`quinn-proto`, lockfile-only) and an `anyhow`
  advisory.

### ✨ Added

- **`protobuf` feature** — `ProtobufSchemaEncoder` / `ProtobufSchemaDecoder`
  built on `prost`, with the **message-index path derived automatically from the
  message descriptor** via `prost-reflect`. Callers no longer hand-write index
  arrays, so the path cannot drift when the `.proto` is reordered.
  `with_expected_descriptor` rejects a payload of the wrong message type, which
  otherwise decodes silently into a struct full of defaults.
- **`RetryPolicy`** — configurable retry count, base and maximum back-off,
  `Retry-After` honouring, and **equal jitter** (new; previously every client
  retried on an identical schedule, reconverging into synchronised waves).
  `RetryPolicy::none()` disables retrying for callers that implement their own.
- **`Retry-After` HTTP-date form** (RFC 9110 §10.2.3), in addition to
  delta-seconds. No new dependency.
- **`max_concurrent_requests`** on both HTTP builders — a hard ceiling on
  in-flight requests, for cold starts that fan out to thousands of *distinct*
  schema IDs (coalescing only collapses same-ID bursts).
- **`AvroSchemaDecoder::with_reader_schema`** — Avro schema resolution
  (defaulted fields, dropped fields, numeric promotion), matching the Confluent
  Java deserializer. Previously decoding always used the writer schema.
- **`GlueSchemaRegistryClient::health_check`**, threaded through the trait, the
  `dyn` shim, and the cache wrapper.
- **`SchemaDecoder for WireFormatDecoder`** — the object-safe framing stripper.
- **`SchemaRegistryClient for dyn DynSchemaRegistryClient`** — type erasure now
  composes in both directions.
- **Cross-language conformance suite** (`conformance/`) — a Docker stack that
  generates fixtures with the official Confluent serializers; CI regenerates and
  diffs them so reference-implementation drift is caught immediately.
- **Property tests** (`proptest`) asserting the decoders never panic, never
  return an out-of-bounds offset, and round-trip everything the encoders emit.
- **Benchmarks** (`criterion`) for framing, detection, cache hits, and
  coalescing.
- **`docs/`** — task-oriented guides for wire formats, security, performance,
  and migration.
- Bounded, observable producer-side subject caches: `cached_subject_count()`,
  `invalidate_subject()`, and `max_subject_cache_entries()` on every encoder.

### 💥 Breaking

| Change | Migration |
|---|---|
| Protobuf framing now spec-conformant | None for correct code — 0.3.0 bytes were not accepted by any conforming consumer |
| Glue errors map to `Api` / `Auth` / `Http` instead of `Network` | Match on those variants, or better, use `is_not_found()` / `is_auth_error()` / `is_retryable()` |
| `is_retryable()` now covers all 5xx, not just 503 | Behavioural; the new classification is the correct one |
| `validate_subject` rejects empty, oversized (>512 B), and `.`/`..` subjects | None of these was ever a valid registry subject |
| `aws-lc-rs` feature **removed** | It was a no-op — `reqwest`'s `rustls-tls-webpki-roots` pins `ring` regardless. See the README's *TLS backend* section for the application-side recipe |
| `native-tls-roots` no longer implies `confluent` | It is a `reqwest` passthrough; enable `confluent` or `apicurio` explicitly |
| Cleartext HTTP + credentials is now a hard error **off-loopback** | `http://localhost:8081` with auth still works (with a warning); a deployed registry needs HTTPS |
| `CachedGlueSchemaRegistry::warm_cache` takes `impl IntoIterator` | Existing slice call sites still compile |
| `tokio` features trimmed to `["sync", "time"]` | None — the removed features were unused |

### 🔧 Changed

- `warm_cache` uses `buffer_unordered` instead of chunked `join_all`, removing
  the per-batch barrier so one slow schema no longer stalls the others.
- The three encoder subject-resolution coalescers (Confluent, Avro, JSON) were
  ~70 near-identical hand-rolled lines each; they now share `InMemoryCache`, so
  the cancellation and invalidation-race guarantees are proven once.
- Crate-level lints: `forbid(unsafe_code)`, `warn(missing_docs,
  missing_debug_implementations, clippy::unwrap_used, clippy::expect_used,
  clippy::panic)`. `Debug` added to seven public types, all masking credentials.
- Redirects bounded to 3 (was reqwest's default 10).
- The re-export surface is complete and uniform — `avro`'s types and
  `decode_glue_wire_format_borrowed` were previously unreachable from the root.
- CI: 12-way clippy matrix with `--all-targets`, 3 operating systems, a
  dedicated MSRV job, nightly supply-chain scans, all examples executed rather
  than merely compiled, and a single aggregate required check.

### 📊 Verification

187 (v0.3.0) → **395 tests** with all features, 250 with none. `cargo clippy
--all-targets -D warnings` across 14 feature combinations. `cargo deny` and
`cargo audit` clean with all features. All 8 examples execute in CI.

Benchmarks confirm a cache hit costs **14.3 ns regardless of schema size**
(64 B → 64 KiB), header decode is **1.6 ns** independent of payload size, and
coalescing per-task cost *falls* from 8.06 µs at 1 task to 0.33 µs at 256.

---

## [0.3.0] — 2026-06

- Apicurio Registry v3 native client.
- `CustomSubjectFn` for fully custom subject-name strategies.
- `health_check`, `set_compatibility`, `get_compatibility` on the client trait.
- Generic `InMemoryCache` extracted and shared by both cache wrappers.
- Request/response body limits, `zeroize`-protected credentials.

## [0.2.0] — 2026-05

- JSON Schema encoder/decoder.
- `SchemaId` / `SchemaVersion` newtypes; `EncodeTarget` replacing `is_key: bool`.
- HTTP client configuration: timeouts, custom CAs, mTLS, connection pooling.

## [0.1.0] — 2026-05

- Initial release: Confluent and AWS Glue wire formats, trait-based client
  abstraction, in-memory caching with in-flight coalescing, Avro codec.

[0.4.0]: https://github.com/hupe1980/schemreg/releases/tag/v0.4.0
[0.3.0]: https://github.com/hupe1980/schemreg/releases/tag/v0.3.0
[0.2.0]: https://github.com/hupe1980/schemreg/releases/tag/v0.2.0
[0.1.0]: https://github.com/hupe1980/schemreg/releases/tag/v0.1.0
