# Changelog

All notable changes to `schemreg` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The crate is pre-1.0: **minor versions may contain breaking changes**, and every
one is listed under a `Breaking` heading with the migration it requires.

---

## [0.5.0] — 2026-08-19

Support for the wire formats Confluent Platform 8 introduced — a 16-byte schema
GUID, and schema identifiers carried in Kafka record headers — reachable from the
codecs rather than only from the raw framing functions; a producer-side
resolution policy so a producer need not hold write credentials; JSON Schema
`$ref` resolution across subjects; and a considerably more complete Apicurio v3
client.

See the [0.5 migration guide](https://hupe1980.github.io/schemreg/docs/migrating-0-5/)
for the upgrade path.

### ✨ Added

- **`SchemaResolution` — producers no longer have to write to the registry.**
  Every encoder (`ConfluentSchemaEncoder`, `AvroSchemaEncoder`,
  `JsonSchemaEncoder`, `ProtobufSchemaEncoder`) takes a `.resolution(..)`:
  `AutoRegister` (the default, matching `auto.register.schemas=true` in the Java
  serdes), `LookupOnly`, or `UseLatestVersion` (matching `use.latest.version`).
  Until now every encoder called `register_schema` unconditionally, so a
  read-only producer was impossible even though `lookup_schema` existed and the
  documentation recommended it. `LookupOnly` needs only `Subject:Read` and turns
  a drifted local schema into a startup failure — `is_not_found()` is `true` and
  `is_retryable()` is `false` — instead of a silently created production version.
- **`Framing` — the codecs can emit wire format v1 and header placement.**
  `.framing(Framing::SchemaGuid)` puts a GUID on the wire; `encode_with_header`
  returns a `HeaderFramed` carrying the header name, the header value, and an
  unprefixed payload — on the concrete encoders and on the object-safe
  `PayloadEncoder` trait, where it defaults to `NotSupported`. Previously v1 and header framing were reachable only from
  the raw `encode_wire_format` / `encode_schema_id_header` functions, so the
  crate advertised formats none of its codecs could produce. Against a registry
  that reports no GUID, v1 framing is a `NotSupported` error rather than an
  invented identifier.
- **JSON Schema references across subjects.** `JsonSchemaDecoder` resolves the
  transitive closure of `Schema::references` and compiles the set together;
  `JsonSchemaEncoderBuilder::dependencies` supplies the same documents as
  `(name, schema)` pairs for the producer side. Bounded exactly as the Avro
  resolver is (32 levels, 256 schemas, visited set), and the retriever has no
  network access — a `$ref` nobody supplied is a compile error, not a fetch.
- **Apicurio v3: `lookup_schema`** via `POST /search/versions`, scoped to the
  group and artifact and not canonicalised, so `SchemaResolution::LookupOnly`
  works on this backend too.
- **Apicurio v3: `delete_version`** via
  `DELETE /groups/{g}/artifacts/{a}/versions/{expr}`, and `delete_subject` now
  reports the versions it removed instead of an empty list.
- **Apicurio v3: global compatibility rules.** An empty subject addresses
  `/admin/rules/COMPATIBILITY`, matching the Confluent client's convention, and
  `set_compatibility` falls back from `PUT` to `POST` when the rule has never
  been configured — which is the common case for a fresh artifact and previously
  surfaced as a bare 404.
- **`ApicurioSchemaRegistryBuilder::dereference_references`** — sends
  `?references=DEREFERENCE`, so Apicurio inlines referenced content server-side
  and a referencing Avro schema becomes parseable on this backend at no extra
  round-trip. Off by default, because it changes the bytes returned.
- **Confluent wire format v1**: magic byte `0x01` followed by a 16-byte
  [`SchemaGuid`]. A GUID is a fingerprint of the schema, so it names the same
  schema in every registry — unlike an ID, which is assigned per registry.
  `encode_wire_format` emits v1 when handed a `SchemaGuid` and v0 when handed a
  `u32`/`SchemaId`; `decode_wire_format` accepts either and reports which it
  found as a `SchemaKey`.
- **Schema ID in a Kafka header**: `encode_schema_id_header` /
  `decode_schema_id_header`, with the `__key_schema_id` and `__value_schema_id`
  names Confluent uses and a `schema_id_header_name(EncodeTarget)` helper. The
  header value is byte-for-byte the prefix the payload would otherwise carry.
- `SchemaRegistryClient::get_schema_by_guid`, `get_schema_by_key` (dispatches on
  the variant a record named), `lookup_schema`, and `delete_version`.
- `lookup_schema` posts to `/subjects/{subject}`, the read-only route that
  reports an existing registration **without creating one**. `Ok(None)` covers
  both "no such subject" and "this content is not registered", so a read-only
  producer no longer has to classify error codes — or risk `register_schema`
  quietly creating a version in production when the local schema drifts.
- `CachedSchemaRegistry` gained a GUID-keyed cache with the same bound,
  coalescing, and cancellation guarantees as the ID cache, plus
  `invalidate_guid` and `guid_cache_len`.
- **Avro schema references are resolved.** A schema naming a type from another
  subject is stored verbatim by the registry and is not parseable alone;
  `AvroSchemaDecoder` now fetches the transitive dependency closure and parses
  the set together. Diamonds are fetched once per subject, and a reference cycle
  errors instead of recursing. `AvroSchemaEncoderBuilder::dependencies` supplies
  the same definitions for the producer side.
- `Schema::guid`, `Schema::key()`, `Schema::with_guid`.
- `error_code` module with the named Confluent constants, plus
  `SchemaRegError::error_code()`, `status()`, `is_incompatible()`, and
  `is_invalid_schema()`.
- `SchemaGuid` and `GlueSchemaVersionId` convert to and from `uuid::Uuid`.
- Every request carries a `User-Agent: schemreg/{version}`.
- `get_compatibility` now asks for `?defaultToGlobal=true`, so a subject with no
  override of its own reports the global default instead of failing with error
  code 40408 — which is what most subjects have, so the common case was an error.
- `SchemaVersion` has always documented a negative value as meaning "latest";
  the Confluent and Apicurio clients now translate it (`latest`,
  `branch=latest`) rather than sending `-1` and getting a rejection.

### 🔴 Fixed

- **The Apicurio v3 client fabricated schema IDs.** Registry v3 removed the
  `X-Registry-GlobalId` / `-Version` / `-ArtifactId` response headers that v2
  set on content endpoints, so `get_latest_schema` and `get_schema_by_version`
  fell back to a schema ID of **`0`** — a valid-looking identifier a producer
  would then frame real records with — and to an artifact type of `AVRO`
  whatever the artifact actually was. Both now come from version metadata,
  fetched alongside the content.
- **Invalidating one cache key discarded concurrent fetches of every other
  key.** The in-flight guard used a single global generation counter, so a
  stream of `invalidate()` calls could stop the cache from ever storing
  anything. `get_or_fetch` now compares the per-key in-flight token, which is
  exact; the global counter remains only for `insert_if_current`, where it is
  conservative in the safe direction.
- **A registry-side `5xxxx` error was classified as permanent.** Those bodies
  parse as JSON, so a 500 from a failed backing store became a non-retryable
  `Api` error while an identical 500 with an HTML body was retried. `5xxxx`
  codes are now retryable.
- `invalidate_subject` was O(n²) — one full scan plus one queue rebuild per
  matching entry. Now a single pass.
- AWS Glue's "unclassified service error" mapped to synthetic code `50001`,
  which the new retry rule would have read as transient; an unrecognised Glue
  error is now classified by its HTTP status alone.
- A Protobuf message index that ZigZag-decodes to a negative number is rejected
  at the framing boundary rather than mis-slicing the payload.
- **`is_not_found()` missed a bare HTTP 404.** A Confluent-compatible registry
  answers 404 with an `error_code` body, but a reverse proxy, an API gateway, or
  a registry without the route answers with HTML or nothing — and that landed in
  `Http`, where the predicate returned `false`. `lookup_schema` therefore
  reported a transport-shaped error instead of `Ok(None)` for a subject that
  simply was not there.
- **The GUID cache and the ID cache never populated each other**, despite the
  documentation promising a schema reachable by both identifiers would be
  findable under either. A by-ID fetch now indexes the GUID the registry
  reported and vice versa; the two maps share one `Arc<Schema>`, so the
  duplicate costs a pointer.
- **`delete_subject` / `delete_version` left the cache serving deleted
  subjects.** `CachedSchemaRegistry` now invalidates the subject's entries after
  a successful delete — and only after a successful one.
- `ConfluentSchemaRegistry::delete_version` sent a literal `-1` for a negative
  version instead of `latest`, so the documented "negative means latest"
  convention worked for `get_schema_by_version` but was rejected with error code
  42202 here.
- The `serde` impls on `SchemaId`, `SchemaVersion`, and `SchemaGuid` were gated
  on the `confluent` feature, so an `apicurio`-only build had `serde` compiled in
  but no derives. They now follow a `serde-impls` feature that every
  JSON-speaking backend enables.
- A Protobuf message-index error message was mangled by a missing line
  continuation, printing a run of spaces mid-sentence.

### 💥 Breaking

- `decode_wire_format` / `decode_wire_format_bytes` return `SchemaKey`, not
  `SchemaId`. `DetectedWireFormat::Confluent` renames `schema_id` to `key`.
- `Schema::id` is `Option<SchemaId>`: a GUID-addressed lookup establishes no
  numeric ID, and reporting `0` was a lie. `Schema::new` takes anything
  convertible to `SchemaKey`.
- Protobuf message indexes are `u32` rather than `i32` — a descriptor position
  is never negative, so the encoder can no longer produce a frame the decoder
  rejects. Emitted bytes are unchanged.
- `SchemaEncoder` / `SchemaDecoder` renamed to `PayloadEncoder` /
  `PayloadDecoder`. They frame already-serialised bytes; the old names implied
  they serialised a value.
- `check_compatible` removed — call `check_compatibility(.., &[])`.
- `CompatibilityLevel::from_str` is case-insensitive, matching `SchemaType`.
- `SchemaType`, `CompatibilityLevel`, and `GlueSchemaVersionId` parse failures
  are `Config` errors rather than `InvalidState`.
- The `codec_cache` module is gone; `DEFAULT_MAX_SUBJECT_CACHE_ENTRIES` now lives
  in `resolver` alongside `SchemaResolution` and `Framing`. It is re-exported at
  the crate root as before, so `use schemreg::DEFAULT_MAX_SUBJECT_CACHE_ENTRIES`
  is unaffected.
- `SchemaRegError::is_not_found()` now returns `true` for `Http { status: 404 }`.
  Code that classified a bare 404 as "some other HTTP failure" will now see it as
  not-found — which is what it always meant.

### 🧹 Internal

- The `SchemaRegistryClient` mirrors — the object-safe `DynSchemaRegistryClient`,
  its blanket impl, the `&T` / `Arc<T>` forwarding impls, and the `dyn →
  generic` bridge — are generated from a single signature list. Adding a method
  no longer means four hand-copied edits, one of which silently answers
  `NotSupported` if forgotten.
- `InMemoryCache` keeps entries and insertion order under one lock, so the two
  cannot disagree about which keys exist.
- `SchemaGuid` and `GlueSchemaVersionId` share the `uuid` crate instead of two
  hand-rolled hex codecs. `uuid` is `std`-only here: no RNG, no transitive
  dependencies.
- `README.md` is compiled as a doctest, so its code cannot rot.
- The four encoders shared four copies of "resolve a subject to a schema ID".
  They now share one `resolver::resolve_schema_key`, so the three resolution
  modes and both framings were implemented once rather than four times.
- `message_index_path`'s validation is reachable with a raw descriptor path, so
  the malformed-shape tests exercise the real function instead of a hand-copied
  mirror of it that could drift.
- New test suites: `tests/apicurio_api.rs` (18 tests pinning the v3 REST surface
  against Apicurio's published OpenAPI — the backend previously had no HTTP-level
  coverage at all), `tests/schema_resolution.rs` (17), and
  `tests/json_references.rs` (11). 537 tests in total.
- The guides moved from `docs/` to `site/`, a [Zola] site published to GitHub
  Pages at <https://hupe1980.github.io/schemreg>, with a landing page, six new
  guides (quick start, producers, codecs, Kafka integration, caching,
  resilience), full-text search, and `zola check` link validation in CI.
  `README.md` is now an orientation document rather than a second copy of the
  guides, and `Cargo.toml` gained a `homepage` pointing at the site.
[Zola]: https://www.getzola.org

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
