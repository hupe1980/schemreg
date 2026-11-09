+++
title = "Testing strategy"
description = "The test layers behind schemreg, what each one is for, and what it would take to fool it — including the cross-language conformance harness and real TLS handshakes."
weight = 11
+++

Thirteen test layers, each closing a gap the others cannot. This page says what
each one is for, and what it would take to fool it. (`just test` runs them
across every feature set; counts are deliberately not quoted here, because
nothing would keep them true.)

The organising principle is that **self-consistency is not conformance**. A
golden vector written from the same misreading as the implementation agrees with
it perfectly, and both disagree with every other Kafka client. So the framing is
pinned to bytes this crate did not produce.

| Layer | Where |
|---|---|
| Cross-language conformance | `tests/conformance_fixtures.rs` |
| Specification golden vectors | `tests/conformance.rs` |
| Property-based | `tests/properties.rs` |
| Adversarial corpus | `tests/adversarial.rs` |
| REST surface (real server) | `tests/confluent_api.rs`, `tests/apicurio_api.rs`, `tests/http_behaviour.rs` |
| TLS handshakes (real server) | `tests/tls.rs` |
| Header-first decoding | `tests/header_decode.rs` |
| Producer configuration | `tests/schema_resolution.rs` |
| Concurrency | `tests/cache.rs`, `tests/codec.rs` |
| Security boundaries | `tests/security.rs` |
| Trait contract | `tests/contract.rs` |
| Codec round-trips | `tests/wire_format.rs`, `tests/wire_format_v1.rs`, `tests/protobuf_codec.rs`, `tests/avro_references.rs`, `tests/json_references.rs`, `tests/decoder.rs`, `tests/subject_strategy.rs` |
| Unit + doctests | `src/`, `README.md` |

**Every guard has been seen to fail.** A test that has never failed has not
been shown to test anything, so each new check is run once against a planted
defect — a little-endian GUID, a swapped key/value header name, first-wins
instead of last-wins header lookup, prefix-first instead of header-first
decoding, a missing TLS 1.3 floor, a retry loop that retries permanent errors —
and kept only if it turns red.

## 1. Cross-language conformance — `tests/conformance_fixtures.rs`

Fixtures produced by the **official `confluent-kafka-python` 2.16.1 serializers**
running against **Confluent Platform 8.4.0**. schemreg does not produce a
single byte of them, and the generator reads every fixture back with the
reference's own deserializer before writing it.

| Placement | Written by |
|---|---|
| v0 prefix — `0x00` + ID | the default `prefix_schema_id_serializer` |
| header mode — `0x01` + GUID (+ indexes) in `__value_schema_id` / `__key_schema_id` | the stock `header_schema_id_serializer` |
| v1 prefix — `0x01` + GUID in the payload | the reference's `SchemaId.guid_to_bytes()` (no Confluent serializer writes it by default) |

Each fixture is asserted three ways:

1. **Decode** — schemreg recovers the identifier, message-index path, and
   payload the reference wrote.
2. **Re-encode** — feeding those parts back reproduces the reference bytes
   *exactly*. This direction catches a decoder that is merely permissive.
3. **Through the codecs** — every fixture, in every placement, decodes through
   `AvroSchemaDecoder`, `JsonSchemaDecoder` or `ProtobufSchemaDecoder` to the
   value the reference serialized.

The `.proto` covers every message-index shape: `[0]`, `[1]`, `[2]`, `[1,0]`,
`[1,1]`, `[1,1,0]`, in both the prefix and header placements. Regenerate with
`just conformance`; CI diffs the result so reference drift is caught
immediately. The Glue framing has no reference fixture — it is pinned only by
specification-derived vectors.

## 2. Specification golden vectors — `tests/conformance.rs`

Hard-coded byte sequences derived from the published specifications, with the
derivation shown in comments. Faster than the Docker harness and runs
everywhere, so it is the first thing to fail on a framing regression.

## 3. Property tests — `tests/properties.rs`

`proptest` over arbitrary and adversarially-shaped input:

- decoders never panic
- every returned offset is a valid index into the input
- everything the encoders produce, the decoders return unchanged
- detection and decoding never disagree about a frame's identity
- a buffer is never decodable as both Confluent *and* Glue

The varint generator is biased towards continuation bytes, which is what breaks
a naive varint loop.

## 4. Adversarial corpus — `tests/adversarial.rs`

Hand-picked hostile inputs: truncated headers, overlong varints, negative
counts, over-limit counts, unknown compression bytes, all-zero buffers.

## 5. REST surface — `tests/http_behaviour.rs`, `tests/confluent_api.rs`, `tests/apicurio_api.rs`

`wiremock` gives an in-process server whose request log can be asserted, so
"how many requests did we make and how long did we wait" is a test rather than a
claim: retry counts, back-off growth, `Retry-After` honouring and clamping,
body-size limits, redirect bounding, auth headers, `204 No Content` handling,
and the concurrency ceiling.

`confluent_api.rs` and `apicurio_api.rs` pin the other half — the request each
operation actually issues (method, path, query string, request body) and how the
response maps onto `Schema`. A dropped `?normalize=true` or a wrong path is
invisible to unit tests and shows up only against a real registry.

Every path and field name in `apicurio_api.rs` comes from Apicurio's published
`openapi.json` for the v3 Core Registry API, which is why it covers the routes
easiest to get wrong: `POST /search/versions` for content lookup, the
`branch=latest` version expression, `/admin/rules` versus artifact rules, and the
`PUT`-then-`POST` dance Apicurio requires to set a rule that does not exist yet.

## 6. TLS handshakes — `tests/tls.rs`

An in-process rustls server with a CA generated per test, because a TLS option
nothing connects through is a claim, not a guarantee. It proves the hybrid
post-quantum group X25519MLKEM768 is what a TLS 1.3 handshake negotiates, that
trust-only accepts its CA and rejects any other, that an added root extends the
OS store, that `require_tls13` refuses a TLS 1.2-only server the default
accepts, that mutual TLS fails without the identity, and that a refused
handshake is attempted once — `is_tls_error()`, never retried.

## 7. Header-first decoding — `tests/header_decode.rs`

The four Confluent placements (v0/v1 × prefix/header) through every codec,
`WireFormatDecoder` and `dyn PayloadDecoder`; the last repeated header wins; a
header beats whatever the payload starts with; a message-index array in an Avro
header and a Protobuf header without one are refused.

## 8. Producer configuration — `tests/schema_resolution.rs`

`SchemaResolution` and `Framing` decide whether a producer writes to your
registry and which identifier reaches the topic. Both are settings whose failure
mode is silent, so the assertions are counters and bytes, not return codes:

- `LookupOnly` issues **zero** `register_schema` calls, no matter the traffic
- an unregistered schema fails with `is_not_found() == true` and
  `is_retryable() == false`, and is not cached as a success
- `Framing::SchemaGuid` puts `0x01` on the wire and leaves the body byte-identical
- against a registry that reports no GUID it is `NotSupported`, never a frame
  built from something invented
- `encode_with_header` leaves the payload unframed and reproduces the prefix —
  including the Protobuf message-index array — in the header value, and is
  reachable through `dyn PayloadEncoder` rather than only from the concrete type

The resolution logic itself is unit-tested in `src/resolver.rs`; this layer
proves each of the four encoders is actually wired to it.

## 9. Concurrency — `tests/cache.rs`, `tests/codec.rs`

Barrier-synchronised, never timing-based. Mocks park on a `Semaphore` and signal
via `Notify`, so the test controls exactly when the backend responds:

- N concurrent cold misses produce exactly one backend call
- an aborted leader wakes every waiter with an error — no hang, no panic
- an `invalidate()` racing a completing fetch discards the stale result

Every potentially-blocking assertion is wrapped in a 5-second timeout so a
regression fails with a message instead of hanging CI.

## 10. Security boundaries — `tests/security.rs`

Eleven hostile subject names across six Confluent and nine Apicurio operations,
plus positive controls proving legitimate dotted subjects still work. Clients point at
`registry.invalid` (RFC 2606, guaranteed not to resolve), so a `Config` error
proves the guard fired locally and a `Network` error proves it did not.

## 11. Trait contract — `tests/contract.rs`

That defaulted methods return `NotSupported` — specifically, not something
retryable — and that every wrapper (`&T`, `Arc<T>`, `CachedSchemaRegistry<T>`,
`dyn`) forwards all fifteen methods rather than falling through to a default.

The forwarding impls are macro-generated from one signature list, so a
half-wired method is already unrepresentable. The count assertion still earns
its place: it catches a method added to the trait but forgotten in that list.

## 12. Documentation — `README.md` and the site

`README.md` is included in the crate as a doctest, so every non-`ignore` code
block in it is compiled on every run. A README that no longer compiles is the
most common form of documentation rot, and the one readers trust most.

These guides carry TOML front matter for the static-site generator, which
rustdoc would render as prose, so they cannot be included the same way. Instead
their runnable snippets mirror the doctested examples on the items they
document, and CI runs `zola check` — which resolves every internal link and
fetches every external one, so a renamed page or a dead upstream link fails the
build.

## 13. Benchmarks — `benches/wire.rs`

Turn performance claims into measurements. The load-bearing one asserts a cache
hit is size-independent: if it starts scaling with schema size, a `String` clone
has crept back in.

---

## Feature matrix

Every feature is linted and tested **in isolation** as well as together — a
`cfg`-gated item that only compiles under `--all-features` is a real bug for
anyone enabling a single backend. `just clippy` lints thirteen feature sets with
`--all-targets`, `just test` tests eight, CI adds macOS, Windows and Rust beta,
and `just msrv` builds and tests on the declared minimum.

---

## Running it

```bash
just ci          # everything CI runs, locally
just ci-full     # plus cargo-deny, cargo-audit, MSRV, and the Docker conformance harness
just conformance # regenerate the cross-language fixtures (needs Docker) and diff them
```

CI calls the same recipes, and `just ci-parity` fails when a gate recipe has no
CI step, so the two cannot drift.
