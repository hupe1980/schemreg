# Testing strategy

395 tests across nine layers. What each layer is for, and what it would take to
fool it.

| Layer | Where | Tests |
|---|---|---|
| Cross-language conformance | `tests/conformance_fixtures.rs` | 7 |
| Specification golden vectors | `tests/conformance.rs` | 24 |
| Property-based | `tests/properties.rs` | 19 |
| Adversarial corpus | `tests/adversarial.rs` | 24 |
| HTTP behaviour (real server) | `tests/http_behaviour.rs` | 21 |
| Concurrency | `tests/cache.rs`, `tests/codec.rs` | 25 |
| Security boundaries | `tests/security.rs` | 15 |
| Trait contract | `tests/contract.rs` | 11 |
| Codec round-trips | `tests/protobuf_codec.rs`, `tests/decoder.rs`, `tests/subject_strategy.rs`, `tests/wire_format.rs` | 99 |
| Unit + doctests | `src/`, doctests | 150 |

---

## The lesson that shaped this

Version 0.3.0 shipped a **critical** Protobuf wire-format bug with a fully green
test suite, including golden byte vectors.

The golden vector encoded the bug. The test asserted `[0x01, 0x00]` as
canonical, the implementation produced `[0x01, 0x00]`, and both disagreed with
every other Kafka client in existence. Self-consistency is not conformance.

Every layer below exists to make a specific class of that mistake impossible.

---

## 1. Cross-language conformance — `tests/conformance_fixtures.rs`

Fixtures produced by the **official `confluent-kafka-python` serializers**
running against a real Confluent Schema Registry. schemreg does not produce a
single byte of them.

Each fixture is asserted twice:

1. **Decode** — schemreg recovers the schema ID, message-index path, and payload
   the reference wrote.
2. **Re-encode** — feeding those parts back reproduces the reference bytes
   *exactly*. This direction catches a decoder that is merely permissive.

The `.proto` covers every message-index shape: `[0]`, `[1]`, `[2]`, `[1,0]`,
`[1,1]`, `[1,1,0]`. Regenerate with
`docker compose -f conformance/docker-compose.yml up --build`; CI diffs the
result so reference drift is caught immediately.

**This is the layer a v0.3.0 build fails on its first fixture.**

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

## 5. HTTP behaviour — `tests/http_behaviour.rs`

`wiremock` gives an in-process server whose request log can be asserted, so
"how many requests did we make and how long did we wait" is a test rather than a
claim: retry counts, back-off growth, `Retry-After` honouring and clamping,
body-size limits, redirect bounding, auth headers, `204 No Content` handling,
and the concurrency ceiling.

## 6. Concurrency — `tests/cache.rs`, `tests/codec.rs`

Barrier-synchronised, never timing-based. Mocks park on a `Semaphore` and signal
via `Notify`, so the test controls exactly when the backend responds:

- N concurrent cold misses produce exactly one backend call
- an aborted leader wakes every waiter with an error — no hang, no panic
- an `invalidate()` racing a completing fetch discards the stale result

Every potentially-blocking assertion is wrapped in a 5-second timeout so a
regression fails with a message instead of hanging CI.

## 7. Security boundaries — `tests/security.rs`

Eleven hostile subject names across six Confluent and seven Apicurio operations,
plus positive controls proving legitimate dotted subjects still work. Clients point at
`registry.invalid` (RFC 2606, guaranteed not to resolve), so a `Config` error
proves the guard fired locally and a `Network` error proves it did not.

## 8. Trait contract — `tests/contract.rs`

That defaulted methods return `NotSupported` — specifically, not something
retryable — and that every wrapper (`&T`, `Arc<T>`, `CachedSchemaRegistry<T>`,
`dyn`) forwards all twelve methods rather than silently falling through to a
default.

## 9. Benchmarks — `benches/wire.rs`

Turn performance claims into measurements. The load-bearing one asserts a cache
hit is size-independent: if it starts scaling with schema size, something
reintroduced a `String` clone.

---

## Feature matrix

Every feature is linted and tested **in isolation** as well as together — a
`cfg`-gated item that only compiles under `--all-features` is a real bug for
anyone enabling a single backend. CI runs a 13-way clippy matrix with
`--all-targets` across three operating systems, plus a dedicated MSRV job.

---

## What is still missing

| Gap | Why it matters |
|---|---|
| `cargo-fuzz` target on the decoders | Property tests explore a wide space; a fuzzer explores it adversarially and continuously |
| Loom model of `InMemoryCache::get_or_fetch` | The token + generation logic is correct by argument and by test, not by exhaustive interleaving check |
| Testcontainers against Karapace and Apicurio 3.x | The "compatible registries" claim is verified by API-shape reading, not by execution |

---

## Running it

```bash
cargo test --all-features            # everything
cargo test --no-default-features     # the core, no transport
cargo bench                          # benchmarks
cargo clippy --all-features --all-targets -- -D warnings

# Regenerate the cross-language fixtures (needs Docker)
docker compose -f conformance/docker-compose.yml up --build --abort-on-container-exit
```
