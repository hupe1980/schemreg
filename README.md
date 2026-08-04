# 🗂️ schemreg

[![Crates.io](https://img.shields.io/crates/v/schemreg.svg)](https://crates.io/crates/schemreg)
[![docs.rs](https://docs.rs/schemreg/badge.svg)](https://docs.rs/schemreg)
[![CI](https://github.com/hupe1980/schemreg/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/schemreg/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![MSRV: 1.88](https://img.shields.io/badge/MSRV-1.88-blue.svg)](https://releases.rs/docs/1.88.0/)

Async schema registry client for **Confluent Schema Registry** (and Karapace), **Apicurio Registry v3**, and **AWS Glue Schema Registry**, with:

- ⚡ Zero-copy wire-format encode / decode — Confluent 5-byte header, Confluent Protobuf message-index, Glue 18-byte header
- ✅ **Verified against the official Confluent serializers** — not just against itself ([conformance harness](conformance/README.md))
- 🚀 Bounded in-memory caching with thundering-herd coalescing and cancellation safety
- 🧬 Protobuf message-index paths **derived from the descriptor**, so they cannot drift from the `.proto`
- 🔌 Pluggable backend via the `SchemaRegistryClient` trait, usable both generically and as `dyn`
- 🎯 Feature-gated: the default feature set pulls in no transport stack at all

---

## ✨ Features

| Feature | Enables |
|---|---|
| *(none)* | 🔧 Core types, both wire codecs, traits, caching |
| `confluent` | 🌐 Confluent HTTP client + encoder, TLS via rustls + webpki-roots |
| `apicurio` | 🗂️ Native Apicurio Registry v3 HTTP client (`/apis/registry/v3/`) |
| `glue` | ☁️ AWS Glue SDK client, ZLIB compression via flate2 |
| `avro` | 🪶 Avro encode / decode via apache-avro, works with any `SchemaRegistryClient` |
| `json` | 📋 JSON Schema encode / decode, works with any `SchemaRegistryClient` |
| `protobuf` | 🧬 Protobuf encode / decode via prost, with descriptor-derived message-index paths |
| `native-tls-roots` | 🔒 Add the platform root store to the HTTPS trust anchors |

`avro`, `json`, `protobuf`, and `glue`'s wire codec are **independent of the transport features** — you can pair the Avro codec with an Apicurio client, or use the Glue framing with no AWS SDK at all.

---

## 🚀 Quick start

### 🌐 Confluent Schema Registry

```toml
# Cargo.toml
[dependencies]
schemreg = { version = "0.4", features = ["confluent"] }
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

```rust
use std::sync::Arc;
use bytes::Bytes;
use schemreg::{
    CachedSchemaRegistry, ConfluentSchemaEncoder, ConfluentSchemaRegistry, EncodeTarget,
    SchemaEncoder, SchemaType, SubjectNameStrategy, WireFormatDecoder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = ConfluentSchemaRegistry::builder()
        .url("https://registry.example.com")
        // .basic_auth("user", "password")
        .build()?;

    // Bounded in-memory cache (1 000 entries, FIFO eviction, coalesced misses).
    let cached = Arc::new(CachedSchemaRegistry::new(registry));

    // Producer: registers the schema on first send, then reuses the cached ID.
    let encoder = ConfluentSchemaEncoder::builder()
        .registry(Arc::clone(&cached))
        .schema(
            r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#,
            SchemaType::Avro,
        )
        .strategy(SubjectNameStrategy::TopicName)
        .build()?;

    let raw = Bytes::from_static(b"\x04\x08some-avro-payload");
    let framed = encoder.encode(raw, "orders", None, EncodeTarget::Value).await?;

    // Consumer: strips the header, reusing the same cache for schema lookups.
    let decoder = WireFormatDecoder::confluent(Arc::clone(&cached));
    let msg = decoder.decode(framed).await?;

    println!("decoded {} bytes as {:?}", msg.payload.len(), msg.schema_format);
    Ok(())
}
```

### ☁️ AWS Glue Schema Registry

```toml
[dependencies]
schemreg = { version = "0.4", features = ["glue"] }
```

```rust
use schemreg::{GlueCompression, GlueSchemaVersionId, decode_glue_wire_format, encode_glue_wire_format};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let version_id: GlueSchemaVersionId = "550e8400-e29b-41d4-a716-446655440000".parse()?;

    let framed = encode_glue_wire_format(version_id, b"avro bytes", GlueCompression::None)?;
    let (id, payload) = decode_glue_wire_format(&framed)?;

    assert_eq!(id, version_id);
    assert_eq!(payload, b"avro bytes");
    Ok(())
}
```

With real AWS credentials, use the SDK-backed client:

```rust
use aws_config::BehaviorVersion;
use schemreg::{AwsGlueSchemaRegistry, CachedGlueSchemaRegistry, GlueSchemaRegistryClient};

let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
let registry = CachedGlueSchemaRegistry::new(AwsGlueSchemaRegistry::from_config(&config));
registry.inner().health_check().await?;   // preflight: creds + network + IAM
```

---

## 🔬 Wire formats

### 🌐 Confluent — Avro and JSON Schema

```text
Byte offset  0        1                            5 …
             ┌────────┬────────────────────────────┬──────────────────────┐
             │  0x00  │     schema_id (u32 BE)     │  payload (N bytes)   │
             └────────┴────────────────────────────┴──────────────────────┘
             magic    │←──────── 4 bytes ─────────→│
```

### 🌐 Confluent — Protobuf

Protobuf inserts a **message-index array** between the header and the payload, identifying which message type in the `.proto` file was serialized:

```text
Byte offset  0        1              5              5+k …
             ┌────────┬──────────────┬──────────────┬──────────────────────┐
             │  0x00  │  schema_id   │ message-index│  payload (N bytes)   │
             └────────┴──────────────┴──────────────┴──────────────────────┘
```

Every integer in the array — **including the leading element count** — is ZigZag-encoded and then written as an unsigned LEB-128 varint, matching `ByteUtils.writeVarint` in the Confluent Java serde. The array `[0]` (the first top-level message, the overwhelmingly common case) is written as a single `0x00` byte.

| Message path | Encoded bytes | Derivation |
|---|---|---|
| `[0]` | `00` | mandated single-byte form |
| `[1]` | `02 02` | ZigZag(1)=2 count, ZigZag(1)=2 |
| `[2]` | `02 04` | ZigZag(1)=2 count, ZigZag(2)=4 |
| `[1, 0]` | `04 02 00` | ZigZag(2)=4 count, ZigZag(1)=2, ZigZag(0)=0 |

```rust
use schemreg::{decode_protobuf_message_indexes, decode_wire_format, encode_protobuf_wire_format};

let framed = encode_protobuf_wire_format(42u32, &[0], b"\x0a\x05hello");
let (id, after_header) = decode_wire_format(&framed)?;
let (indexes, payload_start) = decode_protobuf_message_indexes(after_header)?;

assert_eq!(indexes, vec![0]);
assert_eq!(&after_header[payload_start..], b"\x0a\x05hello");
```

These are not a reading of the spec — they are the literal bytes `confluent-kafka-python` produces, [captured as fixtures](conformance/README.md) and asserted in CI.

**Do not hand-write the index.** With the `protobuf` feature it is derived from the message descriptor, so it stays correct when someone reorders the `.proto`:

```rust,ignore
use schemreg::protobuf::{ProtobufSchemaDecoder, ProtobufSchemaEncoder};

let encoder = ProtobufSchemaEncoder::builder()
    .registry(registry.clone())
    .schema(PROTO_SOURCE)                        // the .proto text, registered as-is
    .descriptor(Order::default().descriptor())   // ← index path derived from here
    .build()?;

let framed = encoder.encode(&order, "orders", EncodeTarget::Value).await?;

// A Protobuf payload does not identify its own type: decoding an Invoice as an
// Order normally *succeeds* and returns a struct full of defaults. Guard it:
let decoder = ProtobufSchemaDecoder::new(registry)
    .with_expected_descriptor(&Order::default().descriptor())?;
let order: Order = decoder.decode(framed).await?;
```

See [`examples/protobuf_wire_format.rs`](examples/protobuf_wire_format.rs) for hex dumps of every shape, and [`examples/protobuf_roundtrip.rs`](examples/protobuf_roundtrip.rs) for the full codec.

### ☁️ AWS Glue Schema Registry

```text
Byte offset  0        1        2                   18 …
             ┌────────┬────────┬───────────────────┬──────────────────────┐
             │  0x03  │  comp  │ schema_version_id │  payload (N bytes)   │
             └────────┴────────┴───────────────────┴──────────────────────┘
             version  │  byte  │←──── 16 bytes ───→│
```

- **comp**: `0x00` = none, `0x05` = ZLIB
- **schema_version_id**: 128-bit UUID in big-endian (network) byte order

### 🔎 Auto-detection

`detect_wire_format` dispatches on the first byte and never guesses:

| First byte | Result |
|---|---|
| `0x00`, ≥ 5 bytes | `Confluent { schema_id, payload_offset }` |
| `0x00`, < 5 bytes | `InvalidConfluent` |
| `0x03`, ≥ 18 bytes, known compression byte | `Glue { version_id, compression, payload_offset }` |
| `0x03`, truncated or unknown compression byte | `InvalidGlue` |
| anything else (`0x01`, `0x02`, `0x04`, …) | `Unknown` |

`WireFormatDecoder` passes `Unknown` and `Invalid*` payloads through unchanged rather than dropping them, so a topic carrying a mix of framed and unframed records stays readable.

---

## 🔌 Custom backend

Any type implementing `SchemaRegistryClient` works as a backend. Only four methods are required; the rest default to `NotSupported`:

```rust
use std::sync::Arc;
use schemreg::{Result, Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion};

struct MyRegistry;

impl SchemaRegistryClient for MyRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> { todo!() }
    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> { todo!() }
    async fn get_schema_by_version(&self, subject: &str, v: SchemaVersion) -> Result<Arc<Schema>> { todo!() }
    async fn register_schema(
        &self, subject: &str, schema: &str,
        schema_type: SchemaType, references: &[SchemaReference],
    ) -> Result<SchemaId> { todo!() }
}

// Compose transparently with the cache:
use schemreg::CachedSchemaRegistry;
let cached = CachedSchemaRegistry::new(MyRegistry);
```

See [`examples/custom_backend.rs`](examples/custom_backend.rs) for a full working example.

### Generic or `dyn` — both work

`SchemaRegistryClient` uses native `async fn` in traits (RPITIT) for zero-cost monomorphized dispatch. For type erasure, use `DynSchemaRegistryClient`, which every `SchemaRegistryClient` implements automatically:

```rust
use std::sync::Arc;
use schemreg::{CachedSchemaRegistry, DynSchemaRegistryClient};

struct AppState {
    registry: Arc<dyn DynSchemaRegistryClient>,
}

// Erasure is a two-way door: `dyn DynSchemaRegistryClient` also implements
// `SchemaRegistryClient`, so an erased client goes straight back into generic code.
let erased: Arc<dyn DynSchemaRegistryClient> = Arc::new(MyRegistry);
let cached = CachedSchemaRegistry::new(erased);
```

> **Note** — both traits expose identically named methods. If you import both into one scope, disambiguate with `SchemaRegistryClient::get_schema_by_id(&client, id)` or `DynSchemaRegistryClient::get_schema_by_id(&client, id)`.

---

## ⚡ Cache behaviour

```rust
use schemreg::CachedSchemaRegistry;

let cached = CachedSchemaRegistry::with_max_entries(my_registry, 512);

// Pre-warm known schema IDs (16 concurrent fetches, no per-batch barrier).
// Failures are collected, not fatal: successful IDs stay cached.
if let Err(e) = cached.warm_cache([1u32, 2, 3]).await {
    for (id, err) in &e.failures {
        eprintln!("warm failed for {id}: {err}");
    }
}

cached.invalidate(2u32);          // drop one entry
cached.invalidate_subject("orders-value");  // drop everything for a subject
cached.clear_cache();             // drop everything
```

| Property | Guarantee |
|---|---|
| Schema-ID cache | Never expires — a registry never reassigns an ID |
| `get_latest_schema` | **Never** served from cache; always hits the backend, then populates the ID cache |
| Bound | Default 1 000 entries; oldest-inserted evicted on overflow |
| Coalescing | N concurrent cold misses for one ID ⇒ exactly one backend request |
| Cancellation | If the leader task is aborted, every waiter is woken with an error — never a hang |
| Invalidation race | A fetch that completes after an `invalidate()` is discarded, not resurrected |

The Avro, JSON, and Protobuf codecs cache parsed schemas and compiled validators with the same bounded, coalescing machinery — 32 consumers hitting a cold schema ID compile it once, not 32 times. Producer-side `subject → schema ID` maps are bounded too (`max_subject_cache_entries`).

Measured: a cache hit costs **14.3 ns regardless of schema size** (64 B to 64 KiB) because `Schema` holds `Arc<str>`; header decode is **1.6 ns** independent of payload size. See [docs/performance.md](docs/performance.md).

---

## 🔍 Format-agnostic decoding

```rust
use std::sync::Arc;
use schemreg::WireFormatDecoder;

let decoder = WireFormatDecoder::new()
    .with_confluent(Arc::clone(&cached_confluent))
    .with_glue(Arc::clone(&cached_glue));   // requires `glue`

let msg = decoder.decode(raw_bytes).await?;
println!("{:?} / {} bytes", msg.schema_format, msg.payload.len());

// Protobuf message-index is stripped and reported separately.
if let Some(path) = &msg.protobuf_message_indexes {
    println!("proto message path: {path:?}");
}
```

`WireFormatDecoder` also implements the object-safe `SchemaDecoder` trait, so it can be stored as `Arc<dyn SchemaDecoder>`.

---

## 🪶 Avro with schema evolution

By default the payload is decoded with the **writer** schema named by the wire header. Supply a **reader** schema to get Avro's full resolution — defaulted fields, dropped fields, promoted numeric types — matching the Confluent Java deserializer:

```rust
use schemreg::AvroSchemaDecoder;

let decoder = AvroSchemaDecoder::new(registry)
    .with_reader_schema(r#"{
        "type": "record", "name": "Order", "namespace": "com.example",
        "fields": [
            {"name": "id",  "type": "int"},
            {"name": "qty", "type": "int", "default": 7}
        ]
    }"#)?;

// A payload written before `qty` existed decodes with qty = 7.
let value = decoder.decode(framed).await?;
```

---

## 🛠️ `SchemaRegistryClient` API surface

Every implementation (`ConfluentSchemaRegistry`, `ApicurioSchemaRegistry`, custom backends) exposes the same methods. `CachedSchemaRegistry` adds caching and delegates the rest.

| Method | Required? | Description |
|---|---|---|
| `get_schema_by_id(id)` | ✅ | Fetch a schema by its globally unique integer ID |
| `get_latest_schema(subject)` | ✅ | Fetch the most recent version under a subject |
| `get_schema_by_version(subject, v)` | ✅ | Fetch a specific version |
| `register_schema(subject, schema, type, refs)` | ✅ | Register (idempotent — returns the existing ID) |
| `check_compatibility(subject, schema, type, refs)` | ⬜ | Test compatibility against the current version |
| `check_compatible(subject, schema, type)` | ⬜ | Convenience alias with no references |
| `delete_subject(subject, permanent)` | ⬜ | Delete all versions of a subject |
| `get_subjects()` | ⬜ | List all subjects (paginated internally on Apicurio) |
| `get_versions(subject)` | ⬜ | List version numbers for a subject |
| `health_check()` | ⬜ | Lightweight liveness probe (backend-specific endpoint) |
| `set_compatibility(subject, level)` | ⬜ | Set the per-subject compatibility policy |
| `get_compatibility(subject)` | ⬜ | Read the compatibility policy |

Methods marked ⬜ default to `Err(SchemaRegError::NotSupported)`. That variant is never `is_retryable()`, so a caller's retry loop distinguishes "this backend can't do that" from "the backend is down".

### Compatibility levels

```rust
use schemreg::CompatibilityLevel;

registry.set_compatibility("orders-value", CompatibilityLevel::BackwardTransitive).await?;
let level = registry.get_compatibility("orders-value").await?;
```

Variants: `Backward`, `BackwardTransitive`, `Forward`, `ForwardTransitive`, `Full`, `FullTransitive`, `None`. On Confluent, an empty subject (`""`) targets the global default.

### Subject name strategies

| Strategy | Produces |
|---|---|
| `TopicName` *(default)* | `{topic}-key` / `{topic}-value` |
| `RecordName` | `{record_name}` |
| `TopicRecordName` | `{topic}-{record_name}` |
| `ApicurioGroupRecordName { group_id }` | `{group_id}/{record_name}` |
| `Custom(Arc<dyn Fn…>)` | anything you want |

### 12-factor configuration

```rust
// SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_USERNAME/PASSWORD or SCHEMA_REGISTRY_BEARER_TOKEN
let registry = ConfluentSchemaRegistryBuilder::from_env()?.build()?;

// APICURIO_REGISTRY_URL, APICURIO_REGISTRY_USERNAME/PASSWORD or APICURIO_REGISTRY_BEARER_TOKEN
let registry = ApicurioSchemaRegistryBuilder::from_env()?.build()?;
```

---

## 🔄 Retry, resilience, and error classification

Retry is built in, and configurable:

| Scenario | Behaviour |
|---|---|
| HTTP 429 | Retried; `Retry-After` honoured (delta-seconds **and** HTTP-date) |
| HTTP 5xx | Retried; `Retry-After` honoured, else exponential back-off |
| Network errors | Retried (connection reset, timeout, DNS) |
| Default budget | 3 retries, 100 ms base, doubling, capped at 60 s |
| Jitter | **Equal jitter** by default — without it every client retries on the same schedule and reconverges into synchronised waves |
| `Retry-After` | Never jittered or shortened; still clamped to `max_backoff` so a hostile `Retry-After: 86400` cannot wedge you |
| Redirects | At most 3; `Authorization` dropped on cross-origin redirects |

```rust
use std::time::Duration;
use schemreg::RetryPolicy;

let registry = ConfluentSchemaRegistry::builder()
    .url("https://registry.example.com")
    .retry_policy(RetryPolicy::new().max_retries(5).base_backoff(Duration::from_millis(50)))
    // Coalescing collapses concurrent misses for the *same* ID. This bounds the
    // other case: a cold start fanning out to thousands of *distinct* IDs.
    .max_concurrent_requests(32)
    .connect_timeout(Duration::from_secs(3))
    .request_timeout(Duration::from_secs(30))
    .build()?;
```

Use `RetryPolicy::none()` when the calling layer already retries, so the two do not multiply.

`SchemaRegError::is_retryable()` is the contract for your own retry loop, and it is **uniform across backends** — including AWS Glue, where SDK errors are classified by service code rather than collapsed into one transport variant:

```rust
match registry.get_schema_by_id(id).await {
    Ok(schema) => { /* … */ }
    Err(e) if e.is_not_found()  => { /* permanent: the schema does not exist */ }
    Err(e) if e.is_auth_error() => { /* permanent: rotate credentials */ }
    Err(e) if e.is_retryable()  => { /* transient: back off and try again */ }
    Err(e) => return Err(e),
}
```

---

## 🔐 Security posture

| Control | Behaviour |
|---|---|
| Credentials in memory | Held in `zeroize::Zeroizing`; wiped on drop |
| Credentials in logs | `Debug` renders `basic(***)` / `bearer(***)`; never the value |
| Credentials in URLs | `user:pass@host` is rejected at construction |
| Auth over cleartext | Basic/Bearer over `http://` is a hard error off-loopback; permitted with a warning on `localhost` / `127.0.0.0/8` / `::1`, the standard local-dev setup |
| TLS | rustls only; no `danger_accept_invalid_certs` path exists, and `openssl` / `native-tls` are banned by `cargo deny` |
| Path traversal | Every subject is validated before URL interpolation — `.`/`..` segments rejected, including percent-encoded forms that a double-decoding proxy would recover |
| Request body cap | 4 MiB |
| Response body cap | 16 MiB, enforced from `Content-Length` *and* while streaming |
| Decompression bomb | Glue ZLIB output capped at 128 MiB |
| Cache memory | Every cache is bounded; no unbounded map on any message-driven path |
| Retry amplification | Bounded attempts, capped delay, jittered back-off |
| Remote `$ref` | `jsonschema` is built without `resolve-http`, so schema compilation cannot make outbound requests (no SSRF) |
| Supply chain | `cargo deny` (advisories, licences, bans, sources) and `cargo audit` run in CI, plus nightly |

### TLS backend

The crate uses rustls with the **ring** provider via `reqwest`'s `rustls-tls-webpki-roots`. To use `aws-lc-rs` instead, configure it in your application — install it as the process-default provider before the first request and depend on `reqwest` with a `-no-provider` feature. `schemreg` deliberately does **not** expose an `aws-lc-rs` feature: enabling the crate alone does not change the provider `reqwest` selects, so such a feature would be a costly no-op.

---

## 📖 Examples

| Example | Description |
|---|---|
| [`confluent_encode_decode`](examples/confluent_encode_decode.rs) | 🌐 Encode→decode round-trip against an in-memory stub registry |
| [`protobuf_wire_format`](examples/protobuf_wire_format.rs) | 🧬 Protobuf message-index framing, with hex dumps and reference bytes |
| [`protobuf_roundtrip`](examples/protobuf_roundtrip.rs) | 🧬 Full Protobuf codec with descriptor-derived paths and wrong-type rejection |
| [`avro_roundtrip`](examples/avro_roundtrip.rs) | 🪶 Avro encode → Confluent framing → decode, plus serde |
| [`json_roundtrip`](examples/json_roundtrip.rs) | 📋 JSON Schema validation on encode and decode |
| [`glue_roundtrip`](examples/glue_roundtrip.rs) | ☁️ Glue framing, with and without ZLIB |
| [`custom_backend`](examples/custom_backend.rs) | 🔌 Implementing `SchemaRegistryClient` + cache + `WireFormatDecoder` |
| [`apicurio_roundtrip`](examples/apicurio_roundtrip.rs) | 🗂️ Apicurio v3 group-scoped round-trip with a mock registry |

```bash
cargo run --example protobuf_wire_format
cargo run --example protobuf_roundtrip      --features protobuf
cargo run --example confluent_encode_decode --features confluent
cargo run --example avro_roundtrip          --features avro
cargo run --example json_roundtrip          --features json
cargo run --example glue_roundtrip          --features glue
cargo run --example apicurio_roundtrip      --features apicurio
cargo run --example custom_backend
```

---

## 🧭 Registry support

| Registry | Wire format | Status |
|---|---|---|
| Confluent Schema Registry | `0x00` + 4-byte ID (+ Protobuf message-index) | ✅ Native client |
| Karapace | Confluent-compatible REST | ✅ Via `ConfluentSchemaRegistry` |
| Apicurio Registry v3 | Confluent framing; group + artifact addressing | ✅ Native v3 client |
| Apicurio (compat mode) | Confluent framing and REST API | ✅ Via `ConfluentSchemaRegistry` |
| AWS Glue Schema Registry | `0x03` + compression + 16-byte UUID | ✅ Native SDK client |
| Azure Event Hubs Schema Registry | Schema ID in a **header**, no payload prefix | ⬜ Out of scope — see [FINDINGS.md](FINDINGS.md) §2 |
| Buf Schema Registry | None — build-time only | ⛔ No runtime framing to implement |

---

## 📚 Documentation

| Where | What |
|---|---|
| [docs.rs/schemreg](https://docs.rs/schemreg) | API reference |
| [docs/wire-formats.md](docs/wire-formats.md) | Exactly what bytes go on the topic |
| [docs/backends.md](docs/backends.md) | Choosing between Confluent, Apicurio, and Glue |
| [docs/security.md](docs/security.md) | Threat model, controls, and what is deliberately not defended |
| [docs/performance.md](docs/performance.md) | Measured numbers, allocation profile, cache sizing |
| [docs/migration-0.4.md](docs/migration-0.4.md) | Upgrading from 0.3.x |
| [docs/testing.md](docs/testing.md) | What each test layer is for |
| [CHANGELOG.md](CHANGELOG.md) | Release notes and breaking changes |
| [FINDINGS.md](FINDINGS.md) | Decision-grade architecture, security, and release review |
| [conformance/](conformance/README.md) | The cross-language conformance harness |

---

## 📄 License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
