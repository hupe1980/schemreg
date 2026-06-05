# 🗂️ schemreg

[![Crates.io](https://img.shields.io/crates/v/schemreg.svg)](https://crates.io/crates/schemreg)
[![docs.rs](https://docs.rs/schemreg/badge.svg)](https://docs.rs/schemreg)
[![CI](https://github.com/hupe1980/schemreg/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/schemreg/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![MSRV: 1.88](https://img.shields.io/badge/MSRV-1.88-blue.svg)](https://releases.rs/docs/1.88.0/)

Async schema registry client for **Confluent Schema Registry** (and Karapace), **Apicurio Registry v3**, and **AWS Glue Schema Registry**, with:

- ⚡ Zero-copy wire-format encode / decode (Confluent 5-byte header, Glue 18-byte header)
- 🚀 Transparent in-memory caching with thundering-herd coalescing
- 🔌 Pluggable backend via the `SchemaRegistryClient` trait
- 🎯 Feature-gated: pull in only what you need

---

## ✨ Features

| Feature | Enables |
|---|---|
| *(none)* | 🔧 Core types, wire format helpers, traits, Glue wire format |
| `confluent` | 🌐 Confluent HTTP client, encoder, decoder, TLS via rustls + webpki-roots |
| `apicurio` | 🗂️ Native Apicurio Registry v3 HTTP client (`ApicurioSchemaRegistry`) |
| `glue` | ☁️ AWS Glue SDK client (`AwsGlueSchemaRegistry`), ZLIB compression via flate2 |
| `avro` | 🪶 Avro encode / decode via apache-avro, works with any `SchemaRegistryClient` |
| `json` | 📋 JSON Schema encode / decode, works with any `SchemaRegistryClient` |
| `native-tls-roots` | 🔒 rustls-native-certs (implies `confluent`) |
| `aws-lc-rs` | 🔑 AWS LC crypto backend instead of ring (implies `confluent`) |

---

## 🚀 Quick start

### 🌐 Confluent Schema Registry

```toml
# Cargo.toml
[dependencies]
schemreg = { version = "0.3", features = ["confluent"] }
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

```rust
use std::sync::Arc;
use schemreg::{
    CachedSchemaRegistry, EncodeTarget, SchemaType, SubjectNameStrategy,
    confluent::{ConfluentSchemaEncoder, ConfluentSchemaRegistry},
    decoder::WireFormatDecoder,
    traits::SchemaEncoder,
};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the HTTP client.
    let registry = ConfluentSchemaRegistry::builder()
        .url("http://localhost:8081")
        // .basic_auth("user", "password")
        .build()?;

    // Wrap with a 1 000-entry LRU cache (default).
    let cached = Arc::new(CachedSchemaRegistry::new(registry));

    // Producer side: register schema on first send, then reuse cached ID.
    let encoder = ConfluentSchemaEncoder::builder()
        .registry(&*cached)
        .schema(r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#, SchemaType::Avro)
        .strategy(SubjectNameStrategy::TopicName)
        .build()?;

    let raw_bytes = Bytes::from_static(b"\x04\x08some-avro-payload");
    let framed = encoder.encode(raw_bytes, "orders", None, EncodeTarget::Value).await?;

    // Consumer side: strip the header (uses the same cached registry for schema lookup).
    let decoder = WireFormatDecoder::confluent(Arc::clone(&cached));
    let msg = decoder.decode(framed).await?;

    println!("Decoded {} bytes", msg.payload.len());
    Ok(())
}
```

### ☁️ AWS Glue Schema Registry

```toml
[dependencies]
schemreg = { version = "0.3", features = ["glue"] }
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

```rust
use schemreg::{
    GlueCompression, GlueDataFormat, GlueSchemaVersionId,
    encode_glue_wire_format, decode_glue_wire_format,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let version_id: GlueSchemaVersionId =
        "550e8400-e29b-41d4-a716-446655440000".parse()?;

    // Encode — adds the 18-byte Glue header.
    let framed = encode_glue_wire_format(version_id, b"avro bytes", GlueCompression::None)?;

    // Decode — strips the header.
    let (id, payload) = decode_glue_wire_format(&framed)?;
    assert_eq!(id, version_id);
    assert_eq!(payload, b"avro bytes");
    Ok(())
}
```

With the `glue` feature and real AWS credentials you can use the high-level `AwsGlueSchemaRegistry` client:

```rust
use aws_config::BehaviorVersion;
use schemreg::glue::AwsGlueSchemaRegistry;

let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
let registry = AwsGlueSchemaRegistry::from_config(&config);
```

---

## 🔬 Wire formats

### 🌐 Confluent Schema Registry

```
Byte offset  0        1        2        3        4        5 …
             ┌────────┬────────────────────────────┬──────────────────────┐
             │ 0x00   │     schema_id (u32 BE)      │  payload (N bytes)   │
             └────────┴────────────────────────────┴──────────────────────┘
             magic    │←──────── 4 bytes ──────────→│
```

### ☁️ AWS Glue Schema Registry

```
Byte offset  0        1        2                   18        18 …
             ┌────────┬────────┬────────────────────┬──────────────────────┐
             │ 0x03   │  comp  │  schema_version_id  │  payload (N bytes)   │
             └────────┴────────┴────────────────────┴──────────────────────┘
             version  │byte    │←───── 16 bytes ─────→│
```

- **comp**: `0x00` = none, `0x05` = ZLIB
- **schema_version_id**: 128-bit UUID stored as big-endian bytes

---

## 🔌 Custom backend

Any struct that implements `SchemaRegistryClient` can be used as the backend:

```rust
use std::sync::Arc;
use schemreg::{Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion};
use schemreg::error::Result;

struct MyRegistry { /* ... */ }

impl SchemaRegistryClient for MyRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        todo!()
    }
    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        todo!()
    }
    async fn get_schema_by_version(&self, subject: &str, version: SchemaVersion) -> Result<Arc<Schema>> {
        todo!()
    }
    async fn register_schema(
        &self, subject: &str, schema: &str,
        schema_type: SchemaType, references: &[SchemaReference],
    ) -> Result<SchemaId> {
        todo!()
    }
}

// Wrap with cache transparently:
use schemreg::CachedSchemaRegistry;
let cached = CachedSchemaRegistry::new(MyRegistry { /* ... */ });
```

See [`examples/custom_backend.rs`](examples/custom_backend.rs) for a full working example.

---

## ⚡ Cache management

`CachedSchemaRegistry` exposes the `AnySchemaCache` trait for lifecycle control:

```rust
use schemreg::CachedSchemaRegistry;

let cached = CachedSchemaRegistry::with_max_entries(my_registry, 512);

// Pre-warm for known schema IDs (avoids cold-miss latency on startup).
cached.warm_cache([1u32, 2u32, 3u32]).await?;

println!("cached: {}", cached.cache_len());

// Invalidate a single stale entry.
cached.invalidate(2u32);

// Wipe everything.
cached.clear_cache();
```

---

## 🔍 Format-agnostic decoding

`WireFormatDecoder` auto-detects the wire format and dispatches to the correct backend:

```rust
use std::sync::Arc;
use schemreg::decoder::WireFormatDecoder;

let decoder = WireFormatDecoder::new()
    .with_confluent(Arc::clone(&cached_confluent_registry))
    .with_glue(Arc::clone(&cached_glue_registry));  // requires `glue` feature

let msg = decoder.decode(raw_bytes).await?;
println!("format: {:?}", msg.schema_format);  // Avro / Protobuf / Json / Unknown
println!("payload: {} bytes", msg.payload.len());
```

---

## 📖 Examples

| Example | Description |
|---|---|
| [`confluent_encode_decode`](examples/confluent_encode_decode.rs) | 🌐 Full encode→decode round-trip with an in-memory stub registry |
| [`avro_roundtrip`](examples/avro_roundtrip.rs) | 🪶 End-to-end Avro encode → Confluent wire format → decode |
| [`glue_roundtrip`](examples/glue_roundtrip.rs) | ☁️ Glue wire format encode / decode, optional ZLIB compression |
| [`custom_backend`](examples/custom_backend.rs) | 🔌 Implementing `SchemaRegistryClient` + cache + WireFormatDecoder |
| [`apicurio_roundtrip`](examples/apicurio_roundtrip.rs) | 🗂️ Apicurio Registry v3 encode→decode round-trip with a mock registry |

```bash
cargo run --example confluent_encode_decode --features confluent
cargo run --example avro_roundtrip --features avro
cargo run --example glue_roundtrip --features glue
cargo run --example custom_backend
cargo run --example apicurio_roundtrip --features apicurio
```

---

## �️ Full API surface — `SchemaRegistryClient` trait

All implementations (`ConfluentSchemaRegistry`, `ApicurioSchemaRegistry`, and any custom backend) expose the same methods through the `SchemaRegistryClient` trait. `CachedSchemaRegistry` adds transparent caching and delegates all calls to the inner client.

| Method | Description |
|---|---|
| `get_schema_by_id(id)` | Fetch schema by its globally unique integer ID |
| `get_latest_schema(subject)` | Fetch the most recently registered version under a subject |
| `get_schema_by_version(subject, version)` | Fetch a specific version under a subject |
| `register_schema(subject, schema, type, refs)` | Register a new schema (idempotent — returns existing ID if already registered) |
| `check_compatibility(subject, schema, type, refs)` | Test whether a schema is compatible with the currently registered version |
| `check_compatible(subject, schema, type)` | Convenience alias for `check_compatibility` with no schema references |
| `delete_subject(subject, permanent)` | Delete all versions of a subject (`permanent = true` for hard delete) |
| `get_subjects()` | List all registered subjects |
| `get_versions(subject)` | List all registered version numbers for a subject |
| `health_check()` | Probe the registry for connectivity (lightweight — `GET /subjects?limit=1`) |
| `set_compatibility(subject, level)` | Set the per-subject compatibility policy |
| `get_compatibility(subject)` | Get the current compatibility policy for a subject |

### Compatibility levels

`CompatibilityLevel` supports all Confluent / Apicurio policies:

```rust
use schemreg::CompatibilityLevel;

registry.set_compatibility("orders-value", CompatibilityLevel::BackwardTransitive).await?;
let level = registry.get_compatibility("orders-value").await?;
```

Available variants: `Backward`, `BackwardTransitive`, `Forward`, `ForwardTransitive`, `Full`, `FullTransitive`, `None`.

### 12-Factor / environment variable configuration

Both `ConfluentSchemaRegistryBuilder` and `ApicurioSchemaRegistryBuilder` support `from_env()`:

```rust
// Reads SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_USERNAME/PASSWORD or SCHEMA_REGISTRY_BEARER_TOKEN
let registry = ConfluentSchemaRegistryBuilder::from_env()?.build()?;

// Reads APICURIO_REGISTRY_URL, APICURIO_REGISTRY_USERNAME/PASSWORD or APICURIO_REGISTRY_BEARER_TOKEN
let registry = ApicurioSchemaRegistryBuilder::from_env()?.build()?;
```

---

## 🔄 Retry and resilience

`schemreg` has **built-in retry logic** for all HTTP requests — no extra configuration is needed:

| Scenario | Behavior |
|---|---|
| HTTP 429 (rate limited) | Retried; `Retry-After` header is honored (server-dictated delay) |
| HTTP 5xx (server error) | Retried with exponential backoff |
| Network errors | Retried (connection reset, timeout, DNS) |
| Max retries | 3 attempts; final error propagated |
| Backoff | 100 ms base, doubles per attempt, capped at 60 s |

No configuration is needed. To set independent connection and request timeouts:

```rust
use std::time::Duration;
use schemreg::confluent::ConfluentSchemaRegistryBuilder;

let registry = ConfluentSchemaRegistryBuilder::default()
    .url("https://registry.example.com")
    .connect_timeout(Duration::from_secs(3))    // TCP connection timeout
    .request_timeout(Duration::from_secs(30))   // full request timeout
    .pool_max_idle_per_host(10)                 // max idle connections per host
    .build()?;
```

---

## �📄 License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
