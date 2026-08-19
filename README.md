# 🗂️ schemreg

[![Crates.io](https://img.shields.io/crates/v/schemreg.svg)](https://crates.io/crates/schemreg)
[![docs.rs](https://docs.rs/schemreg/badge.svg)](https://docs.rs/schemreg)
[![CI](https://github.com/hupe1980/schemreg/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/schemreg/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#-license)
[![MSRV: 1.88](https://img.shields.io/badge/MSRV-1.88-blue.svg)](https://releases.rs/docs/1.88.0/)

**Kafka wire formats, exactly.** An async Rust schema-registry client for
**Confluent Schema Registry** (and Karapace, Redpanda), **Apicurio Registry v3**,
and **AWS Glue Schema Registry**, with Avro, JSON Schema, and Protobuf codecs.

📖 **[Guides and documentation →](https://hupe1980.github.io/schemreg/)** ·
[API reference](https://docs.rs/schemreg) ·
[Changelog](CHANGELOG.md)

---

- ⚡ **Zero-copy framing** — Confluent **v0** (schema ID) *and* **v1** (schema GUID), Protobuf message-index, Glue 18-byte header. Decoding costs 1.6 ns regardless of payload size.
- 📬 **Schema IDs in Kafka headers** (`__key_schema_id` / `__value_schema_id`), the placement Confluent Platform 8 introduced.
- 🔐 **Read-only producers** — `SchemaResolution::LookupOnly` frames records without ever writing to the registry. The setting the Java serdes spell `auto.register.schemas=false`.
- ✅ **Verified against the official serializers**, not just against itself — bytes from `confluent-kafka-python`, decoded *and* re-encoded byte-identically ([conformance harness](conformance/README.md)).
- 🚀 **Bounded caching** with thundering-herd coalescing and cancellation safety. No unbounded map on any message-driven path.
- 🧬 **Protobuf message-index paths derived from the descriptor**, so they cannot drift from the `.proto`.
- 🔗 **Schema references resolved transitively**, for both Avro and JSON Schema.
- 🔌 **Pluggable backend** via the `SchemaRegistryClient` trait, usable generically *and* as `dyn`.

## ✨ Features

Everything is opt-in; the default feature set pulls in no transport stack at all.

| Feature | Enables |
|---|---|
| *(none)* | 🔧 Core types, both wire codecs, traits, caching |
| `confluent` | 🌐 Confluent HTTP client + framing encoder, TLS via rustls |
| `apicurio` | 🗂️ Native Apicurio Registry v3 client (`/apis/registry/v3/`) |
| `glue` | ☁️ AWS Glue SDK client, ZLIB compression |
| `avro` | 🪶 Avro encode / decode, with transitive schema-reference resolution |
| `json` | 📋 JSON Schema validate / serialise, with cross-subject `$ref` resolution |
| `protobuf` | 🧬 Protobuf encode / decode, with descriptor-derived message-index paths |
| `native-tls-roots` | 🔒 Add the platform root store to the HTTPS trust anchors |

The codecs are independent of the transport features — pair the Avro codec with
an Apicurio client, or use the Glue framing with no AWS SDK at all.

## 🚀 Quick start

```sh
cargo add schemreg --features confluent,avro
```

The wire codecs need nothing but the core crate:

```rust
use schemreg::{SchemaId, decode_wire_format, encode_wire_format};

// Producer: frame a payload you have already serialised.
let framed = encode_wire_format(42u32, b"serialised-avro-bytes");
assert_eq!(&framed[..5], &[0x00, 0, 0, 0, 42]);

// Consumer: recover the identifier and the payload, with no copy.
let (key, payload) = decode_wire_format(&framed)?;
assert_eq!(key.as_id(), Some(SchemaId::new(42)));
assert_eq!(payload, b"serialised-avro-bytes");
# Ok::<(), schemreg::SchemaRegError>(())
```

With a registry, and a producer that never writes to it:

```rust,ignore
use std::sync::Arc;
use schemreg::{
    AvroSchemaEncoder, CachedSchemaRegistry, ConfluentSchemaRegistry,
    EncodeTarget, SchemaResolution, WireFormatDecoder,
};

let registry = ConfluentSchemaRegistry::builder()
    .url("https://registry.example.com")
    .basic_auth("user", "password")
    .build()?;

// Bounded (1 000 entries), coalescing, cancellation-safe.
let cached = Arc::new(CachedSchemaRegistry::new(registry));

let encoder = AvroSchemaEncoder::builder()
    .registry(Arc::clone(&cached))
    .schema(ORDER_SCHEMA)
    .resolution(SchemaResolution::LookupOnly)   // needs only Subject:Read
    .build()?;

let framed = encoder.encode(order, "orders", EncodeTarget::Value).await?;

// Consumer: one path for whichever framing the producer chose.
let decoder = WireFormatDecoder::confluent(cached);
let message = decoder.decode(framed).await?;
```

→ [Quick start guide](https://hupe1980.github.io/schemreg/docs/quickstart/)

## 🔬 What goes on the wire

A Kafka record names its schema in whichever way its **producer** chose. A
consumer cannot know in advance, so `decode_wire_format` returns a `SchemaKey`
rather than committing to either — hand it to `get_schema_by_key` and the right
lookup happens.

| Framing | Bytes | Introduced |
|---|---|---|
| Confluent **v0** | `0x00` + 4-byte schema ID | every Confluent Platform release |
| Confluent **v1** | `0x01` + 16-byte schema GUID | Confluent Platform 8 |
| Kafka header | the same prefix in `__value_schema_id`; payload unframed | Confluent Platform 8 |
| Protobuf | any of the above + a ZigZag varint message-index array | — |
| AWS Glue | `0x03` + compression byte + 16-byte version UUID | — |

```rust
use schemreg::{SchemaGuid, decode_wire_format, encode_wire_format};

let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
let framed = encode_wire_format(guid, b"payload");
assert_eq!(framed[0], 0x01);

let (key, _) = decode_wire_format(&framed)?;
assert_eq!(key.as_guid(), Some(guid));
assert_eq!(key.as_id(), None);   // a GUID frame names no numeric ID
# Ok::<(), schemreg::SchemaRegError>(())
```

→ [Wire formats, byte by byte](https://hupe1980.github.io/schemreg/docs/wire-formats/)

## 🎛️ Producer configuration

Two builder settings decide what a producer does before it writes a byte, on
every encoder.

| `SchemaResolution` | Registry permission | Use it when |
|---|---|---|
| `AutoRegister` *(default)* | `Subject:Write` | the application owns its schemas |
| `LookupOnly` | `Subject:Read` | **CI owns the schemas** |
| `UseLatestVersion` | `Subject:Read` | schemas evolve centrally and producers follow |

| `Framing` | Prefix | Requires |
|---|---|---|
| `SchemaId` *(default)* | `0x00` + 4 bytes | anything Confluent-compatible |
| `SchemaGuid` | `0x01` + 16 bytes | Confluent Platform 8+ |

The default writes to your registry — the same default the Confluent Java serdes
ship, and the same one that quietly creates a production version the first time a
developer's local schema drifts. `LookupOnly` turns that into a startup failure
with an error that is **not** retryable, so a retry loop stops rather than spins.

Placement is a per-call choice: `encode` puts the prefix in front of the payload,
`encode_with_header` returns it as a Kafka header value with the payload left
unframed.

→ [Producer configuration](https://hupe1980.github.io/schemreg/docs/producers/)

## 🧭 Registry support

| Registry | Wire format | Status |
|---|---|---|
| Confluent Schema Registry | `0x00`, `0x01`, or a Kafka header | ✅ Native client |
| Karapace | Confluent-compatible REST, v0 framing | ✅ Via `ConfluentSchemaRegistry` |
| Redpanda Schema Registry | Confluent-compatible REST, v0 framing | ✅ Via `ConfluentSchemaRegistry` |
| Apicurio Registry v3 | Confluent framing; group + artifact addressing | ✅ Native v3 client |
| Apicurio (compat mode) | Confluent framing and REST API | ✅ Via `ConfluentSchemaRegistry` |
| AWS Glue Schema Registry | `0x03` + compression + 16-byte UUID | ✅ Native SDK client |
| Azure Event Hubs Schema Registry | Schema ID out-of-band, own REST API | ⬜ Out of scope |
| Buf Schema Registry | None — build-time only | ⛔ No runtime framing to implement |

Any type implementing `SchemaRegistryClient` works as a backend. Only four
methods are required; the rest default to `NotSupported`, which is never
`is_retryable()`.

→ [Choosing a backend](https://hupe1980.github.io/schemreg/docs/backends/)

## 🔗 Using it with a Kafka client

`schemreg` produces and consumes `Bytes` — wiring them to a broker is the
client's job. With [krafka](https://github.com/hupe1980/krafka), a pure-Rust
async Kafka client, that is a newtype bridging `PayloadEncoder` onto krafka's
`Serializer` hook:

```rust,ignore
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .value_serializer(Arc::new(SchemaSerializer(encoder)))
    .build()
    .await?;
```

→ [Kafka integration — the adapter, typed codecs, and header framing](https://hupe1980.github.io/schemreg/docs/kafka-integration/)

## 📖 Examples

Every example runs against an in-memory stub — no live registry needed — and all
of them are executed in CI.

| Example | Description |
|---|---|
| [`schema_resolution`](examples/schema_resolution.rs) | 🎛️ Auto-register vs lookup-only vs latest-version, v0 vs v1 framing, header placement |
| [`schema_guid_and_headers`](examples/schema_guid_and_headers.rs) | 🆔 The three Confluent framings, byte by byte |
| [`confluent_encode_decode`](examples/confluent_encode_decode.rs) | 🌐 Encode→decode round-trip against a stub registry |
| [`protobuf_wire_format`](examples/protobuf_wire_format.rs) | 🧬 Message-index framing, with hex dumps |
| [`protobuf_roundtrip`](examples/protobuf_roundtrip.rs) | 🧬 Descriptor-derived paths and wrong-type rejection |
| [`avro_roundtrip`](examples/avro_roundtrip.rs) | 🪶 Avro encode → framing → decode, plus serde |
| [`json_roundtrip`](examples/json_roundtrip.rs) | 📋 JSON Schema validation on encode and decode |
| [`glue_roundtrip`](examples/glue_roundtrip.rs) | ☁️ Glue framing, with and without ZLIB |
| [`apicurio_roundtrip`](examples/apicurio_roundtrip.rs) | 🗂️ Apicurio v3 group-scoped round-trip |
| [`custom_backend`](examples/custom_backend.rs) | 🔌 Implementing `SchemaRegistryClient` from scratch |

```sh
cargo run --example schema_resolution --features confluent
```

## 📚 Documentation

| Where | What |
|---|---|
| [hupe1980.github.io/schemreg](https://hupe1980.github.io/schemreg/) | Guides: wire formats, producers, codecs, Kafka integration, caching, backends, resilience, security, performance, testing |
| [docs.rs/schemreg](https://docs.rs/schemreg) | API reference |
| [CHANGELOG.md](CHANGELOG.md) | Release notes and breaking changes |
| [Migrating to 0.5](https://hupe1980.github.io/schemreg/docs/migrating-0-5/) | Upgrading from 0.4.x |
| [conformance/](conformance/README.md) | The cross-language conformance harness |

## 🤝 Contributing

```sh
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --check

zola --root site serve      # the documentation site, at http://127.0.0.1:1111
```

Issues and pull requests are welcome. The
[testing strategy](https://hupe1980.github.io/schemreg/docs/testing/) explains
what each layer is for, and where a new test belongs.

## 📄 License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
