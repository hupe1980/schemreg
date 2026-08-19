+++
title = "Quick start"
description = "Install schemreg, frame a Kafka record with its schema ID, and decode it again — with a Confluent Schema Registry, AWS Glue, or no registry at all."
weight = 1
+++

## Install

Everything is opt-in. The default feature set pulls in no transport stack at
all, so pick the backend and the codec you need.

```sh
cargo add schemreg --features confluent,avro
```

| Feature | Adds |
|---|---|
| *(none)* | Core types, both wire codecs, traits, caching |
| `confluent` | Confluent Schema Registry HTTP client + framing encoder |
| `apicurio` | Native Apicurio Registry v3 client (`/apis/registry/v3/`) |
| `glue` | AWS Glue SDK client, plus ZLIB compression |
| `avro` | Avro encode/decode with transitive schema-reference resolution |
| `json` | JSON Schema validate/serialise, with cross-subject `$ref` resolution |
| `protobuf` | Protobuf encode/decode with descriptor-derived message-index paths |
| `native-tls-roots` | Trust the platform root store in addition to webpki roots |

The codecs are independent of the transport features: pair the Avro codec with
an Apicurio client, or use the Glue framing with no AWS SDK at all.

Minimum supported Rust version: **1.88**.

## Frame and unframe, with no registry

The wire codecs need nothing but the core crate. This is the whole of the
Confluent v0 format:

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

Decoding returns a [`SchemaKey`] rather than a bare ID because the *producer*
chooses the wire format version — see [Wire formats](@/docs/wire-formats.md).

[`SchemaKey`]: https://docs.rs/schemreg/latest/schemreg/types/enum.SchemaKey.html

## With a Confluent-compatible registry

```rust,ignore
use std::sync::Arc;

use bytes::Bytes;
use schemreg::{
    CachedSchemaRegistry, ConfluentSchemaEncoder, ConfluentSchemaRegistry, EncodeTarget,
    PayloadEncoder, SchemaResolution, SchemaType, WireFormatDecoder,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = ConfluentSchemaRegistry::builder()
        .url("https://registry.example.com")
        .basic_auth("user", "password")
        .build()?;

    // Bounded (1 000 entries), coalescing, cancellation-safe.
    let cached = Arc::new(CachedSchemaRegistry::new(registry));

    let encoder = ConfluentSchemaEncoder::builder()
        .registry(Arc::clone(&cached))
        .schema(
            r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#,
            SchemaType::Avro,
        )
        // The default registers the schema. This one only ever reads.
        .resolution(SchemaResolution::LookupOnly)
        .build()?;

    let raw = Bytes::from_static(b"\x04\x08some-avro-payload");
    let framed = encoder.encode(raw, "orders", None, EncodeTarget::Value).await?;

    // Consumer: strips the frame, reusing the same cache for schema lookups.
    let decoder = WireFormatDecoder::confluent(cached);
    let message = decoder.decode(framed).await?;

    println!("{} bytes as {:?}", message.payload.len(), message.schema_format);
    Ok(())
}
```

The encoder here frames bytes you serialised yourself. To serialise *and* frame
in one step, use a typed codec — see [Codecs](@/docs/codecs.md).

Next: wire it to a broker with [a Kafka client](@/docs/kafka-integration.md).

## With AWS Glue

The Glue codec is available without the `glue` feature; only the AWS SDK client
and ZLIB compression need it.

```rust
use schemreg::{GlueCompression, GlueSchemaVersionId, decode_glue_wire_format, encode_glue_wire_format};

let version_id: GlueSchemaVersionId = "550e8400-e29b-41d4-a716-446655440000".parse()?;

let framed = encode_glue_wire_format(version_id, b"avro bytes", GlueCompression::None)?;
let (id, payload) = decode_glue_wire_format(&framed)?;

assert_eq!(id, version_id);
assert_eq!(payload, b"avro bytes");
# Ok::<(), Box<dyn std::error::Error>>(())
```

With real credentials, use the SDK-backed client:

```rust,ignore
use aws_config::BehaviorVersion;
use schemreg::{AwsGlueSchemaRegistry, CachedGlueSchemaRegistry, GlueSchemaRegistryClient};

let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
let registry = CachedGlueSchemaRegistry::new(AwsGlueSchemaRegistry::from_config(&config));

registry.inner().health_check().await?;   // preflight: creds, network, IAM
```

## Configuration from the environment

```rust,ignore
// SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_USERNAME / _PASSWORD, or _BEARER_TOKEN
let registry = ConfluentSchemaRegistryBuilder::from_env()?.build()?;

// APICURIO_REGISTRY_URL, APICURIO_REGISTRY_USERNAME / _PASSWORD, or _BEARER_TOKEN
let registry = ApicurioSchemaRegistryBuilder::from_env()?.build()?;
```

## Runnable examples

Every example in the repository runs without a live registry, against an
in-memory stub, and all of them are executed in CI:

```sh
cargo run --example schema_resolution      --features confluent
cargo run --example schema_guid_and_headers
cargo run --example protobuf_wire_format
cargo run --example avro_roundtrip         --features avro
cargo run --example json_roundtrip         --features json
cargo run --example glue_roundtrip         --features glue
cargo run --example apicurio_roundtrip     --features apicurio
cargo run --example custom_backend
```
