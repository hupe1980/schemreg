+++
title = "Codecs"
description = "Avro, JSON Schema, and Protobuf codecs for schemreg: serialisation and framing in one step, reader-schema evolution, cross-subject references, and message-type verification."
weight = 4
+++

Two layers sit on top of the wire format.

| Layer | Types | Takes |
|---|---|---|
| **Framing only** | `ConfluentSchemaEncoder`, `WireFormatDecoder` | bytes you already serialised |
| **Serialise + frame** | `AvroSchemaEncoder`, `JsonSchemaEncoder`, `ProtobufSchemaEncoder` and their decoders | a typed value |

The framing layer implements the object-safe `PayloadEncoder` / `PayloadDecoder`
traits, which is what makes it droppable into a Kafka client's serializer hook.
The typed codecs take a value rather than bytes, so they cannot implement those
traits — call them directly.

All of them share the [producer configuration](@/docs/producers.md) settings and
the same bounded, coalescing caches.

## Avro

```rust,ignore
use apache_avro::types::Value;
use schemreg::{AvroSchemaDecoder, AvroSchemaEncoder, EncodeTarget};

let encoder = AvroSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER_SCHEMA)
    .build()?;

let value = Value::Record(vec![("id".to_string(), Value::String("o-1".into()))]);
let framed = encoder.encode(value, "orders", EncodeTarget::Value).await?;

let decoder = AvroSchemaDecoder::new(cached);
let decoded: Value = decoder.decode(framed).await?;
```

`encode_ser` and `decode_de` take and return any `serde` type instead of an
`apache_avro::types::Value`.

The schema is parsed once at `build()`, so a syntax error surfaces at
construction rather than at the first encode. The record name for the
`RecordName` strategies is extracted from the schema's `name` and `namespace`.

### Schema evolution with a reader schema

By default a payload is decoded with the **writer** schema the wire header
names, so fields the consumer does not know about appear in the value and fields
it expects but the writer dropped are simply absent.

Supply a **reader** schema to get Avro's documented resolution rules — defaulted
fields, dropped fields, promoted numeric types — matching the Confluent Java
`SpecificAvroDeserializer`:

```rust,ignore
let decoder = AvroSchemaDecoder::new(cached).with_reader_schema(r#"{
    "type": "record", "name": "Order", "namespace": "com.example",
    "fields": [
        {"name": "id",  "type": "int"},
        {"name": "qty", "type": "int", "default": 7}
    ]
}"#)?;

// A payload written before `qty` existed decodes with qty = 7.
```

### Schema references

A schema that names a type defined in another subject is stored by the registry
exactly as written, so it is **not** parseable on its own — the definition of
`com.example.Address` lives elsewhere.

`AvroSchemaDecoder` fetches the transitive dependency closure and parses the set
together. A diamond is fetched once per subject, and a cycle terminates instead
of recursing.

The encoder needs the same definitions locally, and Avro resolves a named type
only against definitions that came **earlier** in the list:

```rust,ignore
let encoder = AvroSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER)                        // references com.example.Address
    .dependencies([ADDRESS])              // defined before ORDER uses it
    .references(vec![SchemaReference::new(
        "com.example.Address", "address-value", 1i32,
    )])
    .build()?;
```

`references` is what the registry stores; `dependencies` is what the local Avro
parser needs. A dependency listed after its user stays unresolved and fails at
encode time, so `build()` is the place that tells you.

## JSON Schema

Validation uses the [`jsonschema`] crate, which implements drafts 4, 6, 7,
2019-09, and 2020-12; the draft is taken from the document's `$schema` keyword,
falling back to 2020-12.

[`jsonschema`]: https://docs.rs/jsonschema

```rust,ignore
use schemreg::{EncodeTarget, JsonSchemaDecoder, JsonSchemaEncoder};

let encoder = JsonSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER_SCHEMA)
    .validate_on_encode(true)       // the default
    .build()?;

let framed = encoder
    .encode(&serde_json::json!({ "id": 1, "name": "Widget" }), "orders", EncodeTarget::Value)
    .await?;
```

Validation is **on** for the encoder and **off** for the decoder. Producers are
assumed to have validated on encode, and revalidating every consumed record
doubles the cost for a check that has already run — turn it on with
`JsonSchemaDecoder::with_validation` in test environments or strict pipelines.
With validation off, no validator is ever compiled.

### `$ref` across subjects

The same problem as Avro references, the same shape of answer. A document whose
`$ref` points at another subject is stored verbatim and is not compilable alone;
the decoder fetches the transitive closure and compiles the set together.

The encoder is given the same documents locally as `(name, schema)` pairs, where
`name` is the `$ref` string — the same value that goes in
`SchemaReference::name`:

```rust,ignore
let encoder = JsonSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER)
    .dependencies([("https://example.com/address.json", ADDRESS)])
    .references(vec![SchemaReference::new(
        "https://example.com/address.json", "address-value", 1i32,
    )])
    .build()?;
```

Order is irrelevant here, unlike Avro: JSON Schema resolves by URI. A recursive
`$ref` is legal and is compiled rather than rejected, while the fetch walk still
terminates because each `(subject, version)` is visited once.

Compilation **never reaches the network**. `jsonschema` is built without
`resolve-http`, and the retriever only answers from what you supplied, so a
`$ref` to a URL nobody provided is a compile error rather than an outbound
request.

## Protobuf

Protobuf needs one thing Avro and JSON Schema do not: the **message-index
path**, identifying which message type inside the registered `.proto` was
serialised.

Getting it wrong is not a clean failure — the consumer slices the payload at the
wrong offset and hands the runtime bytes that are *almost* a valid message. And
the correct value changes whenever someone reorders messages in the `.proto`. So
it is derived from the compiled descriptor rather than written by hand:

```rust,ignore
use schemreg::{EncodeTarget, ProtobufSchemaDecoder, ProtobufSchemaEncoder};

let encoder = ProtobufSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(PROTO_SOURCE)                        // the .proto text, registered as-is
    .descriptor(Order::default().descriptor())   // ← path derived from here
    .build()?;

let framed = encoder.encode(&order, "orders", EncodeTarget::Value).await?;
```

`schema` is the file content the registry stores, so consumers in other
languages resolve it by ID. `descriptor` is the compiled type — obtain one from
`prost-reflect`, typically via `prost-build` with `file_descriptor_set_path`.

### Verifying the message type

A Protobuf payload does not identify its own type. Decoding `Invoice` bytes as
an `Order` usually *succeeds*, silently, producing a struct full of defaults and
unknown fields:

```rust,ignore
let decoder = ProtobufSchemaDecoder::new(cached)
    .with_expected_descriptor(&Order::default().descriptor())?;

let order: Order = decoder.decode(framed).await?;   // wrong type ⇒ WireFormat error
```

For routing, `unframe` strips the header and reports the message-index path
without decoding, so a dispatcher can pick the concrete type first.

## Cache sizing

Each decoder keeps a bounded, coalescing cache of parsed schemas or compiled
validators, keyed by the wire identifier; each encoder keeps one of resolved
subjects. Defaults are 1 000 entries.

```rust,ignore
AvroSchemaDecoder::with_max_cache_entries(registry, 4096);
JsonSchemaDecoder::new(registry).with_max_cache_entries(4096);

AvroSchemaEncoder::builder().max_subject_cache_entries(64);
```

Thirty-two consumers hitting a cold schema ID compile it once, not thirty-two
times. See [Caching](@/docs/caching.md) for the guarantees these share with the
registry cache.
