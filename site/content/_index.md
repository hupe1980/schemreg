+++
title = "schemreg"
description = "Async schema registry client for Kafka in Rust. Confluent wire format v0 and v1, schema IDs in Kafka headers, Apicurio Registry v3, and AWS Glue — with Avro, JSON Schema, and Protobuf codecs."
template = "index.html"
[extra]
msrv = "1.88"

facts = [
  { value = "537", label = "tests, across eleven layers" },
  { value = "0", label = "unsafe blocks — the crate forbids them" },
  { value = "0", label = "dependencies with no feature enabled" },
]

stats = [
  { label = "Tests", value = "537" },
  { label = "`unsafe` blocks", value = "0" },
  { label = "Header decode", value = "1.6 ns" },
  { label = "Cache hit", value = "14.3 ns" },
]

hero_code = '''
```rust
use schemreg::{
    CachedSchemaRegistry, ConfluentSchemaRegistry, EncodeTarget,
    SchemaResolution, WireFormatDecoder, AvroSchemaEncoder,
};

let registry = ConfluentSchemaRegistry::builder()
    .url("https://registry.example.com")
    .basic_auth("user", "password")
    .build()?;

// Bounded, coalescing, cancellation-safe.
let cached = Arc::new(CachedSchemaRegistry::new(registry));

// A producer that reads but never writes.
let encoder = AvroSchemaEncoder::builder()
    .registry(Arc::clone(&cached))
    .schema(ORDER_SCHEMA)
    .resolution(SchemaResolution::LookupOnly)
    .build()?;

let framed = encoder.encode(order, "orders", EncodeTarget::Value).await?;

// Consumer: one path for every framing the producer might have chosen.
let decoder = WireFormatDecoder::confluent(cached);
let message = decoder.decode(framed).await?;
```
'''

formats = [
  { name = "Confluent v0", meta = "every Confluent Platform release", bytes = """
┌──────┬────────────────┬─────────┐
│ 0x00 │ schema ID (4B) │ payload │
└──────┴────────────────┴─────────┘""", body = "A registry-assigned integer. The same schema has a different ID in staging and in production, which is why replication has to rewrite every prefix." },

  { name = "Confluent v1", meta = "Confluent Platform 8+", bytes = """
┌──────┬─────────────────┬─────────┐
│ 0x01 │ schema GUID 16B │ payload │
└──────┴─────────────────┴─────────┘""", body = "A 128-bit fingerprint of the schema itself — its definition, references, metadata, and rules. The same everywhere, so records survive a cluster migration." },

  { name = "Kafka header", meta = "Confluent Platform 8+", bytes = """
__value_schema_id: 01 8f 14 e4 …
┌─────────────────────────────────┐
│ payload — no prefix at all      │
└─────────────────────────────────┘""", body = "The same prefix bytes, moved into a record header. The payload stays untouched, which is what makes a topic readable by tooling that knows nothing about the registry." },

  { name = "AWS Glue", meta = "18-byte header", bytes = """
┌──────┬──────┬───────────────┬─────────┐
│ 0x03 │ comp │ version UUID  │ payload │
└──────┴──────┴───────────────┴─────────┘""", body = "A header version byte, a compression indicator (`0x00` none, `0x05` ZLIB), and a 16-byte schema-version UUID in network order." },
]

features = [
  { title = "Zero-copy framing", body = "Decoding slices the input rather than copying it: 1.6 ns regardless of payload size, and flat across three orders of magnitude. Schemas are held behind `Arc<str>`, so a cache hit costs one atomic increment." },
  { title = "Verified against the reference", body = "The Confluent framings are pinned to byte sequences produced by the official `confluent-kafka-python` serializers — decoded and re-encoded byte-identically, not just checked against this crate's own reading of the spec." },
  { title = "Thundering-herd safe", body = "N concurrent cold misses for one schema issue exactly one backend request. If the leading task is cancelled mid-flight, every waiter is woken with an error rather than left parked forever." },
  { title = "Nothing is invented", body = "A field the registry did not report is `None`, never a plausible-looking default. Asking for GUID framing from a registry that has none is a clear error, not a fabricated identifier." },
  { title = "Descriptor-derived Protobuf paths", body = "The message-index path comes from the compiled descriptor, so it cannot drift when someone reorders the `.proto`. Decoding can reject a payload whose message type is not the one you expected." },
  { title = "Feature-gated to the bone", body = "The default feature set pulls in no transport stack at all. Enable `confluent`, `apicurio`, or `glue` for a client; `avro`, `json`, or `protobuf` for a codec. They compose in any combination." },
]

producer_points = [
  "`AutoRegister` — the default, matching `auto.register.schemas=true`. Needs `Subject:Write`.",
  "`LookupOnly` — finds the registration or fails. Needs only `Subject:Read`.",
  "`UseLatestVersion` — follows the subject's head, matching `use.latest.version`.",
  "A drifted schema fails at the first encode with an error that is **not** retryable, so a retry loop stops instead of spinning.",
]

producer_code = '''
```rust
let encoder = AvroSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER_SCHEMA)
    // Never creates a version in production.
    .resolution(SchemaResolution::LookupOnly)
    // 0x01 + a 16-byte GUID instead of 0x00 + 4 bytes.
    .framing(Framing::SchemaGuid)
    .build()?;

// Or move the identifier out of the payload entirely:
let record = encoder
    .encode_with_header(order, "orders", EncodeTarget::Value)
    .await?;

record.header_name;   // "__value_schema_id"
record.header_value;  // the prefix bytes
record.payload;       // serialised, and unframed
```
'''

backends = [
  { name = "Confluent Schema Registry", framing = "0x00 · 0x01 · header", status = "Native client", state = "yes" },
  { name = "Karapace", framing = "0x00", status = "Confluent client", state = "via" },
  { name = "Redpanda Schema Registry", framing = "0x00", status = "Confluent client", state = "via" },
  { name = "Apicurio Registry v3", framing = "0x00", status = "Native v3 client", state = "yes" },
  { name = "Apicurio (ccompat)", framing = "0x00", status = "Confluent client", state = "via" },
  { name = "AWS Glue Schema Registry", framing = "0x03", status = "Native SDK client", state = "yes" },
  { name = "Azure Event Hubs SR", framing = "out-of-band", status = "Out of scope", state = "none" },
  { name = "Buf Schema Registry", framing = "none", status = "No runtime framing", state = "none" },
]
+++
