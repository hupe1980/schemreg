+++
title = "Using it with a Kafka client"
description = "Wire schemreg into a Kafka client — shown with krafka: encode before the send, decode after the poll, schema IDs in record headers, and error mapping at the boundary."
weight = 5
+++

`schemreg` produces and consumes [`bytes::Bytes`]. It is not a Kafka client and
never talks to a broker — moving framed bytes to and from a topic is the
client's job. The examples use [krafka](https://github.com/hupe1980/krafka)
0.28, a pure-Rust async Kafka client; the pattern is the same for any client.

[`bytes::Bytes`]: https://docs.rs/bytes/latest/bytes/struct.Bytes.html

## Produce: encode, then send

schemreg's encoders are async — the first encode for a subject may contact the
registry, later ones hit the cache. Encode, then hand the bytes to the producer:

```rust,ignore
use krafka::producer::Record;
use schemreg::{AvroSchemaEncoder, EncodeTarget, SchemaResolution};

let encoder = AvroSchemaEncoder::builder()
    .registry(Arc::clone(&registry))           // a CachedSchemaRegistry
    .schema(ORDER_SCHEMA)
    .resolution(SchemaResolution::LookupOnly)  // never writes to the registry
    .build()?;

let framed = encoder.encode(order, "orders", EncodeTarget::Value).await?;
producer.send(Record::new("orders", framed).key("order-1")).await?;
```

To put the schema ID in a record header instead — Confluent Platform 8's
placement — use `encode_with_header`; the payload stays unframed:

```rust,ignore
let framed = encoder.encode_with_header(order, "orders", EncodeTarget::Value).await?;
producer
    .send(
        Record::new("orders", framed.payload)
            .key("order-1")
            .header(framed.header_name, framed.header_value),
    )
    .await?;
```

With `.framing(Framing::SchemaGuid)` the header carries the schema GUID, which
is what Confluent's own header serializer writes.

## Consume: poll, then decode

Hand each record's value and headers to a decoder. `WireRecord::from_headers`
picks the schema-id header — the last one, if repeated — and the decoder reads
it before falling back to the payload prefix, as Confluent's deserializers do.
One consumer therefore reads prefix-framed and header-framed producers alike.

```rust,ignore
use schemreg::{AvroSchemaDecoder, EncodeTarget, WireRecord};

let decoder = AvroSchemaDecoder::new(Arc::clone(&registry));

for record in consumer.poll(Duration::from_secs(1)).await? {
    let Some(value) = record.value.clone() else { continue };   // tombstone
    // krafka headers are `(String, Option<Bytes>)`; a null header carries no ID.
    let headers = record.headers.iter().filter_map(|(k, v)| Some((k, v.as_ref()?)));

    let order = decoder
        .decode(WireRecord::from_headers(EncodeTarget::Value, value, headers))
        .await?;
    handle(order);
}
```

Share one `CachedSchemaRegistry` between producer and consumer: subject lookups
and ID lookups then warm the same cache.

## Why not the client's serializer hooks?

krafka's `Serializer<T>` and `Deserializer` hooks are synchronous, and
schemreg's codecs are async — a cold schema means a registry round-trip. Until
schemreg offers a cache-only synchronous path, encode before the send and
decode after the poll, as above. Clients with async hooks can wrap the
object-safe `PayloadEncoder` / `PayloadDecoder` traits directly.

## Error handling at the boundary

Classify with the predicates, not by message text:

| `schemreg` condition | Predicate | Worth retrying? |
|---|---|---|
| Registry unreachable, HTTP 5xx, 429 | `is_retryable()` | yes — schemreg already retried with back-off; one more attempt at a higher level is reasonable |
| TLS handshake refused | `is_tls_error()` | no — fix the trust configuration |
| Subject or schema not found | `is_not_found()` | no |
| Auth rejected | `is_auth_error()` | no |
| Incompatible or invalid schema | `is_incompatible()`, `is_invalid_schema()` | no |
| Malformed framing | `WireFormat` | no — skip the record; it will not decode next time either |

On the consumer side, a record that fails permanently should be skipped (or
routed to a dead-letter topic) rather than retried, or it stalls its partition.
