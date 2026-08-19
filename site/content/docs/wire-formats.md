+++
title = "Wire formats"
description = "Exactly which bytes go on a Kafka topic: Confluent v0 and v1 prefixes, schema IDs in Kafka headers, the Protobuf message-index array, and the AWS Glue 18-byte header."
weight = 2
+++

`schemreg` implements every binary framing that names the schema a Kafka record
was written with. None of them is Kafka's own format: they are prefixes on the
record value (or key), or — since Confluent Platform 8 — a record header.

| Framing | Leading byte | Identifier |
|---|---|---|
| Confluent v0 | `0x00` | 4-byte schema ID |
| Confluent v1 | `0x01` | 16-byte schema GUID |
| Confluent, header placement | *(no prefix)* | the same bytes, in `__key_schema_id` / `__value_schema_id` |
| AWS Glue | `0x03` | compression byte + 16-byte version UUID |

## Confluent v0 — schema ID

```text
┌──────────┬────────────────────┬──────────────────┐
│ 0x00 (1B)│ Schema ID (4B, BE) │ Payload (N bytes)│
└──────────┴────────────────────┴──────────────────┘
```

```rust
use schemreg::{SchemaId, decode_wire_format, encode_wire_format};

let framed = encode_wire_format(42u32, b"serialised-bytes");
let (key, payload) = decode_wire_format(&framed)?;
assert_eq!(key.as_id(), Some(SchemaId::new(42)));
# assert_eq!(payload, b"serialised-bytes");
# Ok::<(), schemreg::SchemaRegError>(())
```

`decode_wire_format_bytes` returns a `Bytes` payload that shares the input
allocation — no copy, at any payload size.

---

## Confluent v1 — schema GUID

```text
┌──────────┬────────────────────────┬──────────────────┐
│ 0x01 (1B)│ Schema GUID (16B, BE)  │ Payload (N bytes)│
└──────────┴────────────────────────┴──────────────────┘
```

Added in Confluent Platform 8. A schema **ID** is assigned by one registry, so
the same schema has different IDs in different clusters — which is why
replicating a topic across regions means rewriting every record's prefix. A
schema **GUID** is a fingerprint of the schema itself (definition, references,
metadata, rule set), so it identifies the same schema everywhere.

```rust
use schemreg::{SchemaGuid, decode_wire_format, encode_wire_format};

let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
let framed = encode_wire_format(guid, b"serialised-bytes");
assert_eq!(framed[0], 0x01);

let (key, _) = decode_wire_format(&framed)?;
assert_eq!(key.as_guid(), Some(guid));
assert_eq!(key.as_id(), None);   // a GUID frame names no numeric ID
# Ok::<(), schemreg::SchemaRegError>(())
```

The 16 bytes are the GUID in big-endian order — byte-for-byte what Java's
`putLong(msb); putLong(lsb)` writes, and what the canonical `8-4-4-4-12` text
form reads left to right.

### Which version to emit

The **producer** chooses. `encode_wire_format` follows the identifier you hand
it: a `u32` or `SchemaId` emits v0, a `SchemaGuid` emits v1. Consumers must
accept both, which is why decoding returns a `SchemaKey`:

```rust,ignore
let (key, payload) = decode_wire_format(&record)?;
let schema = registry.get_schema_by_key(key).await?;   // dispatches on the variant
```

`Schema::key()` gives you the identifier to frame with, preferring the GUID when
the registry reported one.

---

## Confluent — schema ID in a Kafka header

Confluent Platform 8 can move the identifier out of the payload entirely. The
header **value** is byte-for-byte the same prefix — magic byte, identifier, and
for Protobuf the message-index array — and the payload then carries no prefix
at all.

| Target | Header name |
|---|---|
| key | `__key_schema_id` |
| value | `__value_schema_id` |

`schemreg` produces and consumes `Bytes`, not Kafka records, so this is a codec
you wire to your client's header API:

```rust
use schemreg::{EncodeTarget, SchemaGuid, decode_schema_id_header,
               encode_schema_id_header, schema_id_header_name};

let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;

// Producer: set this header, and write the payload unframed.
let name  = schema_id_header_name(EncodeTarget::Value);
let value = encode_schema_id_header(guid, None);   // Some(&indexes) for Protobuf

// Consumer.
let (key, msg_indexes) = decode_schema_id_header(&value)?;
assert_eq!(key.as_guid(), Some(guid));
assert_eq!(msg_indexes, None);
# assert_eq!(name, "__value_schema_id");
# Ok::<(), schemreg::SchemaRegError>(())
```

A header carrying a v0 identifier (`0x00` + 4 bytes) is accepted too, which is
what a registry without GUID support would put there.

Confluent's deserializer is **header-first**: it looks in the header, and falls
back to the payload prefix when there is none. A consumer wanting the same
behaviour checks the header, then calls `decode_wire_format` if it was absent.

### From a codec, not by hand

The functions above are the low-level seam. Every encoder in the crate has an
`encode_with_header` method that produces all three pieces at once, so the
header value and the payload cannot drift apart:

```rust,ignore
let record = encoder.encode_with_header(value, "orders", EncodeTarget::Value).await?;

record.header_name;    // "__value_schema_id"
record.header_value;   // the prefix bytes
record.payload;        // serialised, and unframed
```

Which identifier lands in the header follows the encoder's
`Framing` setting. Confluent's own `HeaderSchemaIdSerializer` only ever emits a
GUID, so `Framing::SchemaGuid` is the interoperable choice; an ID is accepted so
that header placement also works against a registry that has no GUIDs.

Write **both** the header and the payload. With no prefix in the payload, a
consumer that never sees the header has nothing to look the schema up by.

---

## Confluent — Protobuf

Protobuf inserts a **message-index array** between the prefix and the payload.
It identifies which message type inside the registered `.proto` was serialised,
because one registered schema can declare many. This applies to v0 and v1
alike, and to the header form.

```text
┌────────────────────┬──────────────────┬──────────────────┐
│ v0 or v1 prefix    │ Message index    │ Payload (N bytes)│
└────────────────────┴──────────────────┴──────────────────┘
```

### The two rules that matter

1. **Every integer is ZigZag-encoded, then written as an unsigned LEB-128
   varint — including the leading element count.** This matches
   `org.apache.kafka.common.utils.ByteUtils.writeVarint`, which the Confluent
   Java serde uses for both.
2. **The path `[0]` is written as a single `0x00` byte.** It is the first
   top-level message — the overwhelmingly common case — and the serde special-cases
   it. A decoded count of `0` maps back to `[0]`, never to an empty path.

| Path | Bytes | Derivation |
|---|---|---|
| `[0]` | `00` | mandated single-byte form |
| `[1]` | `02 02` | ZigZag(1)=2 count, ZigZag(1)=2 |
| `[2]` | `02 04` | ZigZag(1)=2 count, ZigZag(2)=4 |
| `[1, 0]` | `04 02 00` | ZigZag(2)=4 count, then ZigZag(1), ZigZag(0) |
| `[1, 1, 0]` | `06 02 02 00` | ZigZag(3)=6 count, then the three segments |

These are not derived from a reading of the docs — they are the literal bytes
[`confluent-kafka-python` produces](https://github.com/hupe1980/schemreg/blob/main/conformance/README.md), captured as
fixtures and asserted in CI.

### Why message indexes are `u32`

A message index is a position in a descriptor's `message_type` or `nested_type`
list, so it is never negative. Typing it `u32` means the encoder cannot produce
a frame the decoder rejects.

ZigZag is still the encoding — Kafka's `writeVarint` requires it — and for a
non-negative `n` that is exactly `2n`, so the bytes are unchanged. What changes
is the decoder: an **odd** varint decodes to a negative number and is rejected.
That is precisely what a non-conforming serializer writing a *plain* unsigned
count emits, and it is a deliberate divergence from Confluent's Java decoder,
which reads the value without checking and fails later, when resolving the
message type. Failing at the framing boundary means the error can say what is
actually wrong.

### Getting the path right

Do not hand-write it. With the `protobuf` feature the path is derived from the
message descriptor, so it stays correct when someone reorders the `.proto`:

```rust,ignore
use schemreg::protobuf::ProtobufSchemaEncoder;

let encoder = ProtobufSchemaEncoder::builder()
    .registry(registry)
    .schema(PROTO_SOURCE)
    .descriptor(Order::default().descriptor())  // ← path derived from here
    .build()?;
```

If you are framing bytes yourself, `encode_protobuf_wire_format(id, &[0], body)`
emits the correct single-`0x00` form.

### Decoding the wrong type

A Protobuf payload does not identify its own type. Decoding an `Invoice` as an
`Order` normally *succeeds*, returning a struct full of defaults. Guard against
it explicitly:

```rust,ignore
let decoder = ProtobufSchemaDecoder::new(registry)
    .with_expected_descriptor(&Order::default().descriptor())?;
```

---

## AWS Glue

```text
┌──────────┬─────────────┬──────────────────────┬──────────────────┐
│ 0x03 (1B)│ Compr. (1B) │ Schema version UUID  │ Payload (N bytes)│
│          │             │      (16B, BE)       │                  │
└──────────┴─────────────┴──────────────────────┴──────────────────┘
```

- Compression byte: `0x00` = none, `0x05` = ZLIB
- UUID in big-endian (network) byte order, per RFC 4122
- ZLIB means a **zlib container** (RFC 1950: 2-byte header + Adler-32 trailer),
  not raw deflate

The Glue codec is available without the `glue` feature. Only the AWS SDK client
and ZLIB compression need it — a `NONE`-compressed frame decodes with no AWS
dependency at all.

---

## Auto-detection

`detect_wire_format` dispatches on the first byte and never guesses:

| First byte | Length | Result |
|---|---|---|
| *(empty)* | — | `Unknown` |
| `0x00` | ≥ 5 | `Confluent { key: Id(..), payload_offset: 5 }` |
| `0x01` | ≥ 17 | `Confluent { key: Guid(..), payload_offset: 17 }` |
| `0x00` / `0x01` | shorter | `InvalidConfluent` |
| `0x03` | ≥ 18, known compression byte | `Glue { version_id, compression, payload_offset: 18 }` |
| `0x03` | otherwise | `InvalidGlue` |
| anything else | any | `Unknown` |

`WireFormatDecoder` passes `Unknown` and `Invalid*` through with the original
bytes rather than dropping them, so a topic carrying a mix of framed and
unframed records stays readable.

### The one ambiguity you cannot design away

A raw Avro payload that happens to begin with `0x00` or `0x01` is
indistinguishable from a Confluent frame. This is inherent to the format, not to
this crate: the Confluent framing has no length field or checksum. If a topic
mixes framed and unframed records, distinguish them out-of-band (a header, a
separate topic) rather than relying on detection.

Header placement removes the ambiguity entirely, which is one of the reasons it
exists: the presence of `__value_schema_id` is an unambiguous signal, and the
payload is left untouched.

---

## Guarantees

- **No panics.** The decoders are property-tested against arbitrary input; every
  returned offset is a valid index into the buffer.
- **Bounded work.** Protobuf index paths are capped at 512 segments before any
  allocation sized from the input; ZLIB output is capped at 128 MiB *during*
  decompression, not after.
- **Zero-copy.** `Bytes`-returning decoders slice the input; nothing is copied
  regardless of payload size.
