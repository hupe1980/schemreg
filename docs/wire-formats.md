# Wire formats

`schemreg` implements the two binary framings used on Kafka topics. Both are
prefixes on the record value (or key); neither is Kafka's own format.

---

## Confluent — Avro and JSON Schema

```text
┌──────────┬────────────────────┬──────────────────┐
│ 0x00 (1B)│ Schema ID (4B, BE) │ Payload (N bytes)│
└──────────┴────────────────────┴──────────────────┘
```

```rust
use schemreg::{decode_wire_format, encode_wire_format};

let framed = encode_wire_format(42u32, b"serialised-bytes");
let (id, payload) = decode_wire_format(&framed)?;
# Ok::<(), schemreg::SchemaRegError>(())
```

`decode_wire_format_bytes` returns a `Bytes` payload that shares the input
allocation — no copy, at any payload size.

---

## Confluent — Protobuf

Protobuf inserts a **message-index array** between the header and the payload.
It identifies which message type inside the registered `.proto` was serialised,
because one registered schema can declare many.

```text
┌──────────┬────────────────────┬──────────────────┬──────────────────┐
│ 0x00 (1B)│ Schema ID (4B, BE) │ Message index    │ Payload (N bytes)│
└──────────┴────────────────────┴──────────────────┴──────────────────┘
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
[`confluent-kafka-python` produces](../conformance/README.md), captured as
fixtures and asserted in CI.

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
| `0x00` | ≥ 5 | `Confluent { schema_id, payload_offset: 5 }` |
| `0x00` | < 5 | `InvalidConfluent` |
| `0x03` | ≥ 18, known compression byte | `Glue { version_id, compression, payload_offset: 18 }` |
| `0x03` | otherwise | `InvalidGlue` |
| anything else | any | `Unknown` |

`WireFormatDecoder` passes `Unknown` and `Invalid*` through with the original
bytes rather than dropping them, so a topic carrying a mix of framed and
unframed records stays readable.

### The one ambiguity you cannot design away

A raw Avro payload that happens to begin with `0x00` is indistinguishable from a
Confluent frame. This is inherent to the format, not to this crate: the
Confluent framing has no length field or checksum. If a topic mixes framed and
unframed records, distinguish them out-of-band (a header, a separate topic)
rather than relying on detection.

---

## Guarantees

- **No panics.** The decoders are property-tested against arbitrary input; every
  returned offset is a valid index into the buffer.
- **Bounded work.** Protobuf index paths are capped at 512 segments before any
  allocation sized from the input; ZLIB output is capped at 128 MiB *during*
  decompression, not after.
- **Zero-copy.** `Bytes`-returning decoders slice the input; nothing is copied
  regardless of payload size.
