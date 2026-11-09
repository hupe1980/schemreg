# Cross-language wire-format conformance

The fixtures in [`../tests/fixtures/confluent_conformance.json`](../tests/fixtures/confluent_conformance.json)
are produced by the **official** `confluent-kafka-python` 2.16.1 serializers
against Confluent Platform 8.4.0, and each one is read back by the reference's
own deserializer before it is written. `schemreg` does not produce a single
byte of them.

| Placement | Written by |
|---|---|
| v0 prefix — `0x00` + ID | `prefix_schema_id_serializer`, the default |
| header mode — `0x01` + GUID (+ Protobuf indexes) in `__value_schema_id` / `__key_schema_id` | `header_schema_id_serializer` |
| v1 prefix — `0x01` + GUID in the payload | the reference's `SchemaId.guid_to_bytes()`; no Confluent serializer writes it by default |

[`../tests/conformance_fixtures.rs`](../tests/conformance_fixtures.rs) asserts,
for every fixture, that `schemreg`:

1. **decodes** it — the identifier, the Protobuf message-index path, and the
   payload the reference wrote;
2. **re-encodes** it byte-identically — the direction that catches a decoder
   that is merely permissive; and
3. decodes it **through the codecs** to the value the reference serialized.

## Why this exists

Golden vectors written from reading a specification are only as good as the
reading. That is exactly how the v0.3.0 Protobuf message-index bug survived a
fully green test suite: the implementation and the golden vector agreed with
each other, and both disagreed with every other client in the ecosystem.

The `.proto` in [`shop.proto`](shop.proto) is arranged so the fixture set covers
every message-index shape that exists:

| Message | Index path | Encoded bytes | What it pins |
|---|---|---|---|
| `Order` | `[0]` | `00` | the mandated single-`0x00` optimisation |
| `Invoice` | `[1]` | `02 02` | ZigZag-encoded element **count** |
| `Refund` | `[2]` | `02 04` | ZigZag-encoded segment value |
| `Invoice.Line` | `[1, 0]` | `04 02 00` | two-segment nesting |
| `Invoice.Tax` | `[1, 1]` | `04 02 02` | sibling nested types |
| `Invoice.Tax.Rate` | `[1, 1, 0]` | `06 02 02 00` | three-segment nesting |

Avro and JSON Schema fixtures are included too, pinning that those formats carry
**no** message-index array at all.

## Regenerating

```bash
just conformance
```

This starts Kafka (KRaft) and Schema Registry, serialises a sample record per
placement and message shape with the reference serializer, writes the
fixtures, fails if they differ from the committed file, and runs the Rust
assertions.

CI regenerates the fixtures on every run and fails if the committed file drifts —
so a change in the reference implementation's framing is caught immediately
rather than at the next interop incident.

## Notes

- IDs are assigned by the registry and GUIDs are fingerprints of the schema, so
  a fresh stack reproduces the same fixtures. The Rust tests compare against
  the recorded values, never hard-coded ones.
- `use.deprecated.format: False` is set explicitly. The deprecated format writes
  the index array as plain unsigned varints; the current one ZigZag-encodes
  them. `schemreg` implements the current format only.
