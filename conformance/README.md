# Cross-language wire-format conformance

The fixtures in [`../tests/fixtures/confluent_conformance.json`](../tests/fixtures/confluent_conformance.json)
are produced by the **official** `confluent-kafka-python` serializers running
against a real Confluent Schema Registry. `schemreg` does not participate in
producing a single byte of them.

[`../tests/conformance_fixtures.rs`](../tests/conformance_fixtures.rs) then asserts,
for every fixture, that `schemreg`:

1. **decodes** it — recovering the schema ID, the Protobuf message-index path,
   and the payload the reference serializer wrote; and
2. **re-encodes** it byte-identically — which is the direction that catches a
   decoder that is merely permissive rather than correct.

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
docker compose -f conformance/docker-compose.yml up --build --abort-on-container-exit
```

This starts Kafka (KRaft) + Schema Registry, registers each schema, serialises a
sample record with the reference serializer, and writes the hex-encoded frames
to `tests/fixtures/confluent_conformance.json`.

CI regenerates the fixtures on every run and fails if the committed file drifts —
so a change in the reference implementation's framing is caught immediately
rather than at the next interop incident.

## Notes

- Schema IDs are registry-assigned and therefore vary between runs. The Rust
  test asserts on them only relative to the frame they appear in, never against
  a hard-coded value, so a regenerated fixture set stays valid.
- `use.deprecated.format: False` is set explicitly. The deprecated format writes
  the index array as plain unsigned varints; the current one ZigZag-encodes
  them. `schemreg` implements the current format only.
