# Choosing a backend

| Registry | Wire format | Status |
|---|---|---|
| **Confluent Schema Registry** | `0x00` + 4-byte ID (+ Protobuf index) | ✅ Native client |
| **Karapace** | Confluent-compatible REST | ✅ Use `ConfluentSchemaRegistry` |
| **Apicurio Registry v3** | Confluent framing; group + artifact addressing | ✅ Native v3 client |
| **Apicurio (ccompat)** | Confluent framing and REST | ✅ Use `ConfluentSchemaRegistry` against the ccompat base path |
| **AWS Glue Schema Registry** | `0x03` + compression + 16-byte UUID | ✅ Native SDK client |
| **Azure Event Hubs SR** | Schema ID in a **header**, no payload prefix | ⬜ Out of scope — see below |
| **Buf Schema Registry** | none — build-time only | ⛔ No runtime framing exists |

---

## Confluent vs Apicurio native

Use `ConfluentSchemaRegistry` against Apicurio's ccompat endpoint if you only
need subjects and versions. Use `ApicurioSchemaRegistry` when you need what
ccompat cannot express:

- **group isolation** — Apicurio's multi-tenancy boundary
- **artifact rules** — per-artifact compatibility configuration
- **branch-based version selection**

### The subject encoding

Apicurio addresses artifacts as `(group, artifact, version)`; the
`SchemaRegistryClient` trait uses a single `subject` string. The mapping is a
convention:

| Subject string | Group | Artifact |
|---|---|---|
| `"orders-value"` | `"default"` | `"orders-value"` |
| `"mygroup/orders-value"` | `"mygroup"` | `"orders-value"` |

`ArtifactId::to_subject` / `from_subject` encode and decode it, and
`SubjectNameStrategy::ApicurioGroupRecordName { group_id }` produces
group-scoped subjects directly.

This is lossless — `/` is percent-encoded *inside* each component, so an
artifact ID legitimately containing a slash still round-trips. A third trait
parameter was rejected because Confluent and Glue have no group concept, and
every one of their callers would have to pass a meaningless argument.

---

## Why Azure Event Hubs SR is not supported

Not effort — model mismatch. Azure does not prefix the payload. The schema GUID
travels out-of-band: in the AMQP `content-type` property
(`avro/binary+{guid}`), or in a Kafka record **header**.

Every signature in `schemreg::wire` is `fn(&[u8]) -> Result<(Id, &[u8])>`: the
buffer is the sole input and the sole source of the identifier. There is no
parameter through which a header could arrive, and `detect_wire_format` has no
first byte to dispatch on, because an Azure payload starts with the serialised
Avro directly.

Supporting it cleanly means a separate `azure` module with its own
`AzureSchemaDecoder { fn decode(&self, payload: Bytes, content_type: &str) }`,
sharing only the `SchemaRegistryClient` abstraction. That is tracked as a design
spike, gated on demand, because it needs `azure_identity` (~40 transitive
crates) and cannot be validated in CI without an Azure subscription.

Threading an optional header through the existing decode API was rejected: it
would tax every current caller with a parameter that is `Prefixed` for 100 % of
them.

---

## Why Buf Schema Registry is permanently out of scope

BSR governs `.proto` distribution at build time. There is no runtime binary
framing to implement — nothing for a wire-format crate to do. Protobuf messages
produced by BSR-managed types are framed with the *Confluent* format if they go
through a Confluent-compatible registry, which this crate already handles.
