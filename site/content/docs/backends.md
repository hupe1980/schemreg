+++
title = "Choosing a backend"
description = "Confluent Schema Registry, Karapace, Redpanda, Apicurio Registry v3, and AWS Glue — what each one supports, what the native Apicurio client costs, and why Azure and Buf are out of scope."
weight = 7
+++

| Registry | Wire format | Status |
|---|---|---|
| **Confluent Schema Registry** | `0x00` + 4-byte ID, `0x01` + 16-byte GUID, or a Kafka header (+ Protobuf index) | ✅ Native client |
| **Karapace** | Confluent-compatible REST, v0 framing | ✅ Use `ConfluentSchemaRegistry` |
| **Redpanda Schema Registry** | Confluent-compatible REST, v0 framing | ✅ Use `ConfluentSchemaRegistry` |
| **Apicurio Registry v3** | Confluent framing; group + artifact addressing | ✅ Native v3 client |
| **Apicurio (ccompat)** | Confluent framing and REST | ✅ Use `ConfluentSchemaRegistry` against the ccompat base path |
| **AWS Glue Schema Registry** | `0x03` + compression + 16-byte UUID | ✅ Native SDK client |
| **Azure Event Hubs SR** | Schema GUID out-of-band, own REST API | ⬜ Out of scope — see below |
| **Buf Schema Registry** | none — build-time only | ⛔ No runtime framing exists |

## Confluent vs Apicurio native

Use `ConfluentSchemaRegistry` against Apicurio's ccompat endpoint if you only
need subjects and versions. Use `ApicurioSchemaRegistry` when you need what
ccompat cannot express:

- **group isolation** — Apicurio's multi-tenancy boundary
- **artifact rules** — per-artifact compatibility configuration
- **branch-based version selection**
- **server-side dereferencing** — `?references=DEREFERENCE`, see below

### What the native v3 client costs

Registry v3 removed the `X-Registry-GlobalId` / `-Version` / `-ArtifactId`
response headers that v2 set on content endpoints, so
`GET /versions/{expr}/content` returns the schema text and nothing that
identifies it. `get_latest_schema` and `get_schema_by_version` therefore issue
**two** requests — version metadata, then content — rather than reporting a
schema with a fabricated ID of `0`. Neither call is on a cached path, since
`get_latest_schema` must always reach the backend anyway.

`get_schema_by_id` is unaffected: it addresses `/ids/globalIds/{id}`, where the
identifier is the thing you looked up with.

An Apicurio global ID is an `int64`; the Confluent wire format carries a `u32`.
A registry that has outgrown 4 billion versions cannot be framed at all, and the
client says so rather than truncating into a valid-looking ID that points at a
different schema.

### Schema references on Apicurio

Apicurio stores a referencing schema exactly as written and exposes its
references on a **separate route**, so `Schema::references` comes back empty on
this backend and there is nothing for `AvroSchemaDecoder` to resolve against.

The fix is server-side, and it is one builder call:

```rust,ignore
let registry = ApicurioSchemaRegistry::builder()
    .url("https://registry.example.com")
    .dereference_references(true)     // ?references=DEREFERENCE
    .build()?;
```

Apicurio then inlines the referenced content into what it returns, so the schema
is self-contained — at no extra round-trip, and on both `get_schema_by_id` and
the subject-addressed lookups.

It is **off by default** because it changes the bytes you get back. The returned
text is no longer the text that was registered, which makes it the wrong input
for re-registering a schema elsewhere or for computing a fingerprint. Turn it on
for consumers; leave it off for tooling that copies schemas between registries.
(The other option remains the ccompat endpoint with `ConfluentSchemaRegistry`,
which returns references inline as a list.)

### Apicurio-specific behaviour

| Operation | Behaviour |
|---|---|
| `lookup_schema` | `POST /search/versions`, scoped to the group + artifact, **not** canonicalised. Apicurio's content search does not match versions carrying references ([apicurio-registry#6142]); for those, `register_schema` with its idempotent `FIND_OR_CREATE_VERSION` is the reliable route |
| `delete_subject` | Always permanent — Apicurio has no soft-delete stage, so the `permanent` flag is ignored. The version list is read before the delete rather than fabricated afterwards |
| `delete_version` | Disabled by default server-side; without `apicurio.rest.deletion.artifact-version.enabled=true` the registry answers `405` |
| `get_compatibility` / `set_compatibility` | An empty subject addresses `/admin/rules/COMPATIBILITY`, matching the Confluent client's "empty subject = global default". `set_compatibility` `PUT`s and falls back to `POST` when the rule has never been configured, which is the common case for a fresh artifact |
| `get_subjects` / `get_versions` | Paginated internally at 500 per page and walked to exhaustion — no silent truncation at the first page |

[apicurio-registry#6142]: https://github.com/Apicurio/apicurio-registry/issues/6142

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

## Azure Event Hubs Schema Registry

Out of scope. Azure carries its schema GUID out-of-band, in the AMQP
`content-type` property (`avro/binary+{guid}`) or a record header — a shape
`encode_schema_id_header` already models, so the framing side would be small.

The client is not. Azure's registry is not Confluent-compatible at the REST
layer: different routes, a different identity model (a schema *group* plus a
content-type-encoded GUID), and Entra ID authentication via `azure_identity` —
roughly 40 transitive crates none of the other backends need, and none
exercisable in CI without an Azure subscription.

If you need it, `SchemaRegistryClient` is the seam: implement it against Azure's
REST API and every codec and cache here works unchanged.

## Buf Schema Registry

BSR governs `.proto` distribution at build time. There is no runtime binary
framing to implement. Protobuf messages produced by BSR-managed types are framed
with the *Confluent* format if they go through a Confluent-compatible registry,
which this crate already handles.
