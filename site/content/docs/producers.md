+++
title = "Producer configuration"
description = "SchemaResolution and Framing: whether a producer may register schemas, which wire format it emits, and whether the identifier travels in the payload prefix or a Kafka header."
weight = 3
+++

Before a producer can write a byte it has to answer two questions. Both are
builder settings on every encoder — `ConfluentSchemaEncoder`,
`AvroSchemaEncoder`, `JsonSchemaEncoder`, `ProtobufSchemaEncoder` — and both
default to the least surprising answer.

## 1. Which identifier does this subject resolve to?

| [`SchemaResolution`] | Call on a cold subject | Registry permission | Use it when |
|---|---|---|---|
| `AutoRegister` *(default)* | `POST /subjects/{s}/versions` | `Subject:Write` | the application owns its schemas |
| `LookupOnly` | `POST /subjects/{s}` | `Subject:Read` | **CI owns the schemas** |
| `UseLatestVersion` | `GET /subjects/{s}/versions/latest` | `Subject:Read` | schemas evolve centrally and producers follow |

[`SchemaResolution`]: https://docs.rs/schemreg/latest/schemreg/resolver/enum.SchemaResolution.html

These mirror the Confluent Java serdes' `auto.register.schemas` and
`use.latest.version`, including their defaults.

### The default writes to your registry

`AutoRegister` is idempotent — re-registering identical content returns the
existing ID. The risk is the case where the content is *not* identical: a field
added on a local branch, a namespace typo. There, a producer process silently
creates a new version in production.

`LookupOnly` turns that into a startup failure:

```rust,ignore
use schemreg::SchemaResolution;

let encoder = AvroSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER_SCHEMA)
    .resolution(SchemaResolution::LookupOnly)
    .build()?;
```

An unregistered schema fails at the first encode with an error for which
`is_not_found()` is `true` and `is_retryable()` is `false` — so a retry loop
stops rather than spinning, and the process dies at startup instead of writing
records nobody registered a schema for.

Where the registry is a governance boundary, grant the producer read-only
credentials and set this. It is the single most consequential setting here.

### No mode changes what is serialised

Every mode serialises with the schema the encoder was built with. What changes
is which identifier the frame carries.

That matters most for `UseLatestVersion`: the payload is written against the
encoder's own schema but tagged with the subject head's identifier. Avro
resolution on the consumer side makes that work for a compatible evolution and
*only* for a compatible evolution. Keep the subject's compatibility level
enforcing that — the same contract the Java serde relies on.

### Resolution is cached per subject

The first encode for a subject resolves it; the rest are served from a bounded,
coalescing in-memory map, so N tasks racing on a cold subject issue exactly one
round-trip. Under `UseLatestVersion`, `invalidate_subject` is how a long-lived
producer picks up a newer head without a restart:

```rust,ignore
encoder.invalidate_subject("orders-value");
```

## 2. Which framing carries it?

| [`Framing`] | Prefix | Requires |
|---|---|---|
| `SchemaId` *(default)* | `0x00` + 4 bytes | anything Confluent-compatible |
| `SchemaGuid` | `0x01` + 16 bytes | Confluent Platform 8+ |

[`Framing`]: https://docs.rs/schemreg/latest/schemreg/resolver/enum.Framing.html

```rust,ignore
use schemreg::Framing;

let encoder = AvroSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER_SCHEMA)
    .framing(Framing::SchemaGuid)
    .build()?;
```

A GUID is a fingerprint of the schema rather than a per-registry counter, so
records framed this way stay readable after a cluster migration or a
cross-region replication that would otherwise need every prefix rewritten.

Against a registry that reports no GUID this is a `NotSupported` error, never a
frame built from an invented identifier. Combined with `AutoRegister` it costs
one extra round-trip the first time a subject is seen, because registration
reports only the numeric ID; the other two modes get both identifiers in the
response they already make.

## 3. Prefix or header?

A per-call choice, not a builder setting. `encode` puts the prefix in front of
the payload; `encode_with_header` returns the same bytes as a Kafka header value
with the payload left **unframed**:

```rust,ignore
let record = encoder
    .encode_with_header(order, "orders", EncodeTarget::Value)
    .await?;

record.header_name;    // "__value_schema_id"
record.header_value;   // the prefix bytes
record.payload;        // serialised, and unframed
```

Write **both**. A consumer that never sees the header has nothing to look the
schema up by. See [Using it with a Kafka client](@/docs/kafka-integration.md)
for the producer and consumer sides wired to a broker.

Confluent's own header serializer only ever emits a GUID, so
`Framing::SchemaGuid` is the interoperable choice here; an ID is accepted so
that header placement also works against a registry that has none.

## Subject name strategies

| Strategy | Produces |
|---|---|
| `TopicName` *(default)* | `{topic}-key` / `{topic}-value` |
| `RecordName` | `{record_name}` |
| `TopicRecordName` | `{topic}-{record_name}` |
| `ApicurioGroupRecordName { group_id }` | `{group_id}/{record_name}` |
| `Custom(Arc<dyn Fn…>)` | anything you want |

The Avro and Protobuf encoders extract the record name from the schema or
descriptor automatically. The JSON encoder needs it supplied via
`record_name` on the builder, because a JSON Schema has no mandatory name field.

A `Custom` strategy that derives subjects from message *content* is the one case
where the per-subject cache is not bounded by configuration; that is what
`max_subject_cache_entries` is for.

## Observing what a producer resolved

None of these trigger a registration:

```rust,ignore
encoder.cached_schema_key("orders-value");   // Option<SchemaKey>
encoder.cached_schema_id("orders-value");    // Option<SchemaId>, None under GUID framing
encoder.cached_subject_count();
```

## Read-only lookups without an encoder

`SchemaResolution::LookupOnly` routes through
`SchemaRegistryClient::lookup_schema`, which is also callable directly:

```rust,ignore
match registry.lookup_schema("orders-value", MY_SCHEMA, SchemaType::Avro, &[]).await? {
    Some(schema) => { /* registered; frame with schema.key() */ }
    None         => panic!("orders-value has no version matching this schema"),
}
```

`Ok(None)` means "not registered" — both for a missing subject and for content
the subject has never seen. Errors are reserved for transport, auth, and
malformed-schema failures.
