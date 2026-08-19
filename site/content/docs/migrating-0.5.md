+++
title = "Migrating to 0.5"
description = "Upgrade path from schemreg 0.4.x to 0.5.0: SchemaKey decoding, optional schema IDs, producer resolution policy, and the behavioural changes without a compile error."
weight = 12
+++

Most of what follows is a compile error, so the compiler walks you through it.
Three are behavioural changes with no compile error — §6, §9, and the optional
§11 — and those are the ones to read.

For the complete list of changes see the
[CHANGELOG](https://github.com/hupe1980/schemreg/blob/main/CHANGELOG.md).

## 1. Decoding returns a `SchemaKey`, not a `SchemaId`

A Confluent record now names its schema in one of two ways: a 4-byte ID
(`0x00`) or a 16-byte GUID (`0x01`). The **producer** chooses, so a consumer
cannot assume either.

```rust,ignore
// 0.4
let (id, payload) = decode_wire_format(&record)?;
let schema = registry.get_schema_by_id(id).await?;

// 0.5
let (key, payload) = decode_wire_format(&record)?;
let schema = registry.get_schema_by_key(key).await?;   // dispatches on the variant
```

`get_schema_by_key` is the drop-in: it calls `get_schema_by_id` or
`get_schema_by_guid` as appropriate. If you genuinely need the numeric ID:

```rust,ignore
let Some(id) = key.as_id() else {
    // The record was framed with a GUID; there is no numeric ID to recover.
    return Err(/* … */);
};
```

`SchemaKey` compares directly against `u32` and `SchemaId`, so
`assert_eq!(key, 42u32)` still works in tests.

Affected: `decode_wire_format`, `decode_wire_format_bytes`,
`DetectedWireFormat::Confluent` (its `schema_id` field is now `key`), and
`UnframedProtobuf::schema_id` (now `key`).

---

## 2. `Schema::id` is `Option<SchemaId>`

`GET /schemas/guids/{guid}` returns no numeric ID, and none can be derived from
a GUID — that is the whole point of GUIDs. 0.4 filled the gap with `0`, which is
a valid-looking schema ID that a producer would then frame real records with.

```rust,ignore
// 0.4
println!("{}", schema.id);

// 0.5
println!("{:?}", schema.id);
// or, to frame a payload:
let key = schema.key().expect("the registry reported an identifier");
let framed = encode_wire_format(key, &body);
```

`Schema::key()` returns the identifier to frame with, preferring the GUID when
the registry reported one.

`Schema::new` now takes anything convertible to `SchemaKey`, so
`Schema::new(1u32, …)` is unchanged and `Schema::new(guid, …)` is newly
possible.

---

## 3. Message indexes are `u32`

A Protobuf message index is a position in a descriptor's `message_type` or
`nested_type` list, so it is never negative. 0.4 typed it `i32`, which let the
encoder produce frames the decoder rejected.

```rust,ignore
// 0.4
encode_protobuf_wire_format(id, &[1i32, 0], &body);

// 0.5
encode_protobuf_wire_format(id, &[1u32, 0], &body);
```

The bytes are unchanged: ZigZag of a non-negative `n` is `2n`. What changed is
that the decoder now **rejects** an index that ZigZag-decodes to a negative
number, which is what a serializer writing a plain unsigned count emits.

Affected: `encode_protobuf_wire_format`, `decode_protobuf_message_indexes`,
`message_index_path`, `ProtobufSchemaEncoderBuilder::message_indexes`,
`ConfluentSchemaEncoderBuilder::protobuf_message_indexes`,
`DecodedMessage::protobuf_message_indexes`, `UnframedProtobuf::message_indexes`.

---

## 4. `SchemaEncoder` / `SchemaDecoder` are now `PayloadEncoder` / `PayloadDecoder`

The old names implied they serialised a value against a schema. They do not:
they add and strip *framing* around bytes you have already serialised. The
format-specific codecs (`AvroSchemaEncoder`, `JsonSchemaEncoder`,
`ProtobufSchemaEncoder`) are the ones that serialise, and their names are
unchanged.

```rust,ignore
// 0.4
use schemreg::SchemaEncoder;

// 0.5
use schemreg::PayloadEncoder;
```

---

## 5. `check_compatible` is gone

It was a one-line alias for `check_compatibility(subject, schema, type, &[])`.
Call that instead.

---

## 6. Apicurio subject lookups issue two requests

**This is the one behavioural change without a compile error.**

Apicurio Registry v3 removed the `X-Registry-GlobalId` / `-Version` /
`-ArtifactId` response headers that v2 set on content endpoints. 0.4 still read
them, so on a v3 server `get_latest_schema` and `get_schema_by_version` fell
back to a **fabricated schema ID of `0`** and an artifact type of `AVRO`
regardless of what the artifact actually was.

0.5 fetches version metadata and content separately and reports the real values.
The cost is one extra request on those two calls; neither is on a cached path,
since `get_latest_schema` must always reach the backend anyway.
`get_schema_by_id` is unaffected.

If you built anything on the `0` — a cache keyed by it, a metric, a log
assertion — it will now see real IDs.

---

## 7. Avro schema references now resolve

A schema that names a type from another subject is stored by the registry
exactly as written, so it is not parseable alone. 0.4 handed the raw string to
the Avro parser and failed with "unknown type". 0.5 fetches the transitive
dependency closure and parses the set together.

Decoding needs no change. **Encoding does**, if your schema uses references:
supply the definitions locally, dependencies first.

```rust,ignore
let encoder = AvroSchemaEncoder::builder()
    .registry(registry)
    .schema(ORDER)
    .dependencies([ADDRESS, CUSTOMER])   // ← new; dependency order matters
    .references(vec![/* … */])
    .build()?;
```

`references` is what the registry stores; `dependencies` is what the local
parser needs. Avro resolves a named type only against definitions that came
earlier in the list.

---

## 8. Error classification is finer

- `SchemaRegError::Api` errors in the registry's `5xxxx` range (store failure,
  internal timeout, forwarding failure, no leader) are now **retryable**.
  Previously a 500 whose body happened to parse as JSON was classified as
  permanent while an identical 500 with an HTML body was retried.
- New predicates: `is_incompatible()` (error code 40901) and
  `is_invalid_schema()` (42201, 42209). A schema the subject's policy forbids
  and a schema that is malformed are different problems.
- New accessors: `error_code()` and `status()`.
- `error_code` module exposes the named constants.

If you matched on `SchemaRegError::Api { error_code, .. }` directly, prefer the
predicates — they are stable across backends, and Glue maps its service errors
onto the same codes.

---

## 9. `is_not_found()` now covers a bare HTTP 404

**A behavioural change with no compile error.** A Confluent-compatible registry
answers 404 with an `{"error_code": 404xx}` body, which lands in
`SchemaRegError::Api`. A reverse proxy, an API gateway, or a registry without
the route answers with HTML or nothing at all, which lands in
`SchemaRegError::Http` — and 0.4's predicate returned `false` for it.

That made `lookup_schema` report a transport-shaped error instead of `Ok(None)`
for a subject that simply was not there. 0.5 treats `Http { status: 404 }` as
not-found.

If you classified errors as `is_not_found()` first and "some other HTTP
failure" second, a bare 404 now takes the first branch. That is what it always
meant.

---

## 10. Smaller breakages

| 0.4 | 0.5 |
|---|---|
| `CompatibilityLevel::from_str` was case-sensitive | case-insensitive, matching `SchemaType` |
| `SchemaType::from_str` / `CompatibilityLevel::from_str` returned `InvalidState` | return `Config` |
| `GlueSchemaVersionId` parse errors were `InvalidState` | `Config` |
| `GlueSchemaVersionId` / `SchemaGuid` were opaque | wrap `uuid::Uuid`, with `From`/`Into` both ways |
| `schemreg::codec_cache` module | merged into `schemreg::resolver`; the crate-root re-export of `DEFAULT_MAX_SUBJECT_CACHE_ENTRIES` is unchanged |
| `CachedSchemaRegistry` served entries for deleted subjects | `delete_subject` / `delete_version` invalidate the subject on success |

---

## 11. Decide what your producers are allowed to do

Nothing forces this on you — the default is what 0.4 did — but it is the change
worth acting on.

Until now every encoder called `register_schema` on the first encode for each
subject. There was no way to turn that off, even though `lookup_schema` existed
and the documentation recommended it for read-only producers. A producer
therefore needed `Subject:Write`, and a local schema that had drifted from the
registry created a new production version silently.

```rust,ignore
use schemreg::SchemaResolution;

let encoder = AvroSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER_SCHEMA)
    .resolution(SchemaResolution::LookupOnly)   // ← needs only Subject:Read
    .build()?;
```

Now a drifted schema fails at the first encode with an error for which
`is_not_found()` is `true` and `is_retryable()` is `false`. `UseLatestVersion`
is the third option, matching the Java serdes' `use.latest.version`.

Available on all four encoders: `ConfluentSchemaEncoder`, `AvroSchemaEncoder`,
`JsonSchemaEncoder`, `ProtobufSchemaEncoder`.

---

## 12. Emitting v1 or header framing from a codec

0.5's first draft exposed wire format v1 and header placement only as raw
functions, so the codecs could not produce the formats the crate advertised.
They can now:

```rust,ignore
use schemreg::Framing;

let encoder = AvroSchemaEncoder::builder()
    .registry(cached.clone())
    .schema(ORDER_SCHEMA)
    .framing(Framing::SchemaGuid)      // 0x01 + 16-byte GUID
    .build()?;

// Or move the identifier out of the payload entirely:
let record = encoder.encode_with_header(value, "orders", EncodeTarget::Value).await?;
// record.header_name / record.header_value / record.payload (unframed)
```

Against a registry that reports no GUID, `Framing::SchemaGuid` is a
`NotSupported` error rather than a fabricated identifier.

---

## After upgrading

The new capabilities are worth a look once the compiler is quiet:
[producer configuration](@/docs/producers.md) for read-only producers and v1
framing, [codecs](@/docs/codecs.md) for JSON Schema `$ref` resolution, and
[backends](@/docs/backends.md) for what the Apicurio v3 client now covers.

The [CHANGELOG](https://github.com/hupe1980/schemreg/blob/main/CHANGELOG.md)
lists everything.
