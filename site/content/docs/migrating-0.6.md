+++
title = "Migrating to 0.6"
description = "Upgrade path from schemreg 0.5.x to 0.6.0: the Avro decoder gains a builder, reader schemas resolve their own references, and dependency order stops mattering."
weight = 12
+++

One breaking change, in one place: configuring an `AvroSchemaDecoder`. Two
methods moved onto a builder, and the compiler points at both.

Everything else in 0.6 either fixes a case that used to fail or turns a runtime
failure into a `build()` error. For the complete list see the
[CHANGELOG](https://github.com/hupe1980/schemreg/blob/main/CHANGELOG.md).

## 1. `AvroSchemaDecoder` is configured through a builder

`with_reader_schema` and `with_max_cache_entries` are gone.

```rust,ignore
// 0.5
let decoder = AvroSchemaDecoder::new(registry).with_reader_schema(READER)?;
let bounded = AvroSchemaDecoder::with_max_cache_entries(registry, 4096);

// 0.6
let decoder = AvroSchemaDecoder::builder()
    .registry(registry)
    .reader_schema(READER)
    .build()?;
let bounded = AvroSchemaDecoder::builder()
    .registry(registry)
    .max_cache_entries(4096)
    .build()?;
```

`AvroSchemaDecoder::new(registry)` is unchanged and is still the right call when
you want neither: decode with the writer schema, default cache bound.

The reason for the move is §2 — a reader schema and the definitions it needs are
two settings that can only be validated together, and a chain of infallible
`with_*` calls has nowhere to do that. The JSON and Protobuf decoders keep their
`with_*` methods, because none of their settings can fail.

## 2. A reader schema may name types defined in other subjects

This is the bug that prompted the release. A reader schema referencing another
type was parsed as if it stood alone, and failed at `with_reader_schema` with
`Unknown primitive type: com.example.Address` before the registry was ever
contacted. Inlining the definition was the only way through.

```rust,ignore
let decoder = AvroSchemaDecoder::builder()
    .registry(registry)
    .reader_schema(CUSTOMER)              // "address": "com.example.Address"
    .reader_dependencies([ADDRESS])       // ← the definitions it needs
    .build()?;
```

The writer schema's references are still resolved from the registry with no
configuration — it is registered, so its `references` are on record. A reader
schema is local and the registry has never seen it, which is why its definitions
have to be supplied.

Already holding `apache_avro::Schema` values, from `#[derive(AvroSchema)]` or
`parse_list`? `reader_schema_parsed` and `reader_dependencies_parsed` take them
directly. Both forms have to match: a JSON schema with parsed dependencies is a
`build()` error rather than a silently ignored setting.

## 3. Dependency order no longer matters

In 0.5, `dependencies` had to list a definition before any schema using it. The
wrong order built successfully and then failed on the first encode with
`Unresolved schema reference`.

```rust,ignore
// Both of these now build, encode, and produce identical bytes.
.dependencies([ADDRESS, CUSTOMER])
.dependencies([CUSTOMER, ADDRESS])
```

The set is sorted before the Avro codec sees it. If your lists were already in
dependency order, nothing changes.

## 4. More configuration errors, fewer runtime ones

Cases that used to surface on the first encode or decode — or, in one case, on
every other process start — are now `build()` errors that name the type and the
list that should hold it:

| Situation | 0.5 | 0.6 |
|---|---|---|
| A referenced type nobody supplied | `Unknown primitive type: com.example.Address` | names the type and the list that should hold it |
| Dependencies in the wrong order | `Unresolved schema reference`, at encode time | builds and encodes |
| Two schemas referencing each other | `Unresolved schema reference`, at encode time | `build()` error naming both |
| The same type reached twice through a diamond | `Two schemas with the same fullname were given` | de-duplicated |
| Two different definitions of one type | whichever the parser saw first | `build()` error |
| A reference to a type nested inside another schema | worked about half the time, depending on hash iteration order | consistent `build()` error |

The last row is the one to check if something that used to work now refuses to
build. `apache-avro` resolves a cross-schema reference only to another schema's
**top-level** type; when the definition is nested inside one, whether it
resolves depends on the iteration order of an internal `HashMap` — so it is
refused consistently rather than working on some process starts. Register the
type under its own subject, or inline the definition where it is used.

## 5. JSON Schema: two documents under one `$ref` are rejected

A reference set supplying two different documents for one `$ref` used to keep
whichever arrived last; it is now a `build()` error, matching §4's fifth row.
Supplying the *same* document twice is still fine, and lookup by a `$ref`'s
final path segment is unchanged.

## 6. The `avro` feature pulls in `serde_json`

No new dependency in the tree — `apache-avro` already depends on it — and it is
what lets a schema's declared name be read before the schema is parsed, which is
how a reference closure gets de-duplicated.
