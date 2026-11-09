+++
title = "Migrating to 0.7"
description = "Upgrade path from schemreg 0.6.x to 0.7.0: decoders read schema-id headers first, TLS moves to rustls on aws-lc-rs with the OS trust store, and the MSRV becomes 1.94.1."
weight = 12
+++

Three things move, and the compiler points at every call site but one (§3).
For the complete list see the
[CHANGELOG](https://github.com/hupe1980/schemreg/blob/main/CHANGELOG.md).

## 1. Decoders take a `WireRecord`

Every decoder now accepts `impl Into<WireRecord>` — the record's bytes plus its
schema-id header, if it has one. `Bytes`, `&Bytes` and `Vec<u8>` convert, so
prefix-framed call sites compile unchanged:

```rust,ignore
let order = avro_decoder.decode(framed).await?;            // unchanged
let unframed = protobuf_decoder.unframe(&framed)?;          // unchanged
```

To read Confluent header-mode records, pass the headers. The last
`__value_schema_id` (or `__key_schema_id`) wins, and the decoder falls back to
the prefix when there is none:

```rust,ignore
let order = avro_decoder
    .decode(WireRecord::from_headers(EncodeTarget::Value, value, headers))
    .await?;
```

**What does need editing:**

| 0.6 | 0.7 |
|---|---|
| `PayloadDecoder::decode(payload: Bytes, ..)` | `decode(record: WireRecord, ..)` — call with `payload.into()` |
| `impl PayloadDecoder for MyDecoder` | the same signature change |
| `DecodedMessage { .. }` built by hand | new field `placement: Option<SchemaIdPlacement>` |
| hand-rolled `decode_schema_id_header` + `get_schema_by_key` + deserialize | delete it; use the codec with `WireRecord::from_headers` |

## 2. TLS: aws-lc-rs and the platform verifier

The REST clients now run rustls on **aws-lc-rs** (previously ring) and trust the
**operating system's** store (previously the bundled Mozilla roots), with the
hybrid post-quantum key exchange preferred. The AWS SDK already used aws-lc-rs,
so a build with every backend now links one crypto library instead of two.

| 0.6 | 0.7 |
|---|---|
| feature `native-tls-roots` | removed — the OS store is the default. Delete it from `features` |
| `.add_root_certificate(reqwest::Certificate)` | `.add_root_certificate(schemreg::tls::Certificate)` — the same type, re-exported; still *adds* to the trust set |
| *(none)* | `.trust_only_root_certificates([..])` — replace the trust set |
| `.identity(reqwest::Identity)` | `.identity(schemreg::tls::Identity)` — the same type |
| *(none)* | `.require_tls13()` |

Building needs a C compiler for aws-lc-sys — no CMake, Go or NASM.

On Linux the OS store is whatever `/etc/ssl` holds. An image with no CA
certificates at all (`FROM scratch`) now fails the handshake with
`is_tls_error()` and "No CA certificates were loaded from the system" — copy
in `ca-certificates`, or ship your CA and use `trust_only_root_certificates`.

## 3. A refused TLS handshake is no longer retried

An untrusted certificate, no shared TLS version or a rejected client
certificate is now `SchemaRegError::Tls` — `is_tls_error()` is true and
`is_retryable()` is **false**. In 0.6 it was a retryable `Network` error, so the
client retried it three times and an outer retry loop retried it forever. This
is the change the compiler cannot point at: a `match` with a `Network` arm for
certificate failures now falls through to your catch-all.

## 4. MSRV 1.94.1

The floor the current AWS SDK declares, and what lets `ring` leave the
dependency graph. Stable Rust on the release date is 1.99.
