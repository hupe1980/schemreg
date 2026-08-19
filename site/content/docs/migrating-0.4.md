+++
title = "Migrating to 0.4"
description = "Upgrade path from schemreg 0.3.x to 0.4.0, including the critical Protobuf message-index framing fix."
weight = 13
+++

Most upgrades from 0.3.x are a version bump. The sections below are the ones
that need a code edit or a decision.

For the complete list of changes see the
[CHANGELOG](https://github.com/hupe1980/schemreg/blob/main/CHANGELOG.md).

## 1. Protobuf bytes changed — and 0.3.0's were wrong

**If you produce or consume Protobuf messages, read this.**

0.3.0 wrote the message-index element count as a plain unsigned varint. The
Confluent serde ZigZag-encodes it, and writes the common path `[0]` as a single
`0x00` byte. 0.3.0 did neither.

| Direction | 0.3.0 behaviour |
|---|---|
| schemreg → Java/Python/Go/.NET | consumer reads `size = -1` and throws — message undeliverable |
| Java → schemreg, path `[0]` | index reported as `[]` instead of `[0]` |
| Java → schemreg, path `[1]` | count misread as 2, a payload byte consumed as an index — **silent corruption** |

**Migration: none for correct code.** No conforming consumer accepted 0.3.0's
bytes, so no working integration regresses.

**If you have 0.3.0-produced Protobuf messages retained on a topic**, they are
not readable by 0.4.0 or by any other client — they were never readable by any
other client. Re-produce them, or decode them with a one-off shim that strips
`[0x01, 0x00]` before handing the remainder to your Protobuf runtime.

While you are here, stop hand-writing index arrays — enable the `protobuf`
feature and let the descriptor supply the path:

```rust,ignore
let encoder = ProtobufSchemaEncoder::builder()
    .registry(registry)
    .schema(PROTO_SOURCE)
    .descriptor(Order::default().descriptor())
    .build()?;
```

---

## 2. Glue errors are no longer all `Network`

0.3.0 mapped every AWS SDK failure to `SchemaRegError::Network`, so
`EntityNotFoundException` reported `is_retryable() == true` and a retry loop
never terminated.

```rust,ignore
// 0.3.x — this matched everything, including permanent failures
match err {
    SchemaRegError::Network(_) => retry(),
    _ => give_up(),
}

// 0.4.0 — prefer the predicates; they are correct on every backend
if err.is_not_found()      { /* permanent: the schema does not exist */ }
else if err.is_auth_error() { /* permanent: rotate credentials */ }
else if err.is_retryable()  { /* transient: back off */ }
```

Glue service codes now map to: `EntityNotFoundException` → `Api(40401)`,
`AccessDeniedException` → `Auth`, `InvalidInputException` → `Api(42201)`,
throttling → `Http(429)`, `InternalServiceException` → `Http(5xx)`.

---

## 3. `is_retryable()` now covers all 5xx

It was `429 | 503`; it is now `429 | 500..=599`, matching the crate's own
internal retry policy. A 502 from a gateway was previously retried internally
and then reported to you as permanent.

No code change needed — but if you special-cased 502 as permanent, remove that.

---

## 4. Subject validation is stricter

Rejected with a `Config` error, before any network call:

- empty subjects
- subjects over 512 bytes
- `.` or `..` path segments, including percent-encoded forms

None of these was ever a valid registry subject. If you hit this, you have found
a bug in your subject derivation — or an injection attempt.

---

## 5. `aws-lc-rs` feature removed

It was a no-op. `reqwest`'s `rustls-tls-webpki-roots` pins **ring** regardless,
so enabling the feature added a large C build and changed nothing.

```toml
# Before
schemreg = { version = "0.3", features = ["confluent", "aws-lc-rs"] }
# After
schemreg = { version = "0.4", features = ["confluent"] }
```

To genuinely use `aws-lc-rs`, install it as the process-default `CryptoProvider`
in your application and depend on `reqwest` with a `-no-provider` feature. See
[Security → Crypto provider](@/docs/security.md#crypto-provider).

---

## 6. `native-tls-roots` no longer implies `confluent`

It is a `reqwest` passthrough and applies equally to `apicurio`. If you relied
on it to pull in the Confluent client, enable `confluent` explicitly:

```toml
schemreg = { version = "0.4", features = ["confluent", "native-tls-roots"] }
```

---

## 7. Credentials over cleartext HTTP

Now a hard error for any non-loopback host. `http://localhost:8081` with basic
auth still works (with a warning) — that is the standard local-development
setup. A deployed registry needs HTTPS.

---

## 8. `CachedGlueSchemaRegistry::warm_cache` signature

Was `&[GlueSchemaVersionId]`, now `impl IntoIterator<Item = impl Into<GlueSchemaVersionId>>`.
Existing slice call sites still compile — `&[T]` implements `IntoIterator`.

---

## New things worth adopting

| Feature | Why |
|---|---|
| `RetryPolicy` | Configurable retries with **jitter** (new). `RetryPolicy::none()` if you retry at a higher layer |
| `max_concurrent_requests` | A hard ceiling for cold starts that fan out to thousands of distinct schema IDs |
| `AvroSchemaDecoder::with_reader_schema` | Avro schema resolution — defaulted fields, dropped fields, numeric promotion |
| `protobuf` feature | Descriptor-derived message-index paths; wrong-type rejection |
| `health_check()` | Now on the Glue trait too; usable for readiness probes on all three backends |
| `Arc<dyn DynSchemaRegistryClient>` | Now also implements `SchemaRegistryClient`, so erasure composes with `CachedSchemaRegistry` and the encoders |
