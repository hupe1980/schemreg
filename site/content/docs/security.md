+++
title = "Security"
description = "Threat model and controls: credential handling, path-traversal defence, TLS posture, denial-of-service bounds, schema-reference traversal, and supply-chain gates."
weight = 9
+++

What this crate defends against, how, and what it deliberately does not.

## Threat model

`schemreg` sits between your application and a schema registry. It handles two
kinds of untrusted input:

1. **Message bytes off a Kafka topic** — fully attacker-controlled if any
   producer is compromised or if the topic is multi-tenant.
2. **Registry responses** — trusted less than they look; a compromised or
   misconfigured registry, or anything on the path to it, can return arbitrary
   bytes. Note that a registry response can also *direct further requests*: a
   schema's `references` list names other subjects to fetch, so the response is
   an input to a traversal, not just a value.

And one kind of caller-supplied input that is often attacker-influenced without
anyone noticing: **subject names**, which are frequently derived from topic
names, tenant identifiers, or request fields.

---

## Credentials

| Control | Behaviour |
|---|---|
| In memory | `zeroize::Zeroizing` for username, password, and bearer token — wiped on drop |
| Derived material | The intermediate `user:pass` string *and* its base64 encoding are also `Zeroizing` |
| `Debug` output | Renders `basic(***)` / `bearer(***)`; never the value |
| `tracing` events | No credential is ever a field. Events carry URL, status, attempt, delay, error text |
| In URLs | `user:pass@host` is rejected at construction, authority-scoped so `?q=a@b` still works |
| In headers | Only `Authorization` carries them; the `User-Agent` is a fixed `schemreg/{version}` string with nothing caller-derived in it |
| Over cleartext | A hard `Config` error off-loopback. See below |

### The loopback exemption

Basic/Bearer credentials over `http://` are a **hard error** for any
non-loopback host, and a warning for `localhost`, `127.0.0.0/8`, and `::1`.

This is the same "potentially trustworthy origin" rule browsers apply: loopback
traffic never leaves the machine, so there is no network on which to intercept
it — and `http://localhost:8081` with basic auth is the standard
docker-compose setup.

A private-range address like `10.0.0.5` is **not** exempt. It is still a real
network with real switches.

### What zeroization does not do

`Zeroizing` guarantees the buffer is wiped on drop. It cannot prevent the OS
paging it out beforehand. Mitigating that needs `mlock`, which is
platform-specific and requires privileges — out of scope for a client library.

---

## Path traversal

The percent-encoder deliberately does **not** encode `.`, because
`com.example.Order-value` is the most common real subject shape. That means a
`..` segment would survive encoding, and `DELETE /subjects/..` could be
collapsed by a proxy into `DELETE /subjects` — mass deletion instead of one
subject.

Every operation that interpolates a subject into a URL therefore validates it
first. A subject reaches a URL only after:

- non-empty
- ≤ 512 bytes
- no `.` or `..` segment under either `/` or `\`
- no `.` or `..` segment in its **percent-decoded** form

That last check is defence in depth. This crate encodes `..%2fadmin` correctly,
to the single literal segment `..%252fadmin` — but a gateway that decodes the
path twice recovers `../admin`. Screening the decoded form removes the crate
from that chain entirely.

Apicurio validates both the joined subject *and* each address component
independently, because it splits on `/` before encoding — so `../secrets` would
otherwise become `/groups/../artifacts/secrets` and escape the group boundary
that is Apicurio's whole multi-tenancy mechanism.

Every Apicurio operation routes through one `artifact_id` constructor that
performs those checks, so a route added later cannot skip them by calling
`ArtifactId::from_subject` directly.

---

## TLS

- rustls only. `grep -rn "danger_accept_invalid" src/` returns nothing: there is
  no bypass path, not even a feature-gated one.
- Trust anchors: the Mozilla bundle via webpki-roots, optionally plus the
  platform store via `native-tls-roots`.
- Custom CAs are **additive** — they extend the trust set, they cannot replace
  or disable validation.
- mTLS via `reqwest::Identity`.
- `openssl`, `openssl-sys`, and `native-tls` are **banned in `deny.toml`**, so
  no transitive dependency can quietly swap the TLS stack. Enforced in CI.
- Redirects bounded to 3. reqwest independently strips `Authorization` on
  cross-origin redirects, so credentials cannot follow a hostile `Location`.

### Crypto provider

The provider is **ring**, via `reqwest`'s `rustls-tls-webpki-roots`. There is
deliberately no `aws-lc-rs` feature: `reqwest` pins ring through that feature
regardless, so such a flag would add a large C build and change nothing. To use
`aws-lc-rs`, install it as the process-default `CryptoProvider` in your
application and depend on `reqwest` with a `-no-provider` feature.

---

## Denial of service

| Vector | Control |
|---|---|
| Oversized response | 16 MiB, enforced from `Content-Length` **before** reading and again while streaming |
| Oversized request | 4 MiB cap on schema text, checked before the socket is touched |
| Decompression bomb | Glue ZLIB output capped at 128 MiB **during** decompression |
| Protobuf index explosion | Count capped at 512 before any `Vec::with_capacity` sized from input |
| Varint overflow | Shift-width guard; out-of-domain and negative values rejected |
| Schema-reference chain | Avro **and** JSON Schema dependency closures capped at 32 levels deep and 256 schemas; a visited set makes a reference **cycle** terminate instead of recursing |
| Unbounded cache growth | Every cache is bounded (default 1 000 entries) with FIFO eviction |
| Oversized subject | 512 bytes |
| Retry amplification | Bounded attempts, capped delay, `Retry-After` honoured, **jittered** back-off |
| Connection exhaustion | `max_concurrent_requests` on both builders; coalescing collapses same-ID bursts |
| JSON Schema remote `$ref` | `jsonschema` is built without `resolve-http`, **and** the retriever this crate installs answers only from schemas already fetched via the registry's own `references` list — compilation cannot make outbound requests (no SSRF) |

### Schema-reference traversal

Resolving a schema that uses the `references` mechanism means following a graph
the **registry** controls. Three bounds keep that from becoming a
denial-of-service against the client, and they apply identically to the Avro and
the JSON Schema resolver:

- a visited set keyed by `(subject, version)`, so a cycle `A → B → A`
  terminates and a diamond is fetched once rather than once per path;
- a depth cap of 32, which no real schema graph approaches;
- a total cap of 256 resolved schemas.

Nothing in the Confluent API forbids a cycle, so this is a real shape a registry
can be made to return, not a theoretical one.

For JSON Schema, bounding the traversal to the registry's own reference list is
also what closes the SSRF path. A `$ref` is a URI, and a permissive resolver
would fetch it. The retriever installed here has no network access: it answers
from the fetched closure and from documents the caller supplied to
`dependencies`, and errors on anything else.

### Producer write permissions

`SchemaResolution::AutoRegister` is the default and needs `Subject:Write` — a
usability default, not a security one: a producer holding write credentials
creates schema versions the moment its local schema drifts.
[`LookupOnly`](@/docs/producers.md) needs only `Subject:Read` and turns that
drift into a startup failure. Where the registry is a governance boundary, grant
read-only credentials and set it.

### Retry jitter

Without jitter, every client that saw the same 503 retries at exactly 100 ms,
200 ms, 400 ms — reconverging into synchronised waves that hit the registry
while it is recovering. Back-off uses **equal jitter** (`temp/2 + random(0,
temp/2)`), which spreads clients out while keeping a meaningful minimum delay.

A server-supplied `Retry-After` is never jittered and never shortened — the
server asked for at least that long — but it *is* clamped to `max_backoff`, so a
hostile or mistaken `Retry-After: 86400` cannot wedge the caller.

---

## Supply chain

| Gate | Status |
|---|---|
| `cargo deny check` | advisories, licences, bans, sources — **with `all-features`** |
| `cargo audit --deny warnings` | every push, plus nightly |
| Licence policy | permissive-only allow-list, every entry comment-justified |
| Duplicate versions | denied, with justified skips naming the upstream cause |
| Wildcard versions | denied |
| Yanked crates | denied |
| Source registry | crates.io only; git and path dependencies rejected |
| `Cargo.lock` | committed; the MSRV job builds `--locked` |

With every backend optional, `cargo deny` on the *default* feature set scans
almost nothing. `[graph] all-features = true` is what makes the gate real.

---

## Reporting

Open a private security advisory on the GitHub repository. Please do not file a
public issue for anything that looks exploitable.
