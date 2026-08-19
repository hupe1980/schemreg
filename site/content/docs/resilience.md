+++
title = "Resilience"
description = "Retry policy and error classification in schemreg: jittered exponential back-off, Retry-After handling, and a uniform is_retryable() contract across Confluent, Apicurio, and AWS Glue."
weight = 8
+++

Retry is built in to every HTTP request the Confluent and Apicurio clients
issue, and the classification it uses is the same one exposed to callers — so an
outer retry loop never re-retries something already given up on for a permanent
reason.

## What is retried

| Scenario | Behaviour |
|---|---|
| HTTP 429 | Retried; `Retry-After` honoured |
| HTTP 5xx | Retried; `Retry-After` honoured, else exponential back-off |
| Registry `5xxxx` error codes | Retried — a failed backing store, an internal timeout, a leaderless cluster |
| Transport errors | Retried (connection reset, timeout, DNS) |
| 4xx, auth, invalid or incompatible schema | **Never** retried |

```rust,ignore
use std::time::Duration;
use schemreg::RetryPolicy;

let registry = ConfluentSchemaRegistry::builder()
    .url("https://registry.example.com")
    .retry_policy(RetryPolicy::new().max_retries(5).base_backoff(Duration::from_millis(50)))
    // Coalescing collapses concurrent misses for the *same* ID. This bounds the
    // other case: a cold start fanning out to thousands of *distinct* IDs.
    .max_concurrent_requests(32)
    .connect_timeout(Duration::from_secs(3))
    .request_timeout(Duration::from_secs(30))
    .build()?;
```

Defaults: 3 retries, 100 ms base, doubling, capped at 60 s. Use
`RetryPolicy::none()` when the calling layer already retries, so the two do not
multiply.

## Jitter

Back-off uses **equal jitter** — `delay/2 + random(0, delay/2)`.

Without it, every client that saw the same 503 retries at exactly 100 ms,
200 ms, 400 ms, reconverging into synchronised waves that hit the registry
precisely while it is recovering. Jitter spreads them out. It is deliberately
*equal* rather than *full* jitter so the first retry still waits a meaningful
minimum instead of occasionally firing immediately.

## `Retry-After`

Both forms of RFC 9110 §10.2.3 are understood: delta-seconds
(`Retry-After: 120`) and the IMF-fixdate HTTP-date form.

A server-supplied value is **never jittered and never shortened** — the server
asked for at least that long — but it *is* clamped to `max_backoff`, so a
hostile or mistaken `Retry-After: 86400` cannot wedge the caller. A date in the
past yields an immediate retry rather than a parse failure, because that is what
clock skew looks like.

The obsolete RFC 850 and asctime date formats are deliberately not accepted. No
schema registry emits them, and accepting a looser grammar for a value that
controls how long a caller sleeps is not a trade worth making.

## Classifying errors yourself

`SchemaRegError::is_retryable()` is the contract for your own retry loop, and it
is **uniform across backends** — including AWS Glue, where SDK errors are
classified by service code rather than collapsed into one transport variant:

```rust,ignore
match registry.get_schema_by_id(id).await {
    Ok(schema) => { /* … */ }
    Err(e) if e.is_not_found()  => { /* permanent: the schema does not exist */ }
    Err(e) if e.is_auth_error() => { /* permanent: rotate credentials */ }
    Err(e) if e.is_retryable()  => { /* transient: back off and try again */ }
    Err(e) => return Err(e),
}
```

Classify with the predicates rather than by matching on variants or message
text. Message text is localised, reworded between releases, and different again
on Karapace.

| Predicate | True for |
|---|---|
| `is_not_found` | Error codes 40401/40402/40403, **and** a bare HTTP 404 from a proxy or a registry without the route |
| `is_incompatible` | 40901 — well-formed, but the subject's policy forbids the change |
| `is_invalid_schema` | 42201, 42209 — malformed, or over the registry's size limit |
| `is_auth_error` | HTTP 401 / 403 |
| `is_retryable` | Transport, 429, 5xx, and the registry's `5xxxx` range |
| `is_not_supported` | The backend does not implement that operation |

`is_incompatible` and `is_invalid_schema` are deliberately distinct: a schema
the subject's compatibility policy forbids and a schema that is malformed are
different problems with different fixes. Neither is retryable.

`NotSupported` is never retryable either, which is what lets a caller tell "this
backend cannot do that" apart from "the backend is down".

## Redirects and bounds

Redirects are limited to 3. `reqwest` independently strips `Authorization` on
cross-origin redirects, so credentials cannot follow a hostile `Location`.

Request bodies are capped at 4 MiB and responses at 16 MiB, the latter enforced
from `Content-Length` *before* reading and again while streaming. See
[Security](@/docs/security.md) for the full list of bounds.

## Health checks

Every client exposes a lightweight probe for readiness endpoints and startup
preflight:

```rust,ignore
registry.health_check().await?;
```

It uses the cheapest call that exercises connectivity, credentials, and
authorisation on each backend: `GET /subjects?limit=1` on Confluent,
`GET /search/artifacts?limit=1` on Apicurio, and `GetRegistry` on AWS Glue.
