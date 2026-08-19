//! HTTP transport behaviour, driven against a real local server.
//!
//! Everything here was previously verified by reading the code only — the retry
//! count, the back-off schedule, `Retry-After` honouring, the streaming body
//! cut-off, the request-size cap, and the redirect limit. Those paths run in
//! production on every registry hiccup, so a regression in one of them is
//! invisible until an incident.
//!
//! `wiremock` gives an in-process HTTP server whose request log can be
//! asserted, so "how many requests did we actually make, and how long did we
//! wait between them" becomes a test rather than a claim.

#![cfg(feature = "confluent")]

use std::time::{Duration, Instant};

use schemreg::{ConfluentSchemaRegistry, RetryPolicy, SchemaId, SchemaRegistryClient};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// A registry client pointed at `server`, with jitter off so delay assertions
/// are exact rather than probabilistic.
fn client_for(server: &MockServer, policy: RetryPolicy) -> ConfluentSchemaRegistry {
    ConfluentSchemaRegistry::builder()
        .url(server.uri())
        .retry_policy(policy.jitter(false))
        .build()
        .expect("client builds")
}

fn schema_body() -> serde_json::Value {
    serde_json::json!({ "schema": "\"string\"", "schemaType": "AVRO" })
}

// ── Retry count ───────────────────────────────────────────────────────────

/// The policy says "3 retries", so a permanently failing endpoint must be hit
/// exactly 4 times: the initial attempt plus 3 retries. Not 3, not 5.
#[tokio::test]
async fn retries_exactly_max_retries_times_then_gives_up() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/1"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let client = client_for(
        &server,
        RetryPolicy::new()
            .max_retries(3)
            .base_backoff(Duration::from_millis(1)),
    );

    let err = client
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect_err("a permanent 503 must eventually fail");
    assert!(
        err.is_retryable(),
        "503 must be reported as retryable: {err}"
    );

    assert_eq!(
        server.received_requests().await.unwrap().len(),
        4,
        "1 initial attempt + 3 retries"
    );
}

/// A transient failure that clears must succeed without surfacing an error.
#[tokio::test]
async fn recovers_when_a_transient_failure_clears() {
    let server = MockServer::start().await;

    // wiremock matches the most recently mounted eligible Mock first when
    // `up_to_n_times` is exhausted, so mount the failure with a call budget.
    Mock::given(method("GET"))
        .and(path("/schemas/ids/7"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/7"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", SR_CONTENT_TYPE)
                .set_body_json(schema_body()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = client_for(
        &server,
        RetryPolicy::new()
            .max_retries(3)
            .base_backoff(Duration::from_millis(1)),
    );

    let schema = client
        .get_schema_by_id(SchemaId::from(7u32))
        .await
        .expect("the third attempt succeeds");
    assert_eq!(schema.id, Some(SchemaId::from(7u32)));
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}

/// `RetryPolicy::none()` must make the very first failure terminal — otherwise a
/// caller layering its own retry on top gets a multiplicative blow-up.
#[tokio::test]
async fn retry_policy_none_makes_the_first_failure_terminal() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = client_for(&server, RetryPolicy::none());
    let _ = client.get_schema_by_id(SchemaId::from(1u32)).await;

    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "RetryPolicy::none() must issue exactly one request"
    );
}

// ── Back-off schedule ─────────────────────────────────────────────────────

/// Delays must actually grow. With a 60 ms base and jitter off the schedule is
/// 60 + 120 + 240 = 420 ms of sleeping across three retries.
#[tokio::test]
async fn backoff_grows_exponentially() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let client = client_for(
        &server,
        RetryPolicy::new()
            .max_retries(3)
            .base_backoff(Duration::from_millis(60)),
    );

    let started = Instant::now();
    let _ = client.get_schema_by_id(SchemaId::from(1u32)).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(420),
        "expected >= 420ms of exponential back-off, got {elapsed:?}"
    );
    // Generous upper bound: this asserts the schedule is not accidentally
    // linear or constant, without being sensitive to CI scheduling noise.
    assert!(
        elapsed < Duration::from_secs(5),
        "back-off ran far longer than the schedule allows: {elapsed:?}"
    );
}

// ── Retry-After ───────────────────────────────────────────────────────────

/// A server that asks for a specific pause must get it. Without this, a rolling
/// restart turns into a thundering herd precisely while the registry recovers.
#[tokio::test]
async fn retry_after_delta_seconds_is_honoured_on_503() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).insert_header("retry-after", "1"))
        .mount(&server)
        .await;

    let client = client_for(
        &server,
        RetryPolicy::new()
            .max_retries(1)
            // Base back-off is 1 ms: if Retry-After were ignored the whole call
            // would finish in single-digit milliseconds.
            .base_backoff(Duration::from_millis(1)),
    );

    let started = Instant::now();
    let _ = client.get_schema_by_id(SchemaId::from(1u32)).await;
    assert!(
        started.elapsed() >= Duration::from_millis(1_000),
        "Retry-After: 1 must delay the retry by at least a second, took {:?}",
        started.elapsed()
    );
}

/// 429 was already honoured before 503 was; keep both pinned.
#[tokio::test]
async fn retry_after_is_honoured_on_429() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "1"))
        .mount(&server)
        .await;

    let client = client_for(
        &server,
        RetryPolicy::new()
            .max_retries(1)
            .base_backoff(Duration::from_millis(1)),
    );

    let started = Instant::now();
    let _ = client.get_schema_by_id(SchemaId::from(1u32)).await;
    assert!(started.elapsed() >= Duration::from_millis(1_000));
}

/// A `Retry-After` far beyond the policy ceiling must be clamped, so a hostile
/// or mistaken header cannot wedge the caller for a day.
#[tokio::test]
async fn retry_after_is_clamped_to_max_backoff() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).insert_header("retry-after", "86400"))
        .mount(&server)
        .await;

    let client = client_for(
        &server,
        RetryPolicy::new()
            .max_retries(1)
            .max_backoff(Duration::from_millis(80)),
    );

    let started = Instant::now();
    let _ = client.get_schema_by_id(SchemaId::from(1u32)).await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "Retry-After: 86400 must be clamped to max_backoff"
    );
}

/// `honor_retry_after(false)` must fall back to the policy's own schedule.
#[tokio::test]
async fn retry_after_can_be_ignored_by_policy() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).insert_header("retry-after", "30"))
        .mount(&server)
        .await;

    let client = client_for(
        &server,
        RetryPolicy::new()
            .max_retries(1)
            .honor_retry_after(false)
            .base_backoff(Duration::from_millis(1)),
    );

    let started = Instant::now();
    let _ = client.get_schema_by_id(SchemaId::from(1u32)).await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "with honor_retry_after(false) the 30s header must be ignored"
    );
}

// ── Non-retryable statuses ────────────────────────────────────────────────

/// 4xx responses are the server's final answer. Retrying them wastes the
/// caller's budget and, for 401/403, can trip account lockout.
#[tokio::test]
async fn client_errors_are_never_retried() {
    for status in [400u16, 401, 403, 404, 409, 422] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(status)
                    .insert_header("content-type", "application/json")
                    .set_body_json(serde_json::json!({
                        "error_code": 40403, "message": "nope"
                    })),
            )
            .mount(&server)
            .await;

        let client = client_for(
            &server,
            RetryPolicy::new()
                .max_retries(3)
                .base_backoff(Duration::from_millis(1)),
        );
        let err = client
            .get_schema_by_id(SchemaId::from(1u32))
            .await
            .expect_err("4xx must be an error");
        assert!(!err.is_retryable(), "HTTP {status} must not be retryable");
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "HTTP {status} must not be retried"
        );
    }
}

/// 401/403 must surface as `Auth`, which callers use to trigger credential
/// rotation rather than a retry.
#[tokio::test]
async fn unauthorized_maps_to_an_auth_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("content-type", "application/json")
                .set_body_json(serde_json::json!({
                    "error_code": 40101, "message": "bad credentials"
                })),
        )
        .mount(&server)
        .await;

    let err = client_for(&server, RetryPolicy::none())
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .unwrap_err();
    assert!(err.is_auth_error(), "{err}");
    assert!(!err.is_retryable());
}

/// The Confluent `error_code` must survive into a structured `Api` error so
/// `is_not_found()` works.
#[tokio::test]
async fn confluent_error_codes_are_preserved() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(404)
                .insert_header("content-type", "application/json")
                .set_body_json(serde_json::json!({
                    "error_code": 40403, "message": "Schema not found"
                })),
        )
        .mount(&server)
        .await;

    let err = client_for(&server, RetryPolicy::none())
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "{err}");
    assert!(err.to_string().contains("Schema not found"));
}

// ── Body limits ───────────────────────────────────────────────────────────

/// A response larger than the 16 MiB cap must be refused, and the refusal must
/// come from the size guard rather than from a downstream JSON parse blowing up.
#[tokio::test]
async fn oversized_response_body_is_rejected() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", SR_CONTENT_TYPE)
                // 17 MiB: past the 16 MiB cap. wiremock sets an honest
                // Content-Length, so the pre-read guard is what fires.
                .set_body_bytes(vec![b'x'; 17 * 1024 * 1024]),
        )
        .mount(&server)
        .await;

    let err = client_for(&server, RetryPolicy::none())
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect_err("a 17 MiB response must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("limit") || msg.contains("Content-Length"),
        "the size guard must be what rejects it, got: {msg}"
    );
}

/// A response comfortably under the cap must still work — the guard must not
/// be so eager that it rejects a large-but-legal schema.
#[tokio::test]
async fn large_but_legal_response_body_is_accepted() {
    let server = MockServer::start().await;
    // A ~1 MiB schema string: big, but well within the 16 MiB cap.
    let big_schema = format!("\"{}\"", "x".repeat(1024 * 1024));
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", SR_CONTENT_TYPE)
                .set_body_json(serde_json::json!({
                    "schema": big_schema, "schemaType": "AVRO"
                })),
        )
        .mount(&server)
        .await;

    let schema = client_for(&server, RetryPolicy::none())
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect("a 1 MiB schema is legal");
    assert!(schema.schema.len() > 1024 * 1024);
}

/// A request body over the 4 MiB cap must be refused locally — no socket.
#[tokio::test]
async fn oversized_request_body_is_rejected_locally() {
    let server = MockServer::start().await;
    // No mock is mounted: any request that escapes is an immediate failure.

    let huge_schema = format!("\"{}\"", "x".repeat(5 * 1024 * 1024));
    let err = client_for(&server, RetryPolicy::none())
        .register_schema(
            "orders-value",
            &huge_schema,
            schemreg::SchemaType::Avro,
            &[],
        )
        .await
        .expect_err("a 5 MiB schema must be refused");

    assert!(err.is_config_error(), "{err}");
    assert!(err.to_string().contains("exceeds"), "{err}");
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "an oversized body must never reach the network"
    );
}

// ── Redirects ─────────────────────────────────────────────────────────────

/// A redirect chain longer than the 3-hop limit must fail rather than being
/// followed indefinitely.
#[tokio::test]
async fn redirect_chains_are_bounded() {
    let server = MockServer::start().await;
    let base = server.uri();

    // /hop/N redirects to /hop/N+1, forever.
    struct Hop(String);
    impl Respond for Hop {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let n: u32 = req
                .url
                .path()
                .rsplit('/')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/hop/{}", self.0, n + 1).as_str())
        }
    }

    Mock::given(method("GET"))
        .respond_with(Hop(base.clone()))
        .mount(&server)
        .await;

    let client = client_for(&server, RetryPolicy::none());
    let err = client
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect_err("an endless redirect chain must fail");
    assert!(err.is_network_error(), "{err}");

    // 1 original + at most MAX_REDIRECTS follows. The exact figure is reqwest's
    // to decide; what matters is that it is small and finite.
    let hops = server.received_requests().await.unwrap().len();
    assert!(
        (1..=5).contains(&hops),
        "redirects must be tightly bounded, saw {hops} requests"
    );
}

// ── Auth headers ──────────────────────────────────────────────────────────

/// Basic auth must be sent, correctly base64-encoded.
///
/// The mock server is on loopback, which is exactly the case the builder
/// permits cleartext credentials for — so this also pins the loopback exemption.
#[tokio::test]
async fn basic_auth_header_is_sent() {
    let server = MockServer::start().await;
    // base64("alice:s3cret") == "YWxpY2U6czNjcmV0"
    Mock::given(method("GET"))
        .and(header("authorization", "Basic YWxpY2U6czNjcmV0"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", SR_CONTENT_TYPE)
                .set_body_json(schema_body()),
        )
        .expect(1)
        .mount(&server)
        .await;

    ConfluentSchemaRegistry::builder()
        .url(server.uri())
        .basic_auth("alice", "s3cret")
        .retry_policy(RetryPolicy::none())
        .build()
        .expect("cleartext auth is permitted on loopback")
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect("the Basic auth header must match");
}

/// Bearer tokens must be sent verbatim behind the `Bearer ` prefix.
#[tokio::test]
async fn bearer_auth_header_is_sent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer tok-123"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", SR_CONTENT_TYPE)
                .set_body_json(schema_body()),
        )
        .expect(1)
        .mount(&server)
        .await;

    ConfluentSchemaRegistry::builder()
        .url(server.uri())
        .bearer_token("tok-123")
        .retry_policy(RetryPolicy::none())
        .build()
        .expect("cleartext auth is permitted on loopback")
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect("the Bearer auth header must match");
}

/// With no auth configured, no `Authorization` header may be sent at all —
/// a stray empty header can trip strict gateways.
#[tokio::test]
async fn no_authorization_header_when_unauthenticated() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::header_exists("accept"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", SR_CONTENT_TYPE)
                .set_body_json(schema_body()),
        )
        .mount(&server)
        .await;

    client_for(&server, RetryPolicy::none())
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect("request succeeds");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].headers.get("authorization").is_none(),
        "no Authorization header may be sent when no auth is configured"
    );
    assert_eq!(
        requests[0].headers.get("accept").unwrap(),
        SR_CONTENT_TYPE,
        "the schema-registry media type must be requested"
    );
}

// ── Content-Type handling ─────────────────────────────────────────────────

/// A 2xx that is not JSON is a misrouted request (a proxy error page, a login
/// redirect), not a schema. Parsing it as one would produce a confusing error
/// far from the cause.
#[tokio::test]
async fn non_json_success_response_is_rejected() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/html")
                .set_body_string("<html>login</html>"),
        )
        .mount(&server)
        .await;

    let err = client_for(&server, RetryPolicy::none())
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Content-Type"), "{err}");
}

/// `PUT /config/{subject}` returning `204 No Content` — as Karapace and some
/// proxies do — must count as success. Demanding a JSON echo would turn a
/// completed write into a spurious error.
#[tokio::test]
async fn empty_204_response_to_a_config_write_is_success() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/config/orders-value"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    client_for(&server, RetryPolicy::none())
        .set_compatibility("orders-value", schemreg::CompatibilityLevel::Full)
        .await
        .expect("204 No Content must be treated as success");
}

// ── Concurrency ceiling ───────────────────────────────────────────────────

/// With a ceiling of 1, requests must serialise. Without the limiter all 8
/// would be in flight simultaneously and the server would see a burst.
#[tokio::test]
async fn max_concurrent_requests_serialises_outbound_calls() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", SR_CONTENT_TYPE)
                .set_body_json(schema_body())
                .set_delay(Duration::from_millis(40)),
        )
        .mount(&server)
        .await;

    let client = Arc::new(
        ConfluentSchemaRegistry::builder()
            .url(server.uri())
            .retry_policy(RetryPolicy::none())
            .max_concurrent_requests(1)
            .build()
            .expect("client builds"),
    );

    let done = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut handles = Vec::new();
    for id in 1u32..=8 {
        let client = Arc::clone(&client);
        let done = Arc::clone(&done);
        handles.push(tokio::spawn(async move {
            let _ = client.get_schema_by_id(SchemaId::from(id)).await;
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(done.load(Ordering::SeqCst), 8);
    // 8 serialised 40 ms responses cannot finish in under ~320 ms. Fully
    // concurrent they would finish in ~40 ms.
    assert!(
        started.elapsed() >= Duration::from_millis(280),
        "a ceiling of 1 must serialise the 8 calls, took {:?}",
        started.elapsed()
    );
}
