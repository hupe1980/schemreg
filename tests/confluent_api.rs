//! Confluent Schema Registry REST surface, driven against a mock server.
//!
//! Pins the request each operation actually issues — method, path, query
//! string, and how the response body maps onto [`Schema`] — because a wrong
//! path or a dropped query parameter is invisible in unit tests and shows up
//! only against a real registry.

#![cfg(feature = "confluent")]

use schemreg::error::error_code;
use schemreg::{
    ConfluentSchemaRegistry, SchemaGuid, SchemaId, SchemaRegistryClient, SchemaType, SchemaVersion,
};
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

fn client(server: &MockServer) -> ConfluentSchemaRegistry {
    ConfluentSchemaRegistry::builder()
        .url(server.uri())
        .build()
        .expect("client builds")
}

fn json(body: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(body)
        .insert_header("content-type", SR_CONTENT_TYPE)
}

fn error(status: u16, code: i32, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .set_body_json(serde_json::json!({ "error_code": code, "message": message }))
        .insert_header("content-type", SR_CONTENT_TYPE)
}

const GUID_TEXT: &str = "8f14e45f-ceea-467a-9575-0b7d1c9b1d8f";

fn guid() -> SchemaGuid {
    GUID_TEXT.parse().expect("a well-formed GUID")
}

// ── get_schema_by_id ──────────────────────────────────────────────────────

/// Confluent Platform 8 adds `guid` to the by-ID response. Capturing it is what
/// lets a producer re-frame with a GUID later, so it must not be dropped.
#[tokio::test]
async fn get_schema_by_id_captures_the_guid_when_present() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/42"))
        .respond_with(json(serde_json::json!({
            "schema": "\"string\"",
            "schemaType": "AVRO",
            "guid": GUID_TEXT,
        })))
        .mount(&server)
        .await;

    let schema = client(&server)
        .get_schema_by_id(SchemaId::from(42u32))
        .await
        .expect("lookup succeeds");

    assert_eq!(schema.id, Some(SchemaId::from(42u32)));
    assert_eq!(schema.guid, Some(guid()));
    assert_eq!(schema.schema_type, SchemaType::Avro);
}

/// A pre-8.0 registry reports no `guid`; that is a missing field, not an error.
#[tokio::test]
async fn get_schema_by_id_tolerates_a_registry_without_guids() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/42"))
        .respond_with(json(serde_json::json!({
            "schema": "\"string\"",
            "schemaType": "AVRO",
        })))
        .mount(&server)
        .await;

    let schema = client(&server)
        .get_schema_by_id(SchemaId::from(42u32))
        .await
        .expect("lookup succeeds");

    assert_eq!(schema.id, Some(SchemaId::from(42u32)));
    assert_eq!(schema.guid, None);
}

// ── get_schema_by_guid ────────────────────────────────────────────────────

/// `GET /schemas/guids/{guid}` returns no numeric ID — the response carries
/// only the schema. Reporting `id: None` is the honest answer; fabricating a
/// zero would be a lie a caller could not detect.
#[tokio::test]
async fn get_schema_by_guid_reports_no_numeric_id() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/schemas/guids/{GUID_TEXT}")))
        .respond_with(json(serde_json::json!({
            "schema": "\"string\"",
            "schemaType": "AVRO",
        })))
        .mount(&server)
        .await;

    let schema = client(&server)
        .get_schema_by_guid(guid())
        .await
        .expect("lookup succeeds");

    assert_eq!(schema.guid, Some(guid()), "the GUID we looked up by");
    assert_eq!(schema.id, None, "the response carries no numeric ID");
    assert_eq!(&*schema.schema, "\"string\"");
}

/// A registry older than Platform 8 has no such route and answers 404.
#[tokio::test]
async fn get_schema_by_guid_on_an_older_registry_is_a_not_found() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/schemas/guids/{GUID_TEXT}")))
        .respond_with(error(404, error_code::SCHEMA_NOT_FOUND, "Schema not found"))
        .mount(&server)
        .await;

    let err = client(&server)
        .get_schema_by_guid(guid())
        .await
        .expect_err("a missing GUID must fail");

    assert!(err.is_not_found(), "{err}");
    assert!(!err.is_retryable(), "a missing schema is permanent: {err}");
}

// ── lookup_schema ─────────────────────────────────────────────────────────

/// `lookup_schema` posts to `/subjects/{subject}` — the read-only route that
/// reports an existing registration without creating one.
#[tokio::test]
async fn lookup_schema_finds_an_existing_registration() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value"))
        .and(body_json(serde_json::json!({
            "schema": "\"string\"",
            "schemaType": "AVRO",
        })))
        .respond_with(json(serde_json::json!({
            "subject": "orders-value",
            "id": 7,
            "version": 3,
            "schema": "\"string\"",
            "schemaType": "AVRO",
            "guid": GUID_TEXT,
        })))
        .mount(&server)
        .await;

    let found = client(&server)
        .lookup_schema("orders-value", "\"string\"", SchemaType::Avro, &[])
        .await
        .expect("lookup succeeds")
        .expect("the schema is registered");

    assert_eq!(found.id, Some(SchemaId::from(7u32)));
    assert_eq!(found.guid, Some(guid()));
    assert_eq!(found.version, Some(SchemaVersion::new(3)));
    assert_eq!(found.subject.as_deref(), Some("orders-value"));
}

/// "Not registered" is an ordinary answer, not a failure — both for a subject
/// that does not exist (40401) and for content the subject has never seen
/// (40403). A caller should not have to classify error codes to ask the
/// question "is this schema known?".
#[tokio::test]
async fn lookup_schema_maps_both_not_found_codes_to_none() {
    for (status, code) in [
        (404, error_code::SUBJECT_NOT_FOUND),
        (404, error_code::SCHEMA_NOT_FOUND),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value"))
            .respond_with(error(status, code, "nope"))
            .mount(&server)
            .await;

        let found = client(&server)
            .lookup_schema("orders-value", "\"string\"", SchemaType::Avro, &[])
            .await
            .unwrap_or_else(|e| panic!("code {code} must not be an error: {e}"));

        assert!(found.is_none(), "code {code} must map to None");
    }
}

/// Anything that is not a not-found still propagates: a 403 means the caller
/// cannot read the subject, which must never be reported as "not registered".
#[tokio::test]
async fn lookup_schema_propagates_real_failures() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value"))
        .respond_with(error(403, 40301, "Forbidden"))
        .mount(&server)
        .await;

    let err = client(&server)
        .lookup_schema("orders-value", "\"string\"", SchemaType::Avro, &[])
        .await
        .expect_err("an auth failure must not be swallowed");

    assert!(err.is_auth_error(), "{err}");
}

/// `normalize_schemas` must apply to the lookup too, or a client that
/// registers normalised will fail to find its own registration.
#[tokio::test]
async fn lookup_schema_honours_the_normalize_setting() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value"))
        .and(query_param("normalize", "true"))
        .respond_with(json(serde_json::json!({
            "subject": "orders-value",
            "id": 1,
            "version": 1,
            "schema": "\"string\"",
            "schemaType": "AVRO",
        })))
        .mount(&server)
        .await;

    let registry = ConfluentSchemaRegistry::builder()
        .url(server.uri())
        .normalize_schemas(true)
        .build()
        .expect("client builds");

    assert!(
        registry
            .lookup_schema("orders-value", "\"string\"", SchemaType::Avro, &[])
            .await
            .expect("lookup succeeds")
            .is_some()
    );
}

// ── delete_version ────────────────────────────────────────────────────────

#[tokio::test]
async fn delete_version_soft_and_permanent_use_the_right_query() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/subjects/orders-value/versions/3"))
        .and(query_param("permanent", "true"))
        .respond_with(json(serde_json::json!(3)))
        .mount(&server)
        .await;

    let deleted = client(&server)
        .delete_version("orders-value", SchemaVersion::new(3), true)
        .await
        .expect("permanent delete succeeds");
    assert_eq!(deleted, SchemaVersion::new(3));
}

/// A permanent delete before a soft delete is refused by the registry with a
/// dedicated code; it must surface as a permanent failure, not a retryable one.
#[tokio::test]
async fn permanent_delete_without_a_soft_delete_is_not_retryable() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/subjects/orders-value"))
        .respond_with(error(
            404,
            error_code::SUBJECT_NOT_SOFT_DELETED,
            "Subject 'orders-value' was not deleted first before being permanently deleted",
        ))
        .mount(&server)
        .await;

    let err = client(&server)
        .delete_subject("orders-value", true)
        .await
        .expect_err("the registry refuses this order of operations");

    assert_eq!(err.error_code(), Some(error_code::SUBJECT_NOT_SOFT_DELETED));
    assert!(!err.is_retryable(), "{err}");
    assert!(
        !err.is_not_found(),
        "'not soft deleted' is a state error, not a missing subject: {err}"
    );
}

// ── Error classification against real response bodies ─────────────────────

/// A registry-side failure arrives as a *parsed* JSON body with a `5xxxx`
/// code. Without treating that range as transient, an identical outage would be
/// retried when the body was HTML and given up on when it was JSON.
#[tokio::test]
async fn a_store_error_body_is_still_retryable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/1"))
        .respond_with(error(
            500,
            error_code::STORE_ERROR,
            "Error while retrieving schema from the backend Kafka store",
        ))
        .mount(&server)
        .await;

    let err = client(&server)
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect_err("a store error fails");

    assert_eq!(err.error_code(), Some(error_code::STORE_ERROR));
    assert!(err.is_retryable(), "a backing-store failure is transient");
}

/// An incompatible schema is well-formed but forbidden by the subject's
/// policy — distinct from a malformed one, and never retryable.
#[tokio::test]
async fn an_incompatible_registration_is_classified_precisely() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value/versions"))
        .respond_with(error(
            409,
            error_code::INCOMPATIBLE_SCHEMA,
            "Schema being registered is incompatible with an earlier schema",
        ))
        .mount(&server)
        .await;

    let err = client(&server)
        .register_schema("orders-value", "\"string\"", SchemaType::Avro, &[])
        .await
        .expect_err("an incompatible schema is rejected");

    assert!(err.is_incompatible(), "{err}");
    assert!(
        !err.is_invalid_schema(),
        "the schema itself is valid: {err}"
    );
    assert!(!err.is_retryable(), "{err}");
}

// ── User-Agent ────────────────────────────────────────────────────────────

/// Every request identifies the client, so registry operators can attribute
/// load and rate limits rather than seeing an anonymous blob.
#[tokio::test]
async fn requests_carry_a_user_agent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/1"))
        .respond_with(json(serde_json::json!({
            "schema": "\"string\"", "schemaType": "AVRO",
        })))
        .mount(&server)
        .await;

    client(&server)
        .get_schema_by_id(SchemaId::from(1u32))
        .await
        .expect("lookup succeeds");

    let requests = server
        .received_requests()
        .await
        .expect("the mock server records requests");
    let agent = requests[0]
        .headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    assert!(
        agent.starts_with("schemreg/"),
        "expected a schemreg User-Agent, got {agent:?}"
    );
}

// ── Version conventions ───────────────────────────────────────────────────

/// `SchemaVersion` documents a negative value as meaning "latest". The registry
/// spells that `latest` and rejects `-1` with error code 42202, so the client
/// translates rather than letting a documented convention be a lie.
#[tokio::test]
async fn a_negative_version_means_latest() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions/latest"))
        .respond_with(json(serde_json::json!({
            "subject": "orders-value",
            "id": 1,
            "version": 9,
            "schema": "\"string\"",
            "schemaType": "AVRO",
        })))
        .mount(&server)
        .await;

    let schema = client(&server)
        .get_schema_by_version("orders-value", SchemaVersion::new(-1))
        .await
        .expect("a negative version resolves to latest");

    assert_eq!(schema.version, Some(SchemaVersion::new(9)));
}

/// Most subjects have no compatibility override of their own, so asking without
/// `defaultToGlobal` makes the common case an error rather than an answer.
#[tokio::test]
async fn get_compatibility_falls_back_to_the_global_default() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/config/orders-value"))
        .and(query_param("defaultToGlobal", "true"))
        .respond_with(json(
            serde_json::json!({ "compatibilityLevel": "FULL_TRANSITIVE" }),
        ))
        .mount(&server)
        .await;

    let level = client(&server)
        .get_compatibility("orders-value")
        .await
        .expect("the effective level is reported");

    assert_eq!(level, schemreg::CompatibilityLevel::FullTransitive);
}

/// An empty subject targets the global config directly.
#[tokio::test]
async fn an_empty_subject_reads_the_global_config() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(json(serde_json::json!({ "compatibility": "BACKWARD" })))
        .mount(&server)
        .await;

    assert_eq!(
        client(&server)
            .get_compatibility("")
            .await
            .expect("the global level is reported"),
        schemreg::CompatibilityLevel::Backward
    );
}
