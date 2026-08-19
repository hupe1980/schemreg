//! Apicurio Registry v3 REST surface, driven against a mock server.
//!
//! Every assertion here is a path, a query parameter, or a JSON field name
//! taken from Apicurio's own `openapi.json` for the v3 Core Registry API. A
//! wrong route is invisible to unit tests and shows up only against a live
//! registry, which is exactly the failure this file exists to catch.

#![cfg(feature = "apicurio")]

use schemreg::apicurio::ApicurioSchemaRegistry;
use schemreg::{CompatibilityLevel, SchemaId, SchemaRegistryClient, SchemaType, SchemaVersion};
use wiremock::matchers::{body_string, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const V3: &str = "/apis/registry/v3";
const AVRO: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;

fn client(server: &MockServer) -> ApicurioSchemaRegistry {
    ApicurioSchemaRegistry::builder()
        .url(server.uri())
        .build()
        .expect("client builds")
}

fn json(body: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(body)
        .insert_header("content-type", "application/json")
}

// ── get_schema_by_id ──────────────────────────────────────────────────────

/// `/ids/globalIds/{id}` is the only route that still reports the artifact type
/// in a header, and only when asked — so the query parameter must be sent.
#[tokio::test]
async fn get_schema_by_id_asks_for_the_artifact_type_header() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{V3}/ids/globalIds/7")))
        .and(query_param("returnArtifactType", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(AVRO)
                .insert_header("X-Registry-ArtifactType", "AVRO"),
        )
        .mount(&server)
        .await;

    let schema = client(&server)
        .get_schema_by_id(SchemaId::from(7u32))
        .await
        .expect("lookup succeeds");

    assert_eq!(schema.id, Some(SchemaId::from(7u32)));
    assert_eq!(schema.schema_type, SchemaType::Avro);
    assert_eq!(&*schema.schema, AVRO);
    // Apicurio's native API has no GUID concept — nothing may be invented.
    assert_eq!(schema.guid, None);
}

/// Dereferencing is off unless asked for, because it changes the bytes returned.
#[tokio::test]
async fn dereferencing_is_off_by_default_and_opt_in_per_client() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{V3}/ids/globalIds/7")))
        .and(query_param("references", "DEREFERENCE"))
        .respond_with(ResponseTemplate::new(200).set_body_string(AVRO))
        .mount(&server)
        .await;

    // The default client sends no `references` parameter, so the mock above
    // does not match and the request 404s.
    assert!(
        client(&server)
            .get_schema_by_id(SchemaId::from(7u32))
            .await
            .is_err(),
        "the default client must not ask for dereferencing"
    );

    let dereferencing = ApicurioSchemaRegistry::builder()
        .url(server.uri())
        .dereference_references(true)
        .build()
        .expect("client builds");
    assert!(
        dereferencing
            .get_schema_by_id(SchemaId::from(7u32))
            .await
            .is_ok()
    );
}

// ── get_latest_schema / get_schema_by_version ─────────────────────────────

/// Registry v3 removed the identity headers from content responses, so identity
/// comes from version metadata and content from the sibling route. Both are
/// pinned here, including the `branch=latest` expression.
#[tokio::test]
async fn get_latest_schema_reads_metadata_then_content() {
    let server = MockServer::start().await;
    let base = format!("{V3}/groups/default/artifacts/orders-value/versions/branch=latest");

    Mock::given(method("GET"))
        .and(path(base.clone()))
        .respond_with(json(serde_json::json!({
            "globalId": 42,
            "version": "3",
            "artifactType": "AVRO",
            "groupId": "default",
            "artifactId": "orders-value",
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{base}/content")))
        .respond_with(ResponseTemplate::new(200).set_body_string(AVRO))
        .mount(&server)
        .await;

    let schema = client(&server)
        .get_latest_schema("orders-value")
        .await
        .expect("lookup succeeds");

    assert_eq!(schema.id, Some(SchemaId::from(42u32)));
    assert_eq!(schema.version, Some(SchemaVersion::new(3)));
    assert_eq!(schema.subject.as_deref(), Some("default/orders-value"));
    assert_eq!(&*schema.schema, AVRO);
}

/// A negative version means "latest" throughout this crate; Apicurio spells it
/// `branch=latest`, and a literal `-1` is rejected by the server.
#[tokio::test]
async fn a_negative_version_resolves_to_the_latest_branch() {
    let server = MockServer::start().await;
    let base = format!("{V3}/groups/g/artifacts/a/versions/branch=latest");
    Mock::given(method("GET"))
        .and(path(base.clone()))
        .respond_with(json(
            serde_json::json!({ "globalId": 1, "artifactType": "AVRO" }),
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{base}/content")))
        .respond_with(ResponseTemplate::new(200).set_body_string(AVRO))
        .mount(&server)
        .await;

    assert!(
        client(&server)
            .get_schema_by_version("g/a", SchemaVersion::new(-1))
            .await
            .is_ok()
    );
}

// ── lookup_schema ─────────────────────────────────────────────────────────

/// The read-only content search: scoped to the artifact, not canonicalised, and
/// carrying the schema text as the raw request body.
#[tokio::test]
async fn lookup_schema_posts_the_content_to_the_version_search() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("{V3}/search/versions")))
        .and(query_param("canonical", "false"))
        .and(query_param("artifactType", "AVRO"))
        .and(query_param("groupId", "default"))
        .and(query_param("artifactId", "orders-value"))
        .and(body_string(AVRO))
        .respond_with(json(serde_json::json!({
            "count": 1,
            "versions": [{
                "globalId": 55,
                "version": "2",
                "artifactType": "AVRO",
                "groupId": "default",
                "artifactId": "orders-value",
                "contentId": 1,
                "owner": "x",
                "createdOn": "2026-01-01T00:00:00Z",
                "state": "ENABLED",
            }],
        })))
        .mount(&server)
        .await;

    let found = client(&server)
        .lookup_schema("orders-value", AVRO, SchemaType::Avro, &[])
        .await
        .expect("lookup succeeds")
        .expect("the schema is registered");

    assert_eq!(found.id, Some(SchemaId::from(55u32)));
    assert_eq!(found.version, Some(SchemaVersion::new(2)));
    assert_eq!(&*found.schema, AVRO);
}

/// "Not registered" must be `Ok(None)`, both for an empty result set and for a
/// group or artifact that does not exist at all.
#[tokio::test]
async fn lookup_schema_reports_absence_as_ok_none() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("{V3}/search/versions")))
        .respond_with(json(serde_json::json!({ "count": 0, "versions": [] })))
        .mount(&server)
        .await;
    assert!(
        client(&server)
            .lookup_schema("orders-value", AVRO, SchemaType::Avro, &[])
            .await
            .expect("an empty result is not a failure")
            .is_none()
    );

    let missing = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("{V3}/search/versions")))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "title": "No such artifact",
        })))
        .mount(&missing)
        .await;
    assert!(
        client(&missing)
            .lookup_schema("nope-value", AVRO, SchemaType::Avro, &[])
            .await
            .expect("a missing artifact is not a failure")
            .is_none()
    );
}

// ── register_schema ───────────────────────────────────────────────────────

/// Registration must be idempotent, which is what `FIND_OR_CREATE_VERSION` buys.
#[tokio::test]
async fn register_schema_is_idempotent_via_if_exists() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("{V3}/groups/default/artifacts")))
        .and(query_param("ifExists", "FIND_OR_CREATE_VERSION"))
        .respond_with(json(serde_json::json!({
            "version": { "globalId": 9 },
        })))
        .mount(&server)
        .await;

    let id = client(&server)
        .register_schema("orders-value", AVRO, SchemaType::Avro, &[])
        .await
        .expect("registration succeeds");
    assert_eq!(id, SchemaId::from(9u32));
}

/// An Apicurio global ID is an `int64`; the Confluent wire format carries a
/// `u32`. Truncating would produce a valid-looking ID pointing at nothing.
#[tokio::test]
async fn an_out_of_range_global_id_is_rejected_rather_than_truncated() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("{V3}/groups/default/artifacts")))
        .respond_with(json(serde_json::json!({
            "version": { "globalId": 4_294_967_296i64 },
        })))
        .mount(&server)
        .await;

    let err = client(&server)
        .register_schema("orders-value", AVRO, SchemaType::Avro, &[])
        .await
        .expect_err("an out-of-range global ID must not be truncated");
    assert!(err.is_invalid_state(), "{err}");
}

// ── deletion ──────────────────────────────────────────────────────────────

/// `delete_subject` reports the versions it removed, read before the delete —
/// not an empty list standing in for "we did not look".
#[tokio::test]
async fn delete_subject_reports_the_versions_it_removed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/versions"
        )))
        .respond_with(json(serde_json::json!({
            "count": 2,
            "versions": [{ "version": "1" }, { "version": "2" }],
        })))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path(format!("{V3}/groups/default/artifacts/orders-value")))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let deleted = client(&server)
        .delete_subject("orders-value", true)
        .await
        .expect("delete succeeds");
    assert_eq!(deleted, vec![SchemaVersion::new(1), SchemaVersion::new(2)]);
}

#[tokio::test]
async fn delete_version_targets_the_version_route() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/versions/2"
        )))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let deleted = client(&server)
        .delete_version("orders-value", SchemaVersion::new(2), false)
        .await
        .expect("delete succeeds");
    assert_eq!(deleted, SchemaVersion::new(2));
}

/// Version deletion is disabled by default in Apicurio; a 405 must surface as
/// such rather than as a mystery.
#[tokio::test]
async fn a_registry_with_version_deletion_disabled_reports_405() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/versions/2"
        )))
        .respond_with(ResponseTemplate::new(405))
        .mount(&server)
        .await;

    let err = client(&server)
        .delete_version("orders-value", SchemaVersion::new(2), false)
        .await
        .expect_err("a disabled route must fail");
    assert_eq!(err.status(), Some(405), "{err}");
    assert!(!err.is_retryable(), "{err}");
}

// ── compatibility rules ───────────────────────────────────────────────────

#[tokio::test]
async fn get_compatibility_reads_the_artifact_rule() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/rules/COMPATIBILITY"
        )))
        .respond_with(json(
            serde_json::json!({ "ruleType": "COMPATIBILITY", "config": "BACKWARD_TRANSITIVE" }),
        ))
        .mount(&server)
        .await;

    assert_eq!(
        client(&server)
            .get_compatibility("orders-value")
            .await
            .expect("rule reads"),
        CompatibilityLevel::BackwardTransitive
    );
}

/// An empty subject means "the registry default" on both backends. Apicurio
/// keeps that under `/admin/rules`.
#[tokio::test]
async fn an_empty_subject_addresses_the_global_rule() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("{V3}/admin/rules/COMPATIBILITY")))
        .respond_with(json(
            serde_json::json!({ "ruleType": "COMPATIBILITY", "config": "FULL" }),
        ))
        .mount(&server)
        .await;

    assert_eq!(
        client(&server)
            .get_compatibility("")
            .await
            .expect("global rule reads"),
        CompatibilityLevel::Full
    );
}

/// Apicurio splits creating a rule from updating one: `PUT` 404s when the rule
/// has never been configured, which is the common case for a fresh artifact.
/// The client must fall through to `POST` rather than surfacing the 404.
#[tokio::test]
async fn set_compatibility_creates_the_rule_when_it_does_not_exist_yet() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/rules/COMPATIBILITY"
        )))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "title": "No rule named COMPATIBILITY was found",
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/rules"
        )))
        .and(header("content-type", "application/json"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .set_compatibility("orders-value", CompatibilityLevel::Full)
        .await
        .expect("the rule is created on the fly");
}

/// When the rule already exists, `PUT` succeeds and no `POST` is issued.
#[tokio::test]
async fn set_compatibility_updates_an_existing_rule_without_creating_it() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/rules/COMPATIBILITY"
        )))
        .respond_with(json(
            serde_json::json!({ "ruleType": "COMPATIBILITY", "config": "FULL" }),
        ))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/rules"
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    client(&server)
        .set_compatibility("orders-value", CompatibilityLevel::Full)
        .await
        .expect("the existing rule is updated");
}

// ── check_compatibility ───────────────────────────────────────────────────

#[tokio::test]
async fn check_compatibility_posts_to_the_latest_branch() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!(
            "{V3}/groups/default/artifacts/orders-value/versions/branch=latest/compatibility"
        )))
        .respond_with(json(serde_json::json!({ "compatible": false })))
        .mount(&server)
        .await;

    assert!(
        !client(&server)
            .check_compatibility("orders-value", AVRO, SchemaType::Avro, &[])
            .await
            .expect("the check completes")
    );
}

// ── pagination ────────────────────────────────────────────────────────────

/// `get_subjects` must walk pages. A single bounded request would silently
/// truncate a registry with more artifacts than one page holds.
#[tokio::test]
async fn get_subjects_follows_every_page() {
    let server = MockServer::start().await;
    let full: Vec<serde_json::Value> = (0..500)
        .map(|i| serde_json::json!({ "artifactId": format!("a{i}"), "groupId": "default" }))
        .collect();

    Mock::given(method("GET"))
        .and(path(format!("{V3}/search/artifacts")))
        .and(query_param("offset", "0"))
        .respond_with(json(serde_json::json!({ "count": 501, "artifacts": full })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{V3}/search/artifacts")))
        .and(query_param("offset", "500"))
        .respond_with(json(serde_json::json!({
            "count": 501,
            "artifacts": [{ "artifactId": "last", "groupId": "other" }],
        })))
        .mount(&server)
        .await;

    let subjects = client(&server)
        .get_subjects()
        .await
        .expect("listing succeeds");
    assert_eq!(subjects.len(), 501);
    assert_eq!(subjects.last().map(String::as_str), Some("other/last"));
}

// ── subject validation ────────────────────────────────────────────────────

/// A subject splits into group and artifact *before* percent-encoding, so a
/// traversal in either component has to be rejected at the boundary.
#[tokio::test]
async fn traversal_in_either_subject_component_is_rejected() {
    let server = MockServer::start().await;
    let client = client(&server);
    for bad in ["../admin", "g/../../admin", "../a", "g/.."] {
        let err = client
            .get_latest_schema(bad)
            .await
            .expect_err("{bad} must be rejected");
        assert!(err.is_config_error(), "{bad}: {err}");
    }
}
