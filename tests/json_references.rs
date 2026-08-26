//! JSON Schema references across subjects.
//!
//! Confluent stores a referencing schema exactly as written, so a document with
//! a `$ref` to another subject is **not** compilable on its own. The decoder has
//! to fetch the closure and compile the set together, and the encoder has to be
//! given the same documents locally.
//!
//! The failure this file guards is quiet: without resolution, a `$ref` either
//! fails to compile (loud) or — worse, with a permissive retriever — resolves to
//! nothing and validates everything.

#![cfg(feature = "json")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use schemreg::{
    EncodeTarget, JsonSchemaDecoder, JsonSchemaEncoder, Result, Schema, SchemaId, SchemaReference,
    SchemaRegistryClient, SchemaType, SchemaVersion, encode_wire_format,
};

const ADDRESS_REF: &str = "https://example.com/address.json";
const CITY_REF: &str = "https://example.com/city.json";

const CITY: &str = r#"{"type":"object","properties":{"name":{"type":"string"}},
                       "required":["name"]}"#;
const ADDRESS: &str = r#"{"type":"object",
    "properties":{"city":{"$ref":"https://example.com/city.json"}},
    "required":["city"]}"#;
const ORDER: &str = r#"{"type":"object",
    "properties":{"id":{"type":"integer"},
                  "shipTo":{"$ref":"https://example.com/address.json"}},
    "required":["id","shipTo"]}"#;

/// A registry holding the three-schema chain `City ← Address ← Order`, counting
/// fetches so the diamond and cycle guards can be observed.
#[derive(Debug)]
struct RefRegistry {
    /// subject → (schema text, its own references)
    subjects: HashMap<String, (String, Vec<SchemaReference>)>,
    fetches: AtomicU32,
}

impl RefRegistry {
    fn chain() -> Self {
        let mut subjects = HashMap::new();
        subjects.insert("city-value".to_string(), (CITY.to_string(), vec![]));
        subjects.insert(
            "address-value".to_string(),
            (
                ADDRESS.to_string(),
                vec![SchemaReference::new(CITY_REF, "city-value", 1i32)],
            ),
        );
        Self {
            subjects,
            fetches: AtomicU32::new(0),
        }
    }

    /// `A → B → A`, which nothing in the Confluent API forbids.
    fn cyclic() -> Self {
        let mut subjects = HashMap::new();
        subjects.insert(
            "a-value".to_string(),
            (
                r#"{"$ref":"b"}"#.to_string(),
                vec![SchemaReference::new("b", "b-value", 1i32)],
            ),
        );
        subjects.insert(
            "b-value".to_string(),
            (
                r#"{"$ref":"a"}"#.to_string(),
                vec![SchemaReference::new("a", "a-value", 1i32)],
            ),
        );
        Self {
            subjects,
            fetches: AtomicU32::new(0),
        }
    }

    fn order_schema(&self, id: u32) -> Arc<Schema> {
        Arc::new(
            Schema::new(id, SchemaType::Json, ORDER).with_references(vec![SchemaReference::new(
                ADDRESS_REF,
                "address-value",
                1i32,
            )]),
        )
    }
}

impl SchemaRegistryClient for RefRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        Ok(self.order_schema(id.as_u32()))
    }
    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        self.get_schema_by_version(subject, SchemaVersion::new(1))
            .await
    }
    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let (text, refs) = self.subjects.get(subject).ok_or_else(|| {
            schemreg::SchemaRegError::api(40401, format!("no such subject: {subject}"))
        })?;
        Ok(Arc::new(
            Schema::new(1u32, SchemaType::Json, text.as_str())
                .with_subject(subject, version)
                .with_references(refs.clone()),
        ))
    }
    async fn register_schema(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<SchemaId> {
        Ok(SchemaId::new(1))
    }
}

// ── Decoder ───────────────────────────────────────────────────────────────

/// The transitive closure has to be fetched and compiled together, or the
/// `$ref` in `Order` never resolves.
#[tokio::test]
async fn a_transitive_reference_chain_validates_end_to_end() {
    let registry = Arc::new(RefRegistry::chain());
    let decoder = JsonSchemaDecoder::with_validation(Arc::clone(&registry));

    let valid = serde_json::json!({ "id": 1, "shipTo": { "city": { "name": "Berlin" } } });
    let framed = encode_wire_format(1u32, &serde_json::to_vec(&valid).expect("serialises"));
    let decoded = decoder
        .decode(framed)
        .await
        .expect("a conforming document must validate through both references");
    assert_eq!(decoded, valid);

    // City ← Address is two subjects; the closure walk fetched exactly those.
    assert_eq!(registry.fetches.load(Ordering::SeqCst), 2);
}

/// The referenced constraint must actually be enforced. If the retriever
/// silently answered "empty schema", this document would pass.
#[tokio::test]
async fn a_violation_inside_a_referenced_schema_is_caught() {
    let registry = Arc::new(RefRegistry::chain());
    let decoder = JsonSchemaDecoder::with_validation(registry);

    // `city.name` is required by the deepest schema in the chain.
    let invalid = serde_json::json!({ "id": 1, "shipTo": { "city": { } } });
    let framed = encode_wire_format(1u32, &serde_json::to_vec(&invalid).expect("serialises"));

    let err = decoder
        .decode(framed)
        .await
        .expect_err("the nested constraint must be enforced");
    assert!(err.is_wire_format_error(), "{err}");
}

/// The closure is compiled once per schema identifier and cached with the
/// validator — a second decode must not re-walk the reference graph.
#[tokio::test]
async fn the_closure_is_resolved_once_per_schema_id() {
    let registry = Arc::new(RefRegistry::chain());
    let decoder = JsonSchemaDecoder::with_validation(Arc::clone(&registry));

    let doc = serde_json::json!({ "id": 1, "shipTo": { "city": { "name": "Berlin" } } });
    let body = serde_json::to_vec(&doc).expect("serialises");
    for _ in 0..4 {
        decoder
            .decode(encode_wire_format(1u32, &body))
            .await
            .expect("decodes");
    }
    assert_eq!(registry.fetches.load(Ordering::SeqCst), 2);
    assert_eq!(decoder.cache_len(), 1);
}

/// A reference cycle must **terminate**. Unlike Avro, a recursive `$ref` is
/// perfectly legal JSON Schema — a tree node referring to itself is the textbook
/// case — so the guarantee here is not "errors" but "the fetch walk stops".
///
/// Without the visited set this recurses until the stack runs out.
#[tokio::test]
async fn a_reference_cycle_terminates() {
    #[derive(Debug)]
    struct Cyclic(Arc<RefRegistry>);
    impl SchemaRegistryClient for Cyclic {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            Ok(Arc::new(
                Schema::new(id, SchemaType::Json, r#"{"$ref":"a"}"#)
                    .with_references(vec![SchemaReference::new("a", "a-value", 1i32)]),
            ))
        }
        async fn get_latest_schema(&self, s: &str) -> Result<Arc<Schema>> {
            self.0.get_latest_schema(s).await
        }
        async fn get_schema_by_version(&self, s: &str, v: SchemaVersion) -> Result<Arc<Schema>> {
            self.0.get_schema_by_version(s, v).await
        }
        async fn register_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<SchemaId> {
            Ok(SchemaId::new(1))
        }
    }

    let registry = Arc::new(RefRegistry::cyclic());
    let decoder = JsonSchemaDecoder::with_validation(Cyclic(Arc::clone(&registry)));

    // Terminates at all — this is the assertion; the outcome may legitimately
    // be either success (a recursive schema constrains nothing here) or a
    // compile error, but it must not hang or overflow.
    let _ = decoder.decode(encode_wire_format(1u32, b"{}")).await;

    // Each of the two subjects in the cycle is fetched exactly once.
    assert_eq!(registry.fetches.load(Ordering::SeqCst), 2);
}

// ── Encoder ───────────────────────────────────────────────────────────────

/// A schema with an external `$ref` cannot be compiled from its own text, so
/// `build()` must fail rather than deferring the surprise to the first encode.
#[test]
fn building_an_encoder_without_the_dependency_fails_at_build_time() {
    let err = JsonSchemaEncoder::builder()
        .registry(Arc::new(RefRegistry::chain()))
        .schema(ORDER)
        .build()
        .expect_err("an unresolvable $ref must fail at build time");
    assert!(err.is_config_error(), "{err}");
}

/// With the dependencies supplied, encoding validates against the full chain.
#[tokio::test]
async fn dependencies_make_the_encoder_validate_through_references() {
    let encoder = JsonSchemaEncoder::builder()
        .registry(Arc::new(RefRegistry::chain()))
        .schema(ORDER)
        .dependencies([(ADDRESS_REF, ADDRESS), (CITY_REF, CITY)])
        .references(vec![SchemaReference::new(
            ADDRESS_REF,
            "address-value",
            1i32,
        )])
        .build()
        .expect("the encoder builds once the dependencies are supplied");

    encoder
        .encode(
            &serde_json::json!({ "id": 1, "shipTo": { "city": { "name": "Berlin" } } }),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .expect("a conforming document encodes");

    let err = encoder
        .encode(
            &serde_json::json!({ "id": 1, "shipTo": { "city": {} } }),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .expect_err("the nested constraint must be enforced on encode too");
    assert!(err.is_wire_format_error(), "{err}");
}

/// Order must not matter: JSON Schema resolves by URI, and the Avro side sorts
/// its set before use, so neither codec cares how the list is written.
#[tokio::test]
async fn dependency_order_is_irrelevant() {
    for deps in [
        vec![(ADDRESS_REF, ADDRESS), (CITY_REF, CITY)],
        vec![(CITY_REF, CITY), (ADDRESS_REF, ADDRESS)],
    ] {
        assert!(
            JsonSchemaEncoder::builder()
                .registry(Arc::new(RefRegistry::chain()))
                .schema(ORDER)
                .dependencies(deps)
                .build()
                .is_ok()
        );
    }
}

/// The same document supplied twice under one `$ref` — a diamond, or one
/// schema registered under two subjects — is one document, not a conflict.
#[test]
fn an_identical_duplicate_dependency_is_accepted() {
    JsonSchemaEncoder::builder()
        .registry(Arc::new(RefRegistry::chain()))
        .schema(ORDER)
        .dependencies([
            (ADDRESS_REF, ADDRESS),
            (CITY_REF, CITY),
            (ADDRESS_REF, ADDRESS),
        ])
        .build()
        .expect("the same document twice is still one document");
}

/// Two *different* documents under one `$ref` cannot both be right. Keeping
/// whichever landed last would validate against a document nobody named.
#[test]
fn contradictory_duplicate_dependencies_are_rejected() {
    const OTHER_ADDRESS: &str = r#"{"type":"object",
        "properties":{"city":{"type":"string"}},"required":["city"]}"#;

    let err = JsonSchemaEncoder::builder()
        .registry(Arc::new(RefRegistry::chain()))
        .schema(ORDER)
        .dependencies([
            (ADDRESS_REF, ADDRESS),
            (CITY_REF, CITY),
            (ADDRESS_REF, OTHER_ADDRESS),
        ])
        .build()
        .expect_err("two documents for one $ref");

    assert!(err.is_config_error(), "{err}");
    assert!(err.to_string().contains(ADDRESS_REF), "{err}");
}

/// Compilation must never reach the network. `jsonschema` is built without
/// `resolve-http`, and the retriever only answers from what was supplied — so a
/// `$ref` nobody provided is a compile error, not an outbound request.
#[test]
fn an_unsupplied_remote_ref_is_an_error_not_a_fetch() {
    let err = JsonSchemaEncoder::builder()
        .registry(Arc::new(RefRegistry::chain()))
        .schema(r#"{"$ref":"https://attacker.example/evil.json"}"#)
        .build()
        .expect_err("a remote $ref must not be fetched");
    assert!(err.is_config_error(), "{err}");
}

/// Validation off means no validator is ever compiled, so a schema with an
/// unresolved reference still decodes — the payload is just JSON.
#[tokio::test]
async fn a_non_validating_decoder_never_resolves_references() {
    let registry = Arc::new(RefRegistry::chain());
    let decoder = JsonSchemaDecoder::new(Arc::clone(&registry));
    let framed = encode_wire_format(1u32, br#"{"anything":true}"#);

    let value = decoder.decode(framed).await.expect("decodes");
    assert_eq!(value, serde_json::json!({ "anything": true }));
    assert_eq!(registry.fetches.load(Ordering::SeqCst), 0);
}

/// A `$ref` written as a bare relative name is resolved by `jsonschema` against
/// a base URI before it reaches the retriever, so the lookup has to match on
/// the final path segment as well as on the full URI.
#[tokio::test]
async fn a_relative_ref_resolves_by_its_final_segment() {
    const NAMED: &str = r#"{"type":"object","properties":{"n":{"$ref":"city.json"}}}"#;
    let encoder = JsonSchemaEncoder::builder()
        .registry(Arc::new(RefRegistry::chain()))
        .schema(NAMED)
        .dependencies([("city.json", CITY)])
        .build()
        .expect("a relative $ref resolves against the supplied dependency");

    let err = encoder
        .encode(
            &serde_json::json!({ "n": {} }),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .expect_err("the referenced constraint must be enforced");
    assert!(err.is_wire_format_error(), "{err}");
}

/// A `Bytes` round-trip: the wire prefix must survive reference resolution
/// untouched.
#[tokio::test]
async fn framing_is_unaffected_by_reference_resolution() {
    let encoder = JsonSchemaEncoder::builder()
        .registry(Arc::new(RefRegistry::chain()))
        .schema(ORDER)
        .dependencies([(ADDRESS_REF, ADDRESS), (CITY_REF, CITY)])
        .build()
        .expect("encoder builds");

    let framed: Bytes = encoder
        .encode(
            &serde_json::json!({ "id": 1, "shipTo": { "city": { "name": "Berlin" } } }),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .expect("encodes");

    assert_eq!(framed[0], 0x00);
    assert_eq!(&framed[1..5], &[0, 0, 0, 1]);
}
