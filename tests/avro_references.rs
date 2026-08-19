//! Avro schema references — the `references` mechanism, end to end.
//!
//! A schema that names a type defined in another subject is stored by the
//! registry exactly as written, so it is **not** parseable on its own: the
//! definition of `com.example.Address` lives elsewhere. A decoder that hands
//! the raw string to the Avro parser fails with a bare "unknown type", which is
//! how this crate behaved before the dependency closure was resolved.
//!
//! These tests cover the closure walk itself — transitive dependencies, diamond
//! shapes, and the cycle guard — since each is a way for the walk to hang, blow
//! the stack, or fetch the same subject repeatedly.

#![cfg(feature = "avro")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use apache_avro::types::Value;
use parking_lot::Mutex;
use schemreg::avro::{AvroSchemaDecoder, AvroSchemaEncoder};
use schemreg::{
    EncodeTarget, Result, Schema, SchemaGuid, SchemaId, SchemaReference, SchemaRegistryClient,
    SchemaType, SchemaVersion,
};

const ADDRESS: &str = r#"{
    "type": "record", "name": "Address", "namespace": "com.example",
    "fields": [{"name": "city", "type": "string"}]
}"#;

const CUSTOMER: &str = r#"{
    "type": "record", "name": "Customer", "namespace": "com.example",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "home", "type": "com.example.Address"}
    ]
}"#;

const ORDER: &str = r#"{
    "type": "record", "name": "Order", "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "buyer", "type": "com.example.Customer"},
        {"name": "shipTo", "type": "com.example.Address"}
    ]
}"#;

// ── A registry holding a small dependency graph ───────────────────────────

/// A registration as the test declares it: subject, id, schema, and
/// `(reference name, referenced subject)` pairs.
type Registration<'a> = (&'a str, u32, &'a str, &'a [(&'a str, &'a str)]);

/// One registered schema, as both indexes see it.
#[derive(Clone)]
struct Entry {
    id: u32,
    subject: String,
    schema: String,
    references: Vec<SchemaReference>,
}

#[derive(Default)]
struct RefRegistry {
    by_subject: Mutex<HashMap<String, Entry>>,
    by_id: Mutex<HashMap<u32, Entry>>,
    version_fetches: AtomicU32,
}

impl RefRegistry {
    fn with(entries: &[Registration<'_>]) -> Arc<Self> {
        let this = Arc::new(Self::default());
        for (subject, id, schema, refs) in entries {
            let entry = Entry {
                id: *id,
                subject: (*subject).to_string(),
                schema: (*schema).to_string(),
                references: refs
                    .iter()
                    .map(|(name, ref_subject)| SchemaReference::new(*name, *ref_subject, 1i32))
                    .collect(),
            };
            this.by_subject
                .lock()
                .insert((*subject).to_string(), entry.clone());
            this.by_id.lock().insert(*id, entry);
        }
        this
    }

    fn version_fetches(&self) -> u32 {
        self.version_fetches.load(Ordering::SeqCst)
    }
}

impl SchemaRegistryClient for RefRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        let guard = self.by_id.lock();
        let entry = guard
            .get(&id.as_u32())
            .ok_or_else(|| schemreg::SchemaRegError::api(40403, format!("no schema {id}")))?;
        Ok(Arc::new(
            Schema::new(id, SchemaType::Avro, entry.schema.as_str())
                .with_subject(entry.subject.as_str(), 1i32)
                .with_references(entry.references.clone()),
        ))
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        self.version_fetches.fetch_add(1, Ordering::SeqCst);
        let guard = self.by_subject.lock();
        let entry = guard
            .get(subject)
            .ok_or_else(|| schemreg::SchemaRegError::api(40401, format!("no subject {subject}")))?;
        Ok(Arc::new(
            Schema::new(
                SchemaId::from(entry.id),
                SchemaType::Avro,
                entry.schema.as_str(),
            )
            .with_subject(subject, version)
            .with_references(entry.references.clone()),
        ))
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        self.get_schema_by_version(subject, SchemaVersion::new(1))
            .await
    }

    async fn register_schema(
        &self,
        subject: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<SchemaId> {
        let guard = self.by_subject.lock();
        guard
            .get(subject)
            .map(|entry| SchemaId::from(entry.id))
            .ok_or_else(|| schemreg::SchemaRegError::api(40401, format!("no subject {subject}")))
    }
}

/// Address ← Customer ← Order, with Order also referencing Address directly
/// (a diamond).
fn diamond_registry() -> Arc<RefRegistry> {
    RefRegistry::with(&[
        ("address-value", 1, ADDRESS, &[]),
        (
            "customer-value",
            2,
            CUSTOMER,
            &[("com.example.Address", "address-value")],
        ),
        (
            "orders-value",
            3,
            ORDER,
            &[
                ("com.example.Customer", "customer-value"),
                ("com.example.Address", "address-value"),
            ],
        ),
    ])
}

fn order_value() -> Value {
    Value::Record(vec![
        ("id".into(), Value::Int(7)),
        (
            "buyer".into(),
            Value::Record(vec![
                ("name".into(), Value::String("Ada".into())),
                (
                    "home".into(),
                    Value::Record(vec![("city".into(), Value::String("London".into()))]),
                ),
            ]),
        ),
        (
            "shipTo".into(),
            Value::Record(vec![("city".into(), Value::String("Cambridge".into()))]),
        ),
    ])
}

// ── The gap this closes ───────────────────────────────────────────────────

/// Without dependency resolution the writer schema cannot even be parsed.
/// This is the baseline the decoder has to beat.
#[test]
fn a_referencing_schema_is_not_parseable_on_its_own() {
    assert!(
        apache_avro::Schema::parse_str(ORDER).is_err(),
        "ORDER names com.example.Customer, which is defined elsewhere"
    );
}

// ── Decoder ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn decoding_resolves_a_transitive_reference_closure() {
    let registry = diamond_registry();

    let encoder = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(ORDER)
        .dependencies([ADDRESS, CUSTOMER])
        .build()
        .expect("the encoder builds once dependencies are supplied");

    let framed = encoder
        .encode(order_value(), "orders", EncodeTarget::Value)
        .await
        .expect("encoding succeeds");

    let decoder = AvroSchemaDecoder::new(Arc::clone(&registry));
    let decoded = decoder.decode(framed).await.expect("decoding succeeds");

    let Value::Record(fields) = decoded else {
        unreachable!("the writer schema is a record")
    };
    assert_eq!(fields[0].0, "id");
    assert_eq!(fields[0].1, Value::Int(7));
}

/// A diamond must fetch each referenced subject once, not once per path to it.
/// Order → Customer → Address and Order → Address is 3 edges over 2 subjects.
#[tokio::test]
async fn a_diamond_dependency_is_fetched_once_per_subject() {
    let registry = diamond_registry();

    let encoder = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(ORDER)
        .dependencies([ADDRESS, CUSTOMER])
        .build()
        .expect("encoder builds");
    let framed = encoder
        .encode(order_value(), "orders", EncodeTarget::Value)
        .await
        .expect("encoding succeeds");

    AvroSchemaDecoder::new(Arc::clone(&registry))
        .decode(framed)
        .await
        .expect("decoding succeeds");

    assert_eq!(
        registry.version_fetches(),
        2,
        "customer-value and address-value, each exactly once"
    );
}

/// The parsed schema is cached per identifier, so a second message carrying the
/// same schema resolves nothing at all.
#[tokio::test]
async fn the_closure_is_walked_once_per_schema_id() {
    let registry = diamond_registry();

    let encoder = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(ORDER)
        .dependencies([ADDRESS, CUSTOMER])
        .build()
        .expect("encoder builds");
    let framed = encoder
        .encode(order_value(), "orders", EncodeTarget::Value)
        .await
        .expect("encoding succeeds");

    let decoder = AvroSchemaDecoder::new(Arc::clone(&registry));
    for _ in 0..5 {
        decoder
            .decode(framed.clone())
            .await
            .expect("decoding succeeds");
    }

    assert_eq!(
        registry.version_fetches(),
        2,
        "the closure must be resolved once and cached, not re-walked per message"
    );
}

/// A registry containing a reference cycle must produce an error, not recurse
/// until the stack runs out. Nothing in the Confluent API prevents `A → B → A`.
#[tokio::test]
async fn a_reference_cycle_terminates_with_an_error() {
    const A: &str = r#"{"type":"record","name":"A","namespace":"c",
        "fields":[{"name":"b","type":"c.B"}]}"#;
    const B: &str = r#"{"type":"record","name":"B","namespace":"c",
        "fields":[{"name":"a","type":"c.A"}]}"#;

    let registry = RefRegistry::with(&[
        ("a-value", 1, A, &[("c.B", "b-value")]),
        ("b-value", 2, B, &[("c.A", "a-value")]),
    ]);

    // Decode a frame naming schema 1 directly; the encoder cannot be built for
    // a cyclic schema, which is itself the point.
    let framed = schemreg::encode_wire_format(1u32, b"");
    let err = AvroSchemaDecoder::new(Arc::clone(&registry))
        .decode(framed)
        .await
        .expect_err("a cyclic reference graph must not hang or overflow");

    // The visited set stops the walk after one lap, so the failure is a parse
    // error rather than a stack overflow — either way it terminates.
    assert!(err.is_config_error() || err.is_wire_format_error(), "{err}");
}

// ── Encoder ───────────────────────────────────────────────────────────────

/// Building an encoder for a referencing schema without its dependencies is a
/// configuration error, surfaced at build time rather than at first encode.
#[tokio::test]
async fn building_without_dependencies_fails_fast() {
    let registry = diamond_registry();

    let err = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(ORDER)
        .build()
        .expect_err("ORDER is not parseable without its dependencies");

    assert!(err.is_config_error(), "{err}");
}

/// Avro resolves a named type only against definitions that came earlier, so
/// dependencies must be listed before their users. A wrong order builds
/// successfully and then fails at encode time, which is exactly the trap the
/// builder docs call out — pinned here so the guidance cannot drift from the
/// behaviour.
#[tokio::test]
async fn dependencies_must_be_listed_before_their_users() {
    let registry = diamond_registry();

    let wrong_order = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(ORDER)
        .dependencies([CUSTOMER, ADDRESS]) // Customer used before Address exists
        .build()
        .expect("a bad order still parses");

    let err = wrong_order
        .encode(order_value(), "orders", EncodeTarget::Value)
        .await
        .expect_err("the unresolved reference surfaces at encode time");
    assert!(err.to_string().contains("Unresolved"), "{err}");

    let right_order = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(ORDER)
        .dependencies([ADDRESS, CUSTOMER])
        .build()
        .expect("encoder builds");
    right_order
        .encode(order_value(), "orders", EncodeTarget::Value)
        .await
        .expect("the correct order encodes");
}

// ── GUID-framed decoding ──────────────────────────────────────────────────

/// The Avro decoder keys its parsed-schema cache by [`SchemaKey`], so a
/// v1-framed record resolves through `get_schema_by_guid` with no change at the
/// call site.
///
/// [`SchemaKey`]: schemreg::SchemaKey
#[tokio::test]
async fn the_decoder_handles_a_guid_framed_record() {
    struct GuidOnly;
    impl SchemaRegistryClient for GuidOnly {
        async fn get_schema_by_guid(&self, guid: SchemaGuid) -> Result<Arc<Schema>> {
            Ok(Arc::new(Schema::new(guid, SchemaType::Avro, ADDRESS)))
        }
        async fn get_schema_by_id(&self, _: SchemaId) -> Result<Arc<Schema>> {
            unreachable!("a v1 frame must not fall back to an ID lookup")
        }
        async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
            unreachable!()
        }
        async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
            unreachable!()
        }
        async fn register_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<SchemaId> {
            unreachable!()
        }
    }

    let guid: SchemaGuid = "8f14e45f-ceea-467a-9575-0b7d1c9b1d8f"
        .parse()
        .expect("valid GUID");
    let address = Value::Record(vec![("city".into(), Value::String("Oslo".into()))]);
    let body = apache_avro::to_avro_datum(
        &apache_avro::Schema::parse_str(ADDRESS).expect("ADDRESS is self-contained"),
        address,
    )
    .expect("serialisation succeeds");

    let decoded = AvroSchemaDecoder::new(GuidOnly)
        .decode(schemreg::encode_wire_format(guid, &body))
        .await
        .expect("a GUID-framed Avro record must decode");

    let Value::Record(fields) = decoded else {
        unreachable!("ADDRESS is a record")
    };
    assert_eq!(fields[0].1, Value::String("Oslo".into()));
}
