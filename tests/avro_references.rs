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
//!
//! The reader side is covered as a matrix rather than as scenarios: a writer
//! schema with references against a reader schema with references, and each of
//! them against a self-contained counterpart.

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

/// `ORDER` with every reference spelled out inline, so it parses on its own.
/// `shipTo` still names `com.example.Address` — that is a reference *within* one
/// schema, which Avro resolves without help.
const ORDER_INLINE: &str = r#"{
    "type": "record", "name": "Order", "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "buyer", "type": {
            "type": "record", "name": "Customer", "namespace": "com.example",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "home", "type": {
                    "type": "record", "name": "Address", "namespace": "com.example",
                    "fields": [{"name": "city", "type": "string"}]
                }}
            ]
        }},
        {"name": "shipTo", "type": "com.example.Address"}
    ]
}"#;

/// A consumer's view of `Order`: `shipTo` is gone and `note` is new with a
/// default. Still a referencing schema — the whole point is that a reader
/// schema is allowed to look like the writer's.
const ORDER_READER: &str = r#"{
    "type": "record", "name": "Order", "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "buyer", "type": "com.example.Customer"},
        {"name": "note", "type": "string", "default": "none"}
    ]
}"#;

/// The same reader schema with its references inlined.
const ORDER_READER_INLINE: &str = r#"{
    "type": "record", "name": "Order", "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "buyer", "type": {
            "type": "record", "name": "Customer", "namespace": "com.example",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "home", "type": {
                    "type": "record", "name": "Address", "namespace": "com.example",
                    "fields": [{"name": "city", "type": "string"}]
                }}
            ]
        }},
        {"name": "note", "type": "string", "default": "none"}
    ]
}"#;

/// A self-referential record: no dependencies, but full of `Schema::Ref`.
const NODE: &str = r#"{
    "type": "record", "name": "Node", "namespace": "com.example",
    "fields": [
        {"name": "label", "type": "string"},
        {"name": "next", "type": ["null", "com.example.Node"]}
    ]
}"#;

/// `NODE` as a consumer sees it later: `label` dropped, `depth` defaulted.
const NODE_READER: &str = r#"{
    "type": "record", "name": "Node", "namespace": "com.example",
    "fields": [
        {"name": "depth", "type": "int", "default": 0},
        {"name": "next", "type": ["null", "com.example.Node"]}
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

fn address_value(city: &str) -> Value {
    Value::Record(vec![("city".into(), Value::String(city.into()))])
}

/// A two-element linked list, so the recursive branch is actually taken.
fn node_value() -> Value {
    let leaf = Value::Record(vec![
        ("label".into(), Value::String("leaf".into())),
        ("next".into(), Value::Union(0, Box::new(Value::Null))),
    ]);
    Value::Record(vec![
        ("label".into(), Value::String("head".into())),
        ("next".into(), Value::Union(1, Box::new(leaf))),
    ])
}

/// The registry every reader-side test decodes against: `ORDER` written by a
/// producer that resolved its references, plus `ORDER_INLINE` under its own
/// subject for the self-contained-writer cells of the matrix.
async fn framed_order(registry: &Arc<RefRegistry>, schema: &str, deps: &[&str]) -> bytes::Bytes {
    AvroSchemaEncoder::builder()
        .registry(Arc::clone(registry))
        .schema(schema)
        .dependencies(deps.to_vec())
        .build()
        .expect("the encoder builds")
        .encode(order_value(), "orders", EncodeTarget::Value)
        .await
        .expect("encoding succeeds")
}

/// `Order` decoded through a reader schema: `shipTo` gone, `note` defaulted,
/// `buyer` still resolved through the reference.
fn assert_resolved_to_reader(decoded: &Value) {
    let Value::Record(fields) = decoded else {
        unreachable!("the reader schema is a record")
    };
    let by_name: HashMap<&str, &Value> = fields.iter().map(|(n, v)| (n.as_str(), v)).collect();

    assert_eq!(by_name.get("id"), Some(&&Value::Int(7)));
    assert_eq!(
        by_name.get("note"),
        Some(&&Value::String("none".into())),
        "the reader's default must be filled in"
    );
    assert!(
        !by_name.contains_key("shipTo"),
        "a field absent from the reader schema must be dropped"
    );
    let Some(Value::Record(buyer)) = by_name.get("buyer").copied() else {
        panic!("buyer must survive resolution as a record: {decoded:?}")
    };
    assert_eq!(buyer[0].1, Value::String("Ada".into()));
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

/// Dependencies may arrive in any order, and the order must not reach the wire.
///
/// `apache-avro` builds its name table in one pass and rejects a reference it
/// has not yet seen defined, so the set is sorted before the codec sees it.
#[tokio::test]
async fn dependencies_may_be_listed_in_any_order() {
    let registry = diamond_registry();

    let encode_with = async |deps: [&str; 2]| {
        AvroSchemaEncoder::builder()
            .registry(Arc::clone(&registry))
            .schema(ORDER)
            .dependencies(deps)
            .build()
            .expect("encoder builds in either order")
            .encode(order_value(), "orders", EncodeTarget::Value)
            .await
            .expect("and encodes in either order")
    };

    assert_eq!(
        encode_with([ADDRESS, CUSTOMER]).await,
        encode_with([CUSTOMER, ADDRESS]).await,
        "dependency order must not reach the wire"
    );
}

/// A dependency nobody defines is named in the error, along with the knob that
/// fixes it. The Avro parser calls this "Unknown primitive type", which sends
/// people hunting for a typo.
#[tokio::test]
async fn a_missing_dependency_names_itself_and_the_fix() {
    let registry = diamond_registry();

    let err = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(ORDER)
        .dependencies([CUSTOMER]) // Customer needs Address, which is absent
        .build()
        .expect_err("an incomplete dependency set must not build");

    assert!(err.is_config_error(), "{err}");
    let message = err.to_string();
    assert!(message.contains("com.example.Address"), "{message}");
    assert!(message.contains("dependencies"), "{message}");
}

/// Two schemas that reference each other have no valid order, and the codec
/// resolves names in a single pass, so this cannot be encoded at all. The error
/// belongs at build time, naming both.
#[tokio::test]
async fn a_cycle_between_two_schemas_is_reported_at_build_time() {
    const LEFT: &str = r#"{
        "type": "record", "name": "Left", "namespace": "com.example",
        "fields": [{"name": "right", "type": ["null", "com.example.Right"]}]
    }"#;
    const RIGHT: &str = r#"{
        "type": "record", "name": "Right", "namespace": "com.example",
        "fields": [{"name": "left", "type": ["null", "com.example.Left"]}]
    }"#;

    let err = AvroSchemaEncoder::builder()
        .registry(diamond_registry())
        .schema(LEFT)
        .dependencies([RIGHT])
        .build()
        .expect_err("a cross-schema cycle has no resolvable order");

    let message = err.to_string();
    assert!(message.contains("circular"), "{message}");
    assert!(message.contains("com.example.Left"), "{message}");
    assert!(message.contains("com.example.Right"), "{message}");
}

/// A schema referring to *itself* is ordinary Avro: an over-eager cycle check
/// would reject every linked list and tree.
#[tokio::test]
async fn a_self_referential_schema_is_not_a_cycle() {
    let registry = RefRegistry::with(&[("nodes-value", 9, NODE, &[])]);

    let encoder = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(NODE)
        .build()
        .expect("a self-reference needs no dependencies");

    let framed = encoder
        .encode(node_value(), "nodes", EncodeTarget::Value)
        .await
        .expect("encoding a recursive value succeeds");

    let decoded = AvroSchemaDecoder::new(Arc::clone(&registry))
        .decode(framed)
        .await
        .expect("decoding a recursive value succeeds");
    assert_eq!(decoded, node_value());
}

/// The same definition reached twice through a diamond is one definition, not a
/// name collision. The closure de-duplicates by subject, so this needs two
/// *subjects* holding the same schema.
#[tokio::test]
async fn one_type_registered_under_two_subjects_is_not_a_collision() {
    const PAIR: &str = r#"{
        "type": "record", "name": "Pair", "namespace": "com.example",
        "fields": [
            {"name": "here", "type": "com.example.Address"},
            {"name": "there", "type": "com.example.Address"}
        ]
    }"#;

    let registry = RefRegistry::with(&[
        ("address-value", 1, ADDRESS, &[]),
        ("shared.address-value", 2, ADDRESS, &[]),
        (
            "pairs-value",
            3,
            PAIR,
            &[
                ("com.example.Address", "address-value"),
                ("com.example.Address", "shared.address-value"),
            ],
        ),
    ]);

    let framed = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(PAIR)
        .dependencies([ADDRESS])
        .build()
        .expect("one copy of Address is enough locally")
        .encode(
            Value::Record(vec![
                ("here".into(), address_value("London")),
                ("there".into(), address_value("Oslo")),
            ]),
            "pairs",
            EncodeTarget::Value,
        )
        .await
        .expect("encoding succeeds");

    AvroSchemaDecoder::new(Arc::clone(&registry))
        .decode(framed)
        .await
        .expect("the closure carries Address twice and must collapse it");
}

/// Two *different* definitions of one name cannot both be right, and picking
/// either silently would decode somebody's data wrongly.
#[tokio::test]
async fn contradictory_definitions_of_one_type_are_rejected() {
    const ADDRESS_V2: &str = r#"{
        "type": "record", "name": "Address", "namespace": "com.example",
        "fields": [{"name": "city", "type": "string"}, {"name": "zip", "type": "string"}]
    }"#;

    let err = AvroSchemaEncoder::builder()
        .registry(diamond_registry())
        .schema(CUSTOMER)
        .dependencies([ADDRESS, ADDRESS_V2])
        .build()
        .expect_err("two definitions of com.example.Address must not be merged");

    let message = err.to_string();
    assert!(message.contains("com.example.Address"), "{message}");
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

// ── Reader schema × references ────────────────────────────────────────────
//
// Four cells, two axes: does the writer schema name external types, and does
// the reader schema.

/// The headline case: both sides name types defined in other subjects.
#[tokio::test]
async fn a_reader_schema_may_reference_other_schemas() {
    let registry = diamond_registry();
    let framed = framed_order(&registry, ORDER, &[ADDRESS, CUSTOMER]).await;

    let decoder = AvroSchemaDecoder::builder()
        .registry(Arc::clone(&registry))
        .reader_schema(ORDER_READER)
        .reader_dependencies([ADDRESS, CUSTOMER])
        .build()
        .expect("a reader schema may name types defined elsewhere");

    assert_resolved_to_reader(&decoder.decode(framed).await.expect("decoding succeeds"));
}

/// A referencing writer resolved against a self-contained reader. The writer's
/// name table must not be mistaken for the reader's.
#[tokio::test]
async fn a_referencing_writer_resolves_to_a_self_contained_reader() {
    let registry = diamond_registry();
    let framed = framed_order(&registry, ORDER, &[ADDRESS, CUSTOMER]).await;

    let decoder = AvroSchemaDecoder::builder()
        .registry(Arc::clone(&registry))
        .reader_schema(ORDER_READER_INLINE)
        .build()
        .expect("a self-contained reader needs no dependencies");

    assert_resolved_to_reader(&decoder.decode(framed).await.expect("decoding succeeds"));
}

/// The mirror image, where the writer contributes no name table at all.
#[tokio::test]
async fn a_self_contained_writer_resolves_to_a_referencing_reader() {
    let registry = RefRegistry::with(&[
        ("address-value", 1, ADDRESS, &[]),
        (
            "customer-value",
            2,
            CUSTOMER,
            &[("com.example.Address", "address-value")],
        ),
        ("orders-value", 3, ORDER_INLINE, &[]),
    ]);
    let framed = framed_order(&registry, ORDER_INLINE, &[]).await;

    let decoder = AvroSchemaDecoder::builder()
        .registry(Arc::clone(&registry))
        .reader_schema(ORDER_READER)
        .reader_dependencies([ADDRESS, CUSTOMER])
        .build()
        .expect("the reader's dependencies are its own");

    assert_resolved_to_reader(&decoder.decode(framed).await.expect("decoding succeeds"));
}

/// A schema with no dependencies but plenty of internal references. Resolving
/// it needs a name table built from the schema itself; handing the codec an
/// empty one strands every `Ref` in a recursive type.
#[tokio::test]
async fn a_recursive_schema_resolves_to_a_recursive_reader() {
    let registry = RefRegistry::with(&[("nodes-value", 9, NODE, &[])]);

    let framed = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(NODE)
        .build()
        .expect("encoder builds")
        .encode(node_value(), "nodes", EncodeTarget::Value)
        .await
        .expect("encoding succeeds");

    let decoded = AvroSchemaDecoder::builder()
        .registry(Arc::clone(&registry))
        .reader_schema(NODE_READER)
        .build()
        .expect("decoder builds")
        .decode(framed)
        .await
        .expect("a recursive payload resolves to a recursive reader schema");

    let Value::Record(fields) = &decoded else {
        unreachable!("Node is a record")
    };
    assert_eq!(fields[0].0, "depth");
    assert_eq!(fields[0].1, Value::Int(0), "the default is filled in");
    let Value::Union(1, next) = &fields[1].1 else {
        panic!("the tail must survive: {decoded:?}")
    };
    let Value::Record(tail) = next.as_ref() else {
        panic!("the tail is a record: {decoded:?}")
    };
    assert_eq!(tail[0].1, Value::Int(0), "and again one level down");
}

/// Reader dependencies are order-free, exactly like the encoder's.
#[tokio::test]
async fn reader_dependencies_may_be_listed_in_any_order() {
    let registry = diamond_registry();
    let framed = framed_order(&registry, ORDER, &[ADDRESS, CUSTOMER]).await;

    let decoder = AvroSchemaDecoder::builder()
        .registry(Arc::clone(&registry))
        .reader_schema(ORDER_READER)
        .reader_dependencies([CUSTOMER, ADDRESS]) // user before definition
        .build()
        .expect("order must not matter");

    assert_resolved_to_reader(&decoder.decode(framed).await.expect("decoding succeeds"));
}

/// A reader schema whose references were never supplied: the message has to
/// point at the fix.
#[tokio::test]
async fn a_reader_schema_missing_its_dependencies_says_so() {
    let err = AvroSchemaDecoder::builder()
        .registry(diamond_registry())
        .reader_schema(ORDER_READER)
        .build()
        .expect_err("ORDER_READER names com.example.Customer");

    assert!(err.is_config_error(), "{err}");
    let message = err.to_string();
    assert!(message.contains("com.example.Customer"), "{message}");
    assert!(
        message.contains("reader_dependencies"),
        "the message must name the knob that fixes it: {message}"
    );
    assert!(
        !message.contains("Unknown primitive type"),
        "the Avro parser's wording is what sent people looking for a typo: {message}"
    );
}

/// An incomplete set is caught for the same reason, one level deeper: Customer
/// is supplied, but Customer needs Address.
#[tokio::test]
async fn reader_dependencies_must_be_complete() {
    let err = AvroSchemaDecoder::builder()
        .registry(diamond_registry())
        .reader_schema(ORDER_READER)
        .reader_dependencies([CUSTOMER])
        .build()
        .expect_err("Customer's own reference is unresolved");

    assert!(err.to_string().contains("com.example.Address"), "{err}");
}

/// Dependencies with no reader schema are a configuration mistake, not a
/// silently ignored setting.
#[tokio::test]
async fn reader_dependencies_without_a_reader_schema_are_rejected() {
    let err = AvroSchemaDecoder::builder()
        .registry(diamond_registry())
        .reader_dependencies([ADDRESS])
        .build()
        .expect_err("dependencies resolve a reader schema; there is none");

    assert!(err.is_config_error(), "{err}");
}

// ── Already-parsed reader schemas ─────────────────────────────────────────

/// The `#[derive(AvroSchema)]` shape: a consumer already holding
/// `apache_avro::Schema` values need not serialise them back to JSON.
#[tokio::test]
async fn a_parsed_reader_schema_carries_its_own_dependencies() {
    let registry = diamond_registry();
    let framed = framed_order(&registry, ORDER, &[ADDRESS, CUSTOMER]).await;

    // Parsed as a set, the way `parse_list` hands them back — in the awkward
    // order, to pin that the parsed path sorts too.
    let parsed = apache_avro::Schema::parse_list([ORDER_READER, CUSTOMER, ADDRESS])
        .expect("the set parses together");
    let (reader, deps) = parsed.split_first().expect("three schemas");

    let decoder = AvroSchemaDecoder::builder()
        .registry(Arc::clone(&registry))
        .reader_schema_parsed(reader.clone())
        .reader_dependencies_parsed(deps.to_vec())
        .build()
        .expect("parsed schemas need no round-trip through JSON");

    assert_resolved_to_reader(&decoder.decode(framed).await.expect("decoding succeeds"));
}

/// A parsed reader schema with a dangling reference cannot fail at parse time —
/// it is already parsed — so the check has to happen at build.
#[tokio::test]
async fn a_parsed_reader_schema_with_a_dangling_reference_is_rejected() {
    let parsed = apache_avro::Schema::parse_list([ORDER_READER, CUSTOMER, ADDRESS])
        .expect("the set parses together");

    let err = AvroSchemaDecoder::builder()
        .registry(diamond_registry())
        .reader_schema_parsed(parsed[0].clone())
        .build()
        .expect_err("the references are still unresolved");

    assert!(err.to_string().contains("com.example.Customer"), "{err}");
}

/// Mixing the two forms would drop the dependencies on the floor and decode
/// wrongly, so it is refused.
#[tokio::test]
async fn mixing_parsed_and_json_reader_forms_is_rejected() {
    let address = apache_avro::Schema::parse_str(ADDRESS).expect("ADDRESS is self-contained");

    let err = AvroSchemaDecoder::builder()
        .registry(diamond_registry())
        .reader_schema(ORDER_READER)
        .reader_dependencies_parsed([address])
        .build()
        .expect_err("JSON schema with parsed dependencies");

    assert!(err.is_config_error(), "{err}");
    assert!(err.to_string().contains("different forms"), "{err}");
}
