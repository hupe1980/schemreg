//! Avro schema references, on both sides of the wire.
//!
//! A schema that names a type defined in another subject —
//! `"type": "com.example.Address"` rather than the record spelled out inline —
//! is stored by the registry exactly as written, so it is not parseable on its
//! own. The definitions have to come from somewhere, and where depends on which
//! schema needs them:
//!
//! | Schema | Definitions come from |
//! |---|---|
//! | The encoder's schema | `dependencies`, supplied here |
//! | The writer schema a decoder fetched | the registry, walked automatically |
//! | The decoder's reader schema | `reader_dependencies`, supplied here |
//!
//! Run with:
//!
//! ```text
//! cargo run --example avro_references --features avro
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use apache_avro::types::Value;
use schemreg::avro::{AvroSchemaDecoder, AvroSchemaEncoder};
use schemreg::traits::SchemaRegistryClient;
use schemreg::types::{Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion};
use schemreg::{EncodeTarget, Result, SchemaRegError};

// ── The schemas ───────────────────────────────────────────────────────────

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

/// What the producer writes: `Customer`, which itself references `Address`.
const ORDER: &str = r#"{
    "type": "record", "name": "Order", "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "buyer", "type": "com.example.Customer"},
        {"name": "shipTo", "type": "com.example.Address"}
    ]
}"#;

/// What a later consumer expects: `shipTo` is gone and `note` is new with a
/// default. Still a referencing schema — a reader schema is allowed to look
/// exactly like the writer's.
const ORDER_READER: &str = r#"{
    "type": "record", "name": "Order", "namespace": "com.example",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "buyer", "type": "com.example.Customer"},
        {"name": "note", "type": "string", "default": "none"}
    ]
}"#;

// ── In-memory mock registry ───────────────────────────────────────────────

/// Stores what a real registry stores: the schema text as written, plus the
/// `references` list that says where the names in it are defined.
#[derive(Clone, Default)]
struct MockRegistry {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    next_id: u32,
    by_subject: HashMap<String, Entry>,
    by_id: HashMap<u32, Entry>,
}

#[derive(Clone)]
struct Entry {
    id: u32,
    schema: String,
    references: Vec<SchemaReference>,
}

impl MockRegistry {
    /// Pre-register a schema the way a migration or a CI step would.
    fn seed(&self, subject: &str, schema: &str, references: Vec<SchemaReference>) -> SchemaId {
        let mut inner = self.inner.lock().expect("not poisoned");
        inner.next_id += 1;
        let entry = Entry {
            id: inner.next_id,
            schema: schema.to_string(),
            references,
        };
        inner.by_subject.insert(subject.to_string(), entry.clone());
        inner.by_id.insert(entry.id, entry.clone());
        SchemaId::from(entry.id)
    }

    fn schema_of(entry: &Entry) -> Arc<Schema> {
        Arc::new(
            Schema::new(
                SchemaId::from(entry.id),
                SchemaType::Avro,
                entry.schema.as_str(),
            )
            .with_references(entry.references.clone()),
        )
    }
}

impl SchemaRegistryClient for MockRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        let inner = self.inner.lock().expect("not poisoned");
        inner
            .by_id
            .get(&id.as_u32())
            .map(Self::schema_of)
            .ok_or_else(|| SchemaRegError::invalid_state(format!("schema {id} not found")))
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        _version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        let inner = self.inner.lock().expect("not poisoned");
        inner
            .by_subject
            .get(subject)
            .map(Self::schema_of)
            .ok_or_else(|| SchemaRegError::api(40401, format!("subject {subject} not found")))
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        self.get_schema_by_version(subject, SchemaVersion::new(1))
            .await
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        _: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        if let Some(entry) = self
            .inner
            .lock()
            .expect("not poisoned")
            .by_subject
            .get(subject)
        {
            return Ok(SchemaId::from(entry.id));
        }
        Ok(self.seed(subject, schema, references.to_vec()))
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let registry = MockRegistry::default();

    // The shared types live under their own subjects, as a migration would
    // leave them.
    registry.seed("address-value", ADDRESS, Vec::new());
    registry.seed(
        "customer-value",
        CUSTOMER,
        vec![SchemaReference::new(
            "com.example.Address",
            "address-value",
            1i32,
        )],
    );

    // ── Producer ──────────────────────────────────────────────────────────
    //
    // `references` is what the registry stores so other consumers can resolve
    // the schema; `dependencies` is what the local Avro parser needs to
    // serialise a value. Order does not matter — [CUSTOMER, ADDRESS] would do
    // just as well.
    let encoder = AvroSchemaEncoder::builder()
        .registry(registry.clone())
        .schema(ORDER)
        .dependencies([ADDRESS, CUSTOMER])
        .references(vec![
            SchemaReference::new("com.example.Customer", "customer-value", 1i32),
            SchemaReference::new("com.example.Address", "address-value", 1i32),
        ])
        .build()?;

    let order = Value::Record(vec![
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
    ]);

    let framed = encoder.encode(order, "orders", EncodeTarget::Value).await?;
    println!("Encoded {} bytes with a referencing schema ✓", framed.len());

    // ── Consumer A: the writer schema ─────────────────────────────────────
    //
    // Nothing to configure. The wire header names the schema, the registry
    // reports its `references`, and the decoder walks the closure —
    // Order → Customer → Address, each subject fetched once.
    let plain = AvroSchemaDecoder::new(registry.clone());
    println!(
        "Writer-schema decode: {:?}",
        plain.decode(framed.clone()).await?
    );

    // ── Consumer B: a reader schema, which also has references ────────────
    //
    // The reader schema is local — the registry has never seen it and cannot
    // say where its references live, so its definitions are supplied here.
    // Order-free, and the set has to be complete: Customer needs Address.
    let resolving = AvroSchemaDecoder::builder()
        .registry(registry.clone())
        .reader_schema(ORDER_READER)
        .reader_dependencies([CUSTOMER, ADDRESS])
        .build()?;

    let decoded = resolving.decode(framed).await?;
    println!("Reader-schema decode: {decoded:?}");

    let Value::Record(fields) = &decoded else {
        anyhow::bail!("expected a record")
    };
    let names: Vec<&str> = fields.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, ["id", "buyer", "note"]);
    println!("  `shipTo` dropped, `note` defaulted, `buyer` still resolved ✓");

    // ── What a mistake looks like ─────────────────────────────────────────
    //
    // An incomplete set fails at build(), naming the type and the list that
    // should hold it — not on the first message.
    let err = AvroSchemaDecoder::builder()
        .registry(registry)
        .reader_schema(ORDER_READER)
        .reader_dependencies([CUSTOMER]) // Customer needs Address
        .build()
        .expect_err("an incomplete dependency set must not build");
    println!("\nIncomplete reader dependencies:\n  {err}");

    Ok(())
}
