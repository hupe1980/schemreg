//! End-to-end Avro encode → Confluent wire format → decode round-trip.
//!
//! This example uses an in-memory mock registry so no external Schema Registry
//! is needed. Swap `MockRegistry` for `ConfluentSchemaRegistry::builder()…`
//! to run against a real registry.
//!
//! Run with:
//!
//! ```text
//! cargo run --example avro_roundtrip --features avro
//! ```

use apache_avro::types::Value;
use bytes::Bytes;
use schemreg::avro::{AvroSchemaDecoder, AvroSchemaEncoder};
use schemreg::traits::SchemaRegistryClient;
use schemreg::types::{Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion};
use schemreg::{Result, SchemaRegError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ── In-memory mock registry ───────────────────────────────────────────────

#[derive(Clone, Default)]
struct MockRegistry {
    inner: Arc<Mutex<MockInner>>,
}

#[derive(Default)]
struct MockInner {
    next_id: u32,
    /// subject → schema_id
    subjects: HashMap<String, SchemaId>,
    /// schema_id → schema text
    schemas: HashMap<SchemaId, String>,
}

impl SchemaRegistryClient for MockRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        let inner = self.inner.lock().unwrap();
        inner
            .schemas
            .get(&id)
            .map(|def| Arc::new(Schema::new(id, SchemaType::Avro, def.clone())))
            .ok_or_else(|| SchemaRegError::registry(format!("schema {id} not found")))
    }

    async fn get_latest_schema(&self, _subject: &str) -> Result<Schema> {
        Err(SchemaRegError::not_supported("not implemented in mock"))
    }

    async fn get_schema_by_version(
        &self,
        _subject: &str,
        _version: SchemaVersion,
    ) -> Result<Schema> {
        Err(SchemaRegError::not_supported("not implemented in mock"))
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        _references: &[SchemaReference],
    ) -> Result<SchemaId> {
        let _ = schema_type;
        let mut inner = self.inner.lock().unwrap();
        if let Some(&id) = inner.subjects.get(subject) {
            return Ok(id);
        }
        inner.next_id += 1;
        let id = SchemaId::from(inner.next_id);
        inner.subjects.insert(subject.to_string(), id);
        inner.schemas.insert(id, schema.to_string());
        Ok(id)
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let schema_json = r#"{
        "type": "record",
        "name": "Order",
        "namespace": "com.example",
        "fields": [
            {"name": "id",     "type": "int"},
            {"name": "item",   "type": "string"},
            {"name": "amount", "type": "double"}
        ]
    }"#;

    let registry = MockRegistry::default();

    // ── Encoder ───────────────────────────────────────────────────────────
    let encoder = AvroSchemaEncoder::builder()
        .registry(registry.clone())
        .schema(schema_json)
        .build()?;

    let original = Value::Record(vec![
        ("id".into(), Value::Int(1001)),
        ("item".into(), Value::String("Widget".into())),
        ("amount".into(), Value::Double(19.99)),
    ]);

    println!("Encoding: {original:?}");
    let framed: Bytes = encoder.encode(original.clone(), "orders", false).await?;
    println!(
        "Framed bytes ({} total): magic={:#04x} schema_id={}",
        framed.len(),
        framed[0],
        u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]])
    );

    // ── Decoder ───────────────────────────────────────────────────────────
    let decoder = AvroSchemaDecoder::new(registry);
    let decoded: Value = decoder.decode(framed).await?;
    println!("Decoded:  {decoded:?}");

    assert_eq!(original, decoded, "round-trip mismatch");
    println!("\nRound-trip OK ✓");

    // ── Serde convenience ─────────────────────────────────────────────────
    // The encode_ser/decode_de methods accept any T: Serialize/Deserialize;
    // they convert via apache_avro's serde bridge rather than using a derive macro.
    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
    struct OrderSerde {
        id: i32,
        item: String,
        amount: f64,
    }

    let registry2 = MockRegistry::default();
    let encoder2 = AvroSchemaEncoder::builder()
        .registry(registry2.clone())
        .schema(schema_json)
        .build()?;

    let order = OrderSerde {
        id: 2002,
        item: "Gadget".into(),
        amount: 49.95,
    };
    let framed2 = encoder2.encode_ser(&order, "orders", false).await?;

    let decoder2 = AvroSchemaDecoder::new(registry2);
    let roundtripped: OrderSerde = decoder2.decode_de(framed2).await?;
    println!("Serde round-trip: {roundtripped:?}");
    assert_eq!(order, roundtripped);
    println!("Serde round-trip OK ✓");

    Ok(())
}
