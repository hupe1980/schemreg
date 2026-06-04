//! End-to-end JSON Schema encode/decode example using an in-memory mock registry.
//!
//! Run with:
//! ```bash
//! cargo run --example json_roundtrip --features json
//! ```

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use schemreg::json::{JsonSchemaDecoder, JsonSchemaEncoder};
use schemreg::traits::SchemaRegistryClient;
use schemreg::types::{Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion};
use schemreg::{EncodeTarget, Result, SchemaRegError};

// ── In-memory mock registry ───────────────────────────────────────────────

#[derive(Clone)]
struct MockRegistry {
    inner: Arc<MockInner>,
}

struct MockInner {
    schemas: Mutex<HashMap<SchemaId, Schema>>,
    next_id: Mutex<u32>,
}

impl MockRegistry {
    fn new() -> Self {
        Self {
            inner: Arc::new(MockInner {
                schemas: Mutex::new(HashMap::new()),
                next_id: Mutex::new(1),
            }),
        }
    }
}

impl SchemaRegistryClient for MockRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.inner
            .schemas
            .lock()
            .unwrap()
            .get(&id)
            .map(|s| Arc::new(s.clone()))
            .ok_or_else(|| SchemaRegError::api(40403, format!("schema {id} not found")))
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        Err(SchemaRegError::not_supported(format!(
            "get_latest_schema not implemented in mock (subject: {subject})"
        )))
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        Err(SchemaRegError::not_supported(format!(
            "get_schema_by_version not implemented (subject: {subject}, version: {version})"
        )))
    }

    async fn register_schema(
        &self,
        _subject: &str,
        schema: &str,
        schema_type: SchemaType,
        _references: &[SchemaReference],
    ) -> Result<SchemaId> {
        let mut next_id = self.inner.next_id.lock().unwrap();
        let id = SchemaId::from(*next_id);
        *next_id += 1;
        self.inner
            .schemas
            .lock()
            .unwrap()
            .insert(id, Schema::new(id, schema_type, schema));
        Ok(id)
    }
}

// ── Domain types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: i64,
    item: String,
    price: f64,
}

// ── JSON Schema ───────────────────────────────────────────────────────────

const ORDER_SCHEMA: &str = r#"{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Order",
    "type": "object",
    "properties": {
        "id":    { "type": "integer", "minimum": 1 },
        "item":  { "type": "string",  "minLength": 1 },
        "price": { "type": "number",  "minimum": 0 }
    },
    "required": ["id", "item", "price"],
    "additionalProperties": false
}"#;

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== schemreg JSON Schema round-trip example ===\n");

    let registry = MockRegistry::new();

    // ── 1. Build encoder ─────────────────────────────────────────────────

    let encoder = JsonSchemaEncoder::builder()
        .registry(registry.clone())
        .schema(ORDER_SCHEMA)
        .build()?;

    println!("Encoder built (schema compiled + ready for registration).\n");

    // ── 2. Encode a valid order ───────────────────────────────────────────

    let order = Order {
        id: 1001,
        item: "Sprocket".to_string(),
        price: 4.99,
    };

    println!("Encoding: {order:?}");
    let framed: Bytes = encoder
        .encode_ser(&order, "orders", EncodeTarget::Value)
        .await?;

    let schema_id = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
    println!(
        "Framed:   {} bytes  magic=0x{:02X}  schema_id={schema_id}",
        framed.len(),
        framed[0]
    );

    // ── 3. Attempt to encode an invalid order (validation error) ──────────

    println!("\nAttempting to encode an invalid value (price is negative)…");
    let bad_value = json!({"id": 1, "item": "Bad", "price": -1.0});
    let result = encoder
        .encode(&bad_value, "orders", EncodeTarget::Value)
        .await;
    match result {
        Err(e) => println!("  ✓ Correctly rejected: {e}"),
        Ok(_) => panic!("should have been rejected"),
    }

    // ── 4. Decode ─────────────────────────────────────────────────────────

    let decoder = JsonSchemaDecoder::new(registry.clone());
    let decoded: Order = decoder.decode_de(framed.clone()).await?;
    println!("\nDecoded:  {decoded:?}");
    assert_eq!(order, decoded, "round-trip must be lossless");
    println!("✓ Round-trip verified.");

    // ── 5. Decode to raw serde_json::Value ────────────────────────────────

    let raw_value = decoder.decode(framed.clone()).await?;
    println!("\nDecoded as Value: {raw_value}");

    // ── 6. Decode with validation enabled ────────────────────────────────

    let strict_decoder = JsonSchemaDecoder::with_validation(registry.clone());
    let validated: Order = strict_decoder.decode_de(framed.clone()).await?;
    assert_eq!(order, validated);
    println!("✓ Strict (validate-on-decode) round-trip verified.");

    // ── 7. Key subject ────────────────────────────────────────────────────

    let key_value = json!({"id": 1001, "item": "Sprocket", "price": 4.99});
    let key_framed = encoder
        .encode(&key_value, "orders", EncodeTarget::Key)
        .await?;
    let key_schema_id =
        u32::from_be_bytes([key_framed[1], key_framed[2], key_framed[3], key_framed[4]]);
    println!("\nKey encode:  schema_id={key_schema_id} (different subject from value)");
    assert_ne!(
        schema_id, key_schema_id,
        "key and value must have distinct schema IDs"
    );

    println!("\nAll assertions passed. Done.");
    Ok(())
}
