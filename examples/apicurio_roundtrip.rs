//! Example: Apicurio Registry native v3 roundtrip.
//!
//! This example demonstrates encoding and decoding a simple JSON object using
//! an in-memory mock registry that mimics the Apicurio v3 `SchemaRegistryClient`
//! interface.
//!
//! Run with:
//! ```shell
//! cargo run --example apicurio_roundtrip --features apicurio
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use schemreg::apicurio::ApicurioSchemaRegistry;
use schemreg::error::{Result, SchemaRegError};
use schemreg::wire::{decode_wire_format, encode_wire_format};
use schemreg::{
    ArtifactId, Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion,
};

// ── In-memory mock registry for the example ──────────────────────────────────

struct MockInner {
    next_id: u32,
    schemas: HashMap<SchemaId, Schema>,
    subjects: HashMap<String, SchemaId>,
}

#[derive(Clone)]
struct MockRegistry {
    inner: Arc<Mutex<MockInner>>,
}

impl MockRegistry {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockInner {
                next_id: 0,
                schemas: HashMap::new(),
                subjects: HashMap::new(),
            })),
        }
    }
}

impl SchemaRegistryClient for MockRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        let inner = self.inner.lock().unwrap();
        inner
            .schemas
            .get(&id)
            .cloned()
            .map(Arc::new)
            .ok_or_else(|| SchemaRegError::api(40403, format!("schema {id} not found")))
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        let inner = self.inner.lock().unwrap();
        inner
            .subjects
            .get(subject)
            .and_then(|id| inner.schemas.get(id))
            .cloned()
            .map(Arc::new)
            .ok_or_else(|| SchemaRegError::api(40401, format!("subject {subject} not found")))
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        _version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        // Simplified: just return latest
        self.get_latest_schema(subject).await
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        _references: &[SchemaReference],
    ) -> Result<SchemaId> {
        let mut inner = self.inner.lock().unwrap();

        // Idempotent: if subject already registered, return existing ID.
        if let Some(&existing_id) = inner.subjects.get(subject) {
            return Ok(existing_id);
        }

        inner.next_id += 1;
        let id = SchemaId::from(inner.next_id);
        let s = Schema::new(id, schema_type, schema).with_subject(subject, 1i32);
        inner.schemas.insert(id, s);
        inner.subjects.insert(subject.to_string(), id);
        Ok(id)
    }
}

// ── Domain types ──────────────────────────────────────────────────────────────

/// A simple order event.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Order {
    id: u64,
    product: String,
    quantity: u32,
    price_cents: u64,
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("schemreg=debug")
        .init();

    // ── Demonstrate ArtifactId subject encoding ───────────────────────────────

    println!("=== ArtifactId subject encoding ===");

    let id = ArtifactId::new("production", "orders-value");
    println!("ArtifactId:  {id}");
    println!("to_subject:  {}", id.to_subject());

    let parsed = ArtifactId::from_subject("production/orders-value");
    println!(
        "from_subject: group={} artifact={}",
        parsed.group, parsed.artifact
    );

    let bare = ArtifactId::from_subject("payments-key");
    println!(
        "bare subject: group={} artifact={}",
        bare.group, bare.artifact
    );

    // ── Demonstrate ApicurioSchemaRegistry builder (no live server needed) ───

    println!("\n=== ApicurioSchemaRegistry builder ===");
    let registry_result = ApicurioSchemaRegistry::builder()
        .url("http://localhost:8080")
        // .bearer_token("my-oidc-token")  // uncomment for authenticated calls
        .build();

    match registry_result {
        Ok(r) => println!("Client built: {r:?}"),
        Err(e) => println!("Build error (e.g. invalid URL): {e}"),
    }

    // ── Use mock registry for encode/decode roundtrip ─────────────────────────

    println!("\n=== Encode / decode roundtrip (mock registry) ===");

    let mock = MockRegistry::new();

    // Register the JSON Schema for the Order type.
    let json_schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "id":          { "type": "integer" },
            "product":     { "type": "string" },
            "quantity":    { "type": "integer", "minimum": 1 },
            "price_cents": { "type": "integer", "minimum": 0 }
        },
        "required": ["id", "product", "quantity", "price_cents"]
    }"#;

    // Apicurio subject: "{group}/{artifact}" or just "{artifact}" for default group.
    let subject = ArtifactId::new("production", "orders-value").to_subject();
    println!("Subject: {subject}");

    let schema_id = mock
        .register_schema(&subject, json_schema, SchemaType::Json, &[])
        .await?;
    println!("Registered → schema_id = {schema_id}");

    // Encode: serialize to JSON and add Confluent 5-byte wire header.
    let order = Order {
        id: 1001,
        product: "Gadget Pro".to_string(),
        quantity: 3,
        price_cents: 4999,
    };
    println!("Original:  {order:?}");

    let payload_bytes = serde_json::to_vec(&order)?;
    let framed = encode_wire_format(schema_id, &payload_bytes);
    println!(
        "Framed ({} bytes): magic={:#04x} schema_id={}",
        framed.len(),
        framed[0],
        u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]])
    );

    // Decode: strip wire header, look up schema, deserialize.
    let (wire_schema_id, payload) = decode_wire_format(&framed)?;
    println!(
        "Decoded wire frame: {} payload_len={}",
        wire_schema_id,
        payload.len()
    );

    let looked_up = mock.get_schema_by_key(wire_schema_id).await?;
    println!(
        "Schema from registry: type={} subject={:?}",
        looked_up.schema_type, looked_up.subject
    );

    let decoded_order: Order = serde_json::from_slice(payload)?;
    println!("Decoded:   {decoded_order:?}");
    assert_eq!(
        order, decoded_order,
        "encode/decode roundtrip must be lossless"
    );
    println!("✓  Roundtrip successful");

    // ── Multiple groups ───────────────────────────────────────────────────────

    println!("\n=== Multiple groups ===");
    let groups = ["default", "production", "staging"];
    for group in groups {
        let subj = ArtifactId::new(group, "events-value").to_subject();
        let id = mock
            .register_schema(&subj, json_schema, SchemaType::Json, &[])
            .await?;
        println!("  {subj} → schema_id={id}");
    }

    println!("\nAll checks passed.");
    Ok(())
}
