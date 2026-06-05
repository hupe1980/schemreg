//! Example: Custom registry backend + caching + WireFormatDecoder.
//!
//! Shows how to implement your own [`SchemaRegistryClient`] against any
//! backend (file-system, Redis, custom HTTP proxy, …) and compose it with:
//!
//! - [`CachedSchemaRegistry`] for transparent in-memory caching
//! - [`AnySchemaCache`] for generic cache-lifecycle management
//! - [`WireFormatDecoder`] for format-agnostic decoding
//! - [`SubjectNameStrategy`] for subject name derivation
//!
//! No network I/O or Docker required — the custom backend is backed by a
//! simple in-memory `HashMap`.
//!
//! # Running
//!
//! ```text
//! cargo run --example custom_backend
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use schemreg::{
    AnySchemaCache, CachedSchemaRegistry, Schema, SchemaId, SchemaReference, SchemaRegistryClient,
    SchemaType, SchemaVersion, SubjectNameStrategy,
    decoder::{SchemaFormat, SchemaMetadata, WireFormatDecoder},
    encode_wire_format,
    error::{Result, SchemaRegError},
};

// ── Custom backend ────────────────────────────────────────────────────────

/// A toy in-memory schema registry that serves a fixed catalogue.
///
/// In a real application you might replace this with:
/// - An HTTP client to Apicurio or Karapace
/// - A Redis/DynamoDB cache
/// - A sidecar proxy
/// - A test double with controlled failure injection
struct InMemorySchemaBackend {
    by_id: HashMap<SchemaId, Schema>,
}

impl InMemorySchemaBackend {
    fn new() -> Self {
        let mut by_id = HashMap::new();

        // Seed a few schemas.
        for (raw_id, schema_type, subject, definition) in [
            (
                1u32,
                SchemaType::Avro,
                "orders-value",
                r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"}]}"#,
            ),
            (
                2u32,
                SchemaType::Protobuf,
                "payments-value",
                "syntax = \"proto3\";\nmessage Payment { string id = 1; int64 amount_cents = 2; }",
            ),
            (
                3u32,
                SchemaType::Json,
                "users-value",
                r#"{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"id":{"type":"string"}}}"#,
            ),
        ] {
            let id = SchemaId::from(raw_id);
            by_id.insert(
                id,
                Schema::new(id, schema_type, definition).with_subject(subject, 1i32),
            );
        }

        Self { by_id }
    }
}

impl SchemaRegistryClient for InMemorySchemaBackend {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        tracing::debug!(schema_id = id.as_u32(), "InMemoryBackend: get_schema_by_id");
        self.by_id
            .get(&id)
            .map(|s| Arc::new(s.clone()))
            .ok_or_else(|| SchemaRegError::invalid_state(format!("schema {id} not found")))
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        self.by_id
            .values()
            .find(|s| s.subject.as_deref() == Some(subject))
            .cloned()
            .map(Arc::new)
            .ok_or_else(|| SchemaRegError::invalid_state(format!("subject {subject} not found")))
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        self.by_id
            .values()
            .find(|s| s.subject.as_deref() == Some(subject) && s.version == Some(version))
            .cloned()
            .map(Arc::new)
            .ok_or_else(|| {
                SchemaRegError::invalid_state(format!("subject {subject} v{version} not found"))
            })
    }

    async fn register_schema(
        &self,
        _subject: &str,
        _schema: &str,
        _schema_type: SchemaType,
        _references: &[SchemaReference],
    ) -> Result<SchemaId> {
        // Read-only backend for this example.
        Err(SchemaRegError::invalid_state(
            "InMemorySchemaBackend: register_schema not implemented",
        ))
    }

    async fn get_subjects(&self) -> Result<Vec<String>> {
        Ok(self
            .by_id
            .values()
            .filter_map(|s| s.subject.as_deref().map(str::to_owned))
            .collect())
    }

    async fn get_versions(&self, subject: &str) -> Result<Vec<SchemaVersion>> {
        Ok(self
            .by_id
            .values()
            .filter(|s| s.subject.as_deref() == Some(subject))
            .filter_map(|s| s.version)
            .collect())
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    println!("\n=== Custom backend + CachedSchemaRegistry ===\n");

    // ── 1. Wrap the custom backend with a bounded cache ───────────────────

    let backend = InMemorySchemaBackend::new();
    let cached = CachedSchemaRegistry::with_max_entries(backend, 100);

    // ── 2. Inspect subjects via the underlying backend ────────────────────

    let subjects = cached.get_subjects().await?;
    println!("Registered subjects:");
    for s in &subjects {
        println!("  • {s}");
    }

    // ── 3. Warm the cache for all known schema IDs ────────────────────────

    cached.warm_cache([1u32, 2, 3]).await?;
    println!("\nCache after warm: {} entries", cached.cache_len());

    // ── 4. Subject name strategies ────────────────────────────────────────

    println!("\n--- Subject name strategies ---");
    let topic = "orders";
    let record = "com.example.Order";

    for strategy in [
        SubjectNameStrategy::TopicName,
        SubjectNameStrategy::RecordName,
        SubjectNameStrategy::TopicRecordName,
    ] {
        if let Ok(subject) =
            strategy.subject_name(topic, Some(record), schemreg::EncodeTarget::Value)
        {
            println!("  {strategy:>25?} -> {subject}");
        }
    }

    // ── 5. AnySchemaCache trait object ────────────────────────────────────

    println!("\n--- Cache management via AnySchemaCache trait ---");
    {
        let cache_mgr: &dyn AnySchemaCache<Id = SchemaId> = &cached;
        println!("Cache len: {}", cache_mgr.cache_len());

        cache_mgr.invalidate(SchemaId::from(2u32));
        println!("After invalidate(2): {} entries", cache_mgr.cache_len());

        cache_mgr.clear_cache();
        println!("After clear_cache:   {} entries", cache_mgr.cache_len());
    }

    // ── 6. WireFormatDecoder dispatches correctly ─────────────────────────

    println!("\n--- WireFormatDecoder ---");
    let decoder = WireFormatDecoder::confluent(std::sync::Arc::new(cached));

    // Simulate a Confluent-framed Avro payload (schema ID 1).
    let raw_avro = Bytes::from_static(b"\x06abc"); // tiny Avro UTF-8 string
    let framed = encode_wire_format(1u32, &raw_avro);
    let decoded_msg = decoder.decode(framed).await?;

    println!("Schema format: {:?}", decoded_msg.schema_format);
    println!("Payload bytes: {:?}", &decoded_msg.payload[..]);

    assert_eq!(decoded_msg.schema_format, SchemaFormat::Avro);
    assert_eq!(&decoded_msg.payload[..], &raw_avro[..]);

    if let Some(SchemaMetadata::Confluent(schema)) = decoded_msg.schema_metadata {
        println!("Schema ID:     {}", schema.id);
        println!("Schema type:   {:?}", schema.schema_type);
        println!(
            "Subject:       {}",
            schema.subject.as_deref().unwrap_or("<none>")
        );
    }

    // ── 7. Unknown format passthrough ─────────────────────────────────────

    let plain = Bytes::from_static(b"plain text, no framing");
    let passthrough = decoder.decode(plain.clone()).await?;
    assert_eq!(passthrough.payload, plain);
    assert_eq!(passthrough.schema_format, SchemaFormat::Unknown);
    println!("\n✓ Unknown-framed payload passed through unchanged.");

    println!("\n=== All assertions passed ===\n");
    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "schemreg=debug".parse().unwrap()),
        )
        .try_init();
}
