//! Example: Confluent Schema Registry encode → decode round-trip.
//!
//! This example demonstrates:
//!
//! 1. Building a [`ConfluentSchemaRegistry`] client
//! 2. Wrapping it with a [`CachedSchemaRegistry`] for in-flight coalescing
//! 3. Building a [`ConfluentSchemaEncoder`] to frame producer payloads
//! 4. Building a [`WireFormatDecoder`] to strip the frame on the consumer side
//! 5. Performing a full encode → decode round-trip **without a live registry**
//!    using an in-memory stub registry
//!
//! # Running
//!
//! ```text
//! cargo run --example confluent_encode_decode --features confluent
//! ```
//!
//! To point at a real Confluent-compatible registry:
//!
//! ```text
//! SCHEMA_REGISTRY_URL=http://localhost:8081 \
//! cargo run --example confluent_encode_decode --features confluent
//! ```

#[cfg(not(feature = "confluent"))]
fn main() {
    eprintln!(
        "This example requires the `confluent` Cargo feature.\n\
         Run with:  cargo run --example confluent_encode_decode --features confluent"
    );
}

#[cfg(feature = "confluent")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use schemreg::{
        CachedSchemaRegistry, Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType,
        SubjectNameStrategy, confluent::ConfluentSchemaEncoder, decoder::WireFormatDecoder,
        error::SchemaRegError, traits::PayloadEncoder,
    };

    init_tracing();

    // ── 1. In-memory stub registry ────────────────────────────────────────
    //
    // Replace `StubRegistry` with:
    //   ConfluentSchemaRegistry::new(url)?
    //   ConfluentSchemaRegistry::builder().url(url).basic_auth(u, p).build()?

    #[derive(Default)]
    struct StubRegistry {
        next_id: std::sync::atomic::AtomicU32,
        by_id: parking_lot::RwLock<HashMap<SchemaId, Schema>>,
        by_subject: parking_lot::RwLock<HashMap<String, SchemaId>>,
    }

    impl SchemaRegistryClient for StubRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> schemreg::error::Result<Arc<Schema>> {
            self.by_id
                .read()
                .get(&id)
                .map(|s| Arc::new(s.clone()))
                .ok_or_else(|| SchemaRegError::invalid_state(format!("schema {id} not found")))
        }

        async fn get_latest_schema(&self, subject: &str) -> schemreg::error::Result<Arc<Schema>> {
            let id = self
                .by_subject
                .read()
                .get(subject)
                .copied()
                .ok_or_else(|| {
                    SchemaRegError::invalid_state(format!("subject {subject} not found"))
                })?;
            self.get_schema_by_id(id).await
        }

        async fn get_schema_by_version(
            &self,
            subject: &str,
            _version: schemreg::SchemaVersion,
        ) -> schemreg::error::Result<Arc<Schema>> {
            self.get_latest_schema(subject).await
        }

        async fn register_schema(
            &self,
            subject: &str,
            schema: &str,
            schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> schemreg::error::Result<SchemaId> {
            if let Some(&id) = self.by_subject.read().get(subject) {
                return Ok(id);
            }
            let raw_id = self
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let id = SchemaId::from(raw_id);
            let s = Schema::new(id, schema_type, schema).with_subject(subject, 1i32);
            self.by_id.write().insert(id, s);
            self.by_subject.write().insert(subject.to_string(), id);
            println!("  Registered schema -> id={id} subject={subject}");
            Ok(id)
        }
    }

    // ── 2. Wrap with cache ────────────────────────────────────────────────

    let cached = CachedSchemaRegistry::new(StubRegistry::default());

    // ── 3. Wrap cache in Arc, build encoder and decoder ───────────────────

    const AVRO_ORDER_SCHEMA: &str = r#"{
        "type": "record",
        "name": "Order",
        "namespace": "com.example",
        "fields": [
            {"name": "order_id",   "type": "string"},
            {"name": "amount_usd", "type": "double"},
            {"name": "currency",   "type": "string"}
        ]
    }"#;

    let cached = std::sync::Arc::new(cached);

    let encoder = ConfluentSchemaEncoder::builder()
        .registry(&*cached)
        .schema(AVRO_ORDER_SCHEMA, SchemaType::Avro)
        .strategy(SubjectNameStrategy::TopicName)
        .build()?;

    // WireFormatDecoder fetches schema metadata from the same cache so it can
    // correctly strip Protobuf message-index arrays and populate schema_metadata.
    let decoder = WireFormatDecoder::confluent(std::sync::Arc::clone(&cached));

    let topic = "orders";
    // Tiny synthetic Avro binary: UTF-8 string "order-1"
    let raw_payload = Bytes::from_static(b"\x0eorder-1");

    println!("\n=== Confluent encode -> decode round-trip ===\n");
    println!(
        "Raw payload:   {} bytes  {:02X?}",
        raw_payload.len(),
        &raw_payload[..]
    );

    let framed = encoder
        .encode(
            raw_payload.clone(),
            topic,
            None,
            schemreg::EncodeTarget::Value,
        )
        .await?;
    let schema_id = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
    println!(
        "Framed:        {} bytes  magic=0x{:02X} schema_id={}",
        framed.len(),
        framed[0],
        schema_id,
    );

    // ── 6. Decode via WireFormatDecoder ───────────────────────────────────

    let msg = decoder.decode(framed.clone()).await?;
    println!(
        "Decoded:       {} bytes  format={:?}",
        msg.payload.len(),
        msg.schema_format
    );
    assert_eq!(
        msg.payload, raw_payload,
        "decoded bytes must match original payload"
    );
    println!("Round-trip verified.");

    // ── 7. Second encode reuses cached schema ID ──────────────────────────

    let framed2 = encoder
        .encode(
            raw_payload.clone(),
            topic,
            None,
            schemreg::EncodeTarget::Value,
        )
        .await?;
    assert_eq!(framed, framed2, "same schema ID must be reused");
    println!("Schema ID cached - no extra registry call.");

    // ── 8. Non-Confluent payload passes through the decoder unchanged ─────

    let plain = Bytes::from_static(b"plain text, no framing");
    let pass = decoder.decode(plain.clone()).await?;
    assert_eq!(pass.payload, plain);
    println!("Non-Confluent payload passed through unchanged.");

    println!("\nCache stats: {} schemas cached.", cached.cache_len());
    println!("\n=== All assertions passed ===\n");
    Ok(())
}

#[cfg(feature = "confluent")]
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "schemreg=debug".parse().unwrap()),
        )
        .try_init();
}
