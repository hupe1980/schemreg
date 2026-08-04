//! Protobuf encode → Confluent framing → decode, with the message-index path
//! derived automatically from the descriptor.
//!
//! ```bash
//! cargo run --example protobuf_roundtrip --features protobuf
//! ```
//!
//! Uses a stub in-memory registry and `prost_reflect::DynamicMessage`, so it
//! runs with no Kafka, no Schema Registry, and no `prost-build` step. A
//! generated `prost` struct takes exactly the same code path — swap
//! `DynamicMessage` for `MyMessage` and `descriptor(..)` for
//! `MyMessage::default().descriptor()`.
//!
//! # What this demonstrates
//!
//! The message-index path is the one thing Protobuf framing needs that Avro and
//! JSON do not, and the one thing that silently corrupts data when it is wrong.
//! Notice that **no index array appears anywhere in this file** — it is derived
//! from the descriptor, so reordering messages in the `.proto` cannot desync it
//! from the call site.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;
use prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto,
};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};
use schemreg::protobuf::{ProtobufSchemaDecoder, ProtobufSchemaEncoder, message_index_path};
use schemreg::{
    EncodeTarget, Result, Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType,
    SchemaVersion,
};

/// The `.proto` as the registry stores it. Message ordering is load-bearing:
/// it is what the message-index path indexes into.
const PROTO_SOURCE: &str = r#"
syntax = "proto3";
package shop;

message Order {                 // top-level 0    → index [0]
  string id = 1;
}

message Invoice {               // top-level 1    → index [1]
  string id = 1;
  message Line {                //   nested 0     → index [1, 0]
    string sku = 1;
  }
}
"#;

// ── Stub registry ─────────────────────────────────────────────────────────

#[derive(Default)]
struct StubRegistry {
    schemas: Mutex<HashMap<SchemaId, Schema>>,
    next_id: AtomicU32,
}

impl SchemaRegistryClient for StubRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.schemas
            .lock()
            .get(&id)
            .map(|s| Arc::new(s.clone()))
            .ok_or_else(|| schemreg::SchemaRegError::api(40403, format!("schema {id} not found")))
    }
    async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
        Err(schemreg::SchemaRegError::not_supported("stub"))
    }
    async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
        Err(schemreg::SchemaRegError::not_supported("stub"))
    }
    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        _refs: &[SchemaReference],
    ) -> Result<SchemaId> {
        let id = SchemaId::from(self.next_id.fetch_add(1, Ordering::SeqCst) + 1);
        self.schemas
            .lock()
            .insert(id, Schema::new(id, schema_type, schema));
        println!("  registry: registered {subject} as schema id {id}");
        Ok(id)
    }
}

// ── Descriptor plumbing (a prost-build step would replace all of this) ────

fn string_field(name: &str, number: i32) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        r#type: Some(field_descriptor_proto::Type::String as i32),
        label: Some(field_descriptor_proto::Label::Optional as i32),
        json_name: Some(name.to_string()),
        ..Default::default()
    }
}

fn pool() -> DescriptorPool {
    let file = FileDescriptorProto {
        name: Some("shop.proto".to_string()),
        package: Some("shop".to_string()),
        syntax: Some("proto3".to_string()),
        message_type: vec![
            DescriptorProto {
                name: Some("Order".to_string()),
                field: vec![string_field("id", 1)],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("Invoice".to_string()),
                field: vec![string_field("id", 1)],
                nested_type: vec![DescriptorProto {
                    name: Some("Line".to_string()),
                    field: vec![string_field("sku", 1)],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .expect("descriptor set is well-formed")
}

fn descriptor(pool: &DescriptorPool, name: &str) -> MessageDescriptor {
    pool.get_message_by_name(name)
        .unwrap_or_else(|| panic!("{name} must exist"))
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let pool = pool();
    let registry = Arc::new(StubRegistry::default());

    println!("=== Protobuf round-trip with derived message-index paths ===\n");

    // ── 1. Top-level message ──────────────────────────────────────────────
    println!("--- shop.Order (first top-level message) ---");
    let order_desc = descriptor(&pool, "shop.Order");
    println!(
        "  derived message-index: {:?}",
        message_index_path(&order_desc)?
    );

    let encoder = ProtobufSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(PROTO_SOURCE)
        .descriptor(order_desc.clone())
        .build()?;

    let mut order = DynamicMessage::new(order_desc.clone());
    order.set_field_by_name("id", Value::String("order-1".into()));

    let framed = encoder
        .encode(&order, "orders", EncodeTarget::Value)
        .await?;
    println!("  framed: {}", hex(&framed));
    println!("          ^^ magic  ^^^^^^^^^^^ id     ^^ index (single byte!)");

    let decoder = ProtobufSchemaDecoder::new(Arc::clone(&registry));
    let unframed = decoder.unframe(&framed)?;
    let decoded = DynamicMessage::decode(order_desc.clone(), unframed.payload.clone())?;
    println!(
        "  decoded: id={:?}, indexes={:?}\n",
        decoded.get_field_by_name("id").unwrap().as_str().unwrap(),
        unframed.message_indexes
    );

    // ── 2. Nested message ─────────────────────────────────────────────────
    println!("--- shop.Invoice.Line (nested one level) ---");
    let line_desc = descriptor(&pool, "shop.Invoice.Line");
    println!(
        "  derived message-index: {:?}",
        message_index_path(&line_desc)?
    );

    let line_encoder = ProtobufSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(PROTO_SOURCE)
        .descriptor(line_desc.clone())
        .build()?;

    let mut line = DynamicMessage::new(line_desc.clone());
    line.set_field_by_name("sku", Value::String("WIDGET-9".into()));

    let framed = line_encoder
        .encode(&line, "invoices", EncodeTarget::Value)
        .await?;
    println!("  framed: {}", hex(&framed));
    println!("                             ^^^^^^^^ ZigZag(count=2)=4, then [1, 0]");

    let unframed = decoder.unframe(&framed)?;
    let decoded = DynamicMessage::decode(line_desc.clone(), unframed.payload)?;
    println!(
        "  decoded: sku={:?}, indexes={:?}\n",
        decoded.get_field_by_name("sku").unwrap().as_str().unwrap(),
        unframed.message_indexes
    );

    // ── 3. Wrong-type protection ──────────────────────────────────────────
    //
    // Protobuf payloads do not identify their own type. Decoding an Invoice
    // as an Order normally *succeeds* and returns a struct full of defaults —
    // a silent wrong answer. An expected descriptor turns that into an error.
    println!("--- Wrong-type protection ---");
    let invoice_desc = descriptor(&pool, "shop.Invoice");
    let invoice_encoder = ProtobufSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(PROTO_SOURCE)
        .descriptor(invoice_desc.clone())
        .build()?;

    let mut invoice = DynamicMessage::new(invoice_desc);
    invoice.set_field_by_name("id", Value::String("INV-1".into()));
    let invoice_framed = invoice_encoder
        .encode(&invoice, "mixed", EncodeTarget::Value)
        .await?;

    let strict =
        ProtobufSchemaDecoder::new(Arc::clone(&registry)).with_expected_descriptor(&order_desc)?;
    match strict.unframe(&invoice_framed) {
        Ok(_) => panic!("an Invoice must not be accepted as an Order"),
        Err(e) => println!("  Invoice bytes rejected by an Order decoder: {e}\n"),
    }

    // ── 4. The registered schema is resolvable ────────────────────────────
    let schema = decoder.schema_for(&framed).await?;
    println!("--- Registry round-trip ---");
    println!("  schema type: {}", schema.schema_type);
    println!(
        "  .proto source recovered: {} bytes, contains 'message Invoice': {}",
        schema.schema.len(),
        schema.schema.contains("message Invoice")
    );

    println!("\nAll assertions passed.");
    Ok(())
}
