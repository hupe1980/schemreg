//! End-to-end tests for the Protobuf codec.
//!
//! These use `prost_reflect::DynamicMessage` — which implements
//! `prost::Message` — so the whole encoder/decoder path is exercised against
//! real descriptors without a `prost-build` step in the test harness. A
//! generated `prost` struct takes exactly the same code path.
//!
//! The property under test throughout: **the message-index path is derived from
//! the descriptor, so it is right without the caller ever naming it**, and a
//! payload of the wrong message type is rejected rather than silently
//! mis-decoded.

#![cfg(feature = "protobuf")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto,
};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};
use schemreg::protobuf::{ProtobufSchemaDecoder, ProtobufSchemaEncoder, message_index_path};
use schemreg::{
    EncodeTarget, Result, Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType,
    SchemaVersion, SubjectNameStrategy,
};

// ── Test schema ───────────────────────────────────────────────────────────

/// The `.proto` source, registered verbatim — this is what a consumer in
/// another language resolves by schema ID.
const PROTO_SOURCE: &str = r#"
syntax = "proto3";
package shop;

message Order {
  string id = 1;
}

message Invoice {
  string id = 1;
  message Line {
    string sku = 1;
  }
}
"#;

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

/// A descriptor pool matching [`PROTO_SOURCE`].
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

fn descriptor(name: &str) -> MessageDescriptor {
    pool()
        .get_message_by_name(name)
        .unwrap_or_else(|| panic!("{name} must exist"))
}

fn message(name: &str, field: &str, value: &str) -> DynamicMessage {
    let mut m = DynamicMessage::new(descriptor(name));
    m.set_field_by_name(field, Value::String(value.to_string()));
    m
}

// ── Mock registry ─────────────────────────────────────────────────────────

struct MockRegistry {
    schemas: parking_lot::Mutex<HashMap<SchemaId, Schema>>,
    next_id: AtomicU32,
    register_calls: AtomicU32,
}

impl MockRegistry {
    fn new() -> Self {
        Self {
            schemas: parking_lot::Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
            register_calls: AtomicU32::new(0),
        }
    }
    fn register_calls(&self) -> u32 {
        self.register_calls.load(Ordering::SeqCst)
    }
}

impl SchemaRegistryClient for MockRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.schemas
            .lock()
            .get(&id)
            .map(|s| Arc::new(s.clone()))
            .ok_or_else(|| schemreg::SchemaRegError::api(40403, format!("schema {id} not found")))
    }
    async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
        Err(schemreg::SchemaRegError::not_supported("n/a"))
    }
    async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
        Err(schemreg::SchemaRegError::not_supported("n/a"))
    }
    async fn register_schema(
        &self,
        _subject: &str,
        schema: &str,
        schema_type: SchemaType,
        _refs: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.register_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            schema_type,
            SchemaType::Protobuf,
            "the encoder must register the schema as PROTOBUF"
        );
        let id = SchemaId::from(self.next_id.fetch_add(1, Ordering::SeqCst));
        self.schemas
            .lock()
            .insert(id, Schema::new(id, schema_type, schema));
        Ok(id)
    }
}

fn encoder_for(
    registry: Arc<MockRegistry>,
    message_type: &str,
) -> ProtobufSchemaEncoder<Arc<MockRegistry>> {
    ProtobufSchemaEncoder::builder()
        .registry(registry)
        .schema(PROTO_SOURCE)
        .descriptor(descriptor(message_type))
        .build()
        .expect("encoder builds")
}

// ── Message-index derivation ──────────────────────────────────────────────

/// The whole point of the module: the caller never writes an index array, and
/// the derived one is correct for every message in the file.
#[test]
fn indexes_are_derived_from_the_descriptor() {
    assert_eq!(message_index_path(&descriptor("shop.Order")).unwrap(), [0]);
    assert_eq!(
        message_index_path(&descriptor("shop.Invoice")).unwrap(),
        [1]
    );
    assert_eq!(
        message_index_path(&descriptor("shop.Invoice.Line")).unwrap(),
        [1, 0]
    );
}

/// The encoder must expose what it derived, so the value is auditable rather
/// than hidden.
#[test]
fn encoder_reports_its_derived_index() {
    let reg = Arc::new(MockRegistry::new());
    assert_eq!(
        encoder_for(Arc::clone(&reg), "shop.Order").message_indexes(),
        [0]
    );
    assert_eq!(
        encoder_for(Arc::clone(&reg), "shop.Invoice").message_indexes(),
        [1]
    );
    assert_eq!(
        encoder_for(reg, "shop.Invoice.Line").message_indexes(),
        [1, 0]
    );
}

// ── Framing ───────────────────────────────────────────────────────────────

/// The first top-level message must produce the single-`0x00` framing that the
/// Confluent serde emits — the byte-level interop property.
#[tokio::test]
async fn first_message_uses_the_optimised_single_byte_index() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Order");

    let framed = encoder
        .encode(
            &message("shop.Order", "id", "A-1"),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    assert_eq!(framed[0], 0x00, "magic byte");
    assert_eq!(&framed[1..5], &1u32.to_be_bytes(), "schema id");
    assert_eq!(framed[5], 0x00, "path [0] collapses to a single byte");
}

/// A nested message must produce the ZigZag-encoded multi-segment form.
#[tokio::test]
async fn nested_message_uses_the_zigzag_multi_segment_index() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Invoice.Line");

    let framed = encoder
        .encode(
            &message("shop.Invoice.Line", "sku", "SKU-9"),
            "invoices",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    // ZigZag(count=2)=4, ZigZag(1)=2, ZigZag(0)=0
    assert_eq!(&framed[5..8], &[0x04, 0x02, 0x00]);
}

// ── Round-trip ────────────────────────────────────────────────────────────

#[tokio::test]
async fn round_trip_preserves_the_message() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Order");
    let decoder = ProtobufSchemaDecoder::new(Arc::clone(&reg));

    let framed = encoder
        .encode(
            &message("shop.Order", "id", "order-42"),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    let unframed = decoder.unframe(&framed).unwrap();
    assert_eq!(unframed.message_indexes, vec![0]);

    let decoded = DynamicMessage::decode(descriptor("shop.Order"), unframed.payload).unwrap();
    assert_eq!(
        decoded.get_field_by_name("id").unwrap().as_str(),
        Some("order-42")
    );
}

#[tokio::test]
async fn round_trip_works_for_a_nested_message() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Invoice.Line");
    let decoder = ProtobufSchemaDecoder::new(Arc::clone(&reg));

    let framed = encoder
        .encode(
            &message("shop.Invoice.Line", "sku", "WIDGET"),
            "invoices",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    let unframed = decoder.unframe(&framed).unwrap();
    assert_eq!(unframed.message_indexes, vec![1, 0]);
    let decoded =
        DynamicMessage::decode(descriptor("shop.Invoice.Line"), unframed.payload).unwrap();
    assert_eq!(
        decoded.get_field_by_name("sku").unwrap().as_str(),
        Some("WIDGET")
    );
}

/// `decode::<M>()` must work for any `prost::Message` — here a `DynamicMessage`
/// is impossible (it needs a descriptor), so this covers the generic path with
/// `Vec<u8>`-shaped bytes via `unframe`, and asserts the schema is resolvable.
#[tokio::test]
async fn the_registered_schema_is_resolvable_from_a_framed_message() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Order");
    let decoder = ProtobufSchemaDecoder::new(Arc::clone(&reg));

    let framed = encoder
        .encode(
            &message("shop.Order", "id", "x"),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    let schema = decoder.schema_for(&framed).await.unwrap();
    assert_eq!(schema.schema_type, SchemaType::Protobuf);
    assert!(
        schema.schema.contains("message Order"),
        "the registered .proto source must round-trip"
    );
}

// ── Wrong-type protection ─────────────────────────────────────────────────

/// The defect this guards against: Protobuf payloads do not identify their own
/// type, so decoding an `Invoice` as an `Order` normally *succeeds* and returns
/// a struct full of defaults. The expected-descriptor check turns that silent
/// wrong answer into an error.
#[tokio::test]
async fn a_mismatched_message_type_is_rejected() {
    let reg = Arc::new(MockRegistry::new());
    let invoice_encoder = encoder_for(Arc::clone(&reg), "shop.Invoice");

    let framed = invoice_encoder
        .encode(
            &message("shop.Invoice", "id", "INV-1"),
            "invoices",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    let order_decoder = ProtobufSchemaDecoder::new(Arc::clone(&reg))
        .with_expected_descriptor(&descriptor("shop.Order"))
        .unwrap();

    let err = order_decoder
        .unframe(&framed)
        .expect_err("an Invoice must not decode as an Order");
    assert!(err.is_wire_format_error(), "{err}");
    assert!(err.to_string().contains("shop.Order"), "{err}");
}

/// The matching case must still pass — the guard must not be so strict that it
/// rejects correct traffic.
#[tokio::test]
async fn a_matching_message_type_is_accepted() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Order");

    let framed = encoder
        .encode(
            &message("shop.Order", "id", "ok"),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    ProtobufSchemaDecoder::new(Arc::clone(&reg))
        .with_expected_descriptor(&descriptor("shop.Order"))
        .unwrap()
        .unframe(&framed)
        .expect("a matching type must be accepted");
}

/// Without an expected descriptor the decoder stays permissive — routing on
/// `message_indexes` is a legitimate pattern for multi-type topics.
#[tokio::test]
async fn without_an_expected_descriptor_any_type_unframes() {
    let reg = Arc::new(MockRegistry::new());
    let framed = encoder_for(Arc::clone(&reg), "shop.Invoice")
        .encode(
            &message("shop.Invoice", "id", "INV"),
            "mixed",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    let unframed = ProtobufSchemaDecoder::new(reg).unframe(&framed).unwrap();
    assert_eq!(
        unframed.message_indexes,
        vec![1],
        "the index is reported so callers can route on it"
    );
}

// ── Subject resolution and caching ────────────────────────────────────────

/// The fully-qualified message name must feed the record-name strategies
/// automatically — the caller should not have to repeat it.
#[tokio::test]
async fn record_name_strategy_uses_the_descriptor_full_name() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = ProtobufSchemaEncoder::builder()
        .registry(Arc::clone(&reg))
        .schema(PROTO_SOURCE)
        .descriptor(descriptor("shop.Invoice.Line"))
        .strategy(SubjectNameStrategy::RecordName)
        .build()
        .unwrap();

    encoder
        .encode(
            &message("shop.Invoice.Line", "sku", "S"),
            "any-topic",
            EncodeTarget::Value,
        )
        .await
        .unwrap();

    assert!(
        encoder.cached_schema_id("shop.Invoice.Line").is_some(),
        "the subject must be the descriptor's full name"
    );
}

#[tokio::test]
async fn the_schema_is_registered_once_per_subject() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Order");

    for _ in 0..5 {
        encoder
            .encode(
                &message("shop.Order", "id", "x"),
                "orders",
                EncodeTarget::Value,
            )
            .await
            .unwrap();
    }
    assert_eq!(reg.register_calls(), 1, "the schema ID must be cached");
    assert_eq!(encoder.cached_subject_count(), 1);

    encoder.invalidate_subject("orders-value");
    encoder
        .encode(
            &message("shop.Order", "id", "x"),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .unwrap();
    assert_eq!(
        reg.register_calls(),
        2,
        "invalidation must force a re-register"
    );
}

/// 32 concurrent first-encodes for one subject must register exactly once.
#[tokio::test]
async fn concurrent_first_encodes_coalesce_into_one_registration() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = Arc::new(encoder_for(Arc::clone(&reg), "shop.Order"));

    let mut handles = Vec::new();
    for _ in 0..32 {
        let encoder = Arc::clone(&encoder);
        handles.push(tokio::spawn(async move {
            encoder
                .encode(
                    &message("shop.Order", "id", "x"),
                    "orders",
                    EncodeTarget::Value,
                )
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(
        reg.register_calls(),
        1,
        "32 concurrent encodes must coalesce into one registration"
    );
}

/// Key and value are distinct subjects and must resolve to distinct IDs.
#[tokio::test]
async fn key_and_value_resolve_to_distinct_subjects() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = encoder_for(Arc::clone(&reg), "shop.Order");

    let value = encoder
        .encode(
            &message("shop.Order", "id", "v"),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .unwrap();
    let key = encoder
        .encode(
            &message("shop.Order", "id", "k"),
            "orders",
            EncodeTarget::Key,
        )
        .await
        .unwrap();

    assert_ne!(&value[1..5], &key[1..5], "distinct subjects → distinct IDs");
    assert!(encoder.cached_schema_id("orders-value").is_some());
    assert!(encoder.cached_schema_id("orders-key").is_some());
}

// ── Builder validation ────────────────────────────────────────────────────

#[test]
fn the_builder_requires_registry_schema_and_descriptor() {
    let reg = Arc::new(MockRegistry::new());

    assert!(
        ProtobufSchemaEncoder::<Arc<MockRegistry>>::builder()
            .schema(PROTO_SOURCE)
            .descriptor(descriptor("shop.Order"))
            .build()
            .is_err(),
        "registry is required"
    );
    assert!(
        ProtobufSchemaEncoder::builder()
            .registry(Arc::clone(&reg))
            .descriptor(descriptor("shop.Order"))
            .build()
            .is_err(),
        "schema is required"
    );
    let err = ProtobufSchemaEncoder::builder()
        .registry(reg)
        .schema(PROTO_SOURCE)
        .build()
        .expect_err("descriptor is required");
    assert!(err.to_string().contains("descriptor"), "{err}");
}

/// An explicit override must win over the derived value — the escape hatch has
/// to actually work for the rare registry whose message ordering differs.
#[test]
fn an_explicit_index_overrides_the_derived_one() {
    let reg = Arc::new(MockRegistry::new());
    let encoder = ProtobufSchemaEncoder::builder()
        .registry(reg)
        .schema(PROTO_SOURCE)
        .descriptor(descriptor("shop.Order")) // would derive [0]
        .message_indexes(vec![3, 7])
        .build()
        .unwrap();
    assert_eq!(encoder.message_indexes(), [3, 7]);
}

// ── Interop with the raw wire helpers ─────────────────────────────────────

/// Bytes produced by the Confluent Java serializer for the default message type
/// must unframe correctly through the high-level decoder too, not just through
/// the low-level wire functions.
#[tokio::test]
async fn java_produced_default_index_bytes_unframe() {
    let reg = Arc::new(MockRegistry::new());
    let decoder = ProtobufSchemaDecoder::new(reg);

    let java_bytes = Bytes::from_static(&[
        0x00, 0x00, 0x00, 0x00, 0x2A, // magic + schema id 42
        0x00, // MessageIndexes.DEFAULT_INDEX
        0x0A, 0x03, b'a', b'b', b'c', // proto body
    ]);

    let unframed = decoder.unframe(&java_bytes).unwrap();
    assert_eq!(unframed.key, 42u32);
    assert_eq!(unframed.message_indexes, vec![0]);
    assert_eq!(&unframed.payload[..], b"\x0a\x03abc");
}
