//! Header-first decoding: every codec, the framing-only decoder and the
//! object-safe trait read a schema-id header before falling back to the
//! payload prefix — the order Confluent's own deserializers use.
#![cfg(all(feature = "avro", feature = "json", feature = "protobuf"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use bytes::Bytes;
use prost_reflect::DescriptorPool;
use prost_reflect::prost::Message;
use prost_reflect::prost_types::Timestamp;
use schemreg::{
    AvroSchemaDecoder, AvroSchemaEncoder, EncodeTarget, HeaderFramed, JsonSchemaDecoder,
    JsonSchemaEncoder, PayloadDecoder, ProtobufSchemaDecoder, ProtobufSchemaEncoder, Result,
    Schema, SchemaGuid, SchemaId, SchemaIdPlacement, SchemaReference, SchemaRegError,
    SchemaRegistryClient, SchemaType, SchemaVersion, WireFormatDecoder, WireRecord,
    encode_protobuf_wire_format, encode_wire_format,
};
use serde_json::json;

const ID: u32 = 7;
const GUID: &str = "8f14e45f-ceea-467a-9575-0b7d1c9b1d8f";

const AVRO_ORDER: &str = r#"{"type":"record","name":"Order","namespace":"shop",
    "fields":[{"name":"id","type":"string"},{"name":"quantity","type":"int"}]}"#;
const JSON_ORDER: &str = r#"{"type":"object","properties":{"id":{"type":"string"}},
    "required":["id"],"additionalProperties":false}"#;

fn guid() -> SchemaGuid {
    GUID.parse().unwrap()
}

/// One schema, reachable by ID 7 and by GUID; registration always yields 7.
struct Registry(Arc<Schema>);

impl Registry {
    fn new(schema_type: SchemaType, text: &str) -> Arc<Self> {
        let mut schema = Schema::new(SchemaId::new(ID), schema_type, text);
        schema.guid = Some(guid());
        Arc::new(Self(Arc::new(schema)))
    }
}

impl SchemaRegistryClient for Registry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        if id == SchemaId::new(ID) {
            Ok(Arc::clone(&self.0))
        } else {
            Err(SchemaRegError::not_supported(format!("no schema {id}")))
        }
    }
    async fn get_schema_by_guid(&self, guid: SchemaGuid) -> Result<Arc<Schema>> {
        if Some(guid) == self.0.guid {
            Ok(Arc::clone(&self.0))
        } else {
            Err(SchemaRegError::not_supported("unknown GUID"))
        }
    }
    async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
        Ok(Arc::clone(&self.0))
    }
    async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
        Ok(Arc::clone(&self.0))
    }
    async fn register_schema(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<SchemaId> {
        Ok(SchemaId::new(ID))
    }
}

fn order() -> AvroValue {
    AvroValue::Record(vec![
        ("id".into(), AvroValue::String("o-1".into())),
        ("quantity".into(), AvroValue::Int(3)),
    ])
}

/// The four Confluent placements of one bare payload.
fn placements(bare: &Bytes, indexes: Option<&[u32]>) -> Vec<(&'static str, WireRecord)> {
    let prefixed = |key: schemreg::SchemaKey| match indexes {
        Some(ix) => encode_protobuf_wire_format(key, ix, bare),
        None => encode_wire_format(key, bare),
    };
    let header = |key: schemreg::SchemaKey| {
        WireRecord::from(HeaderFramed::new(
            EncodeTarget::Value,
            key,
            indexes,
            bare.clone(),
        ))
    };
    vec![
        ("prefix v0", prefixed(SchemaId::new(ID).into()).into()),
        ("prefix v1", prefixed(guid().into()).into()),
        ("header v0", header(SchemaId::new(ID).into())),
        ("header v1", header(guid().into())),
    ]
}

// ── WireRecord ────────────────────────────────────────────────────────────

#[test]
fn from_headers_takes_the_last_header_for_the_requested_side() {
    let headers = [
        ("__value_schema_id", &b"first"[..]),
        ("__key_schema_id", &b"key"[..]),
        ("traceparent", &b"00-abc"[..]),
        ("__value_schema_id", &b"last"[..]),
    ];
    let value = WireRecord::from_headers(EncodeTarget::Value, Bytes::new(), headers);
    assert_eq!(
        value.schema_id_header().unwrap(),
        &Bytes::from_static(b"last")
    );

    let key = WireRecord::from_headers(EncodeTarget::Key, Bytes::new(), headers);
    assert_eq!(key.schema_id_header().unwrap(), &Bytes::from_static(b"key"));

    let none = WireRecord::from_headers(EncodeTarget::Value, Bytes::new(), [("other", b"x")]);
    assert_eq!(none.placement(), SchemaIdPlacement::Prefix);
}

// ── Codecs ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn avro_decodes_every_placement() {
    let registry = Registry::new(SchemaType::Avro, AVRO_ORDER);
    let encoder = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&registry))
        .schema(AVRO_ORDER)
        .build()
        .unwrap();
    let bare = encoder
        .encode_with_header(order(), "orders", EncodeTarget::Value)
        .await
        .unwrap()
        .payload;

    let decoder = AvroSchemaDecoder::new(registry);
    for (name, record) in placements(&bare, None) {
        assert_eq!(decoder.decode(record).await.unwrap(), order(), "{name}");
    }
}

#[tokio::test]
async fn json_decodes_every_placement_and_still_validates() {
    let registry = Registry::new(SchemaType::Json, JSON_ORDER);
    let bare = Bytes::from(serde_json::to_vec(&json!({"id": "o-1"})).unwrap());
    let decoder = JsonSchemaDecoder::with_validation(registry);
    for (name, record) in placements(&bare, None) {
        assert_eq!(
            decoder.decode(record).await.unwrap(),
            json!({"id": "o-1"}),
            "{name}"
        );
    }

    // Header mode is not a way around validation.
    let invalid = Bytes::from_static(br#"{"id": 1}"#);
    let record = WireRecord::new(invalid).with_schema_id_header(encode_wire_format(ID, b""));
    assert!(decoder.decode(record).await.is_err());
}

#[tokio::test]
async fn protobuf_reads_the_index_path_from_wherever_the_placement_keeps_it() {
    let registry = Registry::new(SchemaType::Protobuf, "syntax = \"proto3\";");
    let bare = Bytes::from(
        Timestamp {
            seconds: 5,
            nanos: 0,
        }
        .encode_to_vec(),
    );
    let decoder = ProtobufSchemaDecoder::new(registry);

    for (name, record) in placements(&bare, Some(&[1, 0])) {
        let unframed = decoder.unframe(record.clone()).unwrap();
        assert_eq!(unframed.message_indexes, vec![1, 0], "{name}");
        assert_eq!(unframed.payload, bare, "{name}");
        let ts: Timestamp = decoder.decode(record).await.unwrap();
        assert_eq!(ts.seconds, 5, "{name}");
    }
}

#[tokio::test]
async fn the_expected_type_check_applies_to_a_header_supplied_path() {
    let registry = Registry::new(SchemaType::Protobuf, "syntax = \"proto3\";");
    let timestamp = DescriptorPool::global()
        .get_message_by_name("google.protobuf.Timestamp")
        .unwrap();
    let decoder = ProtobufSchemaDecoder::new(registry)
        .with_expected_descriptor(&timestamp)
        .unwrap();

    let wrong_type = HeaderFramed::new(EncodeTarget::Value, ID, Some(&[1, 0]), Bytes::new());
    let err = decoder.unframe(wrong_type).unwrap_err();
    assert!(err.to_string().contains("different message type"), "{err}");
}

// ── Framing-only decode and the trait ─────────────────────────────────────

#[tokio::test]
async fn a_header_wins_over_whatever_the_payload_starts_with() {
    let registry = Registry::new(SchemaType::Avro, AVRO_ORDER);
    // The payload *looks* prefix-framed for an unknown ID; the header must win
    // and the payload must come back whole.
    let payload = encode_wire_format(999u32, b"body");
    let record =
        WireRecord::new(payload.clone()).with_schema_id_header(encode_wire_format(ID, b""));

    let decoded = WireFormatDecoder::confluent(registry)
        .decode(record)
        .await
        .unwrap();
    assert_eq!(decoded.placement, Some(SchemaIdPlacement::Header));
    assert_eq!(decoded.payload, payload);
}

#[tokio::test]
async fn dyn_payload_decoder_unframes_both_placements_to_the_same_bytes() {
    let registry = Registry::new(SchemaType::Protobuf, "syntax = \"proto3\";");
    let decoder: Arc<dyn PayloadDecoder> = Arc::new(WireFormatDecoder::confluent(registry));
    let bare = Bytes::from_static(b"\x08\x05");

    for (name, record) in placements(&bare, Some(&[0])) {
        let inner = decoder
            .decode(record, "orders", EncodeTarget::Value)
            .await
            .unwrap();
        assert_eq!(inner, bare, "{name}");
    }
}

// ── Round trip through the crate's own types ──────────────────────────────

#[tokio::test]
async fn encode_with_header_round_trips_through_every_decoder() {
    let avro = Registry::new(SchemaType::Avro, AVRO_ORDER);
    let framed = AvroSchemaEncoder::builder()
        .registry(Arc::clone(&avro))
        .schema(AVRO_ORDER)
        .build()
        .unwrap()
        .encode_with_header(order(), "orders", EncodeTarget::Value)
        .await
        .unwrap();
    assert_eq!(
        AvroSchemaDecoder::new(avro).decode(framed).await.unwrap(),
        order()
    );

    let jsonr = Registry::new(SchemaType::Json, JSON_ORDER);
    let framed = JsonSchemaEncoder::builder()
        .registry(Arc::clone(&jsonr))
        .schema(JSON_ORDER)
        .build()
        .unwrap()
        .encode_with_header(&json!({"id": "o-1"}), "orders", EncodeTarget::Value)
        .await
        .unwrap();
    assert_eq!(
        JsonSchemaDecoder::new(jsonr).decode(framed).await.unwrap(),
        json!({"id": "o-1"})
    );

    let proto = Registry::new(SchemaType::Protobuf, "syntax = \"proto3\";");
    let timestamp = DescriptorPool::global()
        .get_message_by_name("google.protobuf.Timestamp")
        .unwrap();
    let framed = ProtobufSchemaEncoder::builder()
        .registry(Arc::clone(&proto))
        .schema("syntax = \"proto3\";")
        .descriptor(timestamp)
        .build()
        .unwrap()
        .encode_with_header(
            &Timestamp {
                seconds: 9,
                nanos: 0,
            },
            "ts",
            EncodeTarget::Value,
        )
        .await
        .unwrap();
    let ts: Timestamp = ProtobufSchemaDecoder::new(proto)
        .decode(framed)
        .await
        .unwrap();
    assert_eq!(ts.seconds, 9);
}

// ── Malformed headers ─────────────────────────────────────────────────────

#[tokio::test]
async fn a_message_index_array_in_an_avro_header_is_refused() {
    let registry = Registry::new(SchemaType::Avro, AVRO_ORDER);
    let record = WireRecord::from(HeaderFramed::new(
        EncodeTarget::Value,
        ID,
        Some(&[0]),
        Bytes::new(),
    ));
    let err = AvroSchemaDecoder::new(registry)
        .decode(record)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("only a Protobuf record"), "{err}");
}

#[tokio::test]
async fn a_protobuf_header_without_an_index_array_is_refused() {
    let registry = Registry::new(SchemaType::Protobuf, "syntax = \"proto3\";");
    let record = WireRecord::new(Bytes::new()).with_schema_id_header(encode_wire_format(ID, b""));
    let err = ProtobufSchemaDecoder::new(registry)
        .unframe(record)
        .unwrap_err();
    assert!(err.to_string().contains("no message-index"), "{err}");
}

#[tokio::test]
async fn malformed_header_values_are_errors_never_panics() {
    let registry = Registry::new(SchemaType::Avro, AVRO_ORDER);
    let decoder = AvroSchemaDecoder::new(registry);
    for value in [
        &b""[..],
        &[0x00, 0, 0][..],
        &[0x07, 0, 0, 0, 1][..],
        &[0x01; 9][..],
    ] {
        let record = WireRecord::new(Bytes::new()).with_schema_id_header(value);
        assert!(decoder.decode(record).await.is_err(), "{value:?}");
    }
}
