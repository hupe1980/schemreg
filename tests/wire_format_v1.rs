//! Confluent wire format v1 (schema GUID) and schema-ID-in-header placement.
//!
//! Confluent Platform 8 added a second way to name the schema a record was
//! written with: a 16-byte GUID behind magic byte `0x01`, which may sit in the
//! payload prefix *or* in a `__key_schema_id` / `__value_schema_id` Kafka
//! header. Both are on the wire in production today, so a consumer that only
//! understands the legacy 5-byte prefix silently fails on half a cluster.
//!
//! These tests pin the byte layout against the layout Confluent's own
//! `SchemaId.guidToBytes` / `SchemaId.fromBytes` produce.

use std::sync::Arc;

use bytes::Bytes;
use schemreg::{
    DetectedWireFormat, EncodeTarget, PayloadDecoder, Result, Schema, SchemaGuid, SchemaId,
    SchemaKey, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion, WireFormatDecoder,
    decode_protobuf_message_indexes, decode_schema_id_header, decode_wire_format,
    detect_wire_format, encode_protobuf_wire_format, encode_schema_id_header, encode_wire_format,
    schema_id_header_name,
};

fn guid() -> SchemaGuid {
    "8f14e45f-ceea-467a-9575-0b7d1c9b1d8f"
        .parse()
        .expect("a well-formed GUID")
}

// ── Byte layout ───────────────────────────────────────────────────────────

/// The v1 prefix is `0x01` followed by the GUID's 16 bytes in the order the
/// canonical text form reads — which is what Java's
/// `putLong(msb); putLong(lsb)` writes.
#[test]
fn v1_prefix_layout_matches_confluent() {
    let framed = encode_wire_format(guid(), b"payload");

    assert_eq!(framed[0], 0x01, "v1 magic byte");
    assert_eq!(
        &framed[1..17],
        &[
            0x8f, 0x14, 0xe4, 0x5f, 0xce, 0xea, 0x46, 0x7a, 0x95, 0x75, 0x0b, 0x7d, 0x1c, 0x9b,
            0x1d, 0x8f
        ],
        "GUID bytes go on the wire big-endian, matching the text form"
    );
    assert_eq!(&framed[17..], b"payload");
}

/// v0 and v1 differ only in the prefix; the payload is byte-identical.
#[test]
fn v0_and_v1_carry_the_same_payload() {
    let payload = b"serialised-avro";
    let v0 = encode_wire_format(7u32, payload);
    let v1 = encode_wire_format(guid(), payload);

    assert_eq!(&v0[5..], payload);
    assert_eq!(&v1[17..], payload);
    assert_eq!(v1.len() - v0.len(), 12, "v1's prefix is 12 bytes longer");
}

// ── Round-trips ───────────────────────────────────────────────────────────

#[test]
fn v1_round_trips_through_decode() {
    let framed = encode_wire_format(guid(), b"body");
    let (key, payload) = decode_wire_format(&framed).expect("v1 must decode");

    assert_eq!(key, SchemaKey::Guid(guid()));
    assert_eq!(key.as_guid(), Some(guid()));
    assert_eq!(key.as_id(), None, "a GUID frame names no numeric ID");
    assert_eq!(payload, b"body");
}

#[test]
fn v1_protobuf_round_trips_with_its_message_index() {
    let framed = encode_protobuf_wire_format(guid(), &[1, 0], b"\x0a\x03foo");
    let (key, after_prefix) = decode_wire_format(&framed).expect("v1 protobuf must decode");
    let (indexes, offset) =
        decode_protobuf_message_indexes(after_prefix).expect("index array must parse");

    assert_eq!(key.as_guid(), Some(guid()));
    assert_eq!(indexes, vec![1, 0]);
    assert_eq!(&after_prefix[offset..], b"\x0a\x03foo");
}

#[test]
fn detection_reports_v1_and_its_payload_offset() {
    assert_eq!(
        detect_wire_format(&encode_wire_format(guid(), b"x")),
        DetectedWireFormat::Confluent {
            key: SchemaKey::Guid(guid()),
            payload_offset: 17,
        }
    );
}

// ── Kafka header placement ────────────────────────────────────────────────

#[test]
fn header_names_are_the_ones_confluent_writes() {
    assert_eq!(schema_id_header_name(EncodeTarget::Key), "__key_schema_id");
    assert_eq!(
        schema_id_header_name(EncodeTarget::Value),
        "__value_schema_id"
    );
}

/// The defining property of header placement: the header value is *exactly*
/// the prefix the payload would otherwise have carried, and the payload is then
/// written raw. A caller splitting a record into (header, value) depends on it.
#[test]
fn a_header_value_is_the_prefix_the_payload_no_longer_carries() {
    let payload = b"\x0a\x05hello";

    let prefixed = encode_protobuf_wire_format(guid(), &[0], payload);
    let header = encode_schema_id_header(guid(), Some(&[0]));

    assert_eq!(&header[..], &prefixed[..header.len()]);
    assert_eq!(&prefixed[header.len()..], payload);
}

#[test]
fn header_round_trips_avro_and_protobuf_shapes() {
    // Avro / JSON Schema: identifier only, no message-index array.
    let value = encode_schema_id_header(guid(), None);
    assert_eq!(value.len(), 17);
    let (key, indexes) = decode_schema_id_header(&value).expect("header must parse");
    assert_eq!(key.as_guid(), Some(guid()));
    assert_eq!(indexes, None);

    // Protobuf: identifier plus the message-index array.
    let value = encode_schema_id_header(guid(), Some(&[1, 1, 0]));
    let (key, indexes) = decode_schema_id_header(&value).expect("header must parse");
    assert_eq!(key.as_guid(), Some(guid()));
    assert_eq!(indexes, Some(vec![1, 1, 0]));
}

/// A header may also carry a legacy numeric ID (magic `0x00`), which is what a
/// registry without GUID support would put there.
#[test]
fn header_accepts_a_v0_identifier_too() {
    let value = encode_schema_id_header(99u32, None);
    assert_eq!(value.as_ref(), &[0x00, 0x00, 0x00, 0x00, 0x63]);

    let (key, indexes) = decode_schema_id_header(&value).expect("header must parse");
    assert_eq!(key, SchemaId::new(99));
    assert_eq!(indexes, None);
}

#[test]
fn a_malformed_header_is_rejected_rather_than_guessed() {
    // Unknown magic byte.
    assert!(decode_schema_id_header(&[0x7F, 0, 0, 0, 1]).is_err());
    // Truncated GUID.
    assert!(decode_schema_id_header(&[0x01, 0xAA, 0xBB]).is_err());
    // Empty.
    assert!(decode_schema_id_header(&[]).is_err());

    // Trailing bytes after a complete index array: the header must contain the
    // identifier and nothing else, so this is a framing bug, not padding.
    let mut trailing = encode_schema_id_header(1u32, Some(&[0])).to_vec();
    trailing.push(0x00);
    let err = decode_schema_id_header(&trailing).expect_err("trailing bytes must be rejected");
    assert!(err.to_string().contains("trailing"), "{err}");
}

// ── End-to-end through WireFormatDecoder ──────────────────────────────────

/// A registry that answers by GUID and by ID, recording which was used.
struct GuidRegistry {
    schema_type: SchemaType,
}

impl SchemaRegistryClient for GuidRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        Ok(Arc::new(Schema::new(id, self.schema_type, r#""string""#)))
    }
    async fn get_schema_by_guid(&self, g: SchemaGuid) -> Result<Arc<Schema>> {
        Ok(Arc::new(Schema::new(g, self.schema_type, r#""string""#)))
    }
    async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
        unreachable!("not used by these tests")
    }
    async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
        unreachable!("not used by these tests")
    }
    async fn register_schema(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<SchemaId> {
        unreachable!("not used by these tests")
    }
}

/// `WireFormatDecoder` must resolve a v1 frame through `get_schema_by_guid`
/// without the caller doing anything differently — the producer chooses the
/// wire format version, not the consumer.
#[tokio::test]
async fn wire_format_decoder_resolves_a_v1_frame_by_guid() {
    let decoder = WireFormatDecoder::confluent(GuidRegistry {
        schema_type: SchemaType::Avro,
    });

    let msg = decoder
        .decode(encode_wire_format(guid(), b"avro-bytes"))
        .await
        .expect("v1 frame must decode");

    assert_eq!(msg.payload, &b"avro-bytes"[..]);
    match msg.schema_metadata {
        Some(schemreg::SchemaMetadata::Confluent(schema)) => {
            assert_eq!(schema.guid, Some(guid()));
            assert_eq!(schema.id, None, "a GUID lookup establishes no numeric ID");
        }
        other => unreachable!("expected Confluent metadata, got {other:?}"),
    }
}

/// The Protobuf message-index array must still be stripped when the frame is
/// v1 — the index sits after the prefix regardless of its version.
#[tokio::test]
async fn wire_format_decoder_strips_the_message_index_from_a_v1_frame() {
    let decoder = WireFormatDecoder::confluent(GuidRegistry {
        schema_type: SchemaType::Protobuf,
    });

    let proto = b"\x0a\x05hello";
    let msg = decoder
        .decode(encode_protobuf_wire_format(guid(), &[2], proto))
        .await
        .expect("v1 protobuf frame must decode");

    assert_eq!(msg.payload, &proto[..]);
    assert_eq!(msg.protobuf_message_indexes, Some(vec![2]));
}

/// The object-safe `PayloadDecoder` view must unframe v1 as well as v0.
#[tokio::test]
async fn payload_decoder_trait_handles_both_versions() {
    let decoder: Arc<dyn PayloadDecoder> = Arc::new(WireFormatDecoder::confluent(GuidRegistry {
        schema_type: SchemaType::Avro,
    }));

    for framed in [
        encode_wire_format(1u32, b"body"),
        encode_wire_format(guid(), b"body"),
    ] {
        let payload = decoder
            .decode(framed, "orders", EncodeTarget::Value)
            .await
            .expect("both wire format versions must unframe");
        assert_eq!(payload, Bytes::from_static(b"body"));
    }
}
