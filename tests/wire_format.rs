//! Integration tests for the Confluent and Glue wire format encode/decode.
//!
//! These tests cover:
//! - Round-trip encode → decode for all variants
//! - Error paths (truncated, bad magic byte, bad compression byte)
//! - Zero-copy `Bytes`-based variants
//! - `detect_wire_format` happy paths, edge cases, and both invalid variants
//! - Schema ID boundary values (0, 1, u32::MAX)
//! - Large payloads
//! - Glue wire format with ZLIB compression (requires `glue` feature)

use bytes::Bytes;
use schemreg::{
    DetectedWireFormat, GlueCompression, GlueDataFormat, GlueSchema, GlueSchemaVersionId, SchemaId,
    decode_wire_format, decode_wire_format_bytes, detect_wire_format, encode_wire_format,
};

// ── helpers ──────────────────────────────────────────────────────────────

fn glue_id(s: &str) -> GlueSchemaVersionId {
    s.parse().expect("valid UUID")
}

// ── Confluent round-trips ─────────────────────────────────────────────────

#[test]
fn confluent_roundtrip_typical() {
    let payload = b"serialized avro data";
    let framed = encode_wire_format(42, payload);
    let (id, decoded) = decode_wire_format(&framed).unwrap();
    assert_eq!(id, 42u32);
    assert_eq!(decoded, payload);
}

#[test]
fn confluent_roundtrip_empty_payload() {
    let framed = encode_wire_format(1, b"");
    assert_eq!(framed.len(), 5, "header-only, 5 bytes");
    let (id, decoded) = decode_wire_format(&framed).unwrap();
    assert_eq!(id, 1u32);
    assert!(decoded.is_empty());
}

#[test]
fn confluent_roundtrip_schema_id_zero() {
    let framed = encode_wire_format(0, b"x");
    let (id, _) = decode_wire_format(&framed).unwrap();
    assert_eq!(id, 0u32);
}

#[test]
fn confluent_roundtrip_schema_id_max() {
    let framed = encode_wire_format(u32::MAX, b"y");
    let (id, _) = decode_wire_format(&framed).unwrap();
    assert_eq!(id, u32::MAX);
}

#[test]
fn confluent_header_bytes_big_endian() {
    // Schema ID 256 = 0x0000_0100
    let framed = encode_wire_format(256, b"z");
    assert_eq!(&framed[..5], &[0x00, 0x00, 0x00, 0x01, 0x00]);
    assert_eq!(&framed[5..], b"z");
}

#[test]
fn confluent_large_payload() {
    let payload = vec![0xAB_u8; 1_000_000];
    let framed = encode_wire_format(999, &payload);
    let (id, decoded) = decode_wire_format(&framed).unwrap();
    assert_eq!(id, 999u32);
    assert_eq!(decoded, payload.as_slice());
}

// ── Confluent zero-copy Bytes variant ────────────────────────────────────

#[test]
fn confluent_bytes_roundtrip() {
    let framed = encode_wire_format(7, b"data");
    let (id, payload) = decode_wire_format_bytes(&framed).unwrap();
    assert_eq!(id, 7u32);
    assert_eq!(&payload[..], b"data");
}

#[test]
fn confluent_bytes_zero_copy() {
    let framed = encode_wire_format(1, b"hello world");
    let ptr = framed.as_ptr() as usize;
    let (_, payload) = decode_wire_format_bytes(&framed).unwrap();
    // The sub-slice should reference the same backing allocation.
    assert_eq!(payload.as_ptr() as usize, ptr + 5);
}

#[test]
fn confluent_bytes_empty_payload() {
    let framed = encode_wire_format(0, b"");
    let (id, payload) = decode_wire_format_bytes(&framed).unwrap();
    assert_eq!(id, 0u32);
    assert!(payload.is_empty());
}

// ── Confluent error paths ─────────────────────────────────────────────────

#[test]
fn confluent_decode_empty() {
    let err = decode_wire_format(&[]).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("too short") || msg.contains("short"), "{msg}");
}

#[test]
fn confluent_decode_truncated_header() {
    // Only 3 bytes — need 5.
    let err = decode_wire_format(&[0x00, 0x00, 0x00]).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("too short") || msg.contains("short"), "{msg}");
}

#[test]
fn confluent_decode_wrong_magic_byte() {
    // Magic byte should be 0x00.
    let err = decode_wire_format(&[0x01, 0x00, 0x00, 0x00, 0x01, 0x42]).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("magic byte") || msg.contains("0x01"), "{msg}");
}

#[test]
fn confluent_decode_bytes_wrong_magic() {
    let data = Bytes::from_static(&[0x02, 0x00, 0x00, 0x00, 0x01]);
    let err = decode_wire_format_bytes(&data).unwrap_err();
    assert!(err.to_string().contains("magic byte") || err.to_string().contains("0x02"));
}

// ── detect_wire_format happy paths ────────────────────────────────────────

#[test]
fn detect_confluent_happy() {
    let framed = encode_wire_format(42, b"data");
    assert_eq!(
        detect_wire_format(&framed),
        DetectedWireFormat::Confluent {
            schema_id: SchemaId::from(42u32),
            payload_offset: 5
        }
    );
}

#[test]
fn detect_confluent_id_zero() {
    let framed = encode_wire_format(0, b"x");
    assert_eq!(
        detect_wire_format(&framed),
        DetectedWireFormat::Confluent {
            schema_id: SchemaId::from(0u32),
            payload_offset: 5
        }
    );
}

#[test]
fn detect_confluent_id_max() {
    let framed = encode_wire_format(u32::MAX, b"");
    assert_eq!(
        detect_wire_format(&framed),
        DetectedWireFormat::Confluent {
            schema_id: SchemaId::from(u32::MAX),
            payload_offset: 5
        }
    );
}

#[test]
fn detect_empty_is_unknown() {
    assert_eq!(detect_wire_format(&[]), DetectedWireFormat::Unknown);
}

#[test]
fn detect_unrecognised_first_byte_is_unknown() {
    assert_eq!(
        detect_wire_format(&[0x99, 0x00, 0x00]),
        DetectedWireFormat::Unknown
    );
    assert_eq!(
        detect_wire_format(&[0xFF, 0x01, 0x02, 0x03]),
        DetectedWireFormat::Unknown
    );
}

#[test]
fn detect_confluent_truncated_is_invalid() {
    assert_eq!(
        detect_wire_format(&[0x00, 0x01, 0x02]),
        DetectedWireFormat::InvalidConfluent
    );
}

// ── Glue round-trips ──────────────────────────────────────────────────────

#[test]
fn glue_roundtrip_no_compression() {
    let id = glue_id("550e8400-e29b-41d4-a716-446655440000");
    let payload = b"avro payload";
    let framed = schemreg::encode_glue_wire_format(id, payload, GlueCompression::None).unwrap();
    let (decoded_id, decoded_payload) = schemreg::decode_glue_wire_format(&framed).unwrap();
    assert_eq!(decoded_id, id);
    assert_eq!(decoded_payload, payload);
}

#[test]
fn glue_roundtrip_bytes_variant() {
    let id = glue_id("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    let framed =
        schemreg::encode_glue_wire_format(id, b"hello glue", GlueCompression::None).unwrap();
    let framed_bytes = Bytes::from(framed.to_vec());
    let (decoded_id, payload) = schemreg::decode_glue_wire_format_bytes(&framed_bytes).unwrap();
    assert_eq!(decoded_id, id);
    assert_eq!(&payload[..], b"hello glue");
}

#[test]
fn glue_roundtrip_nil_uuid() {
    let nil = glue_id("00000000-0000-0000-0000-000000000000");
    let framed = schemreg::encode_glue_wire_format(nil, b"nil", GlueCompression::None).unwrap();
    let detected = detect_wire_format(&framed);
    assert_eq!(
        detected,
        DetectedWireFormat::Glue {
            version_id: nil,
            compression: GlueCompression::None,
            payload_offset: 18
        }
    );
}

#[test]
fn glue_roundtrip_empty_payload() {
    let id = glue_id("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    let framed = schemreg::encode_glue_wire_format(id, b"", GlueCompression::None).unwrap();
    let (decoded_id, decoded_payload) = schemreg::decode_glue_wire_format(&framed).unwrap();
    assert_eq!(decoded_id, id);
    assert!(decoded_payload.is_empty());
}

#[cfg(feature = "glue")]
#[test]
fn glue_roundtrip_zlib_compression() {
    let id = glue_id("550e8400-e29b-41d4-a716-446655440000");
    // Highly compressible payload.
    let payload = vec![0xAA_u8; 4096];
    let framed = schemreg::encode_glue_wire_format(id, &payload, GlueCompression::Zlib).unwrap();
    assert!(framed.len() < payload.len() + 18, "ZLIB should shrink it");
    let (decoded_id, decoded_payload) = schemreg::decode_glue_wire_format(&framed).unwrap();
    assert_eq!(decoded_id, id);
    assert_eq!(decoded_payload, payload);
}

// ── Glue detect_wire_format ───────────────────────────────────────────────

#[test]
fn detect_glue_happy() {
    let id = glue_id("550e8400-e29b-41d4-a716-446655440000");
    let framed = schemreg::encode_glue_wire_format(id, b"data", GlueCompression::None).unwrap();
    assert_eq!(
        detect_wire_format(&framed),
        DetectedWireFormat::Glue {
            version_id: id,
            compression: GlueCompression::None,
            payload_offset: 18
        }
    );
}

#[test]
fn detect_glue_truncated_is_invalid() {
    // Version byte 0x03 + compression, but no UUID.
    assert_eq!(
        detect_wire_format(&[0x03, 0x00]),
        DetectedWireFormat::InvalidGlue
    );
}

#[test]
fn detect_glue_bad_compression_byte_is_invalid() {
    // 18 bytes with valid length but unknown compression indicator 0x07.
    let mut data = [0u8; 18];
    data[0] = 0x03;
    data[1] = 0x07; // unknown
    assert_eq!(detect_wire_format(&data), DetectedWireFormat::InvalidGlue);
}

// ── Glue error paths ──────────────────────────────────────────────────────

#[test]
fn glue_decode_empty() {
    let err = schemreg::decode_glue_wire_format(&[]).unwrap_err();
    assert!(err.to_string().contains("too short") || err.to_string().contains("short"));
}

#[test]
fn glue_decode_wrong_version_byte() {
    let mut data = vec![0x00_u8; 20]; // Confluent magic, not Glue
    data[1] = 0x00; // compression none
    let err = schemreg::decode_glue_wire_format(&data).unwrap_err();
    assert!(err.to_string().contains("0x03") || err.to_string().contains("version"));
}

#[test]
fn glue_decode_bad_compression() {
    let mut data = vec![0x00_u8; 20];
    data[0] = 0x03;
    data[1] = 0x99; // unknown compression byte
    let err = schemreg::decode_glue_wire_format(&data).unwrap_err();
    assert!(err.to_string().contains("0x99") || err.to_string().contains("compression"));
}

// ── GlueSchemaVersionId ───────────────────────────────────────────────────

#[test]
fn glue_version_id_parse_display_roundtrip() {
    let s = "550e8400-e29b-41d4-a716-446655440000";
    let id: GlueSchemaVersionId = s.parse().unwrap();
    assert_eq!(id.to_string(), s);
}

#[test]
fn glue_version_id_case_insensitive() {
    let lower = "550e8400-e29b-41d4-a716-446655440000";
    let upper = "550E8400-E29B-41D4-A716-446655440000";
    let id_lower: GlueSchemaVersionId = lower.parse().unwrap();
    let id_upper: GlueSchemaVersionId = upper.parse().unwrap();
    assert_eq!(id_lower, id_upper);
}

#[test]
fn glue_version_id_wrong_length() {
    let err = "not-a-uuid".parse::<GlueSchemaVersionId>().unwrap_err();
    assert!(err.to_string().contains("36") || err.to_string().contains("UUID"));
}

#[test]
fn glue_version_id_bad_dash_positions() {
    // All hex, length 36, but dashes in wrong spots.
    let err = "550e8400e29b41d4a716446655440000---"
        .parse::<GlueSchemaVersionId>()
        .unwrap_err();
    assert!(err.to_string().contains("UUID") || err.to_string().contains("dash"));
}

#[test]
fn glue_version_id_non_hex() {
    let err = "zzzz8400-e29b-41d4-a716-446655440000"
        .parse::<GlueSchemaVersionId>()
        .unwrap_err();
    assert!(err.to_string().contains("hex") || err.to_string().contains("UUID"));
}

#[test]
fn glue_version_id_from_bytes_roundtrip() {
    let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let id = GlueSchemaVersionId::from_bytes(bytes);
    assert_eq!(id.as_bytes(), &bytes);
    let id2: GlueSchemaVersionId = bytes.into();
    assert_eq!(id, id2);
    let bytes2: [u8; 16] = id.into();
    assert_eq!(bytes2, bytes);
}

// ── GlueSchema helpers ────────────────────────────────────────────────────

#[test]
fn glue_schema_new_and_with_metadata() {
    let id = glue_id("550e8400-e29b-41d4-a716-446655440000");
    let schema = GlueSchema::new(id, GlueDataFormat::Avro, r#"{"type":"string"}"#);
    assert_eq!(schema.schema_version_id, id);
    assert_eq!(schema.data_format, GlueDataFormat::Avro);
    assert!(schema.schema_arn.is_none());

    let with_meta = schema.with_metadata("arn:aws:glue:us-east-1::schema/reg/s", 1);
    assert!(with_meta.schema_arn.is_some());
    assert_eq!(with_meta.version_number, Some(1));
}

#[test]
fn glue_data_format_parse_display() {
    for (s, expected) in [
        ("AVRO", GlueDataFormat::Avro),
        ("avro", GlueDataFormat::Avro),
        ("JSON", GlueDataFormat::Json),
        ("json", GlueDataFormat::Json),
        ("PROTOBUF", GlueDataFormat::Protobuf),
        ("protobuf", GlueDataFormat::Protobuf),
    ] {
        let parsed: GlueDataFormat = s.parse().unwrap();
        assert_eq!(parsed, expected, "parse {s}");
    }
    assert!("unknown".parse::<GlueDataFormat>().is_err());
    assert_eq!(GlueDataFormat::Avro.to_string(), "AVRO");
    assert_eq!(GlueDataFormat::Json.to_string(), "JSON");
    assert_eq!(GlueDataFormat::Protobuf.to_string(), "PROTOBUF");
}

// ── Protobuf wire format conformance ──────────────────────────────────────
//
// These tests use golden byte vectors that are byte-for-byte compatible with
// the Confluent Java client's `ProtobufSerializer` output.
//
// Reference:
//   io.confluent.kafka.schemaregistry.protobuf.MessageIndexes
//   org.apache.kafka.common.utils.ByteUtils#writeVarint (ZigZag + LEB-128)
//
// Wire format after the 5-byte Confluent header:
//   - varint: ZigZag(element count)
//   - for each index: varint of ZigZag(signed int32)
//
// With ONE mandated special case: the path [0] — the first top-level message
// in the .proto file, the overwhelmingly common case — is written as a single
// 0x00 byte, and a decoded count of 0 maps back to [0].
//
//   [0]     → 00
//   [2]     → 02 04      (ZigZag(1)=2 count, ZigZag(2)=4)
//   [1, 3]  → 04 02 06   (ZigZag(2)=4 count, ZigZag(1)=2, ZigZag(3)=6)

use schemreg::{decode_protobuf_message_indexes, encode_protobuf_wire_format};

/// Top-level message (schema index `[0]`) — the most common case, and the one
/// the Confluent serde compresses to a single zero byte.
#[test]
fn protobuf_golden_top_level_message() {
    let schema_id: u32 = 1;
    let payload = b"proto payload";

    let framed = encode_protobuf_wire_format(schema_id, &[0], payload);

    assert_eq!(framed[0], 0x00, "magic byte");
    assert_eq!(
        &framed[1..5],
        &schema_id.to_be_bytes(),
        "schema id big-endian"
    );
    assert_eq!(
        framed[5], 0x00,
        "path [0] must be the single-byte optimisation, not a count+value pair"
    );
    assert_eq!(&framed[6..], payload, "payload appended verbatim");
    assert_eq!(framed.len(), 5 + 1 + payload.len());
}

/// Nested message at index 2 — ZigZag(count=1)=2, ZigZag(2)=4.
#[test]
fn protobuf_golden_nested_message_index_2() {
    let framed = encode_protobuf_wire_format(42, &[2], b"x");
    assert_eq!(framed[5], 0x02, "ZigZag(count=1)=2");
    assert_eq!(framed[6], 0x04, "ZigZag(2)=4");
    assert_eq!(&framed[7..], b"x");
}

/// Negative index -1 — ZigZag(-1) = 1.
#[test]
fn protobuf_golden_negative_index_minus1() {
    let framed = encode_protobuf_wire_format(5, &[-1], b"");
    assert_eq!(framed[5], 0x02, "ZigZag(count=1)=2");
    assert_eq!(framed[6], 0x01, "ZigZag(-1)=1");
}

/// Multiple message indexes (deeply nested schema path).
#[test]
fn protobuf_golden_multi_index() {
    // indexes = [1, 3] → ZigZag(2)=4 count, ZigZag(1)=2, ZigZag(3)=6
    let framed = encode_protobuf_wire_format(99, &[1, 3], b"data");
    assert_eq!(framed[5], 0x04, "ZigZag(count=2)=4");
    assert_eq!(framed[6], 0x02, "ZigZag(1)=2");
    assert_eq!(framed[7], 0x06, "ZigZag(3)=6");
    assert_eq!(&framed[8..], b"data");
}

/// Round-trip: encode then decode message indexes, including the i32 extremes.
#[test]
fn protobuf_roundtrip_message_indexes() {
    for indexes in [
        vec![0i32],
        vec![1],
        vec![-1],
        vec![0, 1, 2],
        vec![i32::MAX],
        vec![i32::MIN],
    ] {
        let framed = encode_protobuf_wire_format(1, &indexes, b"payload");
        let after_header = &framed[5..];
        let (decoded_indexes, bytes_consumed) =
            decode_protobuf_message_indexes(after_header).unwrap();
        assert_eq!(decoded_indexes, indexes, "round-trip for {indexes:?}");
        assert_eq!(&after_header[bytes_consumed..], b"payload");
    }
}

/// Decode: truncated message-index array must return an error.
#[test]
fn protobuf_decode_truncated_index_array_is_error() {
    // ZigZag(count=2)=4 but only one index value follows.
    let bad: &[u8] = &[0x04, 0x00];
    let err = decode_protobuf_message_indexes(bad).unwrap_err();
    assert!(
        err.to_string().contains("truncated") || err.to_string().contains("short"),
        "unexpected error: {err}"
    );
}

/// Decode: empty slice must return an error.
#[test]
fn protobuf_decode_empty_is_error() {
    let err = decode_protobuf_message_indexes(&[]).unwrap_err();
    assert!(!err.to_string().is_empty());
}

/// Golden framed bytes for schema_id=1, top-level message, payload "hello".
///
/// This is the exact byte sequence the Confluent Java, Python, Go, and .NET
/// serializers emit — and therefore the exact sequence their deserializers
/// expect. A regression here is a silent cross-language interop break.
#[test]
fn protobuf_golden_bytes_exact() {
    let expected: &[u8] = &[
        0x00, // magic byte
        0x00, 0x00, 0x00, 0x01, // schema_id = 1 (big-endian u32)
        0x00, // message-index: optimised encoding of [0]
        b'h', b'e', b'l', b'l', b'o', // payload
    ];
    let framed = encode_protobuf_wire_format(1, &[0], b"hello");
    assert_eq!(&framed[..], expected);
}

/// Decoding bytes produced by the official Confluent serializers.
///
/// The single `0x00` must resolve to the path `[0]` with the payload starting
/// at offset 1 — not to an empty path.
#[test]
fn protobuf_decodes_official_serializer_default_index() {
    let java_bytes: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x2A, // magic + schema_id = 42
        0x00, // MessageIndexes.DEFAULT_INDEX
        0x0A, 0x03, b'a', b'b', b'c', // proto payload
    ];
    let (indexes, offset) = decode_protobuf_message_indexes(&java_bytes[5..]).unwrap();
    assert_eq!(indexes, vec![0]);
    assert_eq!(&java_bytes[5 + offset..], b"\x0a\x03abc");
}

/// A count written as a plain unsigned varint (the pre-fix behaviour of this
/// crate, and of several third-party implementations) must be rejected loudly
/// rather than silently mis-slicing the payload.
#[test]
fn protobuf_rejects_plain_unsigned_count() {
    // 0x01 ZigZag-decodes to -1.
    let err = decode_protobuf_message_indexes(&[0x01, 0x00, b'p']).unwrap_err();
    assert!(err.to_string().contains("negative"), "{err}");
}
