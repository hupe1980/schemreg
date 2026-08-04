//! Protocol conformance tests — golden byte vectors.
//!
//! Each test here hard-codes the *exact* byte sequence mandated by the
//! published wire-format specifications and asserts that:
//!
//! 1. The crate's **encoder** produces the canonical bytes.
//! 2. The crate's **decoder** round-trips those canonical bytes back to the
//!    original payload and metadata.
//!
//! These tests serve as a regression guard: if the encoder or decoder ever
//! drifts from the spec, at least one assertion here will fail regardless of
//! whether the unit-level round-trip tests still pass.
//!
//! # References
//!
//! - Confluent Wire Format (Avro/JSON): <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
//! - Confluent Wire Format (Protobuf): <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html>
//! - AWS Glue Schema Registry Wire Format: <https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html>

use bytes::Bytes;
use schemreg::{
    GlueCompression, GlueSchemaVersionId, decode_glue_wire_format, decode_glue_wire_format_bytes,
    decode_protobuf_message_indexes, decode_wire_format, decode_wire_format_bytes,
    encode_glue_wire_format, encode_protobuf_wire_format, encode_wire_format,
};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a UUID string into a [`GlueSchemaVersionId`] (panics if invalid).
fn glue_id(s: &str) -> GlueSchemaVersionId {
    s.parse().expect("valid UUID")
}

// ─────────────────────────────────────────────────────────────────────────────
// Confluent wire format — Avro / JSON Schema
//
// Spec layout (5-byte header):
//
//   [0x00] [schema_id byte3] [schema_id byte2] [schema_id byte1] [schema_id byte0] [payload...]
//
// Schema ID is 4-byte **big-endian** unsigned integer.
// ─────────────────────────────────────────────────────────────────────────────

/// Golden bytes for schema_id = 1, payload = b"hello"
///
/// Header: 0x00 | 0x00 0x00 0x00 0x01  (schema_id = 1, big-endian)
/// Payload: b"hello"
const CONFLUENT_AVRO_GOLDEN: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x01, // magic + schema_id = 1
    b'h', b'e', b'l', b'l', b'o', // payload
];

#[test]
fn confluent_avro_encoder_matches_golden() {
    let encoded = encode_wire_format(1u32, b"hello");
    assert_eq!(
        encoded.as_ref(),
        CONFLUENT_AVRO_GOLDEN,
        "Confluent Avro encoder must produce the canonical byte sequence"
    );
}

#[test]
fn confluent_avro_decoder_matches_golden() {
    let (schema_id, payload) = decode_wire_format(CONFLUENT_AVRO_GOLDEN)
        .expect("canonical Confluent Avro frame must decode without error");
    assert_eq!(schema_id, 1u32, "decoded schema_id must be 1");
    assert_eq!(payload, b"hello", "decoded payload must be b\"hello\"");
}

#[test]
fn confluent_avro_bytes_decoder_matches_golden() {
    let data = Bytes::from_static(CONFLUENT_AVRO_GOLDEN);
    let (schema_id, payload) = decode_wire_format_bytes(&data)
        .expect("canonical Confluent Avro frame must decode via Bytes variant");
    assert_eq!(schema_id, 1u32);
    assert_eq!(&payload[..], b"hello");
}

/// Golden bytes for schema_id = u32::MAX, empty payload.
///
/// Header: 0x00 | 0xFF 0xFF 0xFF 0xFF
const CONFLUENT_AVRO_MAX_ID_GOLDEN: &[u8] = &[0x00, 0xFF, 0xFF, 0xFF, 0xFF];

#[test]
fn confluent_avro_max_schema_id_golden() {
    let encoded = encode_wire_format(u32::MAX, b"");
    assert_eq!(
        encoded.as_ref(),
        CONFLUENT_AVRO_MAX_ID_GOLDEN,
        "schema_id u32::MAX must encode as four 0xFF bytes"
    );
    let (id, payload) = decode_wire_format(CONFLUENT_AVRO_MAX_ID_GOLDEN).unwrap();
    assert_eq!(id, u32::MAX);
    assert!(payload.is_empty());
}

/// Golden bytes for schema_id = 256 = 0x0000_0100, payload = b"z".
///
/// Verifies big-endian byte order of the schema ID field.
const CONFLUENT_AVRO_BIG_ENDIAN_GOLDEN: &[u8] = &[0x00, 0x00, 0x00, 0x01, 0x00, b'z'];

#[test]
fn confluent_avro_big_endian_byte_order_golden() {
    let encoded = encode_wire_format(256u32, b"z");
    assert_eq!(
        encoded.as_ref(),
        CONFLUENT_AVRO_BIG_ENDIAN_GOLDEN,
        "schema_id 256 must occupy bytes [0x00, 0x00, 0x01, 0x00] in big-endian order"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Confluent wire format — Protobuf (message-index array)
//
// Spec layout after the 5-byte header:
//
//   [zigzag(count)_varint] [zigzag(index_0)_varint] ... [zigzag(index_n)_varint] [proto_bytes]
//
// Every integer — INCLUDING the leading element count — is ZigZag-encoded and
// then written as an unsigned LEB-128 varint. This mirrors
// `org.apache.kafka.common.utils.ByteUtils.writeVarint`, which the Confluent
// Java serde (`io.confluent.kafka.schemaregistry.protobuf.MessageIndexes`)
// uses for both the count and the path segments.
//
// The serde defines exactly one special case: the array [0] — the first
// top-level message in the .proto file — is written as a SINGLE 0x00 byte,
// and a decoded count of 0 maps back to [0].
//
//   path [0]      → 00              (mandated single-byte optimisation)
//   path [1]      → 02 02           (ZigZag(1)=2 count, ZigZag(1)=2)
//   path [2]      → 02 04           (ZigZag(1)=2 count, ZigZag(2)=4)
//   path [0, 1]   → 04 00 02        (ZigZag(2)=4 count, ZigZag(0)=0, ZigZag(1)=2)
//   path [-1]     → 02 01           (ZigZag(1)=2 count, ZigZag(-1)=1)
// ─────────────────────────────────────────────────────────────────────────────

/// Golden bytes for schema_id = 7, message_index = [0], payload = b"proto".
///
/// Header:        0x00 | 0x00 0x00 0x00 0x07
/// Message-index: 0x00              (single-byte optimisation for [0])
/// Payload:       b"proto"
const CONFLUENT_PROTO_GOLDEN: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x07, // magic + schema_id = 7
    0x00, // message-index: optimised encoding of [0]
    b'p', b'r', b'o', b't', b'o', // payload
];

#[test]
fn confluent_protobuf_encoder_matches_golden() {
    let encoded = encode_protobuf_wire_format(7u32, &[0], b"proto");
    assert_eq!(
        encoded.as_ref(),
        CONFLUENT_PROTO_GOLDEN,
        "Confluent Protobuf encoder must emit the single-0x00 optimisation for path [0]"
    );
}

#[test]
fn confluent_protobuf_decoder_matches_golden() {
    let (schema_id, after_header) = decode_wire_format(CONFLUENT_PROTO_GOLDEN)
        .expect("canonical frame must have a valid Confluent header");
    assert_eq!(schema_id, 7u32);

    let (indexes, offset) = decode_protobuf_message_indexes(after_header)
        .expect("canonical message-index must decode cleanly");
    assert_eq!(indexes, vec![0i32], "count 0 must decode back to path [0]");
    assert_eq!(
        &after_header[offset..],
        b"proto",
        "payload after message-index must be b\"proto\""
    );
}

/// An empty path is treated as the default path `[0]` and produces identical bytes.
#[test]
fn confluent_protobuf_empty_path_is_default_index() {
    let encoded = encode_protobuf_wire_format(7u32, &[], b"proto");
    assert_eq!(encoded.as_ref(), CONFLUENT_PROTO_GOLDEN);
}

/// Golden bytes for schema_id = 42, message_index = [1] (second top-level message).
///
/// count = 1 → ZigZag(1) = 2 → varint 0x02
/// index = 1 → ZigZag(1) = 2 → varint 0x02
const CONFLUENT_PROTO_IDX1_GOLDEN: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x2A, // magic + schema_id = 42
    0x02, 0x02, // message-index: ZigZag(count=1)=2, ZigZag(1)=2
    b'd', b'a', b't', b'a', // payload
];

#[test]
fn confluent_protobuf_index_1_golden() {
    let encoded = encode_protobuf_wire_format(42u32, &[1], b"data");
    assert_eq!(encoded.as_ref(), CONFLUENT_PROTO_IDX1_GOLDEN);

    let (id, after_header) = decode_wire_format(CONFLUENT_PROTO_IDX1_GOLDEN).unwrap();
    assert_eq!(id, 42u32);
    let (indexes, offset) = decode_protobuf_message_indexes(after_header).unwrap();
    assert_eq!(indexes, vec![1i32]);
    assert_eq!(&after_header[offset..], b"data");
}

/// Golden bytes for message_index = [2], documented in the Confluent community
/// write-ups as decimal `[1, 2]` → hex `02 04`.
const CONFLUENT_PROTO_IDX2_GOLDEN: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x01, // magic + schema_id = 1
    0x02, 0x04, // ZigZag(count=1)=2, ZigZag(2)=4
    b'y',
];

#[test]
fn confluent_protobuf_index_2_golden() {
    let encoded = encode_protobuf_wire_format(1u32, &[2], b"y");
    assert_eq!(encoded.as_ref(), CONFLUENT_PROTO_IDX2_GOLDEN);

    let (_, after_header) = decode_wire_format(CONFLUENT_PROTO_IDX2_GOLDEN).unwrap();
    let (indexes, offset) = decode_protobuf_message_indexes(after_header).unwrap();
    assert_eq!(indexes, vec![2i32]);
    assert_eq!(&after_header[offset..], b"y");
}

/// Golden bytes for schema_id = 100, nested message path [0, 1].
///
/// count = 2 → ZigZag(2) = 4 → varint 0x04
/// ZigZag(0) = 0 → varint 0x00
/// ZigZag(1) = 2 → varint 0x02
const CONFLUENT_PROTO_NESTED_GOLDEN: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x64, // magic + schema_id = 100
    0x04, 0x00, 0x02, // ZigZag(count=2)=4, ZigZag([0,1])=[0,2]
    b'n', b'e', b's', b't', // payload
];

#[test]
fn confluent_protobuf_nested_path_golden() {
    let encoded = encode_protobuf_wire_format(100u32, &[0, 1], b"nest");
    assert_eq!(encoded.as_ref(), CONFLUENT_PROTO_NESTED_GOLDEN);

    let (id, after_header) = decode_wire_format(CONFLUENT_PROTO_NESTED_GOLDEN).unwrap();
    assert_eq!(id, 100u32);
    let (indexes, offset) = decode_protobuf_message_indexes(after_header).unwrap();
    assert_eq!(indexes, vec![0i32, 1i32]);
    assert_eq!(&after_header[offset..], b"nest");
}

/// Golden bytes for the three-deep path [2, 0, 1].
///
/// count = 3 → ZigZag(3) = 6 → varint 0x06
/// ZigZag(2) = 4, ZigZag(0) = 0, ZigZag(1) = 2
const CONFLUENT_PROTO_DEEP_GOLDEN: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x01, // magic + schema_id = 1
    0x06, 0x04, 0x00, 0x02, // ZigZag(count=3)=6, then ZigZag([2,0,1])
    b'd',
];

#[test]
fn confluent_protobuf_three_deep_path_golden() {
    let encoded = encode_protobuf_wire_format(1u32, &[2, 0, 1], b"d");
    assert_eq!(encoded.as_ref(), CONFLUENT_PROTO_DEEP_GOLDEN);

    let (_, after_header) = decode_wire_format(CONFLUENT_PROTO_DEEP_GOLDEN).unwrap();
    let (indexes, offset) = decode_protobuf_message_indexes(after_header).unwrap();
    assert_eq!(indexes, vec![2i32, 0i32, 1i32]);
    assert_eq!(&after_header[offset..], b"d");
}

/// ZigZag encoding spec: for small negative values the encoding stays compact.
///
/// ZigZag(-1) = 1, ZigZag(-2) = 3, ZigZag(-3) = 5
const CONFLUENT_PROTO_NEG_INDEX_GOLDEN: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x01, // magic + schema_id = 1
    0x02, 0x01, // ZigZag(count=1)=2, ZigZag(-1)=1
    b'x',
];

#[test]
fn confluent_protobuf_negative_index_zigzag_golden() {
    // ZigZag(-1) = 1; the spec supports negative indices (reserved for future use)
    let encoded = encode_protobuf_wire_format(1u32, &[-1], b"x");
    assert_eq!(encoded.as_ref(), CONFLUENT_PROTO_NEG_INDEX_GOLDEN);

    let (_, after_header) = decode_wire_format(CONFLUENT_PROTO_NEG_INDEX_GOLDEN).unwrap();
    let (indexes, _) = decode_protobuf_message_indexes(after_header).unwrap();
    assert_eq!(indexes, vec![-1i32]);
}

/// A plain (non-ZigZag) count — what a non-conforming encoder emits for
/// count = 1 — must be rejected rather than silently mis-parsed.
///
/// 0x01 ZigZag-decodes to -1, which is not a valid element count.
#[test]
fn confluent_protobuf_rejects_non_zigzag_count() {
    let err = decode_protobuf_message_indexes(&[0x01, 0x00, b'p'])
        .expect_err("a plain unsigned count must not be accepted");
    assert!(err.to_string().contains("negative"), "{err}");
}

// ─────────────────────────────────────────────────────────────────────────────
// AWS Glue wire format
//
// Spec layout (18-byte header):
//
//   [0x03]  [compression_byte]  [uuid_bytes: 16 bytes, big-endian]  [payload...]
//
// compression_byte: 0x00 = None, 0x05 = ZLIB
// UUID is stored in big-endian (network) byte order: most-significant byte first.
// ─────────────────────────────────────────────────────────────────────────────

/// Well-known schema version UUID used across all Glue golden tests.
const GLUE_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

/// UUID bytes for "550e8400-e29b-41d4-a716-446655440000" in big-endian order.
///
/// The UUID standard stores fields in big-endian (network) byte order:
///   time_low  (4B): 55 0e 84 00
///   time_mid  (2B): e2 9b
///   time_hi   (2B): 41 d4
///   clock_seq (2B): a7 16
///   node      (6B): 44 66 55 44 00 00
const GLUE_UUID_BYTES: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, // time_low
    0xe2, 0x9b, // time_mid
    0x41, 0xd4, // time_hi
    0xa7, 0x16, // clock_seq
    0x44, 0x66, 0x55, 0x44, 0x00, 0x00, // node
];

/// Golden bytes for GlueCompression::None, payload = b"glue"
///
/// Byte 0:    0x03        (header version)
/// Byte 1:    0x00        (no compression)
/// Bytes 2–17: UUID bytes
/// Bytes 18+:  b"glue"
const GLUE_NONE_GOLDEN: &[u8] = &[
    0x03, 0x00, // version, compression=none
    // UUID 550e8400-e29b-41d4-a716-446655440000
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    b'g', b'l', b'u', b'e', // payload
];

#[test]
fn glue_none_encoder_matches_golden() {
    let id = glue_id(GLUE_UUID);
    let encoded = encode_glue_wire_format(id, b"glue", GlueCompression::None)
        .expect("None-compression Glue encode must not fail");
    assert_eq!(
        encoded.as_ref(),
        GLUE_NONE_GOLDEN,
        "Glue encoder (None) must produce the canonical byte sequence"
    );
}

#[test]
fn glue_none_decoder_matches_golden() {
    let (version_id, payload) = decode_glue_wire_format(GLUE_NONE_GOLDEN)
        .expect("canonical Glue frame must decode without error");
    assert_eq!(
        version_id,
        glue_id(GLUE_UUID),
        "decoded version UUID must match"
    );
    assert_eq!(payload, b"glue", "decoded payload must be b\"glue\"");
}

#[test]
fn glue_none_bytes_decoder_matches_golden() {
    let data = Bytes::from_static(GLUE_NONE_GOLDEN);
    let (version_id, payload) = decode_glue_wire_format_bytes(&data)
        .expect("canonical Glue Bytes frame must decode without error");
    assert_eq!(version_id, glue_id(GLUE_UUID));
    assert_eq!(&payload[..], b"glue");
}

#[test]
fn glue_uuid_bytes_big_endian_in_header() {
    // Verify the UUID field byte order independently of encode/decode helpers.
    let encoded = encode_glue_wire_format(glue_id(GLUE_UUID), b"", GlueCompression::None).unwrap();
    assert_eq!(encoded[0], 0x03, "byte 0 must be header version 0x03");
    assert_eq!(encoded[1], 0x00, "byte 1 must be compression=None (0x00)");
    assert_eq!(
        &encoded[2..18],
        &GLUE_UUID_BYTES,
        "bytes 2–17 must be the UUID in big-endian order"
    );
}

#[test]
fn glue_header_version_byte_is_0x03() {
    let encoded =
        encode_glue_wire_format(glue_id(GLUE_UUID), b"payload", GlueCompression::None).unwrap();
    assert_eq!(
        encoded[0], 0x03,
        "Glue header version byte must always be 0x03"
    );
}

#[test]
fn glue_none_compression_byte_is_0x00() {
    let encoded = encode_glue_wire_format(glue_id(GLUE_UUID), b"x", GlueCompression::None).unwrap();
    assert_eq!(
        encoded[1], 0x00,
        "Glue None-compression indicator must be 0x00"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Glue ZLIB (feature-gated)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "glue")]
#[test]
fn glue_zlib_compression_byte_is_0x05() {
    let encoded = encode_glue_wire_format(glue_id(GLUE_UUID), b"compressed", GlueCompression::Zlib)
        .expect("Zlib encoding requires glue feature");
    assert_eq!(encoded[0], 0x03, "header version must be 0x03");
    assert_eq!(encoded[1], 0x05, "ZLIB compression indicator must be 0x05");
    // UUID field must still be present at bytes 2–17
    assert_eq!(&encoded[2..18], &GLUE_UUID_BYTES);
}

#[cfg(feature = "glue")]
#[test]
fn glue_zlib_roundtrip_golden() {
    // Build a canonical ZLIB frame and verify end-to-end decode
    let original = b"schema payload data that should be compressed";
    let id = glue_id(GLUE_UUID);
    let encoded = encode_glue_wire_format(id, original, GlueCompression::Zlib)
        .expect("ZLIB encode must succeed");

    // Structural assertions
    assert_eq!(encoded[0], 0x03);
    assert_eq!(encoded[1], 0x05);
    assert_eq!(&encoded[2..18], &GLUE_UUID_BYTES);

    // Round-trip
    let (decoded_id, decoded_payload) =
        decode_glue_wire_format(&encoded).expect("ZLIB-compressed frame must decode");
    assert_eq!(decoded_id, id);
    assert_eq!(decoded_payload.as_slice(), original.as_ref());
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-format — encoder / decoder must not accept the other format's frames
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn confluent_decoder_rejects_glue_frame() {
    // A valid Glue frame starts with 0x03, which is not the Confluent magic byte (0x00).
    let glue_frame =
        encode_glue_wire_format(glue_id(GLUE_UUID), b"data", GlueCompression::None).unwrap();
    let err =
        decode_wire_format(&glue_frame).expect_err("Confluent decoder must reject a Glue frame");
    let msg = err.to_string();
    assert!(
        msg.contains("magic byte") || msg.contains("0x03"),
        "error must reference the invalid magic byte: {msg}"
    );
}

#[test]
fn glue_decoder_rejects_confluent_frame() {
    // A valid Confluent frame starts with 0x00, which is not the Glue header version (0x03).
    // The Confluent test frame is 9 bytes (5-byte header + 4-byte payload), which is also
    // shorter than the 18-byte Glue header — either mismatch causes a decode error.
    let confluent_frame = encode_wire_format(42u32, b"data");
    let err = decode_glue_wire_format(&confluent_frame)
        .expect_err("Glue decoder must reject a Confluent frame");
    // The error may mention "too short", "header version", or the byte values —
    // any of these indicate the decoder correctly rejected the foreign frame.
    let msg = err.to_string();
    assert!(
        msg.contains("too short") || msg.contains("header version") || msg.contains("0x03"),
        "error must indicate rejection of a non-Glue frame: {msg}"
    );
}
