//! Adversarial / fuzz-regression tests for the wire-format detection and
//! decoding layers.
//!
//! These tests verify that malformed or crafted inputs are rejected cleanly
//! (never panic, always return an `Err` or passthrough) rather than silently
//! producing garbage output.

use bytes::Bytes;
use schemreg::wire::DetectedWireFormat;
use schemreg::{decode_wire_format_bytes, detect_wire_format};

// ── Confluent wire-format adversarial inputs ──────────────────────────────

/// Exactly 0 bytes: should not panic, wire format is Unknown.
#[test]
fn confluent_empty_payload_is_unknown() {
    let wf = detect_wire_format(&Bytes::new());
    assert!(matches!(wf, DetectedWireFormat::Unknown), "{wf:?}");
}

/// 1 byte: magic 0x00 but no schema-ID bytes — truncated, must be InvalidConfluent.
#[test]
fn confluent_only_magic_byte_is_unknown() {
    let buf = Bytes::from_static(&[0x00]);
    let wf = detect_wire_format(&buf);
    assert!(matches!(wf, DetectedWireFormat::InvalidConfluent), "{wf:?}");
}

/// 4 bytes: magic + 3 of the 4-byte schema ID — still truncated, InvalidConfluent.
#[test]
fn confluent_header_truncated_to_four_bytes() {
    let buf = Bytes::from_static(&[0x00, 0x00, 0x00, 0x01]);
    let wf = detect_wire_format(&buf);
    assert!(matches!(wf, DetectedWireFormat::InvalidConfluent), "{wf:?}");
}

/// 5 bytes (header only, empty payload): valid Confluent header with schema_id=1.
#[test]
fn confluent_five_byte_header_empty_payload_is_confluent() {
    let buf = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x01]);
    let wf = detect_wire_format(&buf);
    assert!(matches!(wf, DetectedWireFormat::Confluent { .. }), "{wf:?}");
}

/// Wrong magic byte (0x01): must be Unknown passthrough, not Confluent.
#[test]
fn confluent_wrong_magic_byte_is_unknown() {
    let mut buf = vec![0x01u8; 100];
    buf[0] = 0x01; // wrong magic
    let wf = detect_wire_format(&Bytes::from(buf));
    assert!(matches!(wf, DetectedWireFormat::Unknown), "{wf:?}");
}

/// Schema ID all-zeros: parse must succeed (schema_id == 0).
#[test]
fn confluent_schema_id_zero() {
    let buf = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, b'x']);
    let wf = detect_wire_format(&buf);
    assert!(
        matches!(wf, DetectedWireFormat::Confluent { schema_id, .. } if schema_id.as_u32() == 0),
        "{wf:?}"
    );
}

/// Schema ID max u32: parse must succeed (schema_id == u32::MAX).
#[test]
fn confluent_schema_id_max_u32() {
    let buf = Bytes::from_static(&[0x00, 0xFF, 0xFF, 0xFF, 0xFF, b'x']);
    let wf = detect_wire_format(&buf);
    assert!(
        matches!(wf, DetectedWireFormat::Confluent { schema_id, .. } if schema_id.as_u32() == u32::MAX),
        "{wf:?}"
    );
}

/// `decode_wire_format_bytes` on a truncated header must return Err, not panic.
#[test]
fn decode_wire_format_bytes_truncated_is_error() {
    let buf = Bytes::from_static(&[0x00, 0x00]);
    let result = decode_wire_format_bytes(&buf);
    assert!(
        result.is_err(),
        "expected Err for truncated input, got {result:?}"
    );
}

/// `decode_wire_format` on 5-byte header with empty payload — inner is empty.
#[test]
fn decode_wire_format_bytes_header_only_empty_inner() {
    let buf = Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x42]);
    let (schema_id, inner) = decode_wire_format_bytes(&buf).expect("5-byte header is valid");
    assert_eq!(schema_id.as_u32(), 0x42);
    assert!(inner.is_empty());
}

/// All-zeros 100-byte buffer with magic 0x00: parses as Confluent with schema_id=0.
#[test]
fn confluent_all_zeros_parses_schema_id_zero() {
    let buf = Bytes::from(vec![0u8; 100]);
    let wf = detect_wire_format(&buf);
    assert!(
        matches!(wf, DetectedWireFormat::Confluent { schema_id, .. } if schema_id.as_u32() == 0),
        "{wf:?}"
    );
}

// ── Glue wire-format adversarial inputs ──────────────────────────────────

#[cfg(feature = "glue")]
mod glue_adversarial {
    use bytes::Bytes;
    use schemreg::detect_wire_format;
    use schemreg::wire::DetectedWireFormat;

    /// Only the Glue version byte (0x03) — 1 byte, truncated header = InvalidGlue.
    #[test]
    fn glue_only_version_byte_is_unknown() {
        let buf = Bytes::from_static(&[0x03]);
        let wf = detect_wire_format(&buf);
        assert!(matches!(wf, DetectedWireFormat::InvalidGlue), "{wf:?}");
    }

    /// Glue version byte + 16 UUID bytes but missing compression byte — 17 bytes = InvalidGlue.
    #[test]
    fn glue_header_missing_compression_byte() {
        let mut buf = vec![0x03u8];
        buf.extend_from_slice(&[0xAAu8; 16]); // 16 bytes UUID (arbitrary)
        assert_eq!(buf.len(), 17);
        let wf = detect_wire_format(&Bytes::from(buf));
        assert!(matches!(wf, DetectedWireFormat::InvalidGlue), "{wf:?}");
    }

    /// Glue complete 18-byte header with all-zeros UUID and compression=0 (None).
    #[test]
    fn glue_all_zeros_uuid_header() {
        let mut buf = vec![0x03u8]; // version byte
        buf.extend_from_slice(&[0x00u8; 16]); // 16-byte UUID
        buf.push(0x00); // compression = None
        assert_eq!(buf.len(), 18);
        let wf = detect_wire_format(&Bytes::from(buf));
        // An all-zeros UUID is a valid parse (uuid = 00000000-...).
        assert!(matches!(wf, DetectedWireFormat::Glue { .. }), "{wf:?}");
    }

    /// Invalid Glue version byte (not 0x03): should be Unknown.
    #[test]
    fn glue_wrong_version_byte_is_unknown() {
        let mut buf = vec![0x04u8]; // wrong version
        buf.extend_from_slice(&[0xBBu8; 17]);
        let wf = detect_wire_format(&Bytes::from(buf));
        assert!(matches!(wf, DetectedWireFormat::Unknown), "{wf:?}");
    }
}

// ── Protobuf message-index adversarial inputs ─────────────────────────────

mod protobuf_adversarial {
    use bytes::Bytes;
    use schemreg::wire::decode_protobuf_message_indexes;

    /// Empty slice: empty message-index array (index count = 0 means no indexes
    /// preceding the payload) — must succeed and return an empty vec.
    #[test]
    fn protobuf_empty_index_array() {
        // varint 0 = single byte 0x00 (count = 0 → no further ints)
        let buf = Bytes::from_static(&[0x00]);
        let (idxs, consumed) =
            decode_protobuf_message_indexes(&buf).expect("empty index array is valid");
        assert!(idxs.is_empty());
        assert_eq!(consumed, 1);
    }

    /// Overlong varint: 10 bytes all with MSB set, then no terminator.
    /// Must return Err without panicking.
    #[test]
    fn protobuf_overlong_varint_is_error() {
        let buf = Bytes::from(vec![0x80u8; 20]);
        let result = decode_protobuf_message_indexes(&buf);
        assert!(
            result.is_err(),
            "overlong varint must be rejected: {result:?}"
        );
    }

    /// Varint claiming N=1 index but then truncated before the index value.
    #[test]
    fn protobuf_index_count_but_no_index_data() {
        // varint 1 = one index follows, but no index bytes present
        let buf = Bytes::from_static(&[0x01]);
        let result = decode_protobuf_message_indexes(&buf);
        assert!(
            result.is_err(),
            "missing index data must be an error: {result:?}"
        );
    }

    /// Single index [0]: the standard/common-case Confluent Protobuf framing.
    #[test]
    fn protobuf_single_index_zero() {
        // varint 1 (count), varint 0 (index)
        let buf = Bytes::from_static(&[0x01, 0x00]);
        let (idxs, consumed) =
            decode_protobuf_message_indexes(&buf).expect("single index [0] is valid");
        assert_eq!(idxs, vec![0]);
        assert_eq!(consumed, 2);
    }

    /// Large but valid index value — message indexes use zigzag encoding.
    /// Index value 150 encodes as zigzag(150)=300, varint 300=[0xAC, 0x02].
    #[test]
    fn protobuf_large_index_value() {
        // count=1, index=150 (zigzag-encoded as 300 = [0xAC, 0x02])
        let buf = Bytes::from_static(&[0x01, 0xAC, 0x02]);
        let (idxs, consumed) =
            decode_protobuf_message_indexes(&buf).expect("large index value is valid");
        assert_eq!(idxs, vec![150]);
        assert_eq!(consumed, 3);
    }
}
