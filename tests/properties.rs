//! Property-based tests for the wire decoders.
//!
//! The decoders parse attacker-controlled bytes off a Kafka topic. A fixed
//! corpus of adversarial inputs (see `tests/adversarial.rs`) covers the shapes
//! someone thought of; these cover the shapes nobody did.
//!
//! The invariants asserted here are the ones a wire-format crate should be able
//! to state unconditionally:
//!
//! 1. **No panic on any input.** Not for truncated headers, not for hostile
//!    varints, not for empty buffers. A panic in a consumer's decode loop takes
//!    down the partition.
//! 2. **No out-of-bounds offsets.** Every returned offset must be a valid index
//!    into the input, or the caller's slice will panic instead of ours.
//! 3. **Round-trip fidelity.** Anything the encoder produces, the decoder must
//!    return unchanged.

use bytes::Bytes;
use proptest::prelude::*;
use schemreg::{
    DetectedWireFormat, GlueCompression, GlueSchemaVersionId, decode_glue_wire_format,
    decode_glue_wire_format_bytes, decode_protobuf_message_indexes, decode_wire_format,
    decode_wire_format_bytes, detect_wire_format, encode_glue_wire_format,
    encode_protobuf_wire_format, encode_wire_format,
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    // ── Never panic ───────────────────────────────────────────────────────

    /// `detect_wire_format` is the first thing that touches an untrusted buffer.
    /// It must be total: every byte string maps to some variant.
    #[test]
    fn detect_wire_format_never_panics(data: Vec<u8>) {
        let _ = detect_wire_format(&data);
    }

    /// Whatever `detect` reports, the payload offset it hands back must be a
    /// legal index — callers slice with it directly.
    #[test]
    fn detected_payload_offsets_are_always_in_bounds(data: Vec<u8>) {
        match detect_wire_format(&data) {
            DetectedWireFormat::Confluent { payload_offset, .. }
            | DetectedWireFormat::Glue { payload_offset, .. } => {
                prop_assert!(
                    payload_offset <= data.len(),
                    "offset {payload_offset} exceeds buffer length {}",
                    data.len()
                );
                // Slicing at the reported offset must not panic.
                let _ = &data[payload_offset..];
            }
            _ => {}
        }
    }

    #[test]
    fn decode_wire_format_never_panics(data: Vec<u8>) {
        let _ = decode_wire_format(&data);
    }

    #[test]
    fn decode_wire_format_bytes_never_panics(data: Vec<u8>) {
        let _ = decode_wire_format_bytes(&Bytes::from(data));
    }

    /// The highest-value target in the crate: it parses a length-prefixed
    /// varint array straight out of the payload.
    #[test]
    fn decode_protobuf_message_indexes_never_panics(data: Vec<u8>) {
        let _ = decode_protobuf_message_indexes(&data);
    }

    /// A successful message-index parse must report an offset the caller can
    /// slice at. An off-by-one here is a panic in every consumer.
    #[test]
    fn protobuf_payload_offsets_are_always_in_bounds(data: Vec<u8>) {
        if let Ok((indexes, offset)) = decode_protobuf_message_indexes(&data) {
            prop_assert!(
                offset <= data.len(),
                "offset {offset} exceeds buffer length {}",
                data.len()
            );
            let _ = &data[offset..];
            prop_assert!(!indexes.is_empty(), "a successful parse never yields an empty path");
            prop_assert!(indexes.len() <= 512, "the DoS cap must hold");
        }
    }

    #[test]
    fn decode_glue_wire_format_never_panics(data: Vec<u8>) {
        let _ = decode_glue_wire_format(&data);
        let _ = decode_glue_wire_format_bytes(&Bytes::from(data));
    }

    /// Bias the generator towards buffers that *start* like a real frame, so
    /// the header-parsing paths are reached far more often than random bytes
    /// would reach them.
    #[test]
    fn decoders_never_panic_on_near_miss_frames(
        magic in prop::sample::select(vec![0x00u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0xFF]),
        rest in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut data = vec![magic];
        data.extend_from_slice(&rest);

        let _ = detect_wire_format(&data);
        let _ = decode_wire_format(&data);
        let _ = decode_glue_wire_format(&data);
        if data.len() > 5 {
            let _ = decode_protobuf_message_indexes(&data[5..]);
        }
    }

    /// Continuation-bit-heavy input is exactly what breaks a naive varint loop:
    /// unbounded shifts, overlong encodings, truncation mid-varint.
    #[test]
    fn varint_heavy_input_never_panics(
        bytes in prop::collection::vec(
            prop_oneof![
                // Mostly continuation bytes...
                8 => Just(0x80u8),
                2 => Just(0xFFu8),
                1 => any::<u8>(),
            ],
            0..80,
        ),
    ) {
        let _ = decode_protobuf_message_indexes(&bytes);
    }

    // ── Round-trip fidelity ───────────────────────────────────────────────

    #[test]
    fn confluent_round_trip(id: u32, payload: Vec<u8>) {
        let framed = encode_wire_format(id, &payload);
        let (decoded_id, decoded) = decode_wire_format(&framed)
            .expect("our own encoder must produce decodable output");
        prop_assert_eq!(decoded_id, id);
        prop_assert_eq!(decoded, &payload[..]);
    }

    #[test]
    fn confluent_bytes_round_trip_is_zero_copy(id: u32, payload: Vec<u8>) {
        let framed = encode_wire_format(id, &payload);
        let (decoded_id, decoded) = decode_wire_format_bytes(&framed)
            .expect("our own encoder must produce decodable output");
        prop_assert_eq!(decoded_id, id);
        prop_assert_eq!(&decoded[..], &payload[..]);
    }

    /// Every message-index path the encoder accepts must survive the decoder,
    /// across the whole `u32` range including the extremes.
    #[test]
    fn protobuf_index_round_trip(
        id: u32,
        indexes in prop::collection::vec(any::<u32>(), 1..12),
        payload: Vec<u8>,
    ) {
        let framed = encode_protobuf_wire_format(id, &indexes, &payload);
        let (decoded_id, after_header) = decode_wire_format(&framed)
            .expect("header must decode");
        prop_assert_eq!(decoded_id, id);

        let (decoded_indexes, offset) = decode_protobuf_message_indexes(after_header)
            .expect("our own encoder must produce decodable indexes");
        prop_assert_eq!(&decoded_indexes, &indexes);
        prop_assert_eq!(&after_header[offset..], &payload[..]);
    }

    /// The `[0]` special case must be byte-identical however it is expressed.
    #[test]
    fn protobuf_default_index_is_canonical(id: u32, payload: Vec<u8>) {
        let from_zero = encode_protobuf_wire_format(id, &[0], &payload);
        let from_empty = encode_protobuf_wire_format(id, &[], &payload);
        prop_assert_eq!(&from_zero[..], &from_empty[..]);
        prop_assert_eq!(from_zero[5], 0x00);
    }

    #[test]
    fn glue_round_trip_uncompressed(uuid_bytes: [u8; 16], payload: Vec<u8>) {
        let id = GlueSchemaVersionId::from_bytes(uuid_bytes);
        let framed = encode_glue_wire_format(id, &payload, GlueCompression::None)
            .expect("uncompressed Glue encoding is infallible");
        let (decoded_id, decoded) = decode_glue_wire_format(&framed)
            .expect("our own encoder must produce decodable output");
        prop_assert_eq!(decoded_id, id);
        prop_assert_eq!(decoded, payload);
    }

    /// The UUID must survive its text form exactly — a single transposed nibble
    /// silently points at the wrong schema.
    #[test]
    fn glue_uuid_text_round_trip(uuid_bytes: [u8; 16]) {
        let id = GlueSchemaVersionId::from_bytes(uuid_bytes);
        let parsed: GlueSchemaVersionId = id.to_string().parse()
            .expect("a rendered UUID must re-parse");
        prop_assert_eq!(parsed, id);
        prop_assert_eq!(parsed.as_bytes(), &uuid_bytes);
    }

    // ── Detection agrees with decoding ────────────────────────────────────

    /// `detect_wire_format` and `decode_wire_format` must never disagree:
    /// anything detected as Confluent must decode as Confluent, with the same
    /// schema ID. A divergence would route a message to the wrong registry.
    #[test]
    fn detection_agrees_with_confluent_decoding(data: Vec<u8>) {
        match detect_wire_format(&data) {
            DetectedWireFormat::Confluent { key, .. } => {
                let (decoded_key, _) = decode_wire_format(&data)
                    .expect("a detected Confluent frame must decode");
                prop_assert_eq!(decoded_key, key);
            }
            DetectedWireFormat::InvalidConfluent => {
                prop_assert!(
                    decode_wire_format(&data).is_err(),
                    "InvalidConfluent must not decode"
                );
            }
            DetectedWireFormat::Unknown => {
                // Unknown means the magic byte did not match, so decoding must fail.
                prop_assert!(decode_wire_format(&data).is_err());
            }
            _ => {}
        }
    }

    /// The same agreement property for Glue, with one deliberate asymmetry:
    /// detection reads only the 18-byte header, while decoding also runs ZLIB
    /// decompression. A frame with a valid header and a corrupt compressed body
    /// is therefore legitimately "detected but undecodable" — so the assertion
    /// is that detection and decoding never disagree about the *identity* of a
    /// frame, not that every detected frame decodes.
    #[test]
    fn detection_agrees_with_glue_decoding(data: Vec<u8>) {
        match detect_wire_format(&data) {
            DetectedWireFormat::Glue {
                version_id,
                compression,
                ..
            } => match decode_glue_wire_format(&data) {
                Ok((decoded_id, _)) => prop_assert_eq!(decoded_id, version_id),
                Err(e) => {
                    // The only permitted failure is decompression of a body that
                    // is not valid ZLIB. An uncompressed frame must always decode.
                    prop_assert_ne!(
                        compression,
                        GlueCompression::None,
                        "an uncompressed detected frame must always decode: {}",
                        e
                    );
                }
            },
            DetectedWireFormat::InvalidGlue => {
                prop_assert!(
                    decode_glue_wire_format(&data).is_err(),
                    "InvalidGlue must not decode"
                );
            }
            _ => {}
        }
    }

    /// The two formats must be mutually exclusive — a buffer can never be read
    /// as both, or a mixed topic would route messages to the wrong backend.
    #[test]
    fn a_buffer_is_never_both_formats(data: Vec<u8>) {
        let confluent_ok = decode_wire_format(&data).is_ok();
        let glue_ok = decode_glue_wire_format(&data).is_ok();
        prop_assert!(
            !(confluent_ok && glue_ok),
            "a buffer decoded as both Confluent and Glue"
        );
    }

    // ── Subject strategies ────────────────────────────────────────────────

    /// Subject derivation must be total for any topic/record name, and must
    /// never produce an empty subject — an empty subject would collapse a URL
    /// path segment and hit the wrong endpoint.
    #[test]
    fn subject_strategies_never_produce_empty_subjects(
        topic in "[a-zA-Z0-9._-]{1,40}",
        record in "[a-zA-Z0-9._]{1,40}",
    ) {
        use schemreg::{EncodeTarget, SubjectNameStrategy};

        for target in [EncodeTarget::Key, EncodeTarget::Value] {
            for strategy in [
                SubjectNameStrategy::TopicName,
                SubjectNameStrategy::RecordName,
                SubjectNameStrategy::TopicRecordName,
                SubjectNameStrategy::ApicurioGroupRecordName { group_id: "g".into() },
            ] {
                let subject = strategy
                    .subject_name(&topic, Some(&record), target)
                    .expect("a record name is always supplied here");
                prop_assert!(!subject.is_empty());
            }
        }
    }
}
