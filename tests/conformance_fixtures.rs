//! Cross-language conformance: decode bytes produced by the **official**
//! Confluent serializers, and re-encode them byte-identically.
//!
//! `tests/conformance.rs` holds golden vectors derived from reading the
//! specification. Those are a large improvement over validating an
//! implementation against itself — but they are still one person's reading. A
//! wrong reading is exactly how the v0.3.0 Protobuf message-index bug survived
//! a fully green suite: the implementation and the golden vector agreed with
//! each other and disagreed with the world.
//!
//! The fixtures consumed here were produced by `confluent-kafka-python`'s
//! `AvroSerializer`, `JSONSerializer`, and `ProtobufSerializer`, running
//! against a real Confluent Schema Registry. schemreg did not participate in
//! producing a single byte of them. Regenerate with:
//!
//! ```bash
//! docker compose -f conformance/docker-compose.yml up --build --abort-on-container-exit
//! ```
//!
//! Two properties are asserted per fixture:
//!
//! 1. **Decode** — schemreg recovers the schema ID, the message-index path, and
//!    the payload that the reference serializer wrote.
//! 2. **Re-encode** — feeding those parts back through schemreg's encoder
//!    reproduces the reference bytes *exactly*. This is the direction that
//!    catches a decoder which is merely permissive rather than correct.

use schemreg::{
    DetectedWireFormat, decode_protobuf_message_indexes, decode_wire_format, detect_wire_format,
    encode_protobuf_wire_format, encode_wire_format,
};

/// The committed fixture file, produced by the reference implementation.
const FIXTURES_JSON: &str = include_str!("fixtures/confluent_conformance.json");

#[derive(Debug)]
struct Fixture {
    name: String,
    note: String,
    schema_type: String,
    framed: Vec<u8>,
    message_indexes: Option<Vec<i32>>,
}

/// Minimal extraction from the fixture JSON.
///
/// Deliberately hand-rolled rather than pulled through `serde_json`: this test
/// must run under `--no-default-features`, where `serde_json` is not a
/// dependency, and the file shape is fixed by a generator in this repository.
fn fixtures() -> Vec<Fixture> {
    fn field<'a>(block: &'a str, key: &str) -> Option<&'a str> {
        let needle = format!("\"{key}\": \"");
        let start = block.find(&needle)? + needle.len();
        let rest = &block[start..];
        let end = rest.find('"')?;
        Some(&rest[..end])
    }

    fn indexes(block: &str) -> Option<Vec<i32>> {
        let start = block.find("\"message_indexes\": ")? + "\"message_indexes\": ".len();
        let rest = &block[start..];
        if rest.starts_with("null") {
            return None;
        }
        let end = rest.find(']')?;
        let inner = rest[1..end].trim();
        if inner.is_empty() {
            return Some(Vec::new());
        }
        Some(
            inner
                .split(',')
                .filter_map(|s| s.trim().parse::<i32>().ok())
                .collect(),
        )
    }

    fn unhex(hex: &str) -> Vec<u8> {
        assert!(hex.len().is_multiple_of(2), "hex must have even length");
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    let parsed: Vec<Fixture> = FIXTURES_JSON
        .split("\"name\": \"")
        .skip(1)
        .map(|chunk| {
            let block = format!("\"name\": \"{chunk}");
            Fixture {
                name: field(&block, "name").expect("name").to_string(),
                note: field(&block, "note").unwrap_or_default().to_string(),
                schema_type: field(&block, "schema_type")
                    .expect("schema_type")
                    .to_string(),
                framed: unhex(field(&block, "framed_hex").expect("framed_hex")),
                message_indexes: indexes(&block),
            }
        })
        .collect();

    assert!(
        parsed.len() >= 8,
        "expected at least 8 fixtures, found {} — has the fixture file been truncated?",
        parsed.len()
    );
    parsed
}

/// Sanity check on the harness itself: if the parser silently produced empty
/// frames, every other assertion here would vacuously pass.
#[test]
fn the_fixture_file_parses() {
    let all = fixtures();
    for f in &all {
        assert!(
            f.framed.len() > 5,
            "{}: fixture must contain a header plus payload",
            f.name
        );
        assert!(!f.schema_type.is_empty(), "{}: missing schema type", f.name);
    }

    let protobuf_count = all.iter().filter(|f| f.schema_type == "PROTOBUF").count();
    assert!(
        protobuf_count >= 6,
        "the Protobuf message-index shapes are the whole point; found only {protobuf_count}"
    );
}

/// Every reference frame must carry the Confluent magic byte and be detected as
/// Confluent — not as Glue, not as Unknown.
#[test]
fn reference_frames_are_detected_as_confluent() {
    for f in fixtures() {
        assert_eq!(f.framed[0], 0x00, "{}: magic byte", f.name);
        match detect_wire_format(&f.framed) {
            DetectedWireFormat::Confluent { payload_offset, .. } => {
                assert_eq!(payload_offset, 5, "{}: header is always 5 bytes", f.name);
            }
            other => panic!("{}: expected Confluent detection, got {other:?}", f.name),
        }
    }
}

/// Avro and JSON Schema frames carry **no** message-index array — the payload
/// starts immediately after the 5-byte header.
#[test]
fn avro_and_json_frames_have_no_message_index() {
    for f in fixtures()
        .into_iter()
        .filter(|f| f.schema_type == "AVRO" || f.schema_type == "JSON")
    {
        let (schema_id, payload) =
            decode_wire_format(&f.framed).unwrap_or_else(|e| panic!("{}: {e}", f.name));
        assert!(schema_id.as_u32() > 0, "{}: registry-assigned ID", f.name);
        assert!(!payload.is_empty(), "{}: payload present", f.name);

        // Re-encoding the recovered parts must reproduce the reference bytes.
        let reencoded = encode_wire_format(schema_id, payload);
        assert_eq!(
            reencoded.as_ref(),
            f.framed.as_slice(),
            "{}: re-encoding must be byte-identical to the reference",
            f.name
        );
    }
}

/// **The headline conformance test.**
///
/// For every Protobuf message shape in the reference `.proto` — first top-level,
/// later top-level, one level nested, two levels nested — schemreg must recover
/// the exact message-index path the official serializer wrote, and re-encoding
/// it must reproduce the reference bytes.
///
/// A v0.3.0 build fails the very first fixture: it emits `01 00` where the
/// reference emits `00`, and reads the reference's `02 02` as a two-element
/// path, consuming a byte of the payload.
#[test]
fn protobuf_message_index_paths_match_the_reference_byte_for_byte() {
    for f in fixtures()
        .into_iter()
        .filter(|f| f.schema_type == "PROTOBUF")
    {
        let expected_indexes = f
            .message_indexes
            .clone()
            .unwrap_or_else(|| panic!("{}: a Protobuf fixture must record its index", f.name));

        // 1. Decode.
        let (schema_id, after_header) =
            decode_wire_format(&f.framed).unwrap_or_else(|e| panic!("{}: header: {e}", f.name));
        let (indexes, offset) = decode_protobuf_message_indexes(after_header)
            .unwrap_or_else(|e| panic!("{}: message-index: {e} ({})", f.name, f.note));

        assert_eq!(
            indexes, expected_indexes,
            "{}: decoded index path must match the reference — {}",
            f.name, f.note
        );

        let payload = &after_header[offset..];
        assert!(
            !payload.is_empty(),
            "{}: the payload must not be swallowed by the index parser",
            f.name
        );

        // 2. Re-encode. This is what a permissive-but-wrong decoder fails.
        let reencoded = encode_protobuf_wire_format(schema_id, &indexes, payload);
        assert_eq!(
            reencoded.as_ref(),
            f.framed.as_slice(),
            "{}: re-encoding must reproduce the reference bytes exactly — {}\n\
             reference: {}\n\
             schemreg:  {}",
            f.name,
            f.note,
            hex(&f.framed),
            hex(&reencoded),
        );
    }
}

/// The single-`0x00` optimisation, pinned against the reference specifically.
///
/// This is the most common Protobuf frame in existence — the first message type
/// in a `.proto` file — so it is the one worth asserting on its own rather than
/// only inside a loop.
#[test]
fn the_default_message_index_is_a_single_zero_byte_in_the_reference() {
    let order = fixtures()
        .into_iter()
        .find(|f| f.name == "protobuf_order")
        .expect("the fixture set must contain the first top-level message");

    assert_eq!(
        order.message_indexes,
        Some(vec![0]),
        "protobuf_order must be the first top-level message"
    );
    assert_eq!(
        order.framed[5], 0x00,
        "the reference serializer writes exactly one 0x00 byte for path [0], \
         not a ZigZag(count)/ZigZag(index) pair"
    );
    assert_ne!(
        order.framed[6], 0x00,
        "byte 6 must already be payload — if the reference had written a \
         two-byte index this assertion would be wrong"
    );

    let (_, after_header) = decode_wire_format(&order.framed).unwrap();
    let (indexes, offset) = decode_protobuf_message_indexes(after_header).unwrap();
    assert_eq!(indexes, vec![0], "count 0 must decode back to path [0]");
    assert_eq!(offset, 1, "the optimised form consumes exactly one byte");
}

/// The count is ZigZag-encoded, pinned against the reference.
///
/// `[1]` → `02 02`: ZigZag(1)=2 for the count, ZigZag(1)=2 for the segment. A
/// plain unsigned count would be `01 02`.
#[test]
fn the_reference_zigzag_encodes_the_element_count() {
    let invoice = fixtures()
        .into_iter()
        .find(|f| f.name == "protobuf_invoice")
        .expect("the fixture set must contain a later top-level message");

    assert_eq!(invoice.message_indexes, Some(vec![1]));
    assert_eq!(
        &invoice.framed[5..7],
        &[0x02, 0x02],
        "the reference writes ZigZag(count=1)=2, not a plain 1"
    );

    let tax_rate = fixtures()
        .into_iter()
        .find(|f| f.name == "protobuf_invoice_tax_rate")
        .expect("the fixture set must contain a two-level-nested message");

    assert_eq!(tax_rate.message_indexes, Some(vec![1, 1, 0]));
    assert_eq!(
        &tax_rate.framed[5..9],
        &[0x06, 0x02, 0x02, 0x00],
        "three segments: ZigZag(count=3)=6, then ZigZag(1), ZigZag(1), ZigZag(0)"
    );
}

/// A frame the reference produced must never be mistaken for a Glue frame.
#[test]
fn reference_frames_are_not_confusable_with_glue() {
    for f in fixtures() {
        assert!(
            schemreg::decode_glue_wire_format(&f.framed).is_err(),
            "{}: a Confluent frame must not decode as Glue",
            f.name
        );
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
