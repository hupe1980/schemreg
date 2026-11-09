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
//! serializers against a real Confluent Schema Registry, and each one was read
//! back by the reference's own deserializer before it was written. schemreg did
//! not participate in producing a single byte of them. Regenerate with
//! `just conformance`.
//!
//! Three placements are covered:
//!
//! | Placement | Written by | |
//! |---|---|---|
//! | `prefix-v0` | the default `prefix_schema_id_serializer` | `0x00` + ID in front of the payload |
//! | `header-v1` | the stock `header_schema_id_serializer` | `0x01` + GUID (+ Protobuf indexes) in `__value_schema_id` / `__key_schema_id` |
//! | `prefix-v1` | the reference's `SchemaId.guid_to_bytes()` | `0x01` + GUID in front of the payload — read by every Confluent deserializer, written by none by default |
//!
//! Two properties are asserted per fixture:
//!
//! 1. **Decode** — schemreg recovers the identifier, the message-index path,
//!    and the payload the reference wrote.
//! 2. **Re-encode** — feeding those parts back through schemreg's encoder
//!    reproduces the reference bytes *exactly*. This is the direction that
//!    catches a decoder which is merely permissive rather than correct.

use schemreg::{
    DetectedWireFormat, EncodeTarget, SchemaGuid, SchemaKey, decode_protobuf_message_indexes,
    decode_schema_id_header, decode_wire_format, detect_wire_format, encode_protobuf_wire_format,
    encode_schema_id_header, encode_wire_format, schema_id_header_name,
};

/// The committed fixture file, produced by the reference implementation.
const FIXTURES_JSON: &str = include_str!("fixtures/confluent_conformance.json");

#[derive(Debug, Clone)]
struct Fixture {
    name: String,
    note: String,
    schema_type: String,
    placement: String,
    target: EncodeTarget,
    schema_id: u32,
    guid: Option<SchemaGuid>,
    /// Prefix placements: the whole record value.
    framed: Option<Vec<u8>>,
    /// Header placement: the header name, its value, and the bare payload.
    header_name: Option<String>,
    header: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    message_indexes: Option<Vec<u32>>,
}

impl Fixture {
    fn framed(&self) -> &[u8] {
        self.framed
            .as_deref()
            .unwrap_or_else(|| panic!("{}: a prefix fixture records framed_hex", self.name))
    }
    fn guid(&self) -> SchemaGuid {
        self.guid
            .unwrap_or_else(|| panic!("{}: a v1 fixture records the GUID", self.name))
    }
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

    fn number(block: &str, key: &str) -> Option<u32> {
        let needle = format!("\"{key}\": ");
        let start = block.find(&needle)? + needle.len();
        let rest = &block[start..];
        let end = rest.find(|c: char| !c.is_ascii_digit())?;
        rest[..end].parse().ok()
    }

    fn indexes(block: &str) -> Option<Vec<u32>> {
        let start = block.find("\"message_indexes\": ")? + "\"message_indexes\": ".len();
        let rest = &block[start..];
        if rest.starts_with("null") {
            return None;
        }
        let end = rest.find(']')?;
        Some(
            rest[1..end]
                .split(',')
                .filter_map(|s| s.trim().parse::<u32>().ok())
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
                placement: field(&block, "placement").expect("placement").to_string(),
                target: match field(&block, "side").expect("side") {
                    "key" => EncodeTarget::Key,
                    _ => EncodeTarget::Value,
                },
                schema_id: number(&block, "schema_id").expect("schema_id"),
                guid: field(&block, "guid").map(|g| g.parse().expect("valid GUID")),
                framed: field(&block, "framed_hex").map(unhex),
                header_name: field(&block, "header_name").map(str::to_string),
                header: field(&block, "header_hex").map(unhex),
                payload: field(&block, "payload_hex").map(unhex),
                message_indexes: indexes(&block),
            }
        })
        .collect();
    parsed
}

fn placed(placement: &str) -> Vec<Fixture> {
    fixtures()
        .into_iter()
        .filter(|f| f.placement == placement)
        .collect()
}

fn by_name(name: &str) -> Fixture {
    fixtures()
        .into_iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("the fixture set must contain {name}"))
}

/// Sanity check on the harness itself: if the parser silently produced empty
/// frames or dropped a placement, every other assertion here would vacuously
/// pass.
#[test]
fn the_fixture_file_covers_every_placement_and_shape() {
    let all = fixtures();
    let count = |placement: &str, ty: &str| {
        all.iter()
            .filter(|f| f.placement == placement && f.schema_type == ty)
            .count()
    };
    for ty in ["AVRO", "JSON"] {
        assert!(count("prefix-v0", ty) >= 1, "prefix-v0 {ty}");
        assert!(count("header-v1", ty) >= 1, "header-v1 {ty}");
    }
    // Every Protobuf index shape, in both stock placements.
    assert!(count("prefix-v0", "PROTOBUF") >= 6);
    assert!(count("header-v1", "PROTOBUF") >= 6);
    assert!(count("prefix-v1", "AVRO") >= 1 && count("prefix-v1", "PROTOBUF") >= 1);
    assert!(all.iter().any(|f| f.target == EncodeTarget::Key));

    for f in &all {
        match f.placement.as_str() {
            "header-v1" => {
                assert!(
                    f.header.as_ref().is_some_and(|h| h.len() >= 17),
                    "{}",
                    f.name
                );
                assert!(
                    f.payload.as_ref().is_some_and(|p| !p.is_empty()),
                    "{}",
                    f.name
                );
            }
            _ => assert!(f.framed().len() > 5, "{}", f.name),
        }
    }
}

/// Every prefix frame is detected as Confluent — not as Glue, not as Unknown.
#[test]
fn reference_prefix_frames_are_detected_as_confluent() {
    for f in fixtures().iter().filter(|f| f.framed.is_some()) {
        let (magic, len) = match f.placement.as_str() {
            "prefix-v0" => (0x00, 5),
            _ => (0x01, 17),
        };
        assert_eq!(f.framed()[0], magic, "{}: magic byte", f.name);
        match detect_wire_format(f.framed()) {
            DetectedWireFormat::Confluent { payload_offset, .. } => {
                assert_eq!(payload_offset, len, "{}: prefix length", f.name);
            }
            other => panic!("{}: expected Confluent detection, got {other:?}", f.name),
        }
    }
}

// ── prefix-v0 ─────────────────────────────────────────────────────────────

/// Avro and JSON Schema frames carry **no** message-index array — the payload
/// starts immediately after the 5-byte header.
#[test]
fn avro_and_json_frames_have_no_message_index() {
    for f in placed("prefix-v0")
        .into_iter()
        .filter(|f| f.schema_type != "PROTOBUF")
    {
        let (key, payload) =
            decode_wire_format(f.framed()).unwrap_or_else(|e| panic!("{}: {e}", f.name));
        assert_eq!(
            key.as_id().map(|id| id.as_u32()),
            Some(f.schema_id),
            "{}",
            f.name
        );
        assert!(!payload.is_empty(), "{}: payload present", f.name);

        let reencoded = encode_wire_format(f.schema_id, payload);
        assert_eq!(reencoded.as_ref(), f.framed(), "{}: re-encode", f.name);
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
        .filter(|f| f.schema_type == "PROTOBUF" && f.framed.is_some())
    {
        let expected = f
            .message_indexes
            .clone()
            .expect("Protobuf records its index");

        let (key, after_prefix) =
            decode_wire_format(f.framed()).unwrap_or_else(|e| panic!("{}: {e}", f.name));
        let (indexes, offset) = decode_protobuf_message_indexes(after_prefix)
            .unwrap_or_else(|e| panic!("{}: message-index: {e} ({})", f.name, f.note));
        assert_eq!(indexes, expected, "{}: index path — {}", f.name, f.note);

        let payload = &after_prefix[offset..];
        assert!(!payload.is_empty(), "{}: payload swallowed", f.name);

        let reencoded = encode_protobuf_wire_format(key, &indexes, payload);
        assert_eq!(
            reencoded.as_ref(),
            f.framed(),
            "{}: re-encode — {}\nreference: {}\nschemreg:  {}",
            f.name,
            f.note,
            hex(f.framed()),
            hex(&reencoded),
        );
    }
}

/// The single-`0x00` optimisation, pinned against the reference specifically:
/// the most common Protobuf frame in existence.
#[test]
fn the_default_message_index_is_a_single_zero_byte_in_the_reference() {
    let order = by_name("protobuf_order_prefix_v0_value");
    assert_eq!(order.message_indexes, Some(vec![0]));
    assert_eq!(
        order.framed()[5],
        0x00,
        "exactly one 0x00 byte for path [0]"
    );
    assert_ne!(order.framed()[6], 0x00, "byte 6 must already be payload");

    let (_, after_prefix) = decode_wire_format(order.framed()).unwrap();
    let (indexes, offset) = decode_protobuf_message_indexes(after_prefix).unwrap();
    assert_eq!((indexes, offset), (vec![0], 1));
}

/// The count is ZigZag-encoded, pinned against the reference: `[1]` → `02 02`.
#[test]
fn the_reference_zigzag_encodes_the_element_count() {
    let invoice = by_name("protobuf_invoice_prefix_v0_value");
    assert_eq!(&invoice.framed()[5..7], &[0x02, 0x02]);

    let tax_rate = by_name("protobuf_invoice_tax_rate_prefix_v0_value");
    assert_eq!(&tax_rate.framed()[5..9], &[0x06, 0x02, 0x02, 0x00]);
}

// ── header-v1 ─────────────────────────────────────────────────────────────

/// Confluent's header serializer: `0x01` + GUID (+ Protobuf indexes) in the
/// side's header, payload bare. Decode it, and re-encode the header value
/// byte-identically.
#[test]
fn header_mode_matches_the_reference_byte_for_byte() {
    for f in placed("header-v1") {
        let header = f.header.as_deref().expect("header-v1 records the header");
        assert_eq!(
            f.header_name.as_deref(),
            Some(schema_id_header_name(f.target)),
            "{}: header name for the {:?} side",
            f.name,
            f.target
        );

        let (key, indexes) =
            decode_schema_id_header(header).unwrap_or_else(|e| panic!("{}: {e}", f.name));
        assert_eq!(key, SchemaKey::Guid(f.guid()), "{}: GUID", f.name);
        assert_eq!(
            indexes, f.message_indexes,
            "{}: index path — {}",
            f.name, f.note
        );

        let reencoded = encode_schema_id_header(f.guid(), f.message_indexes.as_deref());
        assert_eq!(
            reencoded.as_ref(),
            header,
            "{}: header re-encode\nreference: {}\nschemreg:  {}",
            f.name,
            hex(header),
            hex(&reencoded)
        );
    }
}

/// The payload in header mode is the bare serialized value — identical to the
/// bytes after the prefix (and index array) of the same value prefix-framed.
#[test]
fn a_header_mode_payload_is_the_prefix_mode_payload_unframed() {
    let header = by_name("avro_order_header_v1_value");
    let prefix = by_name("avro_order_prefix_v0_value");
    assert_eq!(header.payload.as_deref(), Some(&prefix.framed()[5..]));
    // Same schema, so the ID the header serializer resolved is the prefix's.
    assert_eq!(header.schema_id, prefix.schema_id);
}

// ── prefix-v1 ─────────────────────────────────────────────────────────────

#[test]
fn the_v1_prefix_matches_the_reference_byte_for_byte() {
    for f in placed("prefix-v1") {
        let (key, after_prefix) =
            decode_wire_format(f.framed()).unwrap_or_else(|e| panic!("{}: {e}", f.name));
        assert_eq!(key, SchemaKey::Guid(f.guid()), "{}", f.name);
        let reencoded = match &f.message_indexes {
            Some(expected) => {
                let (indexes, offset) = decode_protobuf_message_indexes(after_prefix).unwrap();
                assert_eq!(&indexes, expected, "{}", f.name);
                encode_protobuf_wire_format(key, &indexes, &after_prefix[offset..])
            }
            None => encode_wire_format(key, after_prefix),
        };
        assert_eq!(reencoded.as_ref(), f.framed(), "{}: re-encode", f.name);
    }
}

/// A frame the reference produced must never be mistaken for a Glue frame.
#[test]
fn reference_frames_are_not_confusable_with_glue() {
    for f in fixtures().iter().filter(|f| f.framed.is_some()) {
        assert!(
            schemreg::decode_glue_wire_format(f.framed()).is_err(),
            "{}: a Confluent frame must not decode as Glue",
            f.name
        );
    }
}

// ── Through the codecs ────────────────────────────────────────────────────

/// Every placement of every reference fixture decodes through the codec
/// decoders via `WireRecord`, against a registry seeded from the fixture —
/// the consumer path a real application takes.
#[cfg(all(feature = "avro", feature = "json", feature = "protobuf"))]
mod through_the_codecs {
    use std::sync::Arc;

    use bytes::Bytes;
    use schemreg::{
        AvroSchemaDecoder, JsonSchemaDecoder, ProtobufSchemaDecoder, Result, Schema, SchemaGuid,
        SchemaId, SchemaReference, SchemaRegError, SchemaRegistryClient, SchemaType, SchemaVersion,
        WireRecord,
    };

    use super::{Fixture, fixtures};

    struct Seeded(Arc<Schema>);

    impl SchemaRegistryClient for Seeded {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            (self.0.id == Some(id))
                .then(|| Arc::clone(&self.0))
                .ok_or_else(|| SchemaRegError::not_supported("unknown ID"))
        }
        async fn get_schema_by_guid(&self, guid: SchemaGuid) -> Result<Arc<Schema>> {
            (self.0.guid == Some(guid))
                .then(|| Arc::clone(&self.0))
                .ok_or_else(|| SchemaRegError::not_supported("unknown GUID"))
        }
        async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
            unreachable!()
        }
        async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
            unreachable!()
        }
        async fn register_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<SchemaId> {
            unreachable!()
        }
    }

    fn seeded(f: &Fixture, ty: SchemaType) -> Arc<Seeded> {
        let mut schema = Schema::new(SchemaId::new(f.schema_id), ty, "");
        schema.guid = f.guid;
        schema.schema = schema_text(f).into();
        Arc::new(Seeded(Arc::new(schema)))
    }

    /// The fixture's schema text, unescaped from the JSON string.
    fn schema_text(f: &Fixture) -> String {
        let raw: serde_json::Value =
            serde_json::from_str(super::FIXTURES_JSON).expect("fixture file is JSON");
        raw["fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["name"] == f.name)
            .and_then(|x| x["schema"].as_str())
            .unwrap()
            .to_string()
    }

    fn record(f: &Fixture) -> WireRecord {
        match (&f.header, &f.payload) {
            (Some(header), Some(payload)) => WireRecord::from_headers(
                f.target,
                Bytes::copy_from_slice(payload),
                [(f.header_name.as_deref().unwrap(), header.as_slice())],
            ),
            _ => WireRecord::from(f.framed().to_vec()),
        }
    }

    #[tokio::test]
    async fn avro_fixtures_decode_to_the_value_the_reference_serialized() {
        for f in fixtures().iter().filter(|f| f.schema_type == "AVRO") {
            let decoded = AvroSchemaDecoder::new(seeded(f, SchemaType::Avro))
                .decode(record(f))
                .await
                .unwrap_or_else(|e| panic!("{}: {e}", f.name));
            let expected = apache_avro::types::Value::Record(vec![
                (
                    "id".into(),
                    apache_avro::types::Value::String("order-1".into()),
                ),
                ("quantity".into(), apache_avro::types::Value::Int(3)),
            ]);
            assert_eq!(decoded, expected, "{}", f.name);
        }
    }

    #[tokio::test]
    async fn json_fixtures_decode_and_validate() {
        for f in fixtures().iter().filter(|f| f.schema_type == "JSON") {
            let decoded = JsonSchemaDecoder::with_validation(seeded(f, SchemaType::Json))
                .decode(record(f))
                .await
                .unwrap_or_else(|e| panic!("{}: {e}", f.name));
            assert_eq!(decoded, serde_json::json!({"id": "order-2", "quantity": 7}));
        }
    }

    #[tokio::test]
    async fn protobuf_fixtures_unframe_to_the_reference_path_and_payload() {
        for f in fixtures().iter().filter(|f| f.schema_type == "PROTOBUF") {
            let unframed = ProtobufSchemaDecoder::new(seeded(f, SchemaType::Protobuf))
                .unframe(record(f))
                .unwrap_or_else(|e| panic!("{}: {e}", f.name));
            assert_eq!(
                Some(unframed.message_indexes),
                f.message_indexes,
                "{}",
                f.name
            );
            if let Some(payload) = &f.payload {
                assert_eq!(unframed.payload.as_ref(), payload.as_slice(), "{}", f.name);
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
