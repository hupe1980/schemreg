//! The three ways a Confluent record can name its schema.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example schema_guid_and_headers
//! ```
//!
//! Confluent Platform 8 added two placements to the original 5-byte prefix, and
//! all three are on the wire in production:
//!
//! 1. **v0** — `0x00` + a 4-byte schema ID, in the payload prefix.
//! 2. **v1** — `0x01` + a 16-byte schema GUID, in the payload prefix.
//! 3. **Header** — either of the above, in a `__key_schema_id` /
//!    `__value_schema_id` Kafka record header, with the payload left unframed.
//!
//! A consumer does not choose; the producer does. This example shows what each
//! looks like byte-for-byte and how to handle all three with one code path.

use schemreg::{
    DetectedWireFormat, EncodeTarget, SchemaGuid, SchemaId, SchemaKey,
    decode_protobuf_message_indexes, decode_schema_id_header, decode_wire_format,
    detect_wire_format, encode_protobuf_wire_format, encode_schema_id_header, encode_wire_format,
    schema_id_header_name,
};

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let payload = b"serialised-avro-bytes";
    let guid: SchemaGuid = "8f14e45f-ceea-467a-9575-0b7d1c9b1d8f".parse()?;

    // ── 1. v0 — schema ID in the prefix ──────────────────────────────────
    println!("=== v0: schema ID in the payload prefix ===");
    let v0 = encode_wire_format(42u32, payload);
    println!("  bytes:  {}", hex(&v0[..8]));
    println!("          ^^ magic 0x00");
    println!("             ^^^^^^^^^^^ schema id 42, big-endian");
    println!("  total:  {} bytes ({} of prefix)", v0.len(), 5);

    // ── 2. v1 — schema GUID in the prefix ────────────────────────────────
    println!("\n=== v1: schema GUID in the payload prefix ===");
    let v1 = encode_wire_format(guid, payload);
    println!("  bytes:  {}", hex(&v1[..20]));
    println!("          ^^ magic 0x01");
    println!("             ^^ …16 GUID bytes, big-endian");
    println!("  guid:   {guid}");
    println!("  total:  {} bytes ({} of prefix)", v1.len(), 17);

    // The payload is identical; only the prefix differs.
    assert_eq!(&v0[5..], payload);
    assert_eq!(&v1[17..], payload);

    // ── 3. One decode path for both ──────────────────────────────────────
    println!("\n=== Decoding: the producer chose, the consumer copes ===");
    for (label, framed) in [("v0", &v0), ("v1", &v1)] {
        let (key, body) = decode_wire_format(framed)?;
        println!("  {label}: {key:<48} payload {} bytes", body.len());
        assert_eq!(body, payload);

        // In real code, hand the key straight to the registry:
        //     let schema = registry.get_schema_by_key(key).await?;
        match key {
            SchemaKey::Id(id) => assert_eq!(id, SchemaId::new(42)),
            SchemaKey::Guid(g) => assert_eq!(g, guid),
        }
    }

    // Detection reports the same thing without consuming the buffer.
    assert_eq!(
        detect_wire_format(&v1),
        DetectedWireFormat::Confluent {
            key: SchemaKey::Guid(guid),
            payload_offset: 17,
        }
    );

    // ── 4. Header placement ──────────────────────────────────────────────
    println!("\n=== Header placement: identifier beside the payload ===");
    let header_name = schema_id_header_name(EncodeTarget::Value);
    let header_value = encode_schema_id_header(guid, None);

    println!("  header: {header_name}");
    println!("  value:  {}", hex(&header_value));
    println!("  payload: written unframed — {} bytes", payload.len());

    // The header value is *exactly* the prefix the payload no longer carries.
    assert_eq!(&header_value[..], &v1[..header_value.len()]);

    let (key, msg_indexes) = decode_schema_id_header(&header_value)?;
    assert_eq!(key.as_guid(), Some(guid));
    assert_eq!(msg_indexes, None, "Avro and JSON carry no message index");

    // ── 5. Protobuf: the message index rides along ───────────────────────
    println!("\n=== Protobuf: the message index follows the identifier ===");
    let proto_body = b"\x0a\x05hello";
    let path = [1u32, 0]; // second top-level message, its first nested type

    let prefixed = encode_protobuf_wire_format(guid, &path, proto_body);
    let (key, after_prefix) = decode_wire_format(&prefixed)?;
    let (indexes, offset) = decode_protobuf_message_indexes(after_prefix)?;
    println!("  prefixed: {}", hex(&prefixed[..22]));
    println!("  indexes:  {indexes:?} (from {key})");
    assert_eq!(indexes, path);
    assert_eq!(&after_prefix[offset..], proto_body);

    // The same array goes in the header when the identifier does.
    let header_value = encode_schema_id_header(guid, Some(&path));
    let (_, indexes) = decode_schema_id_header(&header_value)?;
    println!("  header:   {}", hex(&header_value));
    assert_eq!(indexes, Some(path.to_vec()));

    // ── 6. Why GUIDs exist ───────────────────────────────────────────────
    println!("\n=== Why a GUID and not an ID ===");
    println!("  A schema ID is assigned by one registry, so the same schema has");
    println!("  a different ID in staging and production — replicating a topic");
    println!("  across clusters means rewriting every record's prefix.");
    println!("  A GUID is a fingerprint of the schema itself, so it is the same");
    println!("  everywhere. `Schema::key()` prefers it when the registry has one.");

    println!("\nAll assertions passed.");
    Ok(())
}
