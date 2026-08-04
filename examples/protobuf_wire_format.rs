//! Confluent **Protobuf** wire format: framing, message-index paths, and
//! byte-for-byte interoperability with the official Confluent serializers.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example protobuf_wire_format
//! ```
//!
//! No features required — the wire codec is always available.
//!
//! # Why Protobuf framing is different
//!
//! For Avro and JSON Schema, the Confluent header is exactly five bytes:
//! `0x00` + a big-endian `u32` schema ID. Protobuf inserts one more field —
//! the **message-index array** — between the header and the serialized bytes.
//!
//! A single registered `.proto` file can declare many message types, and
//! messages can nest. The index array is the path from the file root to the
//! type that was actually serialized:
//!
//! ```text
//! syntax = "proto3";
//! message Order   { ... }        // path [0]  — first top-level message
//! message Invoice {              // path [1]
//!   message Line  { ... }        // path [1, 0]
//! }
//! ```
//!
//! Every integer in the array — **including the leading element count** — is
//! ZigZag-encoded and then written as an unsigned LEB-128 varint, matching
//! `org.apache.kafka.common.utils.ByteUtils.writeVarint`. The single mandated
//! special case is the path `[0]`, which is written as one `0x00` byte.
//!
//! Getting either rule wrong is silent data corruption, not a clean failure:
//! the consumer slices the payload at the wrong offset and hands the
//! Protobuf runtime garbage.

use schemreg::{
    decode_protobuf_message_indexes, decode_wire_format, encode_protobuf_wire_format,
    encode_wire_format,
};

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A minimal, hand-rolled protobuf body: field 1, wire type 2 (length-
    // delimited), 5 bytes, "hello". Equivalent to `Order { name: "hello" }`.
    let proto_body: &[u8] = b"\x0a\x05hello";
    let schema_id = 42u32;

    println!("=== Confluent Protobuf wire format ===\n");

    // ── 1. The common case: first top-level message ───────────────────────
    //
    // Path [0] collapses to a single 0x00 byte. This is not an optimisation
    // schemreg chose — it is what the Confluent serde emits, and what its
    // deserializers expect.
    let framed = encode_protobuf_wire_format(schema_id, &[0], proto_body);
    println!("path [0]      → {}", hex(&framed));
    println!("                 ^^ magic  ^^^^^^^^^^^ schema id   ^^ index   payload");
    assert_eq!(
        &framed[..6],
        &[0x00, 0x00, 0x00, 0x00, 0x2a, 0x00],
        "path [0] must be the single-byte form"
    );

    let (id, after_header) = decode_wire_format(&framed)?;
    let (indexes, payload_offset) = decode_protobuf_message_indexes(after_header)?;
    assert_eq!(id, schema_id);
    assert_eq!(indexes, vec![0]);
    assert_eq!(&after_header[payload_offset..], proto_body);
    println!("  decoded     → id={id}, indexes={indexes:?}, payload={proto_body:?}\n");

    // ── 2. A later top-level message ──────────────────────────────────────
    //
    // Count 1 → ZigZag(1) = 2. Index 1 → ZigZag(1) = 2.
    let framed = encode_protobuf_wire_format(schema_id, &[1], proto_body);
    println!("path [1]      → {}", hex(&framed[5..]));
    assert_eq!(&framed[5..7], &[0x02, 0x02]);
    let (_, after) = decode_wire_format(&framed)?;
    assert_eq!(decode_protobuf_message_indexes(after)?.0, vec![1]);

    // ── 3. A nested message ───────────────────────────────────────────────
    //
    // Count 2 → ZigZag(2) = 4, then ZigZag(1) = 2 and ZigZag(0) = 0.
    let framed = encode_protobuf_wire_format(schema_id, &[1, 0], proto_body);
    println!("path [1, 0]   → {}", hex(&framed[5..]));
    assert_eq!(&framed[5..8], &[0x04, 0x02, 0x00]);
    let (_, after) = decode_wire_format(&framed)?;
    assert_eq!(decode_protobuf_message_indexes(after)?.0, vec![1, 0]);
    println!();

    // ── 4. Decoding bytes produced by the Confluent Java serializer ───────
    let java_produced: &[u8] = &[
        0x00, // magic byte
        0x00, 0x00, 0x00, 0x07, // schema id = 7
        0x00, // MessageIndexes.DEFAULT_INDEX
        0x0a, 0x03, b'a', b'b', b'c', // proto body
    ];
    let (id, after) = decode_wire_format(java_produced)?;
    let (indexes, offset) = decode_protobuf_message_indexes(after)?;
    println!("Java-produced → id={id}, indexes={indexes:?}");
    assert_eq!((id, indexes), (7u32.into(), vec![0]));
    assert_eq!(&after[offset..], b"\x0a\x03abc");
    println!("  a leading count of 0 means the path [0], never an empty path\n");

    // ── 5. Malformed input is rejected, not mis-sliced ────────────────────
    //
    // A plain (non-ZigZag) count of 1 — what a non-conforming encoder emits —
    // ZigZag-decodes to -1. Accepting it would desynchronise the payload
    // offset and silently corrupt every message from that producer.
    let err = decode_protobuf_message_indexes(&[0x01, 0x00, b'x'])
        .expect_err("a plain unsigned count must be rejected");
    println!("non-conforming count → rejected: {err}");

    // ── 6. Avro and JSON Schema use the plain 5-byte header ───────────────
    let avro_framed = encode_wire_format(schema_id, b"avro-bytes");
    println!("\nAvro / JSON   → {}", hex(&avro_framed));
    println!("                 no message-index array — payload starts at byte 5");

    println!("\nAll assertions passed.");
    Ok(())
}
