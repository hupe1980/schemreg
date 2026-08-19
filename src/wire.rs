//! Confluent and Glue wire formats: framing, unframing, and auto-detection.
//!
//! # Confluent framing
//!
//! A Confluent-framed record carries a short prefix naming the schema the
//! payload was written with. There are two versions of that prefix, and both
//! are in active use:
//!
//! ```text
//! v0 — schema ID (every Confluent Platform release)
//! ┌──────────┬────────────────────┬──────────────────┐
//! │ 0x00 (1B)│ Schema ID (4B, BE) │ Payload (N bytes)│
//! └──────────┴────────────────────┴──────────────────┘
//!
//! v1 — schema GUID (Confluent Platform 8+)
//! ┌──────────┬──────────────────────┬──────────────────┐
//! │ 0x01 (1B)│ Schema GUID (16B, BE)│ Payload (N bytes)│
//! └──────────┴──────────────────────┴──────────────────┘
//! ```
//!
//! A GUID is a fingerprint of the schema itself, so it identifies the same
//! schema in every registry; an ID is assigned per registry and does not. That
//! is why Confluent Platform 8 introduced v1 — see [`SchemaGuid`].
//!
//! [`decode_wire_format`] accepts either and reports which it found as a
//! [`SchemaKey`]. [`encode_wire_format`] emits whichever the caller names:
//! pass a `u32`/[`SchemaId`] for v0, a [`SchemaGuid`] for v1.
//!
//! # Confluent framing for Protobuf
//!
//! For Protobuf schemas a **message-index array** sits between the prefix and
//! the serialized bytes, encoding the path from the `.proto` file root to the
//! message type used:
//!
//! ```text
//! ┌────────────────┬─────────────────────────┬──────────────────┐
//! │ v0 or v1 prefix│ Msg-index (varint array)│ Payload (N bytes)│
//! └────────────────┴─────────────────────────┴──────────────────┘
//! ```
//!
//! **Every** integer in the array — including the leading element count — is
//! ZigZag-encoded and then written as an unsigned LEB-128 varint. This matches
//! `org.apache.kafka.common.utils.ByteUtils.writeVarint`, which the Confluent
//! Java serde uses for both the count and the path segments.
//!
//! The serde defines one mandatory special case: the array `[0]` (the first
//! top-level message in the `.proto` file — by far the most common case) is
//! encoded as a **single `0x00` byte**, not as `ZigZag(1), ZigZag(0)`.
//! Decoders must map a leading count of `0` back to `[0]`.
//!
//! | Message-index path | Encoded bytes | Derivation |
//! |---|---|---|
//! | `[0]`    | `00`          | mandated single-byte optimisation |
//! | `[1]`    | `02 02`       | ZigZag(1)=2 (count), ZigZag(1)=2 |
//! | `[2]`    | `02 04`       | ZigZag(1)=2 (count), ZigZag(2)=4 |
//! | `[0, 1]` | `04 00 02`    | ZigZag(2)=4 (count), ZigZag(0)=0, ZigZag(1)=2 |
//!
//! # Schema ID in a Kafka header
//!
//! Confluent Platform 8 can also move the identifier out of the payload
//! entirely and into a Kafka record header — `__key_schema_id` for keys,
//! `__value_schema_id` for values. The header *value* is byte-for-byte the same
//! prefix described above (magic byte, identifier, and for Protobuf the
//! message-index array); the payload then carries **no prefix at all**.
//!
//! `schemreg` produces and consumes `Bytes`, not Kafka records, so header
//! placement is exposed as a pair of codecs the caller wires to their client's
//! header API: [`encode_schema_id_header`] and [`decode_schema_id_header`].
//! See [`schema_id_header_name`] for the header names.
//!
//! # AWS Glue framing
//!
//! The AWS Glue wire format uses an 18-byte header. See [`crate::glue`] for the
//! full specification.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Result, SchemaRegError};
use crate::glue::{
    GLUE_COMPRESSION_NONE_BYTE, GLUE_COMPRESSION_ZLIB_BYTE, GLUE_HEADER_SIZE,
    GLUE_HEADER_VERSION_BYTE, GlueCompression, GlueSchemaVersionId,
};
use crate::types::{EncodeTarget, SchemaGuid, SchemaId, SchemaKey};

/// Number of bytes in a schema GUID / Glue schema-version UUID.
const UUID_BYTES: usize = 16;

// ── Constants ─────────────────────────────────────────────────────────────

/// Magic byte introducing a wire format v0 prefix (4-byte schema ID).
pub const MAGIC_BYTE_V0: u8 = 0x00;

/// Magic byte introducing a wire format v1 prefix (16-byte schema GUID).
pub const MAGIC_BYTE_V1: u8 = 0x01;

/// Length of a wire format v0 prefix: magic byte + 4-byte big-endian schema ID.
pub const PREFIX_LEN_V0: usize = 1 + 4;

/// Length of a wire format v1 prefix: magic byte + 16-byte schema GUID.
pub const PREFIX_LEN_V1: usize = 1 + UUID_BYTES;

/// Kafka record header carrying the **key** schema identifier.
pub const KEY_SCHEMA_ID_HEADER: &str = "__key_schema_id";

/// Kafka record header carrying the **value** schema identifier.
pub const VALUE_SCHEMA_ID_HEADER: &str = "__value_schema_id";

/// The Kafka header name Confluent uses for `target`'s schema identifier.
///
/// ```rust
/// use schemreg::{EncodeTarget, schema_id_header_name};
///
/// assert_eq!(schema_id_header_name(EncodeTarget::Key), "__key_schema_id");
/// assert_eq!(schema_id_header_name(EncodeTarget::Value), "__value_schema_id");
/// ```
#[must_use]
pub const fn schema_id_header_name(target: EncodeTarget) -> &'static str {
    match target {
        EncodeTarget::Key => KEY_SCHEMA_ID_HEADER,
        EncodeTarget::Value => VALUE_SCHEMA_ID_HEADER,
    }
}

/// Maximum number of path segments in a Protobuf message-index array.
///
/// The Confluent spec sets no hard limit, but any realistic message-index path
/// is far shorter than this. Enforcing a cap prevents a crafted message from
/// triggering an unbounded `Vec::with_capacity` allocation.
const MAX_MESSAGE_INDEX_COUNT: u32 = 512;

/// The canonical encoding of the message-index array `[0]`, mandated by the
/// Confluent Protobuf serde as a single-byte optimisation.
const PROTOBUF_DEFAULT_INDEX: [u8; 1] = [0x00];

// ── Varint / ZigZag helpers ───────────────────────────────────────────────

/// Encode a `u64` as an unsigned LEB-128 varint into `buf`.
#[inline]
fn write_varint(buf: &mut BytesMut, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.put_u8(byte);
            break;
        }
        buf.put_u8(byte | 0x80);
    }
}

/// Return the number of bytes required to encode `value` as a LEB-128 varint.
#[inline]
fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// ZigZag-encode a non-negative value.
///
/// The general ZigZag mapping is `(n << 1) ^ (n >> 31)`, which for `n >= 0` is
/// exactly `2n`. Every number in a message-index array — the element count and
/// each path segment — is a position in a descriptor list and so is
/// non-negative, which is why this crate models them as `u32` and never needs
/// the signed form. Widening to `u64` first keeps `u32::MAX` exact.
#[inline]
fn zigzag_encode(n: u32) -> u64 {
    u64::from(n) << 1
}

/// ZigZag-decode a raw varint that must represent a non-negative value.
///
/// Returns an error for an odd encoding (which decodes to a negative number)
/// and for anything beyond the `u32` domain. Both mean the frame was not
/// produced by a conforming Protobuf serializer: Kafka's `writeVarint` is
/// ZigZag, so a serializer that wrote a plain unsigned count produces exactly
/// the odd values rejected here.
#[inline]
fn zigzag_decode_u32(raw: u64) -> Result<u32> {
    if raw & 1 == 1 {
        return Err(SchemaRegError::wire_format(format!(
            "Protobuf message-index value ZigZag-decodes to the negative number {} — \
             the frame is not Confluent Protobuf-framed, or was produced by a \
             non-conforming serializer",
            -(((raw >> 1) + 1) as i64)
        )));
    }
    let value = raw >> 1;
    u32::try_from(value).map_err(|_| {
        SchemaRegError::wire_format(format!(
            "Protobuf message-index value {value} overflows the 32-bit range"
        ))
    })
}

/// Decode one unsigned LEB-128 varint from `data` starting at `offset`.
///
/// Returns `(value, bytes_consumed)`.
fn read_varint(data: &[u8], offset: usize) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    let mut pos = offset;

    loop {
        if pos >= data.len() {
            return Err(SchemaRegError::wire_format(
                "truncated varint in Protobuf message-index",
            ));
        }
        let byte = data[pos] as u64;
        pos += 1;
        result |= (byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(SchemaRegError::wire_format(
                "varint overflow in Protobuf message-index",
            ));
        }
    }
    Ok((result, pos - offset))
}

// ── Prefix codec ──────────────────────────────────────────────────────────

/// Write the magic byte and identifier for `key` into `buf`.
fn put_prefix(buf: &mut BytesMut, key: SchemaKey) {
    match key {
        SchemaKey::Id(id) => {
            buf.put_u8(MAGIC_BYTE_V0);
            buf.put_u32(id.as_u32());
        }
        SchemaKey::Guid(guid) => {
            buf.put_u8(MAGIC_BYTE_V1);
            buf.put_slice(guid.as_bytes());
        }
    }
}

/// Read the magic byte and identifier at the start of `data`.
///
/// Returns the key and the number of prefix bytes consumed.
///
/// # Errors
///
/// Returns a wire-format error when the buffer is empty, the magic byte is
/// neither `0x00` nor `0x01`, or the identifier is truncated.
pub fn decode_wire_prefix(data: &[u8]) -> Result<(SchemaKey, usize)> {
    let Some(&magic) = data.first() else {
        return Err(SchemaRegError::wire_format(
            "wire format data is empty: expected a magic byte",
        ));
    };
    match magic {
        MAGIC_BYTE_V0 => {
            if data.len() < PREFIX_LEN_V0 {
                return Err(SchemaRegError::wire_format(format!(
                    "wire format v0 data too short: expected at least {PREFIX_LEN_V0} bytes, got {}",
                    data.len()
                )));
            }
            let id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
            Ok((SchemaKey::Id(SchemaId::new(id)), PREFIX_LEN_V0))
        }
        MAGIC_BYTE_V1 => {
            if data.len() < PREFIX_LEN_V1 {
                return Err(SchemaRegError::wire_format(format!(
                    "wire format v1 data too short: expected at least {PREFIX_LEN_V1} bytes, got {}",
                    data.len()
                )));
            }
            let mut guid = [0u8; UUID_BYTES];
            guid.copy_from_slice(&data[1..PREFIX_LEN_V1]);
            Ok((SchemaKey::Guid(SchemaGuid::from_bytes(guid)), PREFIX_LEN_V1))
        }
        other => Err(SchemaRegError::wire_format(format!(
            "invalid wire format magic byte: expected 0x{MAGIC_BYTE_V0:02X} (schema ID) or \
             0x{MAGIC_BYTE_V1:02X} (schema GUID), got 0x{other:02X}"
        ))),
    }
}

// ── Confluent wire format (Avro / JSON Schema) ────────────────────────────

/// Frame a serialized payload with a Confluent wire-format prefix.
///
/// The prefix version follows the identifier: a `u32` or [`SchemaId`] emits v0
/// (`0x00` + 4 bytes), a [`SchemaGuid`] emits v1 (`0x01` + 16 bytes).
///
/// For Protobuf payloads use [`encode_protobuf_wire_format`] instead — the
/// message-index array is not optional.
///
/// # Example
///
/// ```rust
/// use schemreg::{SchemaGuid, encode_wire_format};
///
/// let v0 = encode_wire_format(42u32, b"hello");
/// assert_eq!(&v0[..5], &[0x00, 0, 0, 0, 42]);
/// assert_eq!(&v0[5..], b"hello");
///
/// let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
/// let v1 = encode_wire_format(guid, b"hello");
/// assert_eq!(v1[0], 0x01);
/// assert_eq!(&v1[1..17], guid.as_bytes());
/// assert_eq!(&v1[17..], b"hello");
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
#[must_use]
pub fn encode_wire_format(schema: impl Into<SchemaKey>, payload: &[u8]) -> Bytes {
    let key = schema.into();
    let mut buf = BytesMut::with_capacity(key.encoded_len() + payload.len());
    put_prefix(&mut buf, key);
    buf.put_slice(payload);
    buf.freeze()
}

/// Unframe a Confluent wire-format message.
///
/// Returns the schema identifier the prefix named and the payload that follows
/// it. For Protobuf, the returned slice still begins with the message-index
/// array — pass it to [`decode_protobuf_message_indexes`].
///
/// # Errors
///
/// Returns a wire-format error when the buffer is empty, the magic byte is
/// neither `0x00` nor `0x01`, or the prefix is truncated.
///
/// # Example
///
/// ```rust
/// use schemreg::{SchemaId, encode_wire_format, decode_wire_format};
///
/// let framed = encode_wire_format(7u32, b"data");
/// let (key, payload) = decode_wire_format(&framed)?;
/// assert_eq!(key.as_id(), Some(SchemaId::new(7)));
/// assert_eq!(payload, b"data");
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
pub fn decode_wire_format(data: &[u8]) -> Result<(SchemaKey, &[u8])> {
    let (key, prefix_len) = decode_wire_prefix(data)?;
    Ok((key, &data[prefix_len..]))
}

/// Unframe a Confluent wire-format message, returning a zero-copy [`Bytes`]
/// payload that shares `data`'s allocation.
///
/// # Errors
///
/// Same as [`decode_wire_format`].
///
/// # Example
///
/// ```rust
/// use schemreg::{encode_wire_format, decode_wire_format_bytes};
///
/// let framed = encode_wire_format(7u32, b"data");
/// let (key, payload) = decode_wire_format_bytes(&framed)?;
/// assert_eq!(key, schemreg::SchemaId::new(7));
/// assert_eq!(&payload[..], b"data");
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
pub fn decode_wire_format_bytes(data: &Bytes) -> Result<(SchemaKey, Bytes)> {
    let (key, prefix_len) = decode_wire_prefix(data)?;
    Ok((key, data.slice(prefix_len..)))
}

// ── Confluent wire format (Protobuf) ─────────────────────────────────────

/// Number of bytes the message-index array for `msg_indexes` occupies.
fn message_index_len(msg_indexes: &[u32]) -> usize {
    if msg_indexes.is_empty() || msg_indexes == [0] {
        return PROTOBUF_DEFAULT_INDEX.len();
    }
    varint_len(zigzag_encode(saturating_count(msg_indexes)))
        + msg_indexes
            .iter()
            .map(|&i| varint_len(zigzag_encode(i)))
            .sum::<usize>()
}

/// The element count as a `u32`, saturating rather than wrapping.
fn saturating_count(msg_indexes: &[u32]) -> u32 {
    u32::try_from(msg_indexes.len()).unwrap_or(u32::MAX)
}

/// Write the message-index array for `msg_indexes` into `buf`.
fn put_message_indexes(buf: &mut BytesMut, msg_indexes: &[u32]) {
    // Confluent's mandated optimisation: `[0]` (and, by extension, an empty
    // path) is written as a single zero byte rather than ZigZag(1), ZigZag(0).
    if msg_indexes.is_empty() || msg_indexes == [0] {
        buf.put_slice(&PROTOBUF_DEFAULT_INDEX);
        return;
    }
    // The element count is ZigZag-encoded too — matching Kafka's
    // `ByteUtils.writeVarint`, which the Confluent serde uses here.
    write_varint(buf, zigzag_encode(saturating_count(msg_indexes)));
    for &idx in msg_indexes {
        write_varint(buf, zigzag_encode(idx));
    }
}

/// Frame a Protobuf payload with the Confluent Protobuf wire format.
///
/// Writes the wire prefix, then the message-index array, then the serialized
/// Protobuf bytes. This framing is required by the Confluent Schema Registry
/// Protobuf serde and every compatible client (Java, Python, Go, .NET).
///
/// As with [`encode_wire_format`], the prefix version follows the identifier:
/// a `u32`/[`SchemaId`] emits v0, a [`SchemaGuid`] emits v1.
///
/// `msg_indexes` encodes the path to the message type in the `.proto` file. For
/// a top-level message at position 0 (the common case) pass `&[0]`; this is
/// emitted as the mandated single `0x00` byte. An empty slice is treated as
/// `[0]`. Do not hand-write anything else — with the `protobuf` feature the
/// path is derived from the descriptor by
/// [`message_index_path`](crate::protobuf::message_index_path).
///
/// # Example
///
/// ```rust
/// use schemreg::{encode_protobuf_wire_format, decode_wire_format, decode_protobuf_message_indexes};
///
/// // Top-level message at index 0 — encoded as one 0x00 byte.
/// let proto_bytes = b"\x0a\x05hello";
/// let framed = encode_protobuf_wire_format(42u32, &[0], proto_bytes);
/// assert_eq!(&framed[5..6], &[0x00]);
///
/// let (key, rest) = decode_wire_format(&framed)?;
/// assert_eq!(key, schemreg::SchemaId::new(42));
///
/// let (indexes, payload_offset) = decode_protobuf_message_indexes(rest)?;
/// assert_eq!(indexes, vec![0]);
/// assert_eq!(&rest[payload_offset..], proto_bytes);
///
/// // A non-default path is written as ZigZag(count), ZigZag(seg), ...
/// let nested = encode_protobuf_wire_format(1u32, &[0, 1], b"x");
/// assert_eq!(&nested[5..8], &[0x04, 0x00, 0x02]);
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
#[must_use]
pub fn encode_protobuf_wire_format(
    schema: impl Into<SchemaKey>,
    msg_indexes: &[u32],
    payload: &[u8],
) -> Bytes {
    let key = schema.into();
    let capacity = key.encoded_len() + message_index_len(msg_indexes) + payload.len();
    let mut buf = BytesMut::with_capacity(capacity);
    put_prefix(&mut buf, key);
    put_message_indexes(&mut buf, msg_indexes);
    buf.put_slice(payload);
    buf.freeze()
}

/// Strip and parse the Protobuf message-index array from the bytes immediately
/// after the wire prefix.
///
/// Returns `(indexes, bytes_consumed)` where `bytes_consumed` is the offset
/// within `after_prefix` at which the Protobuf payload begins.
///
/// `after_prefix` must be the slice returned by [`decode_wire_format`], not the
/// full framed buffer.
///
/// A leading count of `0` is the Confluent single-byte optimisation and decodes
/// to `[0]` — never to an empty path.
///
/// # Errors
///
/// Returns a wire-format error if the varint data is truncated, if a value
/// overflows the 32-bit ZigZag domain, if the element count is negative, or if
/// the count exceeds 512 (a DoS guard — no real `.proto` nests that deeply).
///
/// A value that ZigZag-decodes to a negative number is what a non-conforming
/// serializer writing a *plain* unsigned count or index emits. Confluent's own
/// Java decoder reads it without checking and fails later, when resolving the
/// message type; this decoder rejects it at the framing boundary, where the
/// error can still say what is actually wrong.
///
/// # Example
///
/// ```rust
/// use schemreg::{encode_protobuf_wire_format, decode_wire_format, decode_protobuf_message_indexes};
///
/// let framed = encode_protobuf_wire_format(1u32, &[0], b"\x0a\x03foo");
/// let (_, after_prefix) = decode_wire_format(&framed)?;
/// let (indexes, payload_start) = decode_protobuf_message_indexes(after_prefix)?;
/// assert_eq!(indexes, vec![0]);
/// assert_eq!(&after_prefix[payload_start..], b"\x0a\x03foo");
///
/// // Bytes produced by the Confluent Java serde for the default message type.
/// let (indexes, payload_start) = decode_protobuf_message_indexes(b"\x00\x0a\x03foo")?;
/// assert_eq!(indexes, vec![0]);
/// assert_eq!(payload_start, 1);
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
pub fn decode_protobuf_message_indexes(after_prefix: &[u8]) -> Result<(Vec<u32>, usize)> {
    let (raw_count, consumed) = read_varint(after_prefix, 0)?;
    let count = zigzag_decode_u32(raw_count)?;

    // Count 0 is the mandated single-byte encoding of the path `[0]`.
    if count == 0 {
        return Ok((vec![0], consumed));
    }
    if count > MAX_MESSAGE_INDEX_COUNT {
        return Err(SchemaRegError::wire_format(format!(
            "Protobuf message-index count {count} exceeds the maximum of {MAX_MESSAGE_INDEX_COUNT}"
        )));
    }

    let mut offset = consumed;
    let mut indexes = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (raw, c) = read_varint(after_prefix, offset)?;
        offset += c;
        indexes.push(zigzag_decode_u32(raw)?);
    }
    Ok((indexes, offset))
}

// ── Schema ID in a Kafka header ───────────────────────────────────────────

/// Build the value of a `__key_schema_id` / `__value_schema_id` Kafka header.
///
/// The header value is the same prefix that would otherwise sit in front of the
/// payload: magic byte, identifier, and — for Protobuf — the message-index
/// array. When the identifier travels in a header, the payload carries **no**
/// prefix at all; write the raw serialized bytes as the record value.
///
/// Pass `None` for `msg_indexes` for Avro and JSON Schema. Confluent's own
/// header serializer only ever emits a GUID, but an ID is accepted here so a
/// v0-only registry can still use header placement.
///
/// # Example
///
/// ```rust
/// use schemreg::{EncodeTarget, SchemaGuid, encode_schema_id_header, schema_id_header_name};
///
/// let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
/// let header_value = encode_schema_id_header(guid, None);
///
/// assert_eq!(schema_id_header_name(EncodeTarget::Value), "__value_schema_id");
/// assert_eq!(header_value[0], 0x01);
/// assert_eq!(header_value.len(), 17);
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
#[must_use]
pub fn encode_schema_id_header(schema: impl Into<SchemaKey>, msg_indexes: Option<&[u32]>) -> Bytes {
    let key = schema.into();
    let index_len = msg_indexes.map_or(0, message_index_len);
    let mut buf = BytesMut::with_capacity(key.encoded_len() + index_len);
    put_prefix(&mut buf, key);
    if let Some(indexes) = msg_indexes {
        put_message_indexes(&mut buf, indexes);
    }
    buf.freeze()
}

/// Parse a `__key_schema_id` / `__value_schema_id` Kafka header value.
///
/// Returns the identifier and, when the header carries a Protobuf
/// message-index array, the decoded path. `None` for the path means the header
/// ended after the identifier, which is what Avro and JSON Schema produce.
///
/// # Errors
///
/// Returns a wire-format error when the magic byte is unrecognised, the
/// identifier is truncated, or trailing bytes are present but are not a
/// well-formed message-index array.
///
/// # Example
///
/// ```rust
/// use schemreg::{SchemaGuid, decode_schema_id_header, encode_schema_id_header};
///
/// let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
///
/// let (key, indexes) = decode_schema_id_header(&encode_schema_id_header(guid, None))?;
/// assert_eq!(key.as_guid(), Some(guid));
/// assert_eq!(indexes, None);
///
/// let (_, indexes) = decode_schema_id_header(&encode_schema_id_header(guid, Some(&[1, 0])))?;
/// assert_eq!(indexes, Some(vec![1, 0]));
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
pub fn decode_schema_id_header(value: &[u8]) -> Result<(SchemaKey, Option<Vec<u32>>)> {
    let (key, prefix_len) = decode_wire_prefix(value)?;
    let rest = &value[prefix_len..];
    if rest.is_empty() {
        return Ok((key, None));
    }
    let (indexes, consumed) = decode_protobuf_message_indexes(rest)?;
    if consumed != rest.len() {
        return Err(SchemaRegError::wire_format(format!(
            "schema-id header has {} trailing byte(s) after the message-index array — \
             the header must contain the identifier and nothing else",
            rest.len() - consumed
        )));
    }
    Ok((key, Some(indexes)))
}

// ── Header-framed record ──────────────────────────────────────────────────

/// A record whose schema identifier travels in a Kafka header rather than in
/// the payload.
///
/// Returned by the codecs' `encode_with_header` methods. Write
/// [`payload`](Self::payload) as the record's key or value **and**
/// [`header_name`](Self::header_name) / [`header_value`](Self::header_value) as
/// a record header — the payload carries no prefix, so a consumer that never
/// sees the header cannot recover the schema.
///
/// # Example
///
/// ```rust
/// use schemreg::{EncodeTarget, HeaderFramed, SchemaGuid, decode_schema_id_header};
///
/// # let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
/// # let framed = HeaderFramed::new(EncodeTarget::Value, guid, None, bytes::Bytes::from_static(b"raw"));
/// // producer.send(record.header(framed.header_name, &framed.header_value)
/// //                     .payload(&framed.payload));
/// assert_eq!(framed.header_name, "__value_schema_id");
/// let (key, _) = decode_schema_id_header(&framed.header_value)?;
/// assert_eq!(key.as_guid(), Some(guid));
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderFramed {
    /// `__key_schema_id` or `__value_schema_id`, per the target.
    pub header_name: &'static str,
    /// The header value: magic byte, identifier, and — for Protobuf — the
    /// message-index array. Byte-for-byte the prefix the payload would
    /// otherwise carry.
    pub header_value: Bytes,
    /// The serialized payload, with **no** wire prefix.
    pub payload: Bytes,
}

impl HeaderFramed {
    /// Build a header-framed record from an identifier and an unframed payload.
    ///
    /// Pass `None` for `msg_indexes` for Avro and JSON Schema.
    #[must_use]
    pub fn new(
        target: EncodeTarget,
        schema: impl Into<SchemaKey>,
        msg_indexes: Option<&[u32]>,
        payload: Bytes,
    ) -> Self {
        Self {
            header_name: schema_id_header_name(target),
            header_value: encode_schema_id_header(schema, msg_indexes),
            payload,
        }
    }
}

// ── Auto-detection ────────────────────────────────────────────────────────

/// Outcome of [`detect_wire_format`] — which framing (if any) a buffer uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DetectedWireFormat {
    /// Confluent framing, v0 (`0x00` + schema ID) or v1 (`0x01` + schema GUID).
    Confluent {
        /// The identifier the prefix named.
        key: SchemaKey,
        /// Offset where payload bytes start (the message-index array, for Protobuf).
        payload_offset: usize,
    },
    /// AWS Glue framing (`0x03` + compression + UUID).
    Glue {
        /// Glue schema version UUID.
        version_id: GlueSchemaVersionId,
        /// Compression algorithm indicated in the header byte.
        compression: GlueCompression,
        /// Offset where payload bytes start.
        payload_offset: usize,
    },
    /// Looks like Confluent framing but the prefix is truncated.
    InvalidConfluent,
    /// Looks like Glue framing (`0x03`) but the header is invalid or truncated.
    InvalidGlue,
    /// Unknown or unrecognised wire format.
    Unknown,
}

/// Detect which wire format a buffer uses, from its first byte.
///
/// Never guesses: an unrecognised leading byte is [`Unknown`], and a recognised
/// one with a truncated header is the matching `Invalid*` variant rather than a
/// half-parsed result.
///
/// | First byte | Result |
/// |---|---|
/// | `0x00`, ≥ 5 bytes | `Confluent { key: Id(..) }` |
/// | `0x01`, ≥ 17 bytes | `Confluent { key: Guid(..) }` |
/// | `0x00` / `0x01`, truncated | `InvalidConfluent` |
/// | `0x03`, ≥ 18 bytes, known compression byte | `Glue { .. }` |
/// | `0x03`, truncated or unknown compression byte | `InvalidGlue` |
/// | anything else | `Unknown` |
///
/// [`Unknown`]: DetectedWireFormat::Unknown
///
/// # Example
///
/// ```rust
/// use schemreg::{DetectedWireFormat, SchemaId, SchemaKey, detect_wire_format, encode_wire_format};
///
/// let framed = encode_wire_format(42u32, b"data");
/// assert_eq!(
///     detect_wire_format(&framed),
///     DetectedWireFormat::Confluent {
///         key: SchemaKey::Id(SchemaId::new(42)),
///         payload_offset: 5,
///     }
/// );
///
/// assert_eq!(detect_wire_format(&[]), DetectedWireFormat::Unknown);
/// ```
#[must_use]
pub fn detect_wire_format(data: &[u8]) -> DetectedWireFormat {
    let Some(&first) = data.first() else {
        return DetectedWireFormat::Unknown;
    };

    match first {
        MAGIC_BYTE_V0 | MAGIC_BYTE_V1 => match decode_wire_prefix(data) {
            Ok((key, payload_offset)) => DetectedWireFormat::Confluent {
                key,
                payload_offset,
            },
            Err(_) => DetectedWireFormat::InvalidConfluent,
        },
        GLUE_HEADER_VERSION_BYTE => {
            if data.len() < GLUE_HEADER_SIZE {
                return DetectedWireFormat::InvalidGlue;
            }
            let compression = match data[1] {
                GLUE_COMPRESSION_NONE_BYTE => GlueCompression::None,
                GLUE_COMPRESSION_ZLIB_BYTE => GlueCompression::Zlib,
                _ => return DetectedWireFormat::InvalidGlue,
            };
            let mut version_bytes = [0u8; UUID_BYTES];
            version_bytes.copy_from_slice(&data[2..GLUE_HEADER_SIZE]);
            DetectedWireFormat::Glue {
                version_id: GlueSchemaVersionId::from_bytes(version_bytes),
                compression,
                payload_offset: GLUE_HEADER_SIZE,
            }
        }
        _ => DetectedWireFormat::Unknown,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn guid() -> SchemaGuid {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    // ── v0 framing ────────────────────────────────────────────────────────

    #[test]
    fn v0_round_trips() {
        let encoded = encode_wire_format(42u32, b"hello world");
        let (key, decoded) = decode_wire_format(&encoded).unwrap();
        assert_eq!(key, SchemaId::new(42));
        assert_eq!(decoded, b"hello world");
    }

    #[test]
    fn v0_empty_payload_is_just_the_prefix() {
        let encoded = encode_wire_format(1u32, b"");
        assert_eq!(encoded.len(), PREFIX_LEN_V0);
        let (key, payload) = decode_wire_format(&encoded).unwrap();
        assert_eq!(key, SchemaId::new(1));
        assert!(payload.is_empty());
    }

    #[test]
    fn v0_max_schema_id() {
        let encoded = encode_wire_format(u32::MAX, b"data");
        let (key, _) = decode_wire_format(&encoded).unwrap();
        assert_eq!(key, SchemaId::new(u32::MAX));
    }

    #[test]
    fn v0_header_bytes_are_big_endian() {
        // Schema ID 256 = 0x00000100
        let encoded = encode_wire_format(256u32, b"x");
        assert_eq!(&encoded[..5], &[0x00, 0x00, 0x00, 0x01, 0x00]);
        assert_eq!(&encoded[5..], b"x");
    }

    // ── v1 framing ────────────────────────────────────────────────────────

    #[test]
    fn v1_round_trips() {
        let encoded = encode_wire_format(guid(), b"hello");
        assert_eq!(encoded.len(), PREFIX_LEN_V1 + 5);
        assert_eq!(encoded[0], MAGIC_BYTE_V1);
        let (key, payload) = decode_wire_format(&encoded).unwrap();
        assert_eq!(key.as_guid(), Some(guid()));
        assert_eq!(payload, b"hello");
    }

    /// The 16 GUID bytes go on the wire in the same order Java's
    /// `ByteBuffer.putLong(msb); putLong(lsb)` produces — i.e. exactly the
    /// canonical text form read left to right.
    #[test]
    fn v1_guid_bytes_are_in_canonical_order() {
        let encoded = encode_wire_format(guid(), b"");
        assert_eq!(
            &encoded[1..17],
            &[
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00
            ]
        );
    }

    #[test]
    fn v1_truncated_guid_is_rejected() {
        let mut short = vec![MAGIC_BYTE_V1];
        short.extend_from_slice(&guid().as_bytes()[..15]);
        let err = decode_wire_format(&short).unwrap_err();
        assert!(err.to_string().contains("too short"), "{err}");
    }

    #[test]
    fn bytes_variant_shares_the_allocation() {
        let framed = encode_wire_format(guid(), b"payload");
        let (key, payload) = decode_wire_format_bytes(&framed).unwrap();
        assert_eq!(key.as_guid(), Some(guid()));
        assert_eq!(&payload[..], b"payload");
    }

    // ── Prefix rejection ──────────────────────────────────────────────────

    #[test]
    fn unknown_magic_byte_is_rejected() {
        let err = decode_wire_format(&[0x02, 0, 0, 0, 1, 0x42]).unwrap_err();
        assert!(err.to_string().contains("magic byte"), "{err}");
    }

    #[test]
    fn truncated_and_empty_buffers_are_rejected() {
        assert!(
            decode_wire_format(&[0x00, 0, 0])
                .unwrap_err()
                .to_string()
                .contains("too short")
        );
        assert!(
            decode_wire_format(&[])
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
    }

    // ── Protobuf message-index conformance ────────────────────────────────

    #[test]
    fn default_index_is_a_single_zero_byte() {
        // Confluent mandates the single-0x00 encoding for the path [0].
        let framed = encode_protobuf_wire_format(7u32, &[0], b"proto");
        assert_eq!(
            &framed[..],
            &[0x00, 0, 0, 0, 7, 0x00, b'p', b'r', b'o', b't', b'o'][..]
        );
    }

    #[test]
    fn empty_path_encodes_as_the_default_index() {
        assert_eq!(
            encode_protobuf_wire_format(1u32, &[], b"x"),
            encode_protobuf_wire_format(1u32, &[0], b"x")
        );
    }

    #[test]
    fn count_and_segments_are_both_zigzag_encoded() {
        // path [1] → ZigZag(1)=2 for the count, ZigZag(1)=2 for the segment.
        let framed = encode_protobuf_wire_format(1u32, &[1], b"x");
        assert_eq!(&framed[PREFIX_LEN_V0..], &[0x02, 0x02, b'x'][..]);

        // path [2] → count ZigZag(1)=2, segment ZigZag(2)=4.
        let framed = encode_protobuf_wire_format(1u32, &[2], b"x");
        assert_eq!(&framed[PREFIX_LEN_V0..], &[0x02, 0x04, b'x'][..]);

        // path [0, 1] → count ZigZag(2)=4, then ZigZag(0)=0, ZigZag(1)=2.
        let framed = encode_protobuf_wire_format(1u32, &[0, 1], b"x");
        assert_eq!(&framed[PREFIX_LEN_V0..], &[0x04, 0x00, 0x02, b'x'][..]);
    }

    #[test]
    fn protobuf_framing_works_over_a_v1_prefix() {
        let framed = encode_protobuf_wire_format(guid(), &[1, 0], b"body");
        assert_eq!(framed[0], MAGIC_BYTE_V1);
        let (key, after) = decode_wire_format(&framed).unwrap();
        assert_eq!(key.as_guid(), Some(guid()));
        let (indexes, offset) = decode_protobuf_message_indexes(after).unwrap();
        assert_eq!(indexes, vec![1, 0]);
        assert_eq!(&after[offset..], b"body");
    }

    #[test]
    fn decoding_the_default_index_yields_the_zero_path() {
        // Byte stream as produced by the Confluent Java/Python serializers.
        let (indexes, offset) = decode_protobuf_message_indexes(b"\x00payload").unwrap();
        assert_eq!(indexes, vec![0]);
        assert_eq!(offset, 1);
    }

    #[test]
    fn a_negative_count_is_rejected() {
        // 0x01 decodes (ZigZag) to -1 — what a non-conforming encoder that
        // writes a plain unsigned count would emit for count = 1.
        let err = decode_protobuf_message_indexes(b"\x01\x00rest").unwrap_err();
        assert!(err.to_string().contains("negative"), "{err}");
    }

    #[test]
    fn a_negative_segment_is_rejected() {
        // count ZigZag(1)=2, segment ZigZag(-1)=1
        let err = decode_protobuf_message_indexes(&[0x02, 0x01]).unwrap_err();
        assert!(err.to_string().contains("negative"), "{err}");
    }

    #[test]
    fn an_oversized_count_is_rejected() {
        // ZigZag(1000) = 2000 → varint 0xD0 0x0F
        let err = decode_protobuf_message_indexes(&[0xD0, 0x0F]).unwrap_err();
        assert!(err.to_string().contains("exceeds the maximum"), "{err}");
    }

    #[test]
    fn every_realistic_path_round_trips() {
        for path in [vec![0], vec![1], vec![2], vec![0, 1], vec![3, 0, 7]] {
            let framed = encode_protobuf_wire_format(9u32, &path, b"body");
            let (_, after) = decode_wire_format(&framed).unwrap();
            let (indexes, offset) = decode_protobuf_message_indexes(after).unwrap();
            assert_eq!(indexes, path, "path {path:?} must round-trip");
            assert_eq!(&after[offset..], b"body");
        }
    }

    #[test]
    fn reserved_capacity_matches_the_encoded_length() {
        for path in [vec![0], vec![1], vec![0, 1], vec![3, 0, 7], vec![300]] {
            let framed = encode_protobuf_wire_format(1u32, &path, b"body");
            assert_eq!(
                framed.len(),
                PREFIX_LEN_V0 + message_index_len(&path) + 4,
                "capacity computation must match the bytes written for {path:?}"
            );
        }
    }

    // ── Kafka header placement ────────────────────────────────────────────

    #[test]
    fn header_names_match_confluent() {
        assert_eq!(schema_id_header_name(EncodeTarget::Key), "__key_schema_id");
        assert_eq!(
            schema_id_header_name(EncodeTarget::Value),
            "__value_schema_id"
        );
    }

    #[test]
    fn header_round_trips_a_guid_without_indexes() {
        let value = encode_schema_id_header(guid(), None);
        assert_eq!(value.len(), PREFIX_LEN_V1);
        let (key, indexes) = decode_schema_id_header(&value).unwrap();
        assert_eq!(key.as_guid(), Some(guid()));
        assert_eq!(indexes, None);
    }

    #[test]
    fn header_round_trips_an_id_with_indexes() {
        let value = encode_schema_id_header(7u32, Some(&[1, 0]));
        let (key, indexes) = decode_schema_id_header(&value).unwrap();
        assert_eq!(key, SchemaId::new(7));
        assert_eq!(indexes, Some(vec![1, 0]));
    }

    /// The default path still collapses to one byte inside a header, so a
    /// header-framed Protobuf record is byte-identical to Java's.
    #[test]
    fn header_default_index_is_one_byte() {
        let value = encode_schema_id_header(guid(), Some(&[0]));
        assert_eq!(value.len(), PREFIX_LEN_V1 + 1);
        assert_eq!(value[PREFIX_LEN_V1], 0x00);
        assert_eq!(decode_schema_id_header(&value).unwrap().1, Some(vec![0]));
    }

    #[test]
    fn header_with_trailing_garbage_is_rejected() {
        let mut value = encode_schema_id_header(7u32, Some(&[0])).to_vec();
        value.push(0xFF);
        let err = decode_schema_id_header(&value).unwrap_err();
        assert!(err.to_string().contains("trailing"), "{err}");
    }

    /// A header value is the *whole* identifier — the payload has no prefix.
    /// This is the property a caller depends on when splitting the two.
    #[test]
    fn header_placement_leaves_the_payload_unframed() {
        let payload = b"\x0a\x05hello";
        let header = encode_schema_id_header(guid(), Some(&[0]));
        let prefixed = encode_protobuf_wire_format(guid(), &[0], payload);

        // The header value is exactly the prefix the payload would have carried.
        assert_eq!(&header[..], &prefixed[..header.len()]);
        assert_eq!(&prefixed[header.len()..], payload);
    }

    // ── Detection ─────────────────────────────────────────────────────────

    #[test]
    fn detects_both_confluent_versions() {
        assert_eq!(
            detect_wire_format(&encode_wire_format(42u32, b"data")),
            DetectedWireFormat::Confluent {
                key: SchemaKey::Id(SchemaId::new(42)),
                payload_offset: PREFIX_LEN_V0,
            }
        );
        assert_eq!(
            detect_wire_format(&encode_wire_format(guid(), b"data")),
            DetectedWireFormat::Confluent {
                key: SchemaKey::Guid(guid()),
                payload_offset: PREFIX_LEN_V1,
            }
        );
    }

    #[test]
    fn detects_schema_id_zero() {
        assert_eq!(
            detect_wire_format(&[MAGIC_BYTE_V0, 0x00, 0x00, 0x00, 0x00, 0x41]),
            DetectedWireFormat::Confluent {
                key: SchemaKey::Id(SchemaId::new(0)),
                payload_offset: PREFIX_LEN_V0,
            }
        );
    }

    #[test]
    fn detects_unknown_and_truncated_headers() {
        assert_eq!(detect_wire_format(&[]), DetectedWireFormat::Unknown);
        assert_eq!(
            detect_wire_format(&[0x99, 0x00, 0x00]),
            DetectedWireFormat::Unknown
        );
        assert_eq!(
            detect_wire_format(&[MAGIC_BYTE_V0, 0x01, 0x02]),
            DetectedWireFormat::InvalidConfluent
        );
        assert_eq!(
            detect_wire_format(&[MAGIC_BYTE_V1, 0x01, 0x02]),
            DetectedWireFormat::InvalidConfluent
        );
        assert_eq!(
            detect_wire_format(&[GLUE_HEADER_VERSION_BYTE, GLUE_COMPRESSION_NONE_BYTE]),
            DetectedWireFormat::InvalidGlue
        );
    }

    /// `0x03` is Glue's version byte, so Confluent detection must not claim it.
    #[test]
    fn glue_and_confluent_magic_bytes_do_not_collide() {
        let mut glue = vec![GLUE_HEADER_VERSION_BYTE, GLUE_COMPRESSION_NONE_BYTE];
        glue.extend_from_slice(&[0u8; UUID_BYTES]);
        assert!(matches!(
            detect_wire_format(&glue),
            DetectedWireFormat::Glue { .. }
        ));
        assert!(decode_wire_format(&glue).is_err());
    }
}
