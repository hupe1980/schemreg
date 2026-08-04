//! Confluent and Glue wire format encode/decode and auto-detection.
//!
//! # Confluent Wire Format (Avro / JSON Schema)
//!
//! The Confluent wire format prepends a 5-byte header to every serialized
//! payload:
//!
//! ```text
//! ┌──────────┬────────────────────┬──────────────────┐
//! │ 0x00 (1B)│ Schema ID (4B, BE) │ Payload (N bytes)│
//! └──────────┴────────────────────┴──────────────────┘
//! ```
//!
//! Use [`encode_wire_format()`] to frame and [`decode_wire_format()`] to
//! unframe.
//!
//! # Confluent Wire Format (Protobuf)
//!
//! For Protobuf schemas, a **message-index array** is inserted between the
//! 5-byte header and the serialized bytes. The array encodes the path from the
//! `.proto` file root to the message type used:
//!
//! ```text
//! ┌──────────┬────────────────────┬─────────────────────────┬──────────────────┐
//! │ 0x00 (1B)│ Schema ID (4B, BE) │ Msg-index (varint array)│ Payload (N bytes)│
//! └──────────┴────────────────────┴─────────────────────────┴──────────────────┘
//! ```
//!
//! **Every** integer in the array — including the leading element count — is
//! ZigZag-encoded and then written as an unsigned LEB-128 varint. This matches
//! `org.apache.kafka.common.utils.ByteUtils.writeVarint`, which the Confluent
//! Java serde uses for both the count and the path segments.
//!
//! The Confluent serde defines one mandatory special case: the array `[0]`
//! (the first top-level message in the `.proto` file — by far the most common
//! case) is encoded as a **single `0x00` byte**, not as `ZigZag(1), ZigZag(0)`.
//! Decoders must map a leading count of `0` back to `[0]`.
//!
//! Worked examples:
//!
//! | Message-index path | Encoded bytes | Derivation |
//! |---|---|---|
//! | `[0]`    | `00`          | mandated single-byte optimisation |
//! | `[1]`    | `02 02`       | ZigZag(1)=2 (count), ZigZag(1)=2 |
//! | `[2]`    | `02 04`       | ZigZag(1)=2 (count), ZigZag(2)=4 |
//! | `[0, 1]` | `04 00 02`    | ZigZag(2)=4 (count), ZigZag(0)=0, ZigZag(1)=2 |
//!
//! Use [`encode_protobuf_wire_format()`] to frame and
//! [`decode_protobuf_message_indexes()`] to strip and parse the index.
//!
//! # AWS Glue Wire Format
//!
//! The AWS Glue wire format uses an 18-byte header. See [`crate::glue`] for
//! the full specification.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Result, SchemaRegError};
use crate::glue::{
    GLUE_COMPRESSION_NONE_BYTE, GLUE_COMPRESSION_ZLIB_BYTE, GLUE_HEADER_SIZE,
    GLUE_HEADER_VERSION_BYTE, GlueCompression, GlueSchemaVersionId,
};
use crate::types::SchemaId;

/// Magic byte for the Confluent wire format header.
pub(crate) const MAGIC_BYTE: u8 = 0x00;

/// Size of the Confluent wire format header (magic byte + 4-byte big-endian schema ID).
pub(crate) const HEADER_SIZE: usize = 5;

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

/// ZigZag-encode a signed 32-bit integer to an unsigned value.
///
/// This maps small-magnitude signed values to small unsigned values so that
/// the subsequent LEB-128 varint encoding stays compact.
#[inline]
fn zigzag_encode(n: i32) -> u64 {
    ((n << 1) ^ (n >> 31)) as u32 as u64
}

/// ZigZag-decode an unsigned value back to a signed 32-bit integer.
#[inline]
fn zigzag_decode(n: u64) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

/// Maximum number of path segments in a Protobuf message-index array.
///
/// The Confluent spec does not set a hard limit, but any realistic
/// message-index path is far shorter than this. Enforcing a cap prevents
/// a crafted message from triggering unbounded `Vec::with_capacity` allocation.
const MAX_MESSAGE_INDEX_COUNT: i32 = 512;

/// ZigZag-decode a raw varint into an `i32`, rejecting values outside the
/// 32-bit ZigZag domain.
#[inline]
fn zigzag_decode_i32(raw: u64) -> Result<i32> {
    if raw > u64::from(u32::MAX) {
        return Err(SchemaRegError::wire_format(
            "Protobuf message-index value overflows the i32 ZigZag range",
        ));
    }
    Ok(zigzag_decode(raw))
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

// ── Confluent wire format (Avro / JSON Schema) ────────────────────────────

/// Encode a payload with the Confluent wire format header.
///
/// Prepends a 5-byte header (`0x00` + 4-byte big-endian schema ID) to the
/// payload, producing a [`Bytes`] value ready for use as a message key or
/// value.
///
/// # Example
///
/// ```rust
/// use schemreg::encode_wire_format;
///
/// let framed = encode_wire_format(42, b"hello");
/// assert_eq!(&framed[..5], &[0x00, 0, 0, 0, 42]);
/// assert_eq!(&framed[5..], b"hello");
/// ```
#[must_use]
pub fn encode_wire_format(schema_id: impl Into<SchemaId>, payload: &[u8]) -> Bytes {
    let schema_id = schema_id.into();
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(MAGIC_BYTE);
    buf.put_u32(schema_id.as_u32());
    buf.put_slice(payload);
    buf.freeze()
}

/// Decode a Confluent wire format message.
///
/// Returns the schema ID and the payload slice after the 5-byte header.
///
/// # Errors
///
/// Returns a wire format error if:
/// - The data is shorter than 5 bytes.
/// - The magic byte is not `0x00`.
///
/// # Example
///
/// ```rust
/// use schemreg::{encode_wire_format, decode_wire_format};
///
/// let framed = encode_wire_format(7, b"data");
/// let (id, payload) = decode_wire_format(&framed).unwrap();
/// assert_eq!(id, 7);
/// assert_eq!(payload, b"data");
/// ```
pub fn decode_wire_format(data: &[u8]) -> Result<(SchemaId, &[u8])> {
    let schema_id = validate_wire_header(data)?;
    Ok((schema_id, &data[HEADER_SIZE..]))
}

/// Decode a Confluent wire format message, returning a zero-copy [`Bytes`] payload.
///
/// The returned payload shares the same backing allocation as `data`
/// (no extra allocation). This variant is preferred when working with
/// [`Bytes`] values.
///
/// # Errors
///
/// Same as [`decode_wire_format()`].
///
/// # Example
///
/// ```rust
/// use bytes::Bytes;
/// use schemreg::{encode_wire_format, decode_wire_format_bytes};
///
/// let framed = encode_wire_format(7, b"data");
/// let (id, payload) = decode_wire_format_bytes(&framed).unwrap();
/// assert_eq!(id, 7);
/// assert_eq!(&payload[..], b"data");
/// ```
pub fn decode_wire_format_bytes(data: &Bytes) -> Result<(SchemaId, Bytes)> {
    let schema_id = validate_wire_header(data)?;
    Ok((schema_id, data.slice(HEADER_SIZE..)))
}

/// Validate the Confluent wire format header and extract the schema ID.
pub(crate) fn validate_wire_header(data: &[u8]) -> Result<SchemaId> {
    if data.len() < HEADER_SIZE {
        return Err(SchemaRegError::wire_format(format!(
            "wire format data too short: expected at least {HEADER_SIZE} bytes, got {}",
            data.len()
        )));
    }
    if data[0] != MAGIC_BYTE {
        return Err(SchemaRegError::wire_format(format!(
            "invalid wire format magic byte: expected 0x{MAGIC_BYTE:02X}, got 0x{:02X}",
            data[0]
        )));
    }
    Ok(SchemaId::from(u32::from_be_bytes([
        data[1], data[2], data[3], data[4],
    ])))
}

// ── Confluent wire format (Protobuf) ─────────────────────────────────────

/// The canonical encoding of the message-index array `[0]`, mandated by the
/// Confluent Protobuf serde as a single-byte optimisation.
const PROTOBUF_DEFAULT_INDEX: [u8; 1] = [0x00];

/// Encode a Protobuf payload with the Confluent Protobuf wire format.
///
/// Inserts the 5-byte Confluent header followed by the message-index array
/// before the serialized Protobuf bytes. This framing is required by the
/// Confluent Schema Registry Protobuf serde and every compatible client
/// (Java, Python, Go, .NET).
///
/// `msg_indexes` encodes the path to the message type in the `.proto` file.
/// For a top-level message at position 0 (the most common case), pass `&[0]`;
/// this is emitted as the mandated single `0x00` byte. For a nested message or
/// one at a different file-level position, pass the corresponding sequence of
/// path components — the element count and every path component are then
/// ZigZag-encoded and written as unsigned LEB-128 varints.
///
/// An empty `msg_indexes` slice is treated as `[0]`.
///
/// # Example
///
/// ```rust
/// use schemreg::{encode_protobuf_wire_format, decode_wire_format, decode_protobuf_message_indexes};
///
/// // Top-level message at index 0 — the standard case, encoded as one 0x00 byte.
/// let proto_bytes = b"\x0a\x05hello";
/// let framed = encode_protobuf_wire_format(42, &[0], proto_bytes);
/// assert_eq!(&framed[5..6], &[0x00]);
///
/// let (id, rest) = decode_wire_format(&framed).unwrap();
/// assert_eq!(id, 42);
///
/// let (indexes, payload_offset) = decode_protobuf_message_indexes(rest).unwrap();
/// assert_eq!(indexes, vec![0]);
/// assert_eq!(&rest[payload_offset..], proto_bytes);
///
/// // A non-default path is written as ZigZag(count), ZigZag(seg), ...
/// let nested = encode_protobuf_wire_format(1, &[0, 1], b"x");
/// assert_eq!(&nested[5..8], &[0x04, 0x00, 0x02]);
/// ```
#[must_use]
pub fn encode_protobuf_wire_format(
    schema_id: impl Into<SchemaId>,
    msg_indexes: &[i32],
    payload: &[u8],
) -> Bytes {
    let schema_id = schema_id.into();

    // Confluent's mandated optimisation: `[0]` (and, by extension, an empty
    // path) is written as a single zero byte rather than ZigZag(1), ZigZag(0).
    let use_default_index = msg_indexes.is_empty() || msg_indexes == [0];

    let index_len: usize = if use_default_index {
        PROTOBUF_DEFAULT_INDEX.len()
    } else {
        varint_len(zigzag_encode(
            i32::try_from(msg_indexes.len()).unwrap_or(i32::MAX),
        )) + msg_indexes
            .iter()
            .map(|&i| varint_len(zigzag_encode(i)))
            .sum::<usize>()
    };

    let mut buf = BytesMut::with_capacity(HEADER_SIZE + index_len + payload.len());
    buf.put_u8(MAGIC_BYTE);
    buf.put_u32(schema_id.as_u32());
    if use_default_index {
        buf.put_slice(&PROTOBUF_DEFAULT_INDEX);
    } else {
        // The element count is ZigZag-encoded too — matching Kafka's
        // `ByteUtils.writeVarint`, which the Confluent serde uses here.
        write_varint(
            &mut buf,
            zigzag_encode(i32::try_from(msg_indexes.len()).unwrap_or(i32::MAX)),
        );
        for &idx in msg_indexes {
            write_varint(&mut buf, zigzag_encode(idx));
        }
    }
    buf.put_slice(payload);
    buf.freeze()
}

/// Strip and parse the Protobuf message-index array from the raw bytes
/// immediately after the 5-byte Confluent header.
///
/// Returns `(indexes, bytes_consumed)` where `bytes_consumed` is the offset
/// within `after_header` at which the actual Protobuf payload begins.
///
/// `after_header` must be the slice *after* the 5-byte header (i.e.
/// `data[HEADER_SIZE..]`), not the full framed buffer.
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
/// # Example
///
/// ```rust
/// use schemreg::{encode_protobuf_wire_format, decode_wire_format, decode_protobuf_message_indexes};
///
/// let framed = encode_protobuf_wire_format(1, &[0], b"\x0a\x03foo");
/// let (_, after_header) = decode_wire_format(&framed).unwrap();
/// let (indexes, payload_start) = decode_protobuf_message_indexes(after_header).unwrap();
/// assert_eq!(indexes, vec![0]);
/// assert_eq!(&after_header[payload_start..], b"\x0a\x03foo");
///
/// // Bytes produced by the Confluent Java serde for the default message type.
/// let (indexes, payload_start) = decode_protobuf_message_indexes(b"\x00\x0a\x03foo").unwrap();
/// assert_eq!(indexes, vec![0]);
/// assert_eq!(payload_start, 1);
/// ```
pub fn decode_protobuf_message_indexes(after_header: &[u8]) -> Result<(Vec<i32>, usize)> {
    let (raw_count, consumed) = read_varint(after_header, 0)?;
    let count = zigzag_decode_i32(raw_count)?;

    // Count 0 is the mandated single-byte encoding of the path `[0]`.
    if count == 0 {
        return Ok((vec![0], consumed));
    }
    if count < 0 {
        return Err(SchemaRegError::wire_format(format!(
            "Protobuf message-index count {count} is negative — the frame is not \
             Confluent Protobuf-framed, or was produced by a non-conforming serializer"
        )));
    }
    if count > MAX_MESSAGE_INDEX_COUNT {
        return Err(SchemaRegError::wire_format(format!(
            "Protobuf message-index count {count} exceeds the maximum of {MAX_MESSAGE_INDEX_COUNT}"
        )));
    }

    let mut offset = consumed;
    let mut indexes = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (raw, c) = read_varint(after_header, offset)?;
        offset += c;
        indexes.push(zigzag_decode_i32(raw)?);
    }
    Ok((indexes, offset))
}

/// Outcome of [`detect_wire_format()`] — which framing (if any) a buffer uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DetectedWireFormat {
    /// Confluent wire format (`0x00` magic + schema ID).
    Confluent {
        /// Confluent schema ID.
        schema_id: SchemaId,
        /// Offset where payload bytes start.
        payload_offset: usize,
    },
    /// AWS Glue wire format (`0x03` version + compression + UUID).
    Glue {
        /// Glue schema version UUID.
        version_id: GlueSchemaVersionId,
        /// Compression algorithm indicated in the header byte.
        compression: GlueCompression,
        /// Offset where payload bytes start.
        payload_offset: usize,
    },
    /// Looks like Confluent framing (`0x00`) but header is invalid/truncated.
    InvalidConfluent,
    /// Looks like Glue framing (`0x03`) but header is invalid/truncated.
    InvalidGlue,
    /// Unknown or unrecognised wire format.
    Unknown,
}

/// Detect schema wire format from the message header.
///
/// Returns [`DetectedWireFormat::Unknown`] for empty buffers or unrecognized
/// magic bytes. Returns [`DetectedWireFormat::InvalidConfluent`] for a valid
/// Confluent magic byte (`0x00`) with a truncated header. Returns
/// [`DetectedWireFormat::InvalidGlue`] for a valid Glue version byte (`0x03`)
/// with an invalid compression indicator or truncated UUID.
///
/// # Example
///
/// ```rust
/// use schemreg::{SchemaId, encode_wire_format, detect_wire_format, DetectedWireFormat};
///
/// let framed = encode_wire_format(42u32, b"data");
/// assert_eq!(
///     detect_wire_format(&framed),
///     DetectedWireFormat::Confluent { schema_id: SchemaId::from(42u32), payload_offset: 5 }
/// );
///
/// assert_eq!(detect_wire_format(&[]), DetectedWireFormat::Unknown);
/// ```
pub fn detect_wire_format(data: &[u8]) -> DetectedWireFormat {
    if data.is_empty() {
        return DetectedWireFormat::Unknown;
    }

    match data[0] {
        MAGIC_BYTE => {
            if data.len() < HEADER_SIZE {
                return DetectedWireFormat::InvalidConfluent;
            }
            let schema_id =
                SchemaId::from(u32::from_be_bytes([data[1], data[2], data[3], data[4]]));
            DetectedWireFormat::Confluent {
                schema_id,
                payload_offset: HEADER_SIZE,
            }
        }
        GLUE_HEADER_VERSION_BYTE => {
            if data.len() < GLUE_HEADER_SIZE {
                return DetectedWireFormat::InvalidGlue;
            }
            let compression = data[1];
            if compression != GLUE_COMPRESSION_NONE_BYTE
                && compression != GLUE_COMPRESSION_ZLIB_BYTE
            {
                return DetectedWireFormat::InvalidGlue;
            }

            let compression = if compression == GLUE_COMPRESSION_NONE_BYTE {
                GlueCompression::None
            } else {
                GlueCompression::Zlib
            };
            let mut version_bytes = [0u8; 16];
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

    #[test]
    fn test_wire_format_roundtrip() {
        let payload = b"hello world";
        let encoded = encode_wire_format(42u32, payload);
        let (id, decoded) = decode_wire_format(&encoded).unwrap();
        assert_eq!(id, SchemaId::from(42u32));
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_wire_format_empty_payload() {
        let encoded = encode_wire_format(1u32, b"");
        assert_eq!(encoded.len(), HEADER_SIZE);
        let (id, payload) = decode_wire_format(&encoded).unwrap();
        assert_eq!(id, SchemaId::from(1u32));
        assert!(payload.is_empty());
    }

    #[test]
    fn test_wire_format_max_schema_id() {
        let encoded = encode_wire_format(u32::MAX, b"data");
        let (id, _) = decode_wire_format(&encoded).unwrap();
        assert_eq!(id, SchemaId::from(u32::MAX));
    }

    #[test]
    fn test_wire_format_header_bytes() {
        // Schema ID 256 = 0x00000100
        let encoded = encode_wire_format(256u32, b"x");
        assert_eq!(&encoded[..5], &[0x00, 0x00, 0x00, 0x01, 0x00]);
        assert_eq!(&encoded[5..], b"x");
    }

    #[test]
    fn test_wire_format_invalid_magic_byte() {
        let data = [0x01, 0, 0, 0, 1, 0x42];
        let result = decode_wire_format(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("magic byte"));
    }

    #[test]
    fn test_wire_format_too_short() {
        let result = decode_wire_format(&[0x00, 0, 0]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_wire_format_empty_data() {
        let result = decode_wire_format(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_detect_wire_format_confluent() {
        let encoded = encode_wire_format(42u32, b"data");
        let detected = detect_wire_format(&encoded);
        assert_eq!(
            detected,
            DetectedWireFormat::Confluent {
                schema_id: SchemaId::from(42u32),
                payload_offset: 5,
            }
        );
    }

    #[test]
    fn test_detect_wire_format_unknown() {
        assert_eq!(detect_wire_format(&[]), DetectedWireFormat::Unknown);
        assert_eq!(
            detect_wire_format(&[0x99, 0x00, 0x00]),
            DetectedWireFormat::Unknown
        );
    }

    #[test]
    fn test_detect_wire_format_confluent_schema_id_zero() {
        assert_eq!(
            detect_wire_format(&[MAGIC_BYTE, 0x00, 0x00, 0x00, 0x00, 0x41]),
            DetectedWireFormat::Confluent {
                schema_id: SchemaId::from(0u32),
                payload_offset: HEADER_SIZE,
            }
        );
    }

    // ── Protobuf message-index conformance ────────────────────────────────

    #[test]
    fn test_protobuf_default_index_is_single_zero_byte() {
        // Confluent mandates the single-0x00 encoding for the path [0].
        let framed = encode_protobuf_wire_format(7u32, &[0], b"proto");
        assert_eq!(
            &framed[..],
            &[0x00, 0, 0, 0, 7, 0x00, b'p', b'r', b'o', b't', b'o'][..]
        );
    }

    #[test]
    fn test_protobuf_empty_path_encodes_as_default_index() {
        let a = encode_protobuf_wire_format(1u32, &[], b"x");
        let b = encode_protobuf_wire_format(1u32, &[0], b"x");
        assert_eq!(a, b);
    }

    #[test]
    fn test_protobuf_count_is_zigzag_encoded() {
        // path [1] → ZigZag(1)=2 for the count, ZigZag(1)=2 for the segment.
        let framed = encode_protobuf_wire_format(1u32, &[1], b"x");
        assert_eq!(&framed[HEADER_SIZE..], &[0x02, 0x02, b'x'][..]);

        // path [2] → count ZigZag(1)=2, segment ZigZag(2)=4.
        let framed = encode_protobuf_wire_format(1u32, &[2], b"x");
        assert_eq!(&framed[HEADER_SIZE..], &[0x02, 0x04, b'x'][..]);

        // path [0, 1] → count ZigZag(2)=4, then ZigZag(0)=0, ZigZag(1)=2.
        let framed = encode_protobuf_wire_format(1u32, &[0, 1], b"x");
        assert_eq!(&framed[HEADER_SIZE..], &[0x04, 0x00, 0x02, b'x'][..]);
    }

    #[test]
    fn test_protobuf_decode_default_index_yields_zero_path() {
        // Byte stream as produced by the Confluent Java/Python serializers.
        let (indexes, offset) = decode_protobuf_message_indexes(b"\x00payload").unwrap();
        assert_eq!(indexes, vec![0]);
        assert_eq!(offset, 1);
    }

    #[test]
    fn test_protobuf_decode_rejects_negative_count() {
        // 0x01 decodes (ZigZag) to -1 — this is what a non-conforming encoder
        // that writes a plain unsigned count would emit for count = 1.
        let err = decode_protobuf_message_indexes(b"\x01\x00rest").unwrap_err();
        assert!(err.to_string().contains("negative"), "{err}");
    }

    #[test]
    fn test_protobuf_decode_rejects_oversized_count() {
        // ZigZag(1000) = 2000 → varint 0xD0 0x0F
        let err = decode_protobuf_message_indexes(&[0xD0, 0x0F]).unwrap_err();
        assert!(err.to_string().contains("exceeds the maximum"), "{err}");
    }

    #[test]
    fn test_protobuf_roundtrip_all_paths() {
        for path in [
            vec![0],
            vec![1],
            vec![2],
            vec![0, 1],
            vec![3, 0, 7],
            vec![-1],
        ] {
            let framed = encode_protobuf_wire_format(9u32, &path, b"body");
            let (_, after) = decode_wire_format(&framed).unwrap();
            let (indexes, offset) = decode_protobuf_message_indexes(after).unwrap();
            assert_eq!(indexes, path, "path {path:?} must round-trip");
            assert_eq!(&after[offset..], b"body");
        }
    }

    #[test]
    fn test_detect_wire_format_invalid_known_headers() {
        assert_eq!(
            detect_wire_format(&[MAGIC_BYTE, 0x01, 0x02]),
            DetectedWireFormat::InvalidConfluent
        );
        use crate::glue::{GLUE_COMPRESSION_NONE_BYTE, GLUE_HEADER_VERSION_BYTE};
        assert_eq!(
            detect_wire_format(&[GLUE_HEADER_VERSION_BYTE, GLUE_COMPRESSION_NONE_BYTE]),
            DetectedWireFormat::InvalidGlue
        );
    }
}
