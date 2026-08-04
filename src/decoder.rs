//! Wire-format-agnostic schema decoder and helpers.
//!
//! [`WireFormatDecoder`] can handle both Confluent and Glue wire formats
//! in a single `.decode()` call. Construct it with whichever backends are
//! needed and call `.decode(payload)` to strip the header and return the
//! raw payload bytes along with schema metadata.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Result, SchemaRegError};
use crate::traits::{DynSchemaRegistryClient, SchemaDecoder};
use crate::types::{EncodeTarget, Schema, SchemaType};
use crate::wire::{decode_protobuf_message_indexes, detect_wire_format};

#[cfg(feature = "glue")]
use crate::glue::{DynGlueSchemaRegistryClient, GlueDataFormat, GlueSchema};

// ── Schema format ─────────────────────────────────────────────────────────

/// The serialization format detected for a decoded message.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaFormat {
    /// Apache Avro.
    Avro,
    /// Protocol Buffers.
    Protobuf,
    /// JSON Schema.
    Json,
    /// Format not determined (e.g. no schema registry backing available).
    Unknown,
}

impl From<SchemaType> for SchemaFormat {
    fn from(t: SchemaType) -> Self {
        match t {
            SchemaType::Avro => Self::Avro,
            SchemaType::Protobuf => Self::Protobuf,
            SchemaType::Json => Self::Json,
        }
    }
}

#[cfg(feature = "glue")]
impl From<GlueDataFormat> for SchemaFormat {
    fn from(f: GlueDataFormat) -> Self {
        match f {
            GlueDataFormat::Avro => Self::Avro,
            GlueDataFormat::Json => Self::Json,
            GlueDataFormat::Protobuf => Self::Protobuf,
        }
    }
}

// ── Schema metadata ───────────────────────────────────────────────────────

/// Schema metadata retrieved alongside a decoded message.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SchemaMetadata {
    /// Schema fetched from a Confluent-compatible registry.
    Confluent(Arc<Schema>),
    /// Schema fetched from an AWS Glue Schema Registry.
    #[cfg(feature = "glue")]
    Glue(Arc<GlueSchema>),
}

impl SchemaMetadata {
    /// Returns the detected serialization format.
    pub fn schema_format(&self) -> SchemaFormat {
        match self {
            Self::Confluent(s) => SchemaFormat::from(s.schema_type),
            #[cfg(feature = "glue")]
            Self::Glue(g) => SchemaFormat::from(g.data_format),
        }
    }
}

// ── Decoded message ───────────────────────────────────────────────────────

/// A wire-format-stripped message payload.
///
/// Returned by [`WireFormatDecoder::decode`].
#[derive(Debug, Clone)]
pub struct DecodedMessage {
    /// Detected serialization format.
    pub schema_format: SchemaFormat,
    /// Raw payload bytes (wire header stripped; message-index stripped for Protobuf).
    pub payload: Bytes,
    /// Schema metadata, if a registry was configured and the header was valid.
    pub schema_metadata: Option<SchemaMetadata>,
    /// Protobuf message-index path, present only for Confluent-framed Protobuf
    /// messages. Identifies which message type in the `.proto` file was used.
    /// `[0]` means the first top-level message (the common case).
    pub protobuf_message_indexes: Option<Vec<i32>>,
}

// ── WireFormatDecoder ─────────────────────────────────────────────────────

/// Multi-format wire decoder.
///
/// Accepts an optional Confluent registry client and an optional Glue registry
/// client. On each `decode()` call it auto-detects the wire format, strips the
/// header, optionally fetches schema metadata, and returns a [`DecodedMessage`].
///
/// Messages with an unrecognised header (neither Confluent nor Glue) are
/// returned as-is with `schema_format = Unknown` and no metadata.
///
/// `WireFormatDecoder` is `Clone` and owns its registry references via [`Arc`],
/// so it can be stored in structs, shared across tasks, and used without
/// lifetime parameters.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use schemreg::{CachedSchemaRegistry, WireFormatDecoder};
/// #[cfg(feature = "confluent")]
/// use schemreg::ConfluentSchemaRegistry;
///
/// let registry = Arc::new(CachedSchemaRegistry::new(
///     ConfluentSchemaRegistry::new("http://localhost:8081").unwrap(),
/// ));
/// let decoder = WireFormatDecoder::confluent(registry);
/// let msg = decoder.decode(record_bytes).await.unwrap();
/// println!("format: {:?}", msg.schema_format);
/// ```
#[derive(Clone)]
pub struct WireFormatDecoder {
    confluent: Option<Arc<dyn DynSchemaRegistryClient>>,
    #[cfg(feature = "glue")]
    glue: Option<Arc<dyn DynGlueSchemaRegistryClient>>,
}

impl WireFormatDecoder {
    /// Create an empty decoder (no registry backends configured).
    ///
    /// Decoded messages will still have the wire header stripped but
    /// `schema_metadata` will be `None` and format will be inferred from the
    /// wire header type or `Unknown`.
    pub fn new() -> Self {
        Self {
            confluent: None,
            #[cfg(feature = "glue")]
            glue: None,
        }
    }

    /// Convenience constructor: create a decoder backed by a single Confluent registry.
    ///
    /// Accepts any `SchemaRegistryClient + Send + Sync + 'static`, including
    /// bare clients and `Arc`-wrapped ones.
    ///
    /// Equivalent to `WireFormatDecoder::new().with_confluent(registry)`.
    pub fn confluent(registry: impl crate::traits::SchemaRegistryClient + 'static) -> Self {
        Self::new().with_confluent(registry)
    }

    /// Convenience constructor: create a decoder backed by a single Glue registry.
    ///
    /// Requires the `glue` feature.
    #[cfg(feature = "glue")]
    pub fn glue(registry: impl crate::glue::GlueSchemaRegistryClient + 'static) -> Self {
        Self::new().with_glue(registry)
    }

    /// Attach (or replace) the Confluent registry backend.
    pub fn with_confluent(
        mut self,
        registry: impl crate::traits::SchemaRegistryClient + 'static,
    ) -> Self {
        self.confluent = Some(Arc::new(registry));
        self
    }

    /// Attach (or replace) the Glue registry backend.
    ///
    /// Requires the `glue` feature.
    #[cfg(feature = "glue")]
    pub fn with_glue(
        mut self,
        registry: impl crate::glue::GlueSchemaRegistryClient + 'static,
    ) -> Self {
        self.glue = Some(Arc::new(registry));
        self
    }

    /// Decode a wire-formatted payload.
    ///
    /// Auto-detects the wire format and strips the header. If a registry client
    /// is configured, fetches schema metadata.
    ///
    /// Messages that do not match any known wire format are returned with their
    /// original bytes, `schema_format = Unknown`, and `schema_metadata = None`.
    pub async fn decode(&self, data: Bytes) -> Result<DecodedMessage> {
        use crate::wire::DetectedWireFormat;

        match detect_wire_format(&data) {
            DetectedWireFormat::Confluent {
                schema_id,
                payload_offset,
            } => {
                let Some(client) = self.confluent.as_deref() else {
                    return Err(SchemaRegError::invalid_state(
                        "Confluent-framed message received but no Confluent registry backend is configured",
                    ));
                };

                let after_header = data.slice(payload_offset..);
                let schema = client.get_schema_by_id(schema_id).await?;
                let schema_type = schema.schema_type;
                let schema_format = SchemaFormat::from(schema_type);

                // For Protobuf, strip the message-index array that sits between
                // the 5-byte header and the serialized proto bytes.
                let (payload, protobuf_message_indexes) = if schema_type == SchemaType::Protobuf {
                    let (indexes, consumed) = decode_protobuf_message_indexes(&after_header)?;
                    (after_header.slice(consumed..), Some(indexes))
                } else {
                    (after_header, None)
                };

                Ok(DecodedMessage {
                    schema_format,
                    payload,
                    schema_metadata: Some(SchemaMetadata::Confluent(schema)),
                    protobuf_message_indexes,
                })
            }

            #[cfg(feature = "glue")]
            DetectedWireFormat::Glue {
                version_id,
                compression,
                payload_offset,
            } => {
                let Some(client) = self.glue.as_deref() else {
                    return Err(SchemaRegError::invalid_state(
                        "Glue-framed message received but no Glue registry backend is configured",
                    ));
                };

                // Decompress according to the header byte (F-01 fix).
                let raw = &data[payload_offset..];
                let payload = match compression {
                    crate::glue::GlueCompression::None => data.slice(payload_offset..),
                    crate::glue::GlueCompression::Zlib => {
                        let decompressed = crate::glue::decompress_zlib(raw)?;
                        bytes::Bytes::from(decompressed)
                    }
                };

                let schema = client.get_schema_by_version_id(version_id).await?;
                let schema_format = SchemaFormat::from(schema.data_format);

                Ok(DecodedMessage {
                    schema_format,
                    payload,
                    schema_metadata: Some(SchemaMetadata::Glue(schema)),
                    protobuf_message_indexes: None,
                })
            }

            #[cfg(not(feature = "glue"))]
            DetectedWireFormat::Glue { .. } => Err(SchemaRegError::not_supported(
                "Glue-framed message received but schemreg was compiled without the `glue` feature",
            )),

            // Malformed headers are treated as unknown / passthrough so that
            // non-schema-registry payloads that happen to start with the magic byte
            // are not silently dropped.
            DetectedWireFormat::InvalidConfluent | DetectedWireFormat::InvalidGlue => {
                Ok(DecodedMessage {
                    schema_format: SchemaFormat::Unknown,
                    payload: data,
                    schema_metadata: None,
                    protobuf_message_indexes: None,
                })
            }

            DetectedWireFormat::Unknown => Ok(DecodedMessage {
                schema_format: SchemaFormat::Unknown,
                payload: data,
                schema_metadata: None,
                protobuf_message_indexes: None,
            }),
        }
    }
}

impl Default for WireFormatDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for WireFormatDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("WireFormatDecoder");
        s.field("confluent", &self.confluent.is_some());
        #[cfg(feature = "glue")]
        s.field("glue", &self.glue.is_some());
        s.finish()
    }
}

/// `WireFormatDecoder` is the object-safe framing stripper: it satisfies
/// [`SchemaDecoder`] by returning the header-stripped payload bytes.
///
/// `topic` and `target` are accepted for interface compatibility but ignored —
/// both the Confluent and Glue wire formats are self-describing, so the schema
/// is located from the header rather than from a derived subject name.
impl SchemaDecoder for WireFormatDecoder {
    fn decode(
        &self,
        payload: Bytes,
        _topic: &str,
        _target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>> {
        Box::pin(async move {
            WireFormatDecoder::decode(self, payload)
                .await
                .map(|m| m.payload)
        })
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::types::{SchemaId, SchemaReference, SchemaType};
    use crate::wire::encode_wire_format;

    struct TestRegistry;

    impl crate::traits::SchemaRegistryClient for TestRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            Ok(Arc::new(Schema::new(
                id,
                SchemaType::Avro,
                r#"{"type":"string"}"#,
            )))
        }

        async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
            Ok(Arc::new(
                Schema::new(
                    SchemaId::from(1u32),
                    SchemaType::Avro,
                    r#"{"type":"string"}"#,
                )
                .with_subject(subject, 1i32),
            ))
        }

        async fn get_schema_by_version(
            &self,
            subject: &str,
            version: crate::types::SchemaVersion,
        ) -> Result<Arc<Schema>> {
            Ok(Arc::new(
                Schema::new(
                    SchemaId::from(1u32),
                    SchemaType::Avro,
                    r#"{"type":"string"}"#,
                )
                .with_subject(subject, version),
            ))
        }

        async fn register_schema(
            &self,
            _subject: &str,
            _schema: &str,
            _schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> Result<SchemaId> {
            Ok(SchemaId::from(1u32))
        }
    }

    #[tokio::test]
    async fn test_decode_confluent_without_registry() {
        let payload = b"hello world";
        let encoded = encode_wire_format(42u32, payload);

        let decoder = WireFormatDecoder::new();
        // A Confluent-framed message without a registry backend is an error.
        let err = decoder.decode(encoded).await.unwrap_err();
        assert!(
            err.to_string().contains("Confluent") || err.to_string().contains("backend"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_decode_confluent_with_registry() {
        let payload = b"test payload";
        let encoded = encode_wire_format(7u32, payload);

        let decoder = WireFormatDecoder::confluent(TestRegistry);
        let msg = decoder.decode(encoded).await.unwrap();
        assert_eq!(msg.payload, &b"test payload"[..]);
        assert!(matches!(msg.schema_format, SchemaFormat::Avro));
        assert!(matches!(
            msg.schema_metadata,
            Some(SchemaMetadata::Confluent(_))
        ));
        assert!(msg.protobuf_message_indexes.is_none());
    }

    #[tokio::test]
    async fn test_decode_unknown_bytes() {
        let raw = Bytes::from_static(b"raw data no header");
        let decoder = WireFormatDecoder::new();
        let msg = decoder.decode(raw.clone()).await.unwrap();
        assert_eq!(msg.payload, raw);
        assert!(matches!(msg.schema_format, SchemaFormat::Unknown));
        assert!(msg.schema_metadata.is_none());
    }

    #[tokio::test]
    async fn test_schema_format_from_schema_type() {
        assert_eq!(SchemaFormat::from(SchemaType::Avro), SchemaFormat::Avro);
        assert_eq!(
            SchemaFormat::from(SchemaType::Protobuf),
            SchemaFormat::Protobuf
        );
        assert_eq!(SchemaFormat::from(SchemaType::Json), SchemaFormat::Json);
    }

    #[test]
    fn test_default() {
        let _decoder: WireFormatDecoder = WireFormatDecoder::default();
    }

    #[test]
    fn test_clone() {
        let decoder = WireFormatDecoder::confluent(TestRegistry);
        let _cloned = decoder.clone();
    }

    // ── Protobuf message-index tests ─────────────────────────────────────

    struct ProtoTestRegistry;

    impl crate::traits::SchemaRegistryClient for ProtoTestRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
            Ok(Arc::new(Schema::new(
                id,
                SchemaType::Protobuf,
                "syntax=\"proto3\"; message Foo {}",
            )))
        }
        async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
            Ok(Arc::new(
                Schema::new(SchemaId::from(1u32), SchemaType::Protobuf, "")
                    .with_subject(subject, 1i32),
            ))
        }
        async fn get_schema_by_version(
            &self,
            subject: &str,
            v: crate::types::SchemaVersion,
        ) -> Result<Arc<Schema>> {
            Ok(Arc::new(
                Schema::new(SchemaId::from(1u32), SchemaType::Protobuf, "")
                    .with_subject(subject, v),
            ))
        }
        async fn register_schema(
            &self,
            _: &str,
            _: &str,
            _: SchemaType,
            _: &[SchemaReference],
        ) -> Result<SchemaId> {
            Ok(SchemaId::from(1u32))
        }
    }

    #[tokio::test]
    async fn test_decode_protobuf_strips_message_index() {
        use crate::wire::encode_protobuf_wire_format;

        let proto_payload = b"\x0a\x05hello";
        let framed = encode_protobuf_wire_format(99u32, &[0], proto_payload);

        let decoder = WireFormatDecoder::confluent(ProtoTestRegistry);
        let msg = decoder.decode(framed).await.unwrap();

        assert_eq!(
            msg.payload,
            &proto_payload[..],
            "message-index must be stripped"
        );
        assert_eq!(msg.protobuf_message_indexes, Some(vec![0]));
        assert!(matches!(msg.schema_format, SchemaFormat::Protobuf));
    }

    #[tokio::test]
    async fn test_decoded_message_is_clone() {
        let payload = b"test payload";
        let encoded = encode_wire_format(7u32, payload);
        let decoder = WireFormatDecoder::confluent(TestRegistry);
        let msg = decoder.decode(encoded).await.unwrap();
        let cloned = msg.clone();
        assert_eq!(cloned.payload, msg.payload);
        assert_eq!(cloned.schema_format, msg.schema_format);
    }
}
