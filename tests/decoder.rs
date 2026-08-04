//! Integration tests for the `WireFormatDecoder`.
//!
//! Uses in-memory mock registries (no network I/O).
//! Tests cover:
//! - Confluent-framed payload → schema fetched from mock registry
//! - Glue-framed payload → schema fetched from mock Glue registry
//! - Passthrough for unknown / invalid wire formats
//! - Error when the relevant registry backend is not configured
//! - SchemaFormat derivation from both backends

use std::sync::Arc;

use bytes::Bytes;

use schemreg::error::Result;
use schemreg::{
    Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType, SchemaVersion,
    decoder::{SchemaFormat, SchemaMetadata, WireFormatDecoder},
    encode_wire_format,
};

#[cfg(feature = "glue")]
use schemreg::glue::GlueSchemaRegistryClient;
#[cfg(feature = "glue")]
use schemreg::{GlueCompression, GlueDataFormat, GlueSchema, GlueSchemaVersionId};

// ── Mock Confluent registry ───────────────────────────────────────────────

struct MockConfluentRegistry {
    schema: Schema,
}

impl SchemaRegistryClient for MockConfluentRegistry {
    async fn get_schema_by_id(&self, _id: SchemaId) -> Result<Arc<Schema>> {
        Ok(Arc::new(self.schema.clone()))
    }

    async fn get_latest_schema(&self, _subject: &str) -> Result<Arc<Schema>> {
        unimplemented!()
    }

    async fn get_schema_by_version(
        &self,
        _subject: &str,
        _version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        unimplemented!()
    }

    async fn register_schema(
        &self,
        _subject: &str,
        _schema: &str,
        _schema_type: SchemaType,
        _references: &[SchemaReference],
    ) -> Result<SchemaId> {
        unimplemented!()
    }
}

// ── Mock Glue registry ────────────────────────────────────────────────────

#[cfg(feature = "glue")]
struct MockGlueRegistry {
    schema: GlueSchema,
}

#[cfg(feature = "glue")]
impl GlueSchemaRegistryClient for MockGlueRegistry {
    async fn get_schema_by_version_id(&self, _id: GlueSchemaVersionId) -> Result<Arc<GlueSchema>> {
        Ok(Arc::new(self.schema.clone()))
    }

    async fn register_schema(
        &self,
        _schema_name: &str,
        _schema: &str,
        _data_format: GlueDataFormat,
    ) -> Result<GlueSchemaVersionId> {
        unimplemented!()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn avro_schema(id: u32) -> Schema {
    Schema::new(SchemaId::from(id), SchemaType::Avro, r#"{"type":"string"}"#)
}

#[cfg(feature = "glue")]
fn glue_id() -> GlueSchemaVersionId {
    "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
}

#[cfg(feature = "glue")]
fn glue_schema() -> GlueSchema {
    GlueSchema::new(glue_id(), GlueDataFormat::Avro, r#"{"type":"string"}"#)
}

fn confluent_framed(schema_id: u32, payload: &[u8]) -> Bytes {
    encode_wire_format(schema_id, payload)
}

#[cfg(feature = "glue")]
fn glue_framed(payload: &[u8]) -> Bytes {
    schemreg::encode_glue_wire_format(glue_id(), payload, GlueCompression::None).unwrap()
}

// ── Confluent path ────────────────────────────────────────────────────────

#[tokio::test]
async fn decode_confluent_framed_returns_schema() {
    let registry = Arc::new(MockConfluentRegistry {
        schema: avro_schema(42),
    });
    let decoder = WireFormatDecoder::confluent(registry);

    let data = confluent_framed(42, b"avro bytes");
    let decoded = decoder.decode(data).await.unwrap();

    assert_eq!(&decoded.payload[..], b"avro bytes");
    assert_eq!(decoded.schema_format, SchemaFormat::Avro);

    let Some(SchemaMetadata::Confluent(schema)) = decoded.schema_metadata else {
        panic!("expected Confluent schema metadata");
    };
    assert_eq!(schema.id, 42u32);
}

#[tokio::test]
async fn decode_confluent_empty_payload() {
    let registry = Arc::new(MockConfluentRegistry {
        schema: avro_schema(1),
    });
    let decoder = WireFormatDecoder::confluent(registry);
    let data = confluent_framed(1, b"");
    let decoded = decoder.decode(data).await.unwrap();
    assert!(decoded.payload.is_empty());
    assert_eq!(decoded.schema_format, SchemaFormat::Avro);
}

#[tokio::test]
async fn decode_confluent_schema_format_protobuf() {
    let schema = Schema::new(
        SchemaId::from(7u32),
        SchemaType::Protobuf,
        "syntax = \"proto3\";",
    );
    let registry = Arc::new(MockConfluentRegistry { schema });
    let decoder = WireFormatDecoder::confluent(registry);
    // Use proper Protobuf framing with message-index [0].
    let framed = schemreg::encode_protobuf_wire_format(7u32, &[0], b"proto bytes");
    let decoded = decoder.decode(framed).await.unwrap();
    assert_eq!(decoded.schema_format, SchemaFormat::Protobuf);
    assert_eq!(&decoded.payload[..], b"proto bytes");
    assert_eq!(decoded.protobuf_message_indexes, Some(vec![0]));
}

#[tokio::test]
async fn decode_confluent_schema_format_json() {
    let schema = Schema::new(SchemaId::from(8u32), SchemaType::Json, "{}");
    let registry = Arc::new(MockConfluentRegistry { schema });
    let decoder = WireFormatDecoder::confluent(registry);
    let data = confluent_framed(8, b"json bytes");
    let decoded = decoder.decode(data).await.unwrap();
    assert_eq!(decoded.schema_format, SchemaFormat::Json);
}

#[tokio::test]
async fn decode_confluent_missing_backend_is_error() {
    // Decoder has no Confluent registry configured.
    let decoder = WireFormatDecoder::new();
    let data = confluent_framed(1, b"payload");
    let err = decoder.decode(data).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Confluent") || msg.contains("backend") || msg.contains("registry"),
        "{msg}"
    );
}

// ── Glue path ─────────────────────────────────────────────────────────────

#[cfg(feature = "glue")]
#[tokio::test]
async fn decode_glue_framed_returns_schema() {
    let registry = Arc::new(MockGlueRegistry {
        schema: glue_schema(),
    });
    let decoder = WireFormatDecoder::glue(registry);
    let data = glue_framed(b"glue bytes");
    let decoded = decoder.decode(Bytes::from(data.to_vec())).await.unwrap();

    assert_eq!(&decoded.payload[..], b"glue bytes");
    assert_eq!(decoded.schema_format, SchemaFormat::Avro);

    let Some(SchemaMetadata::Glue(schema)) = decoded.schema_metadata else {
        panic!("expected Glue schema metadata");
    };
    assert_eq!(schema.schema_version_id, glue_id());
}

#[cfg(feature = "glue")]
#[tokio::test]
async fn decode_glue_missing_backend_is_error() {
    let decoder = WireFormatDecoder::new(); // no Glue registry
    let data = glue_framed(b"payload");
    let err = decoder
        .decode(Bytes::from(data.to_vec()))
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Glue") || msg.contains("backend") || msg.contains("registry"),
        "{msg}"
    );
}

// ── Passthrough paths ─────────────────────────────────────────────────────

#[tokio::test]
async fn decode_unknown_magic_passthrough() {
    let decoder = WireFormatDecoder::new();
    let raw = Bytes::from_static(b"\x99plain text payload");
    let decoded = decoder.decode(raw.clone()).await.unwrap();

    assert_eq!(decoded.payload, raw);
    assert_eq!(decoded.schema_format, SchemaFormat::Unknown);
    assert!(decoded.schema_metadata.is_none());
}

#[tokio::test]
async fn decode_empty_payload_passthrough() {
    let decoder = WireFormatDecoder::new();
    let decoded = decoder.decode(Bytes::new()).await.unwrap();
    assert!(decoded.payload.is_empty());
    assert_eq!(decoded.schema_format, SchemaFormat::Unknown);
}

#[tokio::test]
async fn decode_invalid_confluent_header_passthrough() {
    let decoder = WireFormatDecoder::new();
    // Magic byte is 0x00 but only 3 bytes total → InvalidConfluent → passthrough.
    let raw = Bytes::from_static(&[0x00, 0x00, 0x01]);
    let decoded = decoder.decode(raw.clone()).await.unwrap();
    assert_eq!(decoded.payload, raw);
    assert_eq!(decoded.schema_format, SchemaFormat::Unknown);
}

#[tokio::test]
async fn decode_invalid_glue_header_passthrough() {
    let decoder = WireFormatDecoder::new();
    // Glue version byte but truncated → InvalidGlue → passthrough.
    let raw = Bytes::from_static(&[0x03, 0x00]);
    let decoded = decoder.decode(raw.clone()).await.unwrap();
    assert_eq!(decoded.payload, raw);
    assert_eq!(decoded.schema_format, SchemaFormat::Unknown);
}

// ── Builder ergonomics ────────────────────────────────────────────────────

#[cfg(all(feature = "glue", feature = "confluent"))]
#[tokio::test]
async fn decoder_with_confluent_and_glue_confluent_path() {
    let confluent = Arc::new(MockConfluentRegistry {
        schema: avro_schema(99),
    });
    let glue = Arc::new(MockGlueRegistry {
        schema: glue_schema(),
    });
    let decoder = WireFormatDecoder::new()
        .with_confluent(confluent)
        .with_glue(glue);

    let data = confluent_framed(99, b"c-bytes");
    let decoded = decoder.decode(data).await.unwrap();
    assert!(matches!(
        decoded.schema_metadata,
        Some(SchemaMetadata::Confluent(_))
    ));
}

#[cfg(all(feature = "glue", feature = "confluent"))]
#[tokio::test]
async fn decoder_with_confluent_and_glue_glue_path() {
    let confluent = Arc::new(MockConfluentRegistry {
        schema: avro_schema(99),
    });
    let glue = Arc::new(MockGlueRegistry {
        schema: glue_schema(),
    });
    let decoder = WireFormatDecoder::new()
        .with_confluent(confluent)
        .with_glue(glue);

    let data = glue_framed(b"g-bytes");
    let decoded = decoder.decode(Bytes::from(data.to_vec())).await.unwrap();
    assert!(matches!(
        decoded.schema_metadata,
        Some(SchemaMetadata::Glue(_))
    ));
}

// ── SchemaDecoder trait object ────────────────────────────────────────────

/// `WireFormatDecoder` is the crate's object-safe framing stripper. Without an
/// implementor, `SchemaDecoder` would be a trait nobody could use.
#[tokio::test]
async fn wire_format_decoder_is_usable_as_a_schema_decoder_trait_object() {
    use schemreg::{EncodeTarget, SchemaDecoder};

    let decoder: Arc<dyn SchemaDecoder> =
        Arc::new(WireFormatDecoder::confluent(MockConfluentRegistry {
            schema: avro_schema(1),
        }));

    let framed = confluent_framed(1, b"inner-payload");
    let payload = decoder
        .decode(framed, "orders", EncodeTarget::Value)
        .await
        .expect("decode through the trait object");

    assert_eq!(
        &payload[..],
        b"inner-payload",
        "the trait object must return the header-stripped payload"
    );
}

#[test]
fn schema_decoder_trait_object_is_send_sync() {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<dyn schemreg::SchemaDecoder>();
    assert_send_sync::<dyn schemreg::SchemaEncoder>();
}
