//! Example: AWS Glue Schema Registry wire format round-trip.
//!
//! Demonstrates:
//! 1. Encoding a payload with the 18-byte AWS Glue wire format header
//! 2. Detecting the wire format automatically with [`detect_wire_format`]
//! 3. Decoding via [`WireFormatDecoder`] with an in-memory mock Glue registry
//! 4. Compression with ZLIB (requires `glue` feature)
//! 5. [`GlueSchemaVersionId`] UUID parse / display
//!
//! # Running
//!
//! ```text
//! cargo run --example glue_roundtrip
//! # With ZLIB compression:
//! cargo run --example glue_roundtrip --features glue
//! ```

use std::sync::Arc;

use bytes::Bytes;
use schemreg::{
    GlueCompression, GlueDataFormat, GlueSchema, GlueSchemaVersionId, decode_glue_wire_format,
    decode_glue_wire_format_bytes,
    decoder::{SchemaFormat, SchemaMetadata, WireFormatDecoder},
    detect_wire_format, encode_glue_wire_format,
    error::Result,
    glue::GlueSchemaRegistryClient,
};

// ── In-memory Glue registry stub ─────────────────────────────────────────

struct InMemoryGlueRegistry {
    schema: GlueSchema,
}

impl GlueSchemaRegistryClient for InMemoryGlueRegistry {
    async fn get_schema_by_version_id(&self, _id: GlueSchemaVersionId) -> Result<Arc<GlueSchema>> {
        Ok(Arc::new(self.schema.clone()))
    }

    async fn register_schema(
        &self,
        _schema_name: &str,
        _schema: &str,
        _data_format: GlueDataFormat,
    ) -> Result<GlueSchemaVersionId> {
        Ok(self.schema.schema_version_id)
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    // ── 1. Schema version ID ──────────────────────────────────────────────

    let version_id: GlueSchemaVersionId = "550e8400-e29b-41d4-a716-446655440000".parse()?;
    println!("\n=== AWS Glue wire format round-trip ===\n");
    println!("Schema version ID: {version_id}");
    assert_eq!(
        version_id.to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );
    println!("✓ UUID parse → display round-trip\n");

    // ── 2. Encode without compression ────────────────────────────────────

    let payload = b"serialized avro record bytes";
    let framed_none = encode_glue_wire_format(version_id, payload, GlueCompression::None)?;

    println!("Header structure (no compression):");
    println!("  byte[0]  = 0x{:02X}  (Glue version byte)", framed_none[0]);
    println!("  byte[1]  = 0x{:02X}  (compression: none)", framed_none[1]);
    println!("  byte[2..18] = UUID bytes");
    println!("  byte[18..] = payload");
    assert_eq!(framed_none.len(), 18 + payload.len());

    // ── 3. Decode (borrowed slice) ────────────────────────────────────────

    let (decoded_id, decoded_payload) = decode_glue_wire_format(&framed_none)?;
    assert_eq!(decoded_id, version_id);
    assert_eq!(decoded_payload, payload);
    println!("\n✓ decode_glue_wire_format: payload matches");

    // ── 4. Decode (zero-copy Bytes) ───────────────────────────────────────

    let framed_bytes = Bytes::from(framed_none.to_vec());
    let (id2, payload2) = decode_glue_wire_format_bytes(&framed_bytes)?;
    assert_eq!(id2, version_id);
    assert_eq!(&payload2[..], payload);
    println!("✓ decode_glue_wire_format_bytes: zero-copy slice correct");

    // ── 5. detect_wire_format dispatches correctly ────────────────────────

    let detected = detect_wire_format(&framed_none);
    println!("\nDetected: {detected:?}");
    match detected {
        schemreg::DetectedWireFormat::Glue {
            version_id: vid,
            compression: _,
            payload_offset,
        } => {
            assert_eq!(vid, version_id);
            assert_eq!(payload_offset, 18);
            println!("✓ detect_wire_format → Glue {{ version_id, payload_offset: 18 }}");
        }
        other => panic!("unexpected wire format: {other:?}"),
    }

    // ── 6. WireFormatDecoder with mock Glue registry ──────────────────────

    let registry = InMemoryGlueRegistry {
        schema: GlueSchema::new(version_id, GlueDataFormat::Avro, r#"{"type":"string"}"#)
            .with_metadata("arn:aws:glue:us-east-1:123456789012:schema/reg/MySchema", 1),
    };

    let decoder = WireFormatDecoder::glue(registry);
    let decoded_msg = decoder.decode(Bytes::from(framed_none.to_vec())).await?;

    assert_eq!(decoded_msg.schema_format, SchemaFormat::Avro);
    assert_eq!(&decoded_msg.payload[..], payload);
    let Some(SchemaMetadata::Glue(meta)) = decoded_msg.schema_metadata else {
        panic!("expected Glue metadata");
    };
    assert_eq!(meta.schema_version_id, version_id);
    assert_eq!(meta.data_format, GlueDataFormat::Avro);
    println!("\n✓ WireFormatDecoder: decoded with Glue registry");
    println!(
        "   Schema ARN: {}",
        meta.schema_arn.as_deref().unwrap_or("<none>")
    );
    println!(
        "   Version:    {}",
        meta.version_number
            .map_or("<none>".to_string(), |v| v.to_string())
    );

    // ── 7. ZLIB compression (feature-gated) ──────────────────────────────

    #[cfg(feature = "glue")]
    {
        println!("\n--- ZLIB compression ---");

        let large_payload = vec![0xAB_u8; 4096];
        let framed_zlib =
            encode_glue_wire_format(version_id, &large_payload, GlueCompression::Zlib)?;
        println!(
            "Uncompressed: {} bytes → compressed: {} bytes (ratio: {:.2}x)",
            large_payload.len(),
            framed_zlib.len() - 18,
            large_payload.len() as f64 / (framed_zlib.len() - 18) as f64
        );

        let (zlib_id, zlib_payload) = decode_glue_wire_format(&framed_zlib)?;
        assert_eq!(zlib_id, version_id);
        assert_eq!(zlib_payload, large_payload);
        println!("✓ ZLIB encode → decode round-trip");
    }

    #[cfg(not(feature = "glue"))]
    {
        println!("\n(ZLIB compression skipped — enable with --features glue)");
        let err = encode_glue_wire_format(version_id, b"test", GlueCompression::Zlib).unwrap_err();
        assert!(err.to_string().contains("glue") || err.to_string().contains("ZLIB"));
        println!("✓ ZLIB without feature returns descriptive error");
    }

    println!("\n=== All assertions passed ===\n");
    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "schemreg=debug".parse().unwrap()),
        )
        .try_init();
}
