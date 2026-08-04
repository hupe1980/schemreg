//! Codec-level tests for the Avro and JSON Schema encoder/decoder pairs.
//!
//! Focus areas:
//! - Confluent framing produced by the codecs is byte-exact.
//! - Avro **schema resolution** against an explicit reader schema.
//! - The parsed-schema / compiled-validator caches are bounded and coalescing,
//!   so a burst of consumers cannot flood the registry or re-parse the same
//!   schema N times.

#![cfg(any(feature = "avro", feature = "json"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use schemreg::{
    EncodeTarget, Result, Schema, SchemaId, SchemaReference, SchemaRegistryClient, SchemaType,
    SchemaVersion,
};
use tokio::sync::{Notify, Semaphore};

// ── Mock registry ─────────────────────────────────────────────────────────

/// A registry backed by a fixed map, counting `get_schema_by_id` calls and
/// optionally blocking them so concurrency can be observed deterministically.
struct MockRegistry {
    schemas: HashMap<SchemaId, Schema>,
    get_by_id_calls: AtomicU32,
    /// When set, `get_schema_by_id` parks until `release()` is called.
    gate: Option<Gate>,
}

struct Gate {
    started: Notify,
    release: Semaphore,
    waiting: AtomicU32,
}

impl MockRegistry {
    fn new(schemas: impl IntoIterator<Item = Schema>) -> Self {
        Self {
            schemas: schemas.into_iter().map(|s| (s.id, s)).collect(),
            get_by_id_calls: AtomicU32::new(0),
            gate: None,
        }
    }

    fn gated(schemas: impl IntoIterator<Item = Schema>) -> Self {
        let mut this = Self::new(schemas);
        this.gate = Some(Gate {
            started: Notify::new(),
            release: Semaphore::new(0),
            waiting: AtomicU32::new(0),
        });
        this
    }

    fn calls(&self) -> u32 {
        self.get_by_id_calls.load(Ordering::SeqCst)
    }

    fn release_all(&self) {
        if let Some(g) = &self.gate {
            let n = g.waiting.swap(0, Ordering::SeqCst);
            g.release.add_permits(n as usize);
        }
    }

    async fn wait_started(&self) {
        if let Some(g) = &self.gate {
            g.started.notified().await;
        }
    }
}

impl SchemaRegistryClient for MockRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.get_by_id_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(g) = &self.gate {
            g.started.notify_waiters();
            g.waiting.fetch_add(1, Ordering::SeqCst);
            let _ = g.release.acquire().await;
        }
        self.schemas
            .get(&id)
            .map(|s| Arc::new(s.clone()))
            .ok_or_else(|| schemreg::SchemaRegError::api(40403, format!("schema {id} not found")))
    }

    async fn get_latest_schema(&self, _subject: &str) -> Result<Arc<Schema>> {
        Err(schemreg::SchemaRegError::not_supported("not implemented"))
    }

    async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
        Err(schemreg::SchemaRegError::not_supported("not implemented"))
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

// ── Avro ──────────────────────────────────────────────────────────────────

#[cfg(feature = "avro")]
mod avro_codec {
    use super::*;
    use apache_avro::types::Value;
    use schemreg::avro::{AvroSchemaDecoder, AvroSchemaEncoder};

    /// Writer schema: two fields.
    const WRITER: &str = r#"{
        "type": "record",
        "name": "Order",
        "namespace": "com.example",
        "fields": [
            {"name": "id",   "type": "int"},
            {"name": "note", "type": "string"}
        ]
    }"#;

    /// Reader schema: `note` dropped, `qty` added with a default. Decoding a
    /// WRITER-encoded payload against this reader only works if schema
    /// resolution is applied.
    const READER: &str = r#"{
        "type": "record",
        "name": "Order",
        "namespace": "com.example",
        "fields": [
            {"name": "id",  "type": "int"},
            {"name": "qty", "type": "int", "default": 7}
        ]
    }"#;

    fn writer_schema_entry(id: u32) -> Schema {
        Schema::new(SchemaId::from(id), SchemaType::Avro, WRITER)
    }

    #[tokio::test]
    async fn encode_produces_confluent_framing() {
        let reg = Arc::new(MockRegistry::new([writer_schema_entry(1)]));
        let enc = AvroSchemaEncoder::builder()
            .registry(Arc::clone(&reg))
            .schema(WRITER)
            .build()
            .unwrap();

        let value = Value::Record(vec![
            ("id".into(), Value::Int(1)),
            ("note".into(), Value::String("hello".into())),
        ]);
        let framed = enc
            .encode(value, "orders", EncodeTarget::Value)
            .await
            .unwrap();

        assert_eq!(framed[0], 0x00, "Confluent magic byte");
        assert_eq!(&framed[1..5], &1u32.to_be_bytes(), "schema id big-endian");
        assert!(framed.len() > 5);
    }

    #[tokio::test]
    async fn decode_without_reader_schema_uses_the_writer_schema() {
        let reg = Arc::new(MockRegistry::new([writer_schema_entry(1)]));
        let enc = AvroSchemaEncoder::builder()
            .registry(Arc::clone(&reg))
            .schema(WRITER)
            .build()
            .unwrap();
        let dec = AvroSchemaDecoder::new(Arc::clone(&reg));

        let framed = enc
            .encode(
                Value::Record(vec![
                    ("id".into(), Value::Int(42)),
                    ("note".into(), Value::String("keep".into())),
                ]),
                "orders",
                EncodeTarget::Value,
            )
            .await
            .unwrap();

        let Value::Record(fields) = dec.decode(framed).await.unwrap() else {
            panic!("expected a record");
        };
        let names: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "note"],
            "without a reader schema the writer schema shape is preserved"
        );
    }

    /// The headline schema-evolution case: a consumer compiled against READER
    /// reads a payload written with WRITER. `note` must disappear and `qty`
    /// must materialise from its default.
    #[tokio::test]
    async fn decode_with_reader_schema_resolves_added_and_dropped_fields() {
        let reg = Arc::new(MockRegistry::new([writer_schema_entry(1)]));
        let enc = AvroSchemaEncoder::builder()
            .registry(Arc::clone(&reg))
            .schema(WRITER)
            .build()
            .unwrap();
        let dec = AvroSchemaDecoder::new(Arc::clone(&reg))
            .with_reader_schema(READER)
            .expect("READER is valid Avro");

        let framed = enc
            .encode(
                Value::Record(vec![
                    ("id".into(), Value::Int(9)),
                    ("note".into(), Value::String("dropped".into())),
                ]),
                "orders",
                EncodeTarget::Value,
            )
            .await
            .unwrap();

        let Value::Record(fields) = dec.decode(framed).await.unwrap() else {
            panic!("expected a record");
        };
        let by_name: HashMap<&str, &Value> = fields.iter().map(|(n, v)| (n.as_str(), v)).collect();

        assert_eq!(by_name.get("id"), Some(&&Value::Int(9)));
        assert_eq!(
            by_name.get("qty"),
            Some(&&Value::Int(7)),
            "the reader's default must be filled in"
        );
        assert!(
            !by_name.contains_key("note"),
            "a field absent from the reader schema must be dropped"
        );
    }

    #[tokio::test]
    async fn with_reader_schema_rejects_invalid_json() {
        let reg = Arc::new(MockRegistry::new([]));
        let err = AvroSchemaDecoder::new(reg)
            .with_reader_schema("{ not avro")
            .expect_err("invalid reader schema must be rejected at construction");
        assert!(err.is_config_error(), "{err}");
    }

    #[tokio::test]
    async fn decoder_cache_is_bounded_and_evicts() {
        let entries: Vec<Schema> = (1..=3).map(writer_schema_entry).collect();
        let reg = Arc::new(MockRegistry::new(entries));
        let enc = AvroSchemaEncoder::builder()
            .registry(Arc::clone(&reg))
            .schema(WRITER)
            .build()
            .unwrap();
        let dec = AvroSchemaDecoder::with_max_cache_entries(Arc::clone(&reg), 1);

        let payload = enc
            .encode(
                Value::Record(vec![
                    ("id".into(), Value::Int(1)),
                    ("note".into(), Value::String("x".into())),
                ]),
                "orders",
                EncodeTarget::Value,
            )
            .await
            .unwrap();

        // Re-frame the same Avro body under three different schema IDs.
        let body = &payload[5..];
        for id in 1u32..=3 {
            dec.decode(schemreg::encode_wire_format(id, body))
                .await
                .unwrap();
        }
        assert_eq!(dec.cache_len(), 1, "cache must not grow past its bound");
        assert_eq!(reg.calls(), 3);

        dec.clear_cache();
        assert_eq!(dec.cache_len(), 0);
    }

    /// 32 tasks decode the same cold schema ID at once; exactly one registry
    /// lookup (and one schema parse) may happen.
    #[tokio::test]
    async fn decoder_coalesces_concurrent_cold_misses() {
        let reg = Arc::new(MockRegistry::gated([writer_schema_entry(1)]));
        let dec = Arc::new(AvroSchemaDecoder::new(Arc::clone(&reg)));

        // A valid Avro body for WRITER: int 1 (zigzag 0x02), string "x".
        let body: &[u8] = &[0x02, 0x02, b'x'];
        let framed = schemreg::encode_wire_format(1u32, body);

        let mut handles = Vec::new();
        for _ in 0..32 {
            let dec = Arc::clone(&dec);
            let framed = framed.clone();
            handles.push(tokio::spawn(async move { dec.decode(framed).await }));
        }

        reg.wait_started().await;
        // Give every task a chance to register as a waiter before releasing.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        reg.release_all();

        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(
            reg.calls(),
            1,
            "32 concurrent cold decodes must produce exactly one registry lookup"
        );
    }
}

// ── JSON Schema ───────────────────────────────────────────────────────────

#[cfg(feature = "json")]
mod json_codec {
    use super::*;
    use schemreg::json::{JsonSchemaDecoder, JsonSchemaEncoder};
    use serde_json::json;

    const ORDER: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": { "id": { "type": "integer" } },
        "required": ["id"],
        "additionalProperties": false
    }"#;

    fn schema_entry(id: u32) -> Schema {
        Schema::new(SchemaId::from(id), SchemaType::Json, ORDER)
    }

    #[tokio::test]
    async fn validating_decoder_cache_is_bounded() {
        let reg = Arc::new(MockRegistry::new((1..=3).map(schema_entry)));
        let dec = JsonSchemaDecoder::with_validation(Arc::clone(&reg)).with_max_cache_entries(1);

        for id in 1u32..=3 {
            let framed = schemreg::encode_wire_format(id, br#"{"id":1}"#);
            dec.decode(framed).await.unwrap();
        }
        assert_eq!(dec.cache_len(), 1, "validator cache must respect its bound");
        assert_eq!(reg.calls(), 3);
    }

    #[tokio::test]
    async fn non_validating_decoder_never_touches_the_registry() {
        let reg = Arc::new(MockRegistry::new((1..=3).map(schema_entry)));
        let dec = JsonSchemaDecoder::new(Arc::clone(&reg));

        for id in 1u32..=3 {
            let framed = schemreg::encode_wire_format(id, br#"{"id":1}"#);
            dec.decode(framed).await.unwrap();
        }
        assert_eq!(
            reg.calls(),
            0,
            "with validation off, decoding must not compile or fetch a schema"
        );
        assert_eq!(dec.cache_len(), 0);
    }

    #[tokio::test]
    async fn validating_decoder_coalesces_concurrent_cold_misses() {
        let reg = Arc::new(MockRegistry::gated([schema_entry(1)]));
        let dec = Arc::new(JsonSchemaDecoder::with_validation(Arc::clone(&reg)));

        let framed = schemreg::encode_wire_format(1u32, br#"{"id":1}"#);
        let mut handles = Vec::new();
        for _ in 0..32 {
            let dec = Arc::clone(&dec);
            let framed = framed.clone();
            handles.push(tokio::spawn(async move { dec.decode(framed).await }));
        }

        reg.wait_started().await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        reg.release_all();

        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(reg.calls(), 1, "one compile, not 32");
    }

    #[tokio::test]
    async fn encoder_rejects_values_that_violate_the_schema() {
        let reg = Arc::new(MockRegistry::new([schema_entry(1)]));
        let enc = JsonSchemaEncoder::builder()
            .registry(Arc::clone(&reg))
            .schema(ORDER)
            .build()
            .unwrap();

        let err = enc
            .encode(&json!({"nope": 1}), "orders", EncodeTarget::Value)
            .await
            .unwrap_err();
        assert!(err.is_wire_format_error(), "{err}");
    }
}
