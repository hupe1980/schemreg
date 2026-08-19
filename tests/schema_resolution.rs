//! Producer-side resolution and framing, exercised through the real encoders.
//!
//! Two settings decide what a producer does before it can write a byte:
//!
//! - [`SchemaResolution`] — register the schema, look it up, or follow the
//!   subject's latest version;
//! - [`Framing`] — put a 4-byte ID or a 16-byte GUID in the prefix.
//!
//! The unit tests in `src/resolver.rs` pin the resolution logic itself. This
//! file pins that each encoder *wires it up*: that the setting reaches the
//! registry call, that the resulting identifier reaches the wire, and that the
//! per-subject cache collapses repeats.

#![cfg(any(feature = "confluent", feature = "avro", feature = "json"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use schemreg::{
    EncodeTarget, Framing, Result, Schema, SchemaGuid, SchemaId, SchemaReference,
    SchemaRegistryClient, SchemaResolution, SchemaType, SchemaVersion,
};

const AVRO: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;
const GUID: SchemaGuid = SchemaGuid::from_bytes([0xAB; 16]);

/// A registry that counts every call and can be told whether the schema is
/// already registered and whether it reports GUIDs.
#[derive(Default)]
struct Counting {
    registers: AtomicU32,
    lookups: AtomicU32,
    latests: AtomicU32,
    by_id: AtomicU32,
    registered: bool,
    reports_guid: bool,
}

impl Counting {
    fn registered() -> Self {
        Self {
            registered: true,
            ..Self::default()
        }
    }

    /// A Confluent Platform 8 registry: every response carries a GUID.
    fn with_guids() -> Self {
        Self {
            registered: true,
            reports_guid: true,
            ..Self::default()
        }
    }
    fn schema(&self, id: u32) -> Arc<Schema> {
        let mut s = Schema::new(id, SchemaType::Avro, AVRO);
        if self.reports_guid {
            s.guid = Some(GUID);
        }
        Arc::new(s)
    }
}

impl SchemaRegistryClient for Counting {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        self.by_id.fetch_add(1, Ordering::SeqCst);
        Ok(self.schema(id.as_u32()))
    }
    async fn get_latest_schema(&self, _: &str) -> Result<Arc<Schema>> {
        self.latests.fetch_add(1, Ordering::SeqCst);
        Ok(self.schema(77))
    }
    async fn get_schema_by_version(&self, _: &str, _: SchemaVersion) -> Result<Arc<Schema>> {
        Ok(self.schema(1))
    }
    async fn register_schema(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.registers.fetch_add(1, Ordering::SeqCst);
        Ok(SchemaId::new(11))
    }
    async fn lookup_schema(
        &self,
        _: &str,
        _: &str,
        _: SchemaType,
        _: &[SchemaReference],
    ) -> Result<Option<Arc<Schema>>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        Ok(self.registered.then(|| self.schema(22)))
    }
}

// ── ConfluentSchemaEncoder ────────────────────────────────────────────────

#[cfg(feature = "confluent")]
mod confluent_encoder {
    use super::*;
    use bytes::Bytes;
    use schemreg::{
        ConfluentSchemaEncoder, PayloadEncoder, SchemaKey, decode_schema_id_header,
        decode_wire_format,
    };

    fn encoder(
        registry: Arc<Counting>,
        resolution: SchemaResolution,
        framing: Framing,
    ) -> ConfluentSchemaEncoder<Arc<Counting>> {
        ConfluentSchemaEncoder::builder()
            .registry(registry)
            .schema(AVRO, SchemaType::Avro)
            .resolution(resolution)
            .framing(framing)
            .build()
            .expect("encoder builds")
    }

    /// The default must be the least surprising one, and it must be the one
    /// Confluent's own serdes default to.
    #[tokio::test]
    async fn the_default_registers_and_frames_with_a_numeric_id() {
        let registry = Arc::new(Counting::default());
        let enc = encoder(
            Arc::clone(&registry),
            SchemaResolution::default(),
            Framing::default(),
        );

        let framed = enc
            .encode(
                Bytes::from_static(b"body"),
                "orders",
                None,
                EncodeTarget::Value,
            )
            .await
            .expect("encode succeeds");

        assert_eq!(framed[0], 0x00, "the default framing is wire format v0");
        let (key, payload) = decode_wire_format(&framed).expect("decodes");
        assert_eq!(key, SchemaId::new(11));
        assert_eq!(payload, b"body");
        assert_eq!(registry.registers.load(Ordering::SeqCst), 1);
    }

    /// The whole point of `LookupOnly`: a read-only producer must not create a
    /// version, no matter how many messages it sends.
    #[tokio::test]
    async fn lookup_only_never_calls_register() {
        let registry = Arc::new(Counting::registered());
        let enc = encoder(
            Arc::clone(&registry),
            SchemaResolution::LookupOnly,
            Framing::SchemaId,
        );

        for _ in 0..5 {
            enc.encode(
                Bytes::from_static(b"x"),
                "orders",
                None,
                EncodeTarget::Value,
            )
            .await
            .expect("encode succeeds");
        }

        assert_eq!(registry.registers.load(Ordering::SeqCst), 0);
        assert_eq!(
            registry.lookups.load(Ordering::SeqCst),
            1,
            "the per-subject cache must collapse repeats"
        );
        assert_eq!(
            enc.cached_schema_id("orders-value"),
            Some(SchemaId::new(22))
        );
    }

    /// A drifted producer must stop, with an error a retry loop will not spin on.
    #[tokio::test]
    async fn lookup_only_fails_loudly_when_the_schema_is_not_registered() {
        let registry = Arc::new(Counting::default());
        let enc = encoder(
            Arc::clone(&registry),
            SchemaResolution::LookupOnly,
            Framing::SchemaId,
        );

        let err = enc
            .encode(
                Bytes::from_static(b"x"),
                "orders",
                None,
                EncodeTarget::Value,
            )
            .await
            .expect_err("an unregistered schema must fail");
        assert!(err.is_not_found(), "{err}");
        assert!(!err.is_retryable(), "{err}");
        assert_eq!(registry.registers.load(Ordering::SeqCst), 0);
        // A failed resolution must not be cached as a success.
        assert_eq!(enc.cached_subject_count(), 0);
    }

    #[tokio::test]
    async fn use_latest_version_frames_with_the_subject_head() {
        let registry = Arc::new(Counting::default());
        let enc = encoder(
            Arc::clone(&registry),
            SchemaResolution::UseLatestVersion,
            Framing::SchemaId,
        );

        let framed = enc
            .encode(
                Bytes::from_static(b"x"),
                "orders",
                None,
                EncodeTarget::Value,
            )
            .await
            .expect("encode succeeds");
        assert_eq!(
            decode_wire_format(&framed).expect("decodes").0,
            SchemaId::new(77)
        );
        assert_eq!(registry.latests.load(Ordering::SeqCst), 1);

        // Invalidation is how a long-lived producer picks up a newer version.
        enc.invalidate_subject("orders-value");
        enc.encode(
            Bytes::from_static(b"x"),
            "orders",
            None,
            EncodeTarget::Value,
        )
        .await
        .expect("encode succeeds");
        assert_eq!(registry.latests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn guid_framing_emits_wire_format_v1() {
        let registry = Arc::new(Counting::with_guids());
        let enc = encoder(
            Arc::clone(&registry),
            SchemaResolution::LookupOnly,
            Framing::SchemaGuid,
        );

        let framed = enc
            .encode(
                Bytes::from_static(b"body"),
                "orders",
                None,
                EncodeTarget::Value,
            )
            .await
            .expect("encode succeeds");

        assert_eq!(framed[0], 0x01);
        let (key, payload) = decode_wire_format(&framed).expect("decodes");
        assert_eq!(key, SchemaKey::Guid(GUID));
        assert_eq!(payload, b"body");
        // No numeric ID is on the wire, so none is reported as cached.
        assert_eq!(enc.cached_schema_id("orders-value"), None);
        assert_eq!(
            enc.cached_schema_key("orders-value"),
            Some(SchemaKey::Guid(GUID))
        );
    }

    /// A registry that reports no GUID cannot support v1. That must be a clear
    /// `NotSupported`, not a frame built from something invented.
    #[tokio::test]
    async fn guid_framing_against_a_pre_platform_8_registry_is_not_supported() {
        let registry = Arc::new(Counting::registered());
        let enc = encoder(registry, SchemaResolution::LookupOnly, Framing::SchemaGuid);

        let err = enc
            .encode(
                Bytes::from_static(b"x"),
                "orders",
                None,
                EncodeTarget::Value,
            )
            .await
            .expect_err("v1 framing needs GUIDs");
        assert!(err.is_not_supported(), "{err}");
    }

    /// Header placement: the payload carries no prefix, and the header carries
    /// exactly the prefix it would have had.
    #[tokio::test]
    async fn encode_with_header_moves_the_prefix_out_of_the_payload() {
        let registry = Arc::new(Counting::with_guids());
        let enc = encoder(registry, SchemaResolution::LookupOnly, Framing::SchemaGuid);

        let framed = enc
            .encode_with_header(
                Bytes::from_static(b"body"),
                "orders",
                None,
                EncodeTarget::Value,
            )
            .await
            .expect("encode succeeds");

        assert_eq!(framed.header_name, "__value_schema_id");
        assert_eq!(&framed.payload[..], b"body", "the payload stays unframed");
        let (key, indexes) = decode_schema_id_header(&framed.header_value).expect("decodes");
        assert_eq!(key, SchemaKey::Guid(GUID));
        assert_eq!(indexes, None, "Avro carries no message-index array");
    }

    #[tokio::test]
    async fn a_key_payload_uses_the_key_header_and_the_key_subject() {
        let registry = Arc::new(Counting::registered());
        let enc = encoder(
            Arc::clone(&registry),
            SchemaResolution::LookupOnly,
            Framing::SchemaId,
        );

        let framed = enc
            .encode_with_header(Bytes::from_static(b"k"), "orders", None, EncodeTarget::Key)
            .await
            .expect("encode succeeds");
        assert_eq!(framed.header_name, "__key_schema_id");
        assert_eq!(enc.cached_schema_id("orders-key"), Some(SchemaId::new(22)));
    }
}

// ── AvroSchemaEncoder ─────────────────────────────────────────────────────

#[cfg(feature = "avro")]
mod avro_encoder {
    use super::*;
    use apache_avro::types::Value;
    use schemreg::{AvroSchemaEncoder, SchemaKey, decode_schema_id_header, decode_wire_format};

    fn order() -> Value {
        Value::Record(vec![("id".to_string(), Value::String("o-1".into()))])
    }

    #[tokio::test]
    async fn the_setting_reaches_the_registry_call() {
        let registry = Arc::new(Counting::registered());
        let enc = AvroSchemaEncoder::builder()
            .registry(Arc::clone(&registry))
            .schema(AVRO)
            .resolution(SchemaResolution::LookupOnly)
            .build()
            .expect("encoder builds");

        enc.encode(order(), "orders", EncodeTarget::Value)
            .await
            .expect("encode succeeds");
        assert_eq!(registry.registers.load(Ordering::SeqCst), 0);
        assert_eq!(registry.lookups.load(Ordering::SeqCst), 1);
    }

    /// Framing and serialisation are independent: switching to v1 must change
    /// the prefix and nothing else.
    #[tokio::test]
    async fn guid_framing_changes_the_prefix_and_not_the_body() {
        let registry = Arc::new(Counting::with_guids());
        let build = |framing| {
            AvroSchemaEncoder::builder()
                .registry(Arc::clone(&registry))
                .schema(AVRO)
                .resolution(SchemaResolution::LookupOnly)
                .framing(framing)
                .build()
                .expect("encoder builds")
        };

        let v0 = build(Framing::SchemaId)
            .encode(order(), "orders", EncodeTarget::Value)
            .await
            .expect("encode succeeds");
        let v1 = build(Framing::SchemaGuid)
            .encode(order(), "orders", EncodeTarget::Value)
            .await
            .expect("encode succeeds");

        let (v0_key, v0_body) = decode_wire_format(&v0).expect("decodes");
        let (v1_key, v1_body) = decode_wire_format(&v1).expect("decodes");
        assert_eq!(v0_key, SchemaId::new(22));
        assert_eq!(v1_key, SchemaKey::Guid(GUID));
        assert_eq!(v0_body, v1_body, "the Avro bytes must be identical");
    }

    #[tokio::test]
    async fn encode_with_header_round_trips_through_the_header_codec() {
        let registry = Arc::new(Counting::with_guids());
        let enc = AvroSchemaEncoder::builder()
            .registry(registry)
            .schema(AVRO)
            .resolution(SchemaResolution::LookupOnly)
            .framing(Framing::SchemaGuid)
            .build()
            .expect("encoder builds");

        let framed = enc
            .encode_with_header(order(), "orders", EncodeTarget::Value)
            .await
            .expect("encode succeeds");

        let (key, _) = decode_schema_id_header(&framed.header_value).expect("decodes");
        assert_eq!(key, SchemaKey::Guid(GUID));
        // The payload must be decodable as bare Avro — i.e. carry no prefix.
        assert_ne!(framed.payload[0], 0x00);
    }
}

// ── JsonSchemaEncoder ─────────────────────────────────────────────────────

#[cfg(feature = "json")]
mod json_encoder {
    use super::*;
    use schemreg::{JsonSchemaEncoder, SchemaKey, decode_wire_format};

    const JSON_SCHEMA: &str = r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#;

    #[tokio::test]
    async fn the_setting_reaches_the_registry_call() {
        let registry = Arc::new(Counting::registered());
        let enc = JsonSchemaEncoder::builder()
            .registry(Arc::clone(&registry))
            .schema(JSON_SCHEMA)
            .resolution(SchemaResolution::LookupOnly)
            .build()
            .expect("encoder builds");

        enc.encode(
            &serde_json::json!({ "id": 1 }),
            "orders",
            EncodeTarget::Value,
        )
        .await
        .expect("encode succeeds");
        assert_eq!(registry.registers.load(Ordering::SeqCst), 0);
        assert_eq!(registry.lookups.load(Ordering::SeqCst), 1);
    }

    /// Framing is orthogonal to validation and serialisation: v1 must change the
    /// prefix and leave the JSON body byte-identical.
    #[tokio::test]
    async fn guid_framing_changes_the_prefix_and_not_the_body() {
        let registry = Arc::new(Counting::with_guids());
        let build = |framing| {
            JsonSchemaEncoder::builder()
                .registry(Arc::clone(&registry))
                .schema(JSON_SCHEMA)
                .resolution(SchemaResolution::LookupOnly)
                .framing(framing)
                .build()
                .expect("encoder builds")
        };
        let value = serde_json::json!({ "id": 1 });

        let v0 = build(Framing::SchemaId)
            .encode(&value, "orders", EncodeTarget::Value)
            .await
            .expect("encodes");
        let v1 = build(Framing::SchemaGuid)
            .encode(&value, "orders", EncodeTarget::Value)
            .await
            .expect("encodes");

        let (v0_key, v0_body) = decode_wire_format(&v0).expect("decodes");
        let (v1_key, v1_body) = decode_wire_format(&v1).expect("decodes");
        assert_eq!(v0_key, SchemaId::new(22));
        assert_eq!(v1_key, SchemaKey::Guid(GUID));
        assert_eq!(v0_body, v1_body);
    }

    /// Validation must still run on the header path — it is the same encode,
    /// only the identifier moves.
    #[tokio::test]
    async fn encode_with_header_still_validates() {
        let registry = Arc::new(Counting::registered());
        let enc = JsonSchemaEncoder::builder()
            .registry(registry)
            .schema(r#"{"type":"object","required":["id"]}"#)
            .resolution(SchemaResolution::LookupOnly)
            .build()
            .expect("encoder builds");

        let err = enc
            .encode_with_header(&serde_json::json!({}), "orders", EncodeTarget::Value)
            .await
            .expect_err("a value missing a required field must be rejected");
        assert!(err.is_wire_format_error(), "{err}");
    }
}

// ── ProtobufSchemaEncoder ─────────────────────────────────────────────────

#[cfg(feature = "protobuf")]
mod protobuf_encoder {
    use super::*;
    use prost_reflect::DescriptorPool;
    use prost_reflect::prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
    use schemreg::{ProtobufSchemaEncoder, decode_schema_id_header};

    fn descriptor() -> prost_reflect::MessageDescriptor {
        let file = FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("test".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Other".into()),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Order".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .expect("the synthetic descriptor set is well-formed");
        pool.get_message_by_name("test.Order")
            .expect("test.Order exists")
    }

    /// The header form carries the message-index array too — without it a
    /// consumer knows the schema but not which message type it is.
    #[tokio::test]
    async fn encode_with_header_carries_the_message_index() {
        let registry = Arc::new(Counting::registered());
        let enc = ProtobufSchemaEncoder::builder()
            .registry(registry)
            .schema("syntax = \"proto3\";")
            .descriptor(descriptor())
            .resolution(SchemaResolution::LookupOnly)
            .build()
            .expect("encoder builds");

        // `test.Order` is the second top-level message, so its path is [1].
        assert_eq!(enc.message_indexes(), &[1]);

        let framed = enc
            .encode_with_header(&(), "orders", EncodeTarget::Value)
            .await
            .expect("encode succeeds");

        let (key, indexes) = decode_schema_id_header(&framed.header_value).expect("decodes");
        assert_eq!(key, SchemaId::new(22));
        assert_eq!(indexes, Some(vec![1]));
    }
}

// ── Object-safe surface ───────────────────────────────────────────────────

/// Header placement has to be reachable through `dyn PayloadEncoder`, or an
/// application that erases its encoder is silently confined to prefix framing.
#[cfg(feature = "confluent")]
#[tokio::test]
async fn header_framing_is_reachable_through_a_trait_object() {
    use bytes::Bytes;
    use schemreg::{ConfluentSchemaEncoder, PayloadEncoder, decode_schema_id_header};

    let erased: Arc<dyn PayloadEncoder> = Arc::new(
        ConfluentSchemaEncoder::builder()
            .registry(Arc::new(Counting::with_guids()))
            .schema(AVRO, SchemaType::Avro)
            .resolution(SchemaResolution::LookupOnly)
            .framing(Framing::SchemaGuid)
            .build()
            .expect("encoder builds"),
    );

    let record = erased
        .encode_with_header(
            Bytes::from_static(b"body"),
            "orders",
            None,
            EncodeTarget::Value,
        )
        .await
        .expect("encode succeeds");

    assert_eq!(record.header_name, "__value_schema_id");
    assert_eq!(&record.payload[..], b"body");
    assert_eq!(
        decode_schema_id_header(&record.header_value)
            .expect("decodes")
            .0,
        schemreg::SchemaKey::Guid(GUID)
    );
}

/// An encoder that only knows prefix framing must answer `NotSupported` — and
/// specifically not something a retry loop would spin on.
#[cfg(feature = "confluent")]
#[tokio::test]
async fn an_encoder_without_header_support_says_so() {
    use bytes::Bytes;
    use schemreg::{PayloadEncoder, Result as SrResult};
    use std::future::Future;
    use std::pin::Pin;

    struct PrefixOnly;
    impl PayloadEncoder for PrefixOnly {
        fn encode(
            &self,
            payload: Bytes,
            _: &str,
            _: Option<&str>,
            _: EncodeTarget,
        ) -> Pin<Box<dyn Future<Output = SrResult<bytes::Bytes>> + Send + '_>> {
            Box::pin(async move { Ok(payload) })
        }
    }

    let err = PrefixOnly
        .encode_with_header(Bytes::new(), "orders", None, EncodeTarget::Value)
        .await
        .expect_err("the default must not silently drop the header");
    assert!(err.is_not_supported(), "{err}");
    assert!(!err.is_retryable(), "{err}");
}
