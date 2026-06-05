//! JSON Schema serialisation + Confluent wire-format framing.
//!
//! Provides [`JsonSchemaEncoder`] and [`JsonSchemaDecoder`] which combine
//! [JSON Schema (draft 2020-12)][draft] validation with automatic schema
//! registration and Confluent 5-byte wire-format framing.
//!
//! Validation is performed by the [`jsonschema`] crate, which implements
//! drafts 4, 6, 7, 2019-09, and 2020-12. Both encoder and decoder cache the
//! compiled [`jsonschema::Validator`] so repeated calls do not re-compile the
//! schema.
//!
//! [draft]: https://json-schema.org/draft/2020-12
//!
//! # Feature requirement
//!
//! This module is gated behind the **`json`** Cargo feature:
//!
//! ```toml
//! [dependencies]
//! schemreg = { version = "0.3", features = ["json"] }
//! ```
//!
//! # Layered model
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ JsonSchemaEncoder / JsonSchemaDecoder (this module)          │
//! │  • Optional JSON Schema validation (jsonschema draft 2020-12)│
//! │  • Register / look up schema in Confluent registry           │
//! │  • Wrap / strip Confluent 5-byte wire-format header          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Examples
//!
//! ## Round-trip encode and decode
//!
//! ```rust,ignore
//! use serde_json::json;
//! use schemreg::json::{JsonSchemaDecoder, JsonSchemaEncoder};
//!
//! const SCHEMA: &str = r#"{
//!     "$schema": "https://json-schema.org/draft/2020-12/schema",
//!     "type": "object",
//!     "properties": {
//!         "id":   { "type": "integer" },
//!         "name": { "type": "string" }
//!     },
//!     "required": ["id", "name"]
//! }"#;
//!
//! let encoder = JsonSchemaEncoder::builder()
//!     .registry(registry.clone())
//!     .schema(SCHEMA)
//!     .build()?;
//!
//! let value = json!({"id": 1, "name": "Widget"});
//! let framed = encoder.encode(&value, "orders", EncodeTarget::Value).await?;
//!
//! let decoder = JsonSchemaDecoder::new(registry);
//! let decoded = decoder.decode(framed).await?;
//! assert_eq!(decoded, json!({"id": 1, "name": "Widget"}));
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use jsonschema::Validator;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::error::{Result, SchemaRegError};
use crate::subject::SubjectNameStrategy;
use crate::traits::SchemaRegistryClient;
use crate::types::{EncodeTarget, SchemaId, SchemaReference, SchemaType};
use crate::wire::{decode_wire_format_bytes, encode_wire_format};

// ── Type aliases ──────────────────────────────────────────────────────────

type InFlightMap = Mutex<HashMap<String, Vec<oneshot::Sender<Result<Arc<EncoderEntry>>>>>>;

// ── Helpers ───────────────────────────────────────────────────────────────

/// Compile a JSON Schema string into a [`Validator`].
///
/// Uses the most recent draft supported by the document's `$schema` keyword
/// (falling back to draft 2020-12 when absent).  Errors are mapped to
/// [`SchemaRegError::config`].
fn compile_schema(schema_str: &str) -> Result<Validator> {
    let schema: Value = serde_json::from_str(schema_str)
        .map_err(|e| SchemaRegError::config(format!("invalid JSON Schema (parse): {e}")))?;
    jsonschema::validator_for(&schema)
        .map_err(|e| SchemaRegError::config(format!("invalid JSON Schema (compile): {e}")))
}

/// Validate a JSON value against a compiled `Validator`, collecting all errors.
///
/// Returns a single [`SchemaRegError::WireFormat`] that concatenates all
/// validation error messages, or `Ok(())` when the value is valid.
fn validate(validator: &Validator, value: &Value) -> Result<()> {
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(SchemaRegError::wire_format(format!(
            "JSON Schema validation failed: {}",
            errors.join("; ")
        )))
    }
}

// ── JsonSchemaEncoder ─────────────────────────────────────────────────────

/// Cached subject-resolution entry for the encoder.
struct EncoderEntry {
    schema_id: SchemaId,
    /// Compiled validator shared across encode calls — no re-compilation.
    validator: Arc<Validator>,
}

/// Serialises [`serde_json::Value`] (or any `serde::Serialize` type) to
/// Confluent-framed JSON bytes.
///
/// On the first call for a given subject the encoder registers the JSON Schema
/// with the registry, caches the assigned schema ID, and caches the compiled
/// [`jsonschema::Validator`].  Subsequent encodes hit only an in-memory
/// [`RwLock`]-guarded hash-map read — no registry call, no re-compilation.
///
/// When built with `validate_on_encode(true)` (the default), every value is
/// validated against the JSON Schema before serialisation.  Disable with
/// `validate_on_encode(false)` if you trust your producers and want maximum
/// throughput.
///
/// # Subject name resolution
///
/// The subject is derived from `topic` and [`EncodeTarget`] according to the
/// configured [`SubjectNameStrategy`].  For [`RecordName`] and
/// [`TopicRecordName`] strategies the record name must be supplied via
/// [`JsonSchemaEncoderBuilder::record_name`].
///
/// [`RecordName`]: crate::SubjectNameStrategy::RecordName
/// [`TopicRecordName`]: crate::SubjectNameStrategy::TopicRecordName
pub struct JsonSchemaEncoder<C> {
    registry: C,
    schema_str: String,
    validator: Arc<Validator>,
    record_name: Option<String>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    validate_on_encode: bool,
    /// `subject → (schema_id, compiled validator)` — populated lazily.
    cache: RwLock<HashMap<String, Arc<EncoderEntry>>>,
    /// In-flight coalescing: subjects currently being registered.
    in_flight: InFlightMap,
}

impl<C: std::fmt::Debug> std::fmt::Debug for JsonSchemaEncoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSchemaEncoder")
            .field("registry", &self.registry)
            .field("record_name", &self.record_name)
            .field("strategy", &self.strategy)
            .field("validate_on_encode", &self.validate_on_encode)
            .finish_non_exhaustive()
    }
}

impl<C: SchemaRegistryClient> JsonSchemaEncoder<C> {
    /// Create a builder for `JsonSchemaEncoder`.
    pub fn builder() -> JsonSchemaEncoderBuilder<C> {
        JsonSchemaEncoderBuilder::new()
    }

    /// Return the cached schema ID for `subject`, if it has been resolved.
    ///
    /// Returns `None` for subjects not yet encountered or not yet resolved.
    /// Useful for observability without triggering a registration.
    #[must_use]
    pub fn cached_schema_id(&self, subject: &str) -> Option<SchemaId> {
        self.cache.read().get(subject).map(|e| e.schema_id)
    }

    /// Resolve subject → cached entry, registering the schema if needed.
    ///
    /// Concurrent callers for the same subject coalesce behind a single leader
    /// registration, preventing duplicate schema-register RPCs under high
    /// concurrency.
    async fn resolve_subject(&self, subject: &str) -> Result<Arc<EncoderEntry>> {
        // Fast path: read lock only.
        if let Some(entry) = self.cache.read().get(subject) {
            return Ok(Arc::clone(entry));
        }

        // Slow path: coalescing lock.
        let waiter_rx = {
            let mut in_flight = self.in_flight.lock();
            // Double-check after acquiring the lock.
            if let Some(entry) = self.cache.read().get(subject) {
                return Ok(Arc::clone(entry));
            }
            if let Some(waiters) = in_flight.get_mut(subject) {
                let (tx, rx) = oneshot::channel();
                waiters.push(tx);
                Some(rx)
            } else {
                in_flight.insert(subject.to_string(), Vec::new());
                None
            }
        };

        if let Some(rx) = waiter_rx {
            return rx.await.map_err(|_| {
                SchemaRegError::invalid_state(
                    "JSON schema entry resolution cancelled by the leader",
                )
            })?;
        }

        // We are the leader. Use a drop-guard to cancel waiters on panic/cancellation.
        struct ResolveGuard<'a> {
            in_flight: &'a InFlightMap,
            subject: String,
            done: bool,
        }
        impl Drop for ResolveGuard<'_> {
            fn drop(&mut self) {
                if !self.done {
                    let waiters = self
                        .in_flight
                        .lock()
                        .remove(&self.subject)
                        .unwrap_or_default();
                    for tx in waiters {
                        let _ = tx.send(Err(SchemaRegError::invalid_state(
                            "JSON schema entry resolution cancelled",
                        )));
                    }
                }
            }
        }

        let mut guard = ResolveGuard {
            in_flight: &self.in_flight,
            subject: subject.to_string(),
            done: false,
        };

        let result = self
            .registry
            .register_schema(
                subject,
                &self.schema_str,
                SchemaType::Json,
                &self.references,
            )
            .await
            .map(|schema_id| {
                Arc::new(EncoderEntry {
                    schema_id,
                    validator: Arc::clone(&self.validator),
                })
            });

        let waiters = self.in_flight.lock().remove(subject).unwrap_or_default();

        match &result {
            Ok(entry) => {
                self.cache
                    .write()
                    .insert(subject.to_string(), Arc::clone(entry));
                for tx in waiters {
                    let _ = tx.send(Ok(Arc::clone(entry)));
                }
            }
            Err(e) => {
                let cloned = e.clone();
                for tx in waiters {
                    let _ = tx.send(Err(cloned.clone()));
                }
            }
        }

        guard.done = true;
        result
    }

    /// Serialise `value` to Confluent-framed JSON bytes.
    ///
    /// When `validate_on_encode` is enabled (the default), `value` is
    /// validated against the registered JSON Schema before serialisation.
    ///
    /// # Errors
    ///
    /// - Configuration / registry errors from subject resolution.
    /// - Validation errors if `validate_on_encode` is `true` and `value` is invalid.
    /// - Serialisation errors (should not occur for well-formed `serde_json::Value`).
    pub async fn encode(&self, value: &Value, topic: &str, target: EncodeTarget) -> Result<Bytes> {
        let subject = self
            .strategy
            .subject_name(topic, self.record_name.as_deref(), target)?;
        let entry = self.resolve_subject(&subject).await?;

        if self.validate_on_encode {
            validate(&entry.validator, value)?;
        }

        let raw = serde_json::to_vec(value)
            .map_err(|e| SchemaRegError::wire_format(format!("JSON serialization failed: {e}")))?;
        Ok(encode_wire_format(entry.schema_id, &raw))
    }

    /// Serialise any `serde::Serialize` value to Confluent-framed JSON bytes.
    ///
    /// Converts `value` to [`serde_json::Value`] via [`serde_json::to_value`],
    /// then delegates to [`encode`](Self::encode).  This allows transparent
    /// validation against the JSON Schema even for concrete Rust types.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be serialised to JSON, or if
    /// validation fails.
    pub async fn encode_ser<T: Serialize>(
        &self,
        value: &T,
        topic: &str,
        target: EncodeTarget,
    ) -> Result<Bytes> {
        let json_value = serde_json::to_value(value).map_err(|e| {
            SchemaRegError::wire_format(format!("failed to convert value to JSON: {e}"))
        })?;
        self.encode(&json_value, topic, target).await
    }
}

// ── JsonSchemaEncoderBuilder ──────────────────────────────────────────────

/// Builder for [`JsonSchemaEncoder`].
pub struct JsonSchemaEncoderBuilder<C> {
    registry: Option<C>,
    schema: Option<String>,
    record_name: Option<String>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    validate_on_encode: bool,
}

impl<C: SchemaRegistryClient> JsonSchemaEncoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            schema: None,
            record_name: None,
            strategy: SubjectNameStrategy::TopicName,
            references: Vec::new(),
            validate_on_encode: true,
        }
    }

    /// Set the schema registry client (required).
    pub fn registry(mut self, registry: C) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the JSON Schema string (required).
    ///
    /// The schema is compiled immediately in [`build`](Self::build) so syntax
    /// errors are surfaced at construction time, not at encode time.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set an explicit record name for [`RecordName`] and [`TopicRecordName`]
    /// strategies (optional for [`TopicName`]).
    ///
    /// [`RecordName`]: crate::SubjectNameStrategy::RecordName
    /// [`TopicRecordName`]: crate::SubjectNameStrategy::TopicRecordName
    /// [`TopicName`]: crate::SubjectNameStrategy::TopicName
    pub fn record_name(mut self, name: impl Into<String>) -> Self {
        self.record_name = Some(name.into());
        self
    }

    /// Set the subject name strategy (default: [`TopicName`]).
    ///
    /// [`TopicName`]: crate::SubjectNameStrategy::TopicName
    pub fn strategy(mut self, strategy: SubjectNameStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set schema references (default: empty).
    pub fn references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Enable or disable JSON Schema validation on encode (default: `true`).
    ///
    /// Disabling validation improves throughput at the cost of allowing
    /// invalid payloads to reach the registry and consumers.
    pub fn validate_on_encode(mut self, validate: bool) -> Self {
        self.validate_on_encode = validate;
        self
    }

    /// Build the encoder.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `registry` or `schema` was not set,
    /// or if the schema string is not valid JSON Schema.
    pub fn build(self) -> Result<JsonSchemaEncoder<C>> {
        let registry = self
            .registry
            .ok_or_else(|| SchemaRegError::config("JsonSchemaEncoder: registry must be set"))?;
        let schema_str = self
            .schema
            .ok_or_else(|| SchemaRegError::config("JsonSchemaEncoder: schema must be set"))?;
        let validator = Arc::new(compile_schema(&schema_str)?);
        Ok(JsonSchemaEncoder {
            registry,
            schema_str,
            validator,
            record_name: self.record_name,
            strategy: self.strategy,
            references: self.references,
            validate_on_encode: self.validate_on_encode,
            cache: RwLock::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        })
    }
}

// ── JsonSchemaDecoder ─────────────────────────────────────────────────────

/// Strips the Confluent wire-format header and deserialises the JSON payload.
///
/// On the first decode for each schema ID the decoder fetches the schema from
/// the registry and caches the compiled [`jsonschema::Validator`].
/// Subsequent decodes with the same schema ID are served entirely from the
/// in-memory cache.
///
/// # Validation
///
/// When built with `validate_on_decode(true)` (off by default — see note
/// below), every decoded value is validated against the registered schema.
/// This is useful for detecting schema drift in pipeline testing but adds
/// latency for every decode.
///
/// **Why off by default?**  Producers are assumed to have validated on encode.
/// Adding validation on every consumer decode doubles the validation overhead.
/// Enable it in integration test environments or for strict conformance
/// pipelines.
///
/// # Serde support
///
/// Use [`decode_de`](Self::decode_de) to deserialise directly into a
/// concrete Rust type implementing [`serde::Deserialize`].
pub struct JsonSchemaDecoder<C> {
    registry: C,
    validate_on_decode: bool,
    schema_cache: RwLock<HashMap<SchemaId, Arc<Validator>>>,
}

impl<C: SchemaRegistryClient> JsonSchemaDecoder<C> {
    /// Create a new `JsonSchemaDecoder` backed by the given registry client.
    ///
    /// Validation on decode is **disabled** by default. Use [`with_validation`]
    /// to enable it.
    ///
    /// [`with_validation`]: Self::with_validation
    pub fn new(registry: C) -> Self {
        Self {
            registry,
            validate_on_decode: false,
            schema_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new `JsonSchemaDecoder` with validation on decode enabled.
    pub fn with_validation(registry: C) -> Self {
        Self {
            registry,
            validate_on_decode: true,
            schema_cache: RwLock::new(HashMap::new()),
        }
    }

    async fn get_validator(&self, id: SchemaId) -> Result<Arc<Validator>> {
        // Fast path: already cached.
        if let Some(v) = self.schema_cache.read().get(&id) {
            return Ok(Arc::clone(v));
        }
        // Slow path: fetch from registry, compile, insert.
        let registry_schema = self.registry.get_schema_by_id(id).await?;
        let validator = Arc::new(compile_schema(&registry_schema.schema)?);
        self.schema_cache.write().insert(id, Arc::clone(&validator));
        Ok(validator)
    }

    /// Decode a Confluent-framed JSON message to a [`serde_json::Value`].
    ///
    /// # Errors
    ///
    /// - Invalid Confluent wire header or truncated data.
    /// - Registry lookup failure.
    /// - JSON parse failure.
    /// - Validation failure when `validate_on_decode` is `true`.
    pub async fn decode(&self, data: Bytes) -> Result<Value> {
        let (schema_id, payload) = decode_wire_format_bytes(&data)?;

        let value: Value = serde_json::from_slice(&payload).map_err(|e| {
            SchemaRegError::wire_format(format!("JSON deserialisation failed: {e}"))
        })?;

        if self.validate_on_decode {
            let validator = self.get_validator(schema_id).await?;
            validate(&validator, &value)?;
        }

        Ok(value)
    }

    /// Decode a Confluent-framed JSON message and deserialise into `T`.
    ///
    /// Decodes to [`serde_json::Value`] via [`decode`](Self::decode), then
    /// converts using [`serde_json::from_value`].  Validation (if enabled)
    /// runs on the `Value` before conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails, or if the value cannot be
    /// deserialised into `T`.
    pub async fn decode_de<T: DeserializeOwned>(&self, data: Bytes) -> Result<T> {
        let value = self.decode(data).await?;
        serde_json::from_value(value).map_err(|e| {
            SchemaRegError::wire_format(format!(
                "failed to deserialise JSON value into target type: {e}"
            ))
        })
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use crate::types::{Schema, SchemaType, SchemaVersion};

    // ── Mock registry ─────────────────────────────────────────────────────

    #[derive(Clone, Debug)]
    struct MockRegistry {
        inner: StdArc<MockRegistryInner>,
    }

    struct MockRegistryInner {
        schemas: StdMutex<HashMap<SchemaId, Schema>>,
        next_id: StdMutex<u32>,
    }

    impl std::fmt::Debug for MockRegistryInner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockRegistryInner").finish_non_exhaustive()
        }
    }

    impl MockRegistry {
        fn new() -> Self {
            Self {
                inner: StdArc::new(MockRegistryInner {
                    schemas: StdMutex::new(HashMap::new()),
                    next_id: StdMutex::new(1),
                }),
            }
        }
    }

    impl SchemaRegistryClient for MockRegistry {
        async fn get_schema_by_id(&self, id: SchemaId) -> crate::error::Result<StdArc<Schema>> {
            self.inner
                .schemas
                .lock()
                .unwrap()
                .get(&id)
                .map(|s| StdArc::new(s.clone()))
                .ok_or_else(|| SchemaRegError::api(40403, format!("schema {id} not found")))
        }

        async fn get_latest_schema(&self, _subject: &str) -> crate::error::Result<StdArc<Schema>> {
            Err(SchemaRegError::not_supported("not implemented"))
        }

        async fn get_schema_by_version(
            &self,
            _subject: &str,
            _version: SchemaVersion,
        ) -> crate::error::Result<StdArc<Schema>> {
            Err(SchemaRegError::not_supported("not implemented"))
        }

        async fn register_schema(
            &self,
            _subject: &str,
            schema: &str,
            schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> crate::error::Result<SchemaId> {
            let mut next_id = self.inner.next_id.lock().unwrap();
            let id = SchemaId::from(*next_id);
            *next_id += 1;
            let schema_obj = Schema::new(id, schema_type, schema);
            self.inner.schemas.lock().unwrap().insert(id, schema_obj);
            Ok(id)
        }
    }

    // ── Schema fixture ────────────────────────────────────────────────────

    const ORDER_SCHEMA: &str = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "id":    { "type": "integer" },
            "item":  { "type": "string" },
            "price": { "type": "number" }
        },
        "required": ["id", "item", "price"],
        "additionalProperties": false
    }"#;

    // ── Encoder tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn encode_valid_value() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg)
            .schema(ORDER_SCHEMA)
            .build()
            .unwrap();

        let v = json!({"id": 1, "item": "Widget", "price": 9.99});
        let framed = enc.encode(&v, "orders", EncodeTarget::Value).await.unwrap();

        // Confluent magic byte + 4-byte schema ID + JSON payload
        assert_eq!(framed[0], 0x00, "magic byte must be 0x00");
        assert!(framed.len() > 5, "framed must include payload");
    }

    #[tokio::test]
    async fn encode_invalid_value_rejected() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg)
            .schema(ORDER_SCHEMA)
            .build()
            .unwrap();

        // Missing required field "item"
        let v = json!({"id": 1, "price": 9.99});
        let err = enc
            .encode(&v, "orders", EncodeTarget::Value)
            .await
            .unwrap_err();
        assert!(err.is_wire_format_error(), "should be a wire format error");
        assert!(
            err.to_string().contains("validation"),
            "should mention validation"
        );
    }

    #[tokio::test]
    async fn encode_no_validation() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg)
            .schema(ORDER_SCHEMA)
            .validate_on_encode(false)
            .build()
            .unwrap();

        // Invalid value — validation disabled so encode succeeds
        let v = json!({"id": "not-an-integer"});
        let framed = enc.encode(&v, "orders", EncodeTarget::Value).await.unwrap();
        assert_eq!(framed[0], 0x00);
    }

    #[tokio::test]
    async fn encode_caches_schema_id() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg)
            .schema(ORDER_SCHEMA)
            .build()
            .unwrap();

        let v = json!({"id": 1, "item": "A", "price": 1.0});

        let f1 = enc.encode(&v, "orders", EncodeTarget::Value).await.unwrap();
        let f2 = enc.encode(&v, "orders", EncodeTarget::Value).await.unwrap();

        // Schema IDs in the framed bytes must be identical (same bytes 1..5).
        assert_eq!(
            &f1[1..5],
            &f2[1..5],
            "schema ID must be cached across calls"
        );
    }

    #[tokio::test]
    async fn encode_key_and_value_subjects() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg)
            .schema(ORDER_SCHEMA)
            .validate_on_encode(false)
            .build()
            .unwrap();

        let v = json!({"id": 1, "item": "A", "price": 1.0});

        let fv = enc.encode(&v, "orders", EncodeTarget::Value).await.unwrap();
        let fk = enc.encode(&v, "orders", EncodeTarget::Key).await.unwrap();

        // Different subjects → different schema IDs registered → different IDs in wire frames.
        assert_ne!(
            &fv[1..5],
            &fk[1..5],
            "key and value must use different schema IDs"
        );
        assert!(enc.cached_schema_id("orders-value").is_some());
        assert!(enc.cached_schema_id("orders-key").is_some());
    }

    // ── encode_ser tests ──────────────────────────────────────────────────

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Order {
        id: i64,
        item: String,
        price: f64,
    }

    #[tokio::test]
    async fn encode_ser_roundtrip() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg.clone())
            .schema(ORDER_SCHEMA)
            .build()
            .unwrap();
        let dec = JsonSchemaDecoder::new(reg);

        let original = Order {
            id: 42,
            item: "Gadget".into(),
            price: 19.99,
        };

        let framed = enc
            .encode_ser(&original, "orders", EncodeTarget::Value)
            .await
            .unwrap();
        let decoded: Order = dec.decode_de(framed).await.unwrap();
        assert_eq!(original, decoded);
    }

    // ── Decoder tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn decode_valid_payload() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg.clone())
            .schema(ORDER_SCHEMA)
            .build()
            .unwrap();
        let dec = JsonSchemaDecoder::new(reg);

        let v = json!({"id": 7, "item": "Sprocket", "price": 3.50});
        let framed = enc.encode(&v, "orders", EncodeTarget::Value).await.unwrap();

        let decoded = dec.decode(framed).await.unwrap();
        assert_eq!(decoded, v);
    }

    #[tokio::test]
    async fn decode_with_validation_valid() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg.clone())
            .schema(ORDER_SCHEMA)
            .validate_on_encode(false) // let an invalid payload through
            .build()
            .unwrap();
        let dec = JsonSchemaDecoder::with_validation(reg);

        // Encode without validation so we can test decoder-side validation.
        let valid = json!({"id": 1, "item": "Valid", "price": 1.0});
        let framed = enc
            .encode(&valid, "orders", EncodeTarget::Value)
            .await
            .unwrap();

        // Should pass decoder-side validation.
        let result = dec.decode(framed).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn decode_with_validation_invalid() {
        let reg = MockRegistry::new();
        let enc = JsonSchemaEncoder::builder()
            .registry(reg.clone())
            .schema(ORDER_SCHEMA)
            .validate_on_encode(false) // bypass encoder validation
            .build()
            .unwrap();
        let dec = JsonSchemaDecoder::with_validation(reg);

        // Encode an invalid payload (missing "item" and "price").
        let invalid = json!({"id": 1});
        let framed = enc
            .encode(&invalid, "orders", EncodeTarget::Value)
            .await
            .unwrap();

        // Decoder with validation should reject it.
        let err = dec.decode(framed).await.unwrap_err();
        assert!(err.is_wire_format_error());
    }

    #[tokio::test]
    async fn build_with_invalid_schema_returns_config_error() {
        let reg = MockRegistry::new();
        let result = JsonSchemaEncoder::builder()
            .registry(reg)
            .schema("not valid JSON")
            .build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.is_config_error(),
            "should be a config error, got: {err}"
        );
    }
}
