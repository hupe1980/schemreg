//! Avro serialization + Confluent wire-format framing in a single step.
//!
//! Provides [`AvroSchemaEncoder`] and [`AvroSchemaDecoder`] which combine
//! `apache-avro` serialisation/deserialisation with automatic schema
//! registration and Confluent 5-byte wire-format framing.  Both types cache
//! the parsed [`apache_avro::Schema`] in memory so repeated calls do not
//! re-parse the schema JSON.
//!
//! # Feature requirement
//!
//! This module is gated behind the **`avro`** Cargo feature.  Add to
//! `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! schemreg = { version = "0.2", features = ["avro"] }
//! ```
//!
//! # Layered model
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │ AvroSchemaEncoder / AvroSchemaDecoder (this module)  │
//! │  • Avro serialise / deserialise (apache-avro)        │
//! │  • Register / look up schema in Confluent registry   │
//! │  • Wrap / strip Confluent 5-byte wire-format header  │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! # Examples
//!
//! ## Round-trip with a mock registry
//!
//! ```rust,ignore
//! use apache_avro::types::Value;
//! use schemreg::avro::{AvroSchemaDecoder, AvroSchemaEncoder};
//! use schemreg::CachedSchemaRegistry;
//! use schemreg::ConfluentSchemaRegistry;
//!
//! let registry = CachedSchemaRegistry::new(
//!     ConfluentSchemaRegistry::builder()
//!         .url("http://localhost:8081")
//!         .build()
//!         .unwrap(),
//! );
//!
//! // Encoder: registers the schema once, caches the ID.
//! let encoder = AvroSchemaEncoder::builder()
//!     .registry(registry.clone())
//!     .schema(r#"{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"int"}]}"#)
//!     .build()
//!     .unwrap();
//!
//! let value = Value::Record(vec![("id".to_string(), Value::Int(42))]);
//! let framed: bytes::Bytes = encoder.encode(value, "orders", false).await.unwrap();
//!
//! // Decoder: fetches the schema by ID on first decode, caches it.
//! let decoder = AvroSchemaDecoder::new(registry);
//! let decoded: Value = decoder.decode(framed).await.unwrap();
//! println!("{decoded:?}");
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::error::{Result, SchemaRegError};
use crate::subject::SubjectNameStrategy;
use crate::traits::SchemaRegistryClient;
use crate::types::{EncodeTarget, SchemaId, SchemaReference, SchemaType};
use crate::wire::{decode_wire_format_bytes, encode_wire_format};

// ── Schema helpers ────────────────────────────────────────────────────────

/// Parse an Avro schema JSON string, mapping `apache_avro::Error` to
/// [`SchemaRegError::config`].
fn parse_avro_schema(schema_str: &str) -> Result<AvroSchema> {
    AvroSchema::parse_str(schema_str)
        .map_err(|e| SchemaRegError::config(format!("invalid Avro schema: {e}")))
}

/// Return the fully-qualified name of a named Avro schema type
/// (Record, Enum, or Fixed).  Returns `None` for primitive / union / array /
/// map schemas.
fn schema_fullname(schema: &AvroSchema) -> Option<String> {
    match schema {
        AvroSchema::Record(rs) => Some(rs.name.fullname(rs.name.namespace.clone())),
        AvroSchema::Enum(es) => Some(es.name.fullname(es.name.namespace.clone())),
        AvroSchema::Fixed(fs) => Some(fs.name.fullname(fs.name.namespace.clone())),
        _ => None,
    }
}

// ── AvroSchemaEncoder ─────────────────────────────────────────────────────

/// Cached subject-resolution entry.
struct EncoderEntry {
    schema_id: SchemaId,
    /// Parsed schema shared with every encode call — no re-parsing per topic.
    avro_schema: Arc<AvroSchema>,
}

/// Serialises [`apache_avro::types::Value`] (or any `serde::Serialize` type) to
/// Confluent-framed Avro bytes.
///
/// On the first call for a given subject, the encoder registers the Avro schema
/// with the registry, caches the assigned schema ID, and caches the parsed
/// [`apache_avro::Schema`].  Subsequent encodes hit only an in-memory
/// [`RwLock`]-guarded hash-map read.
///
/// # Subject name resolution
///
/// The subject is derived from `topic` and `is_key` according to the
/// configured [`SubjectNameStrategy`].  For [`RecordName`] and
/// [`TopicRecordName`] strategies, the record name is extracted automatically
/// from the `"name"` / `"namespace"` fields of the Avro schema — no need to
/// pass it separately.
///
/// [`RecordName`]: crate::SubjectNameStrategy::RecordName
/// [`TopicRecordName`]: crate::SubjectNameStrategy::TopicRecordName
pub struct AvroSchemaEncoder<C> {
    registry: C,
    schema_str: String,
    avro_schema: Arc<AvroSchema>,
    /// Fully-qualified record name extracted from the schema at build time.
    schema_fullname: Option<String>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    /// `subject → (schema_id, parsed schema)` — populated lazily on first use.
    cache: RwLock<HashMap<String, Arc<EncoderEntry>>>,
    /// In-flight coalescing: subjects currently being registered.
    in_flight: Mutex<HashMap<String, Vec<oneshot::Sender<Result<Arc<EncoderEntry>>>>>>,
}

impl<C: SchemaRegistryClient> AvroSchemaEncoder<C> {
    /// Create a builder for `AvroSchemaEncoder`.
    pub fn builder() -> AvroSchemaEncoderBuilder<C> {
        AvroSchemaEncoderBuilder::new()
    }

    /// Resolve subject → schema ID, registering if not already cached.
    ///
    /// Uses the same coalescing pattern as [`ConfluentSchemaEncoder`]: concurrent
    /// callers for the same subject coalesce behind a single leader registration,
    /// preventing duplicate schema-register RPCs under high concurrency.
    async fn resolve_subject(&self, subject: &str) -> Result<Arc<EncoderEntry>> {
        // Fast path: already cached.
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
                SchemaRegError::invalid_state("schema entry resolution cancelled by the leader")
            })?;
        }

        // We are the leader. Use a drop-guard to cancel waiters on panic/cancellation.
        struct ResolveGuard<'a> {
            in_flight: &'a Mutex<HashMap<String, Vec<oneshot::Sender<Result<Arc<EncoderEntry>>>>>>,
            subject: &'a str,
            done: bool,
        }
        impl Drop for ResolveGuard<'_> {
            fn drop(&mut self) {
                if !self.done {
                    let waiters = self
                        .in_flight
                        .lock()
                        .remove(self.subject)
                        .unwrap_or_default();
                    for tx in waiters {
                        let _ = tx.send(Err(SchemaRegError::invalid_state(
                            "Avro schema entry resolution cancelled",
                        )));
                    }
                }
            }
        }

        let mut guard = ResolveGuard {
            in_flight: &self.in_flight,
            subject,
            done: false,
        };

        let result = self
            .registry
            .register_schema(
                subject,
                &self.schema_str,
                SchemaType::Avro,
                &self.references,
            )
            .await
            .map(|schema_id| {
                Arc::new(EncoderEntry {
                    schema_id,
                    avro_schema: Arc::clone(&self.avro_schema),
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

    /// Serialise `value` to Confluent-framed Avro bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The subject cannot be resolved (registry error or configuration error).
    /// - `value` does not conform to the Avro schema.
    pub async fn encode(&self, value: Value, topic: &str, target: EncodeTarget) -> Result<Bytes> {
        let subject = self
            .strategy
            .subject_name(topic, self.schema_fullname.as_deref(), target)?;
        let entry = self.resolve_subject(&subject).await?;
        let raw = apache_avro::to_avro_datum(&entry.avro_schema, value)
            .map_err(|e| SchemaRegError::wire_format(format!("Avro serialization failed: {e}")))?;
        Ok(encode_wire_format(entry.schema_id, &raw))
    }

    /// Serialise a `serde::Serialize` value to Confluent-framed Avro bytes.
    ///
    /// Converts `value` to [`apache_avro::types::Value`] via
    /// [`apache_avro::to_value`], then delegates to [`encode`](Self::encode).
    ///
    /// # Errors
    ///
    /// Returns an error if the type cannot be converted to an Avro value,
    /// or if the resulting value does not conform to the schema.
    pub async fn encode_ser<T: Serialize>(
        &self,
        value: &T,
        topic: &str,
        target: EncodeTarget,
    ) -> Result<Bytes> {
        let av_value = apache_avro::to_value(value).map_err(|e| {
            SchemaRegError::wire_format(format!("failed to convert value to Avro: {e}"))
        })?;
        self.encode(av_value, topic, target).await
    }
}

// ── AvroSchemaEncoderBuilder ──────────────────────────────────────────────

/// Builder for [`AvroSchemaEncoder`].
pub struct AvroSchemaEncoderBuilder<C> {
    registry: Option<C>,
    schema: Option<String>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
}

impl<C: SchemaRegistryClient> AvroSchemaEncoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            schema: None,
            strategy: SubjectNameStrategy::TopicName,
            references: Vec::new(),
        }
    }

    /// Set the schema registry client (required).
    pub fn registry(mut self, registry: C) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the Avro schema JSON string (required).
    ///
    /// The string is parsed immediately in [`build`](Self::build) so any
    /// syntax error is surfaced at construction time, not at encode time.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
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
    ///
    /// Only needed when the Avro schema references externally registered
    /// schemas via the Confluent `references` mechanism.
    pub fn references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Build the encoder.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `registry` or `schema` was not set,
    /// or if the schema string is not valid Avro JSON.
    pub fn build(self) -> Result<AvroSchemaEncoder<C>> {
        let registry = self
            .registry
            .ok_or_else(|| SchemaRegError::config("AvroSchemaEncoder: registry must be set"))?;
        let schema_str = self
            .schema
            .ok_or_else(|| SchemaRegError::config("AvroSchemaEncoder: schema must be set"))?;
        let avro_schema = parse_avro_schema(&schema_str)?;
        let fullname = schema_fullname(&avro_schema);
        Ok(AvroSchemaEncoder {
            registry,
            schema_str,
            avro_schema: Arc::new(avro_schema),
            schema_fullname: fullname,
            strategy: self.strategy,
            references: self.references,
            cache: RwLock::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        })
    }
}

// ── AvroSchemaDecoder ─────────────────────────────────────────────────────

/// Strips the Confluent wire-format header and deserialises the Avro payload
/// into an [`apache_avro::types::Value`].
///
/// On the first decode for each schema ID the decoder fetches the schema from
/// the registry and caches the parsed [`apache_avro::Schema`].  Subsequent
/// decodes with the same schema ID are served entirely from the in-memory cache.
///
/// # Serde support
///
/// Use [`decode_de`](Self::decode_de) to deserialise directly into a
/// concrete Rust type implementing [`serde::Deserialize`].
pub struct AvroSchemaDecoder<C> {
    registry: C,
    schema_cache: RwLock<HashMap<SchemaId, Arc<AvroSchema>>>,
}

impl<C: SchemaRegistryClient> AvroSchemaDecoder<C> {
    /// Create a new `AvroSchemaDecoder` backed by the given registry client.
    pub fn new(registry: C) -> Self {
        Self {
            registry,
            schema_cache: RwLock::new(HashMap::new()),
        }
    }

    async fn get_avro_schema(&self, id: SchemaId) -> Result<Arc<AvroSchema>> {
        // Fast path: already cached.
        if let Some(schema) = self.schema_cache.read().get(&id) {
            return Ok(Arc::clone(schema));
        }
        // Slow path: fetch from registry, then parse.
        let registry_schema = self.registry.get_schema_by_id(id).await?;
        let avro_schema = parse_avro_schema(&registry_schema.schema)?;
        let avro_schema = Arc::new(avro_schema);
        self.schema_cache
            .write()
            .insert(id, Arc::clone(&avro_schema));
        Ok(avro_schema)
    }

    /// Decode a Confluent-framed Avro message to a [`Value`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The wire header is invalid (not Confluent-framed or truncated).
    /// - The schema registry lookup fails.
    /// - The Avro bytes do not conform to the schema.
    pub async fn decode(&self, data: Bytes) -> Result<Value> {
        let (schema_id, payload) = decode_wire_format_bytes(&data)?;
        let avro_schema = self.get_avro_schema(schema_id).await?;
        let value = apache_avro::from_avro_datum(&avro_schema, &mut payload.as_ref(), None)
            .map_err(|e| {
                SchemaRegError::wire_format(format!("Avro deserialization failed: {e}"))
            })?;
        Ok(value)
    }

    /// Decode a Confluent-framed Avro message and deserialise it into `T`.
    ///
    /// Decodes to [`Value`] via [`decode`](Self::decode), then converts using
    /// [`apache_avro::from_value`].
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails, or if the decoded value cannot be
    /// mapped to `T`.
    pub async fn decode_de<T: for<'de> Deserialize<'de>>(&self, data: Bytes) -> Result<T> {
        let value = self.decode(data).await?;
        apache_avro::from_value::<T>(&value).map_err(|e| {
            SchemaRegError::wire_format(format!(
                "failed to deserialize Avro value into target type: {e}"
            ))
        })
    }
}
