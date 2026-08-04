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
//! schemreg = { version = "0.4", features = ["avro"] }
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

use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::cache_inner::InMemoryCache;
use crate::codec_cache::{DEFAULT_MAX_SUBJECT_CACHE_ENTRIES, subject_resolution_cancelled};
use crate::error::{Result, SchemaRegError};
use crate::subject::SubjectNameStrategy;
use crate::traits::SchemaRegistryClient;
use crate::types::{EncodeTarget, SchemaId, SchemaReference, SchemaType};
use crate::wire::{decode_wire_format_bytes, encode_wire_format};

/// Default bound on the number of parsed writer schemas an
/// [`AvroSchemaDecoder`] keeps in memory.
///
/// Matches [`DEFAULT_MAX_CACHE_ENTRIES`](crate::DEFAULT_MAX_CACHE_ENTRIES) so a
/// decoder cannot outgrow the registry cache sitting behind it.
pub const DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES: usize = 1000;

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
/// [`apache_avro::Schema`].  Subsequent encodes hit only a bounded in-memory
/// cache; concurrent first-encodes for one subject coalesce behind a single
/// registration.
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
    /// Bounded, coalescing `subject → (schema_id, parsed schema)` cache.
    cache: InMemoryCache<String, EncoderEntry>,
}

impl<C: SchemaRegistryClient> AvroSchemaEncoder<C> {
    /// Create a builder for `AvroSchemaEncoder`.
    pub fn builder() -> AvroSchemaEncoderBuilder<C> {
        AvroSchemaEncoderBuilder::new()
    }

    /// Resolve subject → cached entry, registering the schema if needed.
    ///
    /// Concurrent callers for the same subject coalesce behind a single leader
    /// registration, preventing duplicate schema-register RPCs under high
    /// concurrency. Cancellation of the leader wakes every waiter with an error.
    async fn resolve_subject(&self, subject: &str) -> Result<Arc<EncoderEntry>> {
        self.cache
            .get_or_fetch(subject.to_string(), || async {
                let schema_id = self
                    .registry
                    .register_schema(
                        subject,
                        &self.schema_str,
                        SchemaType::Avro,
                        &self.references,
                    )
                    .await?;
                Ok(Arc::new(EncoderEntry {
                    schema_id,
                    avro_schema: Arc::clone(&self.avro_schema),
                }))
            })
            .await
    }

    /// Return the cached schema ID for `subject`, if it has been resolved.
    #[must_use]
    pub fn cached_schema_id(&self, subject: &str) -> Option<SchemaId> {
        self.cache.get(&subject.to_string()).map(|e| e.schema_id)
    }

    /// Number of `subject → schema ID` mappings currently cached.
    pub fn cached_subject_count(&self) -> usize {
        self.cache.len()
    }

    /// Forget the cached schema ID for `subject`.
    pub fn invalidate_subject(&self, subject: &str) {
        self.cache.invalidate(&subject.to_string());
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
    max_subject_cache_entries: usize,
}

impl<C: SchemaRegistryClient> AvroSchemaEncoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            schema: None,
            strategy: SubjectNameStrategy::TopicName,
            references: Vec::new(),
            max_subject_cache_entries: DEFAULT_MAX_SUBJECT_CACHE_ENTRIES,
        }
    }

    /// Bound the `subject → schema ID` cache (default:
    /// [`DEFAULT_MAX_SUBJECT_CACHE_ENTRIES`]). Values below 1 are clamped to 1.
    pub fn max_subject_cache_entries(mut self, max_entries: usize) -> Self {
        self.max_subject_cache_entries = max_entries;
        self
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
            cache: InMemoryCache::new(
                Some(self.max_subject_cache_entries.max(1)),
                subject_resolution_cancelled,
            ),
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
/// # Cache behaviour
///
/// The parsed-schema cache is **bounded** (default
/// [`DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES`]) and **coalescing**: when N tasks
/// decode messages carrying a schema ID that is not yet cached, exactly one
/// registry lookup and one schema parse happen; the rest wait for the result.
/// Use [`with_max_cache_entries`](Self::with_max_cache_entries) to resize it.
///
/// # Schema evolution
///
/// By default the payload is decoded with the **writer** schema, which is what
/// the wire header identifies. Supply a **reader** schema with
/// [`with_reader_schema`](Self::with_reader_schema) to get Avro's full schema
/// resolution — defaulted fields, dropped fields, promoted numeric types —
/// matching the behaviour of the Confluent Java `SpecificAvroDeserializer`.
///
/// # Serde support
///
/// Use [`decode_de`](Self::decode_de) to deserialise directly into a
/// concrete Rust type implementing [`serde::Deserialize`].
pub struct AvroSchemaDecoder<C> {
    registry: C,
    /// Optional reader schema used for Avro schema resolution.
    reader_schema: Option<Arc<AvroSchema>>,
    schema_cache: InMemoryCache<SchemaId, AvroSchema>,
}

impl<C> std::fmt::Debug for AvroSchemaDecoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemaDecoder")
            .field("has_reader_schema", &self.reader_schema.is_some())
            .field("cached_schemas", &self.schema_cache.len())
            .finish_non_exhaustive()
    }
}

fn avro_schema_lookup_cancelled(id: &SchemaId) -> SchemaRegError {
    SchemaRegError::invalid_state(format!(
        "Avro schema lookup cancelled before completion for schema id {id}"
    ))
}

impl<C: SchemaRegistryClient> AvroSchemaDecoder<C> {
    /// Create a new `AvroSchemaDecoder` backed by the given registry client.
    pub fn new(registry: C) -> Self {
        Self::with_max_cache_entries(registry, DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES)
    }

    /// Create a decoder whose parsed-schema cache holds at most `max_entries`.
    ///
    /// The oldest entry is evicted once the bound is reached. Values below 1 are
    /// clamped to 1.
    pub fn with_max_cache_entries(registry: C, max_entries: usize) -> Self {
        Self {
            registry,
            reader_schema: None,
            schema_cache: InMemoryCache::new(
                Some(max_entries.max(1)),
                avro_schema_lookup_cancelled,
            ),
        }
    }

    /// Decode against an explicit **reader** schema, enabling Avro schema
    /// resolution between the writer schema named by the wire header and the
    /// schema this consumer was compiled against.
    ///
    /// Without a reader schema, a payload written with a newer writer schema is
    /// decoded structurally as that writer schema — fields the consumer does not
    /// know about appear in the [`Value`], and fields the consumer expects but
    /// the writer dropped are simply absent. With a reader schema, Avro applies
    /// its documented resolution rules instead.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `schema` is not valid Avro schema JSON.
    pub fn with_reader_schema(mut self, schema: &str) -> Result<Self> {
        self.reader_schema = Some(Arc::new(parse_avro_schema(schema)?));
        Ok(self)
    }

    /// Number of parsed writer schemas currently cached.
    pub fn cache_len(&self) -> usize {
        self.schema_cache.len()
    }

    /// Drop every cached parsed schema.
    pub fn clear_cache(&self) {
        self.schema_cache.clear();
    }

    async fn get_avro_schema(&self, id: SchemaId) -> Result<Arc<AvroSchema>> {
        self.schema_cache
            .get_or_fetch(id, || async move {
                let registry_schema = self.registry.get_schema_by_id(id).await?;
                parse_avro_schema(&registry_schema.schema).map(Arc::new)
            })
            .await
    }

    /// Decode a Confluent-framed Avro message to a [`Value`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The wire header is invalid (not Confluent-framed or truncated).
    /// - The schema registry lookup fails.
    /// - The Avro bytes do not conform to the schema.
    /// - A reader schema is configured and the writer schema cannot be resolved to it.
    pub async fn decode(&self, data: Bytes) -> Result<Value> {
        let (schema_id, payload) = decode_wire_format_bytes(&data)?;
        let writer_schema = self.get_avro_schema(schema_id).await?;
        let value = apache_avro::from_avro_datum(
            &writer_schema,
            &mut payload.as_ref(),
            self.reader_schema.as_deref(),
        )
        .map_err(|e| SchemaRegError::wire_format(format!("Avro deserialization failed: {e}")))?;
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

impl<C> std::fmt::Debug for AvroSchemaEncoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemaEncoder")
            .field("schema_fullname", &self.schema_fullname)
            .field("strategy", &self.strategy)
            .field("references", &self.references.len())
            .field("cached_subjects", &self.cache.len())
            .finish_non_exhaustive()
    }
}

impl<C> std::fmt::Debug for AvroSchemaEncoderBuilder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemaEncoderBuilder")
            .field("registry", &self.registry.is_some())
            .field("schema_set", &self.schema.is_some())
            .field("strategy", &self.strategy)
            .field("references", &self.references.len())
            .finish()
    }
}
