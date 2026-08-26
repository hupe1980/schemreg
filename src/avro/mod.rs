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
//! schemreg = { version = "0.6", features = ["avro"] }
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
//! # Schema references
//!
//! A schema that names a type defined in another subject is stored by the
//! registry exactly as written, so it is not parseable on its own. Three
//! schemas can be in that position, and they take their definitions from three
//! different places:
//!
//! | Schema | Definitions come from |
//! |---|---|
//! | The encoder's schema | [`AvroSchemaEncoderBuilder::dependencies`] |
//! | The writer schema a decoder fetched | the registry, walked from `references` |
//! | A decoder's reader schema | [`AvroSchemaDecoderBuilder::reader_dependencies`] |
//!
//! Order does not matter in either list, and anything wrong with a set — a
//! type nothing defines, two definitions of one type, two schemas referencing
//! each other — is a `build()` error naming the type and the list that should
//! hold it.
//!
//! # Examples
//!
//! ## Round-trip with a mock registry
//!
//! ```rust,ignore
//! use apache_avro::types::Value;
//! use schemreg::avro::{AvroSchemaDecoder, AvroSchemaEncoder};
//! use schemreg::{CachedSchemaRegistry, ConfluentSchemaRegistry, EncodeTarget};
//!
//! let registry = CachedSchemaRegistry::new(
//!     ConfluentSchemaRegistry::builder()
//!         .url("http://localhost:8081")
//!         .build()?,
//! );
//!
//! // Encoder: registers the schema once, caches the ID.
//! let encoder = AvroSchemaEncoder::builder()
//!     .registry(registry.clone())
//!     .schema(r#"{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"int"}]}"#)
//!     .build()?;
//!
//! let value = Value::Record(vec![("id".to_string(), Value::Int(42))]);
//! let framed: bytes::Bytes = encoder.encode(value, "orders", EncodeTarget::Value).await?;
//!
//! // Decoder: fetches the schema by ID on first decode, caches it.
//! let decoder = AvroSchemaDecoder::new(registry);
//! let decoded: Value = decoder.decode(framed).await?;
//! println!("{decoded:?}");
//! ```
//!
//! ## Decoding against a reader schema that has references
//!
//! ```rust,ignore
//! let decoder = AvroSchemaDecoder::builder()
//!     .registry(registry)
//!     .reader_schema(CUSTOMER)          // "address": "com.example.Address"
//!     .reader_dependencies([ADDRESS])   // the definition it needs
//!     .build()?;
//! ```

mod schema_set;

use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use self::schema_set::{MAX_REFERENCES, ResolvedAvroSchema, SchemaRole};
use crate::cache_inner::InMemoryCache;
use crate::error::{Result, SchemaRegError};
use crate::resolver::{
    DEFAULT_MAX_SUBJECT_CACHE_ENTRIES, Framing, SchemaResolution, resolve_schema_key,
    subject_resolution_cancelled,
};
use crate::subject::SubjectNameStrategy;
use crate::traits::SchemaRegistryClient;
use crate::types::{EncodeTarget, SchemaId, SchemaKey, SchemaReference, SchemaType};
use crate::wire::{HeaderFramed, decode_wire_format_bytes, encode_wire_format};

/// Default bound on the number of parsed writer schemas an
/// [`AvroSchemaDecoder`] keeps in memory.
///
/// Matches [`DEFAULT_MAX_CACHE_ENTRIES`](crate::DEFAULT_MAX_CACHE_ENTRIES) so a
/// decoder cannot outgrow the registry cache sitting behind it.
pub const DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES: usize = 1000;

// ── Schema helpers ────────────────────────────────────────────────────────

/// Maximum depth the reference resolver will follow.
///
/// A registry can contain a reference cycle — nothing in the Confluent API
/// forbids `A → B → A` — and following one would recurse until the stack ran
/// out. Real schema graphs are a handful of levels deep at most.
const MAX_REFERENCE_DEPTH: usize = 32;

/// Depth-first fetch of every schema transitively referenced by `schema`.
///
/// Confluent stores a referencing schema exactly as written, so a record whose
/// field type is `com.example.Address` is *not* parseable on its own — the
/// referenced definition lives under another subject. Java's client resolves
/// this by fetching the dependency closure and handing the whole set to the
/// parser at once, and `apache-avro`'s `parse_str_with_list` accepts the same
/// shape. Without this, every schema using the `references` mechanism fails to
/// parse with a bare "unknown type" error.
///
/// Visited subjects are tracked so a diamond dependency is fetched once and a
/// cycle terminates instead of recursing forever.
async fn collect_reference_closure<C: SchemaRegistryClient>(
    registry: &C,
    references: &[SchemaReference],
    depth: usize,
    visited: &mut std::collections::HashSet<(String, i32)>,
    out: &mut Vec<String>,
) -> Result<()> {
    if references.is_empty() {
        return Ok(());
    }
    if depth >= MAX_REFERENCE_DEPTH {
        return Err(SchemaRegError::config(format!(
            "Avro schema references nest deeper than {MAX_REFERENCE_DEPTH} levels; \
             the registry likely contains a reference cycle"
        )));
    }

    for reference in references {
        let marker = (reference.subject.clone(), reference.version.as_i32());
        if !visited.insert(marker) {
            continue;
        }
        if out.len() >= MAX_REFERENCES {
            return Err(SchemaRegError::config(format!(
                "Avro schema pulls in more than {MAX_REFERENCES} referenced schemas"
            )));
        }

        let referenced = registry
            .get_schema_by_version(&reference.subject, reference.version)
            .await?;

        // Depth-first, so a dependency's own dependencies land in the set too.
        // The order they land in does not matter: `ResolvedAvroSchema` sorts
        // the set before the codec sees it.
        Box::pin(collect_reference_closure(
            registry,
            &referenced.references,
            depth + 1,
            visited,
            out,
        ))
        .await?;

        out.push(referenced.schema.to_string());
    }
    Ok(())
}

// ── AvroSchemaEncoder ─────────────────────────────────────────────────────

/// Cached subject-resolution entry.
struct EncoderEntry {
    key: SchemaKey,
    /// Parsed schema shared with every encode call — no re-parsing per topic.
    avro_schema: Arc<ResolvedAvroSchema>,
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
    avro_schema: Arc<ResolvedAvroSchema>,
    /// Fully-qualified record name extracted from the schema at build time.
    schema_fullname: Option<String>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    resolution: SchemaResolution,
    framing: Framing,
    /// Bounded, coalescing `subject → (identifier, parsed schema)` cache.
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
                let key = resolve_schema_key(
                    &self.registry,
                    self.resolution,
                    self.framing,
                    subject,
                    &self.schema_str,
                    SchemaType::Avro,
                    &self.references,
                )
                .await?;
                Ok(Arc::new(EncoderEntry {
                    key,
                    avro_schema: Arc::clone(&self.avro_schema),
                }))
            })
            .await
    }

    /// Return the cached identifier for `subject`, if it has been resolved.
    #[must_use]
    pub fn cached_schema_key(&self, subject: &str) -> Option<SchemaKey> {
        self.cache.get(&subject.to_string()).map(|e| e.key)
    }

    /// Return the cached schema ID for `subject`, if it has been resolved
    /// **and** framed as a numeric ID (`None` under [`Framing::SchemaGuid`]).
    #[must_use]
    pub fn cached_schema_id(&self, subject: &str) -> Option<SchemaId> {
        self.cached_schema_key(subject).and_then(SchemaKey::as_id)
    }

    /// Number of `subject → identifier` mappings currently cached.
    pub fn cached_subject_count(&self) -> usize {
        self.cache.len()
    }

    /// Forget the cached identifier for `subject`, forcing the next encode to
    /// resolve it again — the way to pick up a newer version under
    /// [`SchemaResolution::UseLatestVersion`] without a restart.
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
        let raw = entry.avro_schema.serialize(value)?;
        Ok(encode_wire_format(entry.key, &raw))
    }

    /// Serialise `value` with the identifier in a Kafka header instead of in
    /// the payload prefix.
    ///
    /// The returned [`HeaderFramed`] carries the header name, the header value,
    /// and an **unprefixed** Avro payload — write all three, or a consumer
    /// cannot recover the schema. See
    /// [`Framing`] for which identifier lands in the header.
    ///
    /// # Errors
    ///
    /// As [`encode`](Self::encode).
    pub async fn encode_with_header(
        &self,
        value: Value,
        topic: &str,
        target: EncodeTarget,
    ) -> Result<HeaderFramed> {
        let subject = self
            .strategy
            .subject_name(topic, self.schema_fullname.as_deref(), target)?;
        let entry = self.resolve_subject(&subject).await?;
        let raw = entry.avro_schema.serialize(value)?;
        Ok(HeaderFramed::new(target, entry.key, None, Bytes::from(raw)))
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
    dependencies: Vec<String>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    resolution: SchemaResolution,
    framing: Framing,
    max_subject_cache_entries: usize,
}

impl<C: SchemaRegistryClient> AvroSchemaEncoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            schema: None,
            dependencies: Vec::new(),
            strategy: SubjectNameStrategy::TopicName,
            references: Vec::new(),
            resolution: SchemaResolution::default(),
            framing: Framing::default(),
            max_subject_cache_entries: DEFAULT_MAX_SUBJECT_CACHE_ENTRIES,
        }
    }

    /// Choose how a subject resolves to an identifier
    /// (default: [`SchemaResolution::AutoRegister`]).
    ///
    /// The default writes to the registry on the first encode for each subject.
    /// Set [`SchemaResolution::LookupOnly`] wherever schemas are owned by CI or
    /// a migration step.
    pub fn resolution(mut self, resolution: SchemaResolution) -> Self {
        self.resolution = resolution;
        self
    }

    /// Choose the wire-format version (default: [`Framing::SchemaId`], v0).
    pub fn framing(mut self, framing: Framing) -> Self {
        self.framing = framing;
        self
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
    /// Needed when the Avro schema names types defined in other registered
    /// schemas. These are sent to the registry so it can resolve the schema
    /// server-side; pair them with [`dependencies`](Self::dependencies), which
    /// supplies the same definitions for local parsing.
    pub fn references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Supply the JSON of every schema this one references.
    ///
    /// A schema that names an externally defined type is not parseable on its
    /// own, so an encoder for it cannot be built from [`schema`](Self::schema)
    /// alone. Registering it works — the registry resolves
    /// [`references`](Self::references) server-side — but serialising a value
    /// locally needs the definitions in hand.
    ///
    /// **Order does not matter**, and a definition supplied twice is fine as
    /// long as the copies agree. The set must be *complete*, though — a
    /// dependency's own dependencies belong in it too. Anything missing,
    /// contradictory, or circular is an error here, naming the type it could
    /// not resolve.
    ///
    /// A reference must name a type another schema defines at its **top
    /// level**; one defined only inside another schema is rejected, because
    /// Avro resolves such a name inconsistently.
    ///
    /// ```rust,no_run
    /// # use schemreg::{AvroSchemaEncoder, SchemaReference, SchemaRegistryClient};
    /// # fn build<C: SchemaRegistryClient>(registry: C) -> schemreg::Result<()> {
    /// const ADDRESS: &str = r#"{"type":"record","name":"Address","namespace":"com.example",
    ///     "fields":[{"name":"city","type":"string"}]}"#;
    /// const ORDER: &str = r#"{"type":"record","name":"Order","namespace":"com.example",
    ///     "fields":[{"name":"shipTo","type":"com.example.Address"}]}"#;
    ///
    /// let encoder = AvroSchemaEncoder::builder()
    ///     .registry(registry)
    ///     .schema(ORDER)
    ///     .dependencies([ADDRESS])       // in any order
    ///     .references(vec![SchemaReference::new(
    ///         "com.example.Address", "address-value", 1i32,
    ///     )])
    ///     .build()?;
    /// # let _ = encoder;
    /// # Ok(())
    /// # }
    /// ```
    pub fn dependencies(mut self, schemas: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.dependencies = schemas.into_iter().map(Into::into).collect();
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
        let avro_schema =
            ResolvedAvroSchema::parse(&schema_str, &self.dependencies, SchemaRole::WriterLocal)?;
        let fullname = avro_schema.fullname();
        Ok(AvroSchemaEncoder {
            registry,
            schema_str,
            avro_schema: Arc::new(avro_schema),
            schema_fullname: fullname,
            strategy: self.strategy,
            references: self.references,
            resolution: self.resolution,
            framing: self.framing,
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
/// Use [`builder`](Self::builder) to resize it.
///
/// # Schema evolution
///
/// By default the payload is decoded with the **writer** schema, which is what
/// the wire header identifies. Supply a **reader** schema with
/// [`AvroSchemaDecoderBuilder::reader_schema`] to get Avro's full schema
/// resolution — defaulted fields, dropped fields, promoted numeric types —
/// matching the behaviour of the Confluent Java `SpecificAvroDeserializer`.
///
/// # Schema references
///
/// The writer schema's closure is fetched from the registry automatically. A
/// reader schema naming externally defined types takes its definitions from
/// [`AvroSchemaDecoderBuilder::reader_dependencies`].
///
/// # Serde support
///
/// Use [`decode_de`](Self::decode_de) to deserialise directly into a
/// concrete Rust type implementing [`serde::Deserialize`].
pub struct AvroSchemaDecoder<C> {
    registry: C,
    /// Reader schema and its dependencies, when schema resolution is wanted.
    reader_schema: Option<Arc<ResolvedAvroSchema>>,
    schema_cache: InMemoryCache<SchemaKey, ResolvedAvroSchema>,
}

impl<C> std::fmt::Debug for AvroSchemaDecoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemaDecoder")
            .field("has_reader_schema", &self.reader_schema.is_some())
            .field("cached_schemas", &self.schema_cache.len())
            .finish_non_exhaustive()
    }
}

fn avro_schema_lookup_cancelled(key: &SchemaKey) -> SchemaRegError {
    SchemaRegError::invalid_state(format!(
        "Avro schema lookup cancelled before completion for schema {key}"
    ))
}

impl<C: SchemaRegistryClient> AvroSchemaDecoder<C> {
    /// Create a decoder that decodes with the writer schema the wire header
    /// names, with the default cache bound.
    ///
    /// Reach for [`builder`](Self::builder) to configure a reader schema or
    /// resize the cache.
    pub fn new(registry: C) -> Self {
        Self {
            registry,
            reader_schema: None,
            schema_cache: InMemoryCache::new(
                Some(DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES),
                avro_schema_lookup_cancelled,
            ),
        }
    }

    /// Create a builder for `AvroSchemaDecoder`.
    pub fn builder() -> AvroSchemaDecoderBuilder<C> {
        AvroSchemaDecoderBuilder::new()
    }

    /// Whether a reader schema is configured.
    #[must_use]
    pub fn has_reader_schema(&self) -> bool {
        self.reader_schema.is_some()
    }

    /// Number of parsed writer schemas currently cached.
    pub fn cache_len(&self) -> usize {
        self.schema_cache.len()
    }

    /// Drop every cached parsed schema.
    pub fn clear_cache(&self) {
        self.schema_cache.clear();
    }

    /// Fetch and parse the writer schema for `key`, resolving any referenced
    /// schemas it depends on.
    async fn get_avro_schema(&self, key: SchemaKey) -> Result<Arc<ResolvedAvroSchema>> {
        self.schema_cache
            .get_or_fetch(key, || async move {
                let registry_schema = self.registry.get_schema_by_key(key).await?;
                let mut deps = Vec::new();
                let mut visited = std::collections::HashSet::new();
                collect_reference_closure(
                    &self.registry,
                    &registry_schema.references,
                    0,
                    &mut visited,
                    &mut deps,
                )
                .await?;
                ResolvedAvroSchema::parse(
                    &registry_schema.schema,
                    &deps,
                    SchemaRole::WriterRegistry,
                )
                .map(Arc::new)
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
        let (key, payload) = decode_wire_format_bytes(&data)?;
        let writer_schema = self.get_avro_schema(key).await?;
        writer_schema.deserialize(&payload, self.reader_schema.as_deref())
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

// ── AvroSchemaDecoderBuilder ──────────────────────────────────────────────

/// Builder for [`AvroSchemaDecoder`].
///
/// Only [`registry`](Self::registry) is required — everything else has a
/// working default, and `AvroSchemaDecoder::new(registry)` is the shorthand for
/// taking all of them.
///
/// ```rust,no_run
/// # use schemreg::{AvroSchemaDecoder, SchemaRegistryClient};
/// # fn build<C: SchemaRegistryClient>(registry: C) -> schemreg::Result<()> {
/// const ADDRESS: &str = r#"{"type":"record","name":"Address","namespace":"com.example",
///     "fields":[{"name":"city","type":"string"}]}"#;
/// const CUSTOMER: &str = r#"{"type":"record","name":"Customer","namespace":"com.example",
///     "fields":[{"name":"name","type":"string"},
///               {"name":"address","type":"com.example.Address"}]}"#;
///
/// let decoder = AvroSchemaDecoder::builder()
///     .registry(registry)
///     .reader_schema(CUSTOMER)
///     .reader_dependencies([ADDRESS])
///     .build()?;
/// # let _ = decoder;
/// # Ok(())
/// # }
/// ```
pub struct AvroSchemaDecoderBuilder<C> {
    registry: Option<C>,
    reader_schema: Option<ReaderSchemaInput>,
    reader_dependencies: ReaderDependencies,
    max_cache_entries: usize,
}

/// A reader schema as the caller supplied it: JSON, or already parsed.
enum ReaderSchemaInput {
    Json(String),
    Parsed(Box<AvroSchema>),
}

/// Definitions backing the reader schema's references, in whichever form the
/// reader schema itself was given.
enum ReaderDependencies {
    Json(Vec<String>),
    Parsed(Vec<AvroSchema>),
}

impl Default for ReaderDependencies {
    fn default() -> Self {
        Self::Json(Vec::new())
    }
}

impl ReaderDependencies {
    fn is_empty(&self) -> bool {
        match self {
            Self::Json(deps) => deps.is_empty(),
            Self::Parsed(deps) => deps.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Json(deps) => deps.len(),
            Self::Parsed(deps) => deps.len(),
        }
    }
}

impl<C: SchemaRegistryClient> AvroSchemaDecoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            reader_schema: None,
            reader_dependencies: ReaderDependencies::default(),
            max_cache_entries: DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES,
        }
    }

    /// Set the schema registry client (required).
    pub fn registry(mut self, registry: C) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Decode against an explicit **reader** schema, enabling Avro schema
    /// resolution between the writer schema the wire header names and the
    /// schema this consumer was written against.
    ///
    /// Without one, the payload is decoded structurally as the writer schema.
    /// With one, Avro applies its resolution rules: defaults are filled in,
    /// unknown fields are dropped, numeric types are promoted.
    ///
    /// If the schema names types defined elsewhere, pair this with
    /// [`reader_dependencies`](Self::reader_dependencies).
    pub fn reader_schema(mut self, schema: impl Into<String>) -> Self {
        self.reader_schema = Some(ReaderSchemaInput::Json(schema.into()));
        self
    }

    /// Supply the JSON of every schema the reader schema references.
    ///
    /// A writer schema's references come from the registry, which stores them
    /// alongside it. A reader schema is local and the registry has never seen
    /// it, so its definitions have to come from here.
    ///
    /// Order does not matter, and a definition supplied twice is fine as long
    /// as the copies agree.
    pub fn reader_dependencies(
        mut self,
        schemas: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.reader_dependencies =
            ReaderDependencies::Json(schemas.into_iter().map(Into::into).collect());
        self
    }

    /// Set an already-parsed reader schema — what `MyRecord::get_schema()`
    /// returns under `#[derive(AvroSchema)]` — skipping the JSON round-trip.
    ///
    /// Pair with
    /// [`reader_dependencies_parsed`](Self::reader_dependencies_parsed) when it
    /// carries unresolved references.
    ///
    /// There is no encoder counterpart on purpose: an encoder sends its schema
    /// to the registry as text, and re-serialising a parsed schema would
    /// register something subtly different from what the author wrote.
    pub fn reader_schema_parsed(mut self, schema: AvroSchema) -> Self {
        self.reader_schema = Some(ReaderSchemaInput::Parsed(Box::new(schema)));
        self
    }

    /// Supply already-parsed definitions for the reader schema's references.
    ///
    /// As [`reader_dependencies`](Self::reader_dependencies). Duplicates
    /// compare by Avro's parsing canonical form, so docs and attributes do not
    /// make two copies of one schema differ.
    pub fn reader_dependencies_parsed(
        mut self,
        schemas: impl IntoIterator<Item = AvroSchema>,
    ) -> Self {
        self.reader_dependencies = ReaderDependencies::Parsed(schemas.into_iter().collect());
        self
    }

    /// Bound the parsed-writer-schema cache (default:
    /// [`DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES`]).
    ///
    /// The oldest entry is evicted once the bound is reached. Values below 1
    /// are clamped to 1.
    pub fn max_cache_entries(mut self, max_entries: usize) -> Self {
        self.max_cache_entries = max_entries;
        self
    }

    /// Build the decoder.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `registry` was not set, if the reader
    /// schema is not valid Avro, if it references a type nothing supplied, or
    /// if the supplied definitions contradict each other.
    pub fn build(self) -> Result<AvroSchemaDecoder<C>> {
        let registry = self
            .registry
            .ok_or_else(|| SchemaRegError::config("AvroSchemaDecoder: registry must be set"))?;

        let reader_schema = match (self.reader_schema, self.reader_dependencies) {
            (Some(ReaderSchemaInput::Json(schema)), ReaderDependencies::Json(deps)) => Some(
                ResolvedAvroSchema::parse(&schema, &deps, SchemaRole::Reader)?,
            ),
            (Some(ReaderSchemaInput::Parsed(schema)), ReaderDependencies::Parsed(deps)) => Some(
                ResolvedAvroSchema::from_parsed(*schema, deps, SchemaRole::Reader)?,
            ),
            // Mixed forms: the dependencies were set in the other
            // representation from the schema. Nothing sensible to resolve
            // against, and silently ignoring them would decode wrongly.
            (Some(ReaderSchemaInput::Json(schema)), ReaderDependencies::Parsed(deps)) => {
                if !deps.is_empty() {
                    return Err(mixed_reader_forms("reader_dependencies_parsed"));
                }
                Some(ResolvedAvroSchema::parse(&schema, &[], SchemaRole::Reader)?)
            }
            (Some(ReaderSchemaInput::Parsed(schema)), ReaderDependencies::Json(deps)) => {
                if !deps.is_empty() {
                    return Err(mixed_reader_forms("reader_dependencies"));
                }
                Some(ResolvedAvroSchema::from_parsed(
                    *schema,
                    Vec::new(),
                    SchemaRole::Reader,
                )?)
            }
            (None, deps) if !deps.is_empty() => {
                return Err(SchemaRegError::config(
                    "AvroSchemaDecoder: reader dependencies were supplied without a \
                     reader schema; they resolve that schema's references and do \
                     nothing on their own",
                ));
            }
            (None, _) => None,
        };

        Ok(AvroSchemaDecoder {
            registry,
            reader_schema: reader_schema.map(Arc::new),
            schema_cache: InMemoryCache::new(
                Some(self.max_cache_entries.max(1)),
                avro_schema_lookup_cancelled,
            ),
        })
    }
}

fn mixed_reader_forms(setter: &'static str) -> SchemaRegError {
    SchemaRegError::config(format!(
        "AvroSchemaDecoder: the reader schema and `{setter}` were given in different \
         forms; supply both as JSON, or both already parsed"
    ))
}

impl<C: SchemaRegistryClient> Default for AvroSchemaDecoderBuilder<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> std::fmt::Debug for AvroSchemaDecoderBuilder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemaDecoderBuilder")
            .field("registry", &self.registry.is_some())
            .field("reader_schema_set", &self.reader_schema.is_some())
            .field("reader_dependencies", &self.reader_dependencies.len())
            .field("max_cache_entries", &self.max_cache_entries)
            .finish()
    }
}

impl<C> std::fmt::Debug for AvroSchemaEncoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemaEncoder")
            .field("schema_fullname", &self.schema_fullname)
            .field("strategy", &self.strategy)
            .field("references", &self.references.len())
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .field("cached_subjects", &self.cache.len())
            .finish_non_exhaustive()
    }
}

impl<C> std::fmt::Debug for AvroSchemaEncoderBuilder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemaEncoderBuilder")
            .field("registry", &self.registry.is_some())
            .field("schema_set", &self.schema.is_some())
            .field("dependencies", &self.dependencies.len())
            .field("strategy", &self.strategy)
            .field("references", &self.references.len())
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .finish()
    }
}
