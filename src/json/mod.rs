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
//! schemreg = { version = "0.6", features = ["json"] }
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

use std::sync::Arc;

use bytes::Bytes;
use jsonschema::Validator;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

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

/// Default bound on the number of compiled JSON Schema validators a
/// [`JsonSchemaDecoder`] keeps in memory.
pub const DEFAULT_MAX_JSON_VALIDATOR_CACHE_ENTRIES: usize = 1000;

/// Maximum depth the reference resolver will follow.
///
/// Nothing in the Confluent API forbids a reference cycle `A → B → A`, and
/// following one would recurse until the stack ran out.
const MAX_REFERENCE_DEPTH: usize = 32;

/// Maximum number of referenced schemas resolved for a single root schema.
const MAX_REFERENCES: usize = 256;

// ── Reference resolution ──────────────────────────────────────────────────

/// The referenced schemas a root schema needs, indexed by the `$ref` strings
/// that can reach them.
///
/// Confluent stores a referencing JSON Schema exactly as written, so a document
/// whose `$ref` points at `https://example.com/address.json` is **not**
/// compilable on its own — the target lives under another subject.
/// [`SchemaReference::name`](crate::SchemaReference::name) is that `$ref`
/// string, so the reference list is the map from `$ref` to subject.
///
/// `jsonschema` resolves a relative `$ref` against a base URI before asking the
/// retriever, so `"address.json"` arrives as `json-schema:///address.json`.
/// Every dependency is therefore indexed twice — under its declared name and
/// under that name's final path segment — and lookup tries the full URI first,
/// then its final segment.
#[derive(Debug, Default)]
struct ReferenceSet {
    by_key: std::collections::HashMap<String, Value>,
}

impl ReferenceSet {
    fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Index `schema` under `name` and under `name`'s final path segment.
    ///
    /// The same document arriving twice — a diamond, or one schema registered
    /// under two subjects — is one document. Two *different* documents under
    /// one name are rejected: a `$ref` resolves to exactly one thing, and
    /// keeping whichever landed last would validate against a document the
    /// author never named.
    ///
    /// The final-segment index is different, and keeps first-wins: distinct
    /// URIs routinely end in the same file name, and that collision is
    /// expected rather than a contradiction.
    fn insert(&mut self, name: &str, schema: Value) -> Result<()> {
        if self.by_key.len() >= MAX_REFERENCES * 2 {
            return Err(SchemaRegError::config(format!(
                "JSON Schema pulls in more than {MAX_REFERENCES} referenced schemas"
            )));
        }
        if let Some(tail) = last_segment(name)
            && tail != name
        {
            self.by_key
                .entry(tail.to_string())
                .or_insert(schema.clone());
        }
        match self.by_key.entry(name.to_string()) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                if *existing.get() != schema {
                    return Err(SchemaRegError::config(format!(
                        "two different documents were supplied for the JSON Schema reference \
                         '{name}'; a `$ref` resolves to one document, so the set must agree on \
                         it (a reference closure spanning two versions of the same subject does \
                         not)"
                    )));
                }
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(schema);
            }
        }
        Ok(())
    }

    fn lookup(&self, uri: &str) -> Option<&Value> {
        self.by_key
            .get(uri)
            .or_else(|| self.by_key.get(last_segment(uri)?))
    }
}

/// The part of `s` after the last `/`, if there is one.
fn last_segment(s: &str) -> Option<&str> {
    s.rsplit_once('/').map(|(_, tail)| tail)
}

impl jsonschema::Retrieve for ReferenceSet {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let key = uri.to_string();
        self.lookup(&key).cloned().ok_or_else(|| {
            format!(
                "JSON Schema reference '{key}' was not supplied. Register it under a subject \
                 and name it in the schema's `references` list, or pass it to \
                 JsonSchemaEncoderBuilder::dependencies"
            )
            .into()
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Compile a JSON Schema together with the schemas it references.
///
/// Uses the most recent draft supported by the document's `$schema` keyword
/// (falling back to draft 2020-12 when absent). Errors are mapped to
/// [`SchemaRegError::config`].
///
/// Remote retrieval stays impossible: `jsonschema` is built without
/// `resolve-http`, and the retriever installed here only ever answers from
/// `refs`. A `$ref` to a URL nobody supplied is a compile error, not an
/// outbound request.
fn compile_schema_with_refs(schema_str: &str, refs: ReferenceSet) -> Result<Validator> {
    let schema: Value = serde_json::from_str(schema_str)
        .map_err(|e| SchemaRegError::config(format!("invalid JSON Schema (parse): {e}")))?;
    if refs.is_empty() {
        return jsonschema::validator_for(&schema)
            .map_err(|e| SchemaRegError::config(format!("invalid JSON Schema (compile): {e}")));
    }
    jsonschema::options()
        .with_retriever(refs)
        .build(&schema)
        .map_err(|e| {
            SchemaRegError::config(format!(
                "invalid JSON Schema (compile, with referenced schemas): {e}"
            ))
        })
}

/// Parse each `(name, schema JSON)` pair into a [`ReferenceSet`].
fn reference_set_from_pairs(deps: &[(String, String)]) -> Result<ReferenceSet> {
    let mut set = ReferenceSet::default();
    for (name, schema) in deps {
        let value: Value = serde_json::from_str(schema).map_err(|e| {
            SchemaRegError::config(format!(
                "invalid JSON Schema for referenced schema '{name}': {e}"
            ))
        })?;
        set.insert(name, value)?;
    }
    Ok(set)
}

/// Depth-first fetch of every schema transitively referenced by `references`.
///
/// Mirrors the Avro resolver: visited `(subject, version)` pairs are tracked, so
/// a diamond dependency is fetched once and a cycle terminates instead of
/// recursing forever.
async fn collect_reference_closure<C: SchemaRegistryClient>(
    registry: &C,
    references: &[SchemaReference],
    depth: usize,
    visited: &mut std::collections::HashSet<(String, i32)>,
    out: &mut ReferenceSet,
) -> Result<()> {
    if references.is_empty() {
        return Ok(());
    }
    if depth >= MAX_REFERENCE_DEPTH {
        return Err(SchemaRegError::config(format!(
            "JSON Schema references nest deeper than {MAX_REFERENCE_DEPTH} levels; \
             the registry likely contains a reference cycle"
        )));
    }

    for reference in references {
        if !visited.insert((reference.subject.clone(), reference.version.as_i32())) {
            continue;
        }

        let referenced = registry
            .get_schema_by_version(&reference.subject, reference.version)
            .await?;

        Box::pin(collect_reference_closure(
            registry,
            &referenced.references,
            depth + 1,
            visited,
            out,
        ))
        .await?;

        let value: Value = serde_json::from_str(&referenced.schema).map_err(|e| {
            SchemaRegError::config(format!(
                "referenced schema '{}' (subject '{}') is not valid JSON: {e}",
                reference.name, reference.subject
            ))
        })?;
        out.insert(&reference.name, value)?;
    }
    Ok(())
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
    key: SchemaKey,
    /// Compiled validator shared across encode calls — no re-compilation.
    validator: Arc<Validator>,
}

/// Serialises [`serde_json::Value`] (or any `serde::Serialize` type) to
/// Confluent-framed JSON bytes.
///
/// On the first call for a given subject the encoder registers the JSON Schema
/// with the registry, caches the assigned schema ID, and caches the compiled
/// [`jsonschema::Validator`].  Subsequent encodes hit only a bounded in-memory
/// cache — no registry call, no re-compilation. Concurrent first-encodes for
/// one subject coalesce behind a single registration.
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
    resolution: SchemaResolution,
    framing: Framing,
    /// Bounded, coalescing `subject → (identifier, compiled validator)` cache.
    cache: InMemoryCache<String, EncoderEntry>,
}

impl<C: std::fmt::Debug> std::fmt::Debug for JsonSchemaEncoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSchemaEncoder")
            .field("registry", &self.registry)
            .field("record_name", &self.record_name)
            .field("strategy", &self.strategy)
            .field("validate_on_encode", &self.validate_on_encode)
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .finish_non_exhaustive()
    }
}

impl<C: SchemaRegistryClient> JsonSchemaEncoder<C> {
    /// Create a builder for `JsonSchemaEncoder`.
    pub fn builder() -> JsonSchemaEncoderBuilder<C> {
        JsonSchemaEncoderBuilder::new()
    }

    /// Return the cached identifier for `subject`, if it has been resolved.
    ///
    /// Returns `None` for subjects not yet encountered or not yet resolved.
    /// Useful for observability without triggering a registration.
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
                    SchemaType::Json,
                    &self.references,
                )
                .await?;
                Ok(Arc::new(EncoderEntry {
                    key,
                    validator: Arc::clone(&self.validator),
                }))
            })
            .await
    }

    /// Serialise `value` and its subject's identifier, validating and framing
    /// exactly as [`encode`](Self::encode) does.
    async fn serialise(
        &self,
        value: &Value,
        topic: &str,
        target: EncodeTarget,
    ) -> Result<(SchemaKey, Vec<u8>)> {
        let subject = self
            .strategy
            .subject_name(topic, self.record_name.as_deref(), target)?;
        let entry = self.resolve_subject(&subject).await?;

        if self.validate_on_encode {
            validate(&entry.validator, value)?;
        }

        let raw = serde_json::to_vec(value)
            .map_err(|e| SchemaRegError::wire_format(format!("JSON serialization failed: {e}")))?;
        Ok((entry.key, raw))
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
        let (key, raw) = self.serialise(value, topic, target).await?;
        Ok(encode_wire_format(key, &raw))
    }

    /// Serialise `value` with the identifier in a Kafka header instead of in
    /// the payload prefix.
    ///
    /// The returned [`HeaderFramed`] carries the header name, the header value,
    /// and an **unprefixed** JSON payload — write all three, or a consumer
    /// cannot recover the schema.
    ///
    /// # Errors
    ///
    /// As [`encode`](Self::encode).
    pub async fn encode_with_header(
        &self,
        value: &Value,
        topic: &str,
        target: EncodeTarget,
    ) -> Result<HeaderFramed> {
        let (key, raw) = self.serialise(value, topic, target).await?;
        Ok(HeaderFramed::new(target, key, None, Bytes::from(raw)))
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
    dependencies: Vec<(String, String)>,
    record_name: Option<String>,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    validate_on_encode: bool,
    resolution: SchemaResolution,
    framing: Framing,
    max_subject_cache_entries: usize,
}

impl<C: SchemaRegistryClient> JsonSchemaEncoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            schema: None,
            dependencies: Vec::new(),
            record_name: None,
            strategy: SubjectNameStrategy::TopicName,
            references: Vec::new(),
            validate_on_encode: true,
            resolution: SchemaResolution::default(),
            framing: Framing::default(),
            max_subject_cache_entries: DEFAULT_MAX_SUBJECT_CACHE_ENTRIES,
        }
    }

    /// Choose how a subject resolves to an identifier
    /// (default: [`SchemaResolution::AutoRegister`]).
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
    ///
    /// Needed when the JSON Schema `$ref`s a document registered under another
    /// subject. These are sent to the registry so it can resolve them
    /// server-side; pair them with [`dependencies`](Self::dependencies), which
    /// supplies the same documents for local compilation.
    pub fn references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Supply the JSON of every schema this one `$ref`s, as `(name, schema)`
    /// pairs.
    ///
    /// A document with an external `$ref` cannot be compiled from
    /// [`schema`](Self::schema) alone, so validation on encode would fail at
    /// `build()` without this. `name` is the `$ref` string as it appears in the
    /// schema — the same value that goes in
    /// [`SchemaReference::name`](crate::SchemaReference::name).
    ///
    /// Order does not matter: unlike Avro, JSON Schema resolves by URI rather
    /// than by declaration order.
    ///
    /// ```rust,no_run
    /// # use schemreg::{JsonSchemaEncoder, SchemaReference, SchemaRegistryClient};
    /// # fn build<C: SchemaRegistryClient>(registry: C) -> schemreg::Result<()> {
    /// const ADDRESS: &str = r#"{"type":"object","properties":{"city":{"type":"string"}}}"#;
    /// const ORDER: &str = r#"{"type":"object","properties":{
    ///     "shipTo":{"$ref":"https://example.com/address.json"}}}"#;
    ///
    /// let encoder = JsonSchemaEncoder::builder()
    ///     .registry(registry)
    ///     .schema(ORDER)
    ///     .dependencies([("https://example.com/address.json", ADDRESS)])
    ///     .references(vec![SchemaReference::new(
    ///         "https://example.com/address.json", "address-value", 1i32,
    ///     )])
    ///     .build()?;
    /// # let _ = encoder;
    /// # Ok(())
    /// # }
    /// ```
    pub fn dependencies(
        mut self,
        schemas: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.dependencies = schemas
            .into_iter()
            .map(|(name, schema)| (name.into(), schema.into()))
            .collect();
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
        let validator = Arc::new(compile_schema_with_refs(
            &schema_str,
            reference_set_from_pairs(&self.dependencies)?,
        )?);
        Ok(JsonSchemaEncoder {
            registry,
            schema_str,
            validator,
            record_name: self.record_name,
            strategy: self.strategy,
            references: self.references,
            validate_on_encode: self.validate_on_encode,
            resolution: self.resolution,
            framing: self.framing,
            cache: InMemoryCache::new(
                Some(self.max_subject_cache_entries.max(1)),
                subject_resolution_cancelled,
            ),
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
/// # Schema references
///
/// A schema whose `$ref` points at another subject is stored by the registry
/// exactly as written and is not compilable alone. When validation is enabled,
/// the decoder fetches the transitive closure of
/// [`Schema::references`](crate::Schema::references) and compiles the set
/// together. A diamond is fetched once per subject, and a cycle terminates
/// instead of recursing — a recursive `$ref` is legal JSON Schema, so it is
/// compiled rather than rejected. This happens once per schema identifier and
/// is cached alongside the validator.
///
/// # Serde support
///
/// Use [`decode_de`](Self::decode_de) to deserialise directly into a
/// concrete Rust type implementing [`serde::Deserialize`].
pub struct JsonSchemaDecoder<C> {
    registry: C,
    validate_on_decode: bool,
    schema_cache: InMemoryCache<SchemaKey, Validator>,
}

impl<C> std::fmt::Debug for JsonSchemaDecoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSchemaDecoder")
            .field("validate_on_decode", &self.validate_on_decode)
            .field("cached_validators", &self.schema_cache.len())
            .finish_non_exhaustive()
    }
}

fn json_validator_lookup_cancelled(key: &SchemaKey) -> SchemaRegError {
    SchemaRegError::invalid_state(format!(
        "JSON Schema validator lookup cancelled before completion for schema {key}"
    ))
}

impl<C: SchemaRegistryClient> JsonSchemaDecoder<C> {
    /// Create a new `JsonSchemaDecoder` backed by the given registry client.
    ///
    /// Validation on decode is **disabled** by default. Use [`with_validation`]
    /// to enable it.
    ///
    /// The compiled-validator cache is bounded to
    /// [`DEFAULT_MAX_JSON_VALIDATOR_CACHE_ENTRIES`] entries and coalesces
    /// concurrent cold misses so a burst of consumers never compiles the same
    /// schema more than once.
    ///
    /// [`with_validation`]: Self::with_validation
    pub fn new(registry: C) -> Self {
        Self::build(registry, false, DEFAULT_MAX_JSON_VALIDATOR_CACHE_ENTRIES)
    }

    /// Create a new `JsonSchemaDecoder` with validation on decode enabled.
    pub fn with_validation(registry: C) -> Self {
        Self::build(registry, true, DEFAULT_MAX_JSON_VALIDATOR_CACHE_ENTRIES)
    }

    /// Set the maximum number of compiled validators held in memory.
    ///
    /// The oldest entry is evicted once the bound is reached. Values below 1 are
    /// clamped to 1. Only meaningful when validation on decode is enabled — with
    /// validation off, no validator is ever compiled or cached.
    pub fn with_max_cache_entries(self, max_entries: usize) -> Self {
        Self::build(self.registry, self.validate_on_decode, max_entries)
    }

    fn build(registry: C, validate_on_decode: bool, max_entries: usize) -> Self {
        Self {
            registry,
            validate_on_decode,
            schema_cache: InMemoryCache::new(
                Some(max_entries.max(1)),
                json_validator_lookup_cancelled,
            ),
        }
    }

    /// Number of compiled validators currently cached.
    pub fn cache_len(&self) -> usize {
        self.schema_cache.len()
    }

    /// Drop every cached compiled validator.
    pub fn clear_cache(&self) {
        self.schema_cache.clear();
    }

    async fn get_validator(&self, key: SchemaKey) -> Result<Arc<Validator>> {
        self.schema_cache
            .get_or_fetch(key, || async move {
                let registry_schema = self.registry.get_schema_by_key(key).await?;
                let mut refs = ReferenceSet::default();
                let mut visited = std::collections::HashSet::new();
                collect_reference_closure(
                    &self.registry,
                    &registry_schema.references,
                    0,
                    &mut visited,
                    &mut refs,
                )
                .await?;
                compile_schema_with_refs(&registry_schema.schema, refs).map(Arc::new)
            })
            .await
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
        let (key, payload) = decode_wire_format_bytes(&data)?;

        let value: Value = serde_json::from_slice(&payload).map_err(|e| {
            SchemaRegError::wire_format(format!("JSON deserialisation failed: {e}"))
        })?;

        if self.validate_on_decode {
            let validator = self.get_validator(key).await?;
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

impl<C> std::fmt::Debug for JsonSchemaEncoderBuilder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSchemaEncoderBuilder")
            .field("registry", &self.registry.is_some())
            .field("schema_set", &self.schema.is_some())
            .field("dependencies", &self.dependencies.len())
            .field("record_name", &self.record_name)
            .field("strategy", &self.strategy)
            .field("references", &self.references.len())
            .field("validate_on_encode", &self.validate_on_encode)
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .finish()
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
