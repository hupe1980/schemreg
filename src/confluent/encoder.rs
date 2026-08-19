//! Confluent wire-format schema encoder.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::cache_inner::InMemoryCache;
use crate::error::{Result, SchemaRegError};
use crate::resolver::{
    DEFAULT_MAX_SUBJECT_CACHE_ENTRIES, Framing, SchemaResolution, resolve_schema_key,
    subject_resolution_cancelled,
};
use crate::subject::SubjectNameStrategy;
use crate::traits::{PayloadEncoder, SchemaRegistryClient};
use crate::types::{EncodeTarget, SchemaId, SchemaKey, SchemaReference, SchemaType};
use crate::wire::{HeaderFramed, encode_protobuf_wire_format, encode_wire_format};

/// A [`PayloadEncoder`] that registers schemas with a Confluent-compatible
/// registry and frames encoded payloads with the 5-byte Confluent wire format.
pub struct ConfluentSchemaEncoder<C> {
    registry: C,
    schema: String,
    schema_type: SchemaType,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    /// For Protobuf schemas: the message-index path used when framing payloads.
    ///
    /// Identifies which message type in the `.proto` file the payload belongs to.
    /// The default `[0]` encodes the first top-level message, which covers the
    /// vast majority of real-world schemas. Override when using nested messages
    /// or a non-zero file-level message position.
    protobuf_message_indexes: Vec<u32>,
    /// How a subject is turned into an identifier — register, look up, or
    /// follow the subject's latest version.
    resolution: SchemaResolution,
    /// Which wire-format version the identifier is framed as.
    framing: Framing,
    /// Bounded, coalescing cache of resolved `subject → SchemaKey` mappings.
    ///
    /// Shares [`InMemoryCache`] with the registry and codec caches, so the
    /// cancellation and invalidation-race guarantees are identical rather than
    /// re-derived per encoder.
    key_cache: InMemoryCache<String, SchemaKey>,
}

impl<C: SchemaRegistryClient> ConfluentSchemaEncoder<C> {
    /// Create a builder for `ConfluentSchemaEncoder`.
    pub fn builder() -> ConfluentSchemaEncoderBuilder<C> {
        ConfluentSchemaEncoderBuilder::new()
    }

    async fn resolve_key(&self, subject: &str) -> Result<SchemaKey> {
        let key = self
            .key_cache
            .get_or_fetch(subject.to_string(), || async {
                resolve_schema_key(
                    &self.registry,
                    self.resolution,
                    self.framing,
                    subject,
                    &self.schema,
                    self.schema_type,
                    &self.references,
                )
                .await
                .map(std::sync::Arc::new)
            })
            .await?;
        Ok(*key)
    }

    /// The subject this encoder would use for `topic` and `target`.
    fn subject_for(
        &self,
        topic: &str,
        record_name: Option<&str>,
        target: EncodeTarget,
    ) -> Result<String> {
        self.strategy.subject_name(topic, record_name, target)
    }

    /// Frame `payload` for the identifier `key`, adding the Protobuf
    /// message-index array when the schema type calls for one.
    fn frame(&self, key: SchemaKey, payload: &[u8]) -> Bytes {
        if self.schema_type == SchemaType::Protobuf {
            encode_protobuf_wire_format(key, &self.protobuf_message_indexes, payload)
        } else {
            encode_wire_format(key, payload)
        }
    }

    /// Frame `payload` with the identifier in a Kafka header instead of in the
    /// payload prefix — the placement Confluent Platform 8 introduced.
    ///
    /// The returned [`HeaderFramed`] carries the header name, the header value,
    /// and an **unprefixed** payload. Write all three: a consumer that never
    /// sees the header cannot recover the schema.
    ///
    /// Which identifier goes in the header follows the builder's
    /// [`framing`](ConfluentSchemaEncoderBuilder::framing) setting. Confluent's
    /// own header serializer always emits a GUID, so
    /// [`Framing::SchemaGuid`] is the interoperable choice; an ID is accepted so
    /// header placement also works against a registry that has no GUIDs.
    ///
    /// Also reachable through [`PayloadEncoder`], so an `Arc<dyn PayloadEncoder>`
    /// producer is not confined to prefix framing.
    pub async fn encode_with_header(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        target: EncodeTarget,
    ) -> Result<HeaderFramed> {
        let subject = self.subject_for(topic, record_name, target)?;
        let key = self.resolve_key(&subject).await?;
        let indexes = (self.schema_type == SchemaType::Protobuf)
            .then_some(self.protobuf_message_indexes.as_slice());
        Ok(HeaderFramed::new(target, key, indexes, payload))
    }

    /// Return the cached identifier for the given subject, if already resolved.
    ///
    /// Returns `None` if the subject has not yet been encoded against —
    /// resolution is deferred until the first
    /// [`encode`](crate::PayloadEncoder::encode) call. Never triggers one.
    pub fn cached_schema_key(&self, subject: &str) -> Option<SchemaKey> {
        self.key_cache.get(&subject.to_string()).map(|key| *key)
    }

    /// Return the cached schema ID for the given subject, if already resolved
    /// **and** framed as a numeric ID.
    ///
    /// `None` also when the encoder frames with a GUID — see
    /// [`cached_schema_key`](Self::cached_schema_key).
    pub fn cached_schema_id(&self, subject: &str) -> Option<SchemaId> {
        self.cached_schema_key(subject).and_then(SchemaKey::as_id)
    }

    /// Number of `subject → identifier` mappings currently cached.
    pub fn cached_subject_count(&self) -> usize {
        self.key_cache.len()
    }

    /// Forget the cached identifier for `subject`, forcing the next encode to
    /// resolve it again.
    ///
    /// Useful after a subject is deleted and recreated, and the way to pick up a
    /// new version under [`SchemaResolution::UseLatestVersion`] without a
    /// restart.
    pub fn invalidate_subject(&self, subject: &str) {
        self.key_cache.invalidate(&subject.to_string());
    }
}

impl<C: SchemaRegistryClient> fmt::Debug for ConfluentSchemaEncoder<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfluentSchemaEncoder")
            .field("schema_type", &self.schema_type)
            .field("strategy", &self.strategy)
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .field("cached_subjects", &self.key_cache.len())
            .finish()
    }
}

impl<C: SchemaRegistryClient> PayloadEncoder for ConfluentSchemaEncoder<C> {
    fn encode(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>> {
        let topic = topic.to_string();
        let record_name = record_name.map(str::to_string);
        Box::pin(async move {
            let subject = self.subject_for(&topic, record_name.as_deref(), target)?;
            let key = self.resolve_key(&subject).await?;
            Ok(self.frame(key, &payload))
        })
    }

    fn encode_with_header(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        target: EncodeTarget,
    ) -> Pin<Box<dyn Future<Output = Result<HeaderFramed>> + Send + '_>> {
        let topic = topic.to_string();
        let record_name = record_name.map(str::to_string);
        Box::pin(async move {
            ConfluentSchemaEncoder::encode_with_header(
                self,
                payload,
                &topic,
                record_name.as_deref(),
                target,
            )
            .await
        })
    }
}

/// Builder for [`ConfluentSchemaEncoder`].
pub struct ConfluentSchemaEncoderBuilder<C> {
    registry: Option<C>,
    schema: Option<String>,
    schema_type: SchemaType,
    strategy: SubjectNameStrategy,
    references: Vec<SchemaReference>,
    protobuf_message_indexes: Vec<u32>,
    resolution: SchemaResolution,
    framing: Framing,
    max_subject_cache_entries: usize,
}

impl<C: SchemaRegistryClient> ConfluentSchemaEncoderBuilder<C> {
    fn new() -> Self {
        Self {
            registry: None,
            schema: None,
            schema_type: SchemaType::Avro,
            strategy: SubjectNameStrategy::TopicName,
            references: Vec::new(),
            protobuf_message_indexes: vec![0],
            resolution: SchemaResolution::default(),
            framing: Framing::default(),
            max_subject_cache_entries: DEFAULT_MAX_SUBJECT_CACHE_ENTRIES,
        }
    }

    /// Choose how a subject resolves to an identifier
    /// (default: [`SchemaResolution::AutoRegister`]).
    ///
    /// The default writes to the registry. Set
    /// [`SchemaResolution::LookupOnly`] wherever schemas are owned by CI.
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

    /// Set the registry client (required).
    pub fn registry(mut self, registry: C) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the schema definition string and type (required).
    pub fn schema(mut self, schema: impl Into<String>, schema_type: SchemaType) -> Self {
        self.schema = Some(schema.into());
        self.schema_type = schema_type;
        self
    }

    /// Set the subject name strategy (default: [`SubjectNameStrategy::TopicName`]).
    pub fn strategy(mut self, strategy: SubjectNameStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set schema references (default: empty).
    pub fn references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Set the Protobuf message-index path for wire framing.
    ///
    /// Only used when the schema type is [`SchemaType::Protobuf`]. The default
    /// `[0]` encodes the first top-level message type, which is correct for the
    /// vast majority of schemas. Override when encoding a nested message or a
    /// message at a non-zero position in the `.proto` file.
    pub fn protobuf_message_indexes(mut self, indexes: Vec<u32>) -> Self {
        self.protobuf_message_indexes = indexes;
        self
    }

    /// Build the encoder.
    pub fn build(self) -> Result<ConfluentSchemaEncoder<C>> {
        let registry = self.registry.ok_or_else(|| {
            SchemaRegError::config("ConfluentSchemaEncoder: registry must be set")
        })?;
        let schema = self
            .schema
            .ok_or_else(|| SchemaRegError::config("ConfluentSchemaEncoder: schema must be set"))?;
        Ok(ConfluentSchemaEncoder {
            registry,
            schema,
            schema_type: self.schema_type,
            strategy: self.strategy,
            references: self.references,
            protobuf_message_indexes: self.protobuf_message_indexes,
            resolution: self.resolution,
            framing: self.framing,
            key_cache: InMemoryCache::new(
                Some(self.max_subject_cache_entries.max(1)),
                subject_resolution_cancelled,
            ),
        })
    }
}

impl<C> fmt::Debug for ConfluentSchemaEncoderBuilder<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfluentSchemaEncoderBuilder")
            .field("registry", &self.registry.is_some())
            .field("schema_set", &self.schema.is_some())
            .field("schema_type", &self.schema_type)
            .field("strategy", &self.strategy)
            .field("references", &self.references.len())
            .field("protobuf_message_indexes", &self.protobuf_message_indexes)
            .field("resolution", &self.resolution)
            .field("framing", &self.framing)
            .finish()
    }
}
