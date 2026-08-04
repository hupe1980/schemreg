//! Confluent wire-format schema encoder.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::cache_inner::InMemoryCache;
use crate::error::{Result, SchemaRegError};
use crate::subject::SubjectNameStrategy;
use crate::traits::{SchemaEncoder, SchemaRegistryClient};
use crate::types::{EncodeTarget, SchemaId, SchemaReference, SchemaType};
use crate::wire::{encode_protobuf_wire_format, encode_wire_format};

use crate::codec_cache::{DEFAULT_MAX_SUBJECT_CACHE_ENTRIES, subject_resolution_cancelled};

/// A [`SchemaEncoder`] that registers schemas with a Confluent-compatible
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
    protobuf_message_indexes: Vec<i32>,
    /// Bounded, coalescing cache of resolved `subject → schema_id` mappings.
    ///
    /// Shares [`InMemoryCache`] with the registry and codec caches, so the
    /// cancellation and invalidation-race guarantees are identical rather than
    /// re-derived per encoder.
    id_cache: InMemoryCache<String, SchemaId>,
}

impl<C: SchemaRegistryClient> ConfluentSchemaEncoder<C> {
    /// Create a builder for `ConfluentSchemaEncoder`.
    pub fn builder() -> ConfluentSchemaEncoderBuilder<C> {
        ConfluentSchemaEncoderBuilder::new()
    }

    async fn resolve_id(&self, subject: &str) -> Result<SchemaId> {
        let id = self
            .id_cache
            .get_or_fetch(subject.to_string(), || async {
                self.registry
                    .register_schema(subject, &self.schema, self.schema_type, &self.references)
                    .await
                    .map(std::sync::Arc::new)
            })
            .await?;
        Ok(*id)
    }

    /// Return the cached schema ID for the given subject, if already resolved.
    ///
    /// Returns `None` if the subject has not yet been encoded against (schema
    /// registration deferred until first [`encode`](crate::SchemaEncoder::encode)
    /// call).
    pub fn cached_schema_id(&self, subject: &str) -> Option<SchemaId> {
        self.id_cache.get(&subject.to_string()).map(|id| *id)
    }

    /// Number of `subject → schema ID` mappings currently cached.
    pub fn cached_subject_count(&self) -> usize {
        self.id_cache.len()
    }

    /// Forget the cached schema ID for `subject`, forcing the next encode to
    /// re-register. Useful after a subject is deleted and recreated.
    pub fn invalidate_subject(&self, subject: &str) {
        self.id_cache.invalidate(&subject.to_string());
    }
}

impl<C: SchemaRegistryClient> fmt::Debug for ConfluentSchemaEncoder<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfluentSchemaEncoder")
            .field("schema_type", &self.schema_type)
            .field("strategy", &self.strategy)
            .field("cached_subjects", &self.id_cache.len())
            .finish()
    }
}

impl<C: SchemaRegistryClient> SchemaEncoder for ConfluentSchemaEncoder<C> {
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
            let subject = self
                .strategy
                .subject_name(&topic, record_name.as_deref(), target)?;
            let id = self.resolve_id(&subject).await?;
            let framed = if self.schema_type == SchemaType::Protobuf {
                encode_protobuf_wire_format(id, &self.protobuf_message_indexes, &payload)
            } else {
                encode_wire_format(id, &payload)
            };
            Ok(framed)
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
    protobuf_message_indexes: Vec<i32>,
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
            max_subject_cache_entries: DEFAULT_MAX_SUBJECT_CACHE_ENTRIES,
        }
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
    pub fn protobuf_message_indexes(mut self, indexes: Vec<i32>) -> Self {
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
            id_cache: InMemoryCache::new(
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
            .finish()
    }
}
