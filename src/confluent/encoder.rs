//! Confluent wire-format schema encoder.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;

use crate::error::{Result, SchemaRegError};
use crate::subject::SubjectNameStrategy;
use crate::traits::{SchemaEncoder, SchemaRegistryClient};
use crate::types::{EncodeTarget, SchemaId, SchemaReference, SchemaType};
use crate::wire::{encode_protobuf_wire_format, encode_wire_format};

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
    /// Cache of resolved `subject → schema_id` mappings.
    id_cache: RwLock<HashMap<String, SchemaId>>,
    /// In-flight coalescing: subjects currently being registered.
    in_flight: Mutex<HashMap<String, Vec<oneshot::Sender<Result<SchemaId>>>>>,
}

impl<C: SchemaRegistryClient> ConfluentSchemaEncoder<C> {
    /// Create a builder for `ConfluentSchemaEncoder`.
    pub fn builder() -> ConfluentSchemaEncoderBuilder<C> {
        ConfluentSchemaEncoderBuilder::new()
    }

    async fn resolve_id(&self, subject: &str) -> Result<SchemaId> {
        // Fast path: already cached.
        if let Some(&id) = self.id_cache.read().get(subject) {
            return Ok(id);
        }

        // Slow path: coalescing lock.
        let waiter_rx = {
            let mut in_flight = self.in_flight.lock();
            // Double-check after acquiring the lock.
            if let Some(&id) = self.id_cache.read().get(subject) {
                return Ok(id);
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
                SchemaRegError::invalid_state("schema ID resolution cancelled by the leader")
            })?;
        }

        // We are the leader. Use a drop-guard to cancel waiters on panic/cancellation.
        struct ResolveGuard<'a> {
            in_flight: &'a Mutex<HashMap<String, Vec<oneshot::Sender<Result<SchemaId>>>>>,
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
                            "schema ID resolution cancelled",
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
            .register_schema(subject, &self.schema, self.schema_type, &self.references)
            .await;

        let waiters = self.in_flight.lock().remove(subject).unwrap_or_default();

        match &result {
            Ok(id) => {
                let id = *id;
                self.id_cache.write().insert(subject.to_string(), id);
                for tx in waiters {
                    let _ = tx.send(Ok(id));
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

    /// Return the cached schema ID for the given subject, if already resolved.
    ///
    /// Returns `None` if the subject has not yet been encoded against (schema
    /// registration deferred until first [`encode`](crate::SchemaEncoder::encode)
    /// call).
    pub fn cached_schema_id(&self, subject: &str) -> Option<SchemaId> {
        self.id_cache.read().get(subject).copied()
    }
}

impl<C: SchemaRegistryClient> fmt::Debug for ConfluentSchemaEncoder<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfluentSchemaEncoder")
            .field("schema_type", &self.schema_type)
            .field("strategy", &self.strategy)
            .field("cached_subjects", &self.id_cache.read().len())
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
        }
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
            id_cache: RwLock::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        })
    }
}
