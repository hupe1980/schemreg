//! Standalone schema registry client library.
//!
//! Provides Confluent Schema Registry, Apicurio Registry v3, and AWS Glue
//! Schema Registry integration with:
//! - Wire format encode/decode (Confluent 5-byte header, AWS Glue 18-byte header)
//! - Subject name strategies
//! - Pluggable async registry clients
//! - In-memory caching with in-flight coalescing
//! - Encoder and decoder adapters (Avro, JSON Schema, raw)

pub mod cache;
pub mod decoder;
pub mod error;
pub mod glue;
pub mod subject;
pub mod traits;
pub mod types;
pub mod wire;

#[cfg(any(feature = "confluent", feature = "apicurio"))]
pub(crate) mod http;

#[cfg(feature = "avro")]
pub mod avro;

#[cfg(feature = "confluent")]
pub mod confluent;

#[cfg(feature = "json")]
pub mod json;

#[cfg(feature = "apicurio")]
pub mod apicurio;

#[cfg(feature = "apicurio")]
pub use apicurio::{ApicurioSchemaRegistry, ApicurioSchemaRegistryBuilder};
pub use cache::{CachedSchemaRegistry, DEFAULT_MAX_CACHE_ENTRIES, WarmCacheError};
pub use decoder::{DecodedMessage, SchemaFormat, SchemaMetadata, WireFormatDecoder};
pub use error::{Result, SchemaRegError};
#[cfg(feature = "glue")]
pub use glue::DynGlueSchemaRegistryClient;
pub use glue::{
    CachedGlueSchemaRegistry, DEFAULT_MAX_GLUE_CACHE_ENTRIES, GlueCompression, GlueDataFormat,
    GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId, WarmGlueCacheError,
    decode_glue_wire_format, decode_glue_wire_format_bytes, encode_glue_wire_format,
};
pub use subject::SubjectNameStrategy;
pub use traits::{
    AnySchemaCache, DynSchemaRegistryClient, SchemaDecoder, SchemaEncoder, SchemaRegistryClient,
};
pub use types::{
    ArtifactId, EncodeTarget, Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion,
};
pub use wire::{
    DetectedWireFormat, decode_protobuf_message_indexes, decode_wire_format,
    decode_wire_format_bytes, detect_wire_format, encode_protobuf_wire_format, encode_wire_format,
};

#[cfg(feature = "confluent")]
pub use confluent::{
    ConfluentSchemaEncoder, ConfluentSchemaEncoderBuilder, ConfluentSchemaRegistry,
    ConfluentSchemaRegistryBuilder,
};

#[cfg(feature = "json")]
pub use json::{JsonSchemaDecoder, JsonSchemaEncoder, JsonSchemaEncoderBuilder};
