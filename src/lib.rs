//! Standalone schema registry client library.
//!
//! Provides Confluent and AWS Glue Schema Registry integration with:
//! - Wire format encode/decode (Confluent 5-byte header, AWS Glue 18-byte header)
//! - Subject name strategies
//! - Pluggable async registry clients
//! - In-memory caching with in-flight coalescing
//! - Encoder and decoder adapters

pub mod cache;
pub mod decoder;
pub mod error;
pub mod glue;
pub mod subject;
pub mod traits;
pub mod types;
pub mod wire;

#[cfg(feature = "avro")]
pub mod avro;

#[cfg(feature = "confluent")]
pub mod confluent;

pub use cache::{CachedSchemaRegistry, DEFAULT_MAX_CACHE_ENTRIES};
pub use decoder::{DecodedMessage, SchemaFormat, SchemaMetadata, WireFormatDecoder};
pub use error::{Result, SchemaRegError};
pub use glue::DynGlueSchemaRegistryClient;
pub use glue::{
    CachedGlueSchemaRegistry, DEFAULT_MAX_GLUE_CACHE_ENTRIES, GlueCompression, GlueDataFormat,
    GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId, decode_glue_wire_format,
    decode_glue_wire_format_bytes, encode_glue_wire_format,
};
pub use subject::SubjectNameStrategy;
pub use traits::{
    AnySchemaCache, DynSchemaRegistryClient, SchemaDecoder, SchemaEncoder, SchemaRegistryClient,
};
pub use types::{Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion};
pub use wire::{
    DetectedWireFormat, decode_protobuf_message_indexes, decode_wire_format,
    decode_wire_format_bytes, detect_wire_format, encode_protobuf_wire_format, encode_wire_format,
};

#[cfg(feature = "confluent")]
pub use confluent::{
    ConfluentSchemaEncoder, ConfluentSchemaEncoderBuilder, ConfluentSchemaRegistry,
    ConfluentSchemaRegistryBuilder,
};
