//! Async-native schema registry client for Kafka wire formats.
//!
//! `schemreg` speaks the two binary framings used on Kafka topics — the
//! Confluent 5-byte header (used by Confluent Schema Registry, Karapace, and
//! Apicurio's compatibility API) and the AWS Glue 18-byte header — and pairs
//! them with pluggable, cached, async registry clients.
//!
//! # What is in the box
//!
//! | Layer | Types |
//! |---|---|
//! | Wire format | [`encode_wire_format`], [`decode_wire_format`], [`encode_protobuf_wire_format`], [`decode_protobuf_message_indexes`], [`encode_glue_wire_format`], [`decode_glue_wire_format`], [`detect_wire_format`] |
//! | Registry clients | [`ConfluentSchemaRegistry`], [`ApicurioSchemaRegistry`], [`AwsGlueSchemaRegistry`] |
//! | Abstraction | [`SchemaRegistryClient`], [`DynSchemaRegistryClient`], [`GlueSchemaRegistryClient`], [`SchemaEncoder`], [`SchemaDecoder`] |
//! | Caching | [`CachedSchemaRegistry`], [`CachedGlueSchemaRegistry`], [`AnySchemaCache`] |
//! | Codecs | [`AvroSchemaEncoder`], [`AvroSchemaDecoder`], [`JsonSchemaEncoder`], [`JsonSchemaDecoder`], [`ProtobufSchemaEncoder`], [`ProtobufSchemaDecoder`] |
//! | Framing-only decode | [`WireFormatDecoder`] |
//!
//! # Cargo features
//!
//! Everything is opt-in; the default feature set is empty and pulls in no
//! transport stack at all.
//!
//! | Feature | Adds |
//! |---|---|
//! | *(none)* | Core types, both wire codecs, traits, caching |
//! | `confluent` | [`ConfluentSchemaRegistry`] HTTP client + [`ConfluentSchemaEncoder`] |
//! | `apicurio` | [`ApicurioSchemaRegistry`] — native Apicurio Registry v3 REST API |
//! | `glue` | [`AwsGlueSchemaRegistry`] via the AWS SDK, plus ZLIB |
//! | `avro` | Avro serialise/deserialise via `apache-avro` |
//! | `json` | JSON Schema validate/serialise via `jsonschema` |
//! | `protobuf` | Protobuf serialise/deserialise via `prost`, with the message-index path derived from the descriptor |
//! | `native-tls-roots` | Trust the platform root store in addition to webpki roots |
//!
//! # Quick start
//!
//! ```rust
//! use schemreg::{decode_wire_format, encode_wire_format};
//!
//! // Producer side: frame a serialised payload with its schema ID.
//! let framed = encode_wire_format(42u32, b"serialised-avro-bytes");
//!
//! // Consumer side: recover the schema ID and the payload with no copy.
//! let (schema_id, payload) = decode_wire_format(&framed)?;
//! assert_eq!(schema_id, 42u32);
//! assert_eq!(payload, b"serialised-avro-bytes");
//! # Ok::<(), schemreg::SchemaRegError>(())
//! ```
//!
//! # Guarantees and non-guarantees
//!
//! - **Schema-ID immutability.** [`CachedSchemaRegistry`] caches
//!   `get_schema_by_id` forever because a registry never reassigns an ID. It
//!   never caches `get_latest_schema`, which can change at any moment.
//! - **Bounded memory.** Every cache in the crate is bounded and evicts the
//!   oldest entry on overflow. There is no unbounded map anywhere on a
//!   message-driven path.
//! - **Thundering-herd safety.** Concurrent cold misses for the same key issue
//!   exactly one backend request; the rest wait on a channel and are woken with
//!   the shared result — including when the leader task is aborted.
//! - **No blocking.** There are no sync shims, no `block_on`, and no owned
//!   runtime. Every I/O entry point is `async fn` and `Send`.
//! - **Not a Kafka client.** `schemreg` produces and consumes `Bytes`; wiring
//!   them to a broker is the caller's job.
//!
//! # Error handling
//!
//! Every fallible entry point returns [`Result<T>`](Result) with a
//! [`SchemaRegError`]. Use [`SchemaRegError::is_retryable`] to decide whether to
//! retry — it is `true` only for transport failures, HTTP 429, and HTTP 5xx.
//! Permanent conditions (not-found, auth, invalid schema) are never marked
//! retryable, on any backend, so an outer retry loop cannot spin forever.

#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]
#![warn(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod cache;
pub(crate) mod cache_inner;
#[cfg(any(
    feature = "confluent",
    feature = "avro",
    feature = "json",
    feature = "protobuf"
))]
pub mod codec_cache;
pub mod decoder;
pub mod error;
pub mod glue;
#[cfg(any(feature = "confluent", feature = "apicurio"))]
pub mod retry;
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

#[cfg(feature = "protobuf")]
pub mod protobuf;

#[cfg(feature = "apicurio")]
pub mod apicurio;

// ── Re-exports ────────────────────────────────────────────────────────────
//
// Every public type reachable from a module is re-exported at the crate root so
// that `use schemreg::X` works uniformly, regardless of which feature provides
// `X`. Module paths remain available for callers who prefer them.

pub use cache::{
    CachedSchemaRegistry, DEFAULT_MAX_CACHE_ENTRIES, WARM_CACHE_CONCURRENCY, WarmCacheError,
};
#[cfg(any(
    feature = "confluent",
    feature = "avro",
    feature = "json",
    feature = "protobuf"
))]
pub use codec_cache::DEFAULT_MAX_SUBJECT_CACHE_ENTRIES;
pub use decoder::{DecodedMessage, SchemaFormat, SchemaMetadata, WireFormatDecoder};
pub use error::{Result, SchemaRegError};
pub use glue::{
    CachedGlueSchemaRegistry, DEFAULT_MAX_GLUE_CACHE_ENTRIES, DynGlueSchemaRegistryClient,
    GlueCompression, GlueDataFormat, GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId,
    WarmGlueCacheError, decode_glue_wire_format, decode_glue_wire_format_borrowed,
    decode_glue_wire_format_bytes, encode_glue_wire_format,
};
#[cfg(any(feature = "confluent", feature = "apicurio"))]
pub use retry::{DEFAULT_BASE_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_MAX_RETRIES, RetryPolicy};
pub use subject::{CustomSubjectFn, SubjectNameStrategy};
pub use traits::{
    AnySchemaCache, DynSchemaRegistryClient, SchemaDecoder, SchemaEncoder, SchemaRegistryClient,
};
pub use types::{
    ArtifactId, CompatibilityLevel, EncodeTarget, Schema, SchemaId, SchemaReference, SchemaType,
    SchemaVersion,
};
pub use wire::{
    DetectedWireFormat, decode_protobuf_message_indexes, decode_wire_format,
    decode_wire_format_bytes, detect_wire_format, encode_protobuf_wire_format, encode_wire_format,
};

#[cfg(feature = "glue")]
pub use glue::{AwsGlueSchemaRegistry, AwsGlueSchemaRegistryBuilder};

#[cfg(feature = "confluent")]
pub use confluent::{
    ConfluentSchemaEncoder, ConfluentSchemaEncoderBuilder, ConfluentSchemaRegistry,
    ConfluentSchemaRegistryBuilder,
};

#[cfg(feature = "apicurio")]
pub use apicurio::{ApicurioSchemaRegistry, ApicurioSchemaRegistryBuilder};

#[cfg(feature = "avro")]
pub use avro::{
    AvroSchemaDecoder, AvroSchemaEncoder, AvroSchemaEncoderBuilder,
    DEFAULT_MAX_AVRO_SCHEMA_CACHE_ENTRIES,
};

#[cfg(feature = "protobuf")]
pub use protobuf::{
    ProtobufSchemaDecoder, ProtobufSchemaEncoder, ProtobufSchemaEncoderBuilder, UnframedProtobuf,
    message_index_path,
};

#[cfg(feature = "json")]
pub use json::{
    DEFAULT_MAX_JSON_VALIDATOR_CACHE_ENTRIES, JsonSchemaDecoder, JsonSchemaEncoder,
    JsonSchemaEncoderBuilder,
};
