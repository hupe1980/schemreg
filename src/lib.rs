//! Async-native schema registry client for Kafka wire formats.
//!
//! `schemreg` speaks the binary framings that identify which schema a Kafka
//! record was written with, and pairs them with pluggable, cached, async
//! registry clients:
//!
//! - **Confluent wire format v0** — `0x00` + a 4-byte schema ID. Used by
//!   Confluent Schema Registry, Karapace, Redpanda, and Apicurio's
//!   compatibility API.
//! - **Confluent wire format v1** — `0x01` + a 16-byte schema GUID, added in
//!   Confluent Platform 8. A GUID is a fingerprint of the schema, so it names
//!   the same schema in every registry; an ID does not.
//! - **Schema ID in a Kafka header** — the same prefix carried in
//!   `__key_schema_id` / `__value_schema_id` instead of in the payload.
//! - **AWS Glue** — `0x03` + a compression byte + a 16-byte version UUID.
//!
//! # What is in the box
//!
//! | Layer | Types |
//! |---|---|
//! | Wire format | [`encode_wire_format`], [`decode_wire_format`], [`encode_protobuf_wire_format`], [`decode_protobuf_message_indexes`], [`encode_schema_id_header`], [`decode_schema_id_header`], [`encode_glue_wire_format`], [`decode_glue_wire_format`], [`detect_wire_format`] |
//! | Schema identity | [`SchemaId`], [`SchemaGuid`], [`SchemaKey`] |
//! | Registry clients | [`ConfluentSchemaRegistry`], [`ApicurioSchemaRegistry`], [`AwsGlueSchemaRegistry`] |
//! | Abstraction | [`SchemaRegistryClient`], [`DynSchemaRegistryClient`], [`GlueSchemaRegistryClient`], [`PayloadEncoder`], [`PayloadDecoder`] |
//! | Caching | [`CachedSchemaRegistry`], [`CachedGlueSchemaRegistry`], [`AnySchemaCache`] |
//! | Codecs | [`AvroSchemaEncoder`], [`AvroSchemaDecoder`], [`JsonSchemaEncoder`], [`JsonSchemaDecoder`], [`ProtobufSchemaEncoder`], [`ProtobufSchemaDecoder`] |
//! | Producer policy | [`SchemaResolution`] — register, look up, or follow the subject head; [`Framing`] — v0 or v1 |
//! | Framing-only decode | [`WireFormatDecoder`], [`HeaderFramed`] |
//! | Resilience | [`RetryPolicy`] — retry budget, jittered back-off, `Retry-After` (with `confluent` or `apicurio`) |
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
//! | `avro` | Avro serialise/deserialise via `apache-avro`, with schema-reference resolution |
//! | `json` | JSON Schema validate/serialise via `jsonschema` |
//! | `protobuf` | Protobuf serialise/deserialise via `prost`, with the message-index path derived from the descriptor |
//! | `native-tls-roots` | Trust the platform root store in addition to webpki roots |
//!
//! # Quick start
//!
//! ```rust
//! use schemreg::{SchemaId, decode_wire_format, encode_wire_format};
//!
//! // Producer: frame a serialised payload with its schema ID.
//! let framed = encode_wire_format(42u32, b"serialised-avro-bytes");
//!
//! // Consumer: recover the identifier and the payload, with no copy.
//! let (key, payload) = decode_wire_format(&framed)?;
//! assert_eq!(key.as_id(), Some(SchemaId::new(42)));
//! assert_eq!(payload, b"serialised-avro-bytes");
//! # Ok::<(), schemreg::SchemaRegError>(())
//! ```
//!
//! Decoding returns a [`SchemaKey`] rather than a bare ID because the
//! *producer* chooses the wire format version. Hand it to
//! [`SchemaRegistryClient::get_schema_by_key`] and the right lookup happens
//! either way.
//!
//! # Producer policy
//!
//! Every encoder in the crate takes two settings that decide what happens
//! before a byte is written, and both default to the least surprising answer:
//!
//! ```rust,ignore
//! let encoder = AvroSchemaEncoder::builder()
//!     .registry(cached.clone())
//!     .schema(ORDER_SCHEMA)
//!     .resolution(SchemaResolution::LookupOnly)   // never writes to the registry
//!     .framing(Framing::SchemaGuid)               // wire format v1
//!     .build()?;
//! ```
//!
//! [`SchemaResolution`] answers *which identifier does this subject resolve
//! to*: [`AutoRegister`](SchemaResolution::AutoRegister) (the default, and the
//! one that writes), [`LookupOnly`](SchemaResolution::LookupOnly), or
//! [`UseLatestVersion`](SchemaResolution::UseLatestVersion). [`Framing`] answers
//! *which wire-format version carries it*. Placement — prefix or Kafka header —
//! is a per-call choice between `encode` and `encode_with_header`.
//!
//! # Guarantees and non-guarantees
//!
//! - **Identifier immutability.** [`CachedSchemaRegistry`] caches
//!   `get_schema_by_id` and `get_schema_by_guid` forever: a registry never
//!   reassigns an ID, and a GUID is a fingerprint of the schema itself. It
//!   never caches `get_latest_schema`, which can change at any moment.
//! - **Bounded memory.** Every cache in the crate is bounded and evicts the
//!   oldest entry on overflow. There is no unbounded map anywhere on a
//!   message-driven path.
//! - **Thundering-herd safety.** Concurrent cold misses for the same key issue
//!   exactly one backend request; the rest wait on a channel and are woken with
//!   the shared result — including when the leader task is aborted.
//! - **No blocking.** There are no sync shims, no `block_on`, and no owned
//!   runtime. Every I/O entry point is `async fn` and `Send`.
//! - **Nothing is invented.** A field the registry did not report is `None`,
//!   never a plausible-looking default: [`Schema::id`] is `None` for a
//!   GUID-addressed lookup, because `GET /schemas/guids/{guid}` returns no
//!   numeric ID and none can be derived. The same rule decides what
//!   [`Framing::SchemaGuid`] does against a registry with no GUIDs — a
//!   `NotSupported` error, not a fabricated identifier.
//! - **No outbound requests from schema compilation.** `jsonschema` is built
//!   without `resolve-http`, and JSON Schema `$ref`s are resolved from the
//!   registry's own reference list, so a schema cannot make this crate fetch a
//!   URL it chose.
//! - **Not a Kafka client.** `schemreg` produces and consumes [`bytes::Bytes`];
//!   wiring them to a broker is the caller's job.
//! - **Wire-format conformance is verified, not asserted.** The Confluent
//!   framings are tested against byte sequences produced by the official
//!   Confluent serializers, decoded *and* re-encoded byte-identically — not
//!   only against this crate's own reading of the specification.
//!
//! # Error handling
//!
//! Every fallible entry point returns [`Result<T>`](Result) with a
//! [`SchemaRegError`]. Classify with the predicates rather than by matching on
//! variants or message text:
//!
//! ```rust,no_run
//! # use schemreg::{Result, Schema, SchemaId, SchemaRegistryClient};
//! # use std::sync::Arc;
//! # async fn run<C: SchemaRegistryClient>(registry: C, id: SchemaId) -> Result<()> {
//! match registry.get_schema_by_id(id).await {
//!     Ok(schema)                       => { let _ = schema; }
//!     Err(e) if e.is_not_found()       => { /* permanent: no such schema */ }
//!     Err(e) if e.is_auth_error()      => { /* permanent: rotate credentials */ }
//!     Err(e) if e.is_retryable()       => { /* transient: back off and retry */ }
//!     Err(e)                           => return Err(e),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [`is_retryable`](SchemaRegError::is_retryable) is `true` only for transport
//! failures, HTTP 429, HTTP 5xx, and the registry's own `5xxxx` error codes.
//! Permanent conditions — not-found, auth, incompatible or invalid schema — are
//! never marked retryable, on any backend, so an outer retry loop cannot spin
//! forever.

#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]
#![warn(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

/// Compiles every non-`ignore` Rust block in `README.md` as a doctest.
///
/// A README that no longer compiles is the most common form of documentation
/// rot, and the one readers trust most. `cfg(doctest)` means this costs nothing
/// in a normal build.
///
/// The guides under `site/` are not included this way: they carry TOML front
/// matter for the static-site generator, which rustdoc would render as prose.
/// Their runnable snippets are mirrored from the doctested examples on the
/// items they document, and their links are checked by `zola check` in CI.
///
/// Gated on `confluent` because the quick-start block names that client; CI
/// runs the suite with `--all-features`, so the guard still fires.
#[cfg(all(doctest, feature = "confluent"))]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod cache;
pub(crate) mod cache_inner;
pub mod decoder;
pub mod error;
pub mod glue;
#[cfg(any(
    feature = "confluent",
    feature = "avro",
    feature = "json",
    feature = "protobuf"
))]
pub mod resolver;
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
pub use decoder::{DecodedMessage, SchemaFormat, SchemaMetadata, WireFormatDecoder};
pub use error::{Result, SchemaRegError};
pub use glue::{
    CachedGlueSchemaRegistry, DEFAULT_MAX_GLUE_CACHE_ENTRIES, DynGlueSchemaRegistryClient,
    GlueCompression, GlueDataFormat, GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId,
    WarmGlueCacheError, decode_glue_wire_format, decode_glue_wire_format_borrowed,
    decode_glue_wire_format_bytes, encode_glue_wire_format,
};
#[cfg(any(
    feature = "confluent",
    feature = "avro",
    feature = "json",
    feature = "protobuf"
))]
pub use resolver::{DEFAULT_MAX_SUBJECT_CACHE_ENTRIES, Framing, SchemaResolution};
#[cfg(any(feature = "confluent", feature = "apicurio"))]
pub use retry::{DEFAULT_BASE_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_MAX_RETRIES, RetryPolicy};
pub use subject::{CustomSubjectFn, SubjectNameStrategy};
pub use traits::{
    AnySchemaCache, DynSchemaRegistryClient, PayloadDecoder, PayloadEncoder, SchemaRegistryClient,
};
pub use types::{
    ArtifactId, CompatibilityLevel, EncodeTarget, Schema, SchemaGuid, SchemaId, SchemaKey,
    SchemaReference, SchemaType, SchemaVersion,
};
pub use wire::{
    DetectedWireFormat, HeaderFramed, KEY_SCHEMA_ID_HEADER, MAGIC_BYTE_V0, MAGIC_BYTE_V1,
    PREFIX_LEN_V0, PREFIX_LEN_V1, VALUE_SCHEMA_ID_HEADER, decode_protobuf_message_indexes,
    decode_schema_id_header, decode_wire_format, decode_wire_format_bytes, decode_wire_prefix,
    detect_wire_format, encode_protobuf_wire_format, encode_schema_id_header, encode_wire_format,
    schema_id_header_name,
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
    AvroSchemaDecoder, AvroSchemaDecoderBuilder, AvroSchemaEncoder, AvroSchemaEncoderBuilder,
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
