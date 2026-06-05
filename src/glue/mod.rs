//! AWS Glue Schema Registry wire format and client types.
//!
//! This module is always available — no extra dependencies required.
//! It provides the Glue wire format encoder/decoder, types, a pluggable
//! client trait, and a caching wrapper.
//!
//! When the `glue` feature is enabled, [`AwsGlueSchemaRegistry`] provides
//! a ready-made AWS SDK client.
//!
//! # Wire Format
//!
//! The AWS Glue wire format uses an 18-byte header:
//!
//! ```text
//! ┌──────────┬─────────────┬──────────────────────┬──────────────────┐
//! │ 0x03 (1B)│ Compr. (1B) │ Schema Version UUID  │ Payload (N bytes)│
//! │          │             │      (16B, BE)        │                  │
//! └──────────┴─────────────┴──────────────────────┴──────────────────┘
//! ```
//!
//! - **Byte 0**: Header version byte (`0x03`)
//! - **Byte 1**: Compression indicator (`0x00` = none, `0x05` = ZLIB)
//! - **Bytes 2–17**: Schema version ID as a 128-bit UUID (big-endian)
//! - **Bytes 18+**: Payload (ZLIB-compressed if byte 1 is `0x05`)

#[cfg(feature = "glue")]
mod client;

#[cfg(feature = "glue")]
pub use client::{AwsGlueSchemaRegistry, AwsGlueSchemaRegistryBuilder};

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
#[cfg(feature = "glue")]
use flate2::Compression;
#[cfg(feature = "glue")]
use flate2::read::ZlibDecoder;
#[cfg(feature = "glue")]
use flate2::write::ZlibEncoder;
#[cfg(feature = "glue")]
use std::io::{Read, Write};

use crate::cache_inner::InMemoryCache;
use crate::error::{Result, SchemaRegError};
use crate::traits::AnySchemaCache;

// `futures` is an unconditional dependency of schemreg (used for join_all in warm_cache).

// ── Constants ────────────────────────────────────────────────────────────

/// Header version byte for the AWS Glue wire format.
pub(crate) const GLUE_HEADER_VERSION_BYTE: u8 = 0x03;

/// Compression indicator: no compression.
pub(crate) const GLUE_COMPRESSION_NONE_BYTE: u8 = 0x00;

/// Compression indicator: ZLIB compression.
pub(crate) const GLUE_COMPRESSION_ZLIB_BYTE: u8 = 0x05;

/// Size of the Glue wire format header (version + compression + 16-byte UUID).
pub(crate) const GLUE_HEADER_SIZE: usize = 18;

/// Size of the UUID field in the Glue wire format.
const UUID_SIZE: usize = 16;

// ── Types ────────────────────────────────────────────────────────────────

/// Error returned when [`CachedGlueSchemaRegistry::warm_cache`] fails to
/// load one or more version IDs.
///
/// Successfully fetched IDs **are** cached. Callers can inspect
/// [`failures`](WarmGlueCacheError::failures) to decide whether to retry.
#[derive(Debug, Clone)]
pub struct WarmGlueCacheError {
    /// The version IDs that could not be fetched, along with the per-ID error.
    pub failures: Vec<(GlueSchemaVersionId, SchemaRegError)>,
}

impl fmt::Display for WarmGlueCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "warm_cache failed for {} Glue schema version ID(s):",
            self.failures.len()
        )?;
        for (id, e) in &self.failures {
            write!(f, " id {id}: {e};")?;
        }
        Ok(())
    }
}

impl std::error::Error for WarmGlueCacheError {}

/// Schema version identifier used by the AWS Glue Schema Registry.
///
/// Internally a 128-bit UUID stored as big-endian bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct GlueSchemaVersionId([u8; UUID_SIZE]);

impl GlueSchemaVersionId {
    /// Create from raw big-endian UUID bytes.
    pub fn from_bytes(bytes: [u8; UUID_SIZE]) -> Self {
        Self(bytes)
    }

    /// Return the raw big-endian UUID bytes.
    pub fn as_bytes(&self) -> &[u8; UUID_SIZE] {
        &self.0
    }
}

impl From<[u8; UUID_SIZE]> for GlueSchemaVersionId {
    fn from(bytes: [u8; UUID_SIZE]) -> Self {
        Self(bytes)
    }
}

impl From<GlueSchemaVersionId> for [u8; UUID_SIZE] {
    fn from(id: GlueSchemaVersionId) -> Self {
        id.0
    }
}

impl fmt::Display for GlueSchemaVersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = &self.0;
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7],
            b[8],
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15]
        )
    }
}

impl fmt::Debug for GlueSchemaVersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GlueSchemaVersionId({self})")
    }
}

impl FromStr for GlueSchemaVersionId {
    type Err = SchemaRegError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let bytes = s.as_bytes();
        if bytes.len() != 36 {
            return Err(SchemaRegError::invalid_state(format!(
                "invalid UUID: expected 36 characters, got {}",
                bytes.len()
            )));
        }
        if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
            return Err(SchemaRegError::invalid_state(
                "invalid UUID format: expected dashes at positions 8, 13, 18, 23",
            ));
        }
        let hex_positions: [usize; UUID_SIZE] =
            [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
        let mut uuid_bytes = [0u8; UUID_SIZE];
        for (i, &pos) in hex_positions.iter().enumerate() {
            uuid_bytes[i] = parse_hex_byte(bytes[pos], bytes[pos + 1]).ok_or_else(|| {
                SchemaRegError::invalid_state("invalid UUID: non-hexadecimal character")
            })?;
        }
        Ok(Self(uuid_bytes))
    }
}

fn parse_hex_byte(hi: u8, lo: u8) -> Option<u8> {
    Some((hex_digit(hi)? << 4) | hex_digit(lo)?)
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn glue_schema_lookup_cancelled_error(id: GlueSchemaVersionId) -> SchemaRegError {
    SchemaRegError::invalid_state(format!(
        "glue schema lookup cancelled before completion for id {id}"
    ))
}

/// Compression type used in the AWS Glue wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum GlueCompression {
    /// No compression (wire format byte `0x00`).
    #[default]
    None,
    /// ZLIB compression (wire format byte `0x05`).
    Zlib,
}

impl GlueCompression {
    /// Return the canonical uppercase name (`"NONE"`, `"ZLIB"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Zlib => "ZLIB",
        }
    }
}

impl fmt::Display for GlueCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Data format supported by the AWS Glue Schema Registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlueDataFormat {
    /// Apache Avro.
    Avro,
    /// JSON Schema.
    Json,
    /// Protocol Buffers.
    Protobuf,
}

impl GlueDataFormat {
    /// Return the canonical uppercase name (`"AVRO"`, `"JSON"`, `"PROTOBUF"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }
}

impl fmt::Display for GlueDataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GlueDataFormat {
    type Err = SchemaRegError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("AVRO") {
            Ok(Self::Avro)
        } else if s.eq_ignore_ascii_case("JSON") {
            Ok(Self::Json)
        } else if s.eq_ignore_ascii_case("PROTOBUF") {
            Ok(Self::Protobuf)
        } else {
            Err(SchemaRegError::invalid_state(format!(
                "unknown Glue data format: '{s}'"
            )))
        }
    }
}

/// A schema retrieved from the AWS Glue Schema Registry.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlueSchema {
    /// Globally unique schema version ID (UUID).
    pub schema_version_id: GlueSchemaVersionId,
    /// Data format (Avro, JSON, Protobuf).
    pub data_format: GlueDataFormat,
    /// Schema definition string.
    ///
    /// Stored as a reference-counted string so that cloning an `Arc<GlueSchema>`
    /// from a cache hit does not duplicate the schema bytes.
    pub schema_definition: Arc<str>,
    /// Schema ARN (present when fetched from the registry).
    pub schema_arn: Option<String>,
    /// Schema version number within its schema (1-based, present when fetched).
    pub version_number: Option<i64>,
}

impl GlueSchema {
    /// Create a schema with the given version ID, data format, and definition.
    ///
    /// Accepts any type that converts to `Arc<str>`: `&str`, `String`,
    /// or an already-allocated `Arc<str>`.
    pub fn new(
        schema_version_id: GlueSchemaVersionId,
        data_format: GlueDataFormat,
        schema_definition: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            schema_version_id,
            data_format,
            schema_definition: schema_definition.into(),
            schema_arn: None,
            version_number: None,
        }
    }

    /// Set the schema ARN and version number.
    pub fn with_metadata(mut self, schema_arn: impl Into<String>, version_number: i64) -> Self {
        self.schema_arn = Some(schema_arn.into());
        self.version_number = Some(version_number);
        self
    }
}

// ── Wire format ──────────────────────────────────────────────────────────

/// Encode a payload with the AWS Glue wire format header.
pub fn encode_glue_wire_format(
    schema_version_id: GlueSchemaVersionId,
    payload: &[u8],
    compression: GlueCompression,
) -> Result<Bytes> {
    let compressed;
    let (compression_byte, payload_bytes): (u8, &[u8]) = match compression {
        GlueCompression::None => (GLUE_COMPRESSION_NONE_BYTE, payload),
        GlueCompression::Zlib => {
            compressed = compress_zlib(payload)?;
            (GLUE_COMPRESSION_ZLIB_BYTE, &compressed)
        }
    };
    let mut buf = BytesMut::with_capacity(GLUE_HEADER_SIZE + payload_bytes.len());
    buf.put_u8(GLUE_HEADER_VERSION_BYTE);
    buf.put_u8(compression_byte);
    buf.put_slice(schema_version_id.as_bytes());
    buf.put_slice(payload_bytes);
    Ok(buf.freeze())
}

/// Decode an AWS Glue wire format message.
pub fn decode_glue_wire_format(data: &[u8]) -> Result<(GlueSchemaVersionId, Vec<u8>)> {
    let (schema_version_id, compression) = validate_glue_wire_header(data)?;
    let raw = &data[GLUE_HEADER_SIZE..];
    let payload = match compression {
        GlueCompression::None => raw.to_vec(),
        GlueCompression::Zlib => decompress_zlib(raw)?,
    };
    Ok((schema_version_id, payload))
}

/// Decode an AWS Glue wire format message, borrowing the payload when possible.
pub fn decode_glue_wire_format_borrowed(
    data: &[u8],
) -> Result<(GlueSchemaVersionId, Cow<'_, [u8]>)> {
    let (schema_version_id, compression) = validate_glue_wire_header(data)?;
    let raw = &data[GLUE_HEADER_SIZE..];
    let payload = match compression {
        GlueCompression::None => Cow::Borrowed(raw),
        GlueCompression::Zlib => Cow::Owned(decompress_zlib(raw)?),
    };
    Ok((schema_version_id, payload))
}

/// Decode an AWS Glue wire format message, returning a [`Bytes`] payload.
pub fn decode_glue_wire_format_bytes(data: &Bytes) -> Result<(GlueSchemaVersionId, Bytes)> {
    let (schema_version_id, compression) = validate_glue_wire_header(data)?;
    let payload = match compression {
        GlueCompression::None => data.slice(GLUE_HEADER_SIZE..),
        GlueCompression::Zlib => {
            let decompressed = decompress_zlib(&data[GLUE_HEADER_SIZE..])?;
            Bytes::from(decompressed)
        }
    };
    Ok((schema_version_id, payload))
}

pub(crate) fn validate_glue_wire_header(
    data: &[u8],
) -> Result<(GlueSchemaVersionId, GlueCompression)> {
    if data.len() < GLUE_HEADER_SIZE {
        return Err(SchemaRegError::wire_format(format!(
            "Glue wire format data too short: expected at least {GLUE_HEADER_SIZE} bytes, got {}",
            data.len()
        )));
    }
    if data[0] != GLUE_HEADER_VERSION_BYTE {
        return Err(SchemaRegError::wire_format(format!(
            "invalid Glue wire format header version byte: expected 0x{GLUE_HEADER_VERSION_BYTE:02X}, got 0x{:02X}",
            data[0]
        )));
    }
    let compression = match data[1] {
        GLUE_COMPRESSION_NONE_BYTE => GlueCompression::None,
        GLUE_COMPRESSION_ZLIB_BYTE => GlueCompression::Zlib,
        other => {
            return Err(SchemaRegError::wire_format(format!(
                "unknown Glue wire format compression byte: 0x{other:02X}"
            )));
        }
    };
    let mut uuid_bytes = [0u8; UUID_SIZE];
    uuid_bytes.copy_from_slice(&data[2..GLUE_HEADER_SIZE]);
    Ok((GlueSchemaVersionId(uuid_bytes), compression))
}

#[cfg(feature = "glue")]
fn compress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .map_err(|e| SchemaRegError::wire_format(format!("ZLIB compression failed: {e}")))?;
    encoder
        .finish()
        .map_err(|e| SchemaRegError::wire_format(format!("ZLIB compression failed: {e}")))
}

#[cfg(not(feature = "glue"))]
fn compress_zlib(_data: &[u8]) -> Result<Vec<u8>> {
    Err(SchemaRegError::wire_format(
        "Glue ZLIB compression requires the `glue` Cargo feature",
    ))
}

/// Maximum decompressed size (128 MiB) to protect against decompression bombs.
#[cfg(feature = "glue")]
const MAX_DECOMPRESSED_SIZE: usize = 128 * 1024 * 1024;

#[cfg(feature = "glue")]
pub(crate) fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    let decoder = ZlibDecoder::new(data);
    let mut limited = decoder.take(MAX_DECOMPRESSED_SIZE as u64 + 1);
    let mut decompressed = Vec::new();
    limited
        .read_to_end(&mut decompressed)
        .map_err(|e| SchemaRegError::wire_format(format!("ZLIB decompression failed: {e}")))?;
    if decompressed.len() > MAX_DECOMPRESSED_SIZE {
        return Err(SchemaRegError::wire_format(format!(
            "ZLIB decompressed size {} exceeds maximum {} bytes (possible decompression bomb)",
            decompressed.len(),
            MAX_DECOMPRESSED_SIZE
        )));
    }
    Ok(decompressed)
}

#[cfg(not(feature = "glue"))]
pub(crate) fn decompress_zlib(_data: &[u8]) -> Result<Vec<u8>> {
    Err(SchemaRegError::wire_format(
        "Glue ZLIB decompression requires the `glue` Cargo feature",
    ))
}

// ── Trait ─────────────────────────────────────────────────────────────────

/// Async client interface for the AWS Glue Schema Registry.
pub trait GlueSchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its version ID (the UUID from the wire format).
    ///
    /// The returned `Arc<GlueSchema>` allows callers to hold a zero-copy
    /// reference into the cache without cloning the schema bytes.
    fn get_schema_by_version_id(
        &self,
        id: GlueSchemaVersionId,
    ) -> impl Future<Output = Result<Arc<GlueSchema>>> + Send + '_;

    /// Register a schema version (idempotent).
    fn register_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a str,
        data_format: GlueDataFormat,
    ) -> impl Future<Output = Result<GlueSchemaVersionId>> + Send + 'a;
}

impl<T: GlueSchemaRegistryClient + ?Sized> GlueSchemaRegistryClient for &T {
    fn get_schema_by_version_id(
        &self,
        id: GlueSchemaVersionId,
    ) -> impl Future<Output = Result<Arc<GlueSchema>>> + Send + '_ {
        T::get_schema_by_version_id(self, id)
    }
    fn register_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a str,
        data_format: GlueDataFormat,
    ) -> impl Future<Output = Result<GlueSchemaVersionId>> + Send + 'a {
        T::register_schema(self, schema_name, schema, data_format)
    }
}

impl<T: GlueSchemaRegistryClient + ?Sized> GlueSchemaRegistryClient for std::sync::Arc<T> {
    fn get_schema_by_version_id(
        &self,
        id: GlueSchemaVersionId,
    ) -> impl Future<Output = Result<Arc<GlueSchema>>> + Send + '_ {
        T::get_schema_by_version_id(self, id)
    }
    fn register_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a str,
        data_format: GlueDataFormat,
    ) -> impl Future<Output = Result<GlueSchemaVersionId>> + Send + 'a {
        T::register_schema(self, schema_name, schema, data_format)
    }
}

// ── DynGlueSchemaRegistryClient ──────────────────────────────────────────

/// Object-safe variant of [`GlueSchemaRegistryClient`].
///
/// Because [`GlueSchemaRegistryClient`] uses `impl Future` return types (RPITIT)
/// it cannot be used as `dyn GlueSchemaRegistryClient`. This trait provides the
/// same interface with [`Pin<Box<dyn Future>>`] return types, enabling you to
/// hold and pass Glue registry clients as trait objects:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use schemreg::DynGlueSchemaRegistryClient;
///
/// fn use_registry(client: Arc<dyn DynGlueSchemaRegistryClient>) {
///     // store in structs, pass across async boundaries, etc.
/// }
/// ```
///
/// A blanket implementation is provided for every type that implements
/// [`GlueSchemaRegistryClient`], so no extra `impl` is needed.
pub trait DynGlueSchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its version ID (the UUID from the wire format).
    fn get_schema_by_version_id<'a>(
        &'a self,
        id: GlueSchemaVersionId,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Arc<GlueSchema>>> + Send + 'a>>;

    /// Register a schema version (idempotent).
    fn register_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a str,
        data_format: GlueDataFormat,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<GlueSchemaVersionId>> + Send + 'a>>;
}

/// Blanket implementation: any [`GlueSchemaRegistryClient`] is automatically a
/// [`DynGlueSchemaRegistryClient`].
impl<T: GlueSchemaRegistryClient> DynGlueSchemaRegistryClient for T {
    fn get_schema_by_version_id<'a>(
        &'a self,
        id: GlueSchemaVersionId,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Arc<GlueSchema>>> + Send + 'a>> {
        Box::pin(GlueSchemaRegistryClient::get_schema_by_version_id(self, id))
    }
    fn register_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a str,
        data_format: GlueDataFormat,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<GlueSchemaVersionId>> + Send + 'a>> {
        Box::pin(GlueSchemaRegistryClient::register_schema(
            self,
            schema_name,
            schema,
            data_format,
        ))
    }
}

// ── CachedGlueSchemaRegistry ─────────────────────────────────────────────

/// Caching wrapper around any [`GlueSchemaRegistryClient`].
pub struct CachedGlueSchemaRegistry<C> {
    inner: C,
    cache: InMemoryCache<GlueSchemaVersionId, GlueSchema>,
}

/// Default maximum number of cached Glue schema entries.
pub const DEFAULT_MAX_GLUE_CACHE_ENTRIES: usize = 1000;

impl<C: GlueSchemaRegistryClient> CachedGlueSchemaRegistry<C> {
    /// Wrap the given client with a bounded in-memory cache.
    ///
    /// Defaults to [`DEFAULT_MAX_GLUE_CACHE_ENTRIES`] (1 000) entries,
    /// evicting the oldest entry on each insert once the limit is reached.
    /// Use [`with_max_entries`](Self::with_max_entries) to override.
    pub fn new(inner: C) -> Self {
        Self::with_max_entries(inner, DEFAULT_MAX_GLUE_CACHE_ENTRIES)
    }

    /// Wrap the given client with a bounded in-memory cache.
    pub fn with_max_entries(inner: C, max_entries: usize) -> Self {
        let max_entries = max_entries.max(1);
        Self {
            inner,
            cache: InMemoryCache::new(Some(max_entries), glue_schema_lookup_cancelled_error),
        }
    }

    /// Returns a reference to the inner (uncached) client.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Number of schemas currently in the cache.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` if the cache contains no schemas.
    pub fn cache_is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear the schema cache.
    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    /// Remove a single schema version ID from the cache.
    pub fn invalidate(&self, version_id: GlueSchemaVersionId) {
        self.cache.invalidate(version_id);
    }

    /// Remove all cached schemas.
    pub fn invalidate_all(&self) {
        self.cache.clear();
    }

    /// Pre-fetch a set of schema version IDs into the cache.
    ///
    /// Duplicate IDs are deduplicated automatically. Up to 16 IDs are fetched
    /// in parallel per chunk. Failures are collected instead of failing fast;
    /// see [`WarmGlueCacheError`].
    ///
    /// # Errors
    ///
    /// Returns a [`WarmGlueCacheError`] if one or more IDs could not be fetched.
    /// IDs that loaded successfully remain in the cache.
    pub async fn warm_cache(
        &self,
        version_ids: &[GlueSchemaVersionId],
    ) -> std::result::Result<(), WarmGlueCacheError> {
        const WARM_CONCURRENCY: usize = 16;

        let unique: HashSet<GlueSchemaVersionId> = version_ids.iter().copied().collect();
        if unique.is_empty() {
            return Ok(());
        }

        let ids: Vec<GlueSchemaVersionId> = unique.into_iter().collect();
        let mut failures: Vec<(GlueSchemaVersionId, SchemaRegError)> = Vec::new();

        for chunk in ids.chunks(WARM_CONCURRENCY) {
            let futs = chunk.iter().map(|&id| async move {
                (
                    id,
                    self.cache
                        .get_or_fetch(id, || self.inner.get_schema_by_version_id(id))
                        .await,
                )
            });
            let results = futures::future::join_all(futs).await;
            for (id, result) in results {
                if let Err(e) = result {
                    failures.push((id, e));
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(WarmGlueCacheError { failures })
        }
    }

    /// Retrieve a schema by its version ID.
    pub async fn get_schema_by_version_id(
        &self,
        id: GlueSchemaVersionId,
    ) -> Result<Arc<GlueSchema>> {
        self.cache
            .get_or_fetch(id, || self.inner.get_schema_by_version_id(id))
            .await
    }

    /// Register a schema version under the given schema name.
    pub async fn register_schema(
        &self,
        schema_name: &str,
        schema: &str,
        data_format: GlueDataFormat,
    ) -> Result<GlueSchemaVersionId> {
        self.inner
            .register_schema(schema_name, schema, data_format)
            .await
    }
}

impl<C> fmt::Debug for CachedGlueSchemaRegistry<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedGlueSchemaRegistry")
            .field("cache_len", &self.cache.len())
            .field("cache", &self.cache)
            .finish()
    }
}

impl<C: GlueSchemaRegistryClient> GlueSchemaRegistryClient for CachedGlueSchemaRegistry<C> {
    async fn get_schema_by_version_id(&self, id: GlueSchemaVersionId) -> Result<Arc<GlueSchema>> {
        self.get_schema_by_version_id(id).await
    }

    async fn register_schema(
        &self,
        schema_name: &str,
        schema: &str,
        data_format: GlueDataFormat,
    ) -> Result<GlueSchemaVersionId> {
        self.register_schema(schema_name, schema, data_format).await
    }
}

impl<C: GlueSchemaRegistryClient> AnySchemaCache for CachedGlueSchemaRegistry<C> {
    type Id = GlueSchemaVersionId;

    fn cache_len(&self) -> usize {
        Self::cache_len(self)
    }

    fn cache_is_empty(&self) -> bool {
        Self::cache_is_empty(self)
    }

    fn clear_cache(&self) {
        Self::clear_cache(self)
    }

    fn invalidate(&self, id: Self::Id) {
        Self::invalidate(self, id)
    }

    fn invalidate_all(&self) {
        Self::invalidate_all(self)
    }

    fn warm_cache<'a>(
        &'a self,
        ids: &'a [Self::Id],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            Self::warm_cache(self, ids)
                .await
                .map_err(|e| SchemaRegError::invalid_state(e.to_string()))
        })
    }
}
