//! Core schema types shared across Confluent and Glue backends.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::error::{Result, SchemaRegError};

/// Globally unique schema ID in the Confluent wire format.
///
/// The wire format encodes this as a big-endian 32-bit unsigned integer.
/// Using a newtype prevents accidental conflation with [`SchemaVersion`],
/// which is a signed 32-bit integer with different semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde-impls", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-impls", serde(transparent))]
pub struct SchemaId(u32);

impl SchemaId {
    /// Wrap a raw `u32` value.
    #[inline]
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Return the underlying `u32` value.
    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for SchemaId {
    #[inline]
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<SchemaId> for u32 {
    #[inline]
    fn from(v: SchemaId) -> Self {
        v.0
    }
}

impl PartialEq<u32> for SchemaId {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialEq<SchemaId> for u32 {
    fn eq(&self, other: &SchemaId) -> bool {
        *self == other.0
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// ── SchemaGuid ────────────────────────────────────────────────────────────

/// Globally unique schema GUID, introduced by Confluent Platform 8.
///
/// A GUID is a 128-bit fingerprint of the schema — its definition, references,
/// metadata, and rule set — so the same schema has the same GUID in every
/// registry and every context. That is the property [`SchemaId`] lacks: an
/// integer ID is assigned per registry, so the *same* schema has *different*
/// IDs in a staging and a production cluster, which is why replication and
/// multi-region setups have to rewrite the wire prefix.
///
/// GUIDs appear in three places:
///
/// - wire format v1 (`0x01` magic byte + 16 bytes) — see [`crate::wire`];
/// - the `__key_schema_id` / `__value_schema_id` Kafka headers;
/// - the `guid` field of every Confluent Schema Registry API response.
///
/// Wraps a [`uuid::Uuid`], whose byte order is already the big-endian
/// ("network order") layout the wire format uses, so
/// [`as_bytes`](Self::as_bytes) is exactly what goes on the topic. Converts
/// freely to and from `Uuid` — a caller who already has one (from the AWS SDK,
/// a database row, their own `uuid` dependency) can pass it straight in.
///
/// The newtype is not decoration: it keeps a schema GUID from being confused
/// with a [`GlueSchemaVersionId`](crate::glue::GlueSchemaVersionId), which is
/// also a UUID but names something else entirely.
///
/// # Example
///
/// ```rust
/// use schemreg::SchemaGuid;
///
/// let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
/// assert_eq!(guid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
/// assert_eq!(guid.as_bytes()[0], 0x55);
///
/// // Free interop with the `uuid` crate.
/// let raw: uuid::Uuid = guid.into();
/// assert_eq!(SchemaGuid::from(raw), guid);
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaGuid(uuid::Uuid);

impl SchemaGuid {
    /// Wrap the 16 big-endian bytes exactly as they appear on the wire.
    #[inline]
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the 16 big-endian bytes as they appear on the wire.
    #[inline]
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Borrow the underlying [`uuid::Uuid`].
    #[inline]
    #[must_use]
    pub const fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl From<uuid::Uuid> for SchemaGuid {
    #[inline]
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<SchemaGuid> for uuid::Uuid {
    #[inline]
    fn from(guid: SchemaGuid) -> Self {
        guid.0
    }
}

impl From<[u8; 16]> for SchemaGuid {
    #[inline]
    fn from(bytes: [u8; 16]) -> Self {
        Self::from_bytes(bytes)
    }
}

impl From<SchemaGuid> for [u8; 16] {
    #[inline]
    fn from(guid: SchemaGuid) -> Self {
        guid.0.into_bytes()
    }
}

impl fmt::Display for SchemaGuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `Uuid`'s Display is the canonical hyphenated lowercase form.
        self.0.fmt(f)
    }
}

impl fmt::Debug for SchemaGuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaGuid({})", self.0)
    }
}

impl FromStr for SchemaGuid {
    type Err = SchemaRegError;

    fn from_str(s: &str) -> Result<Self> {
        uuid::Uuid::parse_str(s)
            .map(Self)
            .map_err(|e| SchemaRegError::config(format!("invalid schema GUID '{s}': {e}")))
    }
}

#[cfg(feature = "serde-impls")]
impl serde::Serialize for SchemaGuid {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde-impls")]
impl<'de> serde::Deserialize<'de> for SchemaGuid {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str> as serde::Deserialize>::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

// ── SchemaKey ─────────────────────────────────────────────────────────────

/// How a framed message names the schema it was written with.
///
/// Confluent's wire format has two versions, and both are in active use:
///
/// | Version | Magic byte | Identifier |
/// |---|---|---|
/// | v0 | `0x00` | 4-byte big-endian [`SchemaId`] |
/// | v1 | `0x01` | 16-byte [`SchemaGuid`] |
///
/// v1 was added in Confluent Platform 8 and is what a producer emits once
/// GUID-based identification is enabled. Since a consumer cannot know in
/// advance which of the two a given record uses, decoding returns a
/// `SchemaKey` rather than committing to either.
///
/// `SchemaKey` converts from both `u32`/[`SchemaId`] and [`SchemaGuid`], so
/// encode call sites read the same as before:
///
/// ```rust
/// use schemreg::{SchemaGuid, SchemaKey, encode_wire_format};
///
/// let by_id = encode_wire_format(42u32, b"payload");
/// assert_eq!(by_id[0], 0x00);
///
/// let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
/// let by_guid = encode_wire_format(guid, b"payload");
/// assert_eq!(by_guid[0], 0x01);
///
/// assert_eq!(SchemaKey::from(42u32).as_id().map(|i| i.as_u32()), Some(42));
/// # Ok::<(), schemreg::SchemaRegError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaKey {
    /// Wire format v0 — a registry-assigned 32-bit ID.
    Id(SchemaId),
    /// Wire format v1 — a registry-independent 128-bit schema fingerprint.
    Guid(SchemaGuid),
}

impl SchemaKey {
    /// The schema ID, or `None` when the message named a GUID instead.
    #[inline]
    #[must_use]
    pub fn as_id(self) -> Option<SchemaId> {
        match self {
            Self::Id(id) => Some(id),
            Self::Guid(_) => None,
        }
    }

    /// The schema GUID, or `None` when the message named an ID instead.
    #[inline]
    #[must_use]
    pub fn as_guid(self) -> Option<SchemaGuid> {
        match self {
            Self::Guid(guid) => Some(guid),
            Self::Id(_) => None,
        }
    }

    /// The wire-format magic byte this key is encoded behind
    /// (`0x00` for an ID, `0x01` for a GUID).
    #[inline]
    #[must_use]
    pub fn magic_byte(self) -> u8 {
        match self {
            Self::Id(_) => crate::wire::MAGIC_BYTE_V0,
            Self::Guid(_) => crate::wire::MAGIC_BYTE_V1,
        }
    }

    /// Number of bytes this key occupies on the wire, magic byte included.
    #[inline]
    #[must_use]
    pub fn encoded_len(self) -> usize {
        match self {
            Self::Id(_) => crate::wire::PREFIX_LEN_V0,
            Self::Guid(_) => crate::wire::PREFIX_LEN_V1,
        }
    }
}

impl From<SchemaId> for SchemaKey {
    #[inline]
    fn from(id: SchemaId) -> Self {
        Self::Id(id)
    }
}

impl From<u32> for SchemaKey {
    #[inline]
    fn from(id: u32) -> Self {
        Self::Id(SchemaId::new(id))
    }
}

impl From<SchemaGuid> for SchemaKey {
    #[inline]
    fn from(guid: SchemaGuid) -> Self {
        Self::Guid(guid)
    }
}

impl PartialEq<SchemaId> for SchemaKey {
    fn eq(&self, other: &SchemaId) -> bool {
        matches!(self, Self::Id(id) if id == other)
    }
}

impl PartialEq<u32> for SchemaKey {
    fn eq(&self, other: &u32) -> bool {
        matches!(self, Self::Id(id) if id.as_u32() == *other)
    }
}

impl PartialEq<SchemaKey> for u32 {
    fn eq(&self, other: &SchemaKey) -> bool {
        other == self
    }
}

impl PartialEq<SchemaGuid> for SchemaKey {
    fn eq(&self, other: &SchemaGuid) -> bool {
        matches!(self, Self::Guid(guid) if guid == other)
    }
}

impl fmt::Display for SchemaKey {
    /// Renders as `id 42` or `guid 550e8400-…`.
    ///
    /// Goes through `Formatter::pad` so width and alignment in a format string
    /// apply to the whole rendering — `{key:<24}` lines up a column of mixed
    /// IDs and GUIDs, which `write!` alone would silently ignore.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(id) => f.pad(&format!("id {id}")),
            Self::Guid(guid) => f.pad(&format!("guid {guid}")),
        }
    }
}

/// Schema version within a subject.
///
/// Registry APIs use signed 32-bit integers for version numbers; negative
/// values (`-1`) conventionally refer to the latest version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde-impls", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-impls", serde(transparent))]
pub struct SchemaVersion(i32);

impl SchemaVersion {
    /// Wrap a raw `i32` value.
    #[inline]
    pub fn new(v: i32) -> Self {
        Self(v)
    }

    /// Return the underlying `i32` value.
    #[inline]
    pub fn as_i32(self) -> i32 {
        self.0
    }
}

impl From<i32> for SchemaVersion {
    #[inline]
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<SchemaVersion> for i32 {
    #[inline]
    fn from(v: SchemaVersion) -> Self {
        v.0
    }
}

impl PartialEq<i32> for SchemaVersion {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other
    }
}

impl PartialEq<SchemaVersion> for i32 {
    fn eq(&self, other: &SchemaVersion) -> bool {
        *self == other.0
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Schema type supported by the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SchemaType {
    /// Apache Avro schema.
    Avro,
    /// Protocol Buffers schema.
    Protobuf,
    /// JSON Schema.
    Json,
}

impl SchemaType {
    /// Return the canonical uppercase name (`"AVRO"`, `"PROTOBUF"`, `"JSON"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Protobuf => "PROTOBUF",
            Self::Json => "JSON",
        }
    }
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SchemaType {
    type Err = SchemaRegError;

    fn from_str(s: &str) -> Result<Self> {
        if s.eq_ignore_ascii_case("AVRO") {
            Ok(Self::Avro)
        } else if s.eq_ignore_ascii_case("PROTOBUF") {
            Ok(Self::Protobuf)
        } else if s.eq_ignore_ascii_case("JSON") {
            Ok(Self::Json)
        } else {
            Err(SchemaRegError::config(format!(
                "unknown schema type: '{s}'"
            )))
        }
    }
}

/// A reference to another schema (used for multi-schema dependencies).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReference {
    /// Reference name (typically the fully qualified type name).
    pub name: String,
    /// Subject that owns the referenced schema.
    pub subject: String,
    /// Version of the referenced schema.
    pub version: SchemaVersion,
}

impl SchemaReference {
    /// Create a new schema reference.
    pub fn new(
        name: impl Into<String>,
        subject: impl Into<String>,
        version: impl Into<SchemaVersion>,
    ) -> Self {
        Self {
            name: name.into(),
            subject: subject.into(),
            version: version.into(),
        }
    }
}

/// A schema retrieved from or registered with a schema registry.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// Registry-assigned schema ID, as it appears in wire format v0.
    ///
    /// `None` when the schema was fetched by GUID: `GET /schemas/guids/{guid}`
    /// does not report a numeric ID, and a registry-assigned ID is not
    /// derivable from a GUID — the same schema has different IDs in different
    /// registries, which is the reason GUIDs exist.
    pub id: Option<SchemaId>,
    /// Registry-independent schema fingerprint, as it appears in wire format v1.
    ///
    /// `None` when the registry does not report one — every Confluent Schema
    /// Registry before Platform 8, and every backend that has no equivalent
    /// concept (Apicurio's native API, AWS Glue).
    pub guid: Option<SchemaGuid>,
    /// Schema type (Avro, Protobuf, or JSON Schema).
    pub schema_type: SchemaType,
    /// Schema definition string.
    ///
    /// For Avro and JSON Schema this is a JSON string. For Protobuf this is
    /// the `.proto` file content.
    ///
    /// Stored as a reference-counted string so that cloning a schema from a
    /// cache hit is O(1) — only the `Arc` refcount is bumped, not the
    /// underlying string bytes.
    pub schema: Arc<str>,
    /// Schema version within its subject (`None` when fetched by ID only).
    pub version: Option<SchemaVersion>,
    /// Subject name (`None` when fetched by ID only).
    ///
    /// Stored as `Arc<str>` so that cloning a [`Schema`] is O(1) regardless
    /// of subject name length.
    pub subject: Option<Arc<str>>,
    /// References to other schemas.
    pub references: Vec<SchemaReference>,
}

impl Schema {
    /// Create a schema identified by an ID or a GUID.
    ///
    /// `key` accepts a `u32`, a [`SchemaId`], or a [`SchemaGuid`] and populates
    /// the matching field; the other stays `None`. `version`, `subject`, and
    /// `references` default to `None`/empty.
    ///
    /// `schema` accepts anything convertible to `Arc<str>`: `&str`, `String`,
    /// or an already-allocated `Arc<str>`.
    ///
    /// ```rust
    /// use schemreg::{Schema, SchemaGuid, SchemaId, SchemaType};
    ///
    /// let by_id = Schema::new(7u32, SchemaType::Avro, r#""string""#);
    /// assert_eq!(by_id.id, Some(SchemaId::new(7)));
    /// assert_eq!(by_id.guid, None);
    ///
    /// let guid: SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse()?;
    /// let by_guid = Schema::new(guid, SchemaType::Avro, r#""string""#);
    /// assert_eq!(by_guid.id, None);
    /// assert_eq!(by_guid.guid, Some(guid));
    /// # Ok::<(), schemreg::SchemaRegError>(())
    /// ```
    pub fn new(
        key: impl Into<SchemaKey>,
        schema_type: SchemaType,
        schema: impl Into<Arc<str>>,
    ) -> Self {
        let key = key.into();
        Self {
            id: key.as_id(),
            guid: key.as_guid(),
            schema_type,
            schema: schema.into(),
            version: None,
            subject: None,
            references: Vec::new(),
        }
    }

    /// Set the subject and version.
    #[must_use]
    pub fn with_subject(
        mut self,
        subject: impl Into<Arc<str>>,
        version: impl Into<SchemaVersion>,
    ) -> Self {
        self.subject = Some(subject.into());
        self.version = Some(version.into());
        self
    }

    /// Set the schema references.
    #[must_use]
    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    /// Set the registry-independent schema GUID.
    #[must_use]
    pub fn with_guid(mut self, guid: SchemaGuid) -> Self {
        self.guid = Some(guid);
        self
    }

    /// The identifier to frame a payload written against this schema with.
    ///
    /// Prefers the [`guid`](Self::guid) when the registry reported one, since a
    /// GUID identifies the same schema in every registry and an ID does not;
    /// falls back to the [`id`](Self::id). `None` only if the registry reported
    /// neither, which no backend in this crate produces.
    #[must_use]
    pub fn key(&self) -> Option<SchemaKey> {
        self.guid
            .map(SchemaKey::Guid)
            .or_else(|| self.id.map(SchemaKey::Id))
    }
}

/// Per-subject (or global) schema compatibility policy.
///
/// Controls which schema changes are allowed when registering a new version.
/// The `Transitive` variants enforce compatibility against *all* prior versions,
/// not just the immediately preceding one.
///
/// # Serialisation
///
/// The string representation matches Confluent Schema Registry's API values
/// (`"BACKWARD"`, `"FORWARD"`, etc.).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompatibilityLevel {
    /// New schema must be readable by code written against the previous schema.
    Backward,
    /// New schema must be readable by code written against any prior schema.
    BackwardTransitive,
    /// Old schema must be readable by code written against the new schema.
    Forward,
    /// Old schema must be readable by code written against any prior schema.
    ForwardTransitive,
    /// Both backward and forward compatible.
    Full,
    /// Both backward and forward compatible against all prior schemas.
    FullTransitive,
    /// No compatibility checks enforced.
    None,
}

impl CompatibilityLevel {
    /// Return the canonical string used by the Confluent Schema Registry API.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
            Self::None => "NONE",
        }
    }
}

impl fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CompatibilityLevel {
    type Err = SchemaRegError;

    /// Parses case-insensitively, matching [`SchemaType`]. Apicurio returns
    /// these values verbatim from a rule config; Confluent uppercases them.
    fn from_str(s: &str) -> Result<Self> {
        const LEVELS: [(&str, CompatibilityLevel); 7] = [
            ("BACKWARD", CompatibilityLevel::Backward),
            (
                "BACKWARD_TRANSITIVE",
                CompatibilityLevel::BackwardTransitive,
            ),
            ("FORWARD", CompatibilityLevel::Forward),
            ("FORWARD_TRANSITIVE", CompatibilityLevel::ForwardTransitive),
            ("FULL", CompatibilityLevel::Full),
            ("FULL_TRANSITIVE", CompatibilityLevel::FullTransitive),
            ("NONE", CompatibilityLevel::None),
        ];
        LEVELS
            .iter()
            .find(|(name, _)| s.eq_ignore_ascii_case(name))
            .map(|(_, level)| *level)
            .ok_or_else(|| SchemaRegError::config(format!("unknown compatibility level: '{s}'")))
    }
}

/// Whether a payload is a Kafka record key or value.
///
/// Replaces the bare `is_key: bool` parameter in encoder/decoder APIs. Using a
/// named enum eliminates the "boolean trap" where callers transpose the argument
/// and silently register schemas under the wrong subject.
///
/// # Example
///
/// ```rust
/// use schemreg::EncodeTarget;
///
/// let target = EncodeTarget::Value;
/// assert!(!target.is_key());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EncodeTarget {
    /// The payload is a Kafka record key (subject suffix `-key`).
    Key,
    /// The payload is a Kafka record value (subject suffix `-value`).
    #[default]
    Value,
}

impl EncodeTarget {
    /// Returns `true` if this is [`EncodeTarget::Key`].
    #[inline]
    pub fn is_key(self) -> bool {
        matches!(self, Self::Key)
    }
}

impl std::fmt::Display for EncodeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key => f.write_str("key"),
            Self::Value => f.write_str("value"),
        }
    }
}

// ── Compile-time Send + Sync assertions ────────────────────────────────

const _: () = {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    fn check() {
        assert_send_sync::<Schema>();
        assert_send_sync::<SchemaReference>();
        assert_send_sync::<ArtifactId>();
        assert_send_sync::<CompatibilityLevel>();
    }
    let _ = check;
};

// ── ArtifactId ────────────────────────────────────────────────────────────

/// Identifies an Apicurio Registry artifact by group and artifact ID.
///
/// In Apicurio Registry v3, all artifacts are scoped to a group. The canonical
/// default group name is `"default"` and is used when no explicit group is
/// provided.
///
/// # Subject string encoding
///
/// `ArtifactId` can be serialised to and parsed from a subject string of the
/// form `"{group}/{artifact}"`. Single-component subjects (no `/`) use the
/// default group `"default"`.
///
/// ```rust
/// use schemreg::ArtifactId;
///
/// let id = ArtifactId::default_group("orders-value");
/// assert_eq!(id.to_subject(), "default/orders-value");
///
/// let parsed = ArtifactId::from_subject("mygroup/orders-value");
/// assert_eq!(parsed.group, "mygroup");
/// assert_eq!(parsed.artifact, "orders-value");
///
/// // Single-component → default group
/// let bare = ArtifactId::from_subject("payments-value");
/// assert_eq!(bare.group, "default");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArtifactId {
    /// The group containing the artifact. Defaults to `"default"`.
    pub group: String,
    /// The artifact identifier within the group.
    pub artifact: String,
}

impl ArtifactId {
    /// Create an `ArtifactId` with explicit group and artifact.
    pub fn new(group: impl Into<String>, artifact: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            artifact: artifact.into(),
        }
    }

    /// Create an `ArtifactId` in the Apicurio default group (`"default"`).
    pub fn default_group(artifact: impl Into<String>) -> Self {
        Self::new("default", artifact)
    }

    /// Parse from a subject string of the form `"{group}/{artifact}"` or
    /// `"{artifact}"` (uses the default group `"default"`).
    pub fn from_subject(subject: &str) -> Self {
        match subject.split_once('/') {
            Some((group, artifact)) => Self::new(group, artifact),
            None => Self::default_group(subject),
        }
    }

    /// Encode as a subject string `"{group}/{artifact}"`.
    pub fn to_subject(&self) -> String {
        format!("{}/{}", self.group, self.artifact)
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.group, self.artifact)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_type_display() {
        assert_eq!(SchemaType::Avro.to_string(), "AVRO");
        assert_eq!(SchemaType::Protobuf.to_string(), "PROTOBUF");
        assert_eq!(SchemaType::Json.to_string(), "JSON");
    }

    #[test]
    fn test_schema_type_from_str() {
        assert_eq!("AVRO".parse::<SchemaType>().unwrap(), SchemaType::Avro);
        assert_eq!(
            "PROTOBUF".parse::<SchemaType>().unwrap(),
            SchemaType::Protobuf
        );
        assert_eq!("JSON".parse::<SchemaType>().unwrap(), SchemaType::Json);
    }

    #[test]
    fn test_schema_type_from_str_case_insensitive() {
        assert_eq!("avro".parse::<SchemaType>().unwrap(), SchemaType::Avro);
    }

    #[test]
    fn test_schema_type_from_str_unknown() {
        let result = "XML".parse::<SchemaType>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("XML"));
    }

    #[test]
    fn test_schema_new() {
        let s = Schema::new(1u32, SchemaType::Avro, r#"{"type":"string"}"#);
        assert_eq!(s.id, Some(SchemaId::new(1)));
        assert_eq!(s.schema_type, SchemaType::Avro);
        assert_eq!(s.schema, Arc::from(r#"{"type":"string"}"#));
        assert_eq!(s.version, None);
        assert_eq!(s.subject, None);
        assert!(s.references.is_empty());
    }

    #[test]
    fn test_schema_with_subject() {
        let s = Schema::new(1u32, SchemaType::Avro, "{}").with_subject("my-topic-value", 3i32);
        assert_eq!(s.subject.as_deref(), Some("my-topic-value"));
        assert_eq!(s.version, Some(SchemaVersion::new(3)));
    }

    #[test]
    fn test_schema_with_references() {
        let refs = vec![SchemaReference::new("Ref", "ref-subject", 1i32)];
        let s = Schema::new(1u32, SchemaType::Avro, "{}").with_references(refs.clone());
        assert_eq!(s.references, refs);
    }

    #[test]
    fn test_schema_reference_new() {
        let r = SchemaReference::new("com.example.Address", "address-value", 2i32);
        assert_eq!(r.name, "com.example.Address");
        assert_eq!(r.subject, "address-value");
        assert_eq!(r.version, SchemaVersion::new(2));
    }

    #[test]
    fn test_schema_id_newtype() {
        let id: SchemaId = 42u32.into();
        assert_eq!(id.as_u32(), 42);
        assert_eq!(u32::from(id), 42);
        assert_eq!(id.to_string(), "42");
    }

    #[test]
    fn test_schema_version_newtype() {
        let v: SchemaVersion = 3i32.into();
        assert_eq!(v.as_i32(), 3);
        assert_eq!(i32::from(v), 3);
        assert_eq!(v.to_string(), "3");
    }
}
