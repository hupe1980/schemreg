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
#[cfg_attr(feature = "confluent", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "confluent", serde(transparent))]
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

/// Schema version within a subject.
///
/// Registry APIs use signed 32-bit integers for version numbers; negative
/// values (`-1`) conventionally refer to the latest version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "confluent", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "confluent", serde(transparent))]
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
            Err(SchemaRegError::invalid_state(format!(
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
    /// Globally unique schema ID.
    pub id: SchemaId,
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
    pub subject: Option<String>,
    /// References to other schemas.
    pub references: Vec<SchemaReference>,
}

impl Schema {
    /// Create a schema with the given ID, type, and definition.
    ///
    /// `version`, `subject`, and `references` default to `None`/empty.
    ///
    /// Accepts any type that converts to `Arc<str>`: `&str`, `String`,
    /// or an already-allocated `Arc<str>`.
    pub fn new(
        id: impl Into<SchemaId>,
        schema_type: SchemaType,
        schema: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            id: id.into(),
            schema_type,
            schema: schema.into(),
            version: None,
            subject: None,
            references: Vec::new(),
        }
    }

    /// Set the subject and version.
    pub fn with_subject(
        mut self,
        subject: impl Into<String>,
        version: impl Into<SchemaVersion>,
    ) -> Self {
        self.subject = Some(subject.into());
        self.version = Some(version.into());
        self
    }

    /// Set the schema references.
    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
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
        assert_eq!(s.id, SchemaId::new(1));
        assert_eq!(s.schema_type, SchemaType::Avro);
        assert_eq!(s.schema, Arc::from(r#"{"type":"string"}"#));
        assert_eq!(s.version, None);
        assert_eq!(s.subject, None);
        assert!(s.references.is_empty());
    }

    #[test]
    fn test_schema_with_subject() {
        let s = Schema::new(1u32, SchemaType::Avro, "{}").with_subject("my-topic-value", 3i32);
        assert_eq!(s.subject, Some("my-topic-value".to_string()));
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
