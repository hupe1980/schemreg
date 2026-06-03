//! Error types for `schemreg`.

use std::fmt;
use std::sync::Arc;

use thiserror::Error;

/// Wraps an `Arc<dyn Error>` so that `thiserror`'s `#[source]` attribute
/// can chain it through the standard `std::error::Error::source()` API.
///
/// `Arc` is used instead of `Box` so that `SchemaRegError` remains `Clone`.
#[derive(Debug, Clone)]
pub struct ArcError(Arc<dyn std::error::Error + Send + Sync>);

impl ArcError {
    /// Wrap any error in an `ArcError`.
    pub(crate) fn new<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self(Arc::new(err))
    }
}

impl fmt::Display for ArcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ArcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// The main error type for schema registry operations.
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum SchemaRegError {
    /// Schema registry communication or API error.
    ///
    /// Covers HTTP transport errors, unexpected responses, and API-level
    /// failures. The `source` field preserves the underlying cause (e.g.
    /// a TLS handshake failure or a JSON parse error) so callers can
    /// distinguish transport errors from API failures without matching on
    /// the message string.
    #[error("schema registry error: {message}")]
    Registry {
        /// Human-readable error message.
        message: String,
        /// Underlying cause, if any.
        #[source]
        source: Option<ArcError>,
    },

    /// Configuration error (invalid URL, missing required field, etc.).
    #[error("configuration error: {message}")]
    Config {
        /// Error message describing the configuration problem.
        message: String,
    },

    /// Wire format error (invalid magic byte, truncated header, ZLIB failure, etc.).
    #[error("wire format error: {0}")]
    WireFormat(String),

    /// Invalid internal state (e.g. a pending cache lookup was cancelled).
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// The operation is not supported by this registry implementation.
    #[error("not supported: {0}")]
    NotSupported(String),
}

impl SchemaRegError {
    /// Create a registry error without an underlying cause.
    #[cold]
    pub fn registry(message: impl Into<String>) -> Self {
        Self::Registry {
            message: message.into(),
            source: None,
        }
    }

    /// Create a registry error with an underlying cause.
    #[cold]
    pub fn registry_with_source<E: std::error::Error + Send + Sync + 'static>(
        message: impl Into<String>,
        source: E,
    ) -> Self {
        Self::Registry {
            message: message.into(),
            source: Some(ArcError::new(source)),
        }
    }

    /// Create a configuration error.
    #[cold]
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Create a wire format error.
    #[cold]
    pub fn wire_format(message: impl Into<String>) -> Self {
        Self::WireFormat(message.into())
    }

    /// Create an invalid-state error.
    #[cold]
    pub fn invalid_state(message: impl Into<String>) -> Self {
        Self::InvalidState(message.into())
    }

    /// Create a not-supported error.
    #[cold]
    pub fn not_supported(message: impl Into<String>) -> Self {
        Self::NotSupported(message.into())
    }

    // ── Predicate helpers ─────────────────────────────────────────────────

    /// Returns `true` if this is a [`SchemaRegError::Registry`] variant
    /// (HTTP or API-level error).
    #[must_use]
    pub fn is_registry_error(&self) -> bool {
        matches!(self, Self::Registry { .. })
    }

    /// Returns `true` if this is a [`SchemaRegError::Config`] variant.
    #[must_use]
    pub fn is_config_error(&self) -> bool {
        matches!(self, Self::Config { .. })
    }

    /// Returns `true` if this is a [`SchemaRegError::WireFormat`] variant.
    #[must_use]
    pub fn is_wire_format_error(&self) -> bool {
        matches!(self, Self::WireFormat(_))
    }

    /// Returns `true` if this is a [`SchemaRegError::NotSupported`] variant.
    #[must_use]
    pub fn is_not_supported(&self) -> bool {
        matches!(self, Self::NotSupported(_))
    }

    /// Returns `true` if the error message suggests a "not found" response
    /// from the Confluent Schema Registry (error codes 404xx).
    ///
    /// The Confluent Schema Registry returns structured error codes such as
    /// `40401` (subject not found), `40402` (version not found), and `40403`
    /// (schema not found). This helper avoids matching on the error message
    /// string directly.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::Registry { message, .. } => {
                // Confluent error codes: 40401, 40402, 40403
                message.contains("(error code 404")
                    || message.contains("Subject not found")
                    || message.contains("Version not found")
                    || message.contains("Schema not found")
            }
            _ => false,
        }
    }
}

/// Convenience `Result` alias.
pub type Result<T> = std::result::Result<T, SchemaRegError>;
