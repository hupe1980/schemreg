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
    /// Transport-level failure: TLS, DNS, connection timeout, I/O error.
    ///
    /// These are retryable by callers that implement retry / circuit-breaker logic.
    #[error("network error: {0}")]
    Network(ArcError),

    /// The registry rejected the request for authentication / authorisation
    /// reasons (HTTP 401 or 403).
    ///
    /// These are **not** retryable without credential rotation.
    #[error("authentication error: HTTP {status} — {message}")]
    Auth {
        /// HTTP status code (401 or 403).
        status: u16,
        /// Message from the registry response body (sanitised).
        message: String,
    },

    /// The registry returned a structured API error with a numeric error code.
    ///
    /// Confluent error codes follow a `4XXYY` pattern:
    /// - `40401`: subject not found
    /// - `40402`: version not found
    /// - `40403`: schema not found
    /// - `42201`–`42203`: schema compatibility / validation errors
    #[error("registry API error {error_code}: {message}")]
    Api {
        /// Confluent-style integer error code.
        error_code: i32,
        /// Human-readable message from the registry.
        message: String,
    },

    /// A non-JSON or unrecognised HTTP error response from the registry.
    ///
    /// Includes the HTTP status and a sanitised preview of the response body.
    #[error("HTTP error: {message}")]
    Http {
        /// HTTP status code.
        status: u16,
        /// Sanitised body preview.
        message: String,
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
    /// Create a network transport error.
    #[cold]
    pub fn network<E: std::error::Error + Send + Sync + 'static>(source: E) -> Self {
        Self::Network(ArcError::new(source))
    }

    /// Create an authentication error.
    #[cold]
    pub fn auth(status: u16, message: impl Into<String>) -> Self {
        Self::Auth {
            status,
            message: message.into(),
        }
    }

    /// Create a structured API error.
    #[cold]
    pub fn api(error_code: i32, message: impl Into<String>) -> Self {
        Self::Api {
            error_code,
            message: message.into(),
        }
    }

    /// Create an HTTP error (non-JSON error body).
    #[cold]
    pub fn http(status: u16, message: impl Into<String>) -> Self {
        Self::Http {
            status,
            message: message.into(),
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

    /// Returns `true` if this is a transport-level [`Network`](Self::Network) error.
    ///
    /// Network errors are typically retryable.
    #[must_use]
    pub fn is_network_error(&self) -> bool {
        matches!(self, Self::Network(_))
    }

    /// Returns `true` if this is an [`Auth`](Self::Auth) error (HTTP 401/403).
    ///
    /// Auth errors require credential rotation and should **not** be retried.
    #[must_use]
    pub fn is_auth_error(&self) -> bool {
        matches!(self, Self::Auth { .. })
    }

    /// Returns `true` if this is a structured [`Api`](Self::Api) error from the registry.
    #[must_use]
    pub fn is_api_error(&self) -> bool {
        matches!(self, Self::Api { .. })
    }

    /// Returns `true` if this is a [`Config`](Self::Config) variant.
    #[must_use]
    pub fn is_config_error(&self) -> bool {
        matches!(self, Self::Config { .. })
    }

    /// Returns `true` if this is a [`WireFormat`](Self::WireFormat) variant.
    #[must_use]
    pub fn is_wire_format_error(&self) -> bool {
        matches!(self, Self::WireFormat(_))
    }

    /// Returns `true` if this is a [`NotSupported`](Self::NotSupported) variant.
    #[must_use]
    pub fn is_not_supported(&self) -> bool {
        matches!(self, Self::NotSupported(_))
    }

    /// Returns `true` if this is an [`InvalidState`](Self::InvalidState) variant.
    #[must_use]
    pub fn is_invalid_state(&self) -> bool {
        matches!(self, Self::InvalidState(_))
    }

    /// Returns `true` if the error represents a "not found" response from the
    /// Confluent Schema Registry (API error codes 40401, 40402, 40403).
    ///
    /// Uses the numeric `error_code` field — no string matching.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Api { error_code, .. } if (40401..=40403).contains(error_code))
    }

    /// Returns `true` if the error is likely transient and safe to retry.
    ///
    /// Network errors and HTTP throttling/unavailability responses
    /// (429 Too Many Requests, 503 Service Unavailable) are considered
    /// retryable. Auth and configuration errors are not.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Network(_) => true,
            Self::Http { status, .. } => matches!(status, 429 | 503),
            _ => false,
        }
    }
}

/// Convenience `Result` alias.
pub type Result<T> = std::result::Result<T, SchemaRegError>;
