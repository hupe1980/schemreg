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

/// Error codes returned by Confluent-compatible registries in the `error_code`
/// field of a JSON error body.
///
/// Mirrors `io.confluent.kafka.schemaregistry.rest.exceptions.Errors`. The
/// codes are how [`SchemaRegError`]'s predicates classify a failure — matching
/// on numbers rather than on message text, which is localised, reworded between
/// releases, and different again on Karapace.
pub mod error_code {
    /// The subject does not exist.
    pub const SUBJECT_NOT_FOUND: i32 = 40401;
    /// The subject exists but not at the requested version.
    pub const VERSION_NOT_FOUND: i32 = 40402;
    /// No schema with the requested ID or GUID.
    pub const SCHEMA_NOT_FOUND: i32 = 40403;
    /// The subject is soft-deleted.
    pub const SUBJECT_SOFT_DELETED: i32 = 40404;
    /// A permanent delete was attempted before a soft delete.
    pub const SUBJECT_NOT_SOFT_DELETED: i32 = 40405;
    /// The version is soft-deleted.
    pub const VERSION_SOFT_DELETED: i32 = 40406;
    /// A permanent version delete was attempted before a soft delete.
    pub const VERSION_NOT_SOFT_DELETED: i32 = 40407;
    /// The subject has no compatibility level of its own; only the global one applies.
    pub const SUBJECT_COMPATIBILITY_NOT_CONFIGURED: i32 = 40408;
    /// The subject has no mode of its own; only the global one applies.
    pub const SUBJECT_MODE_NOT_CONFIGURED: i32 = 40409;

    /// The schema is incompatible with the subject's existing version(s).
    pub const INCOMPATIBLE_SCHEMA: i32 = 40901;

    /// The schema string is not valid for its declared type.
    pub const INVALID_SCHEMA: i32 = 42201;
    /// The version identifier is not a positive integer or `latest`.
    pub const INVALID_VERSION: i32 = 42202;
    /// The compatibility level is not one the registry recognises.
    pub const INVALID_COMPATIBILITY_LEVEL: i32 = 42203;
    /// The schema exceeds the registry's size limit.
    pub const SCHEMA_TOO_LARGE: i32 = 42209;

    /// The registry's backing store failed.
    pub const STORE_ERROR: i32 = 50001;
    /// The operation timed out inside the registry.
    pub const OPERATION_TIMEOUT: i32 = 50002;
    /// Forwarding the write to the leader failed.
    pub const REQUEST_FORWARDING_FAILED: i32 = 50003;
    /// The registry cluster has no elected leader.
    pub const UNKNOWN_LEADER: i32 = 50004;

    /// Inclusive range of `5xxxx` codes, all of which are transient.
    pub(crate) const SERVER_ERROR_RANGE: std::ops::RangeInclusive<i32> = 50000..=50999;
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
    /// Codes follow the HTTP status they accompany, with two extra digits:
    /// `404xx` not found, `409xx` conflict, `422xx` unprocessable, `500xx`
    /// server-side. See [`error_code`] for the named constants, and prefer the
    /// predicates ([`is_not_found`](Self::is_not_found),
    /// [`is_incompatible`](Self::is_incompatible),
    /// [`is_retryable`](Self::is_retryable)) over matching the number directly.
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

    /// The registry's numeric error code, when the response carried one.
    #[must_use]
    pub fn error_code(&self) -> Option<i32> {
        match self {
            Self::Api { error_code, .. } => Some(*error_code),
            _ => None,
        }
    }

    /// The HTTP status, when the failure came from an HTTP response.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Auth { status, .. } | Self::Http { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// Returns `true` if the registry reported that the subject, version, or
    /// schema does not exist.
    ///
    /// Matches on the numeric `error_code`, never on message text — and, as a
    /// fallback, on a bare HTTP 404.
    ///
    /// The fallback is not redundant. A Confluent-compatible registry answers
    /// 404 with a `{"error_code": 404xx}` body, which lands in
    /// [`Api`](Self::Api). A reverse proxy, an API gateway, or a registry that
    /// has not implemented a route answers 404 with HTML or nothing at all,
    /// which lands in [`Http`](Self::Http) — and means exactly the same thing
    /// to the caller. Without this arm,
    /// [`lookup_schema`](crate::SchemaRegistryClient::lookup_schema) would
    /// report a transport-shaped error instead of `Ok(None)` for a subject that
    /// simply is not there.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::Api { error_code, .. } => matches!(
                *error_code,
                error_code::SUBJECT_NOT_FOUND
                    | error_code::VERSION_NOT_FOUND
                    | error_code::SCHEMA_NOT_FOUND
            ),
            Self::Http { status, .. } => *status == 404,
            _ => false,
        }
    }

    /// Returns `true` if the registry rejected the schema as **incompatible**
    /// with the subject's existing version(s).
    ///
    /// Distinct from [`is_invalid_schema`](Self::is_invalid_schema): the schema
    /// is well-formed, but the subject's compatibility policy forbids the
    /// change. Neither is retryable — both need a schema edit.
    #[must_use]
    pub fn is_incompatible(&self) -> bool {
        self.error_code() == Some(error_code::INCOMPATIBLE_SCHEMA)
    }

    /// Returns `true` if the registry rejected the schema as malformed or
    /// otherwise unacceptable for its declared type.
    #[must_use]
    pub fn is_invalid_schema(&self) -> bool {
        matches!(
            self.error_code(),
            Some(error_code::INVALID_SCHEMA | error_code::SCHEMA_TOO_LARGE)
        )
    }

    /// Returns `true` if the error is likely transient and safe to retry.
    ///
    /// Retryable:
    /// - transport-level [`Network`](Self::Network) failures;
    /// - HTTP 429 and every HTTP 5xx response;
    /// - [`Api`](Self::Api) errors in the registry's `5xxxx` range — a failed
    ///   backing store, an internal timeout, a leaderless cluster, or a write
    ///   that could not be forwarded to the leader. These arrive as a *parsed
    ///   JSON body*, so without this arm a 500 whose body happened to be
    ///   well-formed would be classified as permanent while an identical 500
    ///   with an HTML body would be retried.
    ///
    /// Not retryable: [`Auth`](Self::Auth), [`Config`](Self::Config),
    /// [`WireFormat`](Self::WireFormat), [`NotSupported`](Self::NotSupported),
    /// [`InvalidState`](Self::InvalidState), and every 4xx-range
    /// [`Api`](Self::Api) code — retrying those reproduces the same failure and
    /// burns the caller's budget.
    ///
    /// The classification matches the crate's internal retry policy, so an
    /// outer retry loop never re-retries something already given up on for a
    /// permanent reason.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Network(_) => true,
            Self::Http { status, .. } => *status == 429 || (500..600).contains(status),
            Self::Api { error_code, .. } => error_code::SERVER_ERROR_RANGE.contains(error_code),
            _ => false,
        }
    }
}

/// Convenience `Result` alias.
pub type Result<T> = std::result::Result<T, SchemaRegError>;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn not_found_matches_only_the_three_not_found_codes() {
        for code in [
            error_code::SUBJECT_NOT_FOUND,
            error_code::VERSION_NOT_FOUND,
            error_code::SCHEMA_NOT_FOUND,
        ] {
            assert!(SchemaRegError::api(code, "x").is_not_found(), "{code}");
        }
        for code in [
            error_code::SUBJECT_SOFT_DELETED,
            error_code::SUBJECT_COMPATIBILITY_NOT_CONFIGURED,
            error_code::INCOMPATIBLE_SCHEMA,
        ] {
            assert!(!SchemaRegError::api(code, "x").is_not_found(), "{code}");
        }
        // A bare 404 — from a proxy, a gateway, or a registry without the
        // route — carries no error code but means the same thing.
        assert!(SchemaRegError::http(404, "not found").is_not_found());
        assert!(!SchemaRegError::http(500, "boom").is_not_found());
        assert!(!SchemaRegError::auth(403, "denied").is_not_found());
    }

    #[test]
    fn incompatible_and_invalid_are_distinguished() {
        let incompatible = SchemaRegError::api(error_code::INCOMPATIBLE_SCHEMA, "x");
        assert!(incompatible.is_incompatible());
        assert!(!incompatible.is_invalid_schema());

        let invalid = SchemaRegError::api(error_code::INVALID_SCHEMA, "x");
        assert!(invalid.is_invalid_schema());
        assert!(!invalid.is_incompatible());
    }

    /// A registry-side 5xxxx code arrives as a parsed JSON body rather than as
    /// an opaque HTTP error, so it must still be classified as transient.
    #[test]
    fn server_side_api_codes_are_retryable() {
        for code in [
            error_code::STORE_ERROR,
            error_code::OPERATION_TIMEOUT,
            error_code::REQUEST_FORWARDING_FAILED,
            error_code::UNKNOWN_LEADER,
        ] {
            assert!(SchemaRegError::api(code, "x").is_retryable(), "{code}");
        }
    }

    #[test]
    fn client_side_api_codes_are_not_retryable() {
        for code in [
            error_code::SUBJECT_NOT_FOUND,
            error_code::INCOMPATIBLE_SCHEMA,
            error_code::INVALID_SCHEMA,
            error_code::INVALID_COMPATIBILITY_LEVEL,
        ] {
            assert!(!SchemaRegError::api(code, "x").is_retryable(), "{code}");
        }
    }

    #[test]
    fn transport_and_http_classification() {
        assert!(SchemaRegError::http(429, "slow down").is_retryable());
        assert!(SchemaRegError::http(503, "unavailable").is_retryable());
        assert!(!SchemaRegError::http(400, "bad request").is_retryable());
        assert!(!SchemaRegError::auth(401, "nope").is_retryable());
        assert!(!SchemaRegError::config("bad url").is_retryable());
        assert!(!SchemaRegError::wire_format("bad magic").is_retryable());
        assert!(!SchemaRegError::not_supported("nope").is_retryable());
    }

    #[test]
    fn status_and_error_code_accessors() {
        assert_eq!(SchemaRegError::auth(403, "x").status(), Some(403));
        assert_eq!(SchemaRegError::http(500, "x").status(), Some(500));
        assert_eq!(SchemaRegError::api(40401, "x").status(), None);
        assert_eq!(SchemaRegError::api(40401, "x").error_code(), Some(40401));
        assert_eq!(SchemaRegError::config("x").error_code(), None);
    }
}
