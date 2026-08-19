//! Classification of AWS SDK errors into [`SchemaRegError`] variants.
//!
//! The AWS SDK returns a single `SdkError<E>` type for every failure mode:
//! request-construction bugs, connection failures, timeouts, unparseable
//! responses, and modelled service errors such as `EntityNotFoundException`.
//!
//! Collapsing all of them into [`SchemaRegError::Network`] would make
//! [`SchemaRegError::is_retryable`] return `true` for permanent failures, so a
//! caller with a retry loop would spin forever on a schema that does not exist
//! or on an IAM policy that denies access. This module maps each SDK failure
//! mode onto the [`SchemaRegError`] variant with matching retry semantics.

use aws_sdk_glue::error::{ProvideErrorMetadata, SdkError};

use crate::error::SchemaRegError;

/// Confluent-style synthetic error codes used for Glue service errors so that
/// [`SchemaRegError::is_not_found`] and friends behave uniformly across
/// backends.
mod codes {
    /// Subject / artifact / schema not found.
    pub(super) const NOT_FOUND: i32 = 40401;
    /// The entity already exists (registration conflict).
    pub(super) const ALREADY_EXISTS: i32 = 40902;
    /// The request was rejected as invalid (schema validation, bad input).
    pub(super) const INVALID_INPUT: i32 = 42201;
}

/// Map an AWS SDK error onto the [`SchemaRegError`] with matching semantics.
///
/// | SDK failure mode | Mapped variant | Retryable |
/// |---|---|---|
/// | `ConstructionFailure` | [`SchemaRegError::Config`] | no |
/// | `TimeoutError`, `DispatchFailure`, `ResponseError` | [`SchemaRegError::Network`] | yes |
/// | `ServiceError` — `AccessDeniedException`, HTTP 401/403 | [`SchemaRegError::Auth`] | no |
/// | `ServiceError` — `EntityNotFoundException` | [`SchemaRegError::Api`] (40401) | no |
/// | `ServiceError` — `AlreadyExistsException` | [`SchemaRegError::Api`] (40902) | no |
/// | `ServiceError` — `InvalidInputException` | [`SchemaRegError::Api`] (42201) | no |
/// | `ServiceError` — throttling / 5xx | [`SchemaRegError::Http`] | yes |
/// | `ServiceError` — anything else | [`SchemaRegError::Http`] (its status) | by status |
pub(crate) fn map_sdk_error<E>(err: SdkError<E>) -> SchemaRegError
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    // A construction failure is a programming/configuration bug, never a
    // transient condition — surface it before consuming `err`.
    if matches!(err, SdkError::ConstructionFailure(_)) {
        return SchemaRegError::config(format!("failed to build the AWS Glue request: {err}"));
    }

    match err {
        SdkError::ServiceError(ctx) => {
            let status = ctx.raw().status().as_u16();
            let code = ctx.err().code().unwrap_or("UnknownServiceError").to_owned();
            let message = ctx
                .err()
                .message()
                .unwrap_or("<no message returned by AWS Glue>")
                .to_owned();
            classify_service_error(status, &code, &message)
        }
        // TimeoutError / DispatchFailure / ResponseError: the request may or may
        // not have been applied, but the failure is transport-level and retryable.
        other => SchemaRegError::network(other),
    }
}

/// Map an AWS Glue service error code + HTTP status onto a [`SchemaRegError`].
fn classify_service_error(status: u16, code: &str, message: &str) -> SchemaRegError {
    let detail = format!("{code}: {message}");

    match code {
        "AccessDeniedException" | "UnrecognizedClientException" | "InvalidSignatureException" => {
            return SchemaRegError::auth(if status == 0 { 403 } else { status }, detail);
        }
        "EntityNotFoundException" => return SchemaRegError::api(codes::NOT_FOUND, detail),
        "AlreadyExistsException" => return SchemaRegError::api(codes::ALREADY_EXISTS, detail),
        "InvalidInputException" | "SchemaVersionNotFoundException" if status == 404 => {
            return SchemaRegError::api(codes::NOT_FOUND, detail);
        }
        "InvalidInputException" => return SchemaRegError::api(codes::INVALID_INPUT, detail),
        "ThrottlingException"
        | "ThrottledException"
        | "RequestThrottledException"
        | "TooManyRequestsException" => return SchemaRegError::http(429, detail),
        "InternalServiceException" | "ServiceUnavailableException" | "InternalFailure" => {
            return SchemaRegError::http(if status < 500 { 503 } else { status }, detail);
        }
        _ => {}
    }

    match status {
        401 | 403 => SchemaRegError::auth(status, detail),
        404 => SchemaRegError::api(codes::NOT_FOUND, detail),
        409 => SchemaRegError::api(codes::ALREADY_EXISTS, detail),
        // An unrecognised code carries no more information than its status, so
        // classify on that alone: `Http` is retryable for 429 and 5xx and not
        // otherwise, which is exactly the desired reading. Inventing a
        // Confluent-style code here would be worse than saying nothing — every
        // spare code already means something specific to `is_retryable`.
        _ => SchemaRegError::http(status, detail),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn entity_not_found_is_a_non_retryable_not_found() {
        let err = classify_service_error(400, "EntityNotFoundException", "no such schema");
        assert!(err.is_not_found(), "{err}");
        assert!(
            !err.is_retryable(),
            "not-found must never be retried: {err}"
        );
    }

    #[test]
    fn access_denied_is_a_non_retryable_auth_error() {
        let err = classify_service_error(400, "AccessDeniedException", "denied");
        assert!(err.is_auth_error(), "{err}");
        assert!(!err.is_retryable());
    }

    #[test]
    fn invalid_input_is_a_non_retryable_api_error() {
        let err = classify_service_error(400, "InvalidInputException", "bad schema");
        assert!(err.is_api_error(), "{err}");
        assert!(!err.is_retryable());
    }

    #[test]
    fn throttling_is_retryable() {
        let err = classify_service_error(400, "ThrottlingException", "slow down");
        assert!(err.is_retryable(), "{err}");
    }

    #[test]
    fn internal_service_exception_is_retryable() {
        let err = classify_service_error(500, "InternalServiceException", "boom");
        assert!(err.is_retryable(), "{err}");
    }

    #[test]
    fn unknown_code_falls_back_to_status_classification() {
        assert!(classify_service_error(409, "SomethingNew", "conflict").is_api_error());
        assert!(classify_service_error(503, "SomethingNew", "down").is_retryable());
        assert!(!classify_service_error(400, "SomethingNew", "nope").is_retryable());
    }

    /// The synthetic codes borrowed from Confluent must not stray into the
    /// `5xxxx` range, which `is_retryable` reads as "the registry is unwell".
    /// A Glue 4xx classified with such a code would be retried forever.
    #[test]
    fn synthetic_codes_never_look_like_server_errors() {
        for err in [
            classify_service_error(400, "EntityNotFoundException", "x"),
            classify_service_error(400, "AlreadyExistsException", "x"),
            classify_service_error(400, "InvalidInputException", "x"),
            classify_service_error(400, "SomethingNew", "x"),
        ] {
            assert!(!err.is_retryable(), "a 400 must never be retryable: {err}");
        }
    }

    #[test]
    fn message_is_preserved_for_operators() {
        let err = classify_service_error(400, "EntityNotFoundException", "schema xyz not found");
        assert!(err.to_string().contains("schema xyz not found"), "{err}");
    }
}
