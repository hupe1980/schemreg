//! Shared async HTTP client used by Confluent and Apicurio registry connectors.

#[cfg(feature = "apicurio")]
use std::collections::HashMap;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use reqwest::Client;
use tokio::time::sleep;

use crate::error::{Result, SchemaRegError};

/// Hard cap on response body size (16 MiB).
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Hard cap on request body size to prevent accidental oversized schema registrations (4 MiB).
const MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Maximum number of retry attempts for transient errors (5xx, 429, network).
const MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff (100 ms, doubles each retry).
const BACKOFF_BASE_MS: u64 = 100;

/// Upper bound on any retry delay, regardless of `Retry-After` or backoff (60 s).
const MAX_BACKOFF_MS: u64 = 60_000;

/// An HTTP response returned by [`HttpClient::request`].
pub(crate) struct HttpResponse {
    pub status: u16,
    pub content_type: Option<String>,
    /// Zero-copy body bytes — shares the underlying allocation from the read loop.
    pub body: Bytes,
    /// Server-dictated retry delay in milliseconds (from a `Retry-After` header).
    pub retry_after_ms: Option<u64>,
    /// All response headers, with names lowercased for case-insensitive lookup.
    /// Only populated when the `apicurio` feature is enabled.
    #[cfg(feature = "apicurio")]
    pub headers: HashMap<String, String>,
}

/// Returns `true` if the HTTP status code warrants a retry.
fn is_retryable_status(status: u16) -> bool {
    // 429 Too Many Requests, 500–599 server errors
    status == 429 || (500..600).contains(&status)
}

/// Configuration for building an [`HttpClient`].
///
/// Used by [`HttpClient::with_config`]. Extend this struct when new connection
/// options are needed so call sites only need to set the fields they care about.
pub(crate) struct HttpClientConfig {
    /// Request timeout (applies to the entire request including redirect follows).
    pub timeout: Option<Duration>,
    /// Connection timeout (TCP handshake + TLS negotiation only).
    ///
    /// Set this shorter than `timeout` to fail-fast on network partitions
    /// without reducing read timeouts on large schema payloads.
    pub connect_timeout: Option<Duration>,
    /// Additional root CA certificates to trust (e.g. private CA bundles).
    pub root_certificates: Vec<reqwest::Certificate>,
    /// Client identity for mutual TLS (mTLS).
    pub identity: Option<reqwest::Identity>,
    /// Maximum idle connections per host kept in the pool.
    ///
    /// `None` means the reqwest default (no per-host limit). Set to `0` to
    /// disable connection pooling entirely for a given host.
    pub pool_max_idle_per_host: Option<usize>,
}

/// Async HTTP client used by the schema registry connectors.
///
/// Backed by [`reqwest::Client`], which provides connection pooling, automatic
/// redirect following, TLS via rustls, and configurable request timeouts.
pub(crate) struct HttpClient {
    client: Client,
}

impl HttpClient {
    /// Build a client that trusts the platform-bundled WebPKI root CAs.
    pub fn with_webpki_roots(timeout: Option<Duration>) -> Result<Self> {
        Self::with_config(HttpClientConfig {
            timeout,
            connect_timeout: None,
            root_certificates: Vec::new(),
            identity: None,
            pool_max_idle_per_host: None,
        })
    }

    /// Build a client with full transport configuration.
    ///
    /// Supports optional custom CA certificates and a client identity for mTLS.
    /// Falls back to `with_webpki_roots` behaviour when the extra fields are
    /// left at their defaults.
    pub fn with_config(config: HttpClientConfig) -> Result<Self> {
        let mut builder = Client::builder();
        if let Some(t) = config.timeout {
            builder = builder.timeout(t);
        }
        if let Some(ct) = config.connect_timeout {
            builder = builder.connect_timeout(ct);
        }
        for cert in config.root_certificates {
            builder = builder.add_root_certificate(cert);
        }
        if let Some(identity) = config.identity {
            builder = builder.identity(identity);
        }
        if let Some(n) = config.pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(n);
        }
        let client = builder
            .build()
            .map_err(|e| SchemaRegError::config(format!("failed to build HTTP client: {e}")))?;
        Ok(Self { client })
    }

    /// Execute an HTTP request and return the response.
    ///
    /// `extra_headers` are appended after the standard headers.  
    /// `body` is sent as the request body (no body is sent when `None`).  
    /// `auth_header` is added as the `Authorization` header when present.
    ///
    /// The response body is streamed via [`reqwest::Response::chunk`].
    /// If `Content-Length` declares more than [`MAX_BODY_BYTES`] the request
    /// is rejected *before* reading any body data. During streaming, reading
    /// stops as soon as the accumulated size exceeds [`MAX_BODY_BYTES`],
    /// returning an error without buffering the full oversized response.
    ///
    /// Transient failures (5xx status codes, 429 Too Many Requests, and
    /// network-level errors) are retried up to [`MAX_RETRIES`] times with
    /// exponential back-off starting at [`BACKOFF_BASE_MS`] ms.
    pub async fn request(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        auth_header: Option<&str>,
    ) -> Result<HttpResponse> {
        let mut attempt = 0u32;
        loop {
            match self
                .request_once(method, url, extra_headers, body, auth_header)
                .await
            {
                Ok(resp) if is_retryable_status(resp.status) && attempt < MAX_RETRIES => {
                    // Respect a server-supplied `Retry-After` delay (seconds); otherwise
                    // use exponential back-off. Cap to MAX_BACKOFF_MS in both cases.
                    let delay = resp
                        .retry_after_ms
                        .unwrap_or_else(|| BACKOFF_BASE_MS * (1u64 << attempt))
                        .min(MAX_BACKOFF_MS);
                    tracing::warn!(
                        status = resp.status,
                        attempt,
                        delay_ms = delay,
                        url,
                        "transient HTTP error — retrying"
                    );
                    sleep(Duration::from_millis(delay)).await;
                    attempt += 1;
                }
                Ok(resp) => return Ok(resp),
                Err(e) if attempt < MAX_RETRIES => {
                    let delay = (BACKOFF_BASE_MS * (1u64 << attempt)).min(MAX_BACKOFF_MS);
                    tracing::warn!(
                        error = %e,
                        attempt,
                        delay_ms = delay,
                        url,
                        "transient network error — retrying"
                    );
                    sleep(Duration::from_millis(delay)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Perform a single HTTP request attempt without retrying.
    async fn request_once(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        auth_header: Option<&str>,
    ) -> Result<HttpResponse> {
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|_| SchemaRegError::config(format!("invalid HTTP method: {method}")))?;

        let mut builder = self.client.request(method, url);

        for (name, value) in extra_headers {
            builder = builder.header(*name, *value);
        }
        if let Some(auth) = auth_header {
            builder = builder.header("Authorization", auth);
        }
        if let Some(b) = body {
            if b.len() > MAX_REQUEST_BODY_BYTES {
                return Err(SchemaRegError::config(format!(
                    "request body ({} bytes) exceeds the {MAX_REQUEST_BODY_BYTES}-byte limit",
                    b.len()
                )));
            }
            builder = builder.body(b.to_vec());
        }

        let response = builder.send().await.map_err(SchemaRegError::network)?;

        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|ct| ct.split(';').next())
            .map(|s| s.trim().to_string());

        // Parse `Retry-After: <seconds>` if present (integer form only; HTTP-date is ignored).
        let retry_after_ms = if status == 429 {
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(|secs| secs.saturating_mul(1_000))
        } else {
            None
        };

        // Capture all response headers (lowercase names) for consumers like Apicurio
        // that return schema metadata in `X-Registry-*` headers.
        #[cfg(feature = "apicurio")]
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|v| (name.as_str().to_lowercase(), v.to_string()))
            })
            .collect();

        // Reject oversized responses before buffering by checking Content-Length.
        if let Some(len) = response.content_length()
            && len as usize > MAX_BODY_BYTES
        {
            return Err(SchemaRegError::invalid_state(format!(
                "response Content-Length ({len} bytes) exceeds the {MAX_BODY_BYTES}-byte limit"
            )));
        }

        // Stream body chunks. We bail out as soon as we exceed MAX_BODY_BYTES
        // so we never buffer a full oversized response in memory.
        let mut buf = BytesMut::with_capacity(4096);
        let mut total = 0usize;
        let mut response = response;

        loop {
            let chunk = response.chunk().await.map_err(SchemaRegError::network)?;
            match chunk {
                Some(bytes) => {
                    total += bytes.len();
                    if total > MAX_BODY_BYTES {
                        return Err(SchemaRegError::invalid_state(format!(
                            "response body exceeds the {MAX_BODY_BYTES}-byte limit"
                        )));
                    }
                    buf.extend_from_slice(&bytes);
                }
                None => break,
            }
        }

        Ok(HttpResponse {
            status,
            content_type,
            body: buf.freeze(),
            retry_after_ms,
            #[cfg(feature = "apicurio")]
            headers,
        })
    }
}

// ── Shared URL / auth utilities used by confluent and apicurio modules ────────

/// Conservative percent-encoding set for URL path segments.
///
/// Encodes all characters that could break URL path parsing or be misinterpreted
/// by proxies and HTTP clients. Preserves RFC 3986 unreserved characters
/// (`A-Z a-z 0-9 - _ . ~`) and common sub-delimiters valid in path segments.
/// Deliberately encodes several characters that RFC 3986 technically permits
/// in path segments (e.g. `@`, `[`, `]`) to prevent any proxy or server from
/// normalising or reinterpreting them.
///
/// Note: `.` is intentionally NOT encoded so that dotted subjects like
/// `com.example.Order-value` round-trip without modification.  Bare `..` is
/// rejected by [`validate_subject`] before reaching this encoder.
pub(crate) static PATH_SEGMENT_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b'%')
    .add(b'[')
    .add(b']')
    .add(b'\\')
    .add(b'^')
    .add(b'@');

/// Percent-encode a path segment using RFC 3986 rules.
#[inline]
pub(crate) fn percent_encode(input: &str) -> String {
    percent_encoding::utf8_percent_encode(input, &PATH_SEGMENT_ENCODE_SET).to_string()
}

/// Strip trailing slashes from a URL.
pub(crate) fn normalize_url(mut url: String) -> String {
    let trimmed_len = url.trim_end_matches('/').len();
    url.truncate(trimmed_len);
    url
}

/// Reject URLs that embed credentials in the authority component
/// (`user:pass@host`), preventing accidental clear-text credential exposure.
pub(crate) fn reject_embedded_credentials(url: &str) -> crate::error::Result<()> {
    let Some(scheme_end) = url.find("://") else {
        return Ok(());
    };
    let authority_start = scheme_end + 3;
    let authority = &url[authority_start..];
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    let authority_slice = &authority[..authority_end];
    if authority_slice.contains('@') {
        return Err(crate::error::SchemaRegError::config(
            "registry URL must not contain embedded credentials (user:pass@host); \
             use the builder's auth methods instead",
        ));
    }
    Ok(())
}

/// Validate that a subject name does not contain path-traversal segments.
///
/// The percent-encoder intentionally preserves `.` to allow `com.example.X-value`
/// subjects. However a bare `..` or `.` segment would survive URL encoding and
/// could be normalised by intermediate proxies.
pub(crate) fn validate_subject(subject: &str) -> crate::error::Result<()> {
    for segment in subject.split('/') {
        if segment == ".." || segment == "." {
            return Err(crate::error::SchemaRegError::config(
                "subject name must not contain '.' or '..' path segments",
            ));
        }
    }
    Ok(())
}
