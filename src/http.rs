//! Shared async HTTP client used by Confluent and Apicurio registry connectors.

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
    /// Additional root CA certificates to trust (e.g. private CA bundles).
    pub root_certificates: Vec<reqwest::Certificate>,
    /// Client identity for mutual TLS (mTLS).
    pub identity: Option<reqwest::Identity>,
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
            root_certificates: Vec::new(),
            identity: None,
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
        for cert in config.root_certificates {
            builder = builder.add_root_certificate(cert);
        }
        if let Some(identity) = config.identity {
            builder = builder.identity(identity);
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
