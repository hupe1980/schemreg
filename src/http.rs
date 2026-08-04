//! Shared async HTTP client used by Confluent and Apicurio registry connectors.

#[cfg(feature = "apicurio")]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::error::{Result, SchemaRegError};
use crate::retry::RetryPolicy;

/// Hard cap on response body size (16 MiB).
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Hard cap on request body size to prevent accidental oversized schema registrations (4 MiB).
const MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Maximum number of HTTP redirects followed for a single request.
const MAX_REDIRECTS: usize = 3;

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

/// Parse an RFC 9110 §10.2.3 `Retry-After` value into a delay in milliseconds.
///
/// Both forms are accepted:
/// - **delta-seconds** — `Retry-After: 120`
/// - **HTTP-date** — `Retry-After: Wed, 21 Oct 2015 07:28:00 GMT`
///
/// The date form is converted to a delay relative to the local clock. A date in
/// the past yields `Some(0)` (retry immediately) rather than `None`, because the
/// server did signal a retry — it simply named a moment that has already passed,
/// which is what clock skew looks like.
///
/// Returns `None` only when the value parses as neither form.
fn parse_retry_after_ms(value: &str) -> Option<u64> {
    let value = value.trim();

    // delta-seconds is by far the common case; try it first.
    if let Ok(secs) = value.parse::<u64>() {
        return Some(secs.saturating_mul(1_000));
    }

    let target = parse_http_date_unix_secs(value)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    Some((target - now).max(0) as u64 * 1_000)
}

/// Parse an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) into a Unix timestamp.
///
/// RFC 9110 also permits two obsolete formats. They are deliberately not
/// supported: no schema registry emits them, and accepting a looser grammar for
/// a value that controls how long we sleep is not a trade worth making. An
/// unparseable value falls back to the policy's own back-off.
fn parse_http_date_unix_secs(value: &str) -> Option<i64> {
    // "Sun, 06 Nov 1994 08:49:37 GMT" — fixed width, so index rather than split.
    let b = value.as_bytes();
    if b.len() != 29 || !value.ends_with(" GMT") || b[3] != b',' || b[4] != b' ' {
        return None;
    }

    let num = |lo: usize, hi: usize| -> Option<i64> { value.get(lo..hi)?.parse::<i64>().ok() };

    let day = num(5, 7)?;
    let year = num(12, 16)?;
    let hour = num(17, 19)?;
    let min = num(20, 22)?;
    let sec = num(23, 25)?;

    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let month_name = value.get(8..11)?;
    let month = MONTHS.iter().position(|m| *m == month_name)? as i64 + 1;

    if !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&min)
        // Leap seconds are legal in the grammar.
        || !(0..=60).contains(&sec)
    {
        return None;
    }

    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + min * 60 + sec)
}

/// Days since 1970-01-01 for a proleptic-Gregorian date.
///
/// Howard Hinnant's `days_from_civil`, the standard branch-free formulation
/// also used by `<chrono>` and the C++20 calendar library. Correct for every
/// year representable in `i64`, including the 100/400 leap-year exceptions.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (month + 9) % 12; // Mar = 0 … Feb = 11
    let doy = (153 * mp + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Configuration for building an [`HttpClient`].
///
/// Used by [`HttpClient::with_config`]. Extend this struct when new connection
/// options are needed so call sites only need to set the fields they care about.
#[derive(Default)]
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
    /// Retry behaviour for transient failures.
    pub retry_policy: RetryPolicy,
    /// Hard ceiling on requests in flight from this client at any moment.
    ///
    /// `None` leaves concurrency bounded only by the caller and the connection
    /// pool. Set this when a cold start could fan out to thousands of *distinct*
    /// schema IDs at once — coalescing collapses same-ID bursts, but nothing
    /// otherwise bounds distinct-ID bursts, and each one opens a socket.
    pub max_concurrent_requests: Option<usize>,
}

/// Async HTTP client used by the schema registry connectors.
///
/// Backed by [`reqwest::Client`], which provides connection pooling, automatic
/// redirect following, TLS via rustls, and configurable request timeouts.
pub(crate) struct HttpClient {
    client: Client,
    retry_policy: RetryPolicy,
    /// Permits gating outbound requests, when a ceiling was configured.
    concurrency: Option<Arc<Semaphore>>,
}

impl HttpClient {
    /// Build a client that trusts the platform-bundled WebPKI root CAs.
    pub fn with_webpki_roots(timeout: Option<Duration>) -> Result<Self> {
        Self::with_config(HttpClientConfig {
            timeout,
            ..HttpClientConfig::default()
        })
    }

    /// Build a client with full transport configuration.
    ///
    /// Supports optional custom CA certificates and a client identity for mTLS.
    /// Falls back to `with_webpki_roots` behaviour when the extra fields are
    /// left at their defaults.
    pub fn with_config(config: HttpClientConfig) -> Result<Self> {
        let mut builder = Client::builder()
            // Schema registries never legitimately need a long redirect chain.
            // Bounding it low limits SSRF-style redirect pivots and keeps a
            // misconfigured proxy loop from burning the request timeout.
            // reqwest additionally strips `Authorization` on cross-origin
            // redirects, so credentials do not follow a hostile `Location`.
            .redirect(reqwest::redirect::Policy::limited(MAX_REDIRECTS));
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
        let concurrency = config
            .max_concurrent_requests
            .filter(|n| *n > 0)
            .map(|n| Arc::new(Semaphore::new(n)));
        Ok(Self {
            client,
            retry_policy: config.retry_policy,
            concurrency,
        })
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
    /// network-level errors) are retried according to the configured
    /// [`RetryPolicy`]. When a concurrency ceiling is configured, the permit is
    /// held for the whole logical request including retries.
    pub async fn request(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        auth_header: Option<&str>,
    ) -> Result<HttpResponse> {
        // Hold the concurrency permit across every attempt of this logical
        // request: releasing it between retries would let a queued request slip
        // in and defeat the ceiling exactly when the server is struggling.
        let _permit = match &self.concurrency {
            Some(sem) => Some(sem.acquire().await.map_err(|_| {
                SchemaRegError::invalid_state("HTTP concurrency limiter was closed")
            })?),
            None => None,
        };

        let max_retries = self.retry_policy.max_retries_value();
        let mut attempt = 0u32;
        loop {
            match self
                .request_once(method, url, extra_headers, body, auth_header)
                .await
            {
                Ok(resp) if is_retryable_status(resp.status) && attempt < max_retries => {
                    let delay = self.retry_policy.delay_for(attempt, resp.retry_after_ms);
                    tracing::warn!(
                        status = resp.status,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        server_directed = resp.retry_after_ms.is_some(),
                        url,
                        "transient HTTP error — retrying"
                    );
                    sleep(delay).await;
                    attempt += 1;
                }
                Ok(resp) => return Ok(resp),
                Err(e) if attempt < max_retries => {
                    let delay = self.retry_policy.delay_for(attempt, None);
                    tracing::warn!(
                        error = %e,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        url,
                        "transient network error — retrying"
                    );
                    sleep(delay).await;
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

        // Parse `Retry-After` if present. Servers send this on 503 Service
        // Unavailable during rolling restarts just as often as on 429, so honour
        // it for every retryable status.
        let retry_after_ms = if is_retryable_status(status) {
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_retry_after_ms)
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

/// Returns `true` when `url`'s host is a loopback address.
///
/// Used to decide whether cleartext HTTP may carry credentials. Traffic to
/// `localhost`, `127.0.0.0/8`, or `::1` never leaves the machine, so there is no
/// network on which to intercept it — the same "potentially trustworthy origin"
/// rule browsers apply to secure-context features.
///
/// Anything else — including a private-range address like `10.0.0.5`, which is
/// still a real network with real switches — is treated as untrusted.
pub(crate) fn is_loopback_host(url: &str) -> bool {
    let Some(scheme_end) = url.find("://") else {
        return false;
    };
    let authority = &url[scheme_end + 3..];
    let authority = &authority[..authority.find(['/', '?', '#']).unwrap_or(authority.len())];
    // Strip userinfo (already rejected elsewhere) and the port.
    let host = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(rest) = host.strip_prefix('[') {
        // IPv6 literal: [::1]:8081
        rest.split(']').next().unwrap_or(rest)
    } else {
        host.split(':').next().unwrap_or(host)
    };

    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
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

/// Upper bound on a subject name, applied before it is spliced into a URL.
///
/// Confluent Schema Registry itself caps subject names well below this; the
/// limit exists so that a caller-controlled subject cannot be used to build a
/// multi-megabyte request line.
pub(crate) const MAX_SUBJECT_LEN: usize = 512;

/// Validate a caller-supplied subject name before it is spliced into a URL path.
///
/// Rejects:
/// - empty subjects (they would collapse a path segment and hit the wrong endpoint)
/// - `.` and `..` path segments (traversal — see below)
/// - subjects that *percent-decode* to a `.` or `..` segment (defence in depth)
/// - subjects longer than [`MAX_SUBJECT_LEN`]
///
/// The percent-encoder intentionally preserves `.` so that dotted subjects such
/// as `com.example.Order-value` round-trip unchanged. That means a bare `..`
/// segment would survive URL encoding and could be collapsed by an intermediate
/// proxy or by the registry's own router, turning
/// `DELETE /subjects/..` into `DELETE /subjects`. Rejecting the segment outright
/// is the only encoding-independent defence.
///
/// The decoded re-check covers a second class of target: this crate encodes
/// `..%2fadmin` correctly, to the single literal segment `..%252fadmin`, but a
/// proxy or gateway that decodes the path *twice* — a well-documented class of
/// misconfiguration — would recover `../admin`. Screening the decoded form
/// costs one allocation on a non-hot path and removes the crate from that
/// attack chain entirely.
///
/// This must be called by **every** operation that interpolates a subject into
/// a request path, on both the Confluent and Apicurio clients.
pub(crate) fn validate_subject(subject: &str) -> crate::error::Result<()> {
    if subject.is_empty() {
        return Err(crate::error::SchemaRegError::config(
            "subject name must not be empty",
        ));
    }
    if subject.len() > MAX_SUBJECT_LEN {
        return Err(crate::error::SchemaRegError::config(format!(
            "subject name is {} bytes, exceeding the {MAX_SUBJECT_LEN}-byte limit",
            subject.len()
        )));
    }

    has_no_dot_segments(subject)?;

    // Defence in depth against double-decoding intermediaries.
    if subject.contains('%') {
        let decoded = percent_encoding::percent_decode_str(subject).decode_utf8_lossy();
        if decoded != subject {
            has_no_dot_segments(&decoded)?;
        }
    }
    Ok(())
}

/// Reject `.` and `..` path segments in `candidate`.
fn has_no_dot_segments(candidate: &str) -> crate::error::Result<()> {
    for segment in candidate.split(['/', '\\']) {
        if segment == ".." || segment == "." {
            return Err(crate::error::SchemaRegError::config(
                "subject name must not contain '.' or '..' path segments",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn validate_subject_accepts_dotted_names() {
        assert!(validate_subject("com.example.Order-value").is_ok());
        assert!(validate_subject("orders-value").is_ok());
        assert!(validate_subject("mygroup/orders-value").is_ok());
    }

    #[test]
    fn validate_subject_rejects_traversal() {
        for bad in ["..", ".", "../admin", "a/../b", "a/./b", "../../config"] {
            assert!(
                validate_subject(bad).is_err(),
                "{bad:?} must be rejected as a traversal attempt"
            );
        }
    }

    #[test]
    fn validate_subject_rejects_percent_encoded_traversal() {
        // Neutralised by this crate's own encoder, but recovered by a
        // double-decoding proxy — rejected as defence in depth.
        for bad in ["..%2fadmin", "%2e%2e/admin", "%2e%2e%2fadmin", "..%5cadmin"] {
            assert!(
                validate_subject(bad).is_err(),
                "{bad:?} must be rejected: it percent-decodes to a traversal"
            );
        }
    }

    #[test]
    fn validate_subject_allows_benign_percent_signs() {
        assert!(validate_subject("orders%20value").is_ok());
        assert!(validate_subject("100%-complete").is_ok());
    }

    #[test]
    fn validate_subject_rejects_empty_and_oversized() {
        assert!(validate_subject("").is_err());
        assert!(validate_subject(&"x".repeat(MAX_SUBJECT_LEN + 1)).is_err());
        assert!(validate_subject(&"x".repeat(MAX_SUBJECT_LEN)).is_ok());
    }

    #[test]
    fn percent_encode_escapes_separators_and_traversal_helpers() {
        assert_eq!(percent_encode("a/b"), "a%2Fb");
        assert_eq!(percent_encode("a%2e%2e"), "a%252e%252e");
        assert_eq!(percent_encode("a b"), "a%20b");
        assert_eq!(percent_encode("a?b#c"), "a%3Fb%23c");
        // Unreserved characters and dots survive untouched.
        assert_eq!(
            percent_encode("com.example.Order-value"),
            "com.example.Order-value"
        );
    }

    #[test]
    fn reject_embedded_credentials_only_inspects_the_authority() {
        assert!(reject_embedded_credentials("https://user:pass@host").is_err());
        assert!(reject_embedded_credentials("https://host/path@notcreds").is_ok());
        assert!(reject_embedded_credentials("https://host?q=a@b").is_ok());
        assert!(reject_embedded_credentials("not-a-url").is_ok());
    }

    #[test]
    fn normalize_url_strips_only_trailing_slashes() {
        assert_eq!(normalize_url("https://h:8081///".into()), "https://h:8081");
        assert_eq!(normalize_url("https://h:8081".into()), "https://h:8081");
    }

    // ── Retry-After parsing (RFC 9110 §10.2.3) ────────────────────────────

    #[test]
    fn retry_after_delta_seconds() {
        assert_eq!(parse_retry_after_ms("120"), Some(120_000));
        assert_eq!(parse_retry_after_ms("0"), Some(0));
        assert_eq!(parse_retry_after_ms("  7  "), Some(7_000));
    }

    #[test]
    fn retry_after_delta_seconds_saturates() {
        assert_eq!(parse_retry_after_ms(&u64::MAX.to_string()), Some(u64::MAX));
    }

    #[test]
    fn retry_after_rejects_garbage() {
        for bad in ["", "soon", "-5", "1.5", "12x", "Wed, 21 Oct 2015"] {
            assert_eq!(parse_retry_after_ms(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn http_date_epoch_and_known_timestamps() {
        // The canonical RFC example.
        assert_eq!(
            parse_http_date_unix_secs("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777)
        );
        // Unix epoch itself.
        assert_eq!(
            parse_http_date_unix_secs("Thu, 01 Jan 1970 00:00:00 GMT"),
            Some(0)
        );
        // A date the RFC 9110 text uses.
        assert_eq!(
            parse_http_date_unix_secs("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some(1_445_412_480)
        );
        // 2038 — past the 32-bit rollover.
        assert_eq!(
            parse_http_date_unix_secs("Tue, 19 Jan 2038 03:14:08 GMT"),
            Some(2_147_483_648)
        );
    }

    #[test]
    fn http_date_handles_leap_years_and_century_rules() {
        // 2000 is a leap year (divisible by 400).
        assert_eq!(
            parse_http_date_unix_secs("Tue, 29 Feb 2000 00:00:00 GMT"),
            Some(951_782_400)
        );
        // 2024 is a leap year (divisible by 4).
        assert_eq!(
            parse_http_date_unix_secs("Thu, 29 Feb 2024 00:00:00 GMT"),
            Some(1_709_164_800)
        );
        // 1 Mar 1900 — 1900 is NOT a leap year (divisible by 100, not 400).
        // If the 100-year rule were missing this would be off by 86 400.
        assert_eq!(
            parse_http_date_unix_secs("Thu, 01 Mar 1900 00:00:00 GMT"),
            Some(-2_203_891_200)
        );
    }

    #[test]
    fn http_date_accepts_leap_seconds() {
        assert!(parse_http_date_unix_secs("Sat, 31 Dec 2016 23:59:60 GMT").is_some());
    }

    #[test]
    fn http_date_rejects_malformed_input() {
        for bad in [
            "Sun, 06 Nov 1994 08:49:37",      // no GMT
            "Sun 06 Nov 1994 08:49:37 GMT",   // no comma
            "Sun, 06 Xxx 1994 08:49:37 GMT",  // bad month
            "Sun, 06 Nov 1994 24:49:37 GMT",  // hour out of range
            "Sun, 06 Nov 1994 08:60:37 GMT",  // minute out of range
            "Sun, 32 Nov 1994 08:49:37 GMT",  // day out of range
            "Sun, 00 Nov 1994 08:49:37 GMT",  // day zero
            "Sunday, 06-Nov-94 08:49:37 GMT", // obsolete RFC 850 form
            "Sun Nov  6 08:49:37 1994",       // obsolete asctime form
        ] {
            assert_eq!(parse_http_date_unix_secs(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn http_date_in_the_past_means_retry_now() {
        assert_eq!(
            parse_retry_after_ms("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(0),
            "a past date is clock skew, not a parse failure"
        );
    }

    #[test]
    fn http_date_in_the_future_yields_a_positive_delay() {
        let Some(ms) = parse_retry_after_ms("Tue, 19 Jan 2038 03:14:08 GMT") else {
            unreachable!("a well-formed future date must parse")
        };
        assert!(ms > 0, "a future date must produce a positive delay");
    }

    #[test]
    fn days_from_civil_matches_known_anchors() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(2024, 1, 1), 19_723);
    }

    #[test]
    fn loopback_hosts_are_recognised() {
        for url in [
            "http://localhost:8081",
            "http://LOCALHOST/path",
            "http://127.0.0.1:8081",
            "http://127.1.2.3",
            "http://[::1]:8081",
            "https://localhost",
        ] {
            assert!(is_loopback_host(url), "{url} must be loopback");
        }
    }

    #[test]
    fn non_loopback_hosts_are_rejected() {
        for url in [
            "http://registry.example.com",
            // A private range is still a real network with real switches.
            "http://10.0.0.5:8081",
            "http://192.168.1.10",
            "http://[2001:db8::1]:8081",
            // Homograph attempts must not be mistaken for localhost.
            "http://localhost.evil.com",
            "http://notlocalhost",
            "not-a-url",
        ] {
            assert!(!is_loopback_host(url), "{url} must NOT be loopback");
        }
    }

    #[test]
    fn retryable_status_matches_the_documented_policy() {
        assert!(is_retryable_status(429));
        assert!(is_retryable_status(500));
        assert!(is_retryable_status(503));
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(404));
        assert!(!is_retryable_status(200));
    }
}
