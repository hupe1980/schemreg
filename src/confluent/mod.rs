//! Confluent Schema Registry HTTP client.

pub mod encoder;

pub use encoder::{ConfluentSchemaEncoder, ConfluentSchemaEncoderBuilder};

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use base64::Engine as _;

use crate::error::{Result, SchemaRegError};
use crate::http::{
    HttpClient, HttpClientConfig, is_loopback_host, normalize_url, percent_encode,
    reject_embedded_credentials, validate_subject,
};
use crate::retry::RetryPolicy;
use crate::traits::SchemaRegistryClient;
use crate::types::{
    CompatibilityLevel, Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion,
};

const SCHEMA_REGISTRY_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";
const ERROR_BODY_PREVIEW_LIMIT: usize = 512;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// ── API JSON types ───────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SchemaByIdResponse {
    schema: String,
    #[serde(rename = "schemaType", default = "default_avro_type")]
    schema_type: String,
    references: Option<Vec<ReferenceJson>>,
}

#[derive(Deserialize)]
struct SchemaBySubjectResponse {
    id: SchemaId,
    schema: String,
    version: SchemaVersion,
    subject: String,
    #[serde(rename = "schemaType", default = "default_avro_type")]
    schema_type: String,
    references: Option<Vec<ReferenceJson>>,
}

#[derive(Deserialize)]
struct RegisterSchemaResponse {
    id: SchemaId,
}

#[derive(Deserialize)]
struct CompatibilityResponse {
    is_compatible: bool,
}

#[derive(Deserialize)]
struct CompatibilityLevelResponse {
    #[serde(rename = "compatibility", alias = "compatibilityLevel")]
    level: String,
}

#[derive(Serialize)]
struct SetCompatibilityRequest {
    compatibility: &'static str,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error_code: i32,
    message: String,
}

#[derive(Serialize, Deserialize)]
struct ReferenceJson {
    name: String,
    subject: String,
    version: SchemaVersion,
}

#[derive(Serialize)]
struct RegisterSchemaRequest<'a> {
    schema: &'a str,
    #[serde(rename = "schemaType")]
    schema_type: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    references: Vec<ReferenceJson>,
}

fn default_avro_type() -> String {
    "AVRO".to_string()
}

fn sanitized_error_body_preview(body: &str) -> String {
    if body.is_empty() {
        return "<empty>".to_string();
    }

    let mut preview = String::new();
    let mut truncated = false;

    for ch in body.chars() {
        let replacement = match ch {
            '\n' => "\\n".to_string(),
            '\r' => "\\r".to_string(),
            '\t' => "\\t".to_string(),
            ch if ch.is_control() => "?".to_string(),
            ch => ch.to_string(),
        };

        if preview.len() + replacement.len() > ERROR_BODY_PREVIEW_LIMIT {
            truncated = true;
            break;
        }
        preview.push_str(&replacement);
    }

    if truncated {
        preview.push_str("...[truncated]");
    }
    preview
}

// ── Auth ─────────────────────────────────────────────────────────────────

#[derive(Default)]
enum RegistryAuth {
    #[default]
    None,
    Basic {
        username: zeroize::Zeroizing<String>,
        password: zeroize::Zeroizing<String>,
    },
    Bearer {
        token: zeroize::Zeroizing<String>,
    },
}

// ── Client ───────────────────────────────────────────────────────────────

/// HTTP client for the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/).
pub struct ConfluentSchemaRegistry {
    client: HttpClient,
    base_url: String,
    auth: RegistryAuth,
    /// When `true`, append `?normalize=true` to schema registration requests.
    normalize: bool,
}

impl ConfluentSchemaRegistry {
    /// Create a client with the given registry URL and no authentication.
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url = normalize_url(url.into());
        reject_embedded_credentials(&url)?;
        if url.starts_with("http://") {
            tracing::warn!(
                url = %url,
                "schema registry URL uses plain HTTP — credentials and schema data will be \
                 transmitted in cleartext; use HTTPS in production"
            );
        }
        let client = HttpClient::with_webpki_roots(Some(DEFAULT_REQUEST_TIMEOUT))?;
        Ok(Self {
            client,
            base_url: url,
            auth: RegistryAuth::None,
            normalize: false,
        })
    }

    /// Create a builder for advanced configuration.
    pub fn builder() -> ConfluentSchemaRegistryBuilder {
        ConfluentSchemaRegistryBuilder::default()
    }

    /// Check if a schema is compatible with the latest version under a subject.
    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<bool> {
        validate_subject(subject)?;
        let url = format!(
            "{}/compatibility/subjects/{}/versions/latest",
            self.base_url,
            percent_encode(subject)
        );
        let body = RegisterSchemaRequest {
            schema,
            schema_type: schema_type.as_str(),
            references: Self::to_reference_json(references),
        };
        let body_bytes = serde_json::to_vec(&body).map_err(|e| {
            SchemaRegError::invalid_state(format!("failed to serialise request: {e}"))
        })?;
        let result: CompatibilityResponse = self.http_post(&url, &body_bytes).await?;
        Ok(result.is_compatible)
    }

    /// List all subjects in the registry.
    pub async fn get_subjects(&self) -> Result<Vec<String>> {
        let url = format!("{}/subjects", self.base_url);
        self.http_get(&url).await
    }

    /// List all versions registered under a subject.
    pub async fn get_versions(&self, subject: &str) -> Result<Vec<SchemaVersion>> {
        validate_subject(subject)?;
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            percent_encode(subject)
        );
        self.http_get(&url).await
    }

    /// Probe the registry for connectivity.
    ///
    /// Issues `GET /subjects?limit=1` — a lightweight request that succeeds
    /// whenever the registry is reachable and authenticated.
    pub async fn health_check(&self) -> Result<()> {
        let url = format!("{}/subjects?limit=1", self.base_url);
        self.http_get::<Vec<String>>(&url).await?;
        Ok(())
    }

    /// Set the per-subject compatibility policy.
    ///
    /// Uses `PUT /config/{subject}`. To update the global default use an
    /// empty subject string `""`.
    pub async fn set_compatibility(&self, subject: &str, level: CompatibilityLevel) -> Result<()> {
        let url = if subject.is_empty() {
            format!("{}/config", self.base_url)
        } else {
            validate_subject(subject)?;
            format!("{}/config/{}", self.base_url, percent_encode(subject))
        };
        let body = SetCompatibilityRequest {
            compatibility: level.as_str(),
        };
        let body_bytes = serde_json::to_vec(&body).map_err(|e| {
            SchemaRegError::invalid_state(format!("failed to serialise request: {e}"))
        })?;
        self.http_put_unit(&url, &body_bytes).await
    }

    /// Get the current compatibility policy for a subject.
    ///
    /// Uses `GET /config/{subject}`. If `subject` is empty, returns the global
    /// default compatibility level (`GET /config`).
    ///
    /// Returns a registry API error if the subject has no per-subject override
    /// and the caller passed a non-empty subject (i.e. the registry returns 404
    /// for subjects with no explicit override configured). Use an empty string to
    /// unconditionally retrieve the global default.
    pub async fn get_compatibility(&self, subject: &str) -> Result<CompatibilityLevel> {
        let url = if subject.is_empty() {
            format!("{}/config", self.base_url)
        } else {
            validate_subject(subject)?;
            format!("{}/config/{}", self.base_url, percent_encode(subject))
        };
        let resp: CompatibilityLevelResponse = self.http_get(&url).await?;
        resp.level.parse()
    }

    /// Delete a subject and all its versions.
    pub async fn delete_subject(
        &self,
        subject: &str,
        permanent: bool,
    ) -> Result<Vec<SchemaVersion>> {
        validate_subject(subject)?;
        let mut url = format!("{}/subjects/{}", self.base_url, percent_encode(subject));
        if permanent {
            url.push_str("?permanent=true");
        }
        self.http_delete(&url).await
    }

    fn auth_header_value(&self) -> Option<zeroize::Zeroizing<String>> {
        match &self.auth {
            RegistryAuth::None => None,
            RegistryAuth::Basic { username, password } => {
                let creds =
                    zeroize::Zeroizing::new(format!("{}:{}", username.as_str(), password.as_str()));
                let encoded = zeroize::Zeroizing::new(
                    base64::engine::general_purpose::STANDARD.encode(creds.as_bytes()),
                );
                Some(zeroize::Zeroizing::new(format!(
                    "Basic {}",
                    encoded.as_str()
                )))
            }
            RegistryAuth::Bearer { token } => Some(zeroize::Zeroizing::new(format!(
                "Bearer {}",
                token.as_str()
            ))),
        }
    }

    fn handle_response<T: serde::de::DeserializeOwned>(
        status: u16,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<T> {
        if (200..300).contains(&status) {
            match content_type {
                Some(ct) if ct.contains("json") => {}
                Some(ct) => {
                    return Err(SchemaRegError::http(
                        status,
                        format!(
                            "unexpected Content-Type '{ct}' from schema registry (expected JSON)"
                        ),
                    ));
                }
                None => {
                    return Err(SchemaRegError::http(
                        status,
                        "missing Content-Type header from schema registry (expected JSON)",
                    ));
                }
            }
            serde_json::from_slice(body).map_err(|e| {
                SchemaRegError::invalid_state(format!(
                    "failed to parse schema registry response: {e}"
                ))
            })
        } else {
            Err(Self::response_error(status, body))
        }
    }

    /// Handle a response whose successful body carries no information.
    ///
    /// Registries disagree on what a config write returns: Confluent replies
    /// `200` with a JSON echo, while some proxies and Karapace builds reply
    /// `204 No Content` with no `Content-Type` at all. Demanding a parseable
    /// JSON body would turn a successful write into a spurious error, so
    /// success is decided purely from the status code.
    fn handle_unit_response(status: u16, body: &[u8]) -> Result<()> {
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(Self::response_error(status, body))
        }
    }

    /// Convert a non-2xx response into the most specific error variant available.
    fn response_error(status: u16, body: &[u8]) -> SchemaRegError {
        if status == 401 || status == 403 {
            let message = serde_json::from_slice::<ErrorResponse>(body)
                .map(|e| e.message)
                .unwrap_or_else(|_| format!("HTTP {status}"));
            SchemaRegError::auth(status, message)
        } else if let Ok(err) = serde_json::from_slice::<ErrorResponse>(body) {
            SchemaRegError::api(err.error_code, err.message)
        } else {
            let body_str = String::from_utf8_lossy(body);
            let preview = sanitized_error_body_preview(&body_str);
            SchemaRegError::http(status, preview)
        }
    }

    async fn http_get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let auth = self.auth_header_value();
        let auth_str = auth.as_ref().map(|z| z.as_str());
        let resp = self
            .client
            .request(
                "GET",
                url,
                &[("Accept", SCHEMA_REGISTRY_CONTENT_TYPE)],
                None,
                auth_str,
            )
            .await?;
        Self::handle_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    async fn http_post<T: serde::de::DeserializeOwned>(&self, url: &str, body: &[u8]) -> Result<T> {
        let auth = self.auth_header_value();
        let auth_str = auth.as_ref().map(|z| z.as_str());
        let resp = self
            .client
            .request(
                "POST",
                url,
                &[
                    ("Accept", SCHEMA_REGISTRY_CONTENT_TYPE),
                    ("Content-Type", SCHEMA_REGISTRY_CONTENT_TYPE),
                ],
                Some(body),
                auth_str,
            )
            .await?;
        Self::handle_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    /// PUT a JSON body and ignore the (possibly empty) success body.
    async fn http_put_unit(&self, url: &str, body: &[u8]) -> Result<()> {
        let auth = self.auth_header_value();
        let auth_str = auth.as_ref().map(|z| z.as_str());
        let resp = self
            .client
            .request(
                "PUT",
                url,
                &[
                    ("Accept", SCHEMA_REGISTRY_CONTENT_TYPE),
                    ("Content-Type", SCHEMA_REGISTRY_CONTENT_TYPE),
                ],
                Some(body),
                auth_str,
            )
            .await?;
        Self::handle_unit_response(resp.status, &resp.body)
    }

    async fn http_delete<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let auth = self.auth_header_value();
        let auth_str = auth.as_ref().map(|z| z.as_str());
        let resp = self
            .client
            .request(
                "DELETE",
                url,
                &[("Accept", SCHEMA_REGISTRY_CONTENT_TYPE)],
                None,
                auth_str,
            )
            .await?;
        Self::handle_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    fn to_reference_json(refs: &[SchemaReference]) -> Vec<ReferenceJson> {
        refs.iter()
            .map(|r| ReferenceJson {
                name: r.name.clone(),
                subject: r.subject.clone(),
                version: r.version,
            })
            .collect()
    }

    fn parse_references(refs: Option<Vec<ReferenceJson>>) -> Vec<SchemaReference> {
        refs.unwrap_or_default()
            .into_iter()
            .map(|r| SchemaReference {
                name: r.name,
                subject: r.subject,
                version: r.version,
            })
            .collect()
    }

    fn schema_from_subject_response(body: SchemaBySubjectResponse) -> Result<Schema> {
        let schema_type: SchemaType = body.schema_type.parse()?;
        Ok(Schema {
            id: body.id,
            schema_type,
            schema: body.schema.into(),
            version: Some(body.version),
            subject: Some(Arc::from(body.subject.as_str())),
            references: Self::parse_references(body.references),
        })
    }
}

impl fmt::Debug for ConfluentSchemaRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth_desc = match &self.auth {
            RegistryAuth::None => "none",
            RegistryAuth::Basic { .. } => "basic(***)",
            RegistryAuth::Bearer { .. } => "bearer(***)",
        };
        f.debug_struct("ConfluentSchemaRegistry")
            .field("base_url", &self.base_url)
            .field("auth", &auth_desc)
            .finish()
    }
}

impl SchemaRegistryClient for ConfluentSchemaRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        let url = format!("{}/schemas/ids/{id}", self.base_url);
        let body: SchemaByIdResponse = self.http_get(&url).await?;
        let schema_type: SchemaType = body.schema_type.parse()?;

        Ok(Arc::new(Schema {
            id,
            schema_type,
            schema: body.schema.into(),
            version: None,
            subject: None,
            references: Self::parse_references(body.references),
        }))
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        validate_subject(subject)?;
        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.base_url,
            percent_encode(subject)
        );
        let body: SchemaBySubjectResponse = self.http_get(&url).await?;
        Self::schema_from_subject_response(body).map(Arc::new)
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        validate_subject(subject)?;
        let url = format!(
            "{}/subjects/{}/versions/{version}",
            self.base_url,
            percent_encode(subject)
        );
        let body: SchemaBySubjectResponse = self.http_get(&url).await?;
        Self::schema_from_subject_response(body).map(Arc::new)
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        validate_subject(subject)?;
        let refs = Self::to_reference_json(references);
        let url = if self.normalize {
            format!(
                "{}/subjects/{}/versions?normalize=true",
                self.base_url,
                percent_encode(subject)
            )
        } else {
            format!(
                "{}/subjects/{}/versions",
                self.base_url,
                percent_encode(subject)
            )
        };
        let body = RegisterSchemaRequest {
            schema,
            schema_type: schema_type.as_str(),
            references: refs,
        };
        let body_bytes = serde_json::to_vec(&body).map_err(|e| {
            SchemaRegError::invalid_state(format!("failed to serialise request: {e}"))
        })?;
        let result: RegisterSchemaResponse = self.http_post(&url, &body_bytes).await?;
        Ok(result.id)
    }

    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl std::future::Future<Output = Result<bool>> + Send + 'a {
        ConfluentSchemaRegistry::check_compatibility(self, subject, schema, schema_type, references)
    }

    fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        permanent: bool,
    ) -> impl std::future::Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        ConfluentSchemaRegistry::delete_subject(self, subject, permanent)
    }

    fn get_subjects(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send + '_ {
        ConfluentSchemaRegistry::get_subjects(self)
    }

    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        ConfluentSchemaRegistry::get_versions(self, subject)
    }

    fn health_check(&self) -> impl std::future::Future<Output = Result<()>> + Send + '_ {
        ConfluentSchemaRegistry::health_check(self)
    }

    fn set_compatibility<'a>(
        &'a self,
        subject: &'a str,
        level: CompatibilityLevel,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        ConfluentSchemaRegistry::set_compatibility(self, subject, level)
    }

    fn get_compatibility<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl std::future::Future<Output = Result<CompatibilityLevel>> + Send + 'a {
        ConfluentSchemaRegistry::get_compatibility(self, subject)
    }
}

// ── Builder ──────────────────────────────────────────────────────────────

/// Builder for [`ConfluentSchemaRegistry`].
pub struct ConfluentSchemaRegistryBuilder {
    url: Option<String>,
    auth: RegistryAuth,
    request_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    /// When `true`, append `?normalize=true` to schema registration requests.
    normalize: bool,
    /// Additional root CA certificates to trust (e.g. for private CAs).
    root_certificates: Vec<reqwest::Certificate>,
    /// Client certificate + private key for mTLS.
    identity: Option<reqwest::Identity>,
    /// Maximum idle connections per host kept in the connection pool.
    pool_max_idle_per_host: Option<usize>,
    /// Retry behaviour for transient failures.
    retry_policy: RetryPolicy,
    /// Hard ceiling on concurrent in-flight requests.
    max_concurrent_requests: Option<usize>,
}

impl Default for ConfluentSchemaRegistryBuilder {
    fn default() -> Self {
        Self {
            url: None,
            auth: RegistryAuth::None,
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
            connect_timeout: None,
            normalize: false,
            root_certificates: Vec::new(),
            identity: None,
            pool_max_idle_per_host: None,
            retry_policy: RetryPolicy::default(),
            max_concurrent_requests: None,
        }
    }
}

impl ConfluentSchemaRegistryBuilder {
    /// Set the schema registry URL (required).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Build a [`ConfluentSchemaRegistryBuilder`] from environment variables.
    ///
    /// Reads the following variables:
    /// - `SCHEMA_REGISTRY_URL` (required)
    /// - `SCHEMA_REGISTRY_USERNAME` + `SCHEMA_REGISTRY_PASSWORD` → basic auth
    /// - `SCHEMA_REGISTRY_BEARER_TOKEN` → bearer token auth
    ///
    /// If both basic-auth and bearer-token variables are set, bearer token
    /// takes precedence.
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("SCHEMA_REGISTRY_URL").map_err(|_| {
            SchemaRegError::config("SCHEMA_REGISTRY_URL environment variable is required")
        })?;

        let mut builder = Self::default().url(url);

        if let Ok(token) = std::env::var("SCHEMA_REGISTRY_BEARER_TOKEN") {
            builder = builder.bearer_token(token);
        } else if let (Ok(user), Ok(pass)) = (
            std::env::var("SCHEMA_REGISTRY_USERNAME"),
            std::env::var("SCHEMA_REGISTRY_PASSWORD"),
        ) {
            builder = builder.basic_auth(user, pass);
        }

        Ok(builder)
    }

    /// Set basic authentication credentials.
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = RegistryAuth::Basic {
            username: zeroize::Zeroizing::new(username.into()),
            password: zeroize::Zeroizing::new(password.into()),
        };
        self
    }

    /// Set a bearer token for authentication.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = RegistryAuth::Bearer {
            token: zeroize::Zeroizing::new(token.into()),
        };
        self
    }

    /// Set the HTTP request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Clear any explicit HTTP request timeout override.
    pub fn clear_request_timeout(mut self) -> Self {
        self.request_timeout = None;
        self
    }

    /// Set the TCP connection establishment timeout.
    ///
    /// Separate from the per-request timeout; controls how long the client
    /// waits before giving up on the initial TCP handshake.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// When set to `true`, append `?normalize=true` to schema registration
    /// requests so the registry normalises the schema before storing it.
    ///
    /// Useful when comparing schemas registered from different producers that
    /// may use different field ordering.
    pub fn normalize_schemas(mut self, normalize: bool) -> Self {
        self.normalize = normalize;
        self
    }

    /// Add a custom root CA certificate to trust.
    ///
    /// Useful when the registry uses a private certificate authority (e.g. an
    /// internal PKI). Can be called multiple times to trust several CAs.
    pub fn add_root_certificate(mut self, cert: reqwest::Certificate) -> Self {
        self.root_certificates.push(cert);
        self
    }

    /// Set a client certificate and private key for mutual TLS (mTLS).
    ///
    /// Use this when the registry requires client-certificate authentication.
    /// Build the [`reqwest::Identity`] from a PEM bundle using
    /// [`reqwest::Identity::from_pem`].
    pub fn identity(mut self, identity: reqwest::Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Override the retry policy for transient failures.
    ///
    /// The default retries 3 times with jittered exponential back-off and
    /// honours `Retry-After`. Pass [`RetryPolicy::none()`] when the calling
    /// layer already implements retry, so the two do not multiply.
    ///
    /// ```rust,no_run
    /// # use std::time::Duration;
    /// # use schemreg::RetryPolicy;
    /// let policy = RetryPolicy::new()
    ///     .max_retries(5)
    ///     .base_backoff(Duration::from_millis(50));
    /// ```
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Cap the number of requests this client may have in flight at once.
    ///
    /// Coalescing already collapses concurrent misses for the *same* schema ID
    /// to one request. This bounds the other case: a cold start that fans out to
    /// thousands of *distinct* IDs, where each miss opens its own socket. Excess
    /// callers wait for a permit rather than for a file descriptor.
    pub fn max_concurrent_requests(mut self, max: usize) -> Self {
        self.max_concurrent_requests = Some(max);
        self
    }

    /// Set the maximum number of idle connections kept per host in the pool.
    ///
    /// Set to `0` to disable connection reuse entirely. The default (unset)
    /// uses reqwest's built-in pool limit.
    pub fn pool_max_idle_per_host(mut self, n: usize) -> Self {
        self.pool_max_idle_per_host = Some(n);
        self
    }

    /// Build the [`ConfluentSchemaRegistry`] client.
    pub fn build(self) -> Result<ConfluentSchemaRegistry> {
        let url = self
            .url
            .ok_or_else(|| SchemaRegError::config("schema registry URL is required"))?;

        reject_embedded_credentials(&url)?;

        // Cleartext credentials may not cross a network. Loopback is exempt:
        // `http://localhost:8081` is the standard local-development and
        // docker-compose setup, and the traffic never leaves the machine.
        if matches!(
            self.auth,
            RegistryAuth::Basic { .. } | RegistryAuth::Bearer { .. }
        ) && url.starts_with("http://")
        {
            if !is_loopback_host(&url) {
                return Err(SchemaRegError::config(
                    "schema registry auth requires HTTPS — credentials would be sent in \
                     cleartext over HTTP. (Plain HTTP with credentials is permitted only \
                     for loopback hosts such as http://localhost:8081.)",
                ));
            }
            tracing::warn!(
                url = %url,
                "sending credentials over cleartext HTTP to a loopback address — \
                 acceptable for local development, never for a deployed registry"
            );
        }

        let client = HttpClient::with_config(HttpClientConfig {
            timeout: self.request_timeout,
            connect_timeout: self.connect_timeout,
            root_certificates: self.root_certificates,
            identity: self.identity,
            pool_max_idle_per_host: self.pool_max_idle_per_host,
            retry_policy: self.retry_policy,
            max_concurrent_requests: self.max_concurrent_requests,
        })?;

        Ok(ConfluentSchemaRegistry {
            client,
            base_url: normalize_url(url),
            auth: self.auth,
            normalize: self.normalize,
        })
    }
}

impl fmt::Debug for ConfluentSchemaRegistryBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth_desc = match &self.auth {
            RegistryAuth::None => "none",
            RegistryAuth::Basic { .. } => "basic(***)",
            RegistryAuth::Bearer { .. } => "bearer(***)",
        };
        f.debug_struct("ConfluentSchemaRegistryBuilder")
            .field("url", &self.url)
            .field("auth", &auth_desc)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("normalize", &self.normalize)
            .field("root_certificates", &self.root_certificates.len())
            .field("identity", &self.identity.is_some())
            .field("pool_max_idle_per_host", &self.pool_max_idle_per_host)
            .finish()
    }
}
