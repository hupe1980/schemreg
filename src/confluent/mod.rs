//! Confluent Schema Registry HTTP client.

pub mod encoder;

pub use encoder::{ConfluentSchemaEncoder, ConfluentSchemaEncoderBuilder};

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use base64::Engine as _;

use crate::error::{Result, SchemaRegError};
use crate::http::HttpClient;
use crate::traits::SchemaRegistryClient;
use crate::types::{Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion};

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
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            percent_encode(subject)
        );
        self.http_get(&url).await
    }

    /// Delete a subject and all its versions.
    pub async fn delete_subject(
        &self,
        subject: &str,
        permanent: bool,
    ) -> Result<Vec<SchemaVersion>> {
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
        } else if status == 401 || status == 403 {
            let message = serde_json::from_slice::<ErrorResponse>(body)
                .map(|e| e.message)
                .unwrap_or_else(|_| format!("HTTP {status}"));
            Err(SchemaRegError::auth(status, message))
        } else if let Ok(err) = serde_json::from_slice::<ErrorResponse>(body) {
            Err(SchemaRegError::api(err.error_code, err.message))
        } else {
            let body_str = String::from_utf8_lossy(body);
            let preview = sanitized_error_body_preview(&body_str);
            Err(SchemaRegError::http(status, preview))
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
            subject: Some(body.subject),
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
        let refs = Self::to_reference_json(references);
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            percent_encode(subject)
        );
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
}

fn normalize_url(mut url: String) -> String {
    let trimmed_len = url.trim_end_matches('/').len();
    url.truncate(trimmed_len);
    url
}

fn reject_embedded_credentials(url: &str) -> Result<()> {
    let Some(scheme_end) = url.find("://") else {
        return Ok(());
    };
    let authority_start = scheme_end + 3;
    let authority = &url[authority_start..];
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    let authority_slice = &authority[..authority_end];

    if authority_slice.contains('@') {
        return Err(SchemaRegError::config(
            "schema registry URL must not contain embedded credentials (user:pass@host); \
             use ConfluentSchemaRegistryBuilder::basic_auth() instead",
        ));
    }
    Ok(())
}

/// Characters that MUST be percent-encoded in a URL path segment (RFC 3986).
///
/// This set preserves unreserved characters (`A-Z a-z 0-9 - _ . ~`) and all
/// sub-delimiters/path characters that are valid in a segment, while encoding
/// only the characters that would break URL parsing. Notably, `.` and `-` are
/// NOT encoded, which allows typical Confluent subject names such as
/// `com.example.Order-value` to round-trip without modification.
static PATH_SEGMENT_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::CONTROLS
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
    // RFC 3986 gen-delims that must not appear unencoded in a path segment
    .add(b'[')
    .add(b']')
    // Characters not valid in a segment per RFC 3986
    .add(b'\\')
    .add(b'^')
    // @ must be encoded in path segments per RFC 3986 §3.3
    .add(b'@');

fn percent_encode(input: &str) -> String {
    percent_encoding::utf8_percent_encode(input, &PATH_SEGMENT_ENCODE_SET).to_string()
}

// ── Builder ──────────────────────────────────────────────────────────────

/// Builder for [`ConfluentSchemaRegistry`].
pub struct ConfluentSchemaRegistryBuilder {
    url: Option<String>,
    auth: RegistryAuth,
    request_timeout: Option<Duration>,
    /// Additional root CA certificates to trust (e.g. for private CAs).
    root_certificates: Vec<reqwest::Certificate>,
    /// Client certificate + private key for mTLS.
    identity: Option<reqwest::Identity>,
}

impl Default for ConfluentSchemaRegistryBuilder {
    fn default() -> Self {
        Self {
            url: None,
            auth: RegistryAuth::None,
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
            root_certificates: Vec::new(),
            identity: None,
        }
    }
}

impl ConfluentSchemaRegistryBuilder {
    /// Set the schema registry URL (required).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
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
    /// Build the [`reqwest::Identity`] from a PEM or PKCS#12 bundle using
    /// [`reqwest::Identity::from_pem`] or [`reqwest::Identity::from_pkcs12_der`].
    pub fn identity(mut self, identity: reqwest::Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Build the [`ConfluentSchemaRegistry`] client.
    pub fn build(self) -> Result<ConfluentSchemaRegistry> {
        let url = self
            .url
            .ok_or_else(|| SchemaRegError::config("schema registry URL is required"))?;

        reject_embedded_credentials(&url)?;

        if matches!(
            self.auth,
            RegistryAuth::Basic { .. } | RegistryAuth::Bearer { .. }
        ) && url.starts_with("http://")
        {
            return Err(SchemaRegError::config(
                "schema registry auth requires HTTPS — credentials would be sent in cleartext over HTTP",
            ));
        }

        let client = HttpClient::with_config(crate::http::HttpClientConfig {
            timeout: self.request_timeout,
            root_certificates: self.root_certificates,
            identity: self.identity,
        })?;

        Ok(ConfluentSchemaRegistry {
            client,
            base_url: normalize_url(url),
            auth: self.auth,
        })
    }
}
