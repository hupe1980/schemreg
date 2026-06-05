//! Native Apicurio Registry v3 HTTP client.
//!
//! Implements [`SchemaRegistryClient`] against the Apicurio Registry REST API v3
//! (`/apis/registry/v3/`).  The client is compatible with Apicurio Registry 3.x
//! and supports Avro, JSON Schema, and Protobuf artifact types.
//!
//! # Subject ↔ Artifact ID mapping
//!
//! Apicurio v3 uses a two-dimensional **group + artifact ID** address space.
//! The [`SchemaRegistryClient`] trait uses a single `subject` string.  The
//! mapping is:
//!
//! | Subject string | Group ID | Artifact ID |
//! |---|---|---|
//! | `"orders-value"` | `"default"` | `"orders-value"` |
//! | `"mygroup/orders-value"` | `"mygroup"` | `"orders-value"` |
//!
//! Use [`ArtifactId::to_subject`] to encode and [`ArtifactId::from_subject`] to
//! decode.  Callers who only use the default group can pass plain subject names
//! and the client will use the configured `default_group` (default: `"default"`).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SchemaRegError};
use crate::http::{
    HttpClient, HttpClientConfig, normalize_url, percent_encode, reject_embedded_credentials,
    validate_subject,
};
use crate::traits::SchemaRegistryClient;
use crate::types::{
    ArtifactId, CompatibilityLevel, Schema, SchemaId, SchemaReference, SchemaType, SchemaVersion,
};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_GROUP: &str = "default";
const API_PREFIX: &str = "/apis/registry/v3";

// Response header names (lowercase) that Apicurio v3 sets on content endpoints.
const HDR_ARTIFACT_TYPE: &str = "x-registry-artifacttype";
const HDR_GLOBAL_ID: &str = "x-registry-globalid";
const HDR_VERSION: &str = "x-registry-version";
const HDR_GROUP_ID: &str = "x-registry-groupid";
const HDR_ARTIFACT_ID: &str = "x-registry-artifactid";

// ── JSON request / response types ─────────────────────────────────────────────

/// Apicurio v3 error body (RFC-7807 problem-detail style).
#[derive(Deserialize)]
struct ApicurioError {
    detail: Option<String>,
    title: Option<String>,
    message: Option<String>,
}

impl ApicurioError {
    fn into_message(self) -> String {
        self.detail
            .or(self.message)
            .or(self.title)
            .unwrap_or_else(|| "unknown error".to_string())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateArtifactRequest<'a> {
    artifact_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_type: Option<&'a str>,
    first_version: CreateVersionWrapper<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateVersionWrapper<'a> {
    content: VersionContentRequest<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionContentRequest<'a> {
    content: &'a str,
    content_type: &'a str,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    references: Vec<ArtifactReferenceJson>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactReferenceJson {
    group_id: Option<String>,
    artifact_id: String,
    version: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateArtifactResponse {
    version: CreateArtifactVersionInfo,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateArtifactVersionInfo {
    global_id: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactVersionList {
    versions: Vec<ArtifactVersionSummary>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactVersionSummary {
    version: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactSearchResults {
    artifacts: Vec<SearchedArtifact>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchedArtifact {
    artifact_id: String,
    group_id: Option<String>,
}

#[derive(Deserialize)]
struct CompatibilityTestResult {
    compatible: bool,
}

#[derive(Serialize)]
struct SetCompatibilityRuleRequest {
    r#type: &'static str,
    config: &'static str,
}

#[derive(Deserialize)]
struct GetCompatibilityRuleResponse {
    config: String,
}

// ── Auth ──────────────────────────────────────────────────────────────────────

enum ApicurioAuth {
    None,
    Basic {
        username: zeroize::Zeroizing<String>,
        password: zeroize::Zeroizing<String>,
    },
    Bearer {
        token: zeroize::Zeroizing<String>,
    },
}

// ── Client ────────────────────────────────────────────────────────────────────

/// HTTP client for the [Apicurio Registry](https://www.apicur.io/registry/) v3 native API.
///
/// Implements [`SchemaRegistryClient`] using the `/apis/registry/v3/` REST API
/// endpoints.  Subject strings are parsed as `"{groupId}/{artifactId}"` (or
/// just `"{artifactId}"` to use the default group).
///
/// # Quick start
///
/// ```rust,no_run
/// use schemreg::apicurio::ApicurioSchemaRegistry;
/// use schemreg::SchemaRegistryClient;
///
/// # async fn run() -> schemreg::error::Result<()> {
/// let registry = ApicurioSchemaRegistry::new("http://localhost:8080")?;
/// let schema = registry.get_latest_schema("default/orders-value").await?;
/// println!("schema id = {}", schema.id);
/// # Ok(())
/// # }
/// ```
pub struct ApicurioSchemaRegistry {
    client: HttpClient,
    base_url: String,
    auth: ApicurioAuth,
}

impl ApicurioSchemaRegistry {
    /// Create a client with the given Apicurio Registry URL and no authentication.
    ///
    /// The URL should be the base URL of the registry server
    /// (e.g. `"http://localhost:8080"`).  The client automatically prepends
    /// `/apis/registry/v3` to all API paths.
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url = normalize_url(url.into());
        reject_embedded_credentials(&url)?;
        if url.starts_with("http://") {
            tracing::warn!(
                url = %url,
                "Apicurio Registry URL uses plain HTTP — credentials and schema data will be \
                 transmitted in cleartext; use HTTPS in production"
            );
        }
        let client = HttpClient::with_webpki_roots(Some(DEFAULT_REQUEST_TIMEOUT))?;
        Ok(Self {
            client,
            base_url: url,
            auth: ApicurioAuth::None,
        })
    }

    /// Create a builder for advanced configuration.
    pub fn builder() -> ApicurioSchemaRegistryBuilder {
        ApicurioSchemaRegistryBuilder::default()
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn api_url(&self, path: &str) -> String {
        format!("{}{}{}", self.base_url, API_PREFIX, path)
    }

    fn auth_header(&self) -> Option<zeroize::Zeroizing<String>> {
        match &self.auth {
            ApicurioAuth::None => None,
            ApicurioAuth::Basic { username, password } => {
                use base64::Engine as _;
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
            ApicurioAuth::Bearer { token } => Some(zeroize::Zeroizing::new(format!(
                "Bearer {}",
                token.as_str()
            ))),
        }
    }

    /// GET a JSON response from an Apicurio endpoint.
    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let auth = self.auth_header();
        let auth_str = auth.as_deref().map(|z| z.as_str());
        let resp = self
            .client
            .request(
                "GET",
                url,
                &[("Accept", "application/json")],
                None,
                auth_str,
            )
            .await?;
        self.handle_json_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    /// GET raw artifact content (schema bytes) from an Apicurio content endpoint.
    /// Returns the full [`HttpResponse`](crate::http::HttpResponse) so callers can
    /// read `X-Registry-*` headers.
    async fn get_content(&self, url: &str) -> Result<crate::http::HttpResponse> {
        let auth = self.auth_header();
        let auth_str = auth.as_deref().map(|z| z.as_str());
        let resp = self
            .client
            .request("GET", url, &[("Accept", "*/*")], None, auth_str)
            .await?;
        if resp.status == 404 {
            let msg = self.parse_error_body(&resp.body);
            return Err(SchemaRegError::api(40401, msg));
        }
        if resp.status == 401 || resp.status == 403 {
            let msg = self.parse_error_body(&resp.body);
            return Err(SchemaRegError::auth(resp.status, msg));
        }
        if !(200..300).contains(&resp.status as &u16) {
            let msg = self.parse_error_body(&resp.body);
            return Err(SchemaRegError::http(resp.status, msg));
        }
        Ok(resp)
    }

    /// POST a JSON body and decode a JSON response.
    async fn post_json<T: serde::de::DeserializeOwned>(&self, url: &str, body: &[u8]) -> Result<T> {
        let auth = self.auth_header();
        let auth_str = auth.as_deref().map(|z| z.as_str());
        let resp = self
            .client
            .request(
                "POST",
                url,
                &[
                    ("Accept", "application/json"),
                    ("Content-Type", "application/json"),
                ],
                Some(body),
                auth_str,
            )
            .await?;
        self.handle_json_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    /// PUT a JSON body and decode a JSON response.
    async fn put_json<T: serde::de::DeserializeOwned>(&self, url: &str, body: &[u8]) -> Result<T> {
        let auth = self.auth_header();
        let auth_str = auth.as_deref().map(|z| z.as_str());
        let resp = self
            .client
            .request(
                "PUT",
                url,
                &[
                    ("Accept", "application/json"),
                    ("Content-Type", "application/json"),
                ],
                Some(body),
                auth_str,
            )
            .await?;
        self.handle_json_response(resp.status, resp.content_type.as_deref(), &resp.body)
    }

    /// DELETE and expect 204 No Content (or 200).
    async fn delete_no_content(&self, url: &str) -> Result<()> {
        let auth = self.auth_header();
        let auth_str = auth.as_deref().map(|z| z.as_str());
        let resp = self
            .client
            .request("DELETE", url, &[], None, auth_str)
            .await?;
        if resp.status == 404 {
            let msg = self.parse_error_body(&resp.body);
            return Err(SchemaRegError::api(40401, msg));
        }
        if resp.status == 401 || resp.status == 403 {
            let msg = self.parse_error_body(&resp.body);
            return Err(SchemaRegError::auth(resp.status, msg));
        }
        if resp.status == 204 || (200..300).contains(&resp.status) {
            return Ok(());
        }
        let msg = self.parse_error_body(&resp.body);
        Err(SchemaRegError::http(resp.status, msg))
    }

    fn handle_json_response<T: serde::de::DeserializeOwned>(
        &self,
        status: u16,
        _content_type: Option<&str>,
        body: &[u8],
    ) -> Result<T> {
        if (200..300).contains(&status) {
            return serde_json::from_slice(body).map_err(|e| {
                SchemaRegError::invalid_state(format!("failed to parse Apicurio response: {e}"))
            });
        }
        let msg = self.parse_error_body(body);
        match status {
            401 | 403 => Err(SchemaRegError::auth(status, msg)),
            404 => Err(SchemaRegError::api(40401, msg)),
            409 => Err(SchemaRegError::api(40902, msg)),
            _ => Err(SchemaRegError::http(status, msg)),
        }
    }

    fn parse_error_body(&self, body: &[u8]) -> String {
        serde_json::from_slice::<ApicurioError>(body)
            .ok()
            .map(ApicurioError::into_message)
            .unwrap_or_else(|| {
                let preview = String::from_utf8_lossy(body);
                if preview.is_empty() {
                    "<empty>".to_string()
                } else {
                    preview.chars().take(256).collect()
                }
            })
    }

    /// Build a [`Schema`] from the raw content response of an Apicurio content endpoint.
    ///
    /// Reads `X-Registry-*` headers to populate schema metadata. Falls back to
    /// `fallback_id` and `fallback_subject` when headers are absent (e.g. older
    /// Apicurio versions that do not emit them by default).
    fn schema_from_content_response(
        &self,
        resp: crate::http::HttpResponse,
        fallback_id: Option<SchemaId>,
        fallback_subject: Option<&str>,
    ) -> Result<Arc<Schema>> {
        let schema_type = resp
            .headers
            .get(HDR_ARTIFACT_TYPE)
            .and_then(|s| s.parse::<SchemaType>().ok())
            .unwrap_or(SchemaType::Avro);

        let id = resp
            .headers
            .get(HDR_GLOBAL_ID)
            .and_then(|s| s.parse::<i64>().ok())
            .map(|v| -> Result<SchemaId> {
                if v < 0 || v > i64::from(u32::MAX) {
                    return Err(SchemaRegError::invalid_state(format!(
                        "Apicurio global_id {v} is out of u32 range"
                    )));
                }
                Ok(SchemaId::from(v as u32))
            })
            .transpose()?
            .or(fallback_id)
            .unwrap_or_else(|| SchemaId::from(0u32));

        let version = resp
            .headers
            .get(HDR_VERSION)
            .and_then(|s| s.parse::<i32>().ok())
            .map(SchemaVersion::new);

        let group_id = resp
            .headers
            .get(HDR_GROUP_ID)
            .cloned()
            .unwrap_or_else(|| DEFAULT_GROUP.to_string());

        let artifact_id_header = resp.headers.get(HDR_ARTIFACT_ID).cloned();

        let subject = artifact_id_header
            .map(|a| -> Arc<str> { Arc::from(format!("{group_id}/{a}").as_str()) })
            .or_else(|| fallback_subject.map(Arc::from));

        let schema_str = String::from_utf8(resp.body.to_vec()).map_err(|e| {
            SchemaRegError::wire_format(format!("invalid UTF-8 in Apicurio schema content: {e}"))
        })?;

        Ok(Arc::new(Schema {
            id,
            schema_type,
            schema: schema_str.into(),
            version,
            subject,
            references: Vec::new(),
        }))
    }

    /// Parse a version string or serde_json integer into [`SchemaVersion`].
    fn parse_version(v: &serde_json::Value) -> Option<SchemaVersion> {
        match v {
            serde_json::Value::String(s) => s.parse::<i32>().ok().map(SchemaVersion::new),
            serde_json::Value::Number(n) => n.as_i64().map(|n| SchemaVersion::new(n as i32)),
            _ => None,
        }
    }

    /// Convert [`SchemaReference`] slice to Apicurio's reference JSON format.
    fn to_reference_json(refs: &[SchemaReference]) -> Vec<ArtifactReferenceJson> {
        refs.iter()
            .map(|r| {
                let artifact = ArtifactId::from_subject(&r.subject);
                ArtifactReferenceJson {
                    group_id: Some(artifact.group),
                    artifact_id: artifact.artifact,
                    version: r.version.as_i32().to_string(),
                    name: r.name.clone(),
                }
            })
            .collect()
    }

    // ── Public operations (also called by SchemaRegistryClient impl) ──────────

    /// Retrieve the latest schema for the artifact identified by `subject`.
    ///
    /// Subject is parsed as `"{group}/{artifact}"` or `"{artifact}"` (default group).
    pub async fn get_latest_schema_impl(&self, subject: &str) -> Result<Arc<Schema>> {
        let artifact_id = ArtifactId::from_subject(subject);
        let url = self.api_url(&format!(
            "/groups/{}/artifacts/{}/versions/branch=latest/content?returnArtifactType=true",
            percent_encode(&artifact_id.group),
            percent_encode(&artifact_id.artifact),
        ));
        let resp = self.get_content(&url).await?;
        self.schema_from_content_response(resp, None, Some(subject))
    }

    /// Retrieve a specific version of the artifact.
    pub async fn get_schema_by_version_impl(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        let artifact_id = ArtifactId::from_subject(subject);
        let version_str = version.as_i32().to_string();
        let url = self.api_url(&format!(
            "/groups/{}/artifacts/{}/versions/{}/content?returnArtifactType=true",
            percent_encode(&artifact_id.group),
            percent_encode(&artifact_id.artifact),
            percent_encode(&version_str),
        ));
        let resp = self.get_content(&url).await?;
        self.schema_from_content_response(resp, None, Some(subject))
    }

    /// Register a schema and return its global ID.
    ///
    /// Uses `ifExists=FIND_OR_CREATE_VERSION` so the operation is idempotent:
    /// if the same content is already registered, the existing version is returned.
    pub async fn register_schema_impl(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        let artifact_id = ArtifactId::from_subject(subject);
        let url = self.api_url(&format!(
            "/groups/{}/artifacts?ifExists=FIND_OR_CREATE_VERSION",
            percent_encode(&artifact_id.group),
        ));
        let refs = Self::to_reference_json(references);
        let content_type = schema_content_type(schema_type);
        let req = CreateArtifactRequest {
            artifact_id: &artifact_id.artifact,
            artifact_type: Some(schema_type.as_str()),
            first_version: CreateVersionWrapper {
                content: VersionContentRequest {
                    content: schema,
                    content_type,
                    references: refs,
                },
            },
        };
        let body = serde_json::to_vec(&req).map_err(|e| {
            SchemaRegError::invalid_state(format!("failed to serialise Apicurio request: {e}"))
        })?;
        let result: CreateArtifactResponse = self.post_json(&url, &body).await?;
        let global_id = result.version.global_id;
        if global_id < 0 || global_id > i64::from(u32::MAX) {
            return Err(SchemaRegError::invalid_state(format!(
                "Apicurio global_id {global_id} is out of u32 range"
            )));
        }
        Ok(SchemaId::from(global_id as u32))
    }

    /// Check compatibility of a schema against the latest version of the artifact.
    pub async fn check_compatibility_impl(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<bool> {
        let artifact_id = ArtifactId::from_subject(subject);
        let url = self.api_url(&format!(
            "/groups/{}/artifacts/{}/versions/branch=latest/compatibility",
            percent_encode(&artifact_id.group),
            percent_encode(&artifact_id.artifact),
        ));
        let refs = Self::to_reference_json(references);
        let content_type = schema_content_type(schema_type);
        let req = serde_json::json!({
            "content": {
                "content": schema,
                "contentType": content_type,
                "references": refs,
            }
        });
        let body = serde_json::to_vec(&req).map_err(|e| {
            SchemaRegError::invalid_state(format!("failed to serialise compatibility request: {e}"))
        })?;
        let result: CompatibilityTestResult = self.post_json(&url, &body).await?;
        Ok(result.compatible)
    }

    /// Delete the artifact (all its versions) identified by `subject`.
    pub async fn delete_artifact(&self, subject: &str) -> Result<()> {
        let artifact_id = ArtifactId::from_subject(subject);
        let url = self.api_url(&format!(
            "/groups/{}/artifacts/{}",
            percent_encode(&artifact_id.group),
            percent_encode(&artifact_id.artifact),
        ));
        self.delete_no_content(&url).await
    }

    /// List all subjects (formatted as `"{groupId}/{artifactId}"`).
    pub async fn list_subjects(&self, limit: usize) -> Result<Vec<String>> {
        let url = self.api_url(&format!("/search/artifacts?limit={limit}"));
        let results: ArtifactSearchResults = self.get_json(&url).await?;
        Ok(results
            .artifacts
            .into_iter()
            .map(|a| {
                let group = a.group_id.unwrap_or_else(|| DEFAULT_GROUP.to_string());
                format!("{group}/{}", a.artifact_id)
            })
            .collect())
    }

    /// List all versions of the artifact identified by `subject`.
    pub async fn list_versions(&self, subject: &str) -> Result<Vec<SchemaVersion>> {
        let artifact_id = ArtifactId::from_subject(subject);
        let url = self.api_url(&format!(
            "/groups/{}/artifacts/{}/versions?limit=500",
            percent_encode(&artifact_id.group),
            percent_encode(&artifact_id.artifact),
        ));
        let list: ArtifactVersionList = self.get_json(&url).await?;
        let mut versions = Vec::with_capacity(list.versions.len());
        for v in list.versions {
            if let Some(ver_val) = v.version
                && let Some(sv) = Self::parse_version(&ver_val)
            {
                versions.push(sv);
            }
        }
        Ok(versions)
    }
}

impl fmt::Debug for ApicurioSchemaRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let auth_desc = match &self.auth {
            ApicurioAuth::None => "none",
            ApicurioAuth::Basic { .. } => "basic(***)",
            ApicurioAuth::Bearer { .. } => "bearer(***)",
        };
        f.debug_struct("ApicurioSchemaRegistry")
            .field("base_url", &self.base_url)
            .field("auth", &auth_desc)
            .finish()
    }
}

impl SchemaRegistryClient for ApicurioSchemaRegistry {
    async fn get_schema_by_id(&self, id: SchemaId) -> Result<Arc<Schema>> {
        let url = self.api_url(&format!(
            "/ids/globalIds/{}?returnArtifactType=true",
            id.as_u32()
        ));
        let resp = self.get_content(&url).await?;
        self.schema_from_content_response(resp, Some(id), None)
    }

    async fn get_latest_schema(&self, subject: &str) -> Result<Arc<Schema>> {
        self.get_latest_schema_impl(subject).await
    }

    async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>> {
        self.get_schema_by_version_impl(subject, version).await
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.register_schema_impl(subject, schema, schema_type, references)
            .await
    }

    fn check_compatibility<'a>(
        &'a self,
        subject: &'a str,
        schema: &'a str,
        schema_type: SchemaType,
        references: &'a [SchemaReference],
    ) -> impl std::future::Future<Output = Result<bool>> + Send + 'a {
        self.check_compatibility_impl(subject, schema, schema_type, references)
    }

    async fn delete_subject<'a>(
        &'a self,
        subject: &'a str,
        _permanent: bool,
    ) -> Result<Vec<SchemaVersion>> {
        // Apicurio v3 DELETE /groups/{groupId}/artifacts/{artifactId} is always
        // a permanent delete. The `permanent` flag is not applicable; we treat
        // both modes as a hard delete and return an empty version list.
        self.delete_artifact(subject).await?;
        Ok(Vec::new())
    }

    fn get_subjects(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send + '_ {
        self.list_subjects(500)
    }

    fn get_versions<'a>(
        &'a self,
        subject: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<SchemaVersion>>> + Send + 'a {
        self.list_versions(subject)
    }

    async fn health_check(&self) -> Result<()> {
        // GET /search/artifacts?limit=1 is the lightest operation that verifies
        // connectivity and authentication without scanning all artifacts.
        let url = self.api_url("/search/artifacts?limit=1");
        self.get_json::<ArtifactSearchResults>(&url).await?;
        Ok(())
    }

    async fn set_compatibility(&self, subject: &str, level: CompatibilityLevel) -> Result<()> {
        validate_subject(subject)?;
        let artifact_id = ArtifactId::from_subject(subject);
        let url = self.api_url(&format!(
            "/groups/{}/artifacts/{}/rules/COMPATIBILITY",
            percent_encode(&artifact_id.group),
            percent_encode(&artifact_id.artifact),
        ));
        let req = SetCompatibilityRuleRequest {
            r#type: "COMPATIBILITY",
            config: level.as_str(),
        };
        let body = serde_json::to_vec(&req).map_err(|e| {
            SchemaRegError::invalid_state(format!(
                "failed to serialise compatibility rule request: {e}"
            ))
        })?;
        let _: serde_json::Value = self.put_json(&url, &body).await?;
        Ok(())
    }

    async fn get_compatibility(&self, subject: &str) -> Result<CompatibilityLevel> {
        validate_subject(subject)?;
        let artifact_id = ArtifactId::from_subject(subject);
        let url = self.api_url(&format!(
            "/groups/{}/artifacts/{}/rules/COMPATIBILITY",
            percent_encode(&artifact_id.group),
            percent_encode(&artifact_id.artifact),
        ));
        let resp: GetCompatibilityRuleResponse = self.get_json(&url).await?;
        resp.config.parse()
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`ApicurioSchemaRegistry`].
///
/// ```rust,no_run
/// use schemreg::apicurio::ApicurioSchemaRegistry;
///
/// # fn main() -> schemreg::error::Result<()> {
/// let registry = ApicurioSchemaRegistry::builder()
///     .url("https://registry.example.com")
///     .bearer_token("my-token")
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct ApicurioSchemaRegistryBuilder {
    url: Option<String>,
    auth: ApicurioAuth,
    request_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    root_certificates: Vec<reqwest::Certificate>,
    identity: Option<reqwest::Identity>,
    /// Maximum idle connections per host kept in the connection pool.
    pool_max_idle_per_host: Option<usize>,
}

impl Default for ApicurioSchemaRegistryBuilder {
    fn default() -> Self {
        Self {
            url: None,
            auth: ApicurioAuth::None,
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
            connect_timeout: None,
            root_certificates: Vec::new(),
            identity: None,
            pool_max_idle_per_host: None,
        }
    }
}

impl ApicurioSchemaRegistryBuilder {
    /// Set the Apicurio Registry URL (required).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Build an [`ApicurioSchemaRegistryBuilder`] from environment variables.
    ///
    /// Reads the following variables:
    /// - `APICURIO_REGISTRY_URL` (required)
    /// - `APICURIO_REGISTRY_USERNAME` + `APICURIO_REGISTRY_PASSWORD` → basic auth
    /// - `APICURIO_REGISTRY_BEARER_TOKEN` → bearer token auth
    ///
    /// If both basic-auth and bearer-token variables are set, bearer token
    /// takes precedence.
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("APICURIO_REGISTRY_URL").map_err(|_| {
            SchemaRegError::config("APICURIO_REGISTRY_URL environment variable is required")
        })?;

        let mut builder = Self::default().url(url);

        if let Ok(token) = std::env::var("APICURIO_REGISTRY_BEARER_TOKEN") {
            builder = builder.bearer_token(token);
        } else if let (Ok(user), Ok(pass)) = (
            std::env::var("APICURIO_REGISTRY_USERNAME"),
            std::env::var("APICURIO_REGISTRY_PASSWORD"),
        ) {
            builder = builder.basic_auth(user, pass);
        }

        Ok(builder)
    }

    /// Set basic authentication credentials.
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = ApicurioAuth::Basic {
            username: zeroize::Zeroizing::new(username.into()),
            password: zeroize::Zeroizing::new(password.into()),
        };
        self
    }

    /// Set a bearer token for authentication.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = ApicurioAuth::Bearer {
            token: zeroize::Zeroizing::new(token.into()),
        };
        self
    }

    /// Set the HTTP request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
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

    /// Add a custom root CA certificate to trust.
    ///
    /// Useful when the registry uses a private certificate authority (e.g. an
    /// internal PKI). Can be called multiple times to trust several CAs.
    pub fn add_root_certificate(mut self, cert: reqwest::Certificate) -> Self {
        self.root_certificates.push(cert);
        self
    }

    /// Set a client certificate and private key for mutual TLS (mTLS).
    pub fn identity(mut self, identity: reqwest::Identity) -> Self {
        self.identity = Some(identity);
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

    /// Build the [`ApicurioSchemaRegistry`] client.
    pub fn build(self) -> Result<ApicurioSchemaRegistry> {
        let raw_url = self.url.ok_or_else(|| {
            SchemaRegError::config("ApicurioSchemaRegistryBuilder: URL is required")
        })?;
        let url = normalize_url(raw_url);
        reject_embedded_credentials(&url)?;
        if url.starts_with("http://") && !matches!(&self.auth, ApicurioAuth::None) {
            tracing::warn!(
                url = %url,
                "Apicurio Registry URL uses plain HTTP with authentication — credentials will \
                 be sent in cleartext; use HTTPS in production"
            );
        }
        let client = HttpClient::with_config(HttpClientConfig {
            timeout: self.request_timeout,
            connect_timeout: self.connect_timeout,
            root_certificates: self.root_certificates,
            identity: self.identity,
            pool_max_idle_per_host: self.pool_max_idle_per_host,
        })?;
        Ok(ApicurioSchemaRegistry {
            client,
            base_url: url,
            auth: self.auth,
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Map [`SchemaType`] to the MIME content type used in Apicurio's
/// `VersionContent.contentType` field.
fn schema_content_type(schema_type: SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Avro | SchemaType::Json => "application/json",
        SchemaType::Protobuf => "application/x-protobuf",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_artifact_id_subject_roundtrip() {
        let id = ArtifactId::from_subject("default/orders-value");
        assert_eq!(id.group, "default");
        assert_eq!(id.artifact, "orders-value");
        assert_eq!(id.to_subject(), "default/orders-value");
    }

    #[test]
    fn test_artifact_id_bare_subject_uses_default_group() {
        let id = ArtifactId::from_subject("orders-value");
        assert_eq!(id.group, "default");
        assert_eq!(id.artifact, "orders-value");
    }

    #[test]
    fn test_artifact_id_custom_group() {
        let id = ArtifactId::from_subject("production/payments-key");
        assert_eq!(id.group, "production");
        assert_eq!(id.artifact, "payments-key");
        assert_eq!(id.to_subject(), "production/payments-key");
    }

    #[test]
    fn test_schema_content_type() {
        assert_eq!(schema_content_type(SchemaType::Avro), "application/json");
        assert_eq!(schema_content_type(SchemaType::Json), "application/json");
        assert_eq!(
            schema_content_type(SchemaType::Protobuf),
            "application/x-protobuf"
        );
    }

    #[test]
    fn test_debug_masks_credentials() {
        let registry = ApicurioSchemaRegistry {
            client: HttpClient::with_webpki_roots(None).unwrap(),
            base_url: "http://localhost:8080".to_string(),
            auth: ApicurioAuth::Bearer {
                token: zeroize::Zeroizing::new("secret".to_string()),
            },
        };
        let dbg = format!("{registry:?}");
        assert!(dbg.contains("bearer(***)"));
        assert!(!dbg.contains("secret"));
    }

    #[test]
    fn test_normalize_url_strips_trailing_slash() {
        assert_eq!(
            normalize_url("http://localhost:8080/".to_string()),
            "http://localhost:8080"
        );
        assert_eq!(
            normalize_url("http://localhost:8080".to_string()),
            "http://localhost:8080"
        );
    }

    #[test]
    fn test_reject_embedded_credentials() {
        assert!(reject_embedded_credentials("http://user:pass@localhost:8080").is_err());
        assert!(reject_embedded_credentials("http://localhost:8080").is_ok());
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ApicurioSchemaRegistry>();
    }
}
