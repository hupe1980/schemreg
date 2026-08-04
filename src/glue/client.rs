//! AWS Glue Schema Registry SDK client.
//!
//! Available when the `glue` feature is enabled.

use std::fmt;
use std::time::Duration;

use super::error::map_sdk_error;
use super::{GlueDataFormat, GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId};
use crate::error::{Result, SchemaRegError};
use std::sync::Arc;

/// Default registry name used by the AWS Glue Schema Registry.
const DEFAULT_REGISTRY_NAME: &str = "default-registry";

/// Maximum number of polling attempts when waiting for a schema version
/// to reach `AVAILABLE` status after registration.
const DEFAULT_POLL_MAX_ATTEMPTS: u32 = 10;

/// Delay between polling attempts.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// AWS SDK client for the [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).
pub struct AwsGlueSchemaRegistry {
    client: aws_sdk_glue::Client,
    registry_name: String,
    auto_register: bool,
    poll_max_attempts: u32,
    poll_interval: Duration,
}

impl AwsGlueSchemaRegistry {
    /// Create a client with the given Glue SDK client and registry name.
    pub fn new(client: aws_sdk_glue::Client, registry_name: impl Into<String>) -> Self {
        Self {
            client,
            registry_name: registry_name.into(),
            auto_register: false,
            poll_max_attempts: DEFAULT_POLL_MAX_ATTEMPTS,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Create a client from an AWS SDK config, using the default registry.
    pub fn from_config(config: &aws_config::SdkConfig) -> Self {
        Self::new(aws_sdk_glue::Client::new(config), DEFAULT_REGISTRY_NAME)
    }

    /// Create a builder for advanced configuration.
    pub fn builder(client: aws_sdk_glue::Client) -> AwsGlueSchemaRegistryBuilder {
        AwsGlueSchemaRegistryBuilder {
            client,
            registry_name: DEFAULT_REGISTRY_NAME.to_string(),
            auto_register: false,
            poll_max_attempts: DEFAULT_POLL_MAX_ATTEMPTS,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// The registry name this client targets.
    pub fn registry_name(&self) -> &str {
        &self.registry_name
    }

    /// Whether auto-registration is enabled for new schemas.
    pub fn auto_register(&self) -> bool {
        self.auto_register
    }

    async fn wait_for_available(
        &self,
        schema_version_id: &str,
    ) -> Result<aws_sdk_glue::operation::get_schema_version::GetSchemaVersionOutput> {
        for attempt in 0..self.poll_max_attempts {
            let response = self
                .client
                .get_schema_version()
                .schema_version_id(schema_version_id)
                .send()
                .await
                .map_err(map_sdk_error)?;

            match response.status() {
                Some(aws_sdk_glue::types::SchemaVersionStatus::Available) => {
                    return Ok(response);
                }
                Some(aws_sdk_glue::types::SchemaVersionStatus::Failure) => {
                    return Err(SchemaRegError::invalid_state(
                        "schema version registration failed (status: FAILURE)",
                    ));
                }
                Some(aws_sdk_glue::types::SchemaVersionStatus::Deleting) => {
                    return Err(SchemaRegError::invalid_state(
                        "schema version is being deleted",
                    ));
                }
                Some(_) | None => {
                    if attempt + 1 < self.poll_max_attempts {
                        tokio::time::sleep(self.poll_interval).await;
                    }
                }
            }
        }
        Err(SchemaRegError::invalid_state(format!(
            "schema version did not reach AVAILABLE status after {} attempts",
            self.poll_max_attempts
        )))
    }

    fn convert_data_format(format: &aws_sdk_glue::types::DataFormat) -> Result<GlueDataFormat> {
        match format {
            aws_sdk_glue::types::DataFormat::Avro => Ok(GlueDataFormat::Avro),
            aws_sdk_glue::types::DataFormat::Json => Ok(GlueDataFormat::Json),
            aws_sdk_glue::types::DataFormat::Protobuf => Ok(GlueDataFormat::Protobuf),
            other => Err(SchemaRegError::invalid_state(format!(
                "unsupported Glue data format: {other}"
            ))),
        }
    }

    fn to_sdk_data_format(format: GlueDataFormat) -> aws_sdk_glue::types::DataFormat {
        match format {
            GlueDataFormat::Avro => aws_sdk_glue::types::DataFormat::Avro,
            GlueDataFormat::Json => aws_sdk_glue::types::DataFormat::Json,
            GlueDataFormat::Protobuf => aws_sdk_glue::types::DataFormat::Protobuf,
        }
    }

    fn parse_version_id(s: &str) -> Result<GlueSchemaVersionId> {
        s.parse::<GlueSchemaVersionId>().map_err(|e| {
            SchemaRegError::invalid_state(format!("invalid schema version ID from registry: {e}"))
        })
    }

    async fn wait_and_parse_version_id(&self, version_id_str: &str) -> Result<GlueSchemaVersionId> {
        self.wait_for_available(version_id_str).await?;
        Self::parse_version_id(version_id_str)
    }
}

impl fmt::Debug for AwsGlueSchemaRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsGlueSchemaRegistry")
            .field("registry_name", &self.registry_name)
            .field("auto_register", &self.auto_register)
            .finish()
    }
}

impl GlueSchemaRegistryClient for AwsGlueSchemaRegistry {
    async fn get_schema_by_version_id(&self, id: GlueSchemaVersionId) -> Result<Arc<GlueSchema>> {
        let id_str = id.to_string();
        let response = self
            .client
            .get_schema_version()
            .schema_version_id(&id_str)
            .send()
            .await
            .map_err(map_sdk_error)?;

        let data_format = response
            .data_format()
            .ok_or_else(|| {
                SchemaRegError::invalid_state("schema version response missing data_format")
            })
            .and_then(Self::convert_data_format)?;

        let schema_definition = response
            .schema_definition()
            .ok_or_else(|| {
                SchemaRegError::invalid_state("schema version response missing schema_definition")
            })?
            .to_string();

        let mut schema = GlueSchema::new(id, data_format, schema_definition);
        if let Some(arn) = response.schema_arn()
            && let Some(version) = response.version_number()
        {
            schema = schema.with_metadata(arn, version);
        }
        Ok(Arc::new(schema))
    }

    /// Probe AWS Glue for connectivity, credentials, and registry existence.
    ///
    /// Issues `GetRegistry` against the configured registry name — the cheapest
    /// call that exercises credential resolution, network reachability, and the
    /// `glue:GetRegistry` IAM permission.
    async fn health_check(&self) -> Result<()> {
        self.client
            .get_registry()
            .registry_id(
                aws_sdk_glue::types::RegistryId::builder()
                    .registry_name(&self.registry_name)
                    .build(),
            )
            .send()
            .await
            .map_err(map_sdk_error)?;
        Ok(())
    }

    async fn register_schema(
        &self,
        schema_name: &str,
        schema: &str,
        data_format: GlueDataFormat,
    ) -> Result<GlueSchemaVersionId> {
        let sdk_format = Self::to_sdk_data_format(data_format);
        let schema_id = aws_sdk_glue::types::SchemaId::builder()
            .schema_name(schema_name)
            .registry_name(&self.registry_name)
            .build();

        // Check if schema definition already registered.
        let existing = self
            .client
            .get_schema_by_definition()
            .schema_id(schema_id.clone())
            .schema_definition(schema)
            .send()
            .await;

        if let Ok(response) = existing
            && let Some(status) = response.status()
            && *status == aws_sdk_glue::types::SchemaVersionStatus::Available
            && let Some(version_id_str) = response.schema_version_id()
        {
            return Self::parse_version_id(version_id_str);
        }

        // Try registering a new version.
        let register_result = self
            .client
            .register_schema_version()
            .schema_id(schema_id.clone())
            .schema_definition(schema)
            .send()
            .await;

        match register_result {
            Ok(response) => {
                let version_id_str = response.schema_version_id().ok_or_else(|| {
                    SchemaRegError::invalid_state("register response missing schema_version_id")
                })?;
                self.wait_and_parse_version_id(version_id_str).await
            }
            Err(register_err) => {
                if !self.auto_register {
                    return Err(map_sdk_error(register_err));
                }

                // Auto-register — create the schema (first version).
                let create_result = self
                    .client
                    .create_schema()
                    .registry_id(
                        aws_sdk_glue::types::RegistryId::builder()
                            .registry_name(&self.registry_name)
                            .build(),
                    )
                    .schema_name(schema_name)
                    .data_format(sdk_format)
                    .compatibility(aws_sdk_glue::types::Compatibility::Backward)
                    .schema_definition(schema)
                    .send()
                    .await;

                match create_result {
                    Ok(response) => {
                        let version_id_str = response.schema_version_id().ok_or_else(|| {
                            SchemaRegError::invalid_state(
                                "create schema response missing schema_version_id",
                            )
                        })?;
                        self.wait_and_parse_version_id(version_id_str).await
                    }
                    Err(create_err) => {
                        let fallback = self
                            .client
                            .register_schema_version()
                            .schema_id(schema_id)
                            .schema_definition(schema)
                            .send()
                            .await
                            .map_err(|e| {
                                // Preserve the retry semantics of the *final*
                                // failure; mention the create failure as context.
                                let classified = map_sdk_error(e);
                                tracing::warn!(
                                    create_error = %create_err,
                                    "auto-register CreateSchema failed; \
                                     RegisterSchemaVersion retry also failed"
                                );
                                classified
                            })?;

                        let version_id_str = fallback.schema_version_id().ok_or_else(|| {
                            SchemaRegError::invalid_state(
                                "register response missing schema_version_id",
                            )
                        })?;
                        self.wait_and_parse_version_id(version_id_str).await
                    }
                }
            }
        }
    }
}

/// Builder for [`AwsGlueSchemaRegistry`].
pub struct AwsGlueSchemaRegistryBuilder {
    client: aws_sdk_glue::Client,
    registry_name: String,
    auto_register: bool,
    poll_max_attempts: u32,
    poll_interval: Duration,
}

impl AwsGlueSchemaRegistryBuilder {
    /// Set the Glue registry name (default: `"default-registry"`).
    pub fn registry_name(mut self, name: impl Into<String>) -> Self {
        self.registry_name = name.into();
        self
    }

    /// Enable or disable auto-registration of new schemas.
    pub fn auto_register(mut self, enable: bool) -> Self {
        self.auto_register = enable;
        self
    }

    /// Set the maximum number of polling attempts when waiting for a schema
    /// version to become `AVAILABLE` after registration (default: 10).
    pub fn poll_max_attempts(mut self, attempts: u32) -> Self {
        self.poll_max_attempts = attempts;
        self
    }

    /// Set the delay between polling attempts (default: 3 seconds).
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Build the [`AwsGlueSchemaRegistry`] client.
    pub fn build(self) -> AwsGlueSchemaRegistry {
        AwsGlueSchemaRegistry {
            client: self.client,
            registry_name: self.registry_name,
            auto_register: self.auto_register,
            poll_max_attempts: self.poll_max_attempts,
            poll_interval: self.poll_interval,
        }
    }
}

impl fmt::Debug for AwsGlueSchemaRegistryBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsGlueSchemaRegistryBuilder")
            .field("registry_name", &self.registry_name)
            .field("auto_register", &self.auto_register)
            .field("poll_max_attempts", &self.poll_max_attempts)
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}
