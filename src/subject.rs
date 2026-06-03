//! Subject name strategies for schema registry lookups.

use crate::error::{Result, SchemaRegError};

/// Strategy for deriving registry subject names from topics and records.
///
/// The subject name determines where schemas are registered and looked up.
/// The default [`TopicName`](Self::TopicName) strategy produces subjects
/// like `my-topic-key` and `my-topic-value`.
///
/// # Example
///
/// ```rust
/// use schemreg::SubjectNameStrategy;
///
/// let strategy = SubjectNameStrategy::TopicName;
/// let subject = strategy.subject_name("orders", None, false).unwrap();
/// assert_eq!(subject, "orders-value");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SubjectNameStrategy {
    /// `{topic}-key` / `{topic}-value`.
    ///
    /// This is the Confluent default. Each topic has one key schema and one
    /// value schema.
    #[default]
    TopicName,
    /// `{record_name}` (the record's fully qualified name).
    ///
    /// Useful when the same record type appears across multiple topics and
    /// should share a single schema entry.
    RecordName,
    /// `{topic}-{record_name}`.
    ///
    /// Useful when the same record type requires per-topic schema evolution.
    TopicRecordName,
    /// `{group_id}/{record_name}` — Apicurio Registry v3 group-scoped subject.
    ///
    /// Required when using Apicurio Registry v3's native group isolation
    /// feature. The `group_id` identifies the artifact group and is prepended
    /// to the fully-qualified record name with a `/` separator.
    ///
    /// # Example
    ///
    /// ```rust
    /// use schemreg::SubjectNameStrategy;
    ///
    /// let strategy = SubjectNameStrategy::ApicurioGroupRecordName {
    ///     group_id: "my-group".to_string(),
    /// };
    /// let subject = strategy.subject_name("orders", Some("com.example.Order"), false).unwrap();
    /// assert_eq!(subject, "my-group/com.example.Order");
    /// ```
    ApicurioGroupRecordName {
        /// The Apicurio artifact group identifier.
        group_id: String,
    },
}

impl SubjectNameStrategy {
    /// Derive the subject name for the given topic and optional record name.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `record_name` is `None` for
    /// strategies that require it ([`RecordName`](Self::RecordName),
    /// [`TopicRecordName`](Self::TopicRecordName)).
    pub fn subject_name(
        &self,
        topic: &str,
        record_name: Option<&str>,
        is_key: bool,
    ) -> Result<String> {
        match self {
            Self::TopicName => {
                let suffix = if is_key { "key" } else { "value" };
                Ok(format!("{topic}-{suffix}"))
            }
            Self::RecordName => {
                let name = record_name.ok_or_else(|| {
                    SchemaRegError::config("RecordName strategy requires a record name")
                })?;
                Ok(name.to_string())
            }
            Self::TopicRecordName => {
                let name = record_name.ok_or_else(|| {
                    SchemaRegError::config("TopicRecordName strategy requires a record name")
                })?;
                Ok(format!("{topic}-{name}"))
            }
            Self::ApicurioGroupRecordName { group_id } => {
                let name = record_name.ok_or_else(|| {
                    SchemaRegError::config(
                        "ApicurioGroupRecordName strategy requires a record name",
                    )
                })?;
                Ok(format!("{group_id}/{name}"))
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_default_is_topic_name() {
        assert_eq!(
            SubjectNameStrategy::default(),
            SubjectNameStrategy::TopicName
        );
    }

    #[test]
    fn test_subject_topic_name_key() {
        let s = SubjectNameStrategy::TopicName
            .subject_name("orders", None, true)
            .unwrap();
        assert_eq!(s, "orders-key");
    }

    #[test]
    fn test_subject_topic_name_value() {
        let s = SubjectNameStrategy::TopicName
            .subject_name("orders", None, false)
            .unwrap();
        assert_eq!(s, "orders-value");
    }

    #[test]
    fn test_subject_record_name() {
        let s = SubjectNameStrategy::RecordName
            .subject_name("orders", Some("com.example.Order"), false)
            .unwrap();
        assert_eq!(s, "com.example.Order");
    }

    #[test]
    fn test_subject_record_name_missing() {
        let result = SubjectNameStrategy::RecordName.subject_name("orders", None, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_subject_topic_record_name() {
        let s = SubjectNameStrategy::TopicRecordName
            .subject_name("orders", Some("Order"), true)
            .unwrap();
        assert_eq!(s, "orders-Order");
    }

    #[test]
    fn test_subject_topic_record_name_missing() {
        let result = SubjectNameStrategy::TopicRecordName.subject_name("orders", None, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_subject_apicurio_group_record_name() {
        let s = SubjectNameStrategy::ApicurioGroupRecordName {
            group_id: "my-group".to_string(),
        }
        .subject_name("orders", Some("com.example.Order"), false)
        .unwrap();
        assert_eq!(s, "my-group/com.example.Order");
    }

    #[test]
    fn test_subject_apicurio_group_record_name_missing() {
        let result = SubjectNameStrategy::ApicurioGroupRecordName {
            group_id: "g".to_string(),
        }
        .subject_name("orders", None, false);
        assert!(result.is_err());
    }
}
