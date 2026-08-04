//! Subject name strategies for schema registry lookups.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::error::{Result, SchemaRegError};
use crate::types::EncodeTarget;

/// A type-erased custom subject-name function.
///
/// Takes `(topic, record_name, target)` and returns the subject string.
pub type CustomSubjectFn =
    Arc<dyn Fn(&str, Option<&str>, EncodeTarget) -> Result<String> + Send + Sync>;

/// Strategy for deriving registry subject names from topics and records.
///
/// The subject name determines where schemas are registered and looked up.
/// The default [`TopicName`](Self::TopicName) strategy produces subjects
/// like `my-topic-key` and `my-topic-value`.
///
/// # Example
///
/// ```rust
/// use schemreg::{SubjectNameStrategy, EncodeTarget};
///
/// let strategy = SubjectNameStrategy::TopicName;
/// let subject = strategy.subject_name("orders", None, EncodeTarget::Value).unwrap();
/// assert_eq!(subject, "orders-value");
/// ```
#[derive(Clone, Default)]
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
    /// use schemreg::{SubjectNameStrategy, EncodeTarget};
    ///
    /// let strategy = SubjectNameStrategy::ApicurioGroupRecordName {
    ///     group_id: "my-group".to_string(),
    /// };
    /// let subject = strategy.subject_name("orders", Some("com.example.Order"), EncodeTarget::Value).unwrap();
    /// assert_eq!(subject, "my-group/com.example.Order");
    /// ```
    ApicurioGroupRecordName {
        /// The Apicurio artifact group identifier.
        group_id: String,
    },
    /// Fully custom subject name function.
    ///
    /// Allows complete control over subject naming without forking the crate.
    /// The function receives the topic name, optional record name, and
    /// [`EncodeTarget`] and returns the subject string.
    ///
    /// Two `Custom` instances are considered equal only if they share the same
    /// underlying `Arc` allocation (pointer equality).
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::sync::Arc;
    /// use schemreg::{SubjectNameStrategy, EncodeTarget};
    ///
    /// let strategy = SubjectNameStrategy::Custom(Arc::new(|topic, _record, target| {
    ///     Ok(format!("prefix-{topic}-{target}"))
    /// }));
    /// let subject = strategy.subject_name("orders", None, EncodeTarget::Value).unwrap();
    /// assert_eq!(subject, "prefix-orders-value");
    /// ```
    Custom(CustomSubjectFn),
}

impl std::fmt::Debug for SubjectNameStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TopicName => write!(f, "TopicName"),
            Self::RecordName => write!(f, "RecordName"),
            Self::TopicRecordName => write!(f, "TopicRecordName"),
            Self::ApicurioGroupRecordName { group_id } => f
                .debug_struct("ApicurioGroupRecordName")
                .field("group_id", group_id)
                .finish(),
            Self::Custom(_) => write!(f, "Custom(..)"),
        }
    }
}

impl PartialEq for SubjectNameStrategy {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::TopicName, Self::TopicName) => true,
            (Self::RecordName, Self::RecordName) => true,
            (Self::TopicRecordName, Self::TopicRecordName) => true,
            (
                Self::ApicurioGroupRecordName { group_id: a },
                Self::ApicurioGroupRecordName { group_id: b },
            ) => a == b,
            // Two Custom instances are equal only when they share the same Arc allocation.
            (Self::Custom(a), Self::Custom(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for SubjectNameStrategy {}

impl Hash for SubjectNameStrategy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::ApicurioGroupRecordName { group_id } => group_id.hash(state),
            // Hash by pointer address so that equal (ptr_eq) Customs hash the same.
            Self::Custom(f) => (Arc::as_ptr(f) as *const () as usize).hash(state),
            _ => {}
        }
    }
}

impl SubjectNameStrategy {
    /// Derive the subject name for the given topic, optional record name, and
    /// encode target (key or value).
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
        target: EncodeTarget,
    ) -> Result<String> {
        match self {
            Self::TopicName => Ok(format!("{topic}-{target}")),
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
            Self::Custom(f) => f(topic, record_name, target),
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
            .subject_name("orders", None, EncodeTarget::Key)
            .unwrap();
        assert_eq!(s, "orders-key");
    }

    #[test]
    fn test_subject_topic_name_value() {
        let s = SubjectNameStrategy::TopicName
            .subject_name("orders", None, EncodeTarget::Value)
            .unwrap();
        assert_eq!(s, "orders-value");
    }

    #[test]
    fn test_subject_record_name() {
        let s = SubjectNameStrategy::RecordName
            .subject_name("orders", Some("com.example.Order"), EncodeTarget::Value)
            .unwrap();
        assert_eq!(s, "com.example.Order");
    }

    #[test]
    fn test_subject_record_name_missing() {
        let result =
            SubjectNameStrategy::RecordName.subject_name("orders", None, EncodeTarget::Value);
        assert!(result.is_err());
    }

    #[test]
    fn test_subject_topic_record_name() {
        let s = SubjectNameStrategy::TopicRecordName
            .subject_name("orders", Some("Order"), EncodeTarget::Key)
            .unwrap();
        assert_eq!(s, "orders-Order");
    }

    #[test]
    fn test_subject_topic_record_name_missing() {
        let result =
            SubjectNameStrategy::TopicRecordName.subject_name("orders", None, EncodeTarget::Key);
        assert!(result.is_err());
    }

    #[test]
    fn test_subject_apicurio_group_record_name() {
        let s = SubjectNameStrategy::ApicurioGroupRecordName {
            group_id: "my-group".to_string(),
        }
        .subject_name("orders", Some("com.example.Order"), EncodeTarget::Value)
        .unwrap();
        assert_eq!(s, "my-group/com.example.Order");
    }

    #[test]
    fn test_subject_apicurio_group_record_name_missing() {
        let result = SubjectNameStrategy::ApicurioGroupRecordName {
            group_id: "g".to_string(),
        }
        .subject_name("orders", None, EncodeTarget::Value);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_strategy() {
        let strategy = SubjectNameStrategy::Custom(Arc::new(|topic, _, target| {
            Ok(format!("custom-{topic}-{target}"))
        }));
        let s = strategy
            .subject_name("orders", None, EncodeTarget::Value)
            .unwrap();
        assert_eq!(s, "custom-orders-value");
    }

    #[test]
    fn test_custom_strategy_ptr_eq() {
        let f: CustomSubjectFn = Arc::new(|topic, _, target| Ok(format!("{topic}-{target}")));
        let a = SubjectNameStrategy::Custom(Arc::clone(&f));
        let b = SubjectNameStrategy::Custom(Arc::clone(&f));
        let c = SubjectNameStrategy::Custom(Arc::new(|_, _, _| Ok("other".into())));
        assert_eq!(a, b, "same Arc should be equal");
        assert_ne!(a, c, "different Arcs should not be equal");
    }
}
