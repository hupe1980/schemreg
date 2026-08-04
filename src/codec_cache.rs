//! Shared producer-side subject-resolution cache policy.
//!
//! `ConfluentSchemaEncoder`, `AvroSchemaEncoder`, `JsonSchemaEncoder`, and
//! `ProtobufSchemaEncoder` all need the same thing: a bounded, coalescing `subject → schema ID` map whose
//! misses trigger exactly one `register_schema` round-trip no matter how many
//! tasks race for the same subject.
//!
//! The first three used to hand-roll that with a `RwLock<HashMap>`, a
//! `Mutex<HashMap<_, Vec<oneshot::Sender<_>>>>`, and a drop guard apiece — three
//! near-identical copies of subtle cancellation logic, and three places for a
//! future edit to get it wrong. They now share
//! the crate's internal `InMemoryCache` with the registry and
//! decoder caches, so the cancellation and invalidation-race guarantees are
//! proven once and inherited everywhere.

use crate::error::SchemaRegError;

/// Default bound on the number of `subject → schema ID` mappings an encoder
/// keeps in memory.
///
/// Subjects are derived from the topic set a producer writes to, so this is
/// normally bounded by application configuration rather than by traffic. The
/// bound exists for the one case where it is not: a
/// [`SubjectNameStrategy::Custom`](crate::SubjectNameStrategy::Custom) that
/// derives subjects from message content.
///
/// Eviction is cheap here — schema registration is idempotent, so a re-resolved
/// subject costs one extra round-trip and returns the same ID.
pub const DEFAULT_MAX_SUBJECT_CACHE_ENTRIES: usize = 1000;

/// Error handed to coalesced waiters when the leader registration is cancelled
/// (its task was aborted, or the subject was invalidated mid-flight).
pub(crate) fn subject_resolution_cancelled(subject: &String) -> SchemaRegError {
    SchemaRegError::invalid_state(format!(
        "schema registration cancelled before completion for subject '{subject}'"
    ))
}
