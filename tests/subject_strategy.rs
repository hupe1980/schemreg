//! Integration tests for `SubjectNameStrategy`.
//!
//! Tests cover:
//! - All three strategy variants (TopicName, RecordName, TopicRecordName)
//! - Key vs value subject derivation
//! - Error cases (missing record name)
//! - Default variant
//! - Unicode and special characters in topic / record names

use schemreg::SubjectNameStrategy;

// ── Default ───────────────────────────────────────────────────────────────

#[test]
fn default_is_topic_name() {
    assert_eq!(
        SubjectNameStrategy::default(),
        SubjectNameStrategy::TopicName
    );
}

// ── TopicName ─────────────────────────────────────────────────────────────

#[test]
fn topic_name_value() {
    let s = SubjectNameStrategy::TopicName
        .subject_name("orders", None, false)
        .unwrap();
    assert_eq!(s, "orders-value");
}

#[test]
fn topic_name_key() {
    let s = SubjectNameStrategy::TopicName
        .subject_name("orders", None, true)
        .unwrap();
    assert_eq!(s, "orders-key");
}

#[test]
fn topic_name_ignores_record_name() {
    // record_name is irrelevant for TopicName — it must not be used.
    let s = SubjectNameStrategy::TopicName
        .subject_name("orders", Some("com.example.Order"), false)
        .unwrap();
    assert_eq!(s, "orders-value");
}

#[test]
fn topic_name_hyphenated_topic() {
    let s = SubjectNameStrategy::TopicName
        .subject_name("my-service-events", None, false)
        .unwrap();
    assert_eq!(s, "my-service-events-value");
}

#[test]
fn topic_name_dotted_topic() {
    let s = SubjectNameStrategy::TopicName
        .subject_name("org.company.orders", None, true)
        .unwrap();
    assert_eq!(s, "org.company.orders-key");
}

#[test]
fn topic_name_empty_topic() {
    // Empty topic is unusual but should produce "-value" / "-key".
    let s = SubjectNameStrategy::TopicName
        .subject_name("", None, false)
        .unwrap();
    assert_eq!(s, "-value");
}

// ── RecordName ────────────────────────────────────────────────────────────

#[test]
fn record_name_uses_record() {
    let s = SubjectNameStrategy::RecordName
        .subject_name("orders", Some("com.example.Order"), false)
        .unwrap();
    assert_eq!(s, "com.example.Order");
}

#[test]
fn record_name_ignores_key_flag() {
    // key/value distinction is irrelevant for RecordName.
    let key = SubjectNameStrategy::RecordName
        .subject_name("orders", Some("Order"), true)
        .unwrap();
    let val = SubjectNameStrategy::RecordName
        .subject_name("orders", Some("Order"), false)
        .unwrap();
    assert_eq!(key, val);
}

#[test]
fn record_name_ignores_topic() {
    let a = SubjectNameStrategy::RecordName
        .subject_name("topic-a", Some("MyRecord"), false)
        .unwrap();
    let b = SubjectNameStrategy::RecordName
        .subject_name("topic-b", Some("MyRecord"), false)
        .unwrap();
    assert_eq!(a, b, "subject must not depend on topic for RecordName");
}

#[test]
fn record_name_missing_record_name_is_error() {
    let err = SubjectNameStrategy::RecordName
        .subject_name("orders", None, false)
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("record name") || msg.contains("RecordName"),
        "{msg}"
    );
}

// ── TopicRecordName ───────────────────────────────────────────────────────

#[test]
fn topic_record_name_combines_both() {
    let s = SubjectNameStrategy::TopicRecordName
        .subject_name("orders", Some("Order"), false)
        .unwrap();
    assert_eq!(s, "orders-Order");
}

#[test]
fn topic_record_name_key_flag_does_not_affect_subject() {
    // key/value is irrelevant for TopicRecordName.
    let key = SubjectNameStrategy::TopicRecordName
        .subject_name("orders", Some("Order"), true)
        .unwrap();
    let val = SubjectNameStrategy::TopicRecordName
        .subject_name("orders", Some("Order"), false)
        .unwrap();
    assert_eq!(key, val);
}

#[test]
fn topic_record_name_fully_qualified() {
    let s = SubjectNameStrategy::TopicRecordName
        .subject_name("payments", Some("com.example.Payment"), false)
        .unwrap();
    assert_eq!(s, "payments-com.example.Payment");
}

#[test]
fn topic_record_name_missing_record_name_is_error() {
    let err = SubjectNameStrategy::TopicRecordName
        .subject_name("orders", None, false)
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("record name") || msg.contains("TopicRecordName"),
        "{msg}"
    );
}

// ── Unicode / special chars ───────────────────────────────────────────────

#[test]
fn topic_name_unicode_topic() {
    let s = SubjectNameStrategy::TopicName
        .subject_name("eventos-pédidos", None, false)
        .unwrap();
    assert_eq!(s, "eventos-pédidos-value");
}

#[test]
fn record_name_unicode_record() {
    let s = SubjectNameStrategy::RecordName
        .subject_name("t", Some("Événement"), false)
        .unwrap();
    assert_eq!(s, "Événement");
}

// ── Schema strategy is Clone + PartialEq ────────────────────────────────

#[test]
fn strategy_is_clone() {
    let s = SubjectNameStrategy::TopicName;
    let copy = s.clone();
    let _ = s.subject_name("t", None, false).unwrap();
    let _ = copy.subject_name("t", None, false).unwrap();
}

#[test]
fn strategy_equality() {
    assert_eq!(
        SubjectNameStrategy::TopicName,
        SubjectNameStrategy::TopicName
    );
    assert_ne!(
        SubjectNameStrategy::TopicName,
        SubjectNameStrategy::RecordName
    );
    assert_ne!(
        SubjectNameStrategy::RecordName,
        SubjectNameStrategy::TopicRecordName
    );
}
