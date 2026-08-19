//! Security-boundary tests.
//!
//! Every case here asserts that a hostile or malformed input is rejected
//! **before** any network I/O happens — the assertions rely on that, since the
//! clients point at an unresolvable host. A `Config` error proves the guard
//! fired locally; a `Network` error would mean the request went out.

#![cfg(any(feature = "confluent", feature = "apicurio"))]

/// Subject names that must never reach a URL path.
///
/// The percent-encoder deliberately preserves `.` so that dotted subjects like
/// `com.example.Order-value` survive intact — which means `..` also survives,
/// and an intermediate proxy or the registry's own router may collapse it.
/// `DELETE /subjects/..` collapsing to `DELETE /subjects` is the worst case.
const TRAVERSAL_SUBJECTS: &[&str] = &[
    "..",
    ".",
    "../admin",
    "../../config",
    "a/../b",
    "a/./b",
    // Neutralised by correct single-encoding here, but recovered by a
    // double-decoding proxy — rejected as defence in depth.
    "..%2fadmin",
    "%2e%2e/admin",
    "%2e%2e%2fadmin",
    "..%5cadmin",
    "..\\admin",
];

/// Subject names that are structurally invalid.
const MALFORMED_SUBJECTS: &[&str] = &[""];

#[cfg(feature = "confluent")]
mod confluent_guards {
    use super::*;
    use schemreg::{
        CompatibilityLevel, ConfluentSchemaRegistry, SchemaRegistryClient, SchemaType,
        SchemaVersion,
    };

    fn client() -> ConfluentSchemaRegistry {
        // Unresolvable by construction: if a guard fails to fire we get a
        // Network error instead of a Config error and the test fails loudly.
        ConfluentSchemaRegistry::new("https://registry.invalid").expect("client builds")
    }

    /// Every operation that interpolates a subject into a URL path must reject
    /// traversal segments. `delete_subject` is called out explicitly: it is the
    /// highest-impact endpoint, since a collapsed path targets *all* subjects.
    #[tokio::test]
    async fn every_subject_taking_operation_rejects_traversal() {
        let c = client();

        for &subject in TRAVERSAL_SUBJECTS.iter().chain(MALFORMED_SUBJECTS) {
            let checks: Vec<(&str, Option<schemreg::SchemaRegError>)> = vec![
                (
                    "get_latest_schema",
                    c.get_latest_schema(subject).await.err(),
                ),
                (
                    "get_schema_by_version",
                    c.get_schema_by_version(subject, SchemaVersion::new(1))
                        .await
                        .err(),
                ),
                (
                    "register_schema",
                    c.register_schema(subject, "{}", SchemaType::Avro, &[])
                        .await
                        .err(),
                ),
                (
                    "check_compatibility",
                    c.check_compatibility(subject, "{}", SchemaType::Avro, &[])
                        .await
                        .err(),
                ),
                ("get_versions", c.get_versions(subject).await.err()),
                (
                    "delete_subject",
                    c.delete_subject(subject, true).await.err(),
                ),
            ];

            for (op, err) in checks {
                let err =
                    err.unwrap_or_else(|| panic!("{op}({subject:?}) must fail before any request"));
                assert!(
                    err.is_config_error(),
                    "{op}({subject:?}) must be rejected locally as a Config error, got: {err}"
                );
            }
        }
    }

    /// The compatibility-config endpoints treat `""` as "the global default",
    /// so they must accept it while still rejecting traversal.
    #[tokio::test]
    async fn compatibility_endpoints_reject_traversal_but_allow_the_global_scope() {
        let c = client();
        for &subject in TRAVERSAL_SUBJECTS {
            let err = c
                .set_compatibility(subject, CompatibilityLevel::Full)
                .await
                .unwrap_err();
            assert!(
                err.is_config_error(),
                "set_compatibility({subject:?}): {err}"
            );

            let err = c.get_compatibility(subject).await.unwrap_err();
            assert!(
                err.is_config_error(),
                "get_compatibility({subject:?}): {err}"
            );
        }

        // The empty subject means "global config" and must not be rejected
        // locally — it should get as far as the (failing) network call.
        let err = c.get_compatibility("").await.unwrap_err();
        assert!(
            err.is_network_error(),
            "an empty subject selects the global config and must reach the network: {err}"
        );
    }

    /// Legitimate dotted and hyphenated subjects must survive the guard.
    #[tokio::test]
    async fn ordinary_subjects_are_not_rejected() {
        let c = client();
        for subject in [
            "orders-value",
            "com.example.Order-value",
            "a.b.c.d-key",
            "my-group/orders-value",
        ] {
            let err = c.get_versions(subject).await.unwrap_err();
            assert!(
                err.is_network_error(),
                "{subject:?} is a valid subject and must reach the network, got: {err}"
            );
        }
    }

    /// Credentials in the URL authority leak into logs, proxies, and shell
    /// history. Reject them at construction rather than transmitting them.
    #[test]
    fn urls_with_embedded_credentials_are_rejected() {
        for url in [
            "https://user:pass@registry.example.com",
            "http://admin:secret@localhost:8081",
            "https://token@registry.example.com/path",
        ] {
            assert!(
                ConfluentSchemaRegistry::new(url).is_err(),
                "{url} must be rejected"
            );
            assert!(
                ConfluentSchemaRegistry::builder().url(url).build().is_err(),
                "{url} must be rejected via the builder too"
            );
        }
    }

    /// Cleartext credentials must never cross a network.
    ///
    /// Loopback is exempt — `http://localhost:8081` with basic auth is the
    /// standard docker-compose and local-development setup, and that traffic
    /// never leaves the machine. This is the same "potentially trustworthy
    /// origin" rule browsers apply to secure-context features.
    #[test]
    fn cleartext_auth_off_loopback_is_refused() {
        for url in [
            "http://registry.example.com",
            // A private range is still a real network with real switches.
            "http://10.0.0.5:8081",
            "http://192.168.1.10:8081",
            // Must not be mistaken for localhost.
            "http://localhost.evil.com:8081",
        ] {
            let err = ConfluentSchemaRegistry::builder()
                .url(url)
                .basic_auth("user", "hunter2")
                .build()
                .unwrap_err();
            assert!(err.is_config_error(), "{url}: {err}");
            assert!(err.to_string().contains("HTTPS"), "{url}: {err}");
        }
    }

    /// Loopback with credentials must be permitted, over every spelling.
    #[test]
    fn cleartext_auth_on_loopback_is_permitted() {
        for url in [
            "http://localhost:8081",
            "http://127.0.0.1:8081",
            "http://[::1]:8081",
        ] {
            assert!(
                ConfluentSchemaRegistry::builder()
                    .url(url)
                    .basic_auth("user", "hunter2")
                    .build()
                    .is_ok(),
                "{url} is loopback and must be permitted for local development"
            );
        }
    }

    /// HTTPS with auth is always fine.
    #[test]
    fn auth_over_https_is_always_permitted() {
        assert!(
            ConfluentSchemaRegistry::builder()
                .url("https://registry.example.com")
                .basic_auth("user", "hunter2")
                .build()
                .is_ok()
        );
    }

    /// `Debug` output ends up in logs and panic messages. It must never
    /// contain credential material.
    #[test]
    fn debug_output_never_leaks_credentials() {
        let c = ConfluentSchemaRegistry::builder()
            .url("https://registry.example.com")
            .basic_auth("alice", "sup3r-s3cret")
            .build()
            .unwrap();
        let rendered = format!("{c:?}");
        assert!(!rendered.contains("sup3r-s3cret"), "{rendered}");
        assert!(!rendered.contains("alice"), "{rendered}");
        assert!(rendered.contains("basic(***)"), "{rendered}");

        let b = ConfluentSchemaRegistry::builder()
            .url("https://registry.example.com")
            .bearer_token("eyJhbGciOi-secret");
        let rendered = format!("{b:?}");
        assert!(!rendered.contains("eyJhbGciOi-secret"), "{rendered}");
        assert!(rendered.contains("bearer(***)"), "{rendered}");
    }

    /// An oversized subject cannot be used to build a giant request line.
    #[tokio::test]
    async fn oversized_subjects_are_rejected() {
        let c = client();
        let huge = "x".repeat(4096);
        let err = c.get_versions(&huge).await.unwrap_err();
        assert!(err.is_config_error(), "{err}");
    }
}

#[cfg(feature = "apicurio")]
mod apicurio_guards {
    use super::*;
    use schemreg::{ApicurioSchemaRegistry, SchemaRegistryClient, SchemaType, SchemaVersion};

    fn client() -> ApicurioSchemaRegistry {
        ApicurioSchemaRegistry::new("https://registry.invalid").expect("client builds")
    }

    /// Apicurio splits the subject into `{group}/{artifact}` *before* encoding,
    /// so `../secrets` yields the path `/groups/../artifacts/secrets`. Both
    /// components have to be validated, not just the joined string.
    #[tokio::test]
    async fn every_subject_taking_operation_rejects_traversal() {
        let c = client();

        for &subject in TRAVERSAL_SUBJECTS.iter().chain(MALFORMED_SUBJECTS) {
            let checks: Vec<(&str, Option<schemreg::SchemaRegError>)> = vec![
                (
                    "get_latest_schema",
                    c.get_latest_schema(subject).await.err(),
                ),
                (
                    "get_schema_by_version",
                    c.get_schema_by_version(subject, SchemaVersion::new(1))
                        .await
                        .err(),
                ),
                (
                    "register_schema",
                    c.register_schema(subject, "{}", SchemaType::Avro, &[])
                        .await
                        .err(),
                ),
                (
                    "check_compatibility",
                    c.check_compatibility(subject, "{}", SchemaType::Avro, &[])
                        .await
                        .err(),
                ),
                ("get_versions", c.get_versions(subject).await.err()),
                (
                    "delete_subject",
                    c.delete_subject(subject, true).await.err(),
                ),
                ("delete_artifact", c.delete_artifact(subject).await.err()),
                (
                    "delete_version",
                    c.delete_version(subject, SchemaVersion::new(1), true)
                        .await
                        .err(),
                ),
                (
                    "lookup_schema",
                    c.lookup_schema(subject, "{}", SchemaType::Avro, &[])
                        .await
                        .err(),
                ),
            ];

            for (op, err) in checks {
                let err =
                    err.unwrap_or_else(|| panic!("{op}({subject:?}) must fail before any request"));
                assert!(
                    err.is_config_error(),
                    "{op}({subject:?}) must be rejected locally as a Config error, got: {err}"
                );
            }
        }
    }

    /// The compatibility endpoints treat an empty subject as the registry-wide
    /// default (`/admin/rules/COMPATIBILITY`), matching the Confluent client, so
    /// they are tested against traversal alone — `""` is legitimate here and
    /// must reach the network rather than being rejected.
    #[tokio::test]
    async fn compatibility_endpoints_reject_traversal_but_allow_the_global_scope() {
        let c = client();

        for &subject in TRAVERSAL_SUBJECTS {
            for (op, err) in [
                (
                    "get_compatibility",
                    c.get_compatibility(subject).await.err(),
                ),
                (
                    "set_compatibility",
                    c.set_compatibility(subject, schemreg::CompatibilityLevel::Full)
                        .await
                        .err(),
                ),
            ] {
                let err =
                    err.unwrap_or_else(|| panic!("{op}({subject:?}) must fail before any request"));
                assert!(
                    err.is_config_error(),
                    "{op}({subject:?}) must be rejected locally, got: {err}"
                );
            }
        }

        let err = c.get_compatibility("").await.unwrap_err();
        assert!(
            err.is_network_error(),
            "an empty subject is the global scope and must reach the network, got: {err}"
        );
    }

    /// A subject whose group component is empty (`"/artifact"`) would build
    /// `/groups//artifacts/...` and hit the wrong route.
    #[tokio::test]
    async fn empty_address_components_are_rejected() {
        let c = client();
        for subject in ["/orders-value", "group/", "/"] {
            let err = c.get_latest_schema(subject).await.unwrap_err();
            assert!(
                err.is_config_error(),
                "{subject:?} has an empty component and must be rejected: {err}"
            );
        }
    }

    #[tokio::test]
    async fn ordinary_group_scoped_subjects_are_not_rejected() {
        let c = client();
        for subject in [
            "orders-value",
            "default/orders-value",
            "production/com.example.Order",
        ] {
            let err = c.get_versions(subject).await.unwrap_err();
            assert!(
                err.is_network_error(),
                "{subject:?} is valid and must reach the network, got: {err}"
            );
        }
    }

    #[test]
    fn urls_with_embedded_credentials_are_rejected() {
        assert!(ApicurioSchemaRegistry::new("https://u:p@registry.example.com").is_err());
        assert!(
            ApicurioSchemaRegistry::builder()
                .url("https://u:p@registry.example.com")
                .build()
                .is_err()
        );
    }

    /// Apicurio must apply the same cleartext-credential rule as Confluent —
    /// a divergence here would be a security hole that depends on which client
    /// you happened to pick.
    #[test]
    fn cleartext_auth_follows_the_same_loopback_rule() {
        assert!(
            ApicurioSchemaRegistry::builder()
                .url("http://registry.example.com")
                .bearer_token("t0ken")
                .build()
                .is_err(),
            "cleartext auth to a remote host must be refused"
        );
        assert!(
            ApicurioSchemaRegistry::builder()
                .url("http://localhost:8080")
                .bearer_token("t0ken")
                .build()
                .is_ok(),
            "loopback must be permitted for local development"
        );
    }

    #[test]
    fn debug_output_never_leaks_credentials() {
        let c = ApicurioSchemaRegistry::builder()
            .url("https://registry.example.com")
            .bearer_token("apicurio-secret-token")
            .build()
            .unwrap();
        let rendered = format!("{c:?}");
        assert!(!rendered.contains("apicurio-secret-token"), "{rendered}");
        assert!(rendered.contains("bearer(***)"), "{rendered}");
    }
}
