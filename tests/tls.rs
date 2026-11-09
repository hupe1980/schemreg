//! Real TLS handshakes against an in-process rustls server.
//!
//! Every other HTTP test talks plain HTTP to `wiremock`, so without this file
//! nothing would prove the TLS builder options do what they say: trust anchors,
//! the TLS 1.3 floor, mutual TLS, and the post-quantum key exchange rustls on
//! aws-lc-rs negotiates by default.
#![cfg(feature = "confluent")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use schemreg::tls::{Certificate, Identity};
use schemreg::{ConfluentSchemaRegistry, ConfluentSchemaRegistryBuilder, RetryPolicy};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{
    self, NamedGroup, ProtocolVersion, RootCertStore, ServerConfig, SupportedProtocolVersion,
};

/// A throwaway PKI: one CA, a server certificate for `127.0.0.1`, and a
/// client certificate for mTLS.
struct Pki {
    ca_pem: String,
    ca_der: CertificateDer<'static>,
    server_chain: Vec<CertificateDer<'static>>,
    server_key: PrivateKeyDer<'static>,
    client_identity_pem: String,
}

fn pki() -> Pki {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let server_cert = CertificateParams::new(vec!["127.0.0.1".to_string()])
        .unwrap()
        .signed_by(&server_key, &ca)
        .unwrap();

    let client_key = KeyPair::generate().unwrap();
    let client_cert = CertificateParams::new(vec!["client".to_string()])
        .unwrap()
        .signed_by(&client_key, &ca)
        .unwrap();

    Pki {
        ca_pem: ca.pem(),
        ca_der: ca.der().clone(),
        server_chain: vec![server_cert.der().clone()],
        server_key: PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        client_identity_pem: format!("{}{}", client_cert.pem(), client_key.serialize_pem()),
    }
}

/// What the server saw on a completed handshake.
#[derive(Debug)]
struct Handshake {
    version: ProtocolVersion,
    group: NamedGroup,
}

/// A running test server: its URL, completed handshakes, and TCP accepts.
struct Server {
    url: String,
    handshakes: mpsc::UnboundedReceiver<Handshake>,
    connections: Arc<AtomicUsize>,
}

/// Serve `GET /subjects?limit=1` → `[]` over TLS until the test ends.
async fn serve(
    pki: &Pki,
    versions: &[&'static SupportedProtocolVersion],
    require_client_cert: bool,
) -> Server {
    let builder = ServerConfig::builder_with_protocol_versions(versions);
    let builder = if require_client_cert {
        let mut roots = RootCertStore::empty();
        roots.add(pki.ca_der.clone()).unwrap();
        builder.with_client_cert_verifier(
            WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .unwrap(),
        )
    } else {
        builder.with_no_client_auth()
    };
    let config = builder
        .with_single_cert(pki.server_chain.clone(), pki.server_key.clone_key())
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let (tx, rx) = mpsc::unbounded_channel();
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::clone(&connections);
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            accepted.fetch_add(1, Ordering::SeqCst);
            let acceptor = acceptor.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                // A refused handshake is the expected outcome of several tests.
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let (_, conn) = tls.get_ref();
                let _ = tx.send(Handshake {
                    version: conn.protocol_version().unwrap(),
                    group: conn.negotiated_key_exchange_group().unwrap().name(),
                });
                let mut buf = [0u8; 4096];
                let mut seen = Vec::new();
                while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                    match tls.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => seen.extend_from_slice(&buf[..n]),
                    }
                }
                let _ = tls
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                          content-length: 2\r\nconnection: close\r\n\r\n[]",
                    )
                    .await;
                let _ = tls.shutdown().await;
            });
        }
    });
    Server {
        url,
        handshakes: rx,
        connections,
    }
}

fn builder(url: &str) -> ConfluentSchemaRegistryBuilder {
    ConfluentSchemaRegistry::builder()
        .url(url)
        .retry_policy(RetryPolicy::none())
}

fn ca(pki: &Pki) -> Certificate {
    Certificate::from_pem(pki.ca_pem.as_bytes()).unwrap()
}

const TLS13: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];
const TLS12: &[&SupportedProtocolVersion] = &[&rustls::version::TLS12];

#[tokio::test]
async fn trusted_ca_connects_over_tls13_with_post_quantum_key_exchange() {
    let pki = pki();
    let Server {
        url,
        handshakes: mut seen,
        ..
    } = serve(&pki, TLS13, false).await;

    let registry = builder(&url)
        .trust_only_root_certificates([ca(&pki)])
        .build()
        .unwrap();
    registry.health_check().await.unwrap();

    let handshake = seen.recv().await.unwrap();
    assert_eq!(handshake.version, ProtocolVersion::TLSv1_3);
    assert_eq!(
        handshake.group,
        NamedGroup::X25519MLKEM768,
        "rustls on aws-lc-rs must offer the hybrid post-quantum group first"
    );
}

#[tokio::test]
async fn an_unknown_ca_is_rejected() {
    let pki = pki();
    let Server { url, .. } = serve(&pki, TLS13, false).await;

    // Platform trust only — the throwaway CA is in no OS store.
    let err = builder(&url)
        .build()
        .unwrap()
        .health_check()
        .await
        .unwrap_err();
    assert_refused(&err);
}

#[tokio::test]
async fn trust_only_excludes_every_other_root() {
    let pki = pki();
    let other = self::pki();
    let Server { url, .. } = serve(&pki, TLS13, false).await;

    // Trusting only a different CA must not fall back to anything else.
    let err = builder(&url)
        .trust_only_root_certificates([ca(&other)])
        .build()
        .unwrap()
        .health_check()
        .await
        .unwrap_err();
    assert_refused(&err);
}

#[tokio::test]
async fn an_added_root_extends_the_platform_trust() {
    let pki = pki();
    let Server { url, .. } = serve(&pki, TLS13, false).await;

    builder(&url)
        .add_root_certificate(ca(&pki))
        .build()
        .unwrap()
        .health_check()
        .await
        .unwrap();
}

#[tokio::test]
async fn tls12_is_accepted_by_default_and_refused_when_tls13_is_required() {
    let pki = pki();
    let Server {
        url,
        handshakes: mut seen,
        ..
    } = serve(&pki, TLS12, false).await;

    // Control: the same server is reachable without the floor.
    builder(&url)
        .trust_only_root_certificates([ca(&pki)])
        .build()
        .unwrap()
        .health_check()
        .await
        .unwrap();
    assert_eq!(seen.recv().await.unwrap().version, ProtocolVersion::TLSv1_2);

    let err = builder(&url)
        .trust_only_root_certificates([ca(&pki)])
        .require_tls13()
        .build()
        .unwrap()
        .health_check()
        .await
        .unwrap_err();
    assert_refused(&err);
}

#[tokio::test]
async fn mutual_tls_presents_the_client_identity() {
    let pki = pki();
    let Server { url, .. } = serve(&pki, TLS13, true).await;

    // Without an identity the server refuses the handshake.
    assert!(
        builder(&url)
            .trust_only_root_certificates([ca(&pki)])
            .build()
            .unwrap()
            .health_check()
            .await
            .is_err()
    );

    builder(&url)
        .trust_only_root_certificates([ca(&pki)])
        .identity(Identity::from_pem(pki.client_identity_pem.as_bytes()).unwrap())
        .build()
        .unwrap()
        .health_check()
        .await
        .unwrap();
}

/// A refused handshake is a TLS error, and never retryable.
fn assert_refused(err: &schemreg::SchemaRegError) {
    assert!(err.is_tls_error(), "expected a TLS error, got {err:?}");
    assert!(!err.is_retryable(), "{err}");
}

#[tokio::test]
async fn a_refused_handshake_is_attempted_once_even_with_retries_enabled() {
    let pki = pki();
    let server = serve(&pki, TLS13, false).await;

    // The default policy retries transient failures three times.
    let err = ConfluentSchemaRegistry::builder()
        .url(&server.url)
        .build()
        .unwrap()
        .health_check()
        .await
        .unwrap_err();
    assert_refused(&err);
    assert_eq!(server.connections.load(Ordering::SeqCst), 1);
}

#[test]
fn adding_and_replacing_the_roots_at_once_is_a_config_error() {
    let pki = pki();
    let err = builder("https://registry.invalid")
        .add_root_certificate(ca(&pki))
        .trust_only_root_certificates([ca(&pki)])
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("choose one"), "{err}");
}
