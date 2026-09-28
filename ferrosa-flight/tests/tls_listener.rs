//! t_58db6320: the Arrow Flight listener serves gRPC over TLS.
//!
//! Starts the real Flight service through `server::serve_on_listener` with a
//! self-signed test certificate built by `FlightTlsConfig` (the shared
//! `ferrosa_net::tls` builder), then:
//! - a TLS client (tokio-rustls, same crypto provider) completes a Handshake
//!   and a ListFlights call;
//! - a plaintext gRPC client to the same port gets no RPC through;
//! - `require_tls` without a certificate, or half a certificate, refuses to
//!   build a config instead of serving plaintext.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Criteria, HandshakeRequest};
use futures::stream;
use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Request;

use ferrosa_flight::server::{self, FlightTlsConfig};

struct TestCert {
    _dir: tempfile::TempDir,
    der: rustls::pki_types::CertificateDer<'static>,
    config: FlightTlsConfig,
}

fn test_cert() -> TestCert {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("flight.crt");
    let key = dir.path().join("flight.key");
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    TestCert {
        der: certified.cert.der().clone(),
        config: FlightTlsConfig {
            cert_path: Some(cert.to_str().unwrap().into()),
            key_path: Some(key.to_str().unwrap().into()),
            require_tls: true,
        },
        _dir: dir,
    }
}

/// Start the Flight server with `tls` on an ephemeral port.
async fn start(
    tls: &FlightTlsConfig,
) -> (
    SocketAddr,
    tokio::task::JoinHandle<Result<(), server::ServeError>>,
    tempfile::TempDir,
) {
    let data = tempfile::tempdir().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(data.path());
    let server_config = tls.server_config().expect("TLS config builds");
    assert!(server_config.is_some(), "a certificate is configured");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = server::flight_service(state, b"server-key".to_vec());
    let handle = tokio::spawn(server::serve_on_listener(listener, svc, server_config));
    (addr, handle, data)
}

/// A tonic channel whose connector dials TCP and runs a rustls client
/// handshake (ALPN h2) trusting only `root`.
async fn tls_channel(
    addr: SocketAddr,
    root: rustls::pki_types::CertificateDer<'static>,
) -> Result<Channel, tonic::transport::Error> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(root).unwrap();
    let mut client =
        rustls::ClientConfig::builder_with_provider(ferrosa_net::tls::crypto_provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    client.alpn_protocols = vec![b"h2".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    Endpoint::from_shared(format!("http://localhost:{}", addr.port()))
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let connector = connector.clone();
            async move {
                let tcp = tokio::net::TcpStream::connect(addr).await?;
                let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
                let tls = connector.connect(name, tcp).await?;
                Ok::<_, std::io::Error>(TokioIo::new(tls))
            }
        }))
        .await
}

fn admin_handshake() -> impl futures::Stream<Item = HandshakeRequest> {
    stream::once(async {
        HandshakeRequest {
            protocol_version: 0,
            payload: b"ferrosa_admin\0ferrosa_admin".to_vec().into(),
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_client_completes_handshake_and_list_flights() {
    let cert = test_cert();
    let (addr, server, _data) = start(&cert.config).await;

    let channel = tls_channel(addr, cert.der.clone())
        .await
        .expect("TLS connect to Flight");
    let mut client = FlightServiceClient::new(channel);

    let token_msg = client
        .handshake(admin_handshake())
        .await
        .expect("handshake over TLS")
        .into_inner()
        .message()
        .await
        .expect("handshake stream")
        .expect("a handshake response");
    let token = String::from_utf8(token_msg.payload.to_vec()).unwrap();
    assert!(!token.is_empty(), "handshake over TLS issues a token");

    let mut req = Request::new(Criteria::default());
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client
        .list_flights(req)
        .await
        .expect("ListFlights over TLS");

    assert!(!server.is_finished(), "server still running");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_client_cannot_call_a_tls_flight_port() {
    let cert = test_cert();
    let (addr, server, _data) = start(&cert.config).await;

    // A plaintext (h2c) gRPC client: its HTTP/2 preface is not a TLS
    // ClientHello, so the server closes the connection. Either the connect
    // or the RPC must fail; nothing may succeed.
    let attempt = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut client = FlightServiceClient::connect(format!("http://{addr}")).await?;
        client
            .handshake(admin_handshake())
            .await
            .map(|_| ())
            .map_err(|status| -> Box<dyn std::error::Error + Send + Sync> { Box::new(status) })
    })
    .await
    .expect("plaintext attempt must fail promptly, not hang");
    assert!(
        attempt.is_err(),
        "a plaintext gRPC client must not complete an RPC on a TLS Flight port"
    );

    // The server survives the bad client and still serves TLS.
    let channel = tls_channel(addr, cert.der.clone())
        .await
        .expect("TLS connect after a plaintext attempt");
    FlightServiceClient::new(channel)
        .handshake(admin_handshake())
        .await
        .expect("TLS handshake RPC after a plaintext attempt");
    server.abort();
}

#[tokio::test]
async fn tls_client_rejects_an_untrusted_server_certificate() {
    let cert = test_cert();
    let (addr, server, _data) = start(&cert.config).await;
    let other = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    assert!(
        tls_channel(addr, other.cert.der().clone()).await.is_err(),
        "the client must verify the server certificate"
    );
    server.abort();
}

#[test]
fn require_tls_without_a_certificate_is_an_error_naming_flight() {
    let err = FlightTlsConfig {
        require_tls: true,
        ..FlightTlsConfig::default()
    }
    .server_config()
    .expect_err("require_tls with no certificate must not serve plaintext");
    assert!(err.to_string().contains("Arrow Flight"), "{err}");
}

#[test]
fn half_a_certificate_is_an_error_not_plaintext() {
    let err = FlightTlsConfig {
        cert_path: Some("/c.pem".into()),
        ..FlightTlsConfig::default()
    }
    .server_config()
    .expect_err("cert without key must not fall back to plaintext");
    assert!(err.to_string().contains("both"), "{err}");
}

#[test]
fn no_certificate_and_not_required_is_plaintext() {
    assert!(FlightTlsConfig::default()
        .server_config()
        .unwrap()
        .is_none());
}

#[test]
fn the_server_config_advertises_h2_only() {
    let cert = test_cert();
    let config = cert.config.server_config().unwrap().unwrap();
    assert_eq!(config.alpn_protocols, vec![b"h2".to_vec()]);
}

#[tokio::test]
async fn serve_service_refuses_to_bind_with_a_missing_certificate_file() {
    let data = tempfile::tempdir().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(data.path());
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let err = server::serve_service(
        addr,
        ferrosa_flight::service::FerrosaFlight::new(state, b"k".to_vec()),
        &FlightTlsConfig {
            cert_path: Some("/nonexistent/flight.crt".into()),
            key_path: Some("/nonexistent/flight.key".into()),
            require_tls: true,
        },
    )
    .await
    .expect_err("a missing certificate must stop the listener");
    assert!(err.to_string().contains("/nonexistent/flight.crt"), "{err}");
}
