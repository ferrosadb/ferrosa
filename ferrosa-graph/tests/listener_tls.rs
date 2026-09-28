//! t_d5d122ba: the graph HTTP and Bolt listeners serve TLS through the shared
//! `ferrosa_net::tls` machinery, and refuse to start when TLS is required but
//! not configured.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_graph::bolt::server::{start_bolt_server, BoltConfig};
use ferrosa_graph::http::{start_graph_disabled_http, GraphHttpConfig};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Certs {
    _dir: tempfile::TempDir,
    cert: String,
    key: String,
    der: rustls::pki_types::CertificateDer<'static>,
}

fn self_signed() -> Certs {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    Certs {
        der: certified.cert.der().clone(),
        cert: cert.to_str().unwrap().to_string(),
        key: key.to_str().unwrap().to_string(),
        _dir: dir,
    }
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn connector(trust: rustls::pki_types::CertificateDer<'static>) -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(trust).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(ferrosa_net::tls::crypto_provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

async fn connect_retry(addr: SocketAddr) -> tokio::net::TcpStream {
    for _ in 0..100 {
        if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("listener at {addr} never came up");
}

fn localhost() -> rustls::pki_types::ServerName<'static> {
    rustls::pki_types::ServerName::try_from("localhost").unwrap()
}

// ── graph HTTP ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn graph_http_serves_over_tls_when_configured() {
    let certs = self_signed();
    let config = GraphHttpConfig {
        bind_addr: free_addr(),
        tls_cert_path: Some(certs.cert.clone()),
        tls_key_path: Some(certs.key.clone()),
        require_tls: true,
        ..GraphHttpConfig::default()
    };
    let addr = config.bind_addr;
    tokio::spawn(async move { start_graph_disabled_http(&config).await });

    let tcp = connect_retry(addr).await;
    let mut tls = connector(certs.der)
        .connect(localhost(), tcp)
        .await
        .expect("TLS handshake with graph HTTP");
    tls.write_all(b"GET /graph/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    tls.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response);
    assert!(
        response.starts_with("HTTP/1.1 503"),
        "the disabled-engine stub answers over TLS: {response}"
    );

    // A plaintext request gets no HTTP response from a TLS-only port.
    let mut plain = connect_retry(addr).await;
    plain
        .write_all(b"GET /graph/health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), plain.read_to_end(&mut buf)).await;
    let text = String::from_utf8_lossy(&buf);
    assert!(
        !text.starts_with("HTTP/"),
        "plaintext must not be served on a TLS port (read {read:?}): {text}"
    );
}

#[tokio::test]
async fn graph_http_require_tls_without_a_certificate_refuses_to_start() {
    let config = GraphHttpConfig {
        bind_addr: free_addr(),
        require_tls: true,
        ..GraphHttpConfig::default()
    };
    let err = start_graph_disabled_http(&config)
        .await
        .expect_err("require_tls with no certificate must not serve plaintext");
    assert!(err.to_string().contains("require_tls"), "{err}");
}

// ── Bolt ────────────────────────────────────────────────────────────────────

fn graph_engine() -> (
    Arc<ferrosa_graph::engine::GraphEngine>,
    Arc<Schema>,
    tempfile::TempDir,
) {
    let schema = Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap(),
    );
    let tmp = tempfile::tempdir().unwrap();
    let storage_config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: tmp.path().to_path_buf(),
            checkpoint_dir: tmp.path().to_path_buf(),
            archive: None,
        },
        compaction: CompactionConfig::from_env(tmp.path().join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: tmp.path().to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    let storage = Arc::new(ferrosa_storage::StorageEngine::new(storage_config, None).unwrap());
    let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(
        ferrosa_cluster::write_path::WritePath::direct(Arc::clone(&storage)),
    ));
    let engine = Arc::new(ferrosa_graph::engine::GraphEngine::new(
        Arc::clone(&schema),
        storage,
        write_path,
        ferrosa_graph::executor::expand::GraphEngineConfig::default(),
        Duration::from_secs(300),
    ));
    (engine, schema, tmp)
}

/// Bolt magic preamble + four version proposals (5.4, 5.0, 4.4, none).
fn bolt_handshake() -> Vec<u8> {
    let mut hs = vec![0x60, 0x60, 0xB0, 0x17];
    hs.extend_from_slice(&[0, 0, 4, 5]);
    hs.extend_from_slice(&[0, 0, 0, 5]);
    hs.extend_from_slice(&[0, 0, 4, 4]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs
}

#[tokio::test]
async fn bolt_handshakes_over_tls_and_plaintext_gets_no_version() {
    let certs = self_signed();
    let (engine, schema, _dir) = graph_engine();
    let tls = ferrosa_net::tls::optional_server_config(
        "Bolt",
        Some(&certs.cert),
        Some(&certs.key),
        true,
        &[],
    )
    .unwrap();
    let config = BoltConfig {
        bind_addr: free_addr(),
        auth_disabled: true,
        tls,
        require_tls: true,
        ..BoltConfig::default()
    };
    let addr = config.bind_addr;
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(start_bolt_server(engine, schema, config, shutdown_rx));

    let tcp = connect_retry(addr).await;
    let mut tls = connector(certs.der)
        .connect(localhost(), tcp)
        .await
        .expect("TLS handshake with Bolt");
    tls.write_all(&bolt_handshake()).await.unwrap();
    let mut version = [0u8; 4];
    tls.read_exact(&mut version).await.unwrap();
    assert_ne!(version, [0, 0, 0, 0], "a Bolt version is agreed over TLS");

    let mut plain = connect_retry(addr).await;
    plain.write_all(&bolt_handshake()).await.unwrap();
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(12), plain.read_to_end(&mut buf)).await;
    assert!(
        buf.len() != 4,
        "a plaintext Bolt client must not get a version response (read {read:?}, {buf:?})"
    );
}

#[tokio::test]
async fn bolt_require_tls_without_a_certificate_refuses_to_start() {
    let (engine, schema, _dir) = graph_engine();
    let config = BoltConfig {
        bind_addr: free_addr(),
        require_tls: true,
        ..BoltConfig::default()
    };
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let err = start_bolt_server(engine, schema, config, shutdown_rx)
        .await
        .expect_err("require_tls with no certificate must not serve plaintext");
    assert!(err.to_string().contains("require_tls"), "{err}");
}
