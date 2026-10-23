//! TLS helpers shared by every Ferrosa listener.
//!
//! This module is the ONE place the process picks a rustls crypto provider
//! ([`crypto_provider`]). Internode, CQL, PostgreSQL, Bolt, Arrow Flight and
//! the HTTP front-ends all build their `rustls::ServerConfig` through
//! [`server_config_from_pem`], so swapping the provider (e.g. to a FIPS
//! module) is a one-line change here rather than a hunt across crates. The
//! provider is passed explicitly rather than installed as the process default,
//! which avoids ambiguity when several providers are compiled in (e.g. via
//! hyper-rustls).

use std::sync::Arc;

use rustls::crypto::CryptoProvider;
// PEM parsing comes from rustls-pki-types, the maintained successor of
// rustls-pemfile (RUSTSEC-2025-0134).
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::NetConfig;
use crate::error::{NetError, Result};

/// ALPN protocol list for HTTP listeners (HTTP/2 preferred, HTTP/1.1 fallback).
pub const HTTP_ALPN: &[&[u8]] = &[b"h2", b"http/1.1"];

/// ALPN protocol list for gRPC listeners (Arrow Flight): HTTP/2 only.
pub const GRPC_ALPN: &[&[u8]] = &[b"h2"];

/// The crypto provider every Ferrosa TLS endpoint uses.
///
/// Currently rustls' `ring` provider. Keep every `ServerConfig`/`ClientConfig`
/// construction routed through this function so the provider stays a single
/// decision.
pub fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Build a server-side rustls config (no client auth) from a PEM certificate
/// chain and private key, advertising `alpn` (empty for non-HTTP protocols).
///
/// Every failure names the file and the reason; nothing falls back to
/// plaintext.
pub fn server_config_from_pem(
    cert_path: &str,
    key_path: &str,
    alpn: &[&[u8]],
) -> Result<Arc<rustls::ServerConfig>> {
    let (certs, key) = load_cert_and_key(cert_path, key_path)?;
    if certs.is_empty() {
        return Err(NetError::Protocol(format!(
            "TLS cert file {cert_path} contains no certificates"
        )));
    }
    let mut server_config = rustls::ServerConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| NetError::Protocol(format!("TLS protocol error: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| NetError::Protocol(format!("TLS config error: {e}")))?;
    server_config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(server_config))
}

/// Build a TLS acceptor for a raw-TCP protocol (no ALPN) from PEM files.
pub fn server_acceptor_from_pem(cert_path: &str, key_path: &str) -> Result<TlsAcceptor> {
    Ok(TlsAcceptor::from(server_config_from_pem(
        cert_path,
        key_path,
        &[],
    )?))
}

/// Resolve an optional `(cert, key, require_tls)` triple into an acceptor.
///
/// * both paths set → `Some(acceptor)`
/// * neither set and not required → `None` (plaintext listener)
/// * neither set but required → error naming `listener`
/// * exactly one set → error naming `listener` (half-configured TLS is a typo,
///   never a reason to fall back to plaintext)
pub fn optional_server_config(
    listener: &str,
    cert_path: Option<&str>,
    key_path: Option<&str>,
    require_tls: bool,
    alpn: &[&[u8]],
) -> Result<Option<Arc<rustls::ServerConfig>>> {
    match (cert_path, key_path) {
        (Some(cert), Some(key)) => server_config_from_pem(cert, key, alpn).map(Some),
        (None, None) if require_tls => Err(NetError::Protocol(format!(
            "{listener}: require_tls is true but no TLS certificate/key is configured"
        ))),
        (None, None) => Ok(None),
        _ => Err(NetError::Protocol(format!(
            "{listener}: both the TLS certificate and key must be set (or neither)"
        ))),
    }
}

/// Build a TLS acceptor for inbound internode connections.
/// Returns `None` if TLS is not configured.
pub fn build_tls_acceptor(config: &NetConfig) -> Result<Option<TlsAcceptor>> {
    let server_config = optional_server_config(
        "internode",
        config.tls_cert_path.as_deref(),
        config.tls_key_path.as_deref(),
        config.require_tls,
        &[],
    )?;
    if server_config.is_some() {
        tracing::info!("TLS enabled for internode connections");
    }
    Ok(server_config.map(TlsAcceptor::from))
}

/// Build a TLS connector for outbound internode connections.
/// Returns `None` if TLS is not configured.
pub fn build_tls_connector(config: &NetConfig) -> Result<Option<TlsConnector>> {
    if config.tls_cert_path.is_none() && config.tls_key_path.is_none() {
        if config.require_tls {
            return Err(NetError::Protocol(
                "internode: require_tls is true but no TLS certificate/key is configured".into(),
            ));
        }
        return Ok(None);
    }

    let mut root_store = rustls::RootCertStore::empty();

    // Load CA cert if provided, otherwise use the server cert as trust anchor
    if let Some(ca_path) = &config.tls_ca_path {
        for cert in load_certs(ca_path, "TLS CA")? {
            root_store
                .add(cert)
                .map_err(|e| NetError::Protocol(format!("failed to add CA cert: {e}")))?;
        }
    } else if let Some(cert_path) = &config.tls_cert_path {
        // Self-signed mode: trust the server's own cert
        for cert in load_certs(cert_path, "TLS cert")? {
            root_store
                .add(cert)
                .map_err(|e| NetError::Protocol(format!("failed to add cert: {e}")))?;
        }
    }

    let client_config = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| NetError::Protocol(format!("TLS protocol error: {e}")))?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Some(TlsConnector::from(Arc::new(client_config))))
}

fn load_certs(path: &str, what: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .map_err(|e| NetError::Protocol(format!("failed to open {what} {path}: {e}")))?;
    CertificateDer::pem_reader_iter(std::io::BufReader::new(file))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| NetError::Protocol(format!("failed to parse {what} {path}: {e}")))
}

fn load_cert_and_key(
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs = load_certs(cert_path, "TLS cert")?;
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| NetError::Protocol(format!("failed to open TLS key {key_path}: {e}")))?;
    let key =
        PrivateKeyDer::from_pem_reader(std::io::BufReader::new(key_file)).map_err(|e| match e {
            pem::Error::NoItemsFound => {
                NetError::Protocol(format!("no private key found in TLS key file {key_path}"))
            }
            e => NetError::Protocol(format!("failed to parse TLS key {key_path}: {e}")),
        })?;

    Ok((certs, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_without_cert_names_the_listener() {
        let err = optional_server_config("postgres", None, None, true, &[]).unwrap_err();
        assert!(err.to_string().contains("postgres"), "{err}");
    }

    #[test]
    fn not_required_without_cert_is_plaintext() {
        assert!(optional_server_config("web", None, None, false, &[])
            .unwrap()
            .is_none());
    }

    #[test]
    fn half_configured_tls_is_an_error_not_plaintext() {
        let err = optional_server_config("sparql", Some("/c.pem"), None, false, &[]).unwrap_err();
        assert!(err.to_string().contains("both"), "{err}");
    }

    #[test]
    fn missing_cert_file_names_the_path() {
        let err = optional_server_config(
            "graph",
            Some("/nonexistent/c.pem"),
            Some("/nonexistent/k.pem"),
            false,
            HTTP_ALPN,
        )
        .unwrap_err();
        assert!(err.to_string().contains("/nonexistent/c.pem"), "{err}");
    }

    /// Writes `contents` to a fresh file under the temp dir and returns its path.
    fn write_temp(name: &str, contents: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "ferrosa-net-tls-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write temp file");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    #[test]
    fn pem_cert_and_key_round_trip() {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate self-signed cert");
        let cert_path = write_temp("cert.pem", &generated.cert.pem());
        let key_path = write_temp("key.pem", &generated.key_pair.serialize_pem());

        let (certs, key) = load_cert_and_key(&cert_path, &key_path).expect("load PEM pair");

        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].as_ref(), generated.cert.der().as_ref());
        assert_eq!(
            key.secret_der(),
            generated.key_pair.serialize_der().as_slice()
        );
    }

    #[test]
    fn key_file_without_a_key_is_named_as_such() {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate self-signed cert");
        let cert_path = write_temp("cert.pem", &generated.cert.pem());
        // A certificate where the key belongs: parses as PEM, holds no key.
        let key_path = write_temp("key.pem", &generated.cert.pem());

        let err = load_cert_and_key(&cert_path, &key_path).unwrap_err();

        assert!(err.to_string().contains("no private key found"), "{err}");
        assert!(err.to_string().contains(&key_path), "{err}");
    }

    #[test]
    fn malformed_cert_pem_is_a_parse_error() {
        let path = write_temp(
            "cert.pem",
            "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
        );

        let err = load_certs(&path, "TLS cert").unwrap_err();

        assert!(
            err.to_string().contains("failed to parse TLS cert"),
            "{err}"
        );
    }

    #[test]
    fn internode_connector_refuses_required_tls_without_cert() {
        let cfg = NetConfig {
            require_tls: true,
            ..NetConfig::default()
        };
        assert!(build_tls_connector(&cfg).is_err());
    }
}
