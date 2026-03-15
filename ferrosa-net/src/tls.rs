//! TLS helpers for internode connections.
//!
//! Builds rustls `ServerConfig` and `ClientConfig` from `NetConfig` TLS fields.
//! Uses the ring crypto provider explicitly to avoid ambiguity when multiple
//! providers are compiled in (e.g., via hyper-rustls).

use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::NetConfig;
use crate::error::{NetError, Result};

/// Build a TLS acceptor for inbound internode connections.
/// Returns `None` if TLS is not configured.
pub fn build_tls_acceptor(config: &NetConfig) -> Result<Option<TlsAcceptor>> {
    match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let (certs, key) = load_cert_and_key(cert_path, key_path)?;
            let provider = rustls::crypto::ring::default_provider();
            let server_config = rustls::ServerConfig::builder_with_provider(provider.into())
                .with_safe_default_protocol_versions()
                .map_err(|e| NetError::Protocol(format!("TLS protocol error: {e}")))?
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| NetError::Protocol(format!("TLS config error: {e}")))?;
            tracing::info!("TLS enabled for internode connections");
            Ok(Some(TlsAcceptor::from(Arc::new(server_config))))
        }
        (None, None) => {
            if config.require_tls {
                return Err(NetError::Protocol(
                    "require_tls is true but no tls_cert_path/tls_key_path configured".into(),
                ));
            }
            Ok(None)
        }
        _ => Err(NetError::Protocol(
            "both tls_cert_path and tls_key_path must be set (or neither)".into(),
        )),
    }
}

/// Build a TLS connector for outbound internode connections.
/// Returns `None` if TLS is not configured.
pub fn build_tls_connector(config: &NetConfig) -> Result<Option<TlsConnector>> {
    if config.tls_cert_path.is_none() && config.tls_key_path.is_none() {
        return Ok(None);
    }

    let provider = rustls::crypto::ring::default_provider();
    let mut root_store = rustls::RootCertStore::empty();

    // Load CA cert if provided, otherwise use the server cert as trust anchor
    if let Some(ca_path) = &config.tls_ca_path {
        let ca_file = std::fs::File::open(ca_path)
            .map_err(|e| NetError::Protocol(format!("failed to open TLS CA {ca_path}: {e}")))?;
        let ca_certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut std::io::BufReader::new(ca_file))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| NetError::Protocol(format!("failed to parse CA certs: {e}")))?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| NetError::Protocol(format!("failed to add CA cert: {e}")))?;
        }
    } else if let Some(cert_path) = &config.tls_cert_path {
        // Self-signed mode: trust the server's own cert
        let cert_file = std::fs::File::open(cert_path)
            .map_err(|e| NetError::Protocol(format!("failed to open TLS cert {cert_path}: {e}")))?;
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| NetError::Protocol(format!("failed to parse certs: {e}")))?;
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| NetError::Protocol(format!("failed to add cert: {e}")))?;
        }
    }

    let client_config = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_safe_default_protocol_versions()
        .map_err(|e| NetError::Protocol(format!("TLS protocol error: {e}")))?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Some(TlsConnector::from(Arc::new(client_config))))
}

fn load_cert_and_key(
    cert_path: &str,
    key_path: &str,
) -> Result<(
    Vec<CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| NetError::Protocol(format!("failed to open TLS cert {cert_path}: {e}")))?;
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| NetError::Protocol(format!("failed to open TLS key {key_path}: {e}")))?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| NetError::Protocol(format!("failed to parse TLS certs: {e}")))?;

    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_file))
        .map_err(|e| NetError::Protocol(format!("failed to parse TLS key: {e}")))?
        .ok_or_else(|| NetError::Protocol("no private key found in TLS key file".into()))?;

    Ok((certs, key))
}
