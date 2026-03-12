//! Secrets management for Ferrosa.
//!
//! Provides a trait for retrieving secret values by key, plus an
//! environment-variable-backed implementation.

pub mod env;

pub use env::EnvSecretsProvider;

/// Retrieves secret values by key.
pub trait SecretsProvider: Send + Sync {
    fn get_secret(&self, key: &str) -> std::result::Result<Option<String>, SecretsError>;
}

#[derive(Debug)]
pub enum SecretsError {
    ProviderUnavailable(String),
    AccessDenied(String),
    Other(String),
}

impl std::fmt::Display for SecretsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProviderUnavailable(msg) => write!(f, "secrets provider unavailable: {msg}"),
            Self::AccessDenied(msg) => write!(f, "secrets access denied: {msg}"),
            Self::Other(msg) => write!(f, "secrets error: {msg}"),
        }
    }
}

impl std::error::Error for SecretsError {}
