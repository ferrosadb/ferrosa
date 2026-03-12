//! Environment-variable-backed secrets provider.

use super::{SecretsError, SecretsProvider};

/// Reads secrets from environment variables.
///
/// Key mapping: `"superuser_password"` becomes `FERROSA_SUPERUSER_PASSWORD`,
/// `"s3.access_key_id"` becomes `FERROSA_S3_ACCESS_KEY_ID`.
pub struct EnvSecretsProvider;

impl SecretsProvider for EnvSecretsProvider {
    fn get_secret(&self, key: &str) -> std::result::Result<Option<String>, SecretsError> {
        let env_key = key_to_env_var(key);
        match std::env::var(&env_key) {
            Ok(val) => Ok(Some(val)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(e) => Err(SecretsError::Other(format!("env var {env_key}: {e}"))),
        }
    }
}

fn key_to_env_var(key: &str) -> String {
    // "superuser_password" -> "FERROSA_SUPERUSER_PASSWORD"
    // "s3.access_key_id" -> "FERROSA_S3_ACCESS_KEY_ID"
    let normalized = key.replace('.', "_").to_uppercase();
    format!("FERROSA_{normalized}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_to_env_var_simple_key() {
        assert_eq!(
            key_to_env_var("superuser_password"),
            "FERROSA_SUPERUSER_PASSWORD"
        );
    }

    #[test]
    fn key_to_env_var_dotted_key() {
        assert_eq!(
            key_to_env_var("s3.access_key_id"),
            "FERROSA_S3_ACCESS_KEY_ID"
        );
    }

    #[test]
    fn key_to_env_var_nested_dots() {
        assert_eq!(
            key_to_env_var("s3.secret_access_key"),
            "FERROSA_S3_SECRET_ACCESS_KEY"
        );
    }

    #[test]
    fn env_provider_reads_existing_var() {
        // Use a unique env var name to avoid collisions with parallel tests.
        let unique_key = "test_secret_reads_existing";
        let env_var = key_to_env_var(unique_key);
        // Safety: tests in this module are not parallelized on the same env var.
        unsafe {
            std::env::set_var(&env_var, "my-secret-value");
        }
        let provider = EnvSecretsProvider;
        let result = provider.get_secret(unique_key).unwrap();
        assert_eq!(result, Some("my-secret-value".to_string()));
        // Clean up
        unsafe {
            std::env::remove_var(&env_var);
        }
    }

    #[test]
    fn env_provider_returns_none_for_missing() {
        let provider = EnvSecretsProvider;
        let result = provider.get_secret("definitely_not_set_12345").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn secrets_error_display() {
        let err = SecretsError::ProviderUnavailable("gone".into());
        assert_eq!(err.to_string(), "secrets provider unavailable: gone");

        let err = SecretsError::AccessDenied("nope".into());
        assert_eq!(err.to_string(), "secrets access denied: nope");

        let err = SecretsError::Other("oops".into());
        assert_eq!(err.to_string(), "secrets error: oops");
    }

    #[test]
    fn secrets_provider_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<EnvSecretsProvider>();
        assert_sync::<EnvSecretsProvider>();
    }
}
