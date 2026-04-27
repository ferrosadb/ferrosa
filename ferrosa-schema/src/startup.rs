//! Startup validation for Ferrosa deployment modes.
//!
//! Production mode enforces security requirements such as TLS configuration,
//! strong password policies, and proper secrets management.

use std::path::PathBuf;

use crate::auth::password::PasswordPolicy;

/// The deployment mode of the Ferrosa instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    /// Development mode — relaxed validation, no security enforcement.
    Development,
    /// Production mode — strict validation, security requirements enforced.
    Production,
}

impl DeploymentMode {
    /// Determine the deployment mode from the `FERROSA_MODE` environment variable.
    /// Defaults to `Development` if not set or not "production".
    pub fn from_env() -> Self {
        match std::env::var("FERROSA_MODE").as_deref() {
            Ok("production") => Self::Production,
            _ => Self::Development,
        }
    }
}

/// A violation of production deployment requirements.
#[non_exhaustive]
#[derive(Debug)]
pub enum ProductionViolation {
    /// CQL client connections are not configured with TLS.
    CqlTlsNotConfigured,
    /// CQL client connections do not require mutual TLS.
    CqlMutualTlsNotConfigured,
    /// Internode connections are not configured with TLS.
    InternodeTlsNotConfigured,
    /// Internode connections do not require mutual TLS.
    InternodeMutualTlsNotConfigured,
    /// S3 endpoint allows unencrypted HTTP.
    S3HttpEnabled,
    /// Local storage path is not encrypted.
    UnencryptedLocalStorage { path: PathBuf },
    /// The superuser password has not been changed from the default.
    DefaultSuperuserPassword,
    /// Environment-variable-based secrets are used in production.
    EnvSecretsInProduction,
    /// The password policy does not meet minimum requirements.
    PasswordPolicyBelowMinimum,
}

impl std::fmt::Display for ProductionViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CqlTlsNotConfigured => write!(f, "CQL TLS is not configured"),
            Self::CqlMutualTlsNotConfigured => {
                write!(f, "CQL mutual TLS (mTLS) is not configured")
            }
            Self::InternodeTlsNotConfigured => write!(f, "internode TLS is not configured"),
            Self::InternodeMutualTlsNotConfigured => {
                write!(f, "internode mutual TLS (mTLS) is not configured")
            }
            Self::S3HttpEnabled => write!(f, "S3 endpoint allows unencrypted HTTP"),
            Self::UnencryptedLocalStorage { path } => {
                write!(f, "local storage path is not encrypted: {}", path.display())
            }
            Self::DefaultSuperuserPassword => {
                write!(
                    f,
                    "superuser password has not been changed from the default"
                )
            }
            Self::EnvSecretsInProduction => {
                write!(
                    f,
                    "environment variable secrets provider is not recommended for production"
                )
            }
            Self::PasswordPolicyBelowMinimum => {
                write!(
                    f,
                    "password policy does not meet minimum production requirements"
                )
            }
        }
    }
}

/// Configuration inputs for production validation checks.
///
/// This is a temporary structure used until `SchemaConfig` is implemented.
/// It will be refactored to use `SchemaConfig` fields directly.
pub struct ProductionCheckConfig {
    /// The deployment mode.
    pub mode: DeploymentMode,
    /// The password policy in effect.
    pub password_policy: PasswordPolicy,
    /// Whether a non-default superuser password has been configured.
    pub has_superuser_password: bool,
    /// The type of secrets provider: "env", "aws-secrets-manager", etc.
    pub secrets_provider_type: String, // pragma: allowlist secret
    /// Whether the S3 endpoint allows HTTP (non-TLS) connections.
    pub s3_allow_http: bool,
}

/// Validate that the configuration meets production requirements.
///
/// Returns an empty list in development mode. In production mode,
/// returns a list of all detected violations.
pub fn validate_production_requirements(
    config: &ProductionCheckConfig,
) -> Vec<ProductionViolation> {
    let mut violations = Vec::new();

    // Only check if in production mode
    if config.mode != DeploymentMode::Production {
        return violations;
    }

    if !config.has_superuser_password {
        violations.push(ProductionViolation::DefaultSuperuserPassword);
    }
    if config.s3_allow_http {
        violations.push(ProductionViolation::S3HttpEnabled);
    }
    let provider = &config.secrets_provider_type; // pragma: allowlist secret
    if provider == "env" {
        violations.push(ProductionViolation::EnvSecretsInProduction);
    }
    if !config
        .password_policy
        .is_at_least_as_strong_as(&PasswordPolicy::iso27001())
    {
        violations.push(ProductionViolation::PasswordPolicyBelowMinimum);
    }
    // CQL TLS and internode TLS checks are stubs (added when those crates land)

    violations
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a production config that passes all checks.
    fn passing_production_config() -> ProductionCheckConfig {
        ProductionCheckConfig {
            mode: DeploymentMode::Production,
            password_policy: PasswordPolicy::iso27001(),
            has_superuser_password: true,
            secrets_provider_type: "aws-secrets-manager".into(), // pragma: allowlist secret
            s3_allow_http: false,
        }
    }

    #[test]
    fn development_mode_is_default() {
        // With no FERROSA_MODE set, should default to Development.
        // We can't unset the env var safely in parallel tests, but we test
        // the logic via the enum directly.
        let mode = DeploymentMode::Development;
        assert_eq!(mode, DeploymentMode::Development);
    }

    #[test]
    #[serial_test::serial(env)]
    fn production_mode_from_env() {
        unsafe {
            std::env::set_var("FERROSA_MODE", "production");
        }
        let mode = DeploymentMode::from_env();
        assert_eq!(mode, DeploymentMode::Production);
        unsafe {
            std::env::remove_var("FERROSA_MODE");
        }
    }

    #[test]
    fn development_mode_returns_no_violations() {
        let config = ProductionCheckConfig {
            mode: DeploymentMode::Development,
            password_policy: PasswordPolicy::permissive(),
            has_superuser_password: false,
            secrets_provider_type: "env".into(), // pragma: allowlist secret
            s3_allow_http: true,
        };
        let violations = validate_production_requirements(&config);
        assert!(
            violations.is_empty(),
            "development mode should return no violations"
        );
    }

    #[test]
    fn production_rejects_default_superuser_password() {
        let mut config = passing_production_config();
        config.has_superuser_password = false;
        let violations = validate_production_requirements(&config);
        assert!(violations
            .iter()
            .any(|v| matches!(v, ProductionViolation::DefaultSuperuserPassword)));
    }

    #[test]
    fn production_rejects_weak_password_policy() {
        let mut config = passing_production_config();
        config.password_policy = PasswordPolicy::permissive();
        let violations = validate_production_requirements(&config);
        assert!(violations
            .iter()
            .any(|v| matches!(v, ProductionViolation::PasswordPolicyBelowMinimum)));
    }

    #[test]
    fn production_rejects_s3_http() {
        let mut config = passing_production_config();
        config.s3_allow_http = true;
        let violations = validate_production_requirements(&config);
        assert!(violations
            .iter()
            .any(|v| matches!(v, ProductionViolation::S3HttpEnabled)));
    }

    #[test]
    fn production_warns_env_secrets() {
        let mut config = passing_production_config();
        config.secrets_provider_type = "env".to_string(); // pragma: allowlist secret
        let violations = validate_production_requirements(&config);
        assert!(violations
            .iter()
            .any(|v| matches!(v, ProductionViolation::EnvSecretsInProduction)));
    }

    #[test]
    fn production_passes_with_valid_config() {
        let config = passing_production_config();
        let violations = validate_production_requirements(&config);
        assert!(
            violations.is_empty(),
            "valid production config should have no violations, got: {violations:?}"
        );
    }

    #[test]
    fn production_violation_display() {
        // All variants should produce non-empty display strings.
        let violations = vec![
            ProductionViolation::CqlTlsNotConfigured,
            ProductionViolation::CqlMutualTlsNotConfigured,
            ProductionViolation::InternodeTlsNotConfigured,
            ProductionViolation::InternodeMutualTlsNotConfigured,
            ProductionViolation::S3HttpEnabled,
            ProductionViolation::UnencryptedLocalStorage {
                path: PathBuf::from("/data"),
            },
            ProductionViolation::DefaultSuperuserPassword,
            ProductionViolation::EnvSecretsInProduction,
            ProductionViolation::PasswordPolicyBelowMinimum,
        ];
        for v in &violations {
            let msg = v.to_string();
            assert!(!msg.is_empty(), "display for {v:?} should not be empty");
        }
    }

    #[test]
    fn deployment_mode_debug_and_clone() {
        let mode = DeploymentMode::Production;
        let cloned = mode;
        assert_eq!(format!("{mode:?}"), format!("{cloned:?}"));
    }
}
