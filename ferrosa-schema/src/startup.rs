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

/// TLS posture of one client-facing listener, as resolved at startup.
///
/// Production mode requires every *enabled* listener to require TLS
/// (t_d5d122ba). A disabled listener is fine; an enabled listener whose
/// protocol has no TLS implementation at all is refused and must be disabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerTls {
    /// Human name used in the refusal, e.g. `"PostgreSQL"`.
    pub listener: &'static str,
    /// Whether this node will bind the listener.
    pub enabled: bool,
    /// Whether the listener can serve TLS at all.
    pub tls_supported: bool,
    /// Whether the listener is configured to refuse plaintext.
    pub require_tls: bool,
    /// The config key that makes the listener require TLS (or, when
    /// `tls_supported` is false, the key that disables it), named verbatim in
    /// the refusal.
    pub config_key: &'static str,
}

/// A violation of production deployment requirements.
#[non_exhaustive]
#[derive(Debug)]
pub enum ProductionViolation {
    /// An enabled client listener does not require TLS.
    ListenerTlsNotRequired {
        /// The listener's human name.
        listener: &'static str,
        /// The key to set (`[section] require_tls = true` plus cert/key).
        config_key: &'static str,
    },
    /// An enabled client listener has no TLS implementation, so it cannot
    /// run in production at all.
    ListenerWithoutTlsSupport {
        /// The listener's human name.
        listener: &'static str,
        /// The key that disables the listener.
        disable_key: &'static str,
    },
    /// CQL client connections do not require mutual TLS.
    CqlMutualTlsNotConfigured,
    /// Internode connections are not configured to require TLS.
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
    /// Authentication is disabled, leaving the CQL listener and the web
    /// `/admin/*` cluster-control API unauthenticated (FMEA FE-3).
    AuthDisabled,
}

impl std::fmt::Display for ProductionViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ListenerTlsNotRequired {
                listener,
                config_key,
            } => {
                // `[flight] require_tls` -> name `[flight] tls_cert` and
                // `[flight] tls_key` too, so the operator sees every key.
                match config_key.strip_suffix("require_tls") {
                    Some(section) => write!(
                        f,
                        "the {listener} listener does not require TLS; set {config_key} = true \
                         with {section}tls_cert and {section}tls_key"
                    ),
                    None => write!(
                        f,
                        "the {listener} listener does not require TLS; set {config_key} = true \
                         with its tls_cert and tls_key"
                    ),
                }
            }
            Self::ListenerWithoutTlsSupport {
                listener,
                disable_key,
            } => write!(
                f,
                "the {listener} listener has no TLS support and cannot run in production; \
                 disable it with {disable_key}"
            ),
            Self::CqlMutualTlsNotConfigured => {
                write!(f, "CQL mutual TLS (mTLS) is not configured")
            }
            Self::InternodeTlsNotConfigured => write!(
                f,
                "internode traffic is not required to be TLS; set [internode] require_tls = true \
                 with tls_cert, tls_key and tls_ca (or FERROSA_INTERNODE_REQUIRE_TLS / \
                 _TLS_CERT / _TLS_KEY / _TLS_CA)"
            ),
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
            Self::AuthDisabled => {
                write!(
                    f,
                    "authentication is disabled — the CQL listener and web /admin/* \
                     cluster-control API are unauthenticated; set [cql] auth_enabled = true"
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
    /// Whether client/admin authentication is enabled.
    pub auth_enabled: bool,
    /// TLS posture of every client listener the node may bind (CQL,
    /// PostgreSQL, graph HTTP, Bolt, SPARQL, web, Arrow Flight).
    pub listeners: Vec<ListenerTls>,
    /// Whether internode connections require TLS (`[internode] require_tls`).
    pub internode_require_tls: bool,
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

    if !config.auth_enabled {
        violations.push(ProductionViolation::AuthDisabled);
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
    // t_d5d122ba: every enabled client listener must refuse plaintext.
    for l in config.listeners.iter().filter(|l| l.enabled) {
        if !l.tls_supported {
            violations.push(ProductionViolation::ListenerWithoutTlsSupport {
                listener: l.listener,
                disable_key: l.config_key,
            });
        } else if !l.require_tls {
            violations.push(ProductionViolation::ListenerTlsNotRequired {
                listener: l.listener,
                config_key: l.config_key,
            });
        }
    }
    // One-way server TLS is the production floor for internode traffic;
    // mutual TLS is tracked separately (t_b6c820f4).
    if !config.internode_require_tls {
        violations.push(ProductionViolation::InternodeTlsNotConfigured);
    }
    violations
}

impl ProductionViolation {
    /// Whether this violation must BLOCK startup (it is operator-fixable today)
    /// versus only warn. Some violations describe weaknesses whose underlying
    /// config is not operator-configurable yet (the schema's password policy and
    /// secrets provider are currently hardcoded) — blocking on those would leave
    /// no recourse, so they only warn until that config is plumbed.
    pub fn blocks_startup(&self) -> bool {
        !matches!(
            self,
            Self::EnvSecretsInProduction | Self::PasswordPolicyBelowMinimum
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every listener the binary starts, each enabled and requiring TLS.
    fn all_listeners_tls() -> Vec<ListenerTls> {
        [
            ("CQL", "[cql] require_tls"),
            ("PostgreSQL", "[postgres] require_tls"),
            ("graph HTTP", "[graph] require_tls"),
            ("Bolt", "[graph] require_tls"),
            ("SPARQL", "[sparql] require_tls"),
            ("web console", "[web] require_tls"),
            ("Arrow Flight", "[flight] require_tls"),
        ]
        .into_iter()
        .map(|(listener, config_key)| ListenerTls {
            listener,
            enabled: true,
            tls_supported: true,
            require_tls: true,
            config_key,
        })
        .collect()
    }

    /// Helper: build a production config that passes all checks.
    fn passing_production_config() -> ProductionCheckConfig {
        ProductionCheckConfig {
            mode: DeploymentMode::Production,
            password_policy: PasswordPolicy::iso27001(),
            has_superuser_password: true,
            secrets_provider_type: "aws-secrets-manager".into(), // pragma: allowlist secret
            s3_allow_http: false,
            auth_enabled: true,
            listeners: all_listeners_tls(),
            internode_require_tls: true,
        }
    }

    fn with_listener(name: &str, edit: impl Fn(&mut ListenerTls)) -> ProductionCheckConfig {
        let mut config = passing_production_config();
        let listener = config
            .listeners
            .iter_mut()
            .find(|l| l.listener == name)
            .unwrap_or_else(|| panic!("no listener {name}"));
        edit(listener);
        config
    }

    /// t_d5d122ba: each enabled listener that does not require TLS is a
    /// blocking violation that names the listener and its config key.
    #[test]
    fn every_enabled_listener_without_required_tls_blocks_startup() {
        for (name, key) in [
            ("CQL", "[cql] require_tls"),
            ("PostgreSQL", "[postgres] require_tls"),
            ("graph HTTP", "[graph] require_tls"),
            ("Bolt", "[graph] require_tls"),
            ("SPARQL", "[sparql] require_tls"),
            ("web console", "[web] require_tls"),
            ("Arrow Flight", "[flight] require_tls"),
        ] {
            let config = with_listener(name, |l| l.require_tls = false);
            let violations = validate_production_requirements(&config);
            let found = violations.iter().find(|v| {
                matches!(v, ProductionViolation::ListenerTlsNotRequired { listener, .. }
                    if *listener == name)
            });
            let found = found.unwrap_or_else(|| {
                panic!("{name} without require_tls must be a violation; got {violations:?}")
            });
            assert!(found.blocks_startup(), "{name}: must block startup");
            let message = found.to_string();
            let section = key.strip_suffix("require_tls").unwrap();
            let named = [
                key.to_string(),
                format!("{section}tls_cert"),
                format!("{section}tls_key"),
            ];
            assert!(
                message.contains(name) && named.iter().all(|k| message.contains(k.as_str())),
                "{name}: the refusal must name the listener and {named:?}: {message}"
            );
            assert_eq!(
                violations.len(),
                1,
                "{name}: only that listener: {violations:?}"
            );
        }
    }

    #[test]
    fn a_disabled_listener_without_tls_is_fine() {
        let config = with_listener("SPARQL", |l| {
            l.enabled = false;
            l.require_tls = false;
        });
        assert!(
            validate_production_requirements(&config).is_empty(),
            "a disabled listener needs no TLS"
        );
    }

    #[test]
    fn an_enabled_listener_with_no_tls_support_is_refused_with_its_disable_key() {
        let mut config = passing_production_config();
        // No shipped listener lacks TLS today (Arrow Flight gained it in
        // t_58db6320); the rule stays for any future one.
        config.listeners.push(ListenerTls {
            listener: "Example",
            enabled: true,
            tls_supported: false,
            require_tls: false,
            config_key: "[example] enabled = false",
        });
        let violations = validate_production_requirements(&config);
        let [v] = violations.as_slice() else {
            panic!("expected exactly the Example violation, got {violations:?}");
        };
        assert!(matches!(
            v,
            ProductionViolation::ListenerWithoutTlsSupport {
                listener: "Example",
                ..
            }
        ));
        assert!(v.blocks_startup());
        assert!(v.to_string().contains("[example] enabled = false"), "{v}");

        // Disabled, it is fine.
        config.listeners.last_mut().unwrap().enabled = false;
        assert!(validate_production_requirements(&config).is_empty());
    }

    #[test]
    fn production_without_internode_tls_blocks_startup() {
        let mut config = passing_production_config();
        config.internode_require_tls = false;
        let violations = validate_production_requirements(&config);
        let v = violations
            .iter()
            .find(|v| matches!(v, ProductionViolation::InternodeTlsNotConfigured))
            .unwrap_or_else(|| panic!("internode TLS must be required: {violations:?}"));
        assert!(v.blocks_startup());
        assert!(v.to_string().contains("[internode] require_tls"), "{v}");
    }

    #[test]
    fn development_mode_ignores_listener_tls() {
        let mut config = with_listener("web console", |l| l.require_tls = false);
        config.mode = DeploymentMode::Development;
        config.internode_require_tls = false;
        assert!(validate_production_requirements(&config).is_empty());
    }

    #[test]
    fn production_with_auth_disabled_is_a_violation() {
        let mut config = passing_production_config();
        config.auth_enabled = false;
        let violations = validate_production_requirements(&config);
        assert!(
            violations
                .iter()
                .any(|v| matches!(v, ProductionViolation::AuthDisabled)),
            "production mode with auth disabled must report AuthDisabled; got {violations:?}"
        );
    }

    #[test]
    fn development_with_auth_disabled_is_allowed() {
        let mut config = passing_production_config();
        config.mode = DeploymentMode::Development;
        config.auth_enabled = false;
        assert!(
            validate_production_requirements(&config).is_empty(),
            "development mode must not enforce auth"
        );
    }

    #[test]
    fn hardcoded_weaknesses_warn_but_do_not_block() {
        // Secrets provider + password policy are not operator-configurable yet,
        // so they must WARN (not block) until that config is plumbed.
        assert!(!ProductionViolation::EnvSecretsInProduction.blocks_startup());
        assert!(!ProductionViolation::PasswordPolicyBelowMinimum.blocks_startup());
        // The operator-fixable security requirements block startup.
        assert!(ProductionViolation::AuthDisabled.blocks_startup());
        assert!(ProductionViolation::ListenerTlsNotRequired {
            listener: "CQL",
            config_key: "[cql] require_tls",
        }
        .blocks_startup());
        assert!(ProductionViolation::InternodeTlsNotConfigured.blocks_startup());
        assert!(ProductionViolation::DefaultSuperuserPassword.blocks_startup());
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
            auth_enabled: true,
            listeners: all_listeners_tls(),
            internode_require_tls: false,
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
            ProductionViolation::ListenerTlsNotRequired {
                listener: "CQL",
                config_key: "[cql] require_tls",
            },
            ProductionViolation::ListenerWithoutTlsSupport {
                listener: "Example",
                disable_key: "[example] enabled = false",
            },
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
