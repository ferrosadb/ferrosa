//! Per-listener TLS settings.
//!
//! Every client listener reads the same three keys from its config section,
//! with the CQL listener's naming (`[cql] tls_cert` / `tls_key` /
//! `require_tls`) as the template:
//!
//! | TOML (wins)              | env fallback                    |
//! |--------------------------|---------------------------------|
//! | `[<section>] tls_cert`    | `FERROSA_<PREFIX>_TLS_CERT`     |
//! | `[<section>] tls_key`     | `FERROSA_<PREFIX>_TLS_KEY`      |
//! | `[<section>] require_tls` | `FERROSA_<PREFIX>_REQUIRE_TLS`  |
//!
//! An empty string means "not set" (compose files write `VAR=`). A
//! `require_tls` that is not a boolean stops startup: `ture` must never read as
//! `false` and quietly leave a listener in plaintext.

use crate::config_val_opt;

/// Resolved TLS settings for one listener.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListenerTlsConfig {
    /// PEM certificate chain path.
    pub cert: Option<String>,
    /// PEM private key path.
    pub key: Option<String>,
    /// Refuse to serve without TLS.
    pub require_tls: bool,
}

/// Parse a boolean setting strictly: `true`/`false`/`1`/`0` (any case).
pub fn parse_bool_setting(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(format!("expected true or false, got {other:?}")),
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.trim().is_empty())
}

/// Resolve `[section] tls_cert/tls_key/require_tls` with the `FERROSA_<PREFIX>_*`
/// env fallback. Returns an error naming the key when `require_tls` is not a
/// boolean.
pub fn resolve_listener_tls(
    file_config: &toml::Value,
    section: &str,
    env_prefix: &str,
) -> Result<ListenerTlsConfig, String> {
    let cert_env = format!("FERROSA_{env_prefix}_TLS_CERT");
    let key_env = format!("FERROSA_{env_prefix}_TLS_KEY");
    let require_env = format!("FERROSA_{env_prefix}_REQUIRE_TLS");
    let require_tls = match non_empty(config_val_opt(
        &require_env,
        file_config,
        section,
        "require_tls",
    )) {
        None => false,
        Some(raw) => parse_bool_setting(&raw)
            .map_err(|e| format!("invalid [{section}] require_tls / {require_env}: {e}"))?,
    };
    Ok(ListenerTlsConfig {
        cert: non_empty(config_val_opt(&cert_env, file_config, section, "tls_cert")),
        key: non_empty(config_val_opt(&key_env, file_config, section, "tls_key")),
        require_tls,
    })
}

/// [`resolve_listener_tls`], exiting the process with a named error on a
/// malformed value (same fail-loud contract as a malformed bind address).
pub fn resolve_listener_tls_or_exit(
    file_config: &toml::Value,
    section: &str,
    env_prefix: &str,
) -> ListenerTlsConfig {
    match resolve_listener_tls(file_config, section, env_prefix) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("FATAL: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toml(s: &str) -> toml::Value {
        s.parse().unwrap()
    }

    #[test]
    fn reads_the_three_keys_from_the_section() {
        let cfg = resolve_listener_tls(
            &toml("[postgres]\ntls_cert = \"/c.pem\"\ntls_key = \"/k.pem\"\nrequire_tls = true\n"),
            "postgres",
            "POSTGRES_TEST_UNSET",
        )
        .unwrap();
        assert_eq!(
            cfg,
            ListenerTlsConfig {
                cert: Some("/c.pem".into()),
                key: Some("/k.pem".into()),
                require_tls: true,
            }
        );
    }

    #[test]
    fn absent_means_plaintext_not_required() {
        let cfg = resolve_listener_tls(&toml(""), "postgres", "POSTGRES_TEST_UNSET").unwrap();
        assert_eq!(cfg, ListenerTlsConfig::default());
    }

    #[test]
    fn a_non_boolean_require_tls_is_an_error_not_false() {
        let err = resolve_listener_tls(
            &toml("[web]\nrequire_tls = \"ture\"\n"),
            "web",
            "WEB_TEST_UNSET",
        )
        .unwrap_err();
        assert!(err.contains("[web] require_tls"), "{err}");
    }

    #[test]
    fn empty_strings_mean_unset() {
        let cfg = resolve_listener_tls(
            &toml("[sparql]\ntls_cert = \"\"\ntls_key = \"\"\nrequire_tls = \"\"\n"),
            "sparql",
            "SPARQL_TEST_UNSET",
        )
        .unwrap();
        assert_eq!(cfg, ListenerTlsConfig::default());
    }
}
