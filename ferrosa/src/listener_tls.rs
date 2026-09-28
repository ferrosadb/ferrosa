//! Per-listener TLS settings, with one node-wide default.
//!
//! **Node-wide certificate.** `[tls]` sets one certificate for every listener
//! and for internode, so a node needs one certificate configured once:
//!
//! | TOML (wins)       | env fallback           | applies to                          |
//! |-------------------|------------------------|-------------------------------------|
//! | `[tls] cert`       | `FERROSA_TLS_CERT`     | every listener + internode          |
//! | `[tls] key`        | `FERROSA_TLS_KEY`      | every listener + internode          |
//! | `[tls] ca`         | `FERROSA_TLS_CA`       | internode peer verification         |
//! | `[tls] require`    | `FERROSA_TLS_REQUIRE`  | every listener + internode          |
//!
//! **Per-listener override.** Every client listener still reads its own three
//! keys, with the CQL listener's naming (`[cql] tls_cert` / `tls_key` /
//! `require_tls`) as the template:
//!
//! | TOML (wins)              | env fallback                    |
//! |--------------------------|---------------------------------|
//! | `[<section>] tls_cert`    | `FERROSA_<PREFIX>_TLS_CERT`     |
//! | `[<section>] tls_key`     | `FERROSA_<PREFIX>_TLS_KEY`      |
//! | `[<section>] require_tls` | `FERROSA_<PREFIX>_REQUIRE_TLS`  |
//!
//! The most specific setting wins: a listener's own keys, then `[tls]`. The
//! certificate and key are taken as a pair — if a section names either one,
//! the section's pair is used (so half a pair is still an error, never a mix
//! of a section certificate with the node key). `require_tls` is taken from
//! the section when set there, else `[tls] require`, else `false`.
//!
//! An empty string means "not set" (compose files write `VAR=`). A boolean
//! that is not a boolean stops startup: `ture` must never read as `false` and
//! quietly leave a listener in plaintext.

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

/// The node-wide `[tls]` defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeTls {
    /// `[tls] cert` / `FERROSA_TLS_CERT`.
    pub cert: Option<String>,
    /// `[tls] key` / `FERROSA_TLS_KEY`.
    pub key: Option<String>,
    /// `[tls] ca` / `FERROSA_TLS_CA` (internode peer verification).
    pub ca: Option<String>,
    /// `[tls] require` / `FERROSA_TLS_REQUIRE`; `None` when not set.
    pub require: Option<bool>,
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

fn optional_bool(
    file_config: &toml::Value,
    env: &str,
    section: &str,
    key: &str,
) -> Result<Option<bool>, String> {
    match non_empty(config_val_opt(env, file_config, section, key)) {
        None => Ok(None),
        Some(raw) => parse_bool_setting(&raw)
            .map(Some)
            .map_err(|e| format!("invalid [{section}] {key} / {env}: {e}")),
    }
}

/// Resolve the node-wide `[tls] cert/key/ca/require` (env `FERROSA_TLS_*`).
/// A non-boolean `require` is an error naming the key.
pub fn resolve_node_tls(file_config: &toml::Value) -> Result<NodeTls, String> {
    Ok(NodeTls {
        cert: non_empty(config_val_opt(
            "FERROSA_TLS_CERT",
            file_config,
            "tls",
            "cert",
        )),
        key: non_empty(config_val_opt("FERROSA_TLS_KEY", file_config, "tls", "key")),
        ca: non_empty(config_val_opt("FERROSA_TLS_CA", file_config, "tls", "ca")),
        require: optional_bool(file_config, "FERROSA_TLS_REQUIRE", "tls", "require")?,
    })
}

/// [`resolve_node_tls`], exiting the process with a named error on a malformed
/// value.
pub fn resolve_node_tls_or_exit(file_config: &toml::Value) -> NodeTls {
    resolve_node_tls(file_config).unwrap_or_else(|error| {
        eprintln!("FATAL: {error}");
        std::process::exit(1);
    })
}

/// Resolve `[section] tls_cert/tls_key/require_tls` with the `FERROSA_<PREFIX>_*`
/// env fallback, falling back to the node-wide `node` defaults. Returns an
/// error naming the key when `require_tls` is not a boolean.
pub fn resolve_listener_tls(
    file_config: &toml::Value,
    section: &str,
    env_prefix: &str,
    node: &NodeTls,
) -> Result<ListenerTlsConfig, String> {
    let cert_env = format!("FERROSA_{env_prefix}_TLS_CERT");
    let key_env = format!("FERROSA_{env_prefix}_TLS_KEY");
    let require_env = format!("FERROSA_{env_prefix}_REQUIRE_TLS");
    let require_tls = optional_bool(file_config, &require_env, section, "require_tls")?
        .or(node.require)
        .unwrap_or(false);
    let cert = non_empty(config_val_opt(&cert_env, file_config, section, "tls_cert"));
    let key = non_empty(config_val_opt(&key_env, file_config, section, "tls_key"));
    let (cert, key) = if cert.is_some() || key.is_some() {
        (cert, key)
    } else {
        (node.cert.clone(), node.key.clone())
    };
    Ok(ListenerTlsConfig {
        cert,
        key,
        require_tls,
    })
}

/// [`resolve_listener_tls`], exiting the process with a named error on a
/// malformed value (same fail-loud contract as a malformed bind address).
pub fn resolve_listener_tls_or_exit(
    file_config: &toml::Value,
    section: &str,
    env_prefix: &str,
    node: &NodeTls,
) -> ListenerTlsConfig {
    resolve_listener_tls(file_config, section, env_prefix, node).unwrap_or_else(|error| {
        eprintln!("FATAL: {error}");
        std::process::exit(1);
    })
}

/// Fill internode TLS settings that neither `[internode]` nor
/// `FERROSA_INTERNODE_*` set from the node-wide `[tls]` defaults.
///
/// Certificate and key are a pair (taken from `[tls]` only when internode set
/// neither). `require_tls` comes from `[tls] require` only when
/// `require_explicit` is false, i.e. neither `[internode] require_tls` nor
/// `FERROSA_INTERNODE_REQUIRE_TLS` was set.
pub fn apply_node_tls_to_internode(
    cfg: &mut ferrosa_net::config::NetConfig,
    node: &NodeTls,
    require_explicit: bool,
) {
    if cfg.tls_cert_path.is_none() && cfg.tls_key_path.is_none() {
        cfg.tls_cert_path = node.cert.clone();
        cfg.tls_key_path = node.key.clone();
    }
    if cfg.tls_ca_path.is_none() {
        cfg.tls_ca_path = node.ca.clone();
    }
    if !require_explicit {
        if let Some(require) = node.require {
            cfg.require_tls = require;
        }
    }
}

/// Whether `[internode] require_tls` or `FERROSA_INTERNODE_REQUIRE_TLS` is set
/// (so the node-wide `[tls] require` must not override it).
pub fn internode_require_is_explicit(file_config: &toml::Value) -> bool {
    non_empty(config_val_opt(
        "FERROSA_INTERNODE_REQUIRE_TLS",
        file_config,
        "internode",
        "require_tls",
    ))
    .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toml(s: &str) -> toml::Value {
        s.parse().unwrap()
    }

    fn none() -> NodeTls {
        NodeTls::default()
    }

    fn node_wide() -> NodeTls {
        NodeTls {
            cert: Some("/node.crt".into()),
            key: Some("/node.key".into()),
            ca: Some("/ca.crt".into()),
            require: Some(true),
        }
    }

    #[test]
    fn reads_the_three_keys_from_the_section() {
        let cfg = resolve_listener_tls(
            &toml("[postgres]\ntls_cert = \"/c.pem\"\ntls_key = \"/k.pem\"\nrequire_tls = true\n"),
            "postgres",
            "POSTGRES_TEST_UNSET",
            &none(),
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
        let cfg =
            resolve_listener_tls(&toml(""), "postgres", "POSTGRES_TEST_UNSET", &none()).unwrap();
        assert_eq!(cfg, ListenerTlsConfig::default());
    }

    #[test]
    fn a_non_boolean_require_tls_is_an_error_not_false() {
        let err = resolve_listener_tls(
            &toml("[web]\nrequire_tls = \"ture\"\n"),
            "web",
            "WEB_TEST_UNSET",
            &none(),
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
            &none(),
        )
        .unwrap();
        assert_eq!(cfg, ListenerTlsConfig::default());
    }

    #[test]
    fn node_tls_reads_the_tls_section() {
        let node = resolve_node_tls(&toml(
            "[tls]\ncert = \"/node.crt\"\nkey = \"/node.key\"\nca = \"/ca.crt\"\nrequire = true\n",
        ))
        .unwrap();
        assert_eq!(node, node_wide());
        assert_eq!(resolve_node_tls(&toml("")).unwrap().require, None);
    }

    #[test]
    fn a_non_boolean_node_require_is_an_error() {
        let err = resolve_node_tls(&toml("[tls]\nrequire = \"yes\"\n")).unwrap_err();
        assert!(err.contains("[tls] require"), "{err}");
    }

    /// One `[tls]` certificate covers a listener that sets nothing itself.
    #[test]
    fn a_listener_with_no_keys_inherits_the_node_certificate_and_requirement() {
        let cfg =
            resolve_listener_tls(&toml(""), "flight", "FLIGHT_TEST_UNSET", &node_wide()).unwrap();
        assert_eq!(
            cfg,
            ListenerTlsConfig {
                cert: Some("/node.crt".into()),
                key: Some("/node.key".into()),
                require_tls: true,
            }
        );
    }

    #[test]
    fn a_section_certificate_overrides_the_node_certificate_as_a_pair() {
        let cfg = resolve_listener_tls(
            &toml("[web]\ntls_cert = \"/web.crt\"\ntls_key = \"/web.key\"\n"),
            "web",
            "WEB_TEST_UNSET",
            &node_wide(),
        )
        .unwrap();
        assert_eq!(cfg.cert.as_deref(), Some("/web.crt"));
        assert_eq!(cfg.key.as_deref(), Some("/web.key"));
        assert!(cfg.require_tls, "require still comes from [tls]");

        // Half a section pair is NOT completed from [tls]: it stays half, so
        // the TLS builder reports it instead of pairing mismatched files.
        let half = resolve_listener_tls(
            &toml("[web]\ntls_cert = \"/web.crt\"\n"),
            "web",
            "WEB_TEST_UNSET",
            &node_wide(),
        )
        .unwrap();
        assert_eq!(half.cert.as_deref(), Some("/web.crt"));
        assert_eq!(half.key, None);
    }

    #[test]
    fn a_section_require_tls_overrides_the_node_requirement() {
        let cfg = resolve_listener_tls(
            &toml("[sparql]\nrequire_tls = false\n"),
            "sparql",
            "SPARQL_TEST_UNSET",
            &node_wide(),
        )
        .unwrap();
        assert!(!cfg.require_tls);
        assert_eq!(cfg.cert.as_deref(), Some("/node.crt"));
    }

    #[test]
    fn internode_inherits_the_node_tls_it_did_not_set() {
        let mut cfg = ferrosa_net::config::NetConfig::default();
        apply_node_tls_to_internode(&mut cfg, &node_wide(), false);
        assert_eq!(cfg.tls_cert_path.as_deref(), Some("/node.crt"));
        assert_eq!(cfg.tls_key_path.as_deref(), Some("/node.key"));
        assert_eq!(cfg.tls_ca_path.as_deref(), Some("/ca.crt"));
        assert!(cfg.require_tls);
    }

    #[test]
    fn internode_settings_win_over_the_node_tls() {
        let mut cfg = ferrosa_net::config::NetConfig {
            tls_cert_path: Some("/in.crt".into()),
            tls_key_path: Some("/in.key".into()),
            tls_ca_path: Some("/in-ca.crt".into()),
            require_tls: false,
            ..ferrosa_net::config::NetConfig::default()
        };
        apply_node_tls_to_internode(&mut cfg, &node_wide(), true);
        assert_eq!(cfg.tls_cert_path.as_deref(), Some("/in.crt"));
        assert_eq!(cfg.tls_key_path.as_deref(), Some("/in.key"));
        assert_eq!(cfg.tls_ca_path.as_deref(), Some("/in-ca.crt"));
        assert!(!cfg.require_tls, "an explicit internode require_tls wins");
    }

    #[test]
    fn internode_require_is_explicit_only_when_set() {
        assert!(!internode_require_is_explicit(&toml("")));
        assert!(internode_require_is_explicit(&toml(
            "[internode]\nrequire_tls = false\n"
        )));
    }
}
