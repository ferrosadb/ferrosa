//! Ferrosa binary — composes all crates into the running database.
//!
//! Startup sequence:
//! 1. Initialize tracing
//! 2. Load/generate host_id
//! 3. Create StorageEngine (with S3 if configured)
//! 4. Create Schema
//! 5. Create ModeController (standalone WritePath + ClusterState)
//! 6. Create PeerManager + RPC handlers + heartbeat loop
//! 7. Start internode RPC server (port 17000)
//! 8. Start CQL server (port 9042)
//! 9. Start web observability console (port 9090)
//! 10. Create GraphEngine + HTTP server (if enabled)
//! 11. Background: connect to seeds with exponential backoff
//! 12. Background: maintenance loop (flush, compaction, commit log GC)
//! 13. Wait for shutdown signal
//! 14. Graceful shutdown with timeout
//!
//! Correctness: commit-log recovery precedes system-table reconstruction, and
//! persisted roles are restored before missing seed credentials are created.
//! Last revised: 2026-09-16.
//! Last changed: made rotated role passwords authoritative across restart.
//!
//! Correctness: consensus supervision is installed after the controller exists
//! and before any OpenRaft task can start.
//! Last revised: 2026-08-27
//! Last changed: Kept the process alive but fail-closed after Raft failure.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc tuning: release dirty + muzzy pages back to the OS
/// immediately on free instead of caching them for ~10 s of reuse.
///
/// Default jemalloc trades RSS for throughput by holding freed
/// pages in per-arena dirty queues (decay_ms=10000). For ferrosa
/// nodes deployed inside a tight cgroup (e.g. the fmem cluster's
/// 2 GiB cap), a write-heavy hot path or a Merkle build over a
/// large replica can allocate-and-free GBs of partition data
/// within the decay window — the kernel sees the RSS still
/// growing toward the cap and OOM-kills before any reclamation
/// happens.
///
/// `dirty_decay_ms:0,muzzy_decay_ms:0` is the documented
/// memory-constrained tuning: every `free` immediately returns
/// the page to the OS. Throughput trade-off is small for our
/// allocation pattern (most freed pages are not reused
/// in-arena anyway — repair scans walk forward through partitions,
/// flushes hand SSTables off, etc).
///
/// The `unprefixed_malloc_on_supported_platforms` feature on
/// `tikv-jemallocator` makes jemalloc read this symbol at process
/// startup. Override per-deployment with `_RJEM_MALLOC_CONF`, the
/// prefix-qualified environment variable for this jemalloc build.
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:0,muzzy_decay_ms:0\0";

mod cql_broadcast;
mod listener_status;
mod listener_tls;
mod log_rotation;
mod maintenance;
mod repair_wiring;
mod runtime;
mod sentry_reporting;
mod supervisor;
mod web;

use std::path::Path;
use std::sync::Arc;

use uuid::Uuid;

/// Expand a leading `~` / `~/` in a path to `$HOME`. Config files commonly write
/// `data_dir = "~/.ferrosa/data"`; without this the engine creates a directory
/// literally named `~` in the process CWD. Leaves the value unchanged if it does
/// not start with `~` or if `$HOME` is unset (fail-soft for that edge).
fn expand_tilde(p: String) -> String {
    if let Ok(home) = std::env::var("HOME") {
        if p == "~" {
            return home;
        }
        if let Some(rest) = p.strip_prefix("~/") {
            return format!("{home}/{rest}");
        }
    }
    p
}

/// Read a config value: env var takes precedence, then config file, then default.
/// Resolve a configuration value.  Precedence is **TOML wins, env var is the
/// fallback, then the built-in default**: `[section] key` in the config file
/// overrides `env_key`, which overrides `default`.
///
/// (This is a deliberate inversion of the historical env-wins precedence:
/// operators asked for the config file to be authoritative so a committed
/// TOML cannot be silently overridden by a stray env var in the shell /
/// launchd / container environment. Every setting must be reachable from
/// either source.)
fn config_val(
    env_key: &str,
    config: &toml::Value,
    section: &str,
    key: &str,
    default: &str,
) -> String {
    config
        .get(section)
        .and_then(|s| s.get(key))
        .and_then(|v| v.as_str().map(String::from).or_else(|| Some(v.to_string())))
        .or_else(|| std::env::var(env_key).ok())
        .unwrap_or_else(|| default.to_string())
}

/// Like `config_val` but returns `None` when neither the config file nor the
/// env var set the key.  Needed to distinguish "operator did not say" from
/// "operator wrote `false`" for the deprecated `FERROSA_AUTH_DISABLED`
/// override path.  Same TOML-wins precedence as [`config_val`].
fn config_val_opt(env_key: &str, config: &toml::Value, section: &str, key: &str) -> Option<String> {
    config
        .get(section)
        .and_then(|s| s.get(key))
        .and_then(|v| v.as_str().map(String::from).or_else(|| Some(v.to_string())))
        .or_else(|| std::env::var(env_key).ok())
}

/// Resolve a positive-`usize` CQL server limit. `[cql] <key>` (TOML) overrides
/// `env_key`, which overrides `default` -- same TOML-wins precedence as
/// [`config_val`].
///
/// The resolved value must be a *positive* integer. Unlike the runtime worker
/// tunables (which fall back to a sane default on garbage), a bad value here is
/// a startup error: silently clamping a connection or in-flight limit is how a
/// node ends up shedding every request with no operator-visible reason.
///
/// An *empty* value is treated as unset and falls back to `default` -- clearing a
/// variable (`fly machine update --env KEY=`) is a request to unset it, not a
/// malformed one, and must not become a node that cannot start.
fn resolve_cql_positive_usize(
    env_key: &str,
    config: &toml::Value,
    key: &str,
    default: usize,
) -> Result<usize, String> {
    // `config_val` already applies the TOML-wins precedence and the default.
    let raw = config_val(env_key, config, "cql", key, &default.to_string());
    let raw = raw.trim();
    // An *empty* value means "unset", not "garbage". `fly machine update --env KEY=`
    // is the only way to clear a variable, so rejecting "" would make the knob
    // impossible to remove once set -- and would stop every node the moment a
    // sweep cleared its tunables. An unset limit is not an error; a malformed one is.
    if raw.is_empty() {
        return Ok(default);
    }
    let parsed = raw.parse::<usize>().map_err(|_| {
        format!("[cql] {key} (or ${env_key}) must be a positive integer, got {raw:?}")
    })?;
    if parsed == 0 {
        return Err(format!("[cql] {key} (or ${env_key}) must be positive, got 0"));
    }
    Ok(parsed)
}

/// Resolve the UDF sandbox config: `[udf] max_memory_bytes` (TOML) overrides
/// `FERROSA_UDF_MAX_MEMORY_BYTES`, which overrides the 16 MiB default.
///
/// Returns an error string for an unparsable or out-of-range value; the caller
/// exits loud rather than running with an unusable sandbox.
fn resolve_udf_sandbox_config(config: &toml::Value) -> Result<ferrosa_udf::SandboxConfig, String> {
    let raw = config_val(
        "FERROSA_UDF_MAX_MEMORY_BYTES",
        config,
        "udf",
        "max_memory_bytes",
        &ferrosa_udf::sandbox::DEFAULT_MAX_MEMORY_BYTES.to_string(),
    );
    let max_memory_bytes: usize = raw.trim().parse().map_err(|e| {
        format!("[udf] max_memory_bytes / FERROSA_UDF_MAX_MEMORY_BYTES = {raw:?} is not a byte count: {e}")
    })?;
    let sandbox = ferrosa_udf::SandboxConfig {
        max_memory_bytes,
        ..Default::default()
    };
    sandbox.validate().map_err(|e| e.to_string())?;
    Ok(sandbox)
}

/// Storage tunables that the `ferrosa-storage` crate reads from the
/// environment, each settable from the TOML file: `(env var, [section], key)`.
/// The TOML value is pinned into the env before `StorageEngineConfig::from_env`
/// runs, the same way `[storage].data_dir` and `[s3].local_path` are bridged.
const STORAGE_TUNABLE_BRIDGES: [(&str, &str, &str); 3] = [
    ("FERROSA_CACHE_MAX_BYTES", "storage", "cache_max_bytes"),
    (
        "FERROSA_CACHE_HOT_WINDOW_SECS",
        "storage",
        "cache_hot_window_secs",
    ),
    (
        "FERROSA_S3_REQUEST_TIMEOUT_SECS",
        "s3",
        "request_timeout_secs",
    ),
];

/// The `(env var, value)` pairs the TOML file sets for
/// [`STORAGE_TUNABLE_BRIDGES`]. Pure: reads no environment. A key that is
/// present but is not a whole non-negative number is an error, so a typo cannot
/// leave a tunable silently at its default.
fn storage_tunables_from_toml(config: &toml::Value) -> Result<Vec<(&'static str, String)>, String> {
    let mut out = Vec::new();
    for (env_key, section, key) in STORAGE_TUNABLE_BRIDGES {
        let Some(value) = config.get(section).and_then(|s| s.get(key)) else {
            continue;
        };
        let raw = value
            .as_str()
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| value.to_string());
        let parsed: u64 = raw.parse().map_err(|e| {
            format!("[{section}] {key} = {raw:?} is not a whole non-negative number: {e}")
        })?;
        out.push((env_key, parsed.to_string()));
    }
    Ok(out)
}

/// Pin the TOML-set storage tunables into the environment. TOML wins over a
/// value already in the env (the precedence [`config_val`] documents), and an
/// override of a differing env value is logged so it is never silent.
fn apply_storage_tunables(config: &toml::Value) -> Result<(), String> {
    for (env_key, value) in storage_tunables_from_toml(config)? {
        match std::env::var(env_key) {
            Ok(existing) if existing != value => tracing::warn!(
                env_key,
                env_value = existing,
                toml_value = value,
                "config file overrides the environment for this storage tunable"
            ),
            _ => {}
        }
        std::env::set_var(env_key, value);
    }
    Ok(())
}

/// Keys accepted under `[jsonb]`; anything else is refused so a typo cannot
/// silently leave a limit at its default.
const JSONB_KEYS: [&str; 8] = [
    "max_input_bytes",
    "max_encoded_bytes",
    "max_nesting_depth",
    "max_key_list_length",
    "max_index_terms_per_doc",
    "max_path_len",
    "path_step_budget",
    "duplicate_keys",
];

fn jsonb_toml_u64(table: &toml::value::Table, key: &str) -> Result<Option<u64>, String> {
    match table.get(key) {
        None => Ok(None),
        Some(toml::Value::Integer(n)) => u64::try_from(*n)
            .map(Some)
            .map_err(|_| format!("[jsonb] {key} = {n} must not be negative")),
        Some(other) => Err(format!("[jsonb] {key} = {other} must be an integer")),
    }
}

fn jsonb_limits_config(config: &toml::Value) -> Result<ferrosa_jsonb::LimitsConfig, String> {
    let Some(section) = config.get("jsonb") else {
        return Ok(ferrosa_jsonb::LimitsConfig::default());
    };
    let table = section
        .as_table()
        .ok_or_else(|| format!("[jsonb] must be a table, got {section}"))?;
    if let Some(bad) = table.keys().find(|k| !JSONB_KEYS.contains(&k.as_str())) {
        return Err(format!(
            "[jsonb] unknown key {bad:?}; valid keys: {JSONB_KEYS:?}"
        ));
    }
    let duplicate_keys = match table.get("duplicate_keys") {
        None => None,
        Some(toml::Value::String(s)) if s == "last_wins" => {
            Some(ferrosa_jsonb::DuplicateKeyPolicy::LastWins)
        }
        Some(toml::Value::String(s)) if s == "error" => {
            Some(ferrosa_jsonb::DuplicateKeyPolicy::Error)
        }
        Some(other) => {
            return Err(format!(
                "[jsonb] duplicate_keys = {other} must be \"last_wins\" or \"error\""
            ))
        }
    };
    Ok(ferrosa_jsonb::LimitsConfig {
        max_input_bytes: jsonb_toml_u64(table, "max_input_bytes")?,
        max_encoded_bytes: jsonb_toml_u64(table, "max_encoded_bytes")?,
        max_nesting_depth: jsonb_toml_u64(table, "max_nesting_depth")?,
        max_key_list_length: jsonb_toml_u64(table, "max_key_list_length")?,
        max_index_terms_per_doc: jsonb_toml_u64(table, "max_index_terms_per_doc")?,
        max_path_len: jsonb_toml_u64(table, "max_path_len")?,
        path_step_budget: jsonb_toml_u64(table, "path_step_budget")?,
        duplicate_keys,
    })
}

/// Resolve `[jsonb]` limits: TOML overrides `FERROSA_JSONB_*` env, which
/// overrides defaults. `write_path_max` is the effective commit-log segment
/// size. The error names the key, the value and the bound it broke; the caller
/// exits loud.
fn resolve_jsonb_limits_with_env(
    config: &toml::Value,
    env: &dyn Fn(&str) -> Option<String>,
    write_path_max: u64,
) -> Result<ferrosa_jsonb::Limits, String> {
    let cfg = jsonb_limits_config(config)?;
    ferrosa_jsonb::Limits::from_config_with_env(&cfg, env, write_path_max)
        .map_err(|e| format!("[jsonb] {e}"))
}

fn resolve_jsonb_limits(
    config: &toml::Value,
    write_path_max: u64,
) -> Result<ferrosa_jsonb::Limits, String> {
    let env = |k: &str| std::env::var_os(k).map(|v| v.to_string_lossy().into_owned());
    resolve_jsonb_limits_with_env(config, &env, write_path_max)
}

const DEFAULT_WEB_BIND: &str = "127.0.0.1:9090";
const DEFAULT_CQL_BIND: &str = "127.0.0.1:9042";
const DEFAULT_GRAPH_HTTP_BIND: &str = "127.0.0.1:7474";
const DEFAULT_GRAPH_BOLT_PORT: &str = "7687";
const DEFAULT_SPARQL_BIND: &str = "127.0.0.1:8080";
#[cfg(any(feature = "flight", test))]
const DEFAULT_FLIGHT_BIND: &str = "127.0.0.1:8815";
const DEFAULT_POSTGRES_BIND: &str = "127.0.0.1:5432";

/// Parse a `SocketAddr` from a resolved config string, failing **loud and
/// clean** on a malformed value.
///
/// A bad bind address is operator error, not a bug — but it must not surface
/// as a `.expect()` panic. A panic unwinds the `#[tokio::main]` async stack,
/// which drops the subsystem tokio runtimes in an async context and fires the
/// "Cannot drop a runtime …" panic *on top of* the real error, masking it
/// (issue #172). `process::exit` runs no destructors, so the diagnostic below
/// is the last thing the operator sees. `label`/`env_key` name the setting so
/// the message is actionable.
fn parse_bind_addr(label: &str, env_key: &str, value: &str) -> std::net::SocketAddr {
    match value.parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!(
                "FATAL: invalid {label} bind address {value:?}: {e}\n\
                 Fix the config-file value or the {env_key} environment variable \
                 (expected host:port, e.g. 127.0.0.1:9090)."
            );
            std::process::exit(1);
        }
    }
}

/// Parse a listener port from a resolved config value, failing loudly rather
/// than silently replacing an operator-provided value with the default.
fn parse_listener_port(label: &str, env_key: &str, value: &str) -> u16 {
    match value.parse() {
        Ok(port) => port,
        Err(e) => {
            eprintln!(
                "FATAL: invalid {label} port {value:?}: {e}\n\
                 Fix the config-file value or the {env_key} environment variable \
                 (expected an integer from 0 through 65535)."
            );
            std::process::exit(1);
        }
    }
}

fn resolve_web_config(
    file_config: &toml::Value,
    tls: &listener_tls::ListenerTlsConfig,
) -> web::WebConfig {
    let web_bind = config_val(
        "FERROSA_WEB_BIND",
        file_config,
        "web",
        "bind",
        DEFAULT_WEB_BIND,
    );
    web::WebConfig {
        bind_addr: parse_bind_addr("web console", "FERROSA_WEB_BIND", &web_bind),
        tls_cert_path: tls.cert.clone(),
        tls_key_path: tls.key.clone(),
        require_tls: tls.require_tls,
    }
}

fn resolve_graph_http_config(
    file_config: &toml::Value,
    tls: &listener_tls::ListenerTlsConfig,
) -> ferrosa_graph::http::GraphHttpConfig {
    let graph_bind = config_val(
        "FERROSA_GRAPH_BIND",
        file_config,
        "graph",
        "bind",
        DEFAULT_GRAPH_HTTP_BIND,
    );
    ferrosa_graph::http::GraphHttpConfig {
        tls_cert_path: tls.cert.clone(),
        tls_key_path: tls.key.clone(),
        require_tls: tls.require_tls,
        bind_addr: parse_bind_addr("graph HTTP", "FERROSA_GRAPH_BIND", &graph_bind),
        ..ferrosa_graph::http::GraphHttpConfig::default()
    }
}

/// Bolt shares `[graph] tls_cert/tls_key/require_tls` with graph HTTP (one
/// graph host, one certificate). A bad certificate stops startup with the
/// listener named rather than leaving Bolt in plaintext.
fn resolve_graph_bolt_config(
    file_config: &toml::Value,
    graph_http_bind: std::net::SocketAddr,
    auth_disabled: bool,
    tls: &listener_tls::ListenerTlsConfig,
) -> Result<ferrosa_graph::bolt::server::BoltConfig, String> {
    let bolt_port = parse_listener_port(
        "Bolt",
        "FERROSA_BOLT_PORT",
        &config_val(
            "FERROSA_BOLT_PORT",
            file_config,
            "graph",
            "bolt_port",
            DEFAULT_GRAPH_BOLT_PORT,
        ),
    );
    let bind_addr = std::net::SocketAddr::new(graph_http_bind.ip(), bolt_port);
    let tls_config = ferrosa_net::tls::optional_server_config(
        "Bolt",
        tls.cert.as_deref(),
        tls.key.as_deref(),
        tls.require_tls,
        &[],
    )
    .map_err(|e| e.to_string())?;

    Ok(ferrosa_graph::bolt::server::BoltConfig {
        bind_addr,
        auth_disabled,
        tls: tls_config,
        require_tls: tls.require_tls,
        ..ferrosa_graph::bolt::server::BoltConfig::default()
    })
}

/// Resolve the SPARQL listener with the same explicit opt-in remote exposure
/// contract as the other HTTP front-ends.
fn resolve_sparql_bind(file_config: &toml::Value) -> std::net::SocketAddr {
    let sparql_bind = config_val(
        "FERROSA_SPARQL_BIND",
        file_config,
        "sparql",
        "bind",
        DEFAULT_SPARQL_BIND,
    );
    parse_bind_addr("SPARQL", "FERROSA_SPARQL_BIND", &sparql_bind)
}

fn resolve_cql_bind(file_config: &toml::Value) -> std::net::SocketAddr {
    let cql_bind = config_val(
        "FERROSA_CQL_BIND",
        file_config,
        "cql",
        "bind",
        DEFAULT_CQL_BIND,
    );
    parse_bind_addr("CQL", "FERROSA_CQL_BIND", &cql_bind)
}

#[cfg(any(feature = "flight", test))]
fn resolve_flight_bind(file_config: &toml::Value) -> std::net::SocketAddr {
    let flight_bind = config_val(
        "FERROSA_FLIGHT_BIND",
        file_config,
        "flight",
        "bind",
        DEFAULT_FLIGHT_BIND,
    );
    parse_bind_addr("Arrow Flight", "FERROSA_FLIGHT_BIND", &flight_bind)
}

/// The PostgreSQL suspended-portal limits: `[postgres]
/// max_suspended_portals_per_connection` / `max_suspended_portals` /
/// `suspended_portal_idle_timeout_ms`, each overriding its
/// `FERROSA_POSTGRES_*` variable (TOML wins). A malformed value is logged at
/// ERROR and the defaults apply (`PortalLimits::resolve_or_default`).
fn resolve_postgres_portal_limits(file_config: &toml::Value) -> ferrosa_postgres::PortalLimits {
    use ferrosa_postgres::portal_limits::{
        IDLE_TIMEOUT_MS_ENV, MAX_PER_CONNECTION_ENV, MAX_PER_NODE_ENV,
    };
    let per_connection = config_val_opt(
        MAX_PER_CONNECTION_ENV,
        file_config,
        "postgres",
        "max_suspended_portals_per_connection",
    );
    let per_node = config_val_opt(
        MAX_PER_NODE_ENV,
        file_config,
        "postgres",
        "max_suspended_portals",
    );
    let idle_ms = config_val_opt(
        IDLE_TIMEOUT_MS_ENV,
        file_config,
        "postgres",
        "suspended_portal_idle_timeout_ms",
    );
    ferrosa_postgres::PortalLimits::resolve_or_default(
        per_connection.as_deref(),
        per_node.as_deref(),
        idle_ms.as_deref(),
    )
}

fn resolve_postgres_bind(file_config: &toml::Value) -> std::net::SocketAddr {
    let postgres_bind = config_val(
        "FERROSA_POSTGRES_BIND",
        file_config,
        "postgres",
        "bind",
        DEFAULT_POSTGRES_BIND,
    );
    parse_bind_addr("Postgres", "FERROSA_POSTGRES_BIND", &postgres_bind)
}

/// Apply TOML `[internode]` overrides to a `NetConfig`.
///
/// Precedence is **TOML wins** over env (see [`config_val`]): the base
/// `NetConfig::from_env()` seeds each field from `FERROSA_INTERNODE_*`, then
/// this helper overwrites any field the config file sets, so a committed TOML
/// is authoritative. A malformed `[internode].bind`/`broadcast` is logged at
/// WARN and leaves the env/default value in place. A malformed
/// `[internode].require_tls` is an `Err`: a typo must never quietly turn the
/// TLS requirement off (t_d5d122ba).
///
/// TLS keys: `tls_cert`, `tls_key`, `tls_ca`, `require_tls` (env fallback
/// `FERROSA_INTERNODE_TLS_CERT` / `_TLS_KEY` / `_TLS_CA` / `_REQUIRE_TLS`).
fn apply_internode_toml_overrides(
    cfg: &mut ferrosa_net::config::NetConfig,
    file_config: &toml::Value,
) -> Result<(), String> {
    let internode = match file_config.get("internode") {
        Some(t) => t,
        None => return Ok(()),
    };

    if let Some(v) = internode.get("bind").and_then(|v| v.as_str()) {
        match v.parse() {
            Ok(addr) => cfg.bind_addr = addr,
            Err(e) => tracing::warn!(value = %v, %e, "ignoring invalid [internode].bind"),
        }
    }
    if let Some(v) = internode.get("broadcast").and_then(|v| v.as_str()) {
        match v.parse() {
            Ok(addr) => {
                cfg.broadcast_addr = addr;
                cfg.internode_broadcast = Some(v.to_string());
            }
            Err(e) => tracing::warn!(value = %v, %e, "ignoring invalid [internode].broadcast"),
        }
    }
    if let Some(v) = internode.get("cluster_name").and_then(|v| v.as_str()) {
        cfg.cluster_name = v.to_string();
    }
    if let Some(v) = internode.get("psk").and_then(|v| v.as_str()) {
        cfg.psk = Some(v.to_string());
    }
    let path = |key: &str| {
        internode
            .get(key)
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .map(String::from)
    };
    if let Some(v) = path("tls_cert") {
        cfg.tls_cert_path = Some(v);
    }
    if let Some(v) = path("tls_key") {
        cfg.tls_key_path = Some(v);
    }
    if let Some(v) = path("tls_ca") {
        cfg.tls_ca_path = Some(v);
    }
    if let Some(v) = internode.get("require_tls") {
        let raw = v
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| v.to_string());
        cfg.require_tls = listener_tls::parse_bool_setting(&raw)
            .map_err(|e| format!("invalid [internode] require_tls: {e}"))?;
    }
    Ok(())
}

/// Resolve the graph-enabled flag honouring TOML when the env var is unset.
///
/// Defaults to ON (t_acc3c7fd): a fresh install should expose the full
/// user-facing feature set, so the graph HTTP (7474) + Bolt (7687) endpoints
/// come up by default. Opt out explicitly with `FERROSA_GRAPH_ENABLED=false`
/// or `[graph] enabled = false`. TOML wins over the env var (config file is
/// authoritative); the env var is the fallback when the TOML key is absent.
fn resolve_graph_enabled<F>(file_config: &toml::Value, env: F) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(b) = file_config
        .get("graph")
        .and_then(|s| s.get("enabled"))
        .and_then(|v| v.as_bool())
    {
        return b;
    }
    if let Some(v) = env("FERROSA_GRAPH_ENABLED") {
        return v == "true" || v == "1";
    }
    true
}

/// Resolve the SPARQL-enabled flag honouring TOML when the env var is unset.
///
/// Defaults to ON (t_acc3c7fd) so a fresh install exposes the SPARQL 1.1
/// endpoint (8080). Opt out with `FERROSA_SPARQL_ENABLED=false` or
/// `[sparql] enabled = false`. TOML wins over the env var; the env var is the
/// fallback when the TOML key is absent.
fn resolve_sparql_enabled<F>(file_config: &toml::Value, env: F) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(b) = file_config
        .get("sparql")
        .and_then(|s| s.get("enabled"))
        .and_then(|v| v.as_bool())
    {
        return b;
    }
    if let Some(v) = env("FERROSA_SPARQL_ENABLED") {
        return v == "true" || v == "1";
    }
    true
}

/// Every client listener's TLS settings plus whether each optional listener is
/// enabled, resolved before anything binds so the production gate sees the
/// whole node (t_d5d122ba).
struct ListenerTlsInputs {
    cql: listener_tls::ListenerTlsConfig,
    postgres: listener_tls::ListenerTlsConfig,
    graph: listener_tls::ListenerTlsConfig,
    sparql: listener_tls::ListenerTlsConfig,
    web: listener_tls::ListenerTlsConfig,
    /// `[flight] tls_cert/tls_key/require_tls` (env `FERROSA_FLIGHT_*`).
    flight: listener_tls::ListenerTlsConfig,
    graph_enabled: bool,
    sparql_enabled: bool,
    /// `None` when the binary was built without the `flight` feature.
    flight_enabled: Option<bool>,
}

impl ListenerTlsInputs {
    /// Resolve every listener's TLS from its own section, falling back to the
    /// node-wide `[tls]` certificate and requirement (`node`).
    fn resolve(file_config: &toml::Value, node: &listener_tls::NodeTls) -> Self {
        let tls = |section, prefix| {
            listener_tls::resolve_listener_tls_or_exit(file_config, section, prefix, node)
        };
        #[cfg(feature = "flight")]
        let flight_enabled = Some(resolve_flight_enabled_or_exit(file_config));
        #[cfg(not(feature = "flight"))]
        let flight_enabled = None;
        Self {
            cql: tls("cql", "CQL"),
            postgres: tls("postgres", "POSTGRES"),
            graph: tls("graph", "GRAPH"),
            sparql: tls("sparql", "SPARQL"),
            web: tls("web", "WEB"),
            flight: tls("flight", "FLIGHT"),
            graph_enabled: resolve_graph_enabled(file_config, |k| std::env::var(k).ok()),
            sparql_enabled: resolve_sparql_enabled(file_config, |k| std::env::var(k).ok()),
            flight_enabled,
        }
    }

    /// Load every configured certificate/key once, before anything binds, so a
    /// bad path or a half-configured listener stops startup naming the listener
    /// instead of surfacing later as a background listener failure.
    fn validate(&self) -> Result<(), String> {
        let checks: [(&str, &listener_tls::ListenerTlsConfig, bool); 6] = [
            ("CQL", &self.cql, true),
            ("PostgreSQL", &self.postgres, true),
            ("graph HTTP / Bolt", &self.graph, self.graph_enabled),
            ("SPARQL", &self.sparql, self.sparql_enabled),
            ("web console", &self.web, true),
            (
                "Arrow Flight",
                &self.flight,
                self.flight_enabled == Some(true),
            ),
        ];
        for (listener, cfg, enabled) in checks {
            // A disabled listener's require_tls is not enforced, but a
            // certificate it names must still load: the graph stub serves it.
            let require = enabled && cfg.require_tls;
            ferrosa_net::tls::optional_server_config(
                listener,
                cfg.cert.as_deref(),
                cfg.key.as_deref(),
                require,
                &[],
            )
            .map_err(|e| format!("{listener} listener TLS: {e}"))?;
        }
        Ok(())
    }

    /// The production-gate view of every listener this node may bind.
    ///
    /// Per listener: CQL, PostgreSQL, graph HTTP, Bolt, SPARQL and the web
    /// console all support TLS and must require it when enabled, and so does
    /// Arrow Flight (t_58db6320) when the binary has the `flight` feature and
    /// `[flight] enabled` is on. CQL, PostgreSQL and the web console are
    /// always enabled. With the graph engine disabled the graph HTTP port
    /// serves only a fixed 503 remediation stub (no data, no auth) and Bolt
    /// does not bind, so neither is gated then; the stub still uses TLS when a
    /// `[graph]` certificate is configured.
    fn postures(&self) -> Vec<ferrosa_schema::startup::ListenerTls> {
        use ferrosa_schema::startup::ListenerTls;
        let tls = |listener, enabled, cfg: &listener_tls::ListenerTlsConfig, key| ListenerTls {
            listener,
            enabled,
            tls_supported: true,
            require_tls: cfg.require_tls,
            config_key: key,
        };
        let mut listeners = vec![
            tls("CQL", true, &self.cql, "[cql] require_tls"),
            tls("PostgreSQL", true, &self.postgres, "[postgres] require_tls"),
            tls(
                "graph HTTP",
                self.graph_enabled,
                &self.graph,
                "[graph] require_tls",
            ),
            tls(
                "Bolt",
                self.graph_enabled,
                &self.graph,
                "[graph] require_tls",
            ),
            tls(
                "SPARQL",
                self.sparql_enabled,
                &self.sparql,
                "[sparql] require_tls",
            ),
            tls("web console", true, &self.web, "[web] require_tls"),
        ];
        if let Some(enabled) = self.flight_enabled {
            listeners.push(tls(
                "Arrow Flight",
                enabled,
                &self.flight,
                "[flight] require_tls",
            ));
        }
        listeners
    }
}

/// `[flight] enabled` (env `FERROSA_FLIGHT_ENABLED`), default on. Production
/// mode requires an enabled Flight listener to require TLS
/// (`[flight] tls_cert/tls_key/require_tls`), like every other listener.
#[cfg(any(feature = "flight", test))]
fn resolve_flight_enabled(file_config: &toml::Value) -> Result<bool, String> {
    match config_val_opt("FERROSA_FLIGHT_ENABLED", file_config, "flight", "enabled")
        .filter(|v| !v.trim().is_empty())
    {
        None => Ok(true),
        Some(raw) => listener_tls::parse_bool_setting(&raw)
            .map_err(|e| format!("invalid [flight] enabled / FERROSA_FLIGHT_ENABLED: {e}")),
    }
}

/// Build the Flight service from `FERROSA_FLIGHT_SIGNING_KEY` (ephemeral key
/// with a WARN when unset), `FERROSA_FLIGHT_SIGNING_KEY_PREVIOUS` (comma list)
/// and `FERROSA_FLIGHT_TOKEN_TTL_SECS` (default 3600; a non-integer value is
/// fatal rather than silently replaced by the default).
#[cfg(feature = "flight")]
fn build_flight_service(
    state: std::sync::Arc<ferrosa_cql::router::SharedState>,
) -> ferrosa_flight::service::FerrosaFlight {
    let signing_key = match std::env::var("FERROSA_FLIGHT_SIGNING_KEY") {
        Ok(k) if !k.is_empty() => k.into_bytes(),
        _ => {
            tracing::warn!(
                "FERROSA_FLIGHT_SIGNING_KEY unset — using an ephemeral Flight token \
                 key; bearer tokens will not survive a restart or work across nodes. \
                 Set FERROSA_FLIGHT_SIGNING_KEY for stable auth."
            );
            uuid::Uuid::new_v4().into_bytes().to_vec()
        }
    };
    let previous_keys: Vec<Vec<u8>> = std::env::var("FERROSA_FLIGHT_SIGNING_KEY_PREVIOUS")
        .ok()
        .into_iter()
        .flat_map(|v| {
            v.split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>()
        })
        .collect();
    let token_ttl_secs = parse_flight_token_ttl(
        std::env::var("FERROSA_FLIGHT_TOKEN_TTL_SECS").ok(),
    )
    .unwrap_or_else(|error| {
        eprintln!("FATAL: {error}");
        std::process::exit(1);
    });
    let mut service = ferrosa_flight::service::FerrosaFlight::new(state, signing_key)
        .with_token_ttl(token_ttl_secs);
    if !previous_keys.is_empty() {
        service = service.with_previous_keys(previous_keys);
    }
    service
}

/// The Flight port advertised for remote replicas: `FERROSA_FLIGHT_PORT` when
/// set (a typo is an error, not a fallback), otherwise the port this node binds
/// (`[flight] bind`), assuming every node binds the same Flight port.
#[cfg(feature = "flight")]
fn resolve_flight_advertised_port(
    env: Option<String>,
    bind: std::net::SocketAddr,
) -> Result<u16, String> {
    match env.as_deref().filter(|v| !v.trim().is_empty()) {
        Some(raw) => ferrosa_flight::service::parse_flight_port(raw),
        None => Ok(bind.port()),
    }
}

/// `FERROSA_FLIGHT_TOKEN_TTL_SECS`: unset/empty → 3600; otherwise a positive
/// integer. A typo used to fall back to 3600 silently.
#[cfg(any(feature = "flight", test))]
fn parse_flight_token_ttl(raw: Option<String>) -> Result<u64, String> {
    match raw.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        None => Ok(3600),
        Some(v) => match v.parse::<u64>() {
            Ok(secs) if secs > 0 => Ok(secs),
            _ => Err(format!(
                "invalid FERROSA_FLIGHT_TOKEN_TTL_SECS {v:?}: expected a positive integer (seconds)"
            )),
        },
    }
}

#[cfg(feature = "flight")]
fn resolve_flight_enabled_or_exit(file_config: &toml::Value) -> bool {
    resolve_flight_enabled(file_config).unwrap_or_else(|error| {
        eprintln!("FATAL: {error}");
        std::process::exit(1);
    })
}

/// Production deployment gate (FMEA epic). In production mode (`FERROSA_MODE=
/// production`), refuses startup when an operator-fixable security requirement
/// is unmet — authentication disabled (`t_87a50318`), an enabled client
/// listener that does not require TLS or has no TLS at all (`t_27bf4674`,
/// `t_d5d122ba`), internode traffic not required to be TLS, or the default
/// superuser password. Each refusal names the listener and the config key.
/// Weaknesses whose config is not operator-configurable yet (the hardcoded
/// permissive password policy and env secrets provider) only WARN — see
/// `ProductionViolation::blocks_startup`. No-op in development mode. Runs
/// before any listener (internode included) binds. Calls `exit(1)` rather than
/// returning so the gate cannot be accidentally bypassed by a `?` further up.
fn enforce_production_requirements(
    auth_enabled: bool,
    listeners: &ListenerTlsInputs,
    internode_require_tls: bool,
    has_superuser_password: bool,
) {
    use ferrosa_schema::startup::{
        validate_production_requirements, DeploymentMode, ProductionCheckConfig,
    };
    let violations = validate_production_requirements(&ProductionCheckConfig {
        mode: DeploymentMode::from_env(),
        auth_enabled,
        listeners: listeners.postures(),
        internode_require_tls,
        has_superuser_password,
        // The schema is hardcoded to a permissive policy + env secrets (see the
        // SchemaConfig in main); report them truthfully so the WARN fires, but
        // they don't block startup until that config is operator-configurable.
        password_policy: ferrosa_schema::PasswordPolicy::permissive(),
        secrets_provider_type: "env".to_string(),
        s3_allow_http: false, // S3 endpoint scheme isn't surfaced here yet.
    });
    let (blocking, warnings): (Vec<_>, Vec<_>) =
        violations.iter().partition(|v| v.blocks_startup());
    for w in &warnings {
        tracing::warn!("production config weakness (not yet enforceable): {w}");
    }
    if !blocking.is_empty() {
        eprintln!("FATAL: refusing to start — production deployment requirements not met:");
        for b in &blocking {
            eprintln!("  - {b}");
        }
        eprintln!(
            "Remediate the above (e.g. [cql] auth_enabled = true; one certificate for every \
             listener and internode with [tls] cert, key, ca and require = true, or per \
             listener <section> require_tls = true with tls_cert/tls_key; change the default \
             superuser password), or run a development node (unset FERROSA_MODE=production)."
        );
        std::process::exit(1);
    }
}

/// BUG-006: `[cql] auth_enabled = true` in TOML was silently ignored;
/// only `FERROSA_AUTH_ENABLED=true` activated the authenticator. Returns
/// `Some(true|false)` when the operator made an explicit choice in
/// either place, `None` when they did not (so downstream resolution can
/// fall back to the storage default).
fn resolve_auth_enabled_toml<F>(file_config: &toml::Value, env: F) -> Option<bool>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(b) = file_config
        .get("cql")
        .and_then(|s| s.get("auth_enabled"))
        .and_then(|v| v.as_bool())
    {
        return Some(b);
    }
    if let Some(v) = env("FERROSA_AUTH_ENABLED") {
        return Some(v == "true" || v == "1");
    }
    None
}

/// Human label for where the effective `auth_enabled` value came from, for the
/// startup log. `[cql].auth_enabled` in the config file wins, then the
/// `FERROSA_AUTH_ENABLED` env var, then the built-in default. (Issue #172: the
/// log used to always say `"default"` even when the config file set it.)
fn auth_source_label(env_set: bool, toml_has_auth_key: bool) -> &'static str {
    if toml_has_auth_key {
        "config file ([cql].auth_enabled)"
    } else if env_set {
        "FERROSA_AUTH_ENABLED env"
    } else {
        "default"
    }
}

/// Whether log lines carry ANSI colour escapes.
///
/// Colour only when a person is watching. `tracing-subscriber` colours by default
/// regardless of where the output goes, so a container's stdout (a pipe read by a
/// log collector) filled with escape codes that break anchored patterns in the log
/// store. `FERROSA_LOG_ANSI` (`true`/`false`) overrides everything; otherwise
/// `NO_COLOR` (any non-empty value) turns it off, and the default is "is stdout a
/// terminal".
fn log_ansi_enabled(
    stdout_is_terminal: bool,
    no_color: Option<&str>,
    override_value: Option<&str>,
) -> bool {
    match override_value
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("1" | "true" | "on" | "yes") => return true,
        Some("0" | "false" | "off" | "no") => return false,
        // Unset or unrecognised: fall through to the default rules.
        _ => {}
    }
    if no_color.is_some_and(|v| !v.is_empty()) {
        return false;
    }
    stdout_is_terminal
}

/// [`log_ansi_enabled`] read from this process's stdout and environment.
fn log_ansi_from_env() -> bool {
    use std::io::IsTerminal;
    log_ansi_enabled(
        std::io::stdout().is_terminal(),
        std::env::var("NO_COLOR").ok().as_deref(),
        std::env::var("FERROSA_LOG_ANSI").ok().as_deref(),
    )
}

/// Load TOML configuration from disk. Returns an empty table if the file does not exist.
fn load_config(path: &str) -> Result<toml::Value, Box<dyn std::error::Error>> {
    if std::path::Path::new(path).exists() {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    } else {
        Ok(toml::Value::Table(toml::map::Map::new()))
    }
}

/// Resolve hinted-handoff storage under the configured data directory unless
/// the operator explicitly overrides it.
fn resolve_hinted_handoff_dir(
    config: &toml::Value,
    data_dir: &Path,
    env_override: Option<&str>,
) -> std::path::PathBuf {
    // TOML wins over env (config file authoritative), then env, then the
    // per-node default under the data dir.
    config
        .get("cluster")
        .and_then(|s| s.get("hinted_handoff_dir"))
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .or_else(|| env_override.map(std::path::PathBuf::from))
        .unwrap_or_else(|| data_dir.join("hints"))
}

/// Outcome of classifying the on-disk host_id state.
///
/// Extracted from [`load_or_generate_host_id_with`] so the decision
/// logic is unit-testable without touching disk or env. Each variant
/// carries enough context for the call site to emit a precise, actionable
/// diagnostic (BUG-008: previously a stale/corrupt host_id was silently
/// regenerated, leaving the operator with no breadcrumb).
#[derive(Debug, Clone, PartialEq)]
enum HostIdResolution {
    /// Disk had a parseable UUID — use as-is.
    LoadedFromDisk(Uuid),
    /// Operator-supplied override (env var or test); regardless of disk state.
    UsingOverride(Uuid),
    /// File exists but is unparseable. Regenerate, warn, name the path.
    InvalidFileRegenerated {
        /// Path of the bad file (already on disk at this location).
        path: std::path::PathBuf,
        /// Trimmed file contents that failed to parse — included for
        /// diagnostics. May be empty.
        bad_content: String,
        /// Newly generated UUID we will persist.
        new_id: Uuid,
    },
    /// Disk file is empty (zero-byte) — most often a crash mid-write.
    EmptyFileRegenerated {
        path: std::path::PathBuf,
        new_id: Uuid,
    },
    /// No file exists — fresh node. Generate.
    GeneratedNew(Uuid),
}

/// Pure classification: read the file state and decide what to do.
///
/// `override_` is the FERROSA_HOST_ID env var (or test param). When set,
/// it always wins — matches the documented behavior of FERROSA_HOST_ID
/// as an explicit operator override.
fn classify_host_id_state(path: &std::path::Path, override_: Option<&str>) -> HostIdResolution {
    // Env override is authoritative when set + valid.
    if let Some(s) = override_ {
        if let Ok(id) = Uuid::parse_str(s.trim()) {
            return HostIdResolution::UsingOverride(id);
        }
    }

    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let trimmed = contents.trim();
            if trimmed.is_empty() {
                HostIdResolution::EmptyFileRegenerated {
                    path: path.to_path_buf(),
                    new_id: Uuid::new_v4(),
                }
            } else if let Ok(id) = Uuid::parse_str(trimmed) {
                HostIdResolution::LoadedFromDisk(id)
            } else {
                HostIdResolution::InvalidFileRegenerated {
                    path: path.to_path_buf(),
                    bad_content: trimmed.to_string(),
                    new_id: Uuid::new_v4(),
                }
            }
        }
        Err(_) => HostIdResolution::GeneratedNew(Uuid::new_v4()),
    }
}

/// Load host_id from disk, env var, or generate a new one.
fn load_or_generate_host_id(data_dir: &Path) -> Uuid {
    load_or_generate_host_id_with(data_dir, std::env::var("FERROSA_HOST_ID").ok())
}

/// Core implementation that accepts an explicit host_id override.
/// Avoids process-global env var mutation in tests.
fn load_or_generate_host_id_with(data_dir: &Path, env_override: Option<String>) -> Uuid {
    let path = data_dir.join("host_id");

    let resolution = classify_host_id_state(&path, env_override.as_deref());

    match resolution {
        HostIdResolution::LoadedFromDisk(id) => {
            tracing::info!(%id, "loaded host_id from disk");
            id
        }
        HostIdResolution::UsingOverride(id) => {
            if let Err(e) = std::fs::write(&path, id.to_string()) {
                // BUG-008: persistence-failure diagnostic now names the path
                // and the recovery action explicitly.
                tracing::error!(
                    %e,
                    path = %path.display(),
                    "startup: failed to persist host_id override — \
                     re-run after fixing dir permissions or `rm {} && restart`",
                    path.display(),
                );
            }
            tracing::info!(%id, "using host_id from override");
            id
        }
        HostIdResolution::InvalidFileRegenerated {
            path: bad_path,
            bad_content,
            new_id,
        } => {
            // BUG-008: previously this was a silent regen. Now we name the
            // file, show what was in it, and the new id — so an operator
            // tracing a "why did the node change identity?" can see the
            // breadcrumb in the journal.
            tracing::error!(
                path = %bad_path.display(),
                bad_content = %bad_content,
                %new_id,
                "startup: host_id file at {} contained an unparseable value — \
                 regenerated. If this node was part of a cluster, the old \
                 identity is lost; investigate before bootstrapping.",
                bad_path.display(),
            );
            if let Err(e) = std::fs::write(&bad_path, new_id.to_string()) {
                tracing::error!(
                    %e,
                    path = %bad_path.display(),
                    "startup: failed to persist regenerated host_id"
                );
            }
            new_id
        }
        HostIdResolution::EmptyFileRegenerated {
            path: empty_path,
            new_id,
        } => {
            tracing::warn!(
                path = %empty_path.display(),
                %new_id,
                "startup: host_id file at {} was empty (likely crash mid-write) — \
                 regenerated",
                empty_path.display(),
            );
            if let Err(e) = std::fs::write(&empty_path, new_id.to_string()) {
                tracing::error!(
                    %e,
                    path = %empty_path.display(),
                    "startup: failed to persist regenerated host_id"
                );
            }
            new_id
        }
        HostIdResolution::GeneratedNew(new_id) => {
            if let Err(e) = std::fs::write(&path, new_id.to_string()) {
                tracing::error!(
                    %e,
                    path = %path.display(),
                    "startup: failed to persist host_id"
                );
            }
            tracing::info!(%new_id, "generated new host_id");
            new_id
        }
    }
}

/// Bootstrap schema and table registrations from S3.
///
/// On a cold restart (local data wiped, S3 has data), this function:
/// 1. Loads the schema snapshot from S3 (`schema.json`)
/// 2. Applies it to the in-memory schema (creates keyspaces + tables)
/// 3. Registers each table with the StorageEngine so reads can proceed
async fn bootstrap_from_s3(
    storage: &ferrosa_storage::StorageEngine,
    schema: &ferrosa_schema::Schema,
) -> Result<(), Box<dyn std::error::Error>> {
    let (os_config, store) = storage.object_store_and_config()?;
    let prefix = &os_config.prefix;

    // Load schema snapshot (retry up to 5 times — the S3-compatible store may
    // not be ready immediately after a container restart).
    let snapshot_data = {
        let mut loaded = None;
        for attempt in 1..=5u32 {
            match ferrosa_storage::load_schema_snapshot(store.as_ref(), prefix).await {
                Ok(Some(data)) => {
                    loaded = Some(data);
                    break;
                }
                Ok(None) if attempt < 5 => {
                    tracing::info!(attempt, "no schema snapshot in S3 yet, retrying in 2s…");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Ok(None) => {}
                Err(e) if attempt < 5 => {
                    tracing::warn!(attempt, "S3 schema load failed: {e}, retrying in 2s…");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Err(e) => {
                    tracing::warn!("S3 schema load failed after 5 attempts: {e}");
                }
            }
        }
        loaded
    };
    let snapshot_data = match snapshot_data {
        Some(data) => data,
        None => {
            tracing::info!("no schema snapshot in S3 after retries — starting fresh");
            return Ok(());
        }
    };

    let snapshot: ferrosa_schema::SchemaSnapshot =
        serde_json::from_slice(&snapshot_data).map_err(|e| format!("bad schema.json: {e}"))?;

    let table_count = snapshot.tables.len();
    let ks_count = snapshot.keyspaces.len();

    // Apply schema (creates keyspaces, tables, roles, grants)
    schema.apply_snapshot(snapshot)?;

    // Load manifest to know what SSTables exist in S3.
    let (manifest, _version) = ferrosa_storage::Manifest::load(store.as_ref(), prefix).await?;
    let sstable_count: usize = manifest.sstables.values().map(|v| v.len()).sum();

    // Download SSTables from S3 to local disk BEFORE registering tables,
    // so register_table() finds them and opens readers.
    let snap = schema.snapshot();
    let mut downloaded_total = 0usize;
    for ((_ks, _tbl), table_meta) in &snap.tables {
        if ferrosa_schema::is_system_keyspace(&table_meta.keyspace) {
            continue;
        }
        let table_id = ferrosa_storage::TableId::new(&table_meta.keyspace, &table_meta.name);
        match storage
            .download_sstables_from_s3(&table_id, &manifest)
            .await
        {
            Ok(n) => downloaded_total += n,
            Err(e) => {
                tracing::warn!(
                    table = %table_meta.name,
                    ks = %table_meta.keyspace,
                    "failed to download SSTables from S3: {e}"
                );
            }
        }
    }

    // Register each table with the StorageEngine — will find downloaded SSTables on disk.
    for ((_ks, _tbl), table_meta) in &snap.tables {
        if ferrosa_schema::is_system_keyspace(&table_meta.keyspace) {
            continue;
        }
        let storage_schema = table_meta.to_storage_schema();
        if let Err(e) = storage.register_table(storage_schema) {
            tracing::warn!(
                table = %table_meta.name,
                ks = %table_meta.keyspace,
                "failed to register table from S3 bootstrap: {e}"
            );
        }
    }

    tracing::info!(
        ks_count,
        table_count,
        sstable_count,
        downloaded_total,
        "S3 bootstrap complete: schema restored, SSTables downloaded"
    );

    Ok(())
}

/// Persist the current schema snapshot to `{data_dir}/schema.json` for local restart recovery.
///
/// This ensures user-created keyspaces/tables survive binary upgrades where the
/// data directory is preserved but the in-memory schema starts fresh.
fn persist_schema_locally(
    data_dir: &Path,
    schema: &ferrosa_schema::Schema,
) -> ferrosa_common::Result<()> {
    let snap = schema.snapshot();
    ferrosa_storage::schema_snapshot::SchemaSnapshotStore::new(data_dir).persist(&snap)
}

/// Load a schema snapshot from `{data_dir}/schema.json`, if it exists.
///
/// Returns `Ok(None)` only when the file does not exist. Invalid input is
/// quarantined and returned as an error so startup cannot silently lose schema.
fn load_local_schema(
    data_dir: &Path,
) -> ferrosa_common::Result<Option<ferrosa_schema::SchemaSnapshot>> {
    ferrosa_storage::schema_snapshot::SchemaSnapshotStore::new(data_dir).load()
}

/// Register every non-system table the registry currently holds with the
/// storage engine so reads work.
///
/// Extracted from `main` so the registration step is reachable from a test.
fn register_user_tables_with_storage(
    storage: &ferrosa_storage::StorageEngine,
    schema: &ferrosa_schema::Schema,
) -> ferrosa_common::Result<()> {
    let snap = schema.snapshot();
    for ((_ks, _tbl), table_meta) in &snap.tables {
        if ferrosa_schema::is_system_keyspace(&table_meta.keyspace) {
            continue;
        }
        storage.register_table(table_meta.to_storage_schema())?;
    }
    Ok(())
}

/// Report a table-less local schema snapshot, and what the other durable source
/// holds, without pretending the tables can be rebuilt.
///
/// A snapshot with keyspaces but no tables restores a registry that lists no
/// user tables, so CQL answers `keyspace '<ks>' not found` for data whose
/// SSTables are still on disk. The tempting fix — rebuild the registry from
/// `storage-schema.json` — is NOT possible: that format carries the partition
/// key's *type* (`key_type`, e.g. `...marshal.Int32Type`) but never the key
/// column's *name*, and it carries no clustering order, no column masks, and no
/// table id. PK bytes are decoded positionally against the declared type at the
/// key column's index, so a synthesised name would either fail the query or
/// return a wrongly-named column. A fabricated name is strictly worse than this
/// explicit report, so none is invented (D-47, t_2db96eb9).
///
/// The durable fix is to persist a schema record when DDL is acknowledged rather
/// than only from the maintenance loop's tick (t_0acc233d), so a table-less
/// snapshot never becomes the last writer on disk in the first place.
fn report_table_less_schema_snapshot(data_dir: &Path, keyspace_count: usize) {
    let storage_side = ferrosa_storage::schema_snapshot::user_tables_in_storage_schema(data_dir);
    let storage_side: Vec<String> = storage_side
        .into_iter()
        .map(|(ks, table)| format!("{ks}.{table}"))
        .collect();

    if storage_side.is_empty() {
        tracing::warn!(
            data_dir = %data_dir.display(),
            keyspaces = keyspace_count,
            "local schema snapshot contains no tables and storage-schema.json has no user \
             table either; if this node had user tables, their SSTables are on disk but \
             unrecoverable without a schema — see the acknowledged-DDL durability task"
        );
        return;
    }

    tracing::warn!(
        data_dir = %data_dir.display(),
        keyspaces = keyspace_count,
        recoverable_tables = storage_side.len(),
        tables = %storage_side.join(", "),
        "local schema snapshot contains no tables, but storage-schema.json names these user \
         tables. They are NOT being restored into the CQL registry: that format carries the \
         partition key's type but not its column name, so the tables could be read by token \
         yet not named in CQL. Restarting will serve them again only once the schema snapshot \
         is repopulated (acknowledged-DDL durability, t_0acc233d)"
    );
}

/// The value the maintenance loop seeds `last_schema_version` with before its
/// first tick.
///
/// The loop's schema-persist guard is "has the version changed since I last
/// wrote?" — the version it "last wrote" before it has written anything is the
/// version the registry ALREADY holds at startup, because that snapshot was
/// restored from disk (or is freshly seeded). Seeding this with `Uuid::nil()`
/// made the guard true on the loop's immediate first tick, so every process
/// wrote a snapshot within milliseconds of binding — and at that instant the
/// registry held only system keyspaces, so the table-less snapshot became the
/// last writer on disk. A SIGKILL before the next 30s tick then persisted it,
/// and the node came back with its user tables gone (D-47). Seeding from the
/// current version makes the first tick a no-op.
fn maintenance_last_schema_version(current: uuid::Uuid) -> uuid::Uuid {
    current
}

/// Whether the maintenance loop should persist a schema snapshot this tick.
///
/// Pure so the decision is unit-testable without driving the maintenance loop
/// (`maintenance::run_maintenance_loop`).
fn should_persist_schema(current: uuid::Uuid, last: uuid::Uuid) -> bool {
    current != last
}

/// Persist the current schema snapshot to S3 for cold restart recovery.
async fn persist_schema_to_s3(
    storage: &ferrosa_storage::StorageEngine,
    schema: &ferrosa_schema::Schema,
) {
    let Ok((os_config, store)) = storage.object_store_and_config() else {
        return;
    };
    let snap = schema.snapshot();
    match serde_json::to_vec_pretty(&*snap) {
        Ok(json) => {
            if let Err(e) =
                ferrosa_storage::save_schema_snapshot(store.as_ref(), &os_config.prefix, &json)
                    .await
            {
                tracing::warn!("failed to persist schema to S3: {e}");
            }
        }
        Err(e) => tracing::warn!("failed to serialize schema snapshot: {e}"),
    }
}

/// Handle `--version` / `-V` / `--help` / `-h` and report whether we consumed
/// the invocation.
///
/// Returns `true` when the caller should exit without starting the server.
///
/// Deliberately hand-rolled rather than pulling clap into the daemon: the
/// daemon takes no other arguments (it is configured entirely by
/// `FERROSA_CONFIG` / env), so a full parser would be more surface than
/// behaviour. Unrecognised arguments are intentionally NOT rejected here, to
/// avoid breaking any existing wrapper that passes extra flags.
fn handle_cli_meta_flags() -> bool {
    match cli_meta_output(std::env::args().skip(1)) {
        Some(output) => {
            println!("{output}");
            true
        }
        None => false,
    }
}

/// The exact text `--version` / `--help` should print, or `None` to start the
/// server. Pure so it can be tested without spawning a process.
fn cli_meta_output(args: impl IntoIterator<Item = String>) -> Option<String> {
    let version = format!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    for arg in args {
        match arg.as_str() {
            "--version" | "-V" => return Some(version),
            "--help" | "-h" => {
                return Some(format!(
                    "{version}\n\n\
The Ferrosa database server. Configuration comes from the TOML file\n\
named by FERROSA_CONFIG (default /etc/ferrosa/ferrosa.toml) and from\n\
FERROSA_* environment variables; the server takes no other arguments.\n\n\
  -V, --version    print the version and exit\n\
  -h, --help       print this help and exit"
                ));
            }
            _ => {}
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 0. Version/help, before ANYTHING else.
    //
    // This must run before tracing is initialised and before any config or
    // storage work, for two reasons:
    //   - the output has to be a single clean line a caller can parse, not
    //     interleaved with startup logs;
    //   - previously `ferrosa --version` did not print a version at all. The
    //     flag was simply ignored and the DAEMON STARTED. Anything probing the
    //     binary for its version silently launched a database instead, which is
    //     exactly what installers and update checks want to do.
    if handle_cli_meta_flags() {
        return Ok(());
    }

    // 1. Initialize tracing.
    //
    // Non-blocking writer: every `tracing::info!` etc. goes through
    // an in-process channel to a dedicated logging thread that does
    // the synchronous write to stdout. Without this, a slow stdout
    // consumer (e.g. docker's json-file driver under host disk
    // pressure) back-pressures every emitter — and since background
    // tasks (compaction, schema sync, raft heartbeats) emit
    // hundreds of info!() lines per minute, foreground hot paths
    // like the range merger end up blocking on log writes. We
    // measured cold-cache `SELECT … LIMIT 5` stalls of 30 s+ that
    // disappear with non-blocking logging.
    //
    // The `_log_guard` MUST stay alive for the program lifetime —
    // dropping it shuts the worker thread down, which would flush
    // and then drop any pending events. We bind it in `main` so it
    // lives until the process exits.
    let env_filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    // The config file is read HERE, before the subscriber exists, because the
    // process has to know how to maintain its own log before it writes to it.
    // It is read again below for everything else; a second parse of a small
    // TOML costs nothing next to getting this wrong.
    let early_config_path =
        std::env::var("FERROSA_CONFIG").unwrap_or_else(|_| "/etc/ferrosa/ferrosa.toml".to_string());
    let early_config = load_config(&early_config_path).unwrap_or_else(|_| {
        // A malformed config is reported by the real load below, which can fail
        // the boot properly. Here it only means "no logging section".
        toml::Value::Table(toml::map::Map::new())
    });
    let rotation =
        log_rotation::LogRotationConfig::from_config(&early_config, |key| std::env::var(key).ok());
    if let Err(error) = rotation.validate() {
        // Before any subscriber exists, so this is the only way to say it.
        eprintln!("ferrosa: refusing to start: {error}");
        std::process::exit(2);
    }
    // A directory turns on process-owned rotation. Without one the process
    // writes to stdout exactly as before, which is what a foreground run wants.
    let log_dir =
        config_val_opt("FERROSA_LOG_DIR", &early_config, "logging", "directory").map(expand_tilde);

    let (non_blocking_writer, _log_guard) = match (&log_dir, rotation.enabled) {
        (Some(dir), true) => {
            let dir = std::path::PathBuf::from(dir);
            match log_rotation::RotatingWriter::open(&dir, "ferrosa.log", rotation.clone()) {
                Ok(writer) => tracing_appender::non_blocking(writer),
                Err(error) => {
                    // Say so and keep the logs, rather than start a database
                    // that silently writes its diagnostics nowhere.
                    eprintln!(
                        "ferrosa: cannot open the log directory {}: {error} -- logging to stdout",
                        dir.display()
                    );
                    tracing_appender::non_blocking(std::io::stdout())
                }
            }
        }
        _ => tracing_appender::non_blocking(std::io::stdout()),
    };

    // Before the subscriber. The Sentry layer is inert without a client, and
    // the errors most worth having from a database are the ones raised while
    // it is still starting. Bound here so it lives until the process exits:
    // dropping the guard stops sending.
    let _sentry = sentry_reporting::start();

    if std::env::var("FERROSA_TELEMETRY_ENABLED").as_deref() == Ok("true") {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let sample_rate = ferrosa_cluster::telemetry::FerrosaTelemetryLayer::sample_rate_from_env();

        // Warn on suspicious sample rates in non-dev mode.
        let is_dev = std::env::var("FERROSA_MODE").as_deref() == Ok("development");
        if !is_dev && (sample_rate == 0.0 || sample_rate > 0.1) {
            tracing::warn!(
                sample_rate,
                "telemetry sample rate is {}: consider 0.001..0.1 for production",
                if sample_rate == 0.0 {
                    "zero (no spans will be sampled)"
                } else {
                    "high (>10% of spans sampled)"
                }
            );
        }

        let telemetry_layer = ferrosa_cluster::telemetry::FerrosaTelemetryLayer::new(sample_rate);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(log_ansi_from_env())
                    .with_writer(non_blocking_writer),
            )
            .with(telemetry_layer)
            .with(sentry_reporting::layer())
            .init();
    } else {
        // Registry rather than the fmt() builder, so the Sentry layer can sit
        // beside the writer. The writer and filter are unchanged.
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(log_ansi_from_env())
                    .with_writer(non_blocking_writer),
            )
            .with(sentry_reporting::layer())
            .init();
    }

    tracing::info!("ferrosa starting");

    // 1b. Load TOML config file (env vars override file values)
    let config_path =
        std::env::var("FERROSA_CONFIG").unwrap_or_else(|_| "/etc/ferrosa/ferrosa.toml".to_string());
    let file_config = load_config(&config_path)?;
    if std::path::Path::new(&config_path).exists() {
        tracing::info!(path = %config_path, "loaded config file");
    }

    // 2. Load/generate host_id
    //
    // Expand a leading `~`: the bundled config ships `data_dir = "~/.ferrosa/data"`,
    // and without expansion the engine creates a directory literally named `~` in
    // the process CWD instead of under $HOME (issue #172 follow-up — caught by the
    // install smoke). Every derived path (commit log, compaction) flows from this
    // via `FERROSA_DATA_DIR` below, so expanding here fixes them all.
    let data_dir = expand_tilde(config_val(
        "FERROSA_DATA_DIR",
        &file_config,
        "storage",
        "data_dir",
        "/var/lib/ferrosa",
    ));
    // Parse and validate the authoritative registry snapshot before opening
    // storage or serving any protocol. Corrupt or storage-only legacy input is
    // quarantined and aborts startup instead of becoming an empty live schema.
    let local_schema_snapshot = load_local_schema(Path::new(&data_dir))?;
    std::fs::create_dir_all(&data_dir)?;
    let host_id = load_or_generate_host_id(Path::new(&data_dir));

    // Pin the data dir the storage engine will use to the one we just resolved
    // from env / `[storage].data_dir` TOML / default (issue #172). Without this,
    // `StorageEngineConfig::from_env()` below independently re-defaults `data_dir`
    // to `/var/lib/ferrosa` and derives the commit-log + compaction paths from
    // it, IGNORING `[storage].data_dir` in the config file. On a non-root install
    // (e.g. macOS `~/.ferrosa/data`) that default is not writable, so the engine
    // failed to create it — and before the upload-runtime fix that error unwound
    // through `main` and surfaced as the tokio "drop a runtime" panic instead of
    // a clear message. Setting the env the builder reads keeps every derived path
    // consistent with the host_id/data dir created just above. (Edition 2021:
    // `set_var` is safe; this runs at the very start of `main`, before anything
    // else reads `FERROSA_DATA_DIR`.)
    std::env::set_var("FERROSA_DATA_DIR", &data_dir);

    // Durable local `file://` object-store backend (single-node durability
    // without S3). Resolve from `FERROSA_LOCAL_STORE_PATH` env or `[s3].local_path`
    // TOML, expand a leading `~`, and pin it back into the env so the engine's
    // `ObjectStoreConfig::from_env()` selects the local backend. When set, the
    // `FERROSA_S3_*` settings are ignored. Absent → existing S3/no-store
    // behavior is unchanged.
    if let Some(local_store_path) =
        config_val_opt("FERROSA_LOCAL_STORE_PATH", &file_config, "s3", "local_path")
            .map(expand_tilde)
            .filter(|p| !p.trim().is_empty())
    {
        tracing::info!(
            path = %local_store_path,
            "using durable local file:// object-store backend (S3 settings ignored; \
             disk is the durable store, SSTable eviction disabled)"
        );
        std::env::set_var("FERROSA_LOCAL_STORE_PATH", &local_store_path);
    }

    // Cache-size and object-store timeout tunables: TOML wins, env is the
    // fallback (same bridge as `data_dir` / `local_path` above).
    if let Err(e) = apply_storage_tunables(&file_config) {
        eprintln!("FATAL: invalid storage configuration: {e}");
        std::process::exit(1);
    }

    // 3. Create StorageEngine — use open() on restart to replay commit log
    let mut storage_config = ferrosa_storage::StorageEngineConfig::from_env()?;
    // Allow TOML to override the memtable shard count. `from_env`
    // already honored FERROSA_MEMTABLE_NUM_SHARDS; the TOML knob lets
    // operators tune without setting env vars. Env var takes
    // precedence (the `from_env` parse above already saw it); the
    // TOML value is only applied when the env var was unset OR
    // unparseable, leaving from_env's default (64) in place to be
    // overwritten.
    if std::env::var("FERROSA_MEMTABLE_NUM_SHARDS").is_err() {
        if let Some(n) = file_config
            .get("storage")
            .and_then(|s| s.get("memtable_num_shards"))
            .and_then(|v| v.as_integer())
            .and_then(|n| usize::try_from(n).ok())
            .filter(|&n| n > 0)
        {
            storage_config.memtable_num_shards = n;
        }
    }
    // BUG-006: `[cql].auth_enabled` in ferrosa.toml was silently ignored.
    // Apply it now if the env var did not already set it via from_env.
    if let Some(toml_auth) = resolve_auth_enabled_toml(&file_config, |k| std::env::var(k).ok()) {
        // TOML wins inside the resolver, so if we got Some(_) it is the
        // operator's authoritative choice (TOML or env); set on config.
        storage_config.auth_enabled = toml_auth;
    }
    // `[jsonb]` limits are validated against the effective commit-log segment
    // size (D14d): a document that cannot fit one segment must not be accepted.
    let jsonb_limits =
        match resolve_jsonb_limits(&file_config, storage_config.commit_log.segment_size as u64) {
            Ok(limits) => limits,
            Err(e) => {
                eprintln!("FATAL: invalid [jsonb] configuration: {e}");
                std::process::exit(1);
            }
        };
    tracing::info!(?jsonb_limits, "jsonb limits");
    let storage_auth_warn = storage_config.auth_warn;
    // Capture auth enablement before the config is consumed by `new`/`open`.
    // Used below to gate the seed-role bootstrap and the 5-minute
    // default-password reminder task. Falls through to false when the env
    // var is unset; see Sprint A of
    // specs/decisions/design-cql-role-auth-rollout.md.
    let storage_auth_enabled = storage_config.auth_enabled;

    // Emit the auth-state startup logs now that the authoritative value is
    // resolved (env → `[cql].auth_enabled` TOML → default). `from_env` no longer
    // logs these: it only saw the env default and mislabeled a TOML
    // `auth_enabled = true` as `source="default"`, making operators think the
    // config was ignored (issue #172). Report the value actually in force, and
    // where it came from.
    let auth_source = auth_source_label(
        std::env::var_os("FERROSA_AUTH_ENABLED").is_some(),
        file_config
            .get("cql")
            .and_then(|c| c.get("auth_enabled"))
            .is_some(),
    );
    ferrosa_storage::engine::log_cql_auth_state(storage_auth_enabled, auth_source);
    ferrosa_storage::engine::log_auth_warn_state(storage_auth_enabled, storage_auth_warn);

    // SEC (FMEA epic): the production deployment gate (auth, listener +
    // internode TLS, default superuser password, …) runs as
    // `enforce_production_requirements` once the internode config is resolved
    // below, before any listener binds.

    let storage_upload_threads = std::env::var("FERROSA_STORAGE_UPLOAD_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or(4);
    // Dedicated multi-thread runtime for S3 uploads, isolated from the main
    // serving runtime. It must outlive the whole process. Critically, it must
    // NEVER be dropped from within this `#[tokio::main]` async context: dropping
    // a tokio `Runtime` inside an async context panics ("Cannot drop a runtime
    // in a context where blocking is not allowed"). Held as a plain local (as it
    // was, via `Arc`), it dropped at the end of `main` — and on any early
    // error-unwind path — firing that panic *before any listener bound* and
    // masking the real error (issue #172). We instead leak it for the process
    // lifetime; the OS reclaims it at exit. Only a `Handle` is handed out, which
    // does not keep the runtime alive on its own, so the leak is what guarantees
    // the upload runtime stays up for as long as the engine needs it.
    let storage_upload_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(storage_upload_threads)
        .thread_name("storage-upload-rt")
        .enable_all()
        .build()
        .expect("storage upload runtime");
    let storage_upload_handle = storage_upload_runtime.handle().clone();
    std::mem::forget(storage_upload_runtime);
    let has_commitlog_segments = storage_config.commit_log.log_dir.exists()
        && std::fs::read_dir(&storage_config.commit_log.log_dir)
            .map(|entries| {
                entries.filter_map(|e| e.ok()).any(|e| {
                    e.path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
                })
            })
            .unwrap_or(false);

    // Restore-on-boot: a restore requested either by FERROSA_RESTORE_SNAPSHOT
    // (orchestrator) or by POST /api/restore (operator, persisted to the data
    // dir) opens by restoring the snapshot instead of taking the ordinary path.
    // `resolve` drops an intent this node already applied, which is what stops
    // an env var that survives reboots from re-restoring on every start and
    // discarding everything written since.
    let restore_data_dir = storage_config.data_dir.clone();
    let pending_restore = ferrosa_storage::restore::RestoreIntent::resolve(&restore_data_dir)?;

    let (storage, pending_mutations) = if let Some(intent) = &pending_restore {
        tracing::warn!(
            snapshot = %intent.snapshot,
            point_in_time = ?intent.point_in_time,
            force = intent.force,
            "restore-on-boot: opening engine from snapshot instead of local state"
        );
        let engine = ferrosa_storage::StorageEngine::open_from_snapshot(
            storage_config,
            intent,
            &host_id.to_string(),
        )
        .await?;
        // Only now that the engine has actually opened. Marking earlier would
        // skip a restore that never happened.
        intent.mark_applied(&restore_data_dir)?;
        // The marker alone would suppress a re-run, but leaving the request
        // file behind makes a completed restore look permanently pending.
        ferrosa_storage::restore::RestoreIntent::clear_persisted(&restore_data_dir)?;
        tracing::info!(
            snapshot = %intent.snapshot,
            "restore-on-boot: restore complete and recorded"
        );
        (engine, Vec::new())
    } else if has_commitlog_segments {
        tracing::info!("existing commit log segments found — replaying for crash recovery");
        let (engine, mutations) =
            ferrosa_storage::StorageEngine::open(storage_config, Some(&storage_upload_handle))?;
        tracing::info!(
            mutation_count = mutations.len(),
            "commit log replay collected pending mutations"
        );
        (engine, mutations)
    } else {
        let engine =
            ferrosa_storage::StorageEngine::new(storage_config, Some(&storage_upload_handle))?;
        (engine, Vec::new())
    };
    // Verify the S3 bucket is reachable and writable. With FERROSA_S3_REQUIRED an
    // access failure (bad credentials, wrong bucket, no route) stops startup
    // instead of surfacing later as upload warnings.
    let s3_required = ferrosa_storage::upload::config::s3_required_from_env()
        .map_err(|e| format!("invalid FERROSA_S3_REQUIRED: {e}"))?;
    storage.validate_object_store_access(s3_required).await?;
    // Probe object store for conditional put support (CAS).
    // RustFS/MinIO may not support etag-based conditional writes — log a
    // warning but continue. The manifest CAS retry loop will still attempt
    // conditional puts and fall back gracefully.
    if let Err(e) = storage.probe_s3_cas().await {
        tracing::warn!("S3 CAS probe failed (non-fatal): {e}");
    }
    let storage = Arc::new(storage);

    // Attach the push-based CDC bus to the engine's commit log so live CQL
    // SUBSCRIBE (WrittenOnNode / CommittedToCluster) and the Arrow Flight
    // endpoint receive change events. Bounded ring; lock-free, runtime-attached.
    const CDC_BUS_CAPACITY: usize = 1024;
    storage.set_cdc_bus(ferrosa_cdc::CdcBus::new(CDC_BUS_CAPACITY));

    // Register persisted system tables before any local schema restore or
    // cluster-mode Raft replay. Without this, the cluster state machine's
    // SystemTableWriter hits "table not registered: system_schema.*" during
    // startup replay and drops the persistence side effect until a later
    // rewrite happens to touch the same metadata again.
    if let Err(e) = storage.register_system_tables() {
        tracing::warn!(%e, "failed to register system tables at startup");
    }

    ferrosa_storage::StorageEngine::spawn_time_series_materialization_worker(storage.clone());

    // Self-healing controller + automatic-repair scheduler are spawned later,
    // after the ModeController is wired (they need the live ring / peer manager
    // for the verified-healthy-replica ClusterView and the repair executor).
    // See `repair_wiring` + the "automatic repair" block below.

    // Replay any pending S3 uploads that were interrupted by a crash.
    storage.replay_pending_uploads().await;

    // 4. Create Schema
    //
    // G-P0-1 fix (T-R1): Schema::new composites LogAuditSink with
    // SystemTableAuditSink internally, so every audit event is both
    // logged structurally and visible via
    //   SELECT * FROM system_auth.audit_log
    // in live clusters. No callsite change needed here.
    let schema_config = ferrosa_schema::SchemaConfig {
        hasher: ferrosa_schema::PasswordHasher::default(),
        password_policy: ferrosa_schema::PasswordPolicy::permissive(),
        auth_method: ferrosa_schema::AuthMethod::Password,
        rate_limit: ferrosa_schema::RateLimitConfig::default(),
        audit_sink: Box::new(ferrosa_schema::LogAuditSink),
        secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
        mode: ferrosa_schema::DeploymentMode::Development,
    };
    let schema = Arc::new(ferrosa_schema::Schema::new(schema_config)?);

    // Dedicated subsystem runtimes. The main runtime stays supervisor-only;
    // work below is routed to an explicit pool.
    let runtimes = runtime::RuntimeManager::new();
    // Pin them to the process lifetime so no early-error return can drop a
    // tokio Runtime on this async stack and mask the real error with the
    // "Cannot drop a runtime …" panic (issue #172). Must precede any `?` below.
    runtimes.leak_for_process_lifetime();

    // Bound the storage scan-producer pool (t_88223ad0): reserve cores for
    // consensus so a full-table ALLOW FILTERING scan cannot oversubscribe the
    // CPUs and starve raft heartbeats into a CheckQuorum step-down. Init here,
    // before any scan can lazily default it. `FERROSA_SCHED_RESERVED_CORES`
    // (default 1) tunes the headroom.
    let sched_reserved = std::env::var("FERROSA_SCHED_RESERVED_CORES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(ferrosa_sched::DEFAULT_RESERVED_CORES);
    let sched_pool = ferrosa_sched::init_global_pool(
        ferrosa_sched::Reservation::from_available_parallelism(sched_reserved),
    );
    tracing::info!(
        capacity = sched_pool.capacity(),
        reserved = sched_reserved,
        "scheduler: bounded scan-producer pool initialized (consensus headroom reserved)"
    );

    // Runtime-stall detector (Phase 3, O_DIRECT + I/O). Watches the CQL request
    // runtime's own scheduling latency: a healthy runtime wakes this liveness
    // task every ~tick, but when a worker blocks on saturated disk (D-state,
    // `rq_qos_wait`/`folio_wait` — the multi-second freeze the 2026-07-22 Fly A/B
    // caught only under gdb) the wake is delayed, and the overrun is recorded as
    // `ferrosa_sched_runtime_stall_*`. Runs ON `runtimes.cql` so it measures what
    // interactive clients experience; the runtime is leaked for process lifetime,
    // so the task lives forever (handle intentionally dropped).
    let stall_threshold_ms = std::env::var("FERROSA_RUNTIME_STALL_THRESHOLD_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(ferrosa_sched::runtime_monitor::DEFAULT_THRESHOLD.as_millis() as u64);
    let _stall_monitor = runtimes.cql.spawn(async move {
        ferrosa_sched::runtime_monitor::spawn(
            ferrosa_sched::runtime_monitor::DEFAULT_TICK,
            std::time::Duration::from_millis(stall_threshold_ms),
            // Edges, not events. This fired once per stalled tick, and one node
            // logged 19,115 of them into an unrotated file on the very disk
            // whose saturation caused the stalls. Two lines an outage: it began,
            // it ended, and what it cost. Every stall is still counted into
            // ferrosa_sched_runtime_stall_*.
            |edge| match edge {
                ferrosa_sched::runtime_monitor::StallEdge::Started { overrun } => {
                    tracing::warn!(
                        stall_ms = overrun.as_millis() as u64,
                        "runtime scheduling stall STARTED: the CQL request runtime is frozen \
                         — a worker is blocked (likely saturated disk I/O). Interactive \
                         latency is degraded until this recovers. See \
                         ferrosa_sched_runtime_stall_* and \
                         specs/decisions/022-scheduler-vruntime-unit.md (Phase 3)."
                    );
                }
                ferrosa_sched::runtime_monitor::StallEdge::Recovered { stalls, worst } => {
                    tracing::warn!(
                        stalls,
                        worst_ms = worst.as_millis() as u64,
                        "runtime scheduling stall RECOVERED: the CQL request runtime is \
                         scheduling normally again."
                    );
                }
            },
        );
    });
    tracing::info!(
        threshold_ms = stall_threshold_ms,
        "runtime-stall detector armed on the CQL request runtime"
    );

    // 4a. Restore schema from local disk or S3.
    //
    // Priority:
    //   1. Local schema.json (survives binary upgrades with same data dir)
    //   2. S3 schema.json (cold start with empty local data, or local schema lost)
    //   3. Start fresh (no schema found anywhere)
    let data_path = Path::new(&data_dir);
    let mut schema_restored = false;

    if let Some(snapshot) = local_schema_snapshot {
        let ks_count = snapshot.keyspaces.len();
        let table_count = snapshot.tables.len();
        // A snapshot that restored keyspaces but ZERO tables is a usable-looking
        // file that hides every user table from CQL: `apply_snapshot` restores
        // nothing table-wise, `schema_restored` then suppresses the S3 fallback,
        // and any durable SSTables are unreadable because their schema is gone.
        // That is the D-47 shape (t_2db96eb9). Report it loudly with what the
        // other durable source holds, and continue rather than aborting: aborting
        // would make a recoverable node unbootable.
        let table_less = table_count == 0 && !snapshot.keyspaces.is_empty();
        schema.apply_snapshot(snapshot)?;
        tracing::info!(
            ks_count,
            table_count,
            "restored schema from local schema.json"
        );
        schema_restored = true;

        if table_less {
            report_table_less_schema_snapshot(data_path, ks_count);
        }

        // Register existing tables with the storage engine so reads work. The
        // registry holds the KEY COLUMN NAMES the storage schema cannot carry;
        // registering the storage schema instead would make the table readable
        // by token but not nameable in CQL (D-47).
        register_user_tables_with_storage(&storage, &schema)?;
    }

    if !schema_restored && storage.has_s3() {
        tracing::info!("no local schema — attempting S3 bootstrap");
        if let Err(e) = bootstrap_from_s3(&storage, &schema).await {
            tracing::warn!("S3 bootstrap failed (non-fatal, starting fresh): {e}");
        } else {
            // Persist the S3 schema locally so future restarts don't need S3
            persist_schema_locally(data_path, &schema)?;
        }
    }

    // Replay the commit log before reconstructing indexes, types, functions,
    // and roles from their system tables. An acknowledged ALTER ROLE may exist
    // only in the commit log after a crash; loading system_auth first would
    // miss that row and seed the default password again.
    if !pending_mutations.is_empty() {
        tracing::info!(
            count = pending_mutations.len(),
            "replaying commit log mutations into memtables"
        );
        if let Err(e) = storage.replay_mutations(pending_mutations) {
            tracing::error!(%e, "commit log replay failed — some data may be lost");
        } else {
            tracing::info!("commit log replay complete — all pending mutations restored");
        }
    }

    // 4b'. Re-register secondary indexes from the persisted
    // `system_schema.indexes` table. `load_local_schema` (schema.json) restores
    // table schemas with no indexes, so without this every secondary index is
    // silently dropped on restart. This runs after system tables (step 3) and
    // user tables (above) are registered so `add_index` can resolve targets.
    // The storage schema has no partition-key column names, so they come from
    // the CQL schema — without them an index on a partition-key column (the
    // tenant index of a `((tenant_id, session_id), ..)` table) is dropped here
    // while the planner keeps selecting it (t_50c8bc7d).
    let partition_keys: ferrosa_storage::engine::PartitionKeyColumns = schema
        .snapshot()
        .tables
        .values()
        .map(|table| {
            (
                ferrosa_storage::TableId::new(&table.keyspace, &table.name),
                table.partition_key.clone(),
            )
        })
        .collect();
    match storage.reload_indexes_from_system_schema(&partition_keys) {
        // A non-zero `skipped` (dangling registrations) is already warned
        // about — with a count — inside the reload, and surfaces in the
        // `ferrosa_storage_index_reload_skipped_rows_total` metric.
        Ok(outcome) if outcome.restored > 0 => {
            tracing::info!(
                count = outcome.restored,
                skipped = outcome.skipped,
                "re-registered persisted secondary indexes after restart"
            )
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(%e, "failed to reload secondary indexes from system_schema"),
    }

    // 4b'½. Replay persisted indexes into the SCHEMA REGISTRY too (the reload
    // above only repopulates the storage engine). schema.json carries no
    // indexes, so the CQL router's `resolve_fulltext_index_name` — which reads
    // SchemaSnapshot.indexes — would fall back to the bare column name after a
    // restart, silently breaking full-text search even though the FTI sidecars
    // are intact on disk. This restores the index set the router resolves
    // against.
    {
        let loader =
            ferrosa_cluster::system_table_loader::SystemTableLoader::new(Arc::clone(&storage));
        match loader.replay_indexes_into_schema(&schema) {
            Ok(count) if count > 0 => {
                tracing::info!(
                    count,
                    "replayed persisted indexes into schema registry after restart"
                )
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(%e, "failed to replay indexes into schema registry from system_schema")
            }
        }
    }

    // 4b''. Reconstruct user-defined types from the persisted
    // `system_schema.types` table. `schema.json` does not carry UDTs, so without
    // this every CREATE TYPE is silently lost on restart. Runs after system
    // tables (step 3) and user keyspaces are registered so create_type_internal
    // can resolve the owning keyspace.
    {
        let loader =
            ferrosa_cluster::system_table_loader::SystemTableLoader::new(Arc::clone(&storage));
        match loader.replay_types_into_schema(&schema) {
            Ok(count) if count > 0 => {
                tracing::info!(
                    count,
                    "reconstructed persisted user-defined types after restart"
                )
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(%e, "failed to reconstruct user-defined types from system_schema")
            }
        }
    }

    // 4b'''. Reconstruct user-defined functions from the persisted
    // `system_schema.functions` table. Like UDTs, UDFs are not carried in
    // `schema.json`, so without this every CREATE FUNCTION is silently lost on
    // restart. Runs after user types (step 4b'') so a function whose signature
    // references a UDT can resolve it.
    {
        let loader =
            ferrosa_cluster::system_table_loader::SystemTableLoader::new(Arc::clone(&storage));
        match loader.replay_functions_into_schema(&schema) {
            Ok(count) if count > 0 => {
                tracing::info!(
                    count,
                    "reconstructed persisted user-defined functions after restart"
                )
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(%e, "failed to reconstruct user-defined functions from system_schema")
            }
        }
    }

    if storage_auth_enabled {
        let loader =
            ferrosa_cluster::system_table_loader::SystemTableLoader::new(Arc::clone(&storage));
        match loader.replay_roles_into_schema(&schema) {
            Ok(count) if count > 0 => {
                tracing::info!(count, "replayed persisted roles from system_auth")
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(%e, "failed to replay persisted roles"),
        }

        // Seed only after both schema.json and system_auth.roles have been
        // restored. A fresh registry always contains the built-in cassandra
        // role; seeding before recovery recreated the other defaults on every
        // start and let a stale bootstrap hash win after an unclean shutdown.
        ferrosa_schema::auth::bootstrap::seed_default_roles(&schema)?;
        let schema_for_warn = Arc::clone(&schema);
        runtimes.background.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(5 * 60)).await;
            if ferrosa_schema::auth::bootstrap::admin_password_is_default(&schema_for_warn) {
                tracing::warn!(
                    "ferrosa_admin is still using the default seed password \
                     after 5 minutes — rotate it NOW. See \
                     specs/decisions/design-cql-role-auth-rollout.md Sprint A."
                );
            }
        });

        match loader.replay_role_permissions_into_schema(&schema) {
            Ok(count) if count > 0 => {
                tracing::info!(
                    count,
                    "replayed persisted role permissions from system_auth"
                )
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(%e, "failed to replay persisted role permissions"),
        }
    } else {
        tracing::info!(
            "auth_enabled=false — skipping seed-role bootstrap. Set \
             FERROSA_AUTH_ENABLED=true to enforce CQL role auth."
        );
    }

    // 5. Create ModeController — starts in standalone mode
    let mut cluster_config = ferrosa_cluster::ClusterConfig::from_env();
    cluster_config.hinted_handoff_dir = resolve_hinted_handoff_dir(
        &file_config,
        data_path,
        std::env::var("FERROSA_HINTED_HANDOFF_DIR").ok().as_deref(),
    );
    let cluster_config = Arc::new(cluster_config);
    // Capture num_tokens locally before cluster_config is consumed by
    // ModeController — needed downstream when populating system.local.tokens.
    let num_tokens = cluster_config.num_tokens as usize;
    // A typo in an internode value stops startup; a seed or broadcast name that may
    // simply not resolve yet is logged at WARN and startup continues.
    let mut net_config_mut = ferrosa_net::config::NetConfig::from_env_checked()?;
    // Seed the base from `FERROSA_INTERNODE_*`, then let the config file win:
    // `internode.bind`, `internode.broadcast`, etc. in ferrosa.toml override
    // the env-derived defaults (TOML-wins precedence).
    if let Err(error) = apply_internode_toml_overrides(&mut net_config_mut, &file_config) {
        eprintln!("FATAL: {error}");
        std::process::exit(1);
    }
    // Node-wide `[tls]` (FERROSA_TLS_*): one certificate for every listener
    // and internode; anything a section sets itself wins.
    let node_tls = listener_tls::resolve_node_tls_or_exit(&file_config);
    listener_tls::apply_node_tls_to_internode(
        &mut net_config_mut,
        &node_tls,
        listener_tls::internode_require_is_explicit(&file_config),
    );
    let net_config = Arc::new(net_config_mut);

    // SEC (FMEA epic: t_87a50318 auth, t_27bf4674 + t_d5d122ba TLS): refuse to
    // start a production node that fails an operator-fixable security
    // requirement. Every listener's TLS settings are resolved here so the gate
    // runs before ANY listener binds — the internode RPC server included.
    // No-op in development mode.
    let listener_tls_inputs = ListenerTlsInputs::resolve(&file_config, &node_tls);
    if let Err(error) = listener_tls_inputs.validate() {
        eprintln!("FATAL: {error}");
        std::process::exit(1);
    }
    enforce_production_requirements(
        storage_auth_enabled,
        &listener_tls_inputs,
        net_config.require_tls,
        !ferrosa_schema::auth::bootstrap::admin_password_is_default(&schema),
    );

    // Build handler registry — shared between RPC server and ModeController.
    // Catch-up handler is always available; pair write/role-swap handlers are
    // registered dynamically by ModeController on mode transition.
    let registry = Arc::new(ferrosa_net::rpc::HandlerRegistry::new());
    registry.register(
        ferrosa_net::codec::MsgType::Ping,
        Arc::new(ferrosa_net::rpc::PingHandler),
    );
    let catchup_handler = Arc::new(ferrosa_cluster::pair::catchup::PairCatchUpHandler::new(
        storage.clone(),
    ));
    registry.register(ferrosa_net::codec::MsgType::PairCatchUp, catchup_handler);

    // Register MutationForwardHandler for cluster mode write forwarding
    let mutation_fwd_handler = Arc::new(ferrosa_cluster::MutationForwardHandler::new(
        storage.clone(),
    ));
    registry.register(
        ferrosa_net::codec::MsgType::MutationForward,
        mutation_fwd_handler,
    );

    // Register TruncateForwardHandler for cluster mode TRUNCATE propagation
    let truncate_fwd_handler = Arc::new(ferrosa_cluster::TruncateForwardHandler::new(
        storage.clone(),
    ));
    registry.register(
        ferrosa_net::codec::MsgType::TruncateForward,
        truncate_fwd_handler,
    );

    // Register the three anti-entropy repair handlers. The companion
    // RemoteRepairStore on initiating nodes will issue Fetch/Apply RPCs
    // against these handlers during a repair session; the Merkle handler
    // builds and returns a tree for the requested table+range.
    registry.register(
        ferrosa_net::codec::MsgType::RepairMerkleRequest,
        Arc::new(ferrosa_cluster::RepairMerkleHandler::new(storage.clone())),
    );
    registry.register(
        ferrosa_net::codec::MsgType::RepairFetchRequest,
        Arc::new(ferrosa_cluster::RepairFetchHandler::new(storage.clone())),
    );
    registry.register(
        ferrosa_net::codec::MsgType::RepairApplyRequest,
        Arc::new(ferrosa_cluster::RepairApplyHandler::new(storage.clone())),
    );
    // CQL result cursors (paged ORDER BY / DISTINCT) live on the node that
    // built them; peers forward a client's next-page request here.
    let result_cursors = Arc::new(ferrosa_cql::result_cursor::ResultCursorRegistry::new(
        ferrosa_cql::result_cursor::ResultCursorConfig::from_env(),
        host_id,
    ));
    registry.register(
        ferrosa_net::codec::MsgType::ResultCursorPage,
        Arc::new(ferrosa_cql::result_cursor::ResultCursorPageHandler::new(
            result_cursors.clone(),
        )),
    );

    let (mode_controller, handles) = ferrosa_cluster::ModeController::new(
        cluster_config,
        net_config.clone(),
        host_id,
        storage.clone(),
        schema.clone(),
        registry.clone(),
    );

    // T-300 (D24, D15a): jsonb columns are allowed on a standalone node only
    // until the capability ledger lands. A node that will not stay standalone
    // (seeds configured, or a former cluster member) and whose persisted
    // schema holds jsonb must not start: FATAL, naming the tables. There is
    // no flag to bypass this.
    if let Err(refused) = mode_controller.check_startup_jsonb() {
        tracing::error!(%refused, "FATAL: persisted schema holds jsonb columns outside standalone mode");
        return Err(refused.into());
    }

    // OpenRaft starts only after the controller exists. Install supervision
    // now so a panic can atomically close readiness and CQL data admission
    // while the process remains responsive for diagnosis.
    runtime::install_consensus_panic_hook(mode_controller.consensus_health());

    // 6. Create PeerManager — ModeController is the PeerEventListener
    let peer_manager = Arc::new(ferrosa_net::peer::PeerManager::with_weak_listener(
        net_config.clone(),
        host_id,
        mode_controller.as_peer_listener(),
    ));
    peer_manager.set_raft_runtime(runtimes.raft.clone());
    peer_manager.set_data_runtime(runtimes.data.clone());
    mode_controller.set_peer_manager(peer_manager.clone());
    mode_controller.set_raft_runtime(runtimes.raft.clone());
    mode_controller.set_data_runtime(runtimes.data.clone());

    // Shared HLC: created once per process. The CQL transaction committer mints
    // `t0` from it (below, via SharedState.accord_clock) and the node's
    // AccordStateMachine — built at formation — advances the SAME clock past
    // every execution timestamp it witnesses, so a later append's `t0` stays
    // above earlier-committed appends even after they are GC'd (t_813caf39).
    let accord_clock = {
        let host_bytes = host_id.as_bytes();
        let node_id = u64::from_be_bytes(host_bytes[..8].try_into().expect("uuid has 16 bytes"));
        Arc::new(ferrosa_common::accord::HybridLogicalClock::new(node_id, 0))
    };
    mode_controller.set_accord_clock(accord_clock.clone());

    // 6b. Start heartbeat loop for peer failure detection
    let heartbeat_pm = peer_manager.clone();
    runtimes.raft.spawn(async move {
        heartbeat_pm.run_heartbeat_loop().await;
    });

    // 6c. The self-healing controller is spawned ONCE, below with the repair
    // wiring, behind the verified-healthy-replica view. A second controller
    // used to start here as well (t_396d4c80): two controllers scanned every
    // table and acted on the same corrupt generations, and this one gated
    // quarantine on peer LIVENESS, not on a peer proven to hold a good copy
    // (FMEA #1).

    // 7. Start internode RPC server with inbound peer callback
    let rpc_server = Arc::new(
        ferrosa_net::rpc::server::RpcServer::new((*net_config).clone(), host_id, registry)
            .with_inbound_callback(mode_controller.clone())
            .with_raft_runtime(runtimes.raft.clone())
            .with_data_runtime(runtimes.data.clone()),
    );
    let internode_addr = rpc_server.start_and_get_addr().await?;
    tracing::info!(%internode_addr, %host_id, "internode server listening");

    // Initialize the cluster-wide paging HMAC key BEFORE the CQL server serves a
    // query, so every coordinator signs/verifies paging cursors with the SAME
    // key. Without this each node picks a random per-process key and a cursor
    // issued by one coordinator is rejected by another, breaking multi-node
    // paged reads mid-scan (derives from FERROSA_PAGING_HMAC_KEY / internode PSK
    // / cluster name, in that order).
    ferrosa_cql::paging::init_paging_hmac_key(net_config.psk.as_deref(), &net_config.cluster_name);

    // 8. Start CQL server
    let cql_bind = resolve_cql_bind(&file_config);
    // Deprecated direct override: prefer driving auth from FERROSA_AUTH_ENABLED.
    // Set to None when neither env nor file specifies; resolver below
    // defaults to `!storage_auth_enabled` in that case.
    let auth_disabled_override: Option<bool> = config_val_opt(
        "FERROSA_AUTH_DISABLED",
        &file_config,
        "cql",
        "auth_disabled",
    )
    .map(|s| {
        tracing::warn!(
            "FERROSA_AUTH_DISABLED (or [cql].auth_disabled in config file) is \
                 deprecated; use FERROSA_AUTH_ENABLED as the single source of truth. \
                 Honoring the override for this release."
        );
        s == "true" || s == "1"
    });
    // `[cql] tls_cert/tls_key/require_tls` were resolved (strictly) with every
    // other listener's TLS settings before the production gate above.
    let cql_tls = listener_tls_inputs.cql.clone();
    let cql_max_connections: usize = config_val(
        "FERROSA_CQL_MAX_CONNECTIONS",
        &file_config,
        "cql",
        "max_connections",
        "1024",
    )
    .parse()?;
    let cql_max_connections_per_ip: usize = config_val(
        "FERROSA_CQL_MAX_CONNECTIONS_PER_IP",
        &file_config,
        "cql",
        "max_connections_per_ip",
        "64",
    )
    .parse()?;
    // Per-connection in-flight request ceiling. This is the valve that sheds with
    // `Overloaded("request backpressure")` when a client drives more concurrent
    // requests than the connection admits; it was previously unreachable from any
    // config, which left the node shedding before CPU/disk were ever stressed.
    // Raise it only alongside a latency win: throughput = in-flight / latency.
    let cql_max_in_flight_per_connection: usize = resolve_cql_positive_usize(
        "FERROSA_CQL_MAX_IN_FLIGHT_PER_CONNECTION",
        &file_config,
        "max_in_flight_per_connection",
        128,
    )?;
    let cql_config = ferrosa_cql::server::ServerConfig {
        bind_addr: cql_bind,
        auth_disabled: ferrosa_cql::server::resolve_auth_disabled(
            storage_auth_enabled,
            auth_disabled_override,
        ),
        max_connections: cql_max_connections,
        max_connections_per_ip: cql_max_connections_per_ip,
        max_in_flight_per_connection: cql_max_in_flight_per_connection,
        tls_cert_path: cql_tls.cert,
        tls_key_path: cql_tls.key,
        require_tls: cql_tls.require_tls,
        ..ferrosa_cql::server::ServerConfig::default()
    };
    // Determine the advertised CQL address+port for system.local.
    // Both are what drivers like cdrs-tokio use to reconcile contact
    // points against the advertised local node during session bootstrap;
    // advertising the container-bind port (9042) when the host-reachable
    // port is 19042 hangs session build. See cql_broadcast::parse_cql_broadcast.
    // An empty value means "not set" (compose files write `VAR=`); any other value
    // that does not parse or resolve stops startup instead of advertising loopback.
    let cql_broadcast_env = std::env::var("FERROSA_CQL_BROADCAST")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let (cql_broadcast_addr, cql_broadcast_port) = match cql_broadcast_env {
        Some(addr_str) => cql_broadcast::parse_cql_broadcast(&addr_str, cql_bind.port())?,
        None => {
            // Gap 11: when CQL bind is 0.0.0.0 (the normal containerised
            // case), the broadcast address must be the externally reachable
            // IP so cluster peers and drivers can distinguish nodes.  Fall
            // back to the internode-broadcast IP (which the operator already
            // set to a reachable hostname/IP for gossip), not localhost —
            // localhost made every node in a docker-compose cluster report
            // `127.0.0.1` and load balancers / drivers couldn't tell them
            // apart.  See ferrosa-nosqlbench/docs/initial-gaps-found.md.
            let ip = if cql_bind.ip().is_unspecified() {
                net_config.broadcast_addr.ip()
            } else {
                cql_bind.ip()
            };
            (ip, cql_bind.port())
        }
    };
    tracing::info!(
        broadcast_address = %cql_broadcast_addr,
        broadcast_port = cql_broadcast_port,
        bind_port = cql_bind.port(),
        "CQL broadcast configured — clients will reconnect via this address"
    );
    let internal_topology_cidrs = config_val(
        "FERROSA_CQL_INTERNAL_CLIENT_CIDRS",
        &file_config,
        "cql",
        "internal_client_cidrs",
        "",
    );
    let topology_policy =
        ferrosa_cql::topology::ClientTopologyPolicy::from_csv(&internal_topology_cidrs)
            .map_err(|err| format!("invalid FERROSA_CQL_INTERNAL_CLIENT_CIDRS: {err}"))?;
    if topology_policy.is_empty() {
        tracing::info!("CQL topology view policy: all clients receive public addresses");
    } else {
        tracing::info!(
            cidrs = %internal_topology_cidrs,
            "CQL topology view policy: matching clients receive internal addresses"
        );
    }
    // Gap 11: populate system.local with this node's actual tokens and
    // gossip broadcast address.  Pre-fix, every node reported
    // `tokens=['0']` (the NodeConfig::default sentinel) and
    // `broadcast_address=127.0.0.1`, so a 3-node docker cluster looked
    // to drivers like a single host with two duplicates — driving the
    // 5x throughput gap NoSQLBench observed against Cassandra.
    let local_node_id = ferrosa_cluster::raft::uuid_to_node_id(host_id);
    let tokens: Vec<String> =
        ferrosa_cluster::controller::deterministic_tokens_for_node(local_node_id, num_tokens)
            .into_iter()
            .map(|t| t.to_string())
            .collect();
    let node_config = Arc::new(ferrosa_schema::NodeConfig {
        rpc_address: cql_broadcast_addr,
        rpc_port: cql_broadcast_port,
        internal_rpc_address: net_config.broadcast_addr.ip(),
        internal_rpc_port: cql_bind.port(),
        host_id,
        broadcast_address: net_config.broadcast_addr.ip(),
        broadcast_port: net_config.broadcast_addr.port(),
        listen_address: net_config.broadcast_addr.ip(),
        listen_port: internode_addr.port(),
        tokens,
        ..ferrosa_schema::NodeConfig::default()
    });
    let connection_tracker =
        Arc::new(ferrosa_cql::virtual_tables::connections::ConnectionTracker::new());
    let query_tracker = Arc::new(ferrosa_cql::virtual_tables::active_queries::QueryTracker::new());
    // Index-observability trackers: shared between the router (which records
    // full-scan and index-usage events) and the virtual tables registered
    // below (which expose them via system_observability.*). Use one Arc each
    // so what the router records is what operators read.
    let full_scan_tracker = Arc::new(ferrosa_cql::virtual_tables::FullScanTracker::new());
    let index_usage_tracker = Arc::new(ferrosa_cql::virtual_tables::IndexUsageTracker::new());
    let udf_sandbox = match resolve_udf_sandbox_config(&file_config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: invalid UDF sandbox configuration: {e}");
            std::process::exit(1);
        }
    };
    tracing::info!(
        max_memory_bytes = udf_sandbox.max_memory_bytes,
        "UDF guest memory limit"
    );
    let udf_executor = Arc::new(
        ferrosa_udf::UdfExecutor::new(udf_sandbox).expect("failed to initialize UDF executor"),
    );
    storage.set_time_series_wasm_aggregate_executor(Arc::new(
        ferrosa_cql::wasm_aggregate::UdfTimeSeriesAggregateExecutor::new(Arc::clone(&udf_executor)),
    ));
    let txn_registry_config = ferrosa_cql::txn_registry::TransactionRegistryConfig::from_env();
    let shared_state = Arc::new(ferrosa_cql::router::SharedState {
        core: Arc::new(ferrosa_session::SessionCore {
            engine: storage.clone(),
            schema: schema.clone(),
            node_config,
            cluster_state: handles.cluster_state,
            write_path: handles.write_path,
            ddl_path: handles.ddl_path,
            udf_executor,
            mode_controller: Arc::clone(&mode_controller),
            auth_warn: storage_auth_warn,
            // p0-03c: wire the real PeerManager and a process-wide HLC into
            // SessionCore so LWT statements in cluster mode route through
            // AccordCoordinatorDriver over TCP instead of returning ServerError.
            // The PeerManager was constructed above (step 6) and is already used
            // for heartbeats and DDL forwarding — reuse the same Arc so there is
            // exactly one peer map per process.
            peer_manager: Some(peer_manager.clone()),
            // Derive a stable u64 node identifier from the first 8 bytes of
            // host_id (big-endian). The HLC is created once per process; all
            // Accord transactions on this node share it to guarantee monotone
            // timestamp ordering across concurrent LWT coordinators.
            // The SAME shared clock registered on the ModeController above, so the
            // committer's `t0` and the replica's witnessed-timestamp advances act
            // on one clock (t_813caf39).
            accord_clock: Some(accord_clock.clone()),
            // Same shared slot the ModeController fills at cluster formation, so
            // the Accord committer votes the coordinator's own PreAccept against
            // the node's live AccordState (finishes the sole-replica/self-vote
            // path — otherwise live BEGIN…COMMIT fails "quorum unavailable").
            accord_state: mode_controller.accord_state_slot(),
        }),
        prepared_cache: Arc::new(ferrosa_cql::prepared::PreparedCache::new(64 * 1024 * 1024)),
        param_cache: ferrosa_cql::param_cache::from_env(),
        connection_tracker,
        query_tracker,
        full_scan_tracker: full_scan_tracker.clone(),
        index_usage_tracker: index_usage_tracker.clone(),
        event_sender: tokio::sync::broadcast::channel(64).0,
        last_schema_event: tokio::sync::watch::channel(None).0,
        cql_metrics: Arc::new(ferrosa_cql::observability::CqlMetrics::new()),
        topology_policy,
        // Server-wide (per-node) transaction registry: the connection-independent
        // BEGIN/IN TRANSACTION/COMMIT surface. Runtime bounds keep open staged state
        // finite; the reaper below actively evicts abandoned transactions.
        txn_registry: ferrosa_cql::txn_registry::TransactionRegistry::shared_with_config(
            txn_registry_config,
        ),
        result_cursors,
    });
    // Start the open-transaction reaper (A1b): sweep cadence is configured with
    // the registry bounds; expired transactions are evicted without client input.
    ferrosa_cql::txn_registry::spawn_transaction_reaper(shared_state.txn_registry.clone());
    let auth_disabled = cql_config.auth_disabled;

    // 9a. Register observability virtual tables into the schema's shared registry
    //     so they are visible to both the CQL query router and the web console.
    //     Previously a separate VirtualTableRegistry was created for the web
    //     console only, which caused `SELECT * FROM system_observability.*`
    //     queries to fail with "table not found".
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::connections::ConnectionsTable::new(
            shared_state.connection_tracker.clone(),
        ),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::active_queries::ActiveQueriesTable::new(
            shared_state.query_tracker.clone(),
        ),
    ));
    schema
        .virtual_tables()
        .register(Arc::new(ferrosa_cql::virtual_tables::PeersV2Table::new(
            shared_state.node_config.clone(),
            schema.clone(),
            shared_state.topology_policy.clone(),
            shared_state.cluster_state.clone(),
            "",
        )));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::consolidation_status::ConsolidationStatusTable::new(
            schema.clone(),
        ),
    ));
    let materialization_provider =
        Arc::new(ferrosa_cql::virtual_tables::StorageMaterializationProvider::new(storage.clone()));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::MaterializationQueuesTable::new(
            materialization_provider.clone(),
        ),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::MaterializationStatusTable::new(materialization_provider),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::RrdRuntimeSettingsTable::new(
            storage.time_series_runtime_settings(),
        ),
    ));
    // Storage-backed virtual tables: StorageEngine implements the provider traits.
    schema.virtual_tables().register(Arc::new(
        ferrosa_storage::virtual_tables::StorageStatsTable::new(storage.clone()),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_storage::virtual_tables::ArchiveStatusTable::new(storage.clone()),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_storage::virtual_tables::SnapshotsTable::new(storage.clone()),
    ));
    // Object-store tuning stats; empty unless FERROSA_S3_STATS=1.
    schema.virtual_tables().register(Arc::new(
        ferrosa_storage::virtual_tables::ObjectStoreStatsTable::new(
            ferrosa_storage::upload::stats::global().cloned(),
        ),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_storage::virtual_tables::ObjectStoreOpsTable::new(
            ferrosa_storage::upload::stats::global().cloned(),
        ),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_storage::index::virtual_table::SecondaryIndexesVirtualTable::new(
            storage.index_tracker().clone(),
        ),
    ));

    // T-18: Register Batch 5 observability virtual tables.
    let alert_registry = Arc::new(ferrosa_cql::virtual_tables::AlertRegistry::new());
    schema
        .virtual_tables()
        .register(Arc::new(ferrosa_cql::virtual_tables::AlertsTable::new(
            alert_registry.clone(),
        )));
    let billing_meter = Arc::new(ferrosa_cql::virtual_tables::BillingMeter::new());
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::BillingMetersTable::new(billing_meter.clone()),
    ));
    let fingerprint_tracker = Arc::new(ferrosa_cql::virtual_tables::QueryFingerprintTracker::new());
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::QueryFingerprintsTable::new(fingerprint_tracker.clone()),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::FullScanReasonsTable::new(full_scan_tracker.clone()),
    ));
    schema
        .virtual_tables()
        .register(Arc::new(ferrosa_cql::virtual_tables::IndexUsageTable::new(
            index_usage_tracker.clone(),
        )));
    let table_access_tracker = Arc::new(ferrosa_cql::virtual_tables::TableAccessTracker::new());
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::TableAccessSummaryTable::new(table_access_tracker.clone()),
    ));

    // T-27: Spawn the alert evaluator background task.
    ferrosa_cql::virtual_tables::alerts::spawn_alert_evaluator(
        alert_registry.clone(),
        schema.virtual_tables_arc(),
        ferrosa_net::task_pool::TaskPool::runtime("alerts", runtimes.background.clone()),
    );

    // Register stub virtual tables for deferred observability features.
    ferrosa_cql::virtual_tables::register_all_stubs(schema.virtual_tables());

    // Clone write_path and ddl_path before shared_state is moved into the CQL server.
    let cluster_write_path = shared_state.write_path.clone();
    let cluster_ddl_path = shared_state.ddl_path.clone();
    // Clone the shared execution state for the Flight endpoint before it is
    // moved into the CQL server (feature-gated so it is not an unused clone).
    #[cfg(feature = "flight")]
    let flight_state = shared_state.clone();
    let cql_server =
        ferrosa_cql::server::CqlServer::new(cql_config, shared_state.clone()).with_task_pool(
            ferrosa_net::task_pool::TaskPool::runtime("cql", runtimes.cql.clone()),
        );
    let cql_addr = cql_server.start_background().await?;
    tracing::info!(%cql_addr, "CQL server listening");

    // Health of the listeners that start in background tasks (graph HTTP, Bolt,
    // SPARQL, Postgres). A bind failure used to be one ERROR line while `/readyz`
    // kept answering 200; each listener now records itself here, and `/readyz` and
    // `/metrics` report it.
    let listener_status = std::sync::Arc::new(crate::listener_status::ListenerStatus::default());

    // Health of the supervised background tasks (the flusher and the maintenance
    // loop). A dead or hung flusher used to be one ERROR line while the node kept
    // answering `/readyz` 200 and flushed nothing (t_7681b32b).
    let supervision_status = std::sync::Arc::new(crate::supervisor::SupervisionStatus::default());

    // 9c. Arrow Flight (gRPC) query endpoint — port 8815, behind the `flight`
    // feature. Auth is enforced per-RPC (signed bearer tokens); only
    // anonymous-safe because every read RPC requires a verified token. TLS from
    // `[flight] tls_cert/tls_key/require_tls` (t_58db6320), built through
    // ferrosa_net::tls like every other listener.
    #[cfg(feature = "flight")]
    {
        let flight_addr = resolve_flight_bind(&file_config);
        // `[flight] enabled = false` (FERROSA_FLIGHT_ENABLED=false) keeps the
        // port closed.
        if listener_tls_inputs.flight_enabled != Some(true) {
            tracing::info!("Arrow Flight server disabled ([flight] enabled = false)");
        } else {
            // A certificate means the port serves TLS only, so replica
            // locations in GetFlightInfo are advertised as grpc+tls://.
            let serves_tls = listener_tls_inputs.flight.cert.is_some();
            // Remote replica locations use this node's Flight port unless
            // FERROSA_FLIGHT_PORT says otherwise (nodes share one port).
            let advertised_port = resolve_flight_advertised_port(
                std::env::var(ferrosa_flight::service::ENV_FLIGHT_PORT).ok(),
                flight_addr,
            )
            .unwrap_or_else(|error| {
                eprintln!("FATAL: {error}");
                std::process::exit(1);
            });
            let service = build_flight_service(flight_state)
                .with_tls_locations(serves_tls)
                .with_flight_port(advertised_port);
            let flight_tls = ferrosa_flight::server::FlightTlsConfig {
                cert_path: listener_tls_inputs.flight.cert.clone(),
                key_path: listener_tls_inputs.flight.key.clone(),
                require_tls: listener_tls_inputs.flight.require_tls,
            };
            listener_status.mark_up("flight");
            let flight_status = listener_status.clone();
            runtimes.background.spawn(async move {
                let result =
                    ferrosa_flight::server::serve_service(flight_addr, service, &flight_tls).await;
                let reason = match result {
                    Ok(()) => "Arrow Flight server exited".to_string(),
                    Err(e) => e.to_string(),
                };
                tracing::error!(%flight_addr, error = %reason, "Arrow Flight server stopped");
                flight_status.mark_failed("flight", reason);
            });
        }
    }

    // 9b. Web observability console — reuse the same registry as the CQL router.
    let web_state = web::WebAppState {
        registry: schema.virtual_tables_arc(),
        mode_controller: mode_controller.clone(),
        schema: schema.clone(),
        storage: storage.clone(),
        host_id,
        auth_disabled,
        debug: Some(web::debug::DebugState::new()),
        listeners: listener_status.clone(),
        supervision: supervision_status.clone(),
    };
    // `[web] bind` is authoritative over FERROSA_WEB_BIND, then the loopback
    // default. The resolved address is the one passed to the listener.
    let web_config = resolve_web_config(&file_config, &listener_tls_inputs.web);
    tracing::info!(bind_addr = %web_config.bind_addr, "web console bind configured");
    let web_addr = web::start_web_server(&web_config, web_state).await?;
    tracing::info!(%web_addr, "web console listening");

    // 10. Graph engine — FERROSA_GRAPH_ENABLED env var, then [graph] enabled
    // in ferrosa.toml. BUG-006: prior code only checked the env var.
    let graph_enabled = listener_tls_inputs.graph_enabled;

    // Create a shutdown watch channel for services that need graceful shutdown
    // notification (e.g. the Bolt server).
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // ── Automatic repair: self-heal controller (verified-replica quarantine +
    // refill) + periodic anti-entropy repair scheduler ──────────────────────
    //
    // Wired here (after the ModeController + shutdown channel) because both need
    // the live ring / peer manager. See `repair_wiring` and
    // `specs/proposed/automatic-repair-scheduler-design.md`.
    {
        let auto_cfg = ferrosa_cluster::AutoRepairConfig::from_env();
        // Platform-default RF for the per-table replica-health probe (the gate
        // only needs to find SOME verified healthy peer); per-keyspace RF is
        // used by the scheduler's repair path via the schema (FMEA #11).
        const DEFAULT_RF: usize = 3;

        // Shared production RepairContext (scheduler + refill trigger).
        let repair_ctx = std::sync::Arc::new(crate::repair_wiring::BinaryRepairContext::new(
            mode_controller.clone(),
            storage.clone(),
            schema.clone(),
            auto_cfg.skip_keyspaces.clone(),
            DEFAULT_RF,
        ));

        // Self-heal controller with the REAL verified-healthy-replica view
        // (replaces the single-node stub) + a refill trigger so a quarantine
        // schedules a prompt targeted repair.
        let self_host = mode_controller.host_id();
        let ring_snapshot = {
            let mc = mode_controller.clone();
            move || {
                mc.token_ring()
                    .map(|r| (*r).clone())
                    .unwrap_or_else(ferrosa_cluster::ring::TokenRing::new)
            }
        };
        let topology =
            std::sync::Arc::new(ferrosa_cluster::repair::cluster_view::RingTopology::new(
                self_host,
                DEFAULT_RF,
                ring_snapshot,
            ));
        let probe =
            std::sync::Arc::new(ferrosa_cluster::repair::cluster_view::RpcRepairProbe::new(
                peer_manager.clone(),
                runtimes.data.handle().clone(),
            ));
        let cluster_view: std::sync::Arc<dyn ferrosa_storage::self_heal::ClusterView> =
            std::sync::Arc::new(
                ferrosa_cluster::repair::cluster_view::ClusterRepairView::new(
                    topology.clone(),
                    probe.clone(),
                ),
            );
        // Quarantine → anti-entropy refill trigger. Uses the SAME verified-
        // healthy-replica topology + probe as the posture gate (FMEA #1: only
        // refill from a peer proven to hold a non-corrupt copy), and resolves
        // the repair executor against the CURRENT ring per refill via
        // `build_repair_executor` (the same path the periodic scheduler's
        // RepairContext::build_executor uses). The provider returns `None` —
        // refill skipped, periodic cycle is the backstop — until the node is
        // ring-ready.
        let exec_provider: std::sync::Arc<dyn ferrosa_cluster::repair::ExecutorProvider> = {
            let mode_controller = mode_controller.clone();
            let storage = storage.clone();
            std::sync::Arc::new(move || {
                crate::repair_wiring::build_repair_executor(&mode_controller, &storage)
            })
        };
        let refill_trigger: std::sync::Arc<dyn ferrosa_storage::self_heal::RepairTrigger> =
            std::sync::Arc::new(ferrosa_cluster::repair::ClusterRepairTrigger::new(
                topology,
                probe,
                exec_provider,
            ));
        ferrosa_storage::self_heal::SelfHealController::spawn_with_trigger(
            storage.clone(),
            cluster_view,
            ferrosa_storage::self_heal::SelfHealConfig::from_env(),
            refill_trigger,
        );

        // Periodic anti-entropy repair scheduler (deterministic single-initiator,
        // round-robin, enabled by default). Stops on the shutdown signal.
        let scheduler = ferrosa_cluster::AutoRepairScheduler::new(
            ferrosa_cluster::RepairCoordinator::default(),
            repair_ctx as std::sync::Arc<dyn ferrosa_cluster::RepairContext>,
            auto_cfg,
        );
        ferrosa_cluster::AutoRepairScheduler::spawn(scheduler, shutdown_rx.clone());
    }

    // `[graph] bind` is authoritative over FERROSA_GRAPH_BIND, then the
    // loopback default. Bolt shares its host and resolves its port separately.
    let graph_http_config = resolve_graph_http_config(&file_config, &listener_tls_inputs.graph);

    if graph_enabled {
        let graph_config = ferrosa_graph::engine::GraphConfig {
            enabled: true,
            http: graph_http_config.clone(),
            ..ferrosa_graph::engine::GraphConfig::default()
        };

        let http_config = graph_config.http.clone();
        let graph_write_path = cluster_write_path.clone();
        // Route graph-engine-driven DDL (auto-created
        // system_graph_<ks>.adjacency keyspace + table) through the
        // same DdlPath that regular CQL DDL uses. The cluster state
        // machine then applies the DDL on every replica, so the
        // adjacency table is registered in each node's local schema
        // and StorageEngine — without this, MutationForward writes
        // against the adjacency table are rejected on followers and
        // every graph-edge mutation times out at the coordinator. See
        // specs/in-process/bug-system-graph-ks-not-replicated-on-write-path.md.
        let graph_schema_coordinator: Arc<dyn ferrosa_graph::engine::GraphSchemaCoordinator> =
            Arc::new(ferrosa_graph::engine::ClusterGraphSchemaCoordinator::new(
                cluster_ddl_path.clone(),
            ));
        let graph_engine = Arc::new(ferrosa_graph::engine::GraphEngine::new_with_coordinator(
            schema.clone(),
            storage.clone(),
            graph_write_path,
            graph_config.engine,
            graph_config.reconciliation_interval,
            graph_schema_coordinator,
        ));

        // 10a. Graph HTTP server (bind resolved above: [graph] bind / env / 7474)
        let schema_for_http = schema.clone();
        let state = ferrosa_graph::http::AppState {
            engine: graph_engine.clone(),
            schema: schema_for_http,
            auth_disabled,
        };
        listener_status.mark_up("graph_http");
        let graph_http_status = listener_status.clone();
        runtimes.background.spawn(async move {
            if let Err(e) = ferrosa_graph::http::start_graph_http(&http_config, state).await {
                tracing::error!(%e, "graph HTTP server failed");
                graph_http_status.mark_failed("graph_http", &e);
            }
        });

        // 10b. Bolt server — `[graph] bolt_port` uses the same configured host
        // as graph HTTP, so one explicit graph bind controls both listeners.
        let bolt_config = match resolve_graph_bolt_config(
            &file_config,
            graph_config.http.bind_addr,
            auth_disabled,
            &listener_tls_inputs.graph,
        ) {
            Ok(config) => config,
            Err(error) => {
                eprintln!(
                    "FATAL: Bolt listener TLS: {error}\n\
                     Set [graph] tls_cert and tls_key (or FERROSA_GRAPH_TLS_CERT / \
                     FERROSA_GRAPH_TLS_KEY) to readable PEM files."
                );
                std::process::exit(1);
            }
        };
        tracing::info!(
            graph_http_bind = %graph_config.http.bind_addr,
            bolt_bind = %bolt_config.bind_addr,
            "graph listener binds configured"
        );
        let bolt_bind = bolt_config.bind_addr;
        let bolt_engine = graph_engine;
        let bolt_schema = schema.clone();
        let bolt_shutdown = shutdown_rx.clone();
        listener_status.mark_up("bolt");
        let bolt_status = listener_status.clone();
        runtimes.background.spawn(async move {
            if let Err(e) = ferrosa_graph::bolt::server::start_bolt_server(
                bolt_engine,
                bolt_schema,
                bolt_config,
                bolt_shutdown,
            )
            .await
            {
                tracing::error!(%e, "Bolt server failed");
                bolt_status.mark_failed("bolt", &e);
            }
        });
        tracing::info!(%bolt_bind, "Bolt server starting");
    } else {
        // t_2dd438d2: the graph engine is disabled, but instead of leaving the
        // graph HTTP port unbound (clients get an opaque connection-refused, or
        // a misleading missing-table error), serve a thin endpoint that returns
        // a clear "graph engine disabled" error + remediation on every request.
        tracing::info!("graph engine disabled (set FERROSA_GRAPH_ENABLED=true to enable)");
        listener_status.mark_up("graph_http");
        let graph_stub_status = listener_status.clone();
        runtimes.background.spawn(async move {
            if let Err(e) = ferrosa_graph::http::start_graph_disabled_http(&graph_http_config).await
            {
                tracing::error!(%e, "graph disabled-engine HTTP stub failed");
                graph_stub_status.mark_failed("graph_http", &e);
            }
        });
    }

    // 11. SPARQL endpoint — on by default (t_acc3c7fd); env wins over TOML.
    let sparql_enabled = listener_tls_inputs.sparql_enabled;

    if sparql_enabled {
        let sparql_bind = resolve_sparql_bind(&file_config);

        let sparql_write_path = std::sync::Arc::new(
            ferrosa_cluster::write_path::WritePath::direct(storage.clone()),
        );
        let sparql_engine = std::sync::Arc::new(ferrosa_sparql::engine::SparqlEngine::new(
            storage.clone(),
            sparql_write_path.clone(),
            ferrosa_sparql::engine::SparqlConfig::default(),
        ));

        let sparql_state = ferrosa_sparql::http::AppState {
            engine: sparql_engine,
            schema: schema.clone(),
            auth_disabled,
        };

        let sparql_config = ferrosa_sparql::http::SparqlHttpConfig {
            bind_addr: sparql_bind,
            tls_cert_path: listener_tls_inputs.sparql.cert.clone(),
            tls_key_path: listener_tls_inputs.sparql.key.clone(),
            require_tls: listener_tls_inputs.sparql.require_tls,
        };

        listener_status.mark_up("sparql");
        let sparql_status = listener_status.clone();
        runtimes.background.spawn(async move {
            if let Err(e) =
                ferrosa_sparql::http::start_sparql_http(&sparql_config, sparql_state).await
            {
                tracing::error!(%e, "SPARQL HTTP server failed");
                sparql_status.mark_failed("sparql", &e);
            }
        });

        tracing::info!(%sparql_bind, "SPARQL server starting");
    } else {
        tracing::info!("SPARQL server disabled (set FERROSA_SPARQL_ENABLED=true to enable)");
    }

    // 11b. Postgres wire-protocol listener (port 5432). Authenticates real roles
    // via the shared schema's SCRAM verifiers (D4) through the shared
    // failed-login limiter, authorizes every statement with the CQL permission
    // model, and negotiates TLS from `[postgres] tls_cert/tls_key/require_tls`
    // (t_e1c819ad).
    {
        let pg_bind = resolve_postgres_bind(&file_config);
        let pg_tls_config = &listener_tls_inputs.postgres;
        let pg_tls = match ferrosa_postgres::PgTls::from_pem(
            pg_tls_config.cert.as_deref(),
            pg_tls_config.key.as_deref(),
            pg_tls_config.require_tls,
        ) {
            Ok(tls) => tls,
            Err(error) => {
                eprintln!(
                    "FATAL: PostgreSQL listener TLS: {error}\n\
                     Set [postgres] tls_cert and tls_key (or FERROSA_POSTGRES_TLS_CERT / \
                     FERROSA_POSTGRES_TLS_KEY) to readable PEM files."
                );
                std::process::exit(1);
            }
        };
        let pg_store =
            std::sync::Arc::new(ferrosa_postgres::SchemaVerifierStore::new(schema.clone()));
        // Resolve the committer PER STATEMENT, not once here. This listener is
        // bound at step 11b; seed connection (and therefore formation) begins at
        // step 12, so at this moment a node that is about to become a Raft
        // cluster is still a singleton. A committer captured here would be
        // `None` for it forever, silently disabling Accord ordering for every
        // PostgreSQL transaction on a real cluster.
        let accord_core = shared_state.core.clone();
        let query_ctx = std::sync::Arc::new(ferrosa_postgres::QueryContext {
            engine: storage.clone(),
            schema: schema.clone(),
            default_schema: "public".into(),
            mvcc: std::sync::Arc::new(ferrosa_postgres::MvccManager::from_env()),
            accord: ferrosa_postgres::AccordAccess::live(
                move || accord_core.accord_transaction_committer(),
                {
                    let accord_core = shared_state.core.clone();
                    move |observer| {
                        accord_core
                            .register_postgres_mvcc_observer(observer)
                            .map_err(|error| error.to_string())
                    }
                },
            ),
            // The SAME swappable DDL path the CQL router uses (T-132a): PG
            // `CREATE TABLE` is Raft-replicated in cluster mode like CQL DDL.
            ddl: Some(std::sync::Arc::new(ferrosa_postgres::ClusterDdl::new(
                shared_state.ddl_path.clone(),
            ))),
            // The jsonb limits resolved at startup (`[jsonb]` TOML, then env, then
            // the compiled defaults, ceilings enforced). No default is applied
            // here: the PG front end gets exactly what startup resolved.
            jsonb_limits,
            portals: {
                let limits = resolve_postgres_portal_limits(&file_config);
                tracing::info!(?limits, "PostgreSQL suspended-portal limits");
                std::sync::Arc::new(ferrosa_postgres::SuspendedPortals::new(limits))
            },
        });
        let pg_status = listener_status.clone();
        runtimes.background.spawn(async move {
            match tokio::net::TcpListener::bind(pg_bind).await {
                Ok(listener) => {
                    pg_status.mark_up("postgres");
                    tracing::info!(%pg_bind, "Postgres server listening");
                    if let Err(e) =
                        ferrosa_postgres::server::serve(listener, pg_store, query_ctx, pg_tls).await
                    {
                        tracing::error!(%e, "Postgres server failed");
                        pg_status.mark_failed("postgres", &e);
                    }
                }
                Err(e) => {
                    tracing::error!(%e, %pg_bind, "Postgres bind failed");
                    pg_status.mark_failed("postgres", format_args!("bind {pg_bind} failed: {e}"));
                }
            }
        });
    }

    // 12. Background: connect to seeds with exponential backoff
    // Seeds can be hostnames (e.g., "node2:7000") which SocketAddr can't parse.
    // Resolve via DNS in the background task.
    // Seed list: `[internode] seed` in the config file wins over `FERROSA_SEED`
    // (TOML-wins precedence), so a committed node config can join a cluster
    // with no env wiring. Both accept a comma-separated list; entries may be
    // hostnames (resolved via DNS in the background task below).
    let seed_strs: Vec<String> = parse_seed_list(&config_val(
        "FERROSA_SEED",
        &file_config,
        "internode",
        "seed",
        "",
    ));

    if !seed_strs.is_empty() {
        let net_cfg = net_config.clone();
        let pm = peer_manager.clone();
        let raft_runtime = runtimes.raft.clone();
        let data_runtime = runtimes.data.clone();
        runtimes.background.spawn(async move {
            let mut delay = std::time::Duration::from_millis(500);
            let max_delay = std::time::Duration::from_secs(10);

            // Track which seeds have successfully connected so we don't reconnect
            // to already-connected peers on every retry cycle. Reconnecting to a
            // live peer creates a new pool, shuts down the old pool's lane actors,
            // and leaves stale TCP connections on the peer until it sends FIN.
            // When another seed is down and the loop retries every 10s, this
            // churns the live peer's connections until it hits max_connections
            // and starts rejecting — "max connections reached". The lane actor's
            // alive watcher handles reconnection if a live seed drops, so the
            // seed loop only needs to handle initial connection.
            let mut connected_seeds: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            'outer: loop {
                tokio::time::sleep(delay).await;

                let pending = seeds_to_connect(&seed_strs, &connected_seeds);
                if pending.is_empty() {
                    // All seeds connected.
                    break 'outer;
                }
                let mut all_connected = true;
                for seed in &pending {
                    // PriorityPool::connect resolves the hostname internally and
                    // stores the original string for DNS re-resolution on reconnect.
                    match ferrosa_net::pool::PriorityPool::connect(
                        net_cfg.clone(),
                        host_id,
                        seed.as_str(),
                        Some(raft_runtime.clone()),
                        Some(data_runtime.clone()),
                    )
                    .await
                    {
                        Ok(pool) => {
                            let peer_host_id = pool.peer_host_id();
                            let seed_addr = pool.resolved_addr();
                            pm.add_peer((peer_host_id, seed_addr), pool).await;
                            tracing::info!(%seed, %peer_host_id, "seed connected");
                            connected_seeds.insert(seed.clone());
                            continue; // This seed is done
                        }
                        Err(e) => {
                            tracing::debug!(%seed, %e, "seed connection attempt failed, will retry");
                            all_connected = false;
                        }
                    }
                }

                if all_connected {
                    tracing::info!("all seeds connected");
                    break 'outer;
                }

                delay = std::cmp::min(delay * 2, max_delay);
            }
        });
    }

    // 12. Background maintenance loop: periodic flush, compaction polling, commit log GC,
    //     and S3 schema persistence.
    let flush_interval_secs: u64 = config_val(
        "FERROSA_FLUSH_INTERVAL_SECS",
        &file_config,
        "storage",
        "flush_interval_secs",
        "30",
    )
    .parse()
    .unwrap_or(30);
    let urgent_s3_sync_interval_secs: u64 = config_val(
        "FERROSA_URGENT_S3_SYNC_INTERVAL_SECS",
        &file_config,
        "storage",
        "urgent_s3_sync_interval_secs",
        "1",
    )
    .parse()
    .unwrap_or(1);
    let urgent_flush_interval_millis: u64 = config_val(
        "FERROSA_URGENT_FLUSH_INTERVAL_MILLIS",
        &file_config,
        "storage",
        "urgent_flush_interval_millis",
        "100",
    )
    .parse()
    .unwrap_or(100);
    // Supervised (t_7681b32b): the loop is restarted if it panics or returns,
    // each flush runs as a supervised attempt, and past the restart intensity
    // the process syncs the commit log and aborts — see `supervisor`.
    let maintenance_intensity = supervisor::RestartIntensity::from_env();
    let flush_stall_deadline = std::time::Duration::from_secs(
        supervisor::env_or(
            "FERROSA_FLUSH_STALL_DEADLINE_SECS",
            supervisor::DEFAULT_FLUSH_STALL_DEADLINE.as_secs(),
        )
        .max(1),
    );
    let maintenance_heartbeat = Arc::new(supervisor::Heartbeat::new());
    let maintenance_context = maintenance::MaintenanceContext {
        engine: storage.clone(),
        schema: schema.clone(),
        data_dir: data_dir.clone(),
        intervals: maintenance::MaintenanceIntervals {
            flush: std::time::Duration::from_secs(flush_interval_secs),
            urgent_flush: std::time::Duration::from_millis(urgent_flush_interval_millis.max(1)),
            urgent_s3_sync: std::time::Duration::from_secs(urgent_s3_sync_interval_secs.max(1)),
        },
        // Seed from the version the registry ALREADY holds so the loop's
        // immediate first tick is a no-op. See `maintenance_last_schema_version`.
        last_persisted_schema: Arc::new(arc_swap::ArcSwap::from_pointee(
            maintenance_last_schema_version(schema.snapshot().version),
        )),
        supervision: supervision_status.clone(),
        intensity: maintenance_intensity,
        flush_stall_deadline,
        escalation: Arc::new(supervisor::EscalationPolicy::AbortProcess {
            engine: storage.clone(),
        }),
        heartbeat: maintenance_heartbeat.clone(),
        flush_window: supervisor::IntensityWindow::new(maintenance_intensity),
    };
    // The supervisor never returns: it restarts the loop or aborts the process.
    runtimes
        .background
        .spawn(maintenance::run_supervised(maintenance_context));

    // A hang inside the loop (an await that never completes, a blocked GC)
    // is invisible to `supervise`, which only sees panics and returns. The
    // watchdog reads the loop's heartbeat from its own thread.
    let maintenance_stall_deadline = std::time::Duration::from_secs(
        supervisor::env_or(
            "FERROSA_MAINTENANCE_STALL_DEADLINE_SECS",
            supervisor::default_maintenance_stall_deadline(flush_stall_deadline).as_secs(),
        )
        .max(1),
    );
    supervisor::spawn_maintenance_watchdog(supervisor::MaintenanceWatchdog::new(
        maintenance_heartbeat,
        maintenance_stall_deadline,
        supervision_status.clone(),
        maintenance_intensity,
        Arc::new(supervisor::EscalationPolicy::AbortProcess {
            engine: storage.clone(),
        }),
    ))?;

    // P0-6 (t_88479cda): supervise the commit log's fsync thread. The commit
    // log refuses writes on its own while the thread is dead or behind; this
    // restarts it, reports it on /readyz and the metrics, and escalates past
    // the intensity. The watcher runs under `supervise` so a panic in the
    // watcher itself is a counted, restarted failure, not a silent end.
    {
        let storage = storage.clone();
        let status = supervision_status.clone();
        let intensity = supervisor::RestartIntensity::from_env();
        let escalation = Arc::new(supervisor::EscalationPolicy::AbortProcess {
            engine: storage.clone(),
        });
        runtimes.background.spawn(supervisor::supervise(
            supervisor::Child::CommitLogSync,
            status.clone(),
            intensity,
            escalation.clone(),
            move || {
                supervisor::run_commit_log_sync_supervisor(
                    supervisor::CommitLogSyncSupervisor::new(
                        storage.clone(),
                        status.clone(),
                        intensity,
                        escalation.clone(),
                    ),
                    supervisor::COMMIT_LOG_SYNC_POLL,
                )
            },
        ));
    }

    // 13. Wait for shutdown signal (SIGINT or SIGTERM)
    //
    // Docker/Podman sends SIGTERM on `stop`. Without this, the process
    // only handles Ctrl-C (SIGINT) and gets killed after the stop timeout
    // WITHOUT flushing memtables — causing 100% data loss on restart.
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    // 14. Graceful shutdown: flush memtables, sync to S3, stop compaction
    tracing::info!("shutdown signal received, draining...");

    // Signal Bolt server (and any other watch-based services) to stop.
    let _ = shutdown_tx.send(true);

    let shutdown_timeout = std::time::Duration::from_secs(30);
    match tokio::time::timeout(shutdown_timeout, async {
        // Cancel cluster background tasks (Raft init, schema sync, joins).
        mode_controller.shutdown().await;

        // Drain internode connections before flushing memtables so peers stop
        // sending mutations while we're in the middle of a flush.
        tracing::info!("draining internode connections...");
        rpc_server
            .shutdown(std::time::Duration::from_secs(10))
            .await;
        tracing::info!("internode drained");

        // Flush all memtables to SSTables on disk.
        storage.shutdown()?;
        tracing::info!("memtables flushed");

        // Persist schema locally for restart recovery.
        persist_schema_locally(Path::new(&data_dir), &schema)?;
        tracing::info!("shutdown: schema snapshot persisted locally");

        // Sync any new SSTables to S3 and persist schema there too.
        if storage.has_s3() {
            match storage.sync_sstables_to_s3().await {
                Ok(n) => tracing::info!(count = n, "shutdown: synced SSTables to S3"),
                Err(e) => tracing::warn!(%e, "shutdown: S3 SSTable sync failed"),
            }
            persist_schema_to_s3(&storage, &schema).await;
            tracing::info!("shutdown: schema snapshot persisted to S3");
        }

        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    {
        Ok(Ok(())) => tracing::info!("clean shutdown"),
        Ok(Err(e)) => tracing::error!(%e, "shutdown error"),
        Err(_) => tracing::error!("shutdown timed out after 30s"),
    }

    tracing::info!("ferrosa stopped");

    Ok(())
}

/// Return the subset of `seeds` that have not yet been recorded in
/// `connected`, preserving the original order.
///
/// Used by the seed-connect loop to avoid reconnecting to peers that are
/// already connected. Reconnecting to a live peer every retry cycle churns
/// the peer's connection pool: a new pool replaces the old one, the old
/// pool's lane actors shut down, and stale TCP connections accumulate on
/// the peer until it hits `max_connections` and starts rejecting inbound
/// connections — "max connections reached". The lane actor's alive watcher
/// handles reconnection if a live seed drops, so the seed loop only needs
/// to drive the *initial* connection for each seed.
/// Split a seed setting into individual `host:port` entries.
///
/// Both `[internode] seed` and `FERROSA_SEED` accept a comma-separated list.
/// That matters more than it looks: a node with a single seed only ever learns
/// about the peers that seed tells it about, so in a hub-and-spoke layout it
/// can never satisfy a promotion rule counting local connections. Giving every
/// node the full list is what makes the mesh reachable without depending on
/// invite delivery.
///
/// Shared with the tests deliberately. The test for this used to re-implement
/// the split inline, so it verified a copy of the logic rather than the logic,
/// and a change here could not fail it.
fn parse_seed_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn seeds_to_connect(
    seeds: &[String],
    connected: &std::collections::HashSet<String>,
) -> Vec<String> {
    seeds
        .iter()
        .filter(|s| !connected.contains(*s))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// t_396d4c80: `main` spawned two self-heal controllers, one gated on
    /// mere peer liveness. Startup is imperative code with no other seam to
    /// test it through, so this guards the source.
    #[test]
    fn main_spawns_exactly_one_self_heal_controller() {
        let source = include_str!("main.rs");
        let spawns = source
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .filter(|line| line.contains(concat!("SelfHealController", "::spawn")))
            .count();
        assert_eq!(spawns, 1, "main must start one self-heal controller");
    }

    fn udf_toml(v: &str) -> toml::Value {
        format!("[udf]\nmax_memory_bytes = {v}\n").parse().unwrap()
    }

    #[test]
    fn udf_memory_limit_comes_from_toml() {
        let a = resolve_udf_sandbox_config(&udf_toml("4194304")).unwrap();
        let b = resolve_udf_sandbox_config(&udf_toml("33554432")).unwrap();
        assert_eq!(a.max_memory_bytes, 4_194_304);
        assert_eq!(b.max_memory_bytes, 33_554_432);
    }

    #[test]
    fn udf_memory_limit_rejects_zero_absurd_and_garbage() {
        assert!(resolve_udf_sandbox_config(&udf_toml("0")).is_err());
        assert!(resolve_udf_sandbox_config(&udf_toml("99999999999999")).is_err());
        assert!(resolve_udf_sandbox_config(&udf_toml("\"lots\"")).is_err());
    }

    const SEGMENT_32_MIB: u64 = 32 * 1024 * 1024;

    fn jsonb_toml(body: &str) -> toml::Value {
        format!("[jsonb]\n{body}\n").parse().unwrap()
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn jsonb_defaults_are_accepted() {
        let l = resolve_jsonb_limits_with_env(&empty_config(), &no_env, SEGMENT_32_MIB).unwrap();
        assert_eq!(l.max_input_bytes, 10 * 1024 * 1024);
        assert_eq!(l.max_depth, 1000);
    }

    #[test]
    fn jsonb_rejects_input_above_segment_size() {
        let cfg = jsonb_toml("max_input_bytes = 8388608");
        let err = resolve_jsonb_limits_with_env(&cfg, &no_env, 4 * 1024 * 1024).unwrap_err();
        assert!(err.contains("max_input_bytes"), "{err}");
        assert!(err.contains("8388608"), "{err}");
    }

    #[test]
    fn jsonb_rejects_value_above_256_mib_ceiling() {
        let cfg = jsonb_toml("max_input_bytes = 268435457");
        let err = resolve_jsonb_limits_with_env(&cfg, &no_env, 1024 * 1024 * 1024).unwrap_err();
        assert!(err.contains("max_input_bytes"), "{err}");
        assert!(err.contains("268435456"), "{err}");
    }

    #[test]
    fn jsonb_toml_overrides_env() {
        let env = |k: &str| (k == "FERROSA_JSONB_MAX_INPUT_BYTES").then(|| "2097152".to_string());
        let cfg = jsonb_toml("max_input_bytes = 4194304");
        let l = resolve_jsonb_limits_with_env(&cfg, &env, SEGMENT_32_MIB).unwrap();
        assert_eq!(l.max_input_bytes, 4_194_304);
        let l = resolve_jsonb_limits_with_env(&empty_config(), &env, SEGMENT_32_MIB).unwrap();
        assert_eq!(l.max_input_bytes, 2_097_152);
    }

    #[test]
    fn jsonb_maps_every_toml_key() {
        let cfg = jsonb_toml(
            "max_input_bytes = 1000\nmax_encoded_bytes = 2000\nmax_nesting_depth = 30\n\
             max_key_list_length = 40\nmax_index_terms_per_doc = 50\nmax_path_len = 60\n\
             path_step_budget = 70\nduplicate_keys = \"error\"",
        );
        let l = resolve_jsonb_limits_with_env(&cfg, &no_env, SEGMENT_32_MIB).unwrap();
        assert_eq!(
            (
                l.max_input_bytes,
                l.max_encoded_bytes,
                l.max_depth,
                l.max_key_list
            ),
            (1000, 2000, 30, 40)
        );
        assert_eq!(
            (
                l.max_index_terms_per_doc,
                l.max_path_len,
                l.path_step_budget
            ),
            (50, 60, 70)
        );
        assert_eq!(l.duplicate_keys, ferrosa_jsonb::DuplicateKeyPolicy::Error);
    }

    #[test]
    fn jsonb_rejects_bad_types_and_policy() {
        assert!(resolve_jsonb_limits_with_env(
            &jsonb_toml("max_depth_typo = 1"),
            &no_env,
            SEGMENT_32_MIB
        )
        .is_err());
        assert!(resolve_jsonb_limits_with_env(
            &jsonb_toml("max_input_bytes = \"big\""),
            &no_env,
            SEGMENT_32_MIB
        )
        .is_err());
        assert!(resolve_jsonb_limits_with_env(
            &jsonb_toml("max_input_bytes = -1"),
            &no_env,
            SEGMENT_32_MIB
        )
        .is_err());
        assert!(resolve_jsonb_limits_with_env(
            &jsonb_toml("duplicate_keys = \"first\""),
            &no_env,
            SEGMENT_32_MIB
        )
        .is_err());
    }

    fn empty_config() -> toml::Value {
        toml::Value::Table(toml::map::Map::new())
    }

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn tunables_toml() -> toml::Value {
        toml::from_str(
            "[storage]\ncache_max_bytes = 1073741824\ncache_hot_window_secs = 60\n\
             [s3]\nrequest_timeout_secs = 120\n",
        )
        .unwrap()
    }

    #[test]
    fn storage_tunables_from_toml_reads_each_bridged_key() {
        let pairs = storage_tunables_from_toml(&tunables_toml()).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("FERROSA_CACHE_MAX_BYTES", "1073741824".to_string()),
                ("FERROSA_CACHE_HOT_WINDOW_SECS", "60".to_string()),
                ("FERROSA_S3_REQUEST_TIMEOUT_SECS", "120".to_string()),
            ]
        );
        assert!(storage_tunables_from_toml(&empty_config())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn storage_tunables_from_toml_refuses_a_value_that_is_not_a_number() {
        for bad in ["\"lots\"", "-1", "1.5", "true"] {
            let cfg: toml::Value =
                toml::from_str(&format!("[storage]\ncache_max_bytes = {bad}\n")).unwrap();
            let err = storage_tunables_from_toml(&cfg).unwrap_err();
            assert!(err.contains("cache_max_bytes"), "{bad}: {err}");
        }
    }

    /// A TOML value reaches the engine config, and wins over an env value.
    #[test]
    fn toml_storage_tunables_reach_the_engine_config_and_win_over_env() {
        let local_store = tempfile::tempdir().unwrap();
        std::env::set_var("FERROSA_LOCAL_STORE_PATH", local_store.path());
        std::env::set_var("FERROSA_CACHE_MAX_BYTES", "999");
        std::env::remove_var("FERROSA_CACHE_HOT_WINDOW_SECS");
        std::env::remove_var("FERROSA_S3_REQUEST_TIMEOUT_SECS");

        apply_storage_tunables(&tunables_toml()).unwrap();
        let config = ferrosa_storage::StorageEngineConfig::from_env().unwrap();

        std::env::remove_var("FERROSA_LOCAL_STORE_PATH");
        std::env::remove_var("FERROSA_CACHE_MAX_BYTES");
        std::env::remove_var("FERROSA_CACHE_HOT_WINDOW_SECS");
        std::env::remove_var("FERROSA_S3_REQUEST_TIMEOUT_SECS");

        assert_eq!(config.local_cache_max_bytes, 1_073_741_824);
        assert_eq!(config.cache_hot_window_secs, 60);
        let object_store = config.object_store.expect("local store path was set");
        assert_eq!(
            object_store.request_timeout,
            std::time::Duration::from_secs(120)
        );
    }

    #[test]
    fn log_colour_follows_the_terminal_by_default() {
        assert!(log_ansi_enabled(true, None, None), "a person at a terminal");
        assert!(
            !log_ansi_enabled(false, None, None),
            "a pipe (container stdout) must not receive escape codes"
        );
    }

    #[test]
    fn no_color_turns_colour_off_even_on_a_terminal() {
        assert!(!log_ansi_enabled(true, Some("1"), None));
        assert!(!log_ansi_enabled(true, Some("anything"), None));
        assert!(
            log_ansi_enabled(true, Some(""), None),
            "NO_COLOR set but empty does not count (no-color.org)"
        );
    }

    #[test]
    fn ferrosa_log_ansi_overrides_terminal_and_no_color() {
        for on in ["1", "true", "TRUE", "on", "yes"] {
            assert!(log_ansi_enabled(false, Some("1"), Some(on)), "{on:?}");
        }
        for off in ["0", "false", "off", "no"] {
            assert!(!log_ansi_enabled(true, None, Some(off)), "{off:?}");
        }
        // Unrecognised: ignored, the default rules apply.
        assert!(log_ansi_enabled(true, None, Some("maybe")));
        assert!(!log_ansi_enabled(false, None, Some("maybe")));
    }

    /// `--version` previously did NOT print a version: the flag was ignored and
    /// the daemon started. Anything probing this binary for its version — an
    /// installer, an update check — silently launched a database instead.
    #[test]
    fn version_flag_prints_a_single_parseable_line() {
        for flag in ["--version", "-V"] {
            let output = cli_meta_output(args(&[flag]))
                .unwrap_or_else(|| panic!("{flag} must not fall through to starting the server"));
            assert_eq!(output.lines().count(), 1, "{flag} output: {output}");
            assert_eq!(
                output,
                format!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
            );
            // "<name> <semver>" is the shape callers parse.
            let (name, version) = output.split_once(' ').expect("name and version");
            assert_eq!(name, "ferrosa");
            assert_eq!(version.split('.').count(), 3, "semver: {version}");
        }
    }

    #[test]
    fn help_flag_reports_the_version_and_exits() {
        for flag in ["--help", "-h"] {
            let output = cli_meta_output(args(&[flag])).expect("help must be handled");
            assert!(output.starts_with("ferrosa "), "{output}");
            assert!(output.contains("FERROSA_CONFIG"), "{output}");
        }
    }

    /// No meta flag means start the server, and unknown flags must not be
    /// rejected here — existing wrappers pass extra arguments.
    #[test]
    fn other_arguments_still_start_the_server() {
        assert!(cli_meta_output(args(&[])).is_none());
        assert!(cli_meta_output(args(&["--not-a-flag"])).is_none());
        assert!(cli_meta_output(args(&["serve", "--foo=bar"])).is_none());
    }

    /// A meta flag anywhere in the argument list is still honoured.
    #[test]
    fn version_flag_is_found_after_other_arguments() {
        let output = cli_meta_output(args(&["--foo", "--version"])).expect("handled");
        assert!(output.starts_with("ferrosa "), "{output}");
    }

    /// Gap 1: with no auth env vars set and the storage default
    /// (`auth_enabled=false`), the CQL server must NOT advertise an
    /// authenticator — otherwise DataStax-style drivers refuse to connect.
    /// See ferrosa-nosqlbench/docs/initial-gaps-found.md.
    #[test]
    fn config_val_opt_returns_none_when_unset() {
        let key = "FERROSA_TEST_CFG_OPT_e7c1";
        std::env::remove_var(key);
        let result = config_val_opt(key, &empty_config(), "cql", "auth_disabled");
        assert!(result.is_none(), "expected None for unset, got {result:?}");
    }

    #[test]
    fn config_val_opt_reads_env_when_set() {
        let key = "FERROSA_TEST_CFG_OPT_b2f4";
        std::env::set_var(key, "true");
        let result = config_val_opt(key, &empty_config(), "cql", "auth_disabled");
        std::env::remove_var(key);
        assert_eq!(result.as_deref(), Some("true"));
    }

    #[test]
    fn config_val_opt_reads_file_when_env_unset() {
        let key = "FERROSA_TEST_CFG_OPT_f9a3";
        std::env::remove_var(key);
        let cfg: toml::Value = toml::from_str("[cql]\nauth_disabled = true\n").unwrap();
        let result = config_val_opt(key, &cfg, "cql", "auth_disabled");
        assert_eq!(result.as_deref(), Some("true"));
    }

    /// End-to-end of the Gap 1 resolution chain: storage default is
    /// `auth_enabled=false`, no explicit override → resolver returns
    /// `auth_disabled=true` (CQL server sends READY, not AUTHENTICATE).
    #[test]
    fn auth_disabled_default_chain_matches_storage_default() {
        // Storage default for `auth_enabled` is `false` (see
        // StorageEngineConfig::default in ferrosa-storage); no override set.
        let storage_default_auth_enabled = false;
        let override_unset = None;
        let auth_disabled = ferrosa_cql::server::resolve_auth_disabled(
            storage_default_auth_enabled,
            override_unset,
        );
        assert!(
            auth_disabled,
            "with storage auth_enabled=false (default) and no explicit override, \
             CQL server must send READY (auth_disabled=true) so drivers connect"
        );
    }

    fn sample_config() -> toml::Value {
        toml::from_str(
            r#"
            [storage]
            data_dir = "/data/ferrosa"

            [cql]
            bind = "127.0.0.1:19042"
            auth_disabled = true

            [internode]
            cluster_name = "test-cluster"
            "#,
        )
        .unwrap()
    }

    #[test]
    fn config_val_returns_default_when_empty_config_and_no_env() {
        // Use a unique env key to avoid collisions with real env vars.
        let key = "FERROSA_TEST_CFG_EMPTY_5a3b";
        std::env::remove_var(key);

        let result = config_val(
            key,
            &empty_config(),
            "storage",
            "data_dir",
            "/var/lib/ferrosa",
        );
        assert_eq!(result, "/var/lib/ferrosa");
    }

    #[test]
    fn config_val_reads_from_toml_when_no_env() {
        let key = "FERROSA_TEST_CFG_TOML_7d2e";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "storage", "data_dir", "/var/lib/ferrosa");
        assert_eq!(result, "/data/ferrosa");
    }

    #[test]
    fn config_val_toml_overrides_env() {
        // TOML-wins precedence: when both the config file and the env var set a
        // value, the config file is authoritative.
        let key = "FERROSA_TEST_CFG_OVERRIDE_9f1c";
        std::env::set_var(key, "/env/override");
        let config = sample_config(); // [storage] data_dir = "/data/ferrosa"

        let result = config_val(key, &config, "storage", "data_dir", "/var/lib/ferrosa");
        assert_eq!(result, "/data/ferrosa");

        std::env::remove_var(key);
    }

    #[test]
    fn config_val_falls_back_to_env_when_toml_absent() {
        // Env var is the fallback when the config file does not set the key.
        let key = "FERROSA_TEST_CFG_ENV_FALLBACK_3b7d";
        std::env::set_var(key, "/env/only");

        let result = config_val(
            key,
            &empty_config(),
            "storage",
            "data_dir",
            "/var/lib/ferrosa",
        );
        assert_eq!(result, "/env/only");

        std::env::remove_var(key);
    }

    #[test]
    fn config_val_opt_toml_overrides_env() {
        let key = "FERROSA_TEST_CFG_OPT_OVERRIDE_1a2b";
        std::env::set_var(key, "false");
        let cfg: toml::Value = toml::from_str("[cql]\nauth_disabled = true\n").unwrap();
        let result = config_val_opt(key, &cfg, "cql", "auth_disabled");
        std::env::remove_var(key);
        assert_eq!(
            result.as_deref(),
            Some("true"),
            "config file must win over env"
        );
    }

    // ---- resolve_cql_positive_usize: the CQL server limits --------------------
    // `max_connections`, `max_connections_per_ip` and (newly exposed)
    // `max_in_flight_per_connection` all resolve through this, so a bad value
    // must be loud rather than silently clamped to the default.

    #[test]
    fn cql_limit_returns_default_when_neither_env_nor_toml_set_it() {
        let key = "FERROSA_TEST_CQL_LIMIT_UNSET_4c1a";
        std::env::remove_var(key);
        let got =
            resolve_cql_positive_usize(key, &empty_config(), "max_in_flight_per_connection", 128);
        assert_eq!(got, Ok(128));
    }

    #[test]
    fn cql_limit_reads_toml_preferentially_over_env() {
        // TOML-wins: the config file is authoritative, matching config_val.
        let key = "FERROSA_TEST_CQL_LIMIT_TOML_BEATS_ENV_8f2d";
        std::env::set_var(key, "256");
        let cfg: toml::Value =
            toml::from_str("[cql]\nmax_in_flight_per_connection = 512\n").unwrap();
        let got = resolve_cql_positive_usize(key, &cfg, "max_in_flight_per_connection", 128);
        std::env::remove_var(key);
        assert_eq!(got, Ok(512), "TOML must win over env");
    }

    #[test]
    fn cql_limit_falls_back_to_env_when_toml_absent() {
        let key = "FERROSA_TEST_CQL_LIMIT_ENV_ONLY_2d9e";
        std::env::set_var(key, "300");
        let got =
            resolve_cql_positive_usize(key, &empty_config(), "max_in_flight_per_connection", 128);
        std::env::remove_var(key);
        assert_eq!(got, Ok(300));
    }

    #[test]
    fn cql_limit_rejects_zero_rather_than_clamping() {
        // A zero in-flight limit would shed every request. Fail loud.
        let key = "FERROSA_TEST_CQL_LIMIT_ZERO_6b3f";
        std::env::remove_var(key);
        let cfg: toml::Value = toml::from_str("[cql]\nmax_in_flight_per_connection = 0\n").unwrap();
        let got = resolve_cql_positive_usize(key, &cfg, "max_in_flight_per_connection", 128);
        assert!(
            got.is_err(),
            "zero must be rejected, not clamped to the default"
        );
    }

    #[test]
    fn cql_limit_rejects_non_numeric_rather_than_clamping() {
        let key = "FERROSA_TEST_CQL_LIMIT_GARBAGE_9a7c";
        std::env::remove_var(key);
        let cfg: toml::Value =
            toml::from_str("[cql]\nmax_in_flight_per_connection = \"lots\"\n").unwrap();
        let got = resolve_cql_positive_usize(key, &cfg, "max_in_flight_per_connection", 128);
        assert!(
            got.is_err(),
            "a non-numeric limit must fail startup, not silently default"
        );
    }

    #[test]
    fn cql_limit_treats_an_empty_env_var_as_unset() {
        // `fly machine update --env KEY=` is the ONLY way to clear a variable, and
        // sweeping tunables clears them exactly that way. Treating the resulting
        // empty string as garbage would make the knob impossible to remove once
        // set, and would crash every node on the next sweep.
        let key = "FERROSA_TEST_CQL_LIMIT_EMPTY_5e4b";
        std::env::set_var(key, "");
        let got =
            resolve_cql_positive_usize(key, &empty_config(), "max_in_flight_per_connection", 128);
        std::env::remove_var(key);
        assert_eq!(
            got,
            Ok(128),
            "an empty env var means unset and must fall back to the default"
        );
    }

    #[test]
    fn cql_limit_toml_survives_an_empty_env_var() {
        let key = "FERROSA_TEST_CQL_LIMIT_EMPTY_TOML_7c2d";
        std::env::set_var(key, "");
        let cfg: toml::Value =
            toml::from_str("[cql]\nmax_in_flight_per_connection = 512\n").unwrap();
        let got = resolve_cql_positive_usize(key, &cfg, "max_in_flight_per_connection", 128);
        std::env::remove_var(key);
        assert_eq!(got, Ok(512), "TOML must still win when the env var is empty");
    }

    #[test]
    fn config_val_handles_boolean_values() {
        let key = "FERROSA_TEST_CFG_BOOL_4c8a";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "cql", "auth_disabled", "false");
        assert_eq!(result, "true");
    }

    // ---- BUG-008 -----------------------------------------------------

    /// Valid UUID on disk: load and return.
    #[test]
    fn classify_host_id_state_loads_valid_disk_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("host_id");
        let id = Uuid::new_v4();
        std::fs::write(&path, id.to_string()).unwrap();
        assert_eq!(
            classify_host_id_state(&path, None),
            HostIdResolution::LoadedFromDisk(id)
        );
    }

    /// Missing file: generate new.
    #[test]
    fn classify_host_id_state_generates_when_file_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("host_id");
        match classify_host_id_state(&path, None) {
            HostIdResolution::GeneratedNew(_) => {}
            other => panic!("expected GeneratedNew, got {other:?}"),
        }
    }

    /// Empty file: classify as EmptyFileRegenerated with the path.
    #[test]
    fn classify_host_id_state_handles_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("host_id");
        std::fs::write(&path, "").unwrap();
        match classify_host_id_state(&path, None) {
            HostIdResolution::EmptyFileRegenerated { path: p, new_id: _ } => {
                assert_eq!(p, path);
            }
            other => panic!("expected EmptyFileRegenerated, got {other:?}"),
        }
    }

    /// Unparseable contents: classify as InvalidFileRegenerated, preserving
    /// the bad content so the diagnostic can name what was on disk.
    #[test]
    fn classify_host_id_state_handles_garbage_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("host_id");
        std::fs::write(&path, "not-a-uuid-at-all").unwrap();
        match classify_host_id_state(&path, None) {
            HostIdResolution::InvalidFileRegenerated {
                path: p,
                bad_content,
                new_id: _,
            } => {
                assert_eq!(p, path);
                assert_eq!(bad_content, "not-a-uuid-at-all");
            }
            other => panic!("expected InvalidFileRegenerated, got {other:?}"),
        }
    }

    /// Env override beats disk — operator-supplied id wins.
    #[test]
    fn classify_host_id_state_override_wins_over_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("host_id");
        let on_disk = Uuid::new_v4();
        let override_id = Uuid::new_v4();
        std::fs::write(&path, on_disk.to_string()).unwrap();
        assert_eq!(
            classify_host_id_state(&path, Some(&override_id.to_string())),
            HostIdResolution::UsingOverride(override_id)
        );
    }

    /// Invalid override falls through to disk read.
    #[test]
    fn classify_host_id_state_invalid_override_falls_through() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("host_id");
        let on_disk = Uuid::new_v4();
        std::fs::write(&path, on_disk.to_string()).unwrap();
        assert_eq!(
            classify_host_id_state(&path, Some("not-a-uuid")),
            HostIdResolution::LoadedFromDisk(on_disk)
        );
    }

    /// load_or_generate_host_id_with rewrites a corrupt file with a fresh
    /// UUID — the regenerated file must be valid on second load.
    #[test]
    fn load_or_generate_host_id_with_recovers_from_corrupt_file() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("host_id"), "not-a-uuid").unwrap();
        let first = load_or_generate_host_id_with(tmp.path(), None);
        // Second call should now load the persisted UUID, not re-roll.
        let second = load_or_generate_host_id_with(tmp.path(), None);
        assert_eq!(first, second, "regenerated UUID was not persisted");
    }

    // ---- internode config resolution (TOML-wins) ----------------------

    /// Internode bind from TOML is honored when the env var is unset.
    #[test]
    fn apply_internode_toml_overrides_sets_bind_when_env_unset() {
        let mut cfg = ferrosa_net::config::NetConfig::default();
        let toml: toml::Value =
            toml::from_str("[internode]\nbind = \"127.0.0.1:18001\"\n").unwrap();
        apply_internode_toml_overrides(&mut cfg, &toml).unwrap();
        assert_eq!(cfg.bind_addr, "127.0.0.1:18001".parse().unwrap());
    }

    /// TOML beats env: the base `NetConfig::from_env()` seeds the bind, then the
    /// config-file value overwrites it (TOML-wins precedence). Modeled here by
    /// pre-seeding the field to the "env" value and asserting TOML replaces it.
    #[test]
    fn apply_internode_toml_overrides_toml_wins_over_env() {
        // `bind_addr` stands in for the env-derived base value.
        let mut cfg = ferrosa_net::config::NetConfig {
            bind_addr: "127.0.0.1:19999".parse().unwrap(),
            ..ferrosa_net::config::NetConfig::default()
        };
        let toml: toml::Value =
            toml::from_str("[internode]\nbind = \"127.0.0.1:18002\"\n").unwrap();
        apply_internode_toml_overrides(&mut cfg, &toml).unwrap();
        assert_eq!(cfg.bind_addr, "127.0.0.1:18002".parse().unwrap());
    }

    /// Cluster name + broadcast + psk also flow from TOML.
    #[test]
    fn apply_internode_toml_overrides_sets_other_fields() {
        let mut cfg = ferrosa_net::config::NetConfig::default();
        let toml: toml::Value = toml::from_str(
            r#"
            [internode]
            bind         = "127.0.0.1:18003"
            broadcast    = "10.0.0.1:18003"
            cluster_name = "team-cluster"
            psk          = "abc"
            "#,
        )
        .unwrap();
        apply_internode_toml_overrides(&mut cfg, &toml).unwrap();
        assert_eq!(cfg.bind_addr, "127.0.0.1:18003".parse().unwrap());
        assert_eq!(cfg.broadcast_addr, "10.0.0.1:18003".parse().unwrap());
        assert_eq!(cfg.internode_broadcast.as_deref(), Some("10.0.0.1:18003"));
        assert_eq!(cfg.cluster_name, "team-cluster");
        assert_eq!(cfg.psk.as_deref(), Some("abc"));
    }

    /// Invalid bind format in TOML must NOT panic — log and move on.
    #[test]
    fn apply_internode_toml_overrides_invalid_bind_is_ignored() {
        let mut cfg = ferrosa_net::config::NetConfig::default();
        let default_bind = cfg.bind_addr;
        let toml: toml::Value = toml::from_str("[internode]\nbind = \"not-an-addr\"\n").unwrap();
        apply_internode_toml_overrides(&mut cfg, &toml).unwrap();
        assert_eq!(cfg.bind_addr, default_bind);
    }

    /// A seed *list* in TOML must parse to every entry.
    ///
    /// Only the env path had list coverage, and the config file is the path
    /// that ships: `~/.ferrosa/config/ferrosa-nodeN.toml`. The native cluster
    /// gave node1 and node2 a single seed (node3) and node3 none, so node1 and
    /// node2 could each only ever see one peer and never satisfied the
    /// two-peer promotion condition -- they reached Cluster only because node3
    /// pushed them an invite, and when one of those invites failed to deliver
    /// on 2026-08-20 node1 sat in Pair mode indefinitely.
    ///
    /// Giving every node the full list removes that dependency, and this test
    /// is what stops the list being silently parsed as one malformed address.
    #[test]
    #[serial_test::serial(env)]
    fn a_seed_list_in_toml_parses_to_every_entry() {
        std::env::remove_var("FERROSA_SEED");

        let cfg: toml::Value = toml::from_str(
            "[internode]\nseed = \"127.0.0.1:17000, 127.0.0.1:17001,127.0.0.1:17002\"\n",
        )
        .unwrap();

        assert_eq!(
            parse_seed_list(&config_val("FERROSA_SEED", &cfg, "internode", "seed", "")),
            vec![
                "127.0.0.1:17000".to_string(),
                "127.0.0.1:17001".to_string(),
                "127.0.0.1:17002".to_string(),
            ],
            "a comma-separated seed list in the config file must yield one \
             entry per peer, with surrounding whitespace trimmed"
        );
    }

    /// Seed list resolves from `[internode] seed` in the config file (so a
    /// committed node config can join a cluster with no env wiring), with
    /// `FERROSA_SEED` as the fallback and TOML winning when both are set. This
    /// mirrors the resolution in `main` (`config_val` + comma-split).
    #[test]
    #[serial_test::serial(env)]
    fn seed_resolves_from_toml_over_env() {
        let seeds = parse_seed_list;

        std::env::remove_var("FERROSA_SEED");

        // Config file provides the seed with no env set.
        let cfg: toml::Value = toml::from_str("[internode]\nseed = \"127.0.0.1:17000\"\n").unwrap();
        assert_eq!(
            seeds(&config_val("FERROSA_SEED", &cfg, "internode", "seed", "")),
            vec!["127.0.0.1:17000".to_string()]
        );

        // Env var is the fallback when TOML is silent (comma-separated list).
        std::env::set_var("FERROSA_SEED", "a:1, b:2");
        assert_eq!(
            seeds(&config_val(
                "FERROSA_SEED",
                &empty_config(),
                "internode",
                "seed",
                ""
            )),
            vec!["a:1".to_string(), "b:2".to_string()]
        );

        // TOML wins when both are set.
        assert_eq!(
            seeds(&config_val("FERROSA_SEED", &cfg, "internode", "seed", "")),
            vec!["127.0.0.1:17000".to_string()]
        );

        // Neither set → empty seed list (single-node bootstrap).
        std::env::remove_var("FERROSA_SEED");
        assert!(seeds(&config_val(
            "FERROSA_SEED",
            &empty_config(),
            "internode",
            "seed",
            ""
        ))
        .is_empty());
    }

    /// Graph enable via TOML when env unset.
    #[test]
    fn resolve_graph_enabled_reads_toml_when_env_unset() {
        let toml: toml::Value = toml::from_str("[graph]\nenabled = true\n").unwrap();
        assert!(resolve_graph_enabled(&toml, |_| None));
        let toml: toml::Value = toml::from_str("[graph]\nenabled = false\n").unwrap();
        assert!(!resolve_graph_enabled(&toml, |_| None));
    }

    #[test]
    fn resolve_graph_enabled_toml_wins_over_env() {
        // TOML says off, env says on → TOML wins (off).
        let toml: toml::Value = toml::from_str("[graph]\nenabled = false\n").unwrap();
        assert!(!resolve_graph_enabled(&toml, |k| {
            (k == "FERROSA_GRAPH_ENABLED").then(|| "true".into())
        }));
    }

    #[test]
    fn resolve_graph_enabled_falls_back_to_env_when_toml_absent() {
        // No TOML key → env is the fallback.
        let toml = empty_config();
        assert!(resolve_graph_enabled(&toml, |k| {
            (k == "FERROSA_GRAPH_ENABLED").then(|| "true".into())
        }));
        assert!(!resolve_graph_enabled(&toml, |k| {
            (k == "FERROSA_GRAPH_ENABLED").then(|| "false".into())
        }));
    }

    #[test]
    fn resolve_graph_enabled_defaults_on_when_unset_everywhere() {
        // t_acc3c7fd: a fresh install (no env, no TOML) exposes the graph engine.
        let toml = empty_config();
        assert!(resolve_graph_enabled(&toml, |_| None));
    }

    #[test]
    fn resolve_graph_enabled_explicit_opt_out_is_honored() {
        let toml: toml::Value = toml::from_str("[graph]\nenabled = false\n").unwrap();
        assert!(!resolve_graph_enabled(&toml, |_| None));
        // env opt-out wins too.
        let toml = empty_config();
        assert!(!resolve_graph_enabled(&toml, |k| (k
            == "FERROSA_GRAPH_ENABLED")
            .then(|| "false".into())));
    }

    #[test]
    fn resolve_sparql_enabled_defaults_on_with_explicit_opt_out() {
        let toml = empty_config();
        assert!(resolve_sparql_enabled(&toml, |_| None));
        let toml: toml::Value = toml::from_str("[sparql]\nenabled = false\n").unwrap();
        assert!(!resolve_sparql_enabled(&toml, |_| None));
        let toml = empty_config();
        assert!(!resolve_sparql_enabled(&toml, |k| (k
            == "FERROSA_SPARQL_ENABLED")
            .then(|| "false".into())));
    }

    /// Auth enabled via TOML.
    #[test]
    fn resolve_auth_enabled_toml_reads_file_when_env_unset() {
        let toml: toml::Value = toml::from_str("[cql]\nauth_enabled = true\n").unwrap();
        assert_eq!(resolve_auth_enabled_toml(&toml, |_| None), Some(true));
        let toml: toml::Value = toml::from_str("[cql]\nauth_enabled = false\n").unwrap();
        assert_eq!(resolve_auth_enabled_toml(&toml, |_| None), Some(false));
    }

    #[test]
    fn resolve_auth_enabled_toml_wins_over_env() {
        // TOML says false, env says true → TOML wins (false).
        let toml: toml::Value = toml::from_str("[cql]\nauth_enabled = false\n").unwrap();
        assert_eq!(
            resolve_auth_enabled_toml(&toml, |k| (k == "FERROSA_AUTH_ENABLED")
                .then(|| "true".into())),
            Some(false)
        );
    }

    #[test]
    fn resolve_auth_enabled_toml_falls_back_to_env_when_toml_absent() {
        // No TOML key → env is the fallback.
        let toml = empty_config();
        assert_eq!(
            resolve_auth_enabled_toml(&toml, |k| (k == "FERROSA_AUTH_ENABLED")
                .then(|| "true".into())),
            Some(true)
        );
    }

    #[test]
    fn resolve_auth_enabled_toml_returns_none_when_unspecified() {
        let toml = empty_config();
        assert_eq!(resolve_auth_enabled_toml(&toml, |_| None), None);
    }

    /// Regression (issue #172): the startup auth log used to always report
    /// `source="default"`, hiding the fact that `[cql].auth_enabled` in the
    /// config file was in force. Under TOML-wins precedence, `auth_source_label`
    /// must attribute a config-file value to the config file (even when the env
    /// var is also set), fall back to env when only the env var is set, and only
    /// report "default" when neither is set.
    #[test]
    fn auth_source_label_attributes_config_file_not_default() {
        // [cql].auth_enabled present in TOML -> config file wins regardless of env
        assert_eq!(
            auth_source_label(false, true),
            "config file ([cql].auth_enabled)"
        );
        assert_eq!(
            auth_source_label(true, true),
            "config file ([cql].auth_enabled)"
        );
        // only env set -> env
        assert_eq!(auth_source_label(true, false), "FERROSA_AUTH_ENABLED env");
        // neither set -> default
        assert_eq!(auth_source_label(false, false), "default");
    }

    /// Regression (caught by the install smoke): the bundled config ships
    /// `data_dir = "~/.ferrosa/data"`; a leading `~` must expand to $HOME, or the
    /// engine creates a directory literally named `~` in the process CWD.
    #[test]
    #[serial_test::serial(home_env)]
    fn expand_tilde_expands_leading_home() {
        let prev = std::env::var("HOME").ok();
        std::env::set_var("HOME", "/home/smoke");
        assert_eq!(
            expand_tilde("~/.ferrosa/data".into()),
            "/home/smoke/.ferrosa/data"
        );
        assert_eq!(expand_tilde("~".into()), "/home/smoke");
        // No leading ~: unchanged.
        assert_eq!(expand_tilde("/var/lib/ferrosa".into()), "/var/lib/ferrosa");
        // A `~` not at the start (or `~user`) is left alone — we only handle $HOME.
        assert_eq!(expand_tilde("/x/~/y".into()), "/x/~/y");
        assert_eq!(expand_tilde("~bob/data".into()), "~bob/data");
        match prev {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }

    #[test]
    fn config_val_missing_section_returns_default() {
        let key = "FERROSA_TEST_CFG_NOSEC_1b7f";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "nonexistent", "key", "fallback");
        assert_eq!(result, "fallback");
    }

    #[test]
    fn config_val_missing_key_returns_default() {
        let key = "FERROSA_TEST_CFG_NOKEY_8e3d";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "storage", "nonexistent", "default_val");
        assert_eq!(result, "default_val");
    }

    #[test]
    fn default_hinted_handoff_dir_lives_under_storage_data_dir() {
        let config = empty_config();
        let data_dir = Path::new("/var/lib/ferrosa");

        let result = resolve_hinted_handoff_dir(&config, data_dir, None);

        assert_eq!(result, Path::new("/var/lib/ferrosa").join("hints"));
    }

    #[test]
    fn hinted_handoff_env_override_is_preserved() {
        let config = empty_config();
        let data_dir = Path::new("/var/lib/ferrosa");

        let result = resolve_hinted_handoff_dir(&config, data_dir, Some("/custom/hints"));

        assert_eq!(result, Path::new("/custom/hints"));
    }

    #[test]
    fn hinted_handoff_toml_override_is_preserved() {
        let config = toml::from_str(
            r#"
            [cluster]
            hinted_handoff_dir = "/toml/hints"
            "#,
        )
        .unwrap();
        let data_dir = Path::new("/var/lib/ferrosa");

        let result = resolve_hinted_handoff_dir(&config, data_dir, None);

        assert_eq!(result, Path::new("/toml/hints"));
    }

    #[test]
    fn load_config_returns_empty_table_for_missing_file() {
        let result = load_config("/tmp/ferrosa_nonexistent_config_test.toml").unwrap();
        assert!(result.is_table());
        assert!(result.as_table().unwrap().is_empty());
    }

    #[test]
    fn load_config_parses_valid_toml_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ferrosa.toml");
        std::fs::write(
            &path,
            r#"
            [storage]
            data_dir = "/test/data"

            [cql]
            bind = "0.0.0.0:9042"
            "#,
        )
        .unwrap();

        let config = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(
            config
                .get("storage")
                .unwrap()
                .get("data_dir")
                .unwrap()
                .as_str()
                .unwrap(),
            "/test/data"
        );
        assert_eq!(
            config
                .get("cql")
                .unwrap()
                .get("bind")
                .unwrap()
                .as_str()
                .unwrap(),
            "0.0.0.0:9042"
        );
    }

    /// RED (D-47): a table-less-but-PRESENT `schema.json` must not hide user
    /// tables whose SSTables are intact on disk and whose complete schema sits
    /// in `storage-schema.json` beside it.
    ///
    /// This is the shape a SIGKILL inside D-47's 30 s maintenance window left
    /// behind: the registry snapshot carries the four system keyspaces and ZERO
    /// tables, so `apply_snapshot` restores nothing, `schema_restored = true`
    /// suppresses the S3 fallback, and the node serves a schema whose only user
    /// table (`fault_ks.kv`) is invisible: `system_schema.tables` answers with
    /// no user rows and `SELECT ... FROM fault_ks.kv` fails
    /// "keyspace 'fault_ks' not found" — CQL resolves through the REGISTRY, not
    /// the engine (ferrosa-cql/src/router.rs `validate_keyspace_exists`).
    ///
    /// The engine's own fallback (`load_local_table_schemas`) cannot fix this:
    /// the flat storage format carries the partition key's TYPE but not its
    /// COLUMN NAME, and the key column name is unrecoverable from every other
    /// durable artifact (`system_schema.tables`/`columns` hold 0 SSTable files
    /// on this fixture, and key columns are NOT named in the SSTable
    /// `SerializationHeader` — see `ferrosa-ctl` `TableLayout`).
    #[test]
    fn table_less_snapshot_reports_disk_tables_and_fabricates_none() {
        let dir = tempfile::tempdir().unwrap();

        // storage-schema.json: the complete flat schema, present all along.
        let flat = serde_json::json!([{
            "keyspace": "fault_ks",
            "table": "kv",
            "key_type": "org.apache.cassandra.db.marshal.Int32Type",
            "clustering_columns": [],
            "static_columns": [],
            "regular_columns": [
                {"name": "v", "type_name": "org.apache.cassandra.db.marshal.UTF8Type"}
            ],
            "extensions": {}
        }]);
        std::fs::write(
            dir.path().join("storage-schema.json"),
            serde_json::to_vec(&flat).unwrap(),
        )
        .unwrap();

        // schema.json: keyspaces but ZERO tables — present, so the S3 fallback
        // is suppressed by `schema_restored = true`.
        let mut snap = ferrosa_schema::SchemaSnapshot::new();
        snap.keyspaces.insert(
            "fault_ks".into(),
            ferrosa_schema::KeyspaceMetadata {
                name: "fault_ks".into(),
                replication: ferrosa_schema::ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: [("replication_factor".into(), "1".into())]
                        .into_iter()
                        .collect(),
                },
                durable_writes: true,
            },
        );
        let json = serde_json::to_vec_pretty(&snap).unwrap();
        std::fs::write(dir.path().join("schema.json"), &json).unwrap();

        let loaded = load_local_schema(dir.path()).unwrap().unwrap();
        assert!(
            loaded.tables.is_empty() && !loaded.keyspaces.is_empty(),
            "fixture precondition: the snapshot must be table-less but present"
        );

        let schema = ferrosa_schema::Schema::new(test_schema_config()).unwrap();
        schema.apply_snapshot(loaded).unwrap();

        // The storage engine the restore path is handed. Its own construction
        // already consumed storage-schema.json (FIX t_2db96eb9), so
        // `fault_ks.kv` IS registered and its SSTables ARE readable.
        let engine = ferrosa_storage::StorageEngine::new(
            ferrosa_storage::StorageEngineConfig::test_config(dir.path()),
            None,
        )
        .unwrap();

        register_user_tables_with_storage(&engine, &schema).unwrap();

        let snap = schema.snapshot();

        // The registry cannot be rebuilt from storage-schema.json, and this test
        // pins WHY rather than asserting a recovery that cannot happen.
        //
        // A registry table needs `TableMetadata.partition_key: Vec<String>` — the
        // partition key's COLUMN NAMES. `TableSchema` carries only `key_type`, a
        // type class (`org.apache.cassandra.db.marshal.Int32Type`), and carries no
        // clustering order, no column masks and no table id. Partition-key bytes
        // are decoded positionally against the declared type at the key column's
        // index, so a synthesised name either fails the query or returns a
        // wrongly-named column — strictly worse than an explicit failure.
        //
        // So the contract for a table-less snapshot is: report loudly, name what
        // storage-schema.json still holds, and do NOT fabricate a table. Recovery
        // requires a durable schema record written when DDL is acknowledged
        // (t_0acc233d), which is why this test asserts the report and the absence
        // of invention rather than a rebuilt table.
        assert!(
            !snap
                .tables
                .contains_key(&("fault_ks".to_string(), "kv".to_string())),
            "a table-less snapshot must not silently gain a table built from \
             storage-schema.json: that format names no partition-key column, so any \
             reconstructed table would be nameable in CQL but undecodable. Found \
             tables={:?}",
            snap.tables.keys().collect::<Vec<_>>()
        );

        // What the diagnostic must surface: the tables that exist on disk, so an
        // operator can see the data is present even though the registry lost it.
        let on_disk = ferrosa_storage::schema_snapshot::user_tables_in_storage_schema(dir.path());
        assert!(
            on_disk
                .iter()
                .any(|(ks, table)| ks == "fault_ks" && table == "kv"),
            "the table-less-snapshot report must name the user tables present in \
             storage-schema.json; got {on_disk:?}"
        );
    }

    fn test_schema_config() -> ferrosa_schema::SchemaConfig {
        ferrosa_schema::SchemaConfig {
            hasher: ferrosa_schema::PasswordHasher::default(),
            password_policy: ferrosa_schema::PasswordPolicy::permissive(),
            auth_method: ferrosa_schema::AuthMethod::Password,
            rate_limit: ferrosa_schema::RateLimitConfig::default(),
            audit_sink: Box::new(ferrosa_schema::LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: ferrosa_schema::DeploymentMode::Development,
        }
    }

    #[test]
    fn load_config_rejects_invalid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "this is not [valid toml =").unwrap();

        assert!(load_config(path.to_str().unwrap()).is_err());
    }

    #[test]
    fn persist_schema_locally_writes_schema_json() {
        let dir = tempfile::tempdir().unwrap();
        let schema_config = ferrosa_schema::SchemaConfig {
            hasher: ferrosa_schema::PasswordHasher::default(),
            password_policy: ferrosa_schema::PasswordPolicy::permissive(),
            auth_method: ferrosa_schema::AuthMethod::Password,
            rate_limit: ferrosa_schema::RateLimitConfig::default(),
            audit_sink: Box::new(ferrosa_schema::LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: ferrosa_schema::DeploymentMode::Development,
        };
        let schema = ferrosa_schema::Schema::new(schema_config).unwrap();

        // Add a user keyspace via internal API (bypasses auth check)
        schema
            .create_keyspace_internal(ferrosa_schema::KeyspaceMetadata {
                name: "test_ks".into(),
                replication: ferrosa_schema::ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: [("replication_factor".into(), "1".into())]
                        .into_iter()
                        .collect(),
                },
                durable_writes: true,
            })
            .unwrap();

        persist_schema_locally(dir.path(), &schema).unwrap();

        let schema_path = dir.path().join("schema.json");
        assert!(
            schema_path.exists(),
            "schema.json must be written to data_dir"
        );

        let restored = load_local_schema(dir.path()).unwrap().unwrap();
        assert!(
            restored.keyspaces.contains_key("test_ks"),
            "restored snapshot must contain user keyspace"
        );
    }

    #[test]
    fn load_local_schema_returns_snapshot_when_file_exists() {
        let dir = tempfile::tempdir().unwrap();

        // Write a minimal schema.json
        let mut snap = ferrosa_schema::SchemaSnapshot::new();
        snap.keyspaces.insert(
            "my_ks".into(),
            ferrosa_schema::KeyspaceMetadata {
                name: "my_ks".into(),
                replication: ferrosa_schema::ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: [("replication_factor".into(), "1".into())]
                        .into_iter()
                        .collect(),
                },
                durable_writes: true,
            },
        );
        let json = serde_json::to_vec_pretty(&snap).unwrap();
        std::fs::write(dir.path().join("schema.json"), &json).unwrap();

        let loaded = load_local_schema(dir.path()).unwrap();
        assert!(loaded.is_some(), "must load schema.json from data_dir");
        let loaded = loaded.unwrap();
        assert!(loaded.keyspaces.contains_key("my_ks"));
    }

    #[test]
    fn load_local_schema_returns_none_when_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = load_local_schema(dir.path()).unwrap();
        assert!(
            loaded.is_none(),
            "must return None when schema.json is absent"
        );
    }

    /// RED (D-47): the maintenance loop's schema-persist guard must be false on
    /// the loop's very first tick when nothing has changed, because
    /// `tokio::time::interval` fires its first tick immediately (t≈0) and at
    /// that instant the registry holds only system keyspaces. Persisting there
    /// writes a table-less `schema.json`; a SIGKILL before the next 30s tick
    /// leaves it as the last writer on disk and the node restarts with its user
    /// tables gone.
    #[test]
    fn maintenance_loop_does_not_persist_schema_on_first_tick() {
        let current = uuid::Uuid::new_v4();
        let seeded = maintenance_last_schema_version(current);

        assert!(
            !should_persist_schema(current, seeded),
            "the first maintenance tick must see no version change and skip the \
             persist; seeding `last_schema_version` with a value other than the \
             live snapshot version (e.g. Uuid::nil()) writes a table-less \
             schema.json within milliseconds of startup"
        );
    }

    /// The seed must be the CURRENT registry version, not a fixed sentinel: a
    /// constant would make the guard true on the first tick for every node.
    #[test]
    fn maintenance_seed_equals_the_live_snapshot_version() {
        let current = uuid::Uuid::new_v4();
        assert_eq!(
            maintenance_last_schema_version(current),
            current,
            "the loop must start from the version the registry already holds"
        );
    }

    #[test]
    fn should_persist_schema_true_only_after_a_version_advance() {
        let last = uuid::Uuid::new_v4();
        assert!(
            !should_persist_schema(last, last),
            "an unchanged version must not trigger a persist"
        );
        assert!(
            should_persist_schema(uuid::Uuid::new_v4(), last),
            "a version the loop has not yet written must trigger a persist"
        );
        // The nil sentinel is exactly the case the old code got wrong: a real
        // version differs from nil, so the guard fired on the first tick.
        assert!(
            should_persist_schema(uuid::Uuid::new_v4(), uuid::Uuid::nil()),
            "a real version differs from nil — this is why nil as the seed fired \
             the persist on tick one"
        );
    }

    #[test]
    fn example_config_file_is_valid_toml() {
        let example = include_str!("../ferrosa.example.toml");
        let parsed: Result<toml::Value, _> = toml::from_str(example);
        assert!(parsed.is_ok(), "ferrosa.example.toml must be valid TOML");

        let config = parsed.unwrap();
        // Verify key sections exist
        assert!(config.get("cql").is_some());
        assert!(config.get("internode").is_some());
        assert!(config.get("storage").is_some());
        assert!(config.get("s3").is_some());
        assert!(config.get("graph").is_some());
        assert!(config.get("web").is_some());
    }

    // =========================================================================
    // BT-005: Config loading edge cases
    // =========================================================================

    /// BT-005a: config_val falls back to default when the TOML section exists
    /// but the requested key is missing. Verifies that a partial config doesn't
    /// cause panics.
    #[test]
    fn bt005_config_val_partial_section_missing_key() {
        let key = "FERROSA_TEST_BT005A_PARTIAL_SEC";
        std::env::remove_var(key);
        let config: toml::Value = toml::from_str(
            r#"
            [storage]
            data_dir = "/data"
            "#,
        )
        .unwrap();

        // Key "flush_interval" is absent from [storage] — default must apply.
        let result = config_val(key, &config, "storage", "flush_interval", "30");
        assert_eq!(result, "30", "missing key must fall back to default");
    }

    /// BT-005b: config_val works with numeric TOML values (not just strings).
    /// TOML `port = 9042` is an integer, not a string — config_val must
    /// stringify it via `to_string()`.
    #[test]
    fn bt005_config_val_numeric_toml_value() {
        let key = "FERROSA_TEST_BT005B_NUMERIC";
        std::env::remove_var(key);
        let config: toml::Value = toml::from_str(
            r#"
            [cql]
            port = 9042
            "#,
        )
        .unwrap();

        let result = config_val(key, &config, "cql", "port", "9999");
        assert_eq!(
            result, "9042",
            "numeric TOML values must be stringified correctly"
        );
    }

    /// BT-005c: config_val with completely empty TOML (empty string) returns
    /// the default for every key.
    #[test]
    fn bt005_config_val_empty_toml_string() {
        let key = "FERROSA_TEST_BT005C_EMPTY";
        std::env::remove_var(key);
        let config: toml::Value = toml::from_str("").unwrap();

        let result = config_val(key, &config, "storage", "data_dir", "/default/path");
        assert_eq!(result, "/default/path");
    }

    /// BT-005d: load_config with empty TOML file returns an empty table
    /// (not an error).
    #[test]
    fn bt005_load_config_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.toml");
        std::fs::write(&path, "").unwrap();

        let config = load_config(path.to_str().unwrap()).unwrap();
        assert!(config.is_table(), "empty TOML file must parse as a table");
    }

    /// BT-005e: WebConfig::default() is loopback-only by default.
    /// Uses Default impl directly to avoid env var race conditions in parallel tests.
    #[test]
    fn bt005_web_config_default_bind() {
        let wc = crate::web::WebConfig::default();
        assert_eq!(
            wc.bind_addr.to_string(),
            "127.0.0.1:9090",
            "default web bind must be loopback-only"
        );
    }

    /// `[web] bind` in the config file wins over `FERROSA_WEB_BIND`, which
    /// wins over the loopback default.
    #[test]
    #[serial_test::serial(env)]
    fn bt005_web_bind_resolves_toml_over_env() {
        std::env::remove_var("FERROSA_WEB_BIND");

        // Default when neither source sets it.
        assert_eq!(
            resolve_web_config(&empty_config(), &listener_tls::ListenerTlsConfig::default())
                .bind_addr
                .to_string(),
            DEFAULT_WEB_BIND
        );

        // Env var is the fallback when the config file is silent.
        std::env::set_var("FERROSA_WEB_BIND", "127.0.0.1:8080");
        assert_eq!(
            resolve_web_config(&empty_config(), &listener_tls::ListenerTlsConfig::default())
                .bind_addr
                .to_string(),
            "127.0.0.1:8080"
        );

        // Config file wins over the env var.
        let cfg: toml::Value = toml::from_str("[web]\nbind = \"127.0.0.1:19091\"\n").unwrap();
        assert_eq!(
            resolve_web_config(&cfg, &listener_tls::ListenerTlsConfig::default())
                .bind_addr
                .to_string(),
            "127.0.0.1:19091",
            "config file [web] bind must win over FERROSA_WEB_BIND"
        );

        std::env::remove_var("FERROSA_WEB_BIND");
    }

    #[test]
    #[serial_test::serial(env)]
    fn graph_listener_binds_resolve_toml_over_env() {
        std::env::remove_var("FERROSA_GRAPH_BIND");
        std::env::remove_var("FERROSA_BOLT_PORT");

        let default_http =
            resolve_graph_http_config(&empty_config(), &listener_tls::ListenerTlsConfig::default());
        let default_bolt = resolve_graph_bolt_config(
            &empty_config(),
            default_http.bind_addr,
            false,
            &listener_tls::ListenerTlsConfig::default(),
        )
        .unwrap();
        assert_eq!(default_http.bind_addr.to_string(), DEFAULT_GRAPH_HTTP_BIND);
        assert_eq!(default_bolt.bind_addr.to_string(), "127.0.0.1:7687");

        std::env::set_var("FERROSA_GRAPH_BIND", "127.0.0.1:17474");
        std::env::set_var("FERROSA_BOLT_PORT", "17687");
        let env_http =
            resolve_graph_http_config(&empty_config(), &listener_tls::ListenerTlsConfig::default());
        let env_bolt = resolve_graph_bolt_config(
            &empty_config(),
            env_http.bind_addr,
            false,
            &listener_tls::ListenerTlsConfig::default(),
        )
        .unwrap();
        assert_eq!(env_http.bind_addr.to_string(), "127.0.0.1:17474");
        assert_eq!(env_bolt.bind_addr.to_string(), "127.0.0.1:17687");

        let config: toml::Value = toml::from_str(
            r#"
            [graph]
            bind = "127.0.0.1:27474"
            bolt_port = 27687
            "#,
        )
        .unwrap();
        let toml_http =
            resolve_graph_http_config(&config, &listener_tls::ListenerTlsConfig::default());
        let toml_bolt = resolve_graph_bolt_config(
            &config,
            toml_http.bind_addr,
            false,
            &listener_tls::ListenerTlsConfig::default(),
        )
        .unwrap();
        assert_eq!(toml_http.bind_addr.to_string(), "127.0.0.1:27474");
        assert_eq!(toml_bolt.bind_addr.to_string(), "127.0.0.1:27687");

        std::env::remove_var("FERROSA_GRAPH_BIND");
        std::env::remove_var("FERROSA_BOLT_PORT");
    }

    /// SPARQL stays private by default; an explicit config entry remains the
    /// supported way to expose it beyond loopback.
    #[test]
    #[serial_test::serial(env)]
    fn sparql_bind_resolves_toml_over_env() {
        std::env::remove_var("FERROSA_SPARQL_BIND");
        assert_eq!(
            resolve_sparql_bind(&empty_config()).to_string(),
            DEFAULT_SPARQL_BIND
        );

        std::env::set_var("FERROSA_SPARQL_BIND", "127.0.0.1:18080");
        assert_eq!(
            resolve_sparql_bind(&empty_config()).to_string(),
            "127.0.0.1:18080"
        );

        let config: toml::Value = toml::from_str(
            r#"
            [sparql]
            bind = "0.0.0.0:28080"
            "#,
        )
        .unwrap();
        assert_eq!(
            resolve_sparql_bind(&config).to_string(),
            "0.0.0.0:28080",
            "config file [sparql] bind must win over FERROSA_SPARQL_BIND"
        );

        std::env::remove_var("FERROSA_SPARQL_BIND");
    }

    /// A local install must not expose database/query listeners unless the
    /// operator deliberately configures a non-loopback address.
    #[test]
    #[serial_test::serial(env)]
    fn core_listener_binds_resolve_toml_over_env() {
        for key in [
            "FERROSA_CQL_BIND",
            "FERROSA_FLIGHT_BIND",
            "FERROSA_POSTGRES_BIND",
        ] {
            std::env::remove_var(key);
        }

        assert_eq!(
            resolve_cql_bind(&empty_config()).to_string(),
            DEFAULT_CQL_BIND
        );
        assert_eq!(
            resolve_flight_bind(&empty_config()).to_string(),
            DEFAULT_FLIGHT_BIND
        );
        assert_eq!(
            resolve_postgres_bind(&empty_config()).to_string(),
            DEFAULT_POSTGRES_BIND
        );

        std::env::set_var("FERROSA_CQL_BIND", "127.0.0.1:19042");
        std::env::set_var("FERROSA_FLIGHT_BIND", "127.0.0.1:18815");
        std::env::set_var("FERROSA_POSTGRES_BIND", "127.0.0.1:15432");
        assert_eq!(
            resolve_cql_bind(&empty_config()).to_string(),
            "127.0.0.1:19042"
        );
        assert_eq!(
            resolve_flight_bind(&empty_config()).to_string(),
            "127.0.0.1:18815"
        );
        assert_eq!(
            resolve_postgres_bind(&empty_config()).to_string(),
            "127.0.0.1:15432"
        );

        let config: toml::Value = toml::from_str(
            r#"
            [cql]
            bind = "0.0.0.0:29042"
            [flight]
            bind = "0.0.0.0:28815"
            [postgres]
            bind = "0.0.0.0:25432"
            "#,
        )
        .unwrap();
        assert_eq!(resolve_cql_bind(&config).to_string(), "0.0.0.0:29042");
        assert_eq!(resolve_flight_bind(&config).to_string(), "0.0.0.0:28815");
        assert_eq!(resolve_postgres_bind(&config).to_string(), "0.0.0.0:25432");

        for key in [
            "FERROSA_CQL_BIND",
            "FERROSA_FLIGHT_BIND",
            "FERROSA_POSTGRES_BIND",
        ] {
            std::env::remove_var(key);
        }
    }

    /// BT-005h: an env var set to the empty string is treated as a set value
    /// (not as "unset") when it is the effective source. With TOML-wins
    /// precedence, that means: when the config file does NOT set the key, the
    /// empty env var is returned as-is rather than falling through to default.
    #[test]
    fn bt005_config_val_env_empty_string() {
        let key = "FERROSA_TEST_BT005H_EMPTY_STR";
        std::env::set_var(key, "");

        // Config file silent → env var (even "") is the fallback, returned as-is.
        let result = config_val(key, &empty_config(), "storage", "data_dir", "/default");
        assert_eq!(
            result, "",
            "empty env var must be returned as-is (not fall through to default)"
        );

        // But the config file still wins when it sets the key.
        let result = config_val(key, &sample_config(), "storage", "data_dir", "/default");
        assert_eq!(
            result, "/data/ferrosa",
            "config file value must win over an empty env var"
        );
        std::env::remove_var(key);
    }

    /// BT-005i: load_or_generate_host_id generates a valid UUID and persists it.
    #[test]
    #[serial_test::serial(env)]
    fn bt005_host_id_generation_and_persistence() {
        let dir = tempfile::tempdir().unwrap();
        std::env::remove_var("FERROSA_HOST_ID");

        let id1 = load_or_generate_host_id(dir.path());
        // Must be a valid UUID (non-nil).
        assert_ne!(id1, Uuid::nil(), "generated host_id must not be nil");

        // Re-read: same UUID should come back from disk.
        let id2 = load_or_generate_host_id(dir.path());
        assert_eq!(id1, id2, "host_id must be stable across reads");
    }

    /// BT-005j: load_or_generate_host_id respects FERROSA_HOST_ID env var.
    #[test]
    fn bt005_host_id_from_env_var() {
        let dir = tempfile::tempdir().unwrap();
        let expected = Uuid::new_v4();

        // Use the _with variant directly — no process-global env var mutation,
        // so this test is safe to run in parallel with other tests.
        let id = load_or_generate_host_id_with(dir.path(), Some(expected.to_string()));
        assert_eq!(id, expected, "host_id must match override");
    }

    /// BT-005k: corrupt schema.json is preserved and startup fails loud.
    #[test]
    fn bt005_load_local_schema_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("schema.json"), "{{invalid json}}").unwrap();

        let error = load_local_schema(dir.path()).unwrap_err().to_string();
        assert!(error.contains("refusing to start"));
        assert!(!dir.path().join("schema.json").exists());
        assert_eq!(
            std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("schema.json.unparseable-"))
                .count(),
            1,
            "the unreadable input must be retained exactly once"
        );
    }

    /// BT-005l: load_config with deeply nested TOML values extracts correctly.
    #[test]
    fn bt005_config_val_deeply_nested_section() {
        let key = "FERROSA_TEST_BT005L_NESTED";
        std::env::remove_var(key);
        // config_val only supports one level of nesting (section.key),
        // so a nested table under a section should be returned as its
        // TOML representation string.
        let config: toml::Value = toml::from_str(
            r#"
            [replication]
            class = "SimpleStrategy"
            "#,
        )
        .unwrap();

        let result = config_val(
            key,
            &config,
            "replication",
            "class",
            "NetworkTopologyStrategy",
        );
        assert_eq!(
            result, "SimpleStrategy",
            "string value in section must be extracted"
        );
    }

    #[test]
    fn telemetry_layer_created_with_env_sample_rate() {
        // Verify that FerrosaTelemetryLayer can be instantiated and configured
        // from env-derived sample rate, matching the code path in main().
        let layer = ferrosa_cluster::telemetry::FerrosaTelemetryLayer::new(0.05);
        // After creation, no spans have been sampled.
        assert_eq!(layer.sampled(), 0);
    }

    // ---- Lane churn: seed loop must not reconnect to already-connected seeds ----

    /// When one seed is down, the seed loop must not reconnect to the seed
    /// that is already connected on every retry iteration. Reconnecting
    /// churns the peer's pool (old lane actors shut down, stale TCP
    /// connections accumulate) until the peer hits `max_connections` and
    /// rejects new inbound connections — "max connections reached".
    #[test]
    fn seeds_to_connect_excludes_already_connected_seeds() {
        let seeds = vec!["node2:7000".to_string(), "node3:7000".to_string()];
        let mut connected = std::collections::HashSet::new();
        connected.insert("node2:7000".to_string());

        let pending = seeds_to_connect(&seeds, &connected);
        assert_eq!(
            pending,
            vec!["node3:7000".to_string()],
            "already-connected seed must not appear in the pending list"
        );
    }

    /// With no seeds connected, all should be pending.
    #[test]
    fn seeds_to_connect_returns_all_when_none_connected() {
        let seeds = vec!["node2:7000".to_string(), "node3:7000".to_string()];
        let connected = std::collections::HashSet::new();

        let pending = seeds_to_connect(&seeds, &connected);
        assert_eq!(
            pending, seeds,
            "all seeds should be pending when none are connected"
        );
    }

    /// Once all seeds are connected, the pending list should be empty —
    /// the loop breaks immediately.
    #[test]
    fn seeds_to_connect_returns_empty_when_all_connected() {
        let seeds = vec!["node2:7000".to_string(), "node3:7000".to_string()];
        let mut connected = std::collections::HashSet::new();
        connected.insert("node2:7000".to_string());
        connected.insert("node3:7000".to_string());

        let pending = seeds_to_connect(&seeds, &connected);
        assert!(
            pending.is_empty(),
            "no seeds should be pending when all are connected"
        );
    }

    /// The pending list must preserve the original seed order so that
    /// retry behavior is deterministic across loop iterations.
    #[test]
    fn seeds_to_connect_preserves_seed_order() {
        let seeds = vec![
            "alpha:7000".to_string(),
            "beta:7000".to_string(),
            "gamma:7000".to_string(),
        ];
        let mut connected = std::collections::HashSet::new();
        connected.insert("beta:7000".to_string());

        let pending = seeds_to_connect(&seeds, &connected);
        assert_eq!(
            pending,
            vec!["alpha:7000".to_string(), "gamma:7000".to_string()],
            "remaining seeds must preserve original order"
        );
    }

    // ── t_d5d122ba: production mode requires TLS on every enabled listener ──

    fn tls_required() -> listener_tls::ListenerTlsConfig {
        listener_tls::ListenerTlsConfig {
            cert: Some("/certs/node.crt".into()),
            key: Some("/certs/node.key".into()),
            require_tls: true,
        }
    }

    fn all_listeners_require_tls() -> ListenerTlsInputs {
        ListenerTlsInputs {
            cql: tls_required(),
            postgres: tls_required(),
            graph: tls_required(),
            sparql: tls_required(),
            web: tls_required(),
            flight: tls_required(),
            graph_enabled: true,
            sparql_enabled: true,
            flight_enabled: Some(true),
        }
    }

    fn production_violations(
        inputs: &ListenerTlsInputs,
        internode_require_tls: bool,
    ) -> Vec<ferrosa_schema::startup::ProductionViolation> {
        use ferrosa_schema::startup::{
            validate_production_requirements, DeploymentMode, ProductionCheckConfig,
        };
        validate_production_requirements(&ProductionCheckConfig {
            mode: DeploymentMode::Production,
            password_policy: ferrosa_schema::PasswordPolicy::iso27001(),
            has_superuser_password: true,
            secrets_provider_type: "aws-secrets-manager".into(), // pragma: allowlist secret
            s3_allow_http: false,
            auth_enabled: true,
            listeners: inputs.postures(),
            internode_require_tls,
        })
    }

    #[test]
    fn production_passes_when_every_listener_and_internode_require_tls() {
        let violations = production_violations(&all_listeners_require_tls(), true);
        assert!(violations.is_empty(), "{violations:?}");
    }

    /// Each listener the binary starts, turned to plaintext on its own, is
    /// refused by name with the key that fixes it.
    #[test]
    fn production_refuses_each_enabled_listener_that_does_not_require_tls() {
        type Edit = fn(&mut ListenerTlsInputs);
        let cases: [(&str, &str, Edit); 6] = [
            ("CQL", "[cql] require_tls", |i| i.cql.require_tls = false),
            ("PostgreSQL", "[postgres] require_tls", |i| {
                i.postgres.require_tls = false
            }),
            ("graph HTTP", "[graph] require_tls", |i| {
                i.graph.require_tls = false
            }),
            ("Bolt", "[graph] require_tls", |i| {
                i.graph.require_tls = false
            }),
            ("SPARQL", "[sparql] require_tls", |i| {
                i.sparql.require_tls = false
            }),
            ("web console", "[web] require_tls", |i| {
                i.web.require_tls = false
            }),
        ];
        for (listener, key, edit) in cases {
            let mut inputs = all_listeners_require_tls();
            edit(&mut inputs);
            let messages: Vec<String> = production_violations(&inputs, true)
                .iter()
                .filter(|v| v.blocks_startup())
                .map(|v| v.to_string())
                .collect();
            assert!(
                messages
                    .iter()
                    .any(|m| m.contains(&format!("the {listener} listener")) && m.contains(key)),
                "{listener}: expected a blocking refusal naming {key}, got {messages:?}"
            );
        }
    }

    #[test]
    fn production_allows_disabled_graph_and_sparql_without_tls() {
        let mut inputs = all_listeners_require_tls();
        inputs.graph = listener_tls::ListenerTlsConfig::default();
        inputs.sparql = listener_tls::ListenerTlsConfig::default();
        inputs.graph_enabled = false;
        inputs.sparql_enabled = false;
        let violations = production_violations(&inputs, true);
        assert!(violations.is_empty(), "{violations:?}");
    }

    /// t_58db6320: Flight has TLS now, so production applies the same rule
    /// as every other listener: enabled Flight must require TLS, and the
    /// refusal names `[flight] require_tls`, `tls_cert` and `tls_key`.
    #[test]
    fn production_refuses_an_enabled_flight_listener_that_does_not_require_tls() {
        let mut inputs = all_listeners_require_tls();
        inputs.flight = listener_tls::ListenerTlsConfig::default();
        let messages: Vec<String> = production_violations(&inputs, true)
            .iter()
            .filter(|v| v.blocks_startup())
            .map(|v| v.to_string())
            .collect();
        let [message] = messages.as_slice() else {
            panic!("expected exactly the Flight refusal, got {messages:?}");
        };
        for key in [
            "the Arrow Flight listener",
            "[flight] require_tls",
            "[flight] tls_cert",
            "[flight] tls_key",
        ] {
            assert!(message.contains(key), "missing {key:?}: {message}");
        }
        assert!(
            !message.contains("enabled = false"),
            "Flight has TLS; the remedy is TLS, not disabling it: {message}"
        );

        // Disabled Flight needs no TLS.
        inputs.flight_enabled = Some(false);
        assert!(production_violations(&inputs, true).is_empty());
        // A build without the flight feature has no Flight listener at all.
        inputs.flight_enabled = None;
        assert!(production_violations(&inputs, true).is_empty());
    }

    #[test]
    fn production_accepts_an_enabled_flight_listener_that_requires_tls() {
        let inputs = all_listeners_require_tls();
        assert_eq!(inputs.flight_enabled, Some(true));
        let violations = production_violations(&inputs, true);
        assert!(violations.is_empty(), "{violations:?}");
    }

    /// One node-wide `[tls]` certificate with `require = true` satisfies the
    /// production gate for every listener, Flight included, and internode.
    #[test]
    fn one_node_wide_certificate_satisfies_the_production_gate() {
        let toml: toml::Value = "[tls]\ncert = \"/n.crt\"\nkey = \"/n.key\"\n\
                                 ca = \"/ca.crt\"\nrequire = true\n"
            .parse()
            .unwrap();
        let node = listener_tls::resolve_node_tls(&toml).unwrap();
        let mut inputs = ListenerTlsInputs::resolve(&toml, &node);
        inputs.graph_enabled = true;
        inputs.sparql_enabled = true;
        inputs.flight_enabled = Some(true);
        let expected = listener_tls::ListenerTlsConfig {
            cert: Some("/n.crt".into()),
            key: Some("/n.key".into()),
            require_tls: true,
        };
        let all = [
            &inputs.cql,
            &inputs.postgres,
            &inputs.graph,
            &inputs.sparql,
            &inputs.web,
            &inputs.flight,
        ];
        assert!(all.iter().all(|c| **c == expected), "{all:?}");

        let mut net = ferrosa_net::config::NetConfig::default();
        apply_internode_toml_overrides(&mut net, &toml).unwrap();
        listener_tls::apply_node_tls_to_internode(
            &mut net,
            &node,
            listener_tls::internode_require_is_explicit(&toml),
        );
        let violations = production_violations(&inputs, net.require_tls);
        assert!(violations.is_empty(), "{violations:?}");
        assert_eq!(net.tls_ca_path.as_deref(), Some("/ca.crt"));

        // A listener that opts out explicitly is still refused by name.
        let opt_out: toml::Value = "[tls]\ncert = \"/n.crt\"\nkey = \"/n.key\"\n\
                                    require = true\n[flight]\nrequire_tls = false\n"
            .parse()
            .unwrap();
        let mut inputs = ListenerTlsInputs::resolve(&opt_out, &node);
        inputs.flight_enabled = Some(true);
        let messages: Vec<String> = production_violations(&inputs, true)
            .iter()
            .map(|v| v.to_string())
            .collect();
        assert!(
            messages.iter().any(|m| m.contains("[flight] require_tls")),
            "{messages:?}"
        );
    }

    #[test]
    fn flight_tls_keys_come_from_the_flight_section() {
        let toml: toml::Value = "[flight]\ntls_cert = \"/f.crt\"\ntls_key = \"/f.key\"\n\
                                 require_tls = true\n"
            .parse()
            .unwrap();
        let inputs = ListenerTlsInputs::resolve(&toml, &listener_tls::NodeTls::default());
        assert_eq!(
            inputs.flight,
            listener_tls::ListenerTlsConfig {
                cert: Some("/f.crt".into()),
                key: Some("/f.key".into()),
                require_tls: true,
            }
        );
    }

    #[test]
    fn flight_tls_validation_names_flight_when_enabled_and_ignores_it_when_disabled() {
        let mut inputs = all_listeners_require_tls();
        for cfg in [
            &mut inputs.cql,
            &mut inputs.postgres,
            &mut inputs.graph,
            &mut inputs.sparql,
            &mut inputs.web,
        ] {
            *cfg = listener_tls::ListenerTlsConfig::default();
        }
        inputs.flight = listener_tls::ListenerTlsConfig {
            cert: None,
            key: None,
            require_tls: true,
        };
        let err = inputs
            .validate()
            .expect_err("enabled Flight with require_tls and no certificate must stop startup");
        assert!(err.contains("Arrow Flight"), "{err}");

        inputs.flight_enabled = Some(false);
        inputs
            .validate()
            .expect("a disabled Flight listener's require_tls is not enforced");
    }

    #[test]
    fn production_refuses_internode_without_required_tls() {
        let messages: Vec<String> = production_violations(&all_listeners_require_tls(), false)
            .iter()
            .filter(|v| v.blocks_startup())
            .map(|v| v.to_string())
            .collect();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("[internode] require_tls")),
            "{messages:?}"
        );
    }

    #[test]
    fn internode_tls_keys_come_from_toml_and_require_tls_is_strict() {
        let mut cfg = ferrosa_net::config::NetConfig::default();
        let toml: toml::Value = "[internode]\ntls_cert = \"/c.pem\"\ntls_key = \"/k.pem\"\n\
                                 tls_ca = \"/ca.pem\"\nrequire_tls = true\n"
            .parse()
            .unwrap();
        apply_internode_toml_overrides(&mut cfg, &toml).unwrap();
        assert_eq!(cfg.tls_cert_path.as_deref(), Some("/c.pem"));
        assert_eq!(cfg.tls_key_path.as_deref(), Some("/k.pem"));
        assert_eq!(cfg.tls_ca_path.as_deref(), Some("/ca.pem"));
        assert!(cfg.require_tls);

        let typo: toml::Value = "[internode]\nrequire_tls = \"ture\"\n".parse().unwrap();
        let mut cfg = ferrosa_net::config::NetConfig::default();
        let err = apply_internode_toml_overrides(&mut cfg, &typo)
            .expect_err("a typo must not read as require_tls = false");
        assert!(err.contains("[internode] require_tls"), "{err}");
    }

    #[test]
    fn listener_tls_validation_names_the_listener_whose_certificate_is_missing() {
        let mut inputs = all_listeners_require_tls();
        // Every other listener: plaintext, not required (validation passes).
        for cfg in [
            &mut inputs.cql,
            &mut inputs.postgres,
            &mut inputs.graph,
            &mut inputs.web,
        ] {
            *cfg = listener_tls::ListenerTlsConfig::default();
        }
        inputs.sparql.cert = Some("/nonexistent/sparql.crt".into());
        let err = inputs
            .validate()
            .expect_err("a missing certificate must stop startup");
        assert!(
            err.contains("SPARQL") && err.contains("/nonexistent/sparql.crt"),
            "{err}"
        );
    }

    #[test]
    fn flight_enabled_defaults_on_and_parses_strictly() {
        assert_eq!(resolve_flight_enabled(&empty_config()), Ok(true));
        let off: toml::Value = "[flight]\nenabled = false\n".parse().unwrap();
        assert_eq!(resolve_flight_enabled(&off), Ok(false));
        let typo: toml::Value = "[flight]\nenabled = \"flase\"\n".parse().unwrap();
        assert!(resolve_flight_enabled(&typo).is_err());
    }

    #[cfg(feature = "flight")]
    #[test]
    fn flight_advertised_port_follows_the_bind_port_unless_overridden() {
        let bind: std::net::SocketAddr = "0.0.0.0:28815".parse().unwrap();
        assert_eq!(resolve_flight_advertised_port(None, bind), Ok(28815));
        assert_eq!(
            resolve_flight_advertised_port(Some(String::new()), bind),
            Ok(28815)
        );
        assert_eq!(
            resolve_flight_advertised_port(Some("9815".into()), bind),
            Ok(9815)
        );
        for bad in ["port", "0", "70000"] {
            let err = resolve_flight_advertised_port(Some(bad.into()), bind).unwrap_err();
            assert!(err.contains("FERROSA_FLIGHT_PORT"), "{err}");
        }
        assert_eq!(
            resolve_flight_bind(&empty_config()).port(),
            ferrosa_flight::service::DEFAULT_FLIGHT_PORT,
            "the library default must match the binary's default bind port"
        );
    }

    #[test]
    fn flight_token_ttl_defaults_and_rejects_typos() {
        assert_eq!(parse_flight_token_ttl(None), Ok(3600));
        assert_eq!(parse_flight_token_ttl(Some(String::new())), Ok(3600));
        assert_eq!(parse_flight_token_ttl(Some("600".into())), Ok(600));
        for bad in ["1h", "-5", "0"] {
            let err = parse_flight_token_ttl(Some(bad.into())).unwrap_err();
            assert!(err.contains("FERROSA_FLIGHT_TOKEN_TTL_SECS"), "{err}");
        }
    }
}
