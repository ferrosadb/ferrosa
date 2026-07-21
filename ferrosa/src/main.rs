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
/// startup. Override per-deployment with the `MALLOC_CONF` env
/// var (env wins over the link-time default).
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:0,muzzy_decay_ms:0\0";

mod cql_broadcast;
mod repair_wiring;
mod runtime;
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

/// Apply TOML `[internode]` overrides to a `NetConfig`.
///
/// Precedence is **TOML wins** over env (see [`config_val`]): the base
/// `NetConfig::from_env()` seeds each field from `FERROSA_INTERNODE_*`, then
/// this helper overwrites any field the config file sets, so a committed TOML
/// is authoritative. Parse failures are logged but never fatal — a malformed
/// `[internode].bind` leaves the env/default value in place rather than
/// crashing startup.
fn apply_internode_toml_overrides(
    cfg: &mut ferrosa_net::config::NetConfig,
    file_config: &toml::Value,
) {
    let internode = match file_config.get("internode") {
        Some(t) => t,
        None => return,
    };

    if let Some(v) = internode.get("bind").and_then(|v| v.as_str()) {
        match v.parse() {
            Ok(addr) => cfg.bind_addr = addr,
            Err(e) => tracing::warn!(value = %v, %e, "ignoring invalid [internode].bind"),
        }
    }
    if let Some(v) = internode.get("broadcast").and_then(|v| v.as_str()) {
        match v.parse() {
            Ok(addr) => cfg.broadcast_addr = addr,
            Err(e) => tracing::warn!(value = %v, %e, "ignoring invalid [internode].broadcast"),
        }
    }
    if let Some(v) = internode.get("cluster_name").and_then(|v| v.as_str()) {
        cfg.cluster_name = v.to_string();
    }
    if let Some(v) = internode.get("psk").and_then(|v| v.as_str()) {
        cfg.psk = Some(v.to_string());
    }
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

/// Resolve the auth-enabled flag honouring TOML when the env var is unset.
///
/// BUG-006: `[cql] auth_enabled = true` in TOML was silently ignored;
/// only `FERROSA_AUTH_ENABLED=true` activated the authenticator. Returns
/// `Some(true|false)` when the operator made an explicit choice in
/// either place, `None` when they did not (so downstream resolution can
/// fall back to the storage default).
/// Production deployment gate (FMEA epic). In production mode (`FERROSA_MODE=
/// production`), refuses startup when an operator-fixable security requirement
/// is unmet — authentication disabled (`t_87a50318`), CQL TLS not required
/// (`t_27bf4674`), or the default superuser password. Weaknesses whose config is
/// not operator-configurable yet (the hardcoded permissive password policy and
/// env secrets provider) only WARN — see `ProductionViolation::blocks_startup`.
/// No-op in development mode. Calls `exit(1)` rather than returning so the gate
/// cannot be accidentally bypassed by a `?` further up.
fn enforce_production_requirements(
    auth_enabled: bool,
    cql_require_tls: bool,
    has_superuser_password: bool,
) {
    use ferrosa_schema::startup::{
        validate_production_requirements, DeploymentMode, ProductionCheckConfig,
    };
    let violations = validate_production_requirements(&ProductionCheckConfig {
        mode: DeploymentMode::from_env(),
        auth_enabled,
        cql_require_tls,
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
            "Remediate the above (e.g. [cql] auth_enabled = true, [cql] require_tls = true, \
             change the default superuser password), or run a development node (unset \
             FERROSA_MODE=production)."
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

/// Adapts the live [`PeerManager`](ferrosa_net::peer::PeerManager) into the
/// self-heal [`PeerHealthProbe`](ferrosa_storage::self_heal::PeerHealthProbe) so
/// the controller's replica posture reflects currently-reachable peers at each
/// tick (a healthy peer present ⇒ corrupt SSTables may be quarantined).
struct LivePeerHealth(std::sync::Arc<ferrosa_net::peer::PeerManager>);

impl ferrosa_storage::self_heal::PeerHealthProbe for LivePeerHealth {
    fn healthy_peer_count(&self) -> usize {
        self.0.live_peer_ids().len()
    }
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
fn persist_schema_locally(data_dir: &Path, schema: &ferrosa_schema::Schema) {
    let snap = schema.snapshot();
    match serde_json::to_vec_pretty(&*snap) {
        Ok(json) => {
            let schema_path = data_dir.join("schema.json");
            if let Err(e) = std::fs::write(&schema_path, &json) {
                tracing::warn!(%e, "failed to persist schema to local disk");
            }
        }
        Err(e) => tracing::warn!(%e, "failed to serialize schema snapshot for local persist"),
    }
}

/// Load a schema snapshot from `{data_dir}/schema.json`, if it exists.
///
/// Returns `None` if the file doesn't exist or can't be parsed.
fn load_local_schema(data_dir: &Path) -> Option<ferrosa_schema::SchemaSnapshot> {
    let schema_path = data_dir.join("schema.json");
    let data = match std::fs::read(&schema_path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(%e, "failed to read local schema.json");
            return None;
        }
    };
    match serde_json::from_slice(&data) {
        Ok(snap) => Some(snap),
        Err(e) => {
            tracing::warn!(%e, "failed to parse local schema.json");
            None
        }
    }
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let (non_blocking_writer, _log_guard) = tracing_appender::non_blocking(std::io::stdout());

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
            .with(tracing_subscriber::fmt::layer().with_writer(non_blocking_writer))
            .with(telemetry_layer)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(non_blocking_writer)
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

    // SEC (FMEA epic): the production deployment gate (auth, CQL TLS, default
    // superuser password, …) runs as `enforce_production_requirements` once the
    // CQL TLS config is resolved below — it needs `cql_require_tls` and `schema`.

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

    let (storage, pending_mutations) = if has_commitlog_segments {
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

    // 4a. Seed default roles if auth is enabled.
    //
    // `ferrosa_schema::Schema::new` always creates the built-in `cassandra`
    // superuser; the bootstrap helper creates three additional well-known
    // roles (`ferrosa_admin` SUPERUSER, `graph_engine` and `app_reader`
    // unprivileged LOGIN) so a fresh cluster with auth enabled is never
    // locked out. All calls are idempotent — subsequent restarts are no-ops.
    //
    // A one-shot 5-minute task fires a loud WARN if `ferrosa_admin` is
    // still using the default seed password. This mirrors Cassandra's
    // default-credentials nag and gives the operator a single reminder
    // window to rotate.
    if storage_auth_enabled {
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
    } else {
        tracing::info!(
            "auth_enabled=false — skipping seed-role bootstrap. Set \
             FERROSA_AUTH_ENABLED=true to enforce CQL role auth."
        );
    }

    // 4b. Restore schema from local disk or S3.
    //
    // Priority:
    //   1. Local schema.json (survives binary upgrades with same data dir)
    //   2. S3 schema.json (cold start with empty local data, or local schema lost)
    //   3. Start fresh (no schema found anywhere)
    let data_path = Path::new(&data_dir);
    let mut schema_restored = false;

    if let Some(snapshot) = load_local_schema(data_path) {
        let ks_count = snapshot.keyspaces.len();
        let table_count = snapshot.tables.len();
        match schema.apply_snapshot(snapshot) {
            Ok(()) => {
                tracing::info!(
                    ks_count,
                    table_count,
                    "restored schema from local schema.json"
                );
                schema_restored = true;

                // Register existing tables with the storage engine so reads work.
                let snap = schema.snapshot();
                for ((_ks, _tbl), table_meta) in &snap.tables {
                    if ferrosa_schema::is_system_keyspace(&table_meta.keyspace) {
                        continue;
                    }
                    let storage_schema = table_meta.to_storage_schema();
                    if let Err(e) = storage.register_table(storage_schema) {
                        tracing::warn!(
                            table = %table_meta.name,
                            ks = %table_meta.keyspace,
                            "failed to register table from local schema: {e}"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(%e, "failed to apply local schema snapshot, trying S3");
            }
        }
    }

    if !schema_restored && storage.has_s3() {
        tracing::info!("no local schema — attempting S3 bootstrap");
        if let Err(e) = bootstrap_from_s3(&storage, &schema).await {
            tracing::warn!("S3 bootstrap failed (non-fatal, starting fresh): {e}");
        } else {
            // Persist the S3 schema locally so future restarts don't need S3
            persist_schema_locally(data_path, &schema);
        }
    }

    // 4b'. Re-register secondary indexes from the persisted
    // `system_schema.indexes` table. `load_local_schema` (schema.json) restores
    // table schemas with no indexes, so without this every secondary index is
    // silently dropped on restart. This runs after system tables (step 3) and
    // user tables (above) are registered so `add_index` can resolve targets.
    match storage.reload_indexes_from_system_schema() {
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
    }

    // 4c. Replay pending commit log mutations now that tables are registered.
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
    let mut net_config_mut = ferrosa_net::config::NetConfig::from_env();
    // Seed the base from `FERROSA_INTERNODE_*`, then let the config file win:
    // `internode.bind`, `internode.broadcast`, etc. in ferrosa.toml override
    // the env-derived defaults (TOML-wins precedence).
    apply_internode_toml_overrides(&mut net_config_mut, &file_config);
    let net_config = Arc::new(net_config_mut);

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

    let (mode_controller, handles) = ferrosa_cluster::ModeController::new(
        cluster_config,
        net_config.clone(),
        host_id,
        storage.clone(),
        schema.clone(),
        registry.clone(),
    );

    // 6. Create PeerManager — ModeController is the PeerEventListener
    let peer_manager = Arc::new(ferrosa_net::peer::PeerManager::new(
        net_config.clone(),
        host_id,
        mode_controller.clone(),
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

    // 6c. Self-healing controller: deterministic autonomous quarantine of
    // corrupt SSTables. Spawned here (after the PeerManager) so the replica
    // posture reflects LIVE membership: with a healthy peer reachable, a node
    // quarantines its OWN local corrupt generations — moving them out of the
    // active set so they stop being re-detected (and re-logged) every tick;
    // isolated, it leaves them in place (FMEA #1: never quarantine a possibly-
    // only copy). Quarantine only moves files, never deletes. Gated by
    // FERROSA_SELFHEAL_ENABLED (default on). Startup smoke-test warnings fire
    // independently of this controller.
    {
        let selfheal_cfg = ferrosa_storage::self_heal::SelfHealConfig::from_env();
        let peer_probe: std::sync::Arc<dyn ferrosa_storage::self_heal::PeerHealthProbe> =
            std::sync::Arc::new(LivePeerHealth(peer_manager.clone()));
        let cluster_view: std::sync::Arc<dyn ferrosa_storage::self_heal::ClusterView> =
            std::sync::Arc::new(ferrosa_storage::self_heal::ReplicaAwareClusterView::new(
                ferrosa_cluster::raft::uuid_to_node_id(host_id),
                peer_probe,
            ));
        ferrosa_storage::self_heal::SelfHealController::spawn(
            storage.clone(),
            cluster_view,
            selfheal_cfg,
        );
    }

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
    let cql_bind: std::net::SocketAddr = config_val(
        "FERROSA_CQL_BIND",
        &file_config,
        "cql",
        "bind",
        "0.0.0.0:9042",
    )
    .parse()?;
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
    let cql_tls_cert = config_val("FERROSA_CQL_TLS_CERT", &file_config, "cql", "tls_cert", "");
    let cql_tls_key = config_val("FERROSA_CQL_TLS_KEY", &file_config, "cql", "tls_key", "");
    let cql_require_tls = config_val(
        "FERROSA_CQL_REQUIRE_TLS",
        &file_config,
        "cql",
        "require_tls",
        "false",
    );

    // SEC (FMEA epic: t_87a50318 auth + t_27bf4674 TLS): refuse to start a
    // production node that fails an operator-fixable security requirement. Runs
    // before any listener binds. No-op in development mode.
    enforce_production_requirements(
        storage_auth_enabled,
        cql_require_tls == "true" || cql_require_tls == "1",
        !ferrosa_schema::auth::bootstrap::admin_password_is_default(&schema),
    );
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
    let cql_config = ferrosa_cql::server::ServerConfig {
        bind_addr: cql_bind,
        auth_disabled: ferrosa_cql::server::resolve_auth_disabled(
            storage_auth_enabled,
            auth_disabled_override,
        ),
        max_connections: cql_max_connections,
        max_connections_per_ip: cql_max_connections_per_ip,
        tls_cert_path: if cql_tls_cert.is_empty() {
            None
        } else {
            Some(cql_tls_cert)
        },
        tls_key_path: if cql_tls_key.is_empty() {
            None
        } else {
            Some(cql_tls_key)
        },
        require_tls: cql_require_tls == "true" || cql_require_tls == "1",
        ..ferrosa_cql::server::ServerConfig::default()
    };
    // Determine the advertised CQL address+port for system.local.
    // Both are what drivers like cdrs-tokio use to reconcile contact
    // points against the advertised local node during session bootstrap;
    // advertising the container-bind port (9042) when the host-reachable
    // port is 19042 hangs session build. See cql_broadcast::parse_cql_broadcast.
    let (cql_broadcast_addr, cql_broadcast_port) = match std::env::var("FERROSA_CQL_BROADCAST") {
        Ok(addr_str) => cql_broadcast::parse_cql_broadcast(&addr_str, cql_bind.port()),
        Err(_) => {
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
    let udf_executor = Arc::new(
        ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default())
            .expect("failed to initialize UDF executor"),
    );
    storage.set_time_series_wasm_aggregate_executor(Arc::new(
        ferrosa_cql::wasm_aggregate::UdfTimeSeriesAggregateExecutor::new(Arc::clone(&udf_executor)),
    ));
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
        connection_tracker,
        query_tracker,
        full_scan_tracker: full_scan_tracker.clone(),
        index_usage_tracker: index_usage_tracker.clone(),
        event_sender: tokio::sync::broadcast::channel(64).0,
        last_schema_event: tokio::sync::watch::channel(None).0,
        cql_metrics: Arc::new(ferrosa_cql::observability::CqlMetrics::new()),
        topology_policy,
    });
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
    // Capture the Accord transaction committer for the Postgres front-end before
    // shared_state is moved into the CQL server. `None` in standalone mode — a
    // Postgres BEGIN/COMMIT with buffered DML then fails loud (FMEA PG-1).
    let pg_accord_committer = shared_state.core.accord_transaction_committer();
    // Clone the shared execution state for the Flight endpoint before it is
    // moved into the CQL server (feature-gated so it is not an unused clone).
    #[cfg(feature = "flight")]
    let flight_state = shared_state.clone();
    let cql_server = ferrosa_cql::server::CqlServer::new(cql_config, shared_state).with_task_pool(
        ferrosa_net::task_pool::TaskPool::runtime("cql", runtimes.cql.clone()),
    );
    let cql_addr = cql_server.start_background().await?;
    tracing::info!(%cql_addr, "CQL server listening");

    // 9c. Arrow Flight (gRPC) query endpoint — port 8815, behind the `flight`
    // feature. Auth is enforced per-RPC (signed bearer tokens); only
    // anonymous-safe because every read RPC requires a verified token.
    #[cfg(feature = "flight")]
    {
        let flight_bind =
            std::env::var("FERROSA_FLIGHT_BIND").unwrap_or_else(|_| "0.0.0.0:8815".to_string());
        match flight_bind.parse::<std::net::SocketAddr>() {
            Ok(flight_addr) => {
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
                let previous_keys: Vec<Vec<u8>> =
                    std::env::var("FERROSA_FLIGHT_SIGNING_KEY_PREVIOUS")
                        .ok()
                        .into_iter()
                        .flat_map(|v| {
                            v.split(',')
                                .filter(|s| !s.is_empty())
                                .map(|s| s.as_bytes().to_vec())
                                .collect::<Vec<_>>()
                        })
                        .collect();
                let token_ttl_secs: u64 = std::env::var("FERROSA_FLIGHT_TOKEN_TTL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3600);
                let mut service =
                    ferrosa_flight::service::FerrosaFlight::new(flight_state, signing_key)
                        .with_token_ttl(token_ttl_secs);
                if !previous_keys.is_empty() {
                    service = service.with_previous_keys(previous_keys);
                }
                runtimes.background.spawn(async move {
                    if let Err(e) =
                        ferrosa_flight::server::serve_service(flight_addr, service).await
                    {
                        tracing::error!(error = %e, "Arrow Flight server exited");
                    }
                });
                tracing::info!(%flight_addr, "Arrow Flight server listening");
            }
            Err(e) => tracing::error!(
                bind = %flight_bind,
                error = %e,
                "invalid FERROSA_FLIGHT_BIND — Flight server not started"
            ),
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
    };
    // Web console bind: `[web] bind` in the config file wins over
    // `FERROSA_WEB_BIND`, then the default. Previously this read only the env
    // var and defaulted to 0.0.0.0:9090 — so every co-located node collided on
    // 9090, the second binder hit EADDRINUSE, and the resulting `?` return
    // masked the real error via the runtime-drop panic (issue #172).
    let web_bind = config_val(
        "FERROSA_WEB_BIND",
        &file_config,
        "web",
        "bind",
        "0.0.0.0:9090",
    );
    let web_config = web::WebConfig {
        bind_addr: parse_bind_addr("web console", "FERROSA_WEB_BIND", &web_bind),
    };
    let web_addr = web::start_web_server(&web_config, web_state).await?;
    tracing::info!(%web_addr, "web console listening");

    // 10. Graph engine — FERROSA_GRAPH_ENABLED env var, then [graph] enabled
    // in ferrosa.toml. BUG-006: prior code only checked the env var.
    let graph_enabled = resolve_graph_enabled(&file_config, |k| std::env::var(k).ok());

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

    // Graph HTTP bind: `[graph] bind` in the config file wins over
    // `FERROSA_GRAPH_BIND`, then the default. Previously this was hardcoded to
    // the 7474 default, so co-located nodes collided (the second binder logged
    // "graph HTTP server failed … Address already in use" and served no graph
    // endpoint) — the same class of bug as the web-console 9090 collision, just
    // non-fatal because the listener runs in a background task.
    let graph_bind = config_val(
        "FERROSA_GRAPH_BIND",
        &file_config,
        "graph",
        "bind",
        "0.0.0.0:7474",
    );
    let graph_bind_addr = parse_bind_addr("graph HTTP", "FERROSA_GRAPH_BIND", &graph_bind);

    if graph_enabled {
        let graph_config = ferrosa_graph::engine::GraphConfig {
            enabled: true,
            http: ferrosa_graph::http::GraphHttpConfig {
                require_tls: false,
                bind_addr: graph_bind_addr,
                ..ferrosa_graph::http::GraphHttpConfig::default()
            },
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
        runtimes.background.spawn(async move {
            if let Err(e) = ferrosa_graph::http::start_graph_http(&http_config, state).await {
                tracing::error!(%e, "graph HTTP server failed");
            }
        });

        // 10b. Bolt server (port 7687)
        let bolt_port: u16 = config_val(
            "FERROSA_BOLT_PORT",
            &file_config,
            "graph",
            "bolt_port",
            "7687",
        )
        .parse()
        .unwrap_or(7687);
        let bolt_bind: std::net::SocketAddr = std::net::SocketAddr::from(([0, 0, 0, 0], bolt_port));
        let bolt_config = ferrosa_graph::bolt::server::BoltConfig {
            bind_addr: bolt_bind,
            auth_disabled,
            ..ferrosa_graph::bolt::server::BoltConfig::default()
        };
        let bolt_engine = graph_engine;
        let bolt_schema = schema.clone();
        let bolt_shutdown = shutdown_rx.clone();
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
            }
        });
        tracing::info!(%bolt_bind, "Bolt server starting");
    } else {
        // t_2dd438d2: the graph engine is disabled, but instead of leaving the
        // graph HTTP port unbound (clients get an opaque connection-refused, or
        // a misleading missing-table error), serve a thin endpoint that returns
        // a clear "graph engine disabled" error + remediation on every request.
        tracing::info!("graph engine disabled (set FERROSA_GRAPH_ENABLED=true to enable)");
        let disabled_http_config = ferrosa_graph::http::GraphHttpConfig {
            require_tls: false,
            bind_addr: graph_bind_addr,
            ..ferrosa_graph::http::GraphHttpConfig::default()
        };
        runtimes.background.spawn(async move {
            if let Err(e) =
                ferrosa_graph::http::start_graph_disabled_http(&disabled_http_config).await
            {
                tracing::error!(%e, "graph disabled-engine HTTP stub failed");
            }
        });
    }

    // 11. SPARQL endpoint — on by default (t_acc3c7fd); env wins over TOML.
    let sparql_enabled = resolve_sparql_enabled(&file_config, |k| std::env::var(k).ok());

    if sparql_enabled {
        // `[sparql] bind` in the config file wins over `FERROSA_SPARQL_BIND`.
        let sparql_bind_str = config_val(
            "FERROSA_SPARQL_BIND",
            &file_config,
            "sparql",
            "bind",
            "0.0.0.0:8080",
        );
        let sparql_bind: std::net::SocketAddr =
            parse_bind_addr("SPARQL", "FERROSA_SPARQL_BIND", &sparql_bind_str);

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
        };

        runtimes.background.spawn(async move {
            if let Err(e) =
                ferrosa_sparql::http::start_sparql_http(&sparql_config, sparql_state).await
            {
                tracing::error!(%e, "SPARQL HTTP server failed");
            }
        });

        tracing::info!(%sparql_bind, "SPARQL server starting");
    } else {
        tracing::info!("SPARQL server disabled (set FERROSA_SPARQL_ENABLED=true to enable)");
    }

    // 11b. Postgres wire-protocol listener (port 5432). Authenticates real roles
    // via the shared schema's SCRAM verifiers (D4); shares storage/schema with
    // the other front-ends. Query execution is a fail-loud stub until the
    // relational engine (ferrosa-sql) is wired in (M1).
    {
        // `[postgres] bind` in the config file wins over `FERROSA_POSTGRES_BIND`.
        let pg_bind_str = config_val(
            "FERROSA_POSTGRES_BIND",
            &file_config,
            "postgres",
            "bind",
            "0.0.0.0:5432",
        );
        let pg_bind: std::net::SocketAddr =
            parse_bind_addr("Postgres", "FERROSA_POSTGRES_BIND", &pg_bind_str);
        let pg_store =
            std::sync::Arc::new(ferrosa_postgres::SchemaVerifierStore::new(schema.clone()));
        let query_ctx = std::sync::Arc::new(ferrosa_postgres::QueryContext {
            engine: storage.clone(),
            schema: schema.clone(),
            default_schema: "public".into(),
            accord_committer: pg_accord_committer,
        });
        runtimes.background.spawn(async move {
            match tokio::net::TcpListener::bind(pg_bind).await {
                Ok(listener) => {
                    tracing::info!(%pg_bind, "Postgres server listening");
                    if let Err(e) =
                        ferrosa_postgres::server::serve(listener, pg_store, query_ctx).await
                    {
                        tracing::error!(%e, "Postgres server failed");
                    }
                }
                Err(e) => tracing::error!(%e, %pg_bind, "Postgres bind failed"),
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
    let seed_strs: Vec<String> = config_val("FERROSA_SEED", &file_config, "internode", "seed", "")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

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
    let maintenance_engine = storage.clone();
    let maintenance_schema = schema.clone();
    let maintenance_data_dir = data_dir.clone();
    let has_s3 = maintenance_engine.has_s3();
    runtimes.background.spawn(async move {
        let mut flush_interval =
            tokio::time::interval(std::time::Duration::from_secs(flush_interval_secs));
        let mut urgent_flush_interval = tokio::time::interval(std::time::Duration::from_millis(
            urgent_flush_interval_millis.max(1),
        ));
        let mut urgent_s3_sync_interval = tokio::time::interval(std::time::Duration::from_secs(
            urgent_s3_sync_interval_secs.max(1),
        ));
        let mut compact_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        // Persist schema snapshot + flush all memtables to S3 every 30s.
        let mut schema_sync_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut last_schema_version = uuid::Uuid::nil();

        loop {
            tokio::select! {
                _ = flush_interval.tick() => {
                    // Run flush on a dedicated OS thread so SSTable encoding,
                    // compression, and disk I/O do not consume Tokio's shared
                    // blocking pool.
                    let engine = maintenance_engine.clone();
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    match std::thread::Builder::new()
                        .name("storage-flush".into())
                        .spawn(move || {
                            let _ = tx.send(engine.flush_if_needed());
                        }) {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!(%e, "failed to spawn storage flush thread");
                            continue;
                        }
                    }
                    match rx.await {
                        Ok(Err(e)) => tracing::warn!(%e, "periodic flush failed"),
                        Err(e) => tracing::error!(%e, "flush task panicked"),
                        _ => {}
                    }

                    // After flush, sync new SSTables to S3.
                    // Run on a dedicated thread so HTTP uploads don't starve
                    // the main tokio runtime (which handles Raft RPCs).
                    if has_s3 {
                        let engine = maintenance_engine.clone();
                        let _ = std::thread::Builder::new()
                            .name("s3-sync".into())
                            .spawn(move || {
                                let rt = tokio::runtime::Builder::new_current_thread()
                                    .enable_all()
                                    .build()
                                    .expect("s3-sync runtime");
                                rt.block_on(async {
                                    match engine.sync_sstables_to_s3().await {
                                        Ok(n) if n > 0 => {
                                            tracing::info!(count = n, "synced SSTables to S3");
                                        }
                                        Err(e) => {
                                            tracing::warn!(%e, "S3 SSTable sync failed");
                                        }
                                        _ => {}
                                    }
                                });
                            });
                    }

                    // Commit log GC: discard segments with no remaining dirty tables.
                    match maintenance_engine.discard_completed_commit_log_segments() {
                        Ok(n) if n > 0 => {
                            tracing::debug!(segments = n, "commit log GC cleaned up segments");
                        }
                        Err(e) => {
                            tracing::warn!(%e, "commit log GC failed");
                        }
                        _ => {}
                    }
                }
                _ = compact_interval.tick() => {
                    maintenance_engine.poll_compactions().await;
                }
                _ = urgent_flush_interval.tick() => {
                    if maintenance_engine.take_flush_request() {
                        let engine = maintenance_engine.clone();
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        match std::thread::Builder::new()
                            .name("storage-flush-urgent".into())
                            .spawn(move || {
                                let _ = tx.send(engine.flush_if_needed());
                            }) {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(%e, "failed to spawn urgent storage flush thread");
                                continue;
                            }
                        }
                        match rx.await {
                            Ok(Err(e)) => tracing::warn!(%e, "urgent flush failed"),
                            Err(e) => tracing::error!(%e, "urgent flush task panicked"),
                            _ => {}
                        }
                    }
                }
                _ = urgent_s3_sync_interval.tick(), if has_s3 => {
                    if maintenance_engine.take_s3_sync_request() {
                        let engine = maintenance_engine.clone();
                        let _ = std::thread::Builder::new()
                            .name("s3-sync-urgent".into())
                            .spawn(move || {
                                let rt = tokio::runtime::Builder::new_current_thread()
                                    .enable_all()
                                    .build()
                                    .expect("s3-sync-urgent runtime");
                                rt.block_on(async {
                                    match engine.sync_sstables_to_s3().await {
                                        Ok(n) => {
                                            tracing::info!(
                                                count = n,
                                                "urgent S3 SSTable sync completed after write backpressure"
                                            );
                                        }
                                        Err(e) => {
                                            tracing::warn!(%e, "urgent S3 SSTable sync failed");
                                        }
                                    }
                                });
                            });
                    }
                }
                _ = schema_sync_interval.tick() => {
                    let snap = maintenance_schema.snapshot();
                    if snap.version != last_schema_version {
                        // Flush all memtables before persisting schema so SSTables
                        // on disk match the schema snapshot. Run it outside the
                        // Tokio blocking pool for the same reason as periodic
                        // flushes: SSTable compression can be CPU-expensive.
                        let engine = maintenance_engine.clone();
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        match std::thread::Builder::new()
                            .name("storage-schema-flush".into())
                            .spawn(move || {
                                let _ = tx.send(engine.flush_all());
                            }) {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(%e, "failed to spawn schema flush thread");
                                continue;
                            }
                        }
                        match rx.await {
                            Ok(Err(e)) => {
                                tracing::warn!(%e, "pre-schema-persist flush failed, skipping schema persist");
                                continue; // Don't persist schema without data — retry next tick
                            }
                            Err(e) => {
                                tracing::error!(%e, "pre-schema-persist flush task panicked");
                                continue;
                            }
                            _ => {}
                        }

                        // Always persist schema locally for restart recovery.
                        persist_schema_locally(Path::new(&maintenance_data_dir), &maintenance_schema);

                        // Sync to S3 if configured — on a dedicated thread.
                        if has_s3 {
                            let engine = maintenance_engine.clone();
                            let schema_ref = maintenance_schema.clone();
                            let _ = std::thread::Builder::new()
                                .name("s3-schema-sync".into())
                                .spawn(move || {
                                    let rt = tokio::runtime::Builder::new_current_thread()
                                        .enable_all()
                                        .build()
                                        .expect("s3-schema-sync runtime");
                                    rt.block_on(async {
                                        match engine.sync_sstables_to_s3().await {
                                            Ok(n) if n > 0 => {
                                                tracing::info!(count = n, "pre-schema-persist S3 sync");
                                            }
                                            Err(e) => {
                                                tracing::warn!(%e, "pre-schema-persist S3 sync failed");
                                            }
                                            _ => {}
                                        }
                                        persist_schema_to_s3(&engine, &schema_ref).await;
                                    });
                                });
                        }

                        last_schema_version = snap.version;
                    }
                }
            }
        }
    });

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
        persist_schema_locally(Path::new(&data_dir), &schema);
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

    fn empty_config() -> toml::Value {
        toml::Value::Table(toml::map::Map::new())
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
        apply_internode_toml_overrides(&mut cfg, &toml);
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
        apply_internode_toml_overrides(&mut cfg, &toml);
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
        apply_internode_toml_overrides(&mut cfg, &toml);
        assert_eq!(cfg.bind_addr, "127.0.0.1:18003".parse().unwrap());
        assert_eq!(cfg.broadcast_addr, "10.0.0.1:18003".parse().unwrap());
        assert_eq!(cfg.cluster_name, "team-cluster");
        assert_eq!(cfg.psk.as_deref(), Some("abc"));
    }

    /// Invalid bind format in TOML must NOT panic — log and move on.
    #[test]
    fn apply_internode_toml_overrides_invalid_bind_is_ignored() {
        let mut cfg = ferrosa_net::config::NetConfig::default();
        let default_bind = cfg.bind_addr;
        let toml: toml::Value = toml::from_str("[internode]\nbind = \"not-an-addr\"\n").unwrap();
        apply_internode_toml_overrides(&mut cfg, &toml);
        assert_eq!(cfg.bind_addr, default_bind);
    }

    /// Seed list resolves from `[internode] seed` in the config file (so a
    /// committed node config can join a cluster with no env wiring), with
    /// `FERROSA_SEED` as the fallback and TOML winning when both are set. This
    /// mirrors the resolution in `main` (`config_val` + comma-split).
    #[test]
    #[serial_test::serial(env)]
    fn seed_resolves_from_toml_over_env() {
        fn seeds(raw: &str) -> Vec<String> {
            raw.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }

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

        persist_schema_locally(dir.path(), &schema);

        let schema_path = dir.path().join("schema.json");
        assert!(
            schema_path.exists(),
            "schema.json must be written to data_dir"
        );

        let data = std::fs::read(&schema_path).unwrap();
        let restored: ferrosa_schema::SchemaSnapshot = serde_json::from_slice(&data).unwrap();
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

        let loaded = load_local_schema(dir.path());
        assert!(loaded.is_some(), "must load schema.json from data_dir");
        let loaded = loaded.unwrap();
        assert!(loaded.keyspaces.contains_key("my_ks"));
    }

    #[test]
    fn load_local_schema_returns_none_when_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = load_local_schema(dir.path());
        assert!(
            loaded.is_none(),
            "must return None when schema.json is absent"
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

    /// BT-005e: WebConfig::default() — the bind address defaults to 0.0.0.0:9090.
    /// Uses Default impl directly to avoid env var race conditions in parallel tests.
    #[test]
    fn bt005_web_config_default_bind() {
        let wc = crate::web::WebConfig::default();
        assert_eq!(
            wc.bind_addr.to_string(),
            "0.0.0.0:9090",
            "default web bind must be 0.0.0.0:9090"
        );
    }

    /// BT-005f: the web-console bind resolves via `config_val` — `[web] bind`
    /// in the config file wins over `FERROSA_WEB_BIND`, which wins over the
    /// default. (Regression: the old path read only the env var and defaulted
    /// to 0.0.0.0:9090, so co-located nodes collided on 9090.)
    #[test]
    #[serial_test::serial(env)]
    fn bt005_web_bind_resolves_toml_over_env() {
        std::env::remove_var("FERROSA_WEB_BIND");

        // Default when neither source sets it.
        assert_eq!(
            config_val(
                "FERROSA_WEB_BIND",
                &empty_config(),
                "web",
                "bind",
                "0.0.0.0:9090"
            ),
            "0.0.0.0:9090"
        );

        // Env var is the fallback when the config file is silent.
        std::env::set_var("FERROSA_WEB_BIND", "127.0.0.1:8080");
        assert_eq!(
            config_val(
                "FERROSA_WEB_BIND",
                &empty_config(),
                "web",
                "bind",
                "0.0.0.0:9090"
            ),
            "127.0.0.1:8080"
        );

        // Config file wins over the env var.
        let cfg: toml::Value = toml::from_str("[web]\nbind = \"127.0.0.1:19091\"\n").unwrap();
        assert_eq!(
            config_val("FERROSA_WEB_BIND", &cfg, "web", "bind", "0.0.0.0:9090"),
            "127.0.0.1:19091",
            "config file [web] bind must win over FERROSA_WEB_BIND"
        );

        std::env::remove_var("FERROSA_WEB_BIND");
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

    /// BT-005k: load_local_schema returns None for corrupt schema.json.
    #[test]
    fn bt005_load_local_schema_corrupt_json() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("schema.json"), "{{invalid json}}").unwrap();

        let loaded = load_local_schema(dir.path());
        assert!(
            loaded.is_none(),
            "corrupt schema.json must return None, not panic"
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
}
