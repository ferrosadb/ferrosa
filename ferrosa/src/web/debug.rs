//! On-demand flame chart profiling endpoint (T-17: O3.6).
//!
//! `GET /api/debug/flamechart?seconds=N` captures tracing span data for N
//! seconds (capped at 60), renders an SVG flame chart via `inferno`, and
//! returns it as `image/svg+xml`.
//!
//! Guards:
//! - Admin auth token via `FERROSA_DEBUG_AUTH_TOKEN` env var.
//! - Optional IP whitelist via `FERROSA_DEBUG_IP_WHITELIST` (comma-separated).
//! - Mutex to prevent concurrent profiling (returns 429 if busy).

use std::io::BufWriter;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::WebAppState;

/// Maximum profiling duration in seconds.
const MAX_SECONDS: u64 = 60;

/// Maximum collected folded-stack data before we stop (64 MB).
const MAX_COLLECT_BYTES: usize = 64 * 1024 * 1024;

/// Query parameters for the flamechart endpoint.
#[derive(Debug, Deserialize)]
pub struct FlamechartParams {
    /// Duration in seconds to profile.
    seconds: Option<u64>,
}

/// Shared state for the debug profiler — one profiling session at a time.
#[derive(Clone)]
pub struct DebugState {
    /// Mutex to enforce single concurrent profiling session.
    profiling_lock: Arc<Mutex<()>>,
}

impl DebugState {
    pub fn new() -> Self {
        Self {
            profiling_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl Default for DebugState {
    fn default() -> Self {
        Self::new()
    }
}

/// Build debug routes. Must be mounted under `/api/debug`.
pub fn debug_routes() -> Router<WebAppState> {
    Router::new()
        .route("/flamechart", get(flamechart_handler))
        .route("/force-compact", axum::routing::post(force_compact_handler))
}

/// Force compaction for all tables. Useful for debugging compaction issues.
/// POST /api/debug/force-compact
async fn force_compact_handler(State(state): State<WebAppState>) -> impl IntoResponse {
    state.storage.force_compact_all();
    // Give compaction a moment to start, then poll results
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    state.storage.poll_compactions().await;
    (StatusCode::OK, "compaction triggered and polled\n")
}

/// Check the admin auth token from the `Authorization: Bearer <token>` header.
#[allow(clippy::result_large_err)]
fn check_auth(headers: &axum::http::HeaderMap) -> Result<(), Response> {
    let expected = match std::env::var("FERROSA_DEBUG_AUTH_TOKEN") {
        Ok(tok) if !tok.is_empty() => tok,
        _ => {
            return Err((
                StatusCode::FORBIDDEN,
                "FERROSA_DEBUG_AUTH_TOKEN not configured",
            )
                .into_response());
        }
    };

    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let provided = auth_header.strip_prefix("Bearer ").unwrap_or("");
    if provided != expected {
        return Err((StatusCode::UNAUTHORIZED, "invalid debug auth token").into_response());
    }

    Ok(())
}

/// Check IP whitelist if `FERROSA_DEBUG_IP_WHITELIST` is set.
#[allow(clippy::result_large_err)]
fn check_ip_whitelist(remote_ip: Option<IpAddr>) -> Result<(), Response> {
    let whitelist = match std::env::var("FERROSA_DEBUG_IP_WHITELIST") {
        Ok(val) if !val.is_empty() => val,
        _ => return Ok(()), // No whitelist configured — allow all.
    };

    let allowed: Vec<IpAddr> = whitelist
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    if allowed.is_empty() {
        return Ok(());
    }

    let remote = remote_ip.unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    if !allowed.contains(&remote) {
        return Err((StatusCode::FORBIDDEN, "IP not in debug whitelist").into_response());
    }

    Ok(())
}

/// Handler for `GET /api/debug/flamechart?seconds=N`.
///
/// Generates a synthetic flame chart from a short profiling window. In this
/// v1 implementation, we collect active span names during the profiling
/// window and render a basic flame graph.
async fn flamechart_handler(
    headers: axum::http::HeaderMap,
    Query(params): Query<FlamechartParams>,
    State(state): State<WebAppState>,
) -> Response {
    // Auth checks
    if let Err(resp) = check_auth(&headers) {
        return resp;
    }
    // IP whitelist check — skipped when ConnectInfo is unavailable.
    if let Err(resp) = check_ip_whitelist(None) {
        return resp;
    }

    let seconds = params.seconds.unwrap_or(5).min(MAX_SECONDS);

    // Try to acquire the profiling lock (non-blocking).
    let debug_state = state
        .debug
        .as_ref()
        .cloned()
        .unwrap_or_else(DebugState::new);

    let guard = match debug_state.profiling_lock.try_lock() {
        Ok(g) => g,
        Err(_) => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                "profiling already in progress",
            )
                .into_response();
        }
    };

    // Collect span data during the profiling window.
    // For v1, we generate a basic folded-stack sample based on active queries.
    let folded_stacks = collect_span_samples(seconds, &state).await;

    drop(guard);

    // Render SVG via inferno.
    match render_flamegraph(&folded_stacks) {
        Ok(svg) => (StatusCode::OK, [("content-type", "image/svg+xml")], svg).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to render flame chart: {e}"),
        )
            .into_response(),
    }
}

/// Collect folded stack samples for `seconds` by polling active queries.
async fn collect_span_samples(seconds: u64, state: &WebAppState) -> Vec<String> {
    let mut stacks = Vec::new();
    let poll_interval = Duration::from_millis(100);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);

    while tokio::time::Instant::now() < deadline && stacks.len() < MAX_COLLECT_BYTES / 64 {
        // Sample virtual tables for current activity.
        if let Some(table) = state.registry.get("system_observability", "active_queries") {
            let rows = table.read(None);
            for row in &rows {
                // Row layout: query_id, client_address, username, query_text, keyspace,
                // start_time, elapsed_ms, state
                if row.cells.len() >= 5 {
                    let keyspace = row.cells[4]
                        .value
                        .as_deref()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    let query = row.cells[3]
                        .value
                        .as_deref()
                        .map(|b| {
                            let s = String::from_utf8_lossy(b);
                            // Truncate for display
                            if s.len() > 40 {
                                format!("{}...", &s[..40])
                            } else {
                                s.to_string()
                            }
                        })
                        .unwrap_or_else(|| "?".to_string());
                    stacks.push(format!("ferrosa;cql;{keyspace};{query} 1"));
                }
            }
        }

        // Also sample connection count.
        if let Some(table) = state.registry.get("system_observability", "connections") {
            let count = table.read(None).len();
            if count > 0 {
                stacks.push(format!("ferrosa;connections;active {count}"));
            }
        }

        tokio::time::sleep(poll_interval).await;
    }

    // If no real data collected, add a placeholder so inferno doesn't error.
    if stacks.is_empty() {
        stacks.push("ferrosa;idle 1".to_string());
    }

    stacks
}

/// Render folded stacks into an SVG flame graph using inferno.
fn render_flamegraph(folded_stacks: &[String]) -> Result<Vec<u8>, String> {
    let input = folded_stacks.join("\n");
    let mut opts = inferno::flamegraph::Options::default();
    opts.title = "Ferrosa Flame Chart".to_string();
    opts.count_name = "samples".to_string();

    let mut svg_output = Vec::new();
    {
        let mut writer = BufWriter::new(&mut svg_output);
        inferno::flamegraph::from_reader(
            &mut opts,
            std::io::Cursor::new(input.as_bytes()),
            &mut writer,
        )
        .map_err(|e| format!("inferno error: {e}"))?;
    }

    Ok(svg_output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flamechart_endpoint_requires_auth() {
        // Verify check_auth rejects requests when no token is set.
        // Safety: test-only env var manipulation.
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
        let headers = axum::http::HeaderMap::new();
        let result = check_auth(&headers);
        assert!(result.is_err(), "should reject when no token configured");
    }

    #[test]
    #[serial_test::serial(env)]
    fn flamechart_auth_accepts_valid_token() {
        // Safety: test-only env var manipulation.
        unsafe {
            std::env::set_var("FERROSA_DEBUG_AUTH_TOKEN", "test-secret-42");
        }
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer test-secret-42".parse().unwrap(),
        );
        let result = check_auth(&headers);
        assert!(result.is_ok(), "should accept valid token");

        // Cleanup
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
    }

    #[test]
    #[serial_test::serial(env)]
    fn flamechart_auth_rejects_wrong_token() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_AUTH_TOKEN", "correct-token");
        }
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer wrong-token".parse().unwrap(),
        );
        let result = check_auth(&headers);
        assert!(result.is_err(), "should reject wrong token");

        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
    }

    #[test]
    fn ip_whitelist_allows_when_not_set() {
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_IP_WHITELIST");
        }
        let result = check_ip_whitelist(Some("192.168.1.1".parse().unwrap()));
        assert!(result.is_ok());
    }

    #[test]
    fn render_flamegraph_produces_svg() {
        let stacks = vec![
            "ferrosa;cql;myks;SELECT 10".to_string(),
            "ferrosa;cql;myks;INSERT 5".to_string(),
            "ferrosa;idle 1".to_string(),
        ];
        let svg = render_flamegraph(&stacks).expect("should render SVG");
        let svg_str = String::from_utf8_lossy(&svg);
        assert!(svg_str.contains("<svg"), "output should be SVG");
        assert!(svg_str.contains("ferrosa"), "should contain stack names");
    }

    #[test]
    fn debug_state_default() {
        let state = DebugState::default();
        // Should be able to acquire lock.
        let lock = state.profiling_lock.try_lock();
        assert!(lock.is_ok());
    }

    // -----------------------------------------------------------------------
    // DebugState — profiling lock semantics
    // -----------------------------------------------------------------------

    #[test]
    fn debug_state_lock_is_exclusive() {
        let state = DebugState::new();
        let guard1 = state.profiling_lock.try_lock();
        assert!(guard1.is_ok(), "first lock acquisition should succeed");
        // While guard1 is held, a second try_lock should fail.
        let guard2 = state.profiling_lock.try_lock();
        assert!(
            guard2.is_err(),
            "second lock acquisition should fail while first is held"
        );
    }

    #[test]
    fn debug_state_lock_released_after_drop() {
        let state = DebugState::new();
        {
            let _guard = state.profiling_lock.try_lock().expect("first lock");
            // guard goes out of scope here
        }
        let guard2 = state.profiling_lock.try_lock();
        assert!(
            guard2.is_ok(),
            "lock should be available after guard is dropped"
        );
    }

    #[test]
    fn debug_state_clone_shares_lock() {
        let state1 = DebugState::new();
        let state2 = state1.clone();
        let _guard = state1.profiling_lock.try_lock().expect("lock via state1");
        // Clone shares the Arc, so lock should be held via state2 too.
        let result = state2.profiling_lock.try_lock();
        assert!(
            result.is_err(),
            "cloned DebugState should share the same profiling lock"
        );
    }

    // -----------------------------------------------------------------------
    // FlamechartParams deserialization
    // -----------------------------------------------------------------------

    #[test]
    fn flamechart_params_default_seconds() {
        let params: FlamechartParams = serde_json::from_str("{}").expect("empty object");
        assert!(
            params.seconds.is_none(),
            "seconds should be None when omitted"
        );
    }

    #[test]
    fn flamechart_params_with_seconds() {
        let params: FlamechartParams =
            serde_json::from_str(r#"{"seconds": 30}"#).expect("valid params");
        assert_eq!(params.seconds, Some(30));
    }

    #[test]
    fn flamechart_params_seconds_capped_at_max() {
        let params: FlamechartParams =
            serde_json::from_str(r#"{"seconds": 120}"#).expect("valid params");
        // The params struct stores the raw value; capping happens in the handler.
        assert_eq!(params.seconds, Some(120));
        // Verify the handler logic: unwrap_or(5).min(MAX_SECONDS)
        let capped = params.seconds.unwrap_or(5).min(MAX_SECONDS);
        assert_eq!(capped, MAX_SECONDS, "should be capped at MAX_SECONDS");
    }

    // -----------------------------------------------------------------------
    // check_auth — additional edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn check_auth_rejects_empty_bearer_token() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_AUTH_TOKEN", "nonempty");
        }
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer ".parse().unwrap(),
        );
        let result = check_auth(&headers);
        assert!(result.is_err(), "empty Bearer token should be rejected");
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
    }

    #[test]
    fn check_auth_rejects_missing_authorization_header() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_AUTH_TOKEN", "secret");
        }
        let headers = axum::http::HeaderMap::new();
        let result = check_auth(&headers);
        assert!(
            result.is_err(),
            "missing Authorization header should be rejected"
        );
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
    }

    #[test]
    fn check_auth_rejects_non_bearer_scheme() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_AUTH_TOKEN", "secret");
        }
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Basic secret".parse().unwrap(),
        );
        let result = check_auth(&headers);
        assert!(result.is_err(), "non-Bearer auth scheme should be rejected");
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
    }

    #[test]
    fn check_auth_rejects_empty_env_token() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_AUTH_TOKEN", "");
        }
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer anything".parse().unwrap(),
        );
        let result = check_auth(&headers);
        assert!(
            result.is_err(),
            "empty FERROSA_DEBUG_AUTH_TOKEN should be treated as unconfigured"
        );
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
    }

    // -----------------------------------------------------------------------
    // check_ip_whitelist — additional edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn ip_whitelist_allows_listed_ip() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_IP_WHITELIST", "10.0.0.1,192.168.1.1");
        }
        let result = check_ip_whitelist(Some("192.168.1.1".parse().unwrap()));
        assert!(result.is_ok(), "listed IP should be allowed");
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_IP_WHITELIST");
        }
    }

    #[test]
    fn ip_whitelist_rejects_unlisted_ip() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_IP_WHITELIST", "10.0.0.1");
        }
        let result = check_ip_whitelist(Some("10.0.0.2".parse().unwrap()));
        assert!(result.is_err(), "unlisted IP should be rejected");
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_IP_WHITELIST");
        }
    }

    #[test]
    fn ip_whitelist_rejects_none_remote_when_configured() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_IP_WHITELIST", "10.0.0.1");
        }
        // None remote IP => 0.0.0.0, which is not in the whitelist.
        let result = check_ip_whitelist(None);
        assert!(
            result.is_err(),
            "None remote IP should be rejected when whitelist is configured"
        );
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_IP_WHITELIST");
        }
    }

    #[test]
    fn ip_whitelist_allows_all_when_empty_string() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_IP_WHITELIST", "");
        }
        let result = check_ip_whitelist(Some("1.2.3.4".parse().unwrap()));
        assert!(
            result.is_ok(),
            "empty whitelist string should allow all IPs"
        );
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_IP_WHITELIST");
        }
    }

    #[test]
    fn ip_whitelist_handles_whitespace_in_entries() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_IP_WHITELIST", " 10.0.0.1 , 10.0.0.2 ");
        }
        let result = check_ip_whitelist(Some("10.0.0.2".parse().unwrap()));
        assert!(
            result.is_ok(),
            "whitespace-padded IP entries should be parsed correctly"
        );
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_IP_WHITELIST");
        }
    }

    #[test]
    fn ip_whitelist_allows_when_only_invalid_entries() {
        unsafe {
            std::env::set_var("FERROSA_DEBUG_IP_WHITELIST", "not-an-ip,also-bad");
        }
        // All entries are unparsable, so allowed list is empty => allow all.
        let result = check_ip_whitelist(Some("1.2.3.4".parse().unwrap()));
        assert!(
            result.is_ok(),
            "unparsable entries should result in empty allowed list (allow all)"
        );
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_IP_WHITELIST");
        }
    }

    // -----------------------------------------------------------------------
    // render_flamegraph — additional edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn render_flamegraph_single_idle_stack() {
        let stacks = vec!["ferrosa;idle 1".to_string()];
        let svg = render_flamegraph(&stacks).expect("should render SVG from single stack");
        let svg_str = String::from_utf8_lossy(&svg);
        assert!(
            svg_str.contains("<svg"),
            "output should contain SVG element"
        );
    }

    #[test]
    fn render_flamegraph_svg_has_title() {
        let stacks = vec!["ferrosa;cql;ks;SELECT 5".to_string()];
        let svg = render_flamegraph(&stacks).expect("should render");
        let svg_str = String::from_utf8_lossy(&svg);
        assert!(
            svg_str.contains("Ferrosa Flame Chart"),
            "SVG should contain the configured title"
        );
    }

    #[test]
    fn render_flamegraph_empty_input_produces_error_or_svg() {
        // Empty folded stacks: inferno may error or produce a minimal SVG.
        // Either is acceptable — we just verify no panic.
        let stacks: Vec<String> = vec![];
        let _ = render_flamegraph(&stacks);
    }

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    #[test]
    fn max_seconds_is_reasonable() {
        const {
            assert!(MAX_SECONDS > 0, "MAX_SECONDS must be positive");
            assert!(
                MAX_SECONDS <= 300,
                "MAX_SECONDS should be bounded to prevent extremely long profiling"
            );
        }
    }

    #[test]
    fn max_collect_bytes_is_reasonable() {
        const {
            assert!(
                MAX_COLLECT_BYTES >= 1024 * 1024,
                "MAX_COLLECT_BYTES should be at least 1 MB"
            );
            assert!(
                MAX_COLLECT_BYTES <= 256 * 1024 * 1024,
                "MAX_COLLECT_BYTES should not exceed 256 MB"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Route-level tests — GET /api/debug/flamechart
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn flamechart_endpoint_is_routable() {
        let state = make_state();
        let router = crate::web::build_router(state);
        let req = axum::http::Request::builder()
            .uri("/api/debug/flamechart")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = tower::ServiceExt::oneshot(router, req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "GET /api/debug/flamechart must not return 404"
        );
    }

    #[tokio::test]
    async fn flamechart_rejects_without_auth_token() {
        let state = make_state();
        let router = crate::web::build_router(state);
        // Ensure no debug auth token is set.
        unsafe {
            std::env::remove_var("FERROSA_DEBUG_AUTH_TOKEN");
        }
        let req = axum::http::Request::builder()
            .uri("/api/debug/flamechart")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = tower::ServiceExt::oneshot(router, req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "should return 403 when FERROSA_DEBUG_AUTH_TOKEN is not configured"
        );
    }

    /// Build a minimal `WebAppState` for debug tests.
    fn make_state() -> crate::web::WebAppState {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage_config = ferrosa_storage::StorageEngineConfig {
            commit_log: ferrosa_storage::commitlog::CommitLogConfig {
                log_dir: dir.path().join("commitlog"),
                checkpoint_dir: dir.path().join("commitlog"),
                archive: None,
                ..ferrosa_storage::commitlog::CommitLogConfig::default()
            },
            compaction: ferrosa_storage::compaction::CompactionConfig::from_env(
                dir.path().join("compaction"),
            ),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        };
        let storage =
            Arc::new(ferrosa_storage::StorageEngine::new(storage_config, None).expect("storage"));
        let rpc_registry = Arc::new(ferrosa_net::rpc::HandlerRegistry::new());
        let schema = Arc::new(
            ferrosa_schema::Schema::new(ferrosa_schema::SchemaConfig {
                hasher: ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 },
                password_policy: ferrosa_schema::PasswordPolicy::permissive(),
                auth_method: ferrosa_schema::AuthMethod::Password,
                rate_limit: ferrosa_schema::RateLimitConfig::default(),
                audit_sink: Box::new(ferrosa_schema::TestAuditSink::new()),
                secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                mode: ferrosa_schema::DeploymentMode::Development,
            })
            .expect("schema"),
        );
        let host_id = uuid::Uuid::new_v4();
        let (mc, _) = ferrosa_cluster::ModeController::new(
            Arc::new(ferrosa_cluster::ClusterConfig::default()),
            Arc::new(ferrosa_net::config::NetConfig::default()),
            host_id,
            storage.clone(),
            schema.clone(),
            rpc_registry,
        );
        crate::web::WebAppState {
            registry: Arc::new(ferrosa_schema::VirtualTableRegistry::new()),
            mode_controller: mc,
            schema,
            storage,
            host_id,
            auth_disabled: true,
            debug: Some(DebugState::new()),
        }
    }
}
