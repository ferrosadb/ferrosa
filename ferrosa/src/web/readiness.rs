//! Leader-aware readiness probe — `GET /readyz`.
//!
//! Returns `200 OK` with `{"ready":true}` when the node is ready to serve
//! traffic. Returns `503 Service Unavailable` with a JSON body explaining
//! the missing condition otherwise.
//!
//! ## Readiness criteria
//!
//! | Mode       | Condition                                   |
//! |------------|---------------------------------------------|
//! | Standalone | Always ready once the web server is up      |
//! | Pair       | Always ready (mirrors `is_cql_ready()`)     |
//! | Forming    | Ready only once a Raft leader is elected    |
//! | Cluster    | Ready only once a Raft leader is elected    |
//! | Degraded*  | Always ready (stale reads available)        |
//!
//! The probe is intentionally additive — no existing endpoint behavior is
//! changed. It lives outside the `/api/*` auth middleware so external
//! orchestrators (docker-compose, k8s, smoke scripts) can probe it without
//! credentials.
//!
//! ## Fail-loud contract
//!
//! When not ready, the response body names the missing condition explicitly
//! so operators can diagnose the hold-up from logs or a curl:
//!
//! ```json
//! {"ready":false,"waiting_for":"raft_leader"}
//! ```

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use ferrosa_cluster::{DeploymentMode, ModeController};
use serde_json::{json, Value};

use super::WebAppState;

/// Register the `/readyz` route on the given router.
pub fn readiness_route() -> Router<WebAppState> {
    Router::new().route("/readyz", get(readyz_handler))
}

/// `GET /readyz` — leader-aware readiness probe.
///
/// # Standalone / Pair / Degraded modes
/// Returns `200` immediately — these modes serve requests without Raft.
///
/// # Forming / Cluster modes
/// Returns `200` only if a Raft leader is currently known to this node.
/// Otherwise returns `503` with `{"ready":false,"waiting_for":"raft_leader"}`.
pub async fn readyz_handler(State(mc): State<Arc<ModeController>>) -> (StatusCode, Json<Value>) {
    let mode = mc.mode();

    match mode {
        // Standalone always ready: no peers, no Raft.
        DeploymentMode::Standalone => (StatusCode::OK, Json(json!({"ready": true}))),

        // Pair modes: primary accepts connections, degraded mode allows stale reads.
        // Mirror the `is_cql_ready()` logic — if CQL is ready, so is the readiness probe.
        DeploymentMode::Pair | DeploymentMode::DegradedPair | DeploymentMode::DegradedCluster => {
            (StatusCode::OK, Json(json!({"ready": true})))
        }

        // Forming / Cluster: gate on Raft leader presence.
        DeploymentMode::Forming | DeploymentMode::Cluster => {
            match mc.raft() {
                None => {
                    // Raft instance not yet installed — still initializing.
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({
                            "ready": false,
                            "waiting_for": "raft_leader",
                            "detail": "raft not yet initialized"
                        })),
                    )
                }
                Some(raft) => {
                    let leader = raft.current_leader().await;
                    if leader.is_some() {
                        (StatusCode::OK, Json(json!({"ready": true})))
                    } else {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({
                                "ready": false,
                                "waiting_for": "raft_leader",
                                "detail": "no raft leader elected yet"
                            })),
                        )
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use ferrosa_cluster::ModeController;
    use ferrosa_net::rpc::HandlerRegistry;
    use ferrosa_storage::commitlog::CommitLogConfig;
    use ferrosa_storage::compaction::CompactionConfig;
    use ferrosa_storage::{StorageEngine, StorageEngineConfig};
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::web::{build_router, WebAppState};

    fn make_state() -> WebAppState {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage_config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.path().join("commitlog"),
                checkpoint_dir: dir.path().join("commitlog"),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        let storage = Arc::new(StorageEngine::new(storage_config, None).expect("storage engine"));
        let registry = Arc::new(HandlerRegistry::new());
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
            .expect("test schema"),
        );
        let host_id = uuid::Uuid::new_v4();
        let (mc, _handles) = ModeController::new(
            Arc::new(ferrosa_cluster::ClusterConfig::default()),
            Arc::new(ferrosa_net::config::NetConfig::default()),
            host_id,
            storage.clone(),
            schema.clone(),
            registry,
        );
        WebAppState {
            registry: Arc::new(ferrosa_schema::VirtualTableRegistry::new()),
            mode_controller: mc,
            schema,
            storage,
            host_id,
            auth_disabled: true,
            debug: None,
        }
    }

    // -------------------------------------------------------------------------
    // Red tests (written first — these fail before the route is wired up)
    // -------------------------------------------------------------------------

    /// `/readyz` must be routable — not a 404.
    #[tokio::test]
    async fn readyz_endpoint_is_routable() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "GET /readyz must not return 404"
        );
    }

    /// Standalone mode (the default for a new `ModeController`) must return 200.
    #[tokio::test]
    async fn readyz_standalone_returns_200() {
        let state = make_state();
        // ModeController starts in Standalone mode.
        assert_eq!(state.mode_controller.mode(), DeploymentMode::Standalone);

        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Standalone mode response body must be `{"ready":true}`.
    #[tokio::test]
    async fn readyz_standalone_body_is_ready_true() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            parsed["ready"], true,
            "standalone node must report ready=true"
        );
    }

    /// `/readyz` must return valid JSON in all cases.
    #[tokio::test]
    async fn readyz_returns_valid_json() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&body);
        assert!(
            parsed.is_ok(),
            "GET /readyz must return valid JSON, got: {}",
            String::from_utf8_lossy(&body)
        );
    }

    /// The Forming mode (no Raft instance installed) must return 503.
    #[tokio::test]
    async fn readyz_forming_without_raft_returns_503() {
        let state = make_state();
        // Use the test-only helper to drive the mode into Forming.
        state
            .mode_controller
            .set_mode_for_test(DeploymentMode::Forming);
        // No Raft instance installed — raft() returns None.
        assert!(state.mode_controller.raft().is_none());

        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "Forming mode with no Raft must return 503"
        );
    }

    /// The Forming mode (no Raft instance) response body must name the missing condition.
    #[tokio::test]
    async fn readyz_forming_without_raft_body_names_waiting_for() {
        let state = make_state();
        state
            .mode_controller
            .set_mode_for_test(DeploymentMode::Forming);

        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["ready"], false);
        assert_eq!(
            parsed["waiting_for"], "raft_leader",
            "response must name 'raft_leader' as the missing condition"
        );
    }

    /// The Cluster mode (no Raft instance installed yet) must return 503.
    #[tokio::test]
    async fn readyz_cluster_without_raft_returns_503() {
        let state = make_state();
        state
            .mode_controller
            .set_mode_for_test(DeploymentMode::Cluster);

        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "Cluster mode with no Raft must return 503"
        );
    }
}
