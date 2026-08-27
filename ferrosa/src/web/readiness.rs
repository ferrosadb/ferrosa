//! Leader-aware readiness probe — `GET /readyz`.
//!
//! Returns `200 OK` with `{"ready":true}` when the node is ready to serve
//! traffic. Returns `503 Service Unavailable` with a JSON body explaining
//! the missing condition otherwise.
//! A failed consensus runtime overrides every deployment-mode shortcut and
//! returns 503 without awaiting a Raft handle.
//! Last revised: 2026-08-27
//! Last changed: Added the immediate consensus-runtime failure gate.
//!
//! ## Readiness criteria
//!
//! | Mode       | Condition                                   |
//! |------------|---------------------------------------------|
//! | Standalone | Ready unless consensus supervision failed   |
//! | Pair       | Ready unless consensus supervision failed   |
//! | Forming    | Ready only once a Raft leader is elected    |
//! | Cluster    | Ready only once a Raft leader is elected    |
//! | Degraded*  | Mode rules apply unless consensus failed    |
//!
//! It lives outside the `/api/*` auth middleware so external
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

/// Register the readiness routes on the given router.
///
/// Both `/readyz` and `/health` are wired to the same leader-aware handler.
/// `/health` is an alias kept for orchestrator probes (docker-compose
/// healthchecks, the Jepsen multi-DC bring-up workflow, k8s) that historically
/// expect a `/health` path. Before this alias existed, those probes hit the
/// static-file fallback and always received `404`, so the healthchecks were a
/// no-op (they could never gate on the cluster actually forming). Routing
/// `/health` through the readiness handler makes the bring-up fail-loud: in
/// Forming/Cluster mode it returns `503` until a Raft leader is elected.
pub fn readiness_route() -> Router<WebAppState> {
    Router::new()
        .route("/readyz", get(readyz_handler))
        .route("/health", get(readyz_handler))
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
    if !mc.consensus_is_healthy() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "waiting_for": "consensus_runtime",
                "detail": "consensus runtime failed; retry another node"
            })),
        );
    }
    let mode = mc.mode();

    match mode {
        // Standalone always ready: no peers, no Raft.
        DeploymentMode::Standalone => (StatusCode::OK, Json(json!({"ready": true}))),

        // Pair modes: primary accepts connections, degraded pair allows stale
        // reads. Mirrors `is_cql_ready()` — if CQL is ready, so is the probe.
        DeploymentMode::Pair | DeploymentMode::DegradedPair => {
            (StatusCode::OK, Json(json!({"ready": true})))
        }

        // A degraded CLUSTER is not ready, and grouping it with the pair modes
        // above is what made the 2026-08-20 outage invisible. node1 sat outside
        // the cluster for hours -- no Raft handler, no schema, answering
        // `keyspace 'agent_memory' not found` to every query -- while this
        // endpoint returned 200 {"ready":true} throughout. Every health check
        // believed it, so nothing routed away and nobody was paged; it was
        // found by a person noticing their task board was down.
        //
        // A degraded cluster member is a member WITHOUT quorum. It cannot serve
        // a consistent read, so reporting ready makes it indistinguishable from
        // a healthy member -- exactly the distinction a readiness probe exists
        // to draw. A degraded PAIR is different and stays ready: that shape has
        // no quorum to lose and its stale-read behaviour is deliberate.
        DeploymentMode::DegradedCluster => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "mode": "degraded-cluster",
                "waiting_for": "raft_quorum",
                "detail": "this node is a cluster member without quorum; it cannot \
            serve consistent reads until the quorum is restored"
            })),
        ),

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

    /// `/health` is an alias of `/readyz` and must be routable — not a 404.
    /// This is what the docker-compose healthchecks and the Jepsen multi-DC
    /// bring-up workflow probe; before the alias existed it hit the static
    /// fallback and 404'd, making those probes a silent no-op.
    #[tokio::test]
    async fn health_alias_is_routable() {
        let state = make_state();
        let router = build_router(state);
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "GET /health must not return 404 — orchestrators probe it"
        );
    }

    /// `/health` must behave identically to `/readyz`: standalone returns 200.
    #[tokio::test]
    async fn health_alias_standalone_returns_200() {
        let state = make_state();
        assert_eq!(state.mode_controller.mode(), DeploymentMode::Standalone);
        let router = build_router(state);
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
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

    /// A dead consensus lane overrides deployment mode and returns immediately.
    /// Standalone is intentional here: the handler must consult the health gate
    /// before any mode shortcut or Raft-handle await.
    #[tokio::test]
    async fn readyz_consensus_failure_is_immediate_503() {
        let state = make_state();
        state.mode_controller.consensus_health().fail(
            "raft-runtime-panic",
            format_args!("raft_core.rs:769 empty apply window"),
        );
        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();

        let resp = tokio::time::timeout(std::time::Duration::from_millis(100), router.oneshot(req))
            .await
            .expect("failed readiness must not wait on a dead Raft handle")
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["ready"], false);
        assert_eq!(parsed["waiting_for"], "consensus_runtime");
        assert_eq!(
            parsed["detail"],
            "consensus runtime failed; retry another node"
        );
        assert!(
            !String::from_utf8_lossy(&body).contains("raft_core.rs"),
            "internal panic details belong in bounded server logs, not health responses"
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

    /// A degraded cluster member must NOT report itself ready.
    ///
    /// This is why the 2026-08-20 outage was silent. node1 sat outside the
    /// cluster for hours -- no Raft handler, no schema, answering
    /// `keyspace 'agent_memory' not found` to every query -- and `/readyz`
    /// returned 200 `{"ready":true}` the entire time, because DegradedCluster
    /// was grouped with the pair modes and answered unconditionally.
    ///
    /// Every health check believed it. A load balancer would have kept routing
    /// to it; an orchestrator would not have restarted it; nobody was paged.
    /// The node was found by a person noticing their task board was down.
    ///
    /// A degraded cluster member is a member WITHOUT quorum. It cannot serve a
    /// consistent read, so reporting ready makes it indistinguishable from a
    /// healthy member -- which is precisely the distinction a readiness probe
    /// exists to draw.
    #[tokio::test]
    async fn readyz_degraded_cluster_is_not_ready() {
        let state = make_state();
        state
            .mode_controller
            .set_mode_for_test(DeploymentMode::DegradedCluster);

        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a cluster member without quorum must not report ready; saying so is \
what made this failure invisible"
        );
    }

    /// The 503 must say what is wrong, not merely refuse.
    ///
    /// An operator reading `{"ready":false}` learns nothing actionable. The
    /// whole cost of this outage was diagnosis time, so the probe names the
    /// state and what it is waiting for.
    #[tokio::test]
    async fn readyz_degraded_cluster_says_why() {
        let state = make_state();
        state
            .mode_controller
            .set_mode_for_test(DeploymentMode::DegradedCluster);

        let router = build_router(state);
        let req = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .expect("body");
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("quorum"),
            "the reason must name quorum so an operator knows what to look at: {text}"
        );
        assert!(
            text.contains("degraded-cluster"),
            "and the mode it is actually in: {text}"
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
