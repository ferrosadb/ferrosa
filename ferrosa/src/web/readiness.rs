//! Leader-aware readiness probe — `GET /readyz`.
//!
//! Returns `200 OK` with `{"ready":true}` when the node is ready to serve
//! traffic. Returns `503 Service Unavailable` with a JSON body explaining
//! the missing condition otherwise.
//! A failed consensus runtime overrides every deployment-mode shortcut and
//! returns 503 without awaiting a Raft handle.
//! Last revised: 2026-09-26
//! Last changed: A node that declared `FERROSA_EXPECTED_CLUSTER_SIZE` is not ready
//!   until that topology is met (a booting Standalone pod and a cluster that fell
//!   back to Pair used to answer 200 while CQL refused connections).
//!
//! ## Readiness criteria
//!
//! When `FERROSA_EXPECTED_CLUSTER_SIZE` is set, the mode rules below apply only
//! after the declared topology is met (the same gate CQL uses); until then the
//! answer is `503 {"waiting_for":"declared_topology"}`. With no declared size the
//! table is unchanged.
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

/// Stable, lowercase name of a deployment mode for probe bodies.
fn mode_label(mode: DeploymentMode) -> &'static str {
    match mode {
        DeploymentMode::Standalone => "standalone",
        DeploymentMode::Pair => "pair",
        DeploymentMode::DegradedPair => "degraded-pair",
        DeploymentMode::Forming => "forming",
        DeploymentMode::Cluster => "cluster",
        DeploymentMode::DegradedCluster => "degraded-cluster",
    }
}

/// `GET /readyz` — leader-aware readiness probe.
///
/// # Standalone / Pair / Degraded modes
/// Returns `200` immediately — these modes serve requests without Raft.
///
/// # Forming / Cluster modes
/// Returns `200` only if a Raft leader is currently known to this node.
/// Otherwise returns `503` with `{"ready":false,"waiting_for":"raft_leader"}`.
pub async fn readyz_handler(
    State(mc): State<Arc<ModeController>>,
    State(listeners): State<Arc<crate::listener_status::ListenerStatus>>,
    State(storage): State<Arc<ferrosa_storage::StorageEngine>>,
) -> (StatusCode, Json<Value>) {
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
    // A background client listener (Postgres, SPARQL, graph HTTP, Bolt) that failed
    // to bind was only an ERROR log line; the node kept probing ready with a client
    // port missing. Name the failed listeners instead.
    let failed_listeners = listeners.failed();
    if !failed_listeners.is_empty() {
        let failed: Vec<Value> = failed_listeners
            .into_iter()
            .map(|(listener, reason)| json!({"listener": listener, "reason": reason}))
            .collect();
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "waiting_for": "listeners",
                "failed": failed,
                "detail": "a client listener failed to start; see the reasons and the \
                    ferrosa_listener_up metric"
            })),
        );
    }
    // Commit-log mutations startup replay could not bind to a table schema are
    // durable on disk but in no memtable, so reads cannot see them. Answering 200
    // would make a node holding invisible rows indistinguishable from a healthy
    // one. They are re-ingested automatically once the table's schema registers.
    let set_aside = storage.replay_set_aside_status();
    if !set_aside.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "waiting_for": "set_aside_mutations",
                "mutations": set_aside.mutations(),
                "tables": set_aside.tables(),
                "unreadable_files": set_aside
                    .unreadable()
                    .into_iter()
                    .map(|f| json!({"path": f.path.display().to_string(), "error": f.error}))
                    .collect::<Vec<Value>>(),
                "detail": "committed mutations are set aside on disk and invisible to reads \
                    until their table schema is registered; inspect with \
                    `ferrosa-ctl commitlog set-aside` and see the ferrosa_commitlog_replay_\
                    set_aside_pending_mutations metric"
            })),
        );
    }
    let mode = mc.mode();

    // A node that declared its cluster size refuses CQL connections until that
    // topology is met, so it must not probe ready before then: a booting pod is
    // Standalone, and a cluster that missed its formation timeout falls back to
    // Pair, and both used to answer 200 below. Undeclared nodes (size 0) and a
    // degraded cluster (which has its own, more specific answer) are unchanged.
    let expected = mc.expected_cluster_size();
    if expected > 0 && mode != DeploymentMode::DegradedCluster && !mc.accepts_cql_connections() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "waiting_for": "declared_topology",
                "expected_cluster_size": expected,
                "mode": mode_label(mode),
                "detail": "this node declared an expected cluster size and the \
                    topology is not formed yet; CQL connections are refused"
            })),
        );
    }

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
        make_state_with_cluster_config(ferrosa_cluster::ClusterConfig::default())
    }

    /// State whose node declared `FERROSA_EXPECTED_CLUSTER_SIZE = expected`.
    fn make_state_expecting(expected: usize) -> WebAppState {
        make_state_with_cluster_config(ferrosa_cluster::ClusterConfig {
            expected_cluster_size: expected,
            ..ferrosa_cluster::ClusterConfig::default()
        })
    }

    fn make_state_with_cluster_config(
        cluster_config: ferrosa_cluster::ClusterConfig,
    ) -> WebAppState {
        let dir = tempfile::tempdir().expect("tempdir");
        make_state_in(dir.path(), cluster_config)
    }

    /// State over the storage data dir `data_dir`, which may already hold files
    /// (the engine looks for set-aside files while it is constructed).
    fn make_state_in(
        data_dir: &std::path::Path,
        cluster_config: ferrosa_cluster::ClusterConfig,
    ) -> WebAppState {
        let dir = data_dir;
        let storage_config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.join("commitlog"),
                checkpoint_dir: dir.join("commitlog"),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
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
            Arc::new(cluster_config),
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
            listeners: std::sync::Arc::new(crate::listener_status::ListenerStatus::default()),
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

    async fn probe(state: WebAppState) -> (StatusCode, serde_json::Value) {
        let resp = build_router(state)
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    /// A pod that declared a 3-node cluster and is still Standalone (it has not
    /// joined anyone) refuses CQL connections, so it must not report ready. Before
    /// this, `/readyz` answered 200 for a booting pod while its CQL listener still
    /// refused clients, and an orchestrator that gates on `/readyz` believed it.
    #[tokio::test]
    async fn readyz_is_not_ready_while_a_declared_cluster_has_not_formed() {
        let state = make_state_expecting(3);
        assert_eq!(state.mode_controller.mode(), DeploymentMode::Standalone);
        let (status, body) = probe(state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["ready"], false);
        assert_eq!(body["waiting_for"], "declared_topology");
        assert_eq!(body["expected_cluster_size"], 3);
        assert_eq!(body["mode"], "standalone");
    }

    /// A 3-node cluster that fell back to Pair after the formation timeout used to
    /// probe ready because Pair always returned 200.
    #[tokio::test]
    async fn readyz_is_not_ready_when_a_declared_cluster_fell_back_to_pair() {
        let state = make_state_expecting(3);
        state
            .mode_controller
            .set_mode_for_test(DeploymentMode::Pair);
        let (status, body) = probe(state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["waiting_for"], "declared_topology");
        assert_eq!(body["mode"], "pair");
    }

    /// A declared single node is exactly what Standalone is: ready.
    #[tokio::test]
    async fn readyz_is_ready_when_the_declared_topology_is_met() {
        let (status, body) = probe(make_state_expecting(1)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ready"], true);
    }

    /// No declared size (the default) keeps today's behavior exactly: Standalone
    /// and Pair answer 200 without consulting a topology.
    #[tokio::test]
    async fn readyz_is_unchanged_when_no_cluster_size_is_declared() {
        let standalone = make_state();
        assert_eq!(probe(standalone).await.0, StatusCode::OK);
        let pair = make_state();
        pair.mode_controller.set_mode_for_test(DeploymentMode::Pair);
        assert_eq!(probe(pair).await.0, StatusCode::OK);
    }

    /// A client listener that failed to bind used to be one ERROR log line while the
    /// node kept answering `/readyz` 200, so an orchestrator saw a healthy node with
    /// a missing port. The probe now names the failed listener.
    #[tokio::test]
    async fn readyz_is_not_ready_while_a_client_listener_has_failed() {
        let state = make_state();
        state.listeners.mark_up("postgres");
        state
            .listeners
            .mark_failed("sparql", "Address already in use (os error 98)");
        let (status, body) = probe(state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["ready"], false);
        assert_eq!(body["waiting_for"], "listeners");
        assert_eq!(body["failed"][0]["listener"], "sparql");
        assert_eq!(
            body["failed"][0]["reason"],
            "Address already in use (os error 98)"
        );
        assert_eq!(
            body["failed"].as_array().unwrap().len(),
            1,
            "only the failed one"
        );
    }

    #[tokio::test]
    async fn readyz_recovers_once_the_listener_is_serving_again() {
        let state = make_state();
        state.listeners.mark_failed("bolt", "denied");
        state.listeners.mark_up("bolt");
        assert_eq!(probe(state).await.0, StatusCode::OK);
    }

    fn set_aside_table_schema() -> ferrosa_common::schema::TableSchema {
        ferrosa_common::schema::TableSchema {
            keyspace: "aside_ks".to_string(),
            table: "aside_t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::schema::ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    /// Writes `n` mutations for `aside_ks.aside_t` into a set-aside file, as a
    /// startup replay with no schema would.
    fn set_aside_mutations(data_dir: &std::path::Path, n: usize) {
        let mut aside = ferrosa_storage::replay_set_aside::ReplaySetAside::new(data_dir);
        for i in 0..n {
            let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
                format!("k{i}").into_bytes(),
            ));
            let row = ferrosa_sstable::types::Row {
                clustering: vec![],
                cells: vec![(0, ferrosa_common::CellValue::live(b"v".to_vec(), 5))],
                deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(5),
            };
            aside
                .append(&ferrosa_storage::Mutation::new(
                    "aside_ks".into(),
                    "aside_t".into(),
                    key,
                    vec![row],
                    5,
                ))
                .expect("append set-aside mutation");
        }
        aside.sync().expect("sync set-aside file");
    }

    /// A node holding committed rows it cannot read back must say so: the rows
    /// are durable but invisible, and answering 200 is the silent missing-data
    /// state this probe exists to rule out. It clears once they are applied.
    #[tokio::test]
    async fn readyz_is_not_ready_while_set_aside_mutations_are_unapplied() {
        let dir = tempfile::tempdir().expect("tempdir");
        set_aside_mutations(dir.path(), 2);
        let state = make_state_in(dir.path(), ferrosa_cluster::ClusterConfig::default());
        let storage = Arc::clone(&state.storage);

        let (status, body) = probe(state.clone()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["ready"], false);
        assert_eq!(body["waiting_for"], "set_aside_mutations");
        assert_eq!(body["mutations"], 2);
        assert_eq!(body["tables"]["aside_ks.aside_t"], 2);

        storage
            .register_table(set_aside_table_schema())
            .expect("register table");

        let (status, body) = probe(state).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "applied rows clear the state: {body}"
        );
        assert_eq!(body["ready"], true);
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
