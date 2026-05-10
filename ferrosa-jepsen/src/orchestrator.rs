use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::chaos::NemesisRegistry;
use crate::checker::{check_linearizability, CheckResult};
use crate::config::{Concurrency, RunConfig, Tier, Topology};
use crate::cql_session::ScyllaCqlSession;
use crate::docker_provision::{provision_docker_cluster, teardown_docker_cluster, ClusterInfo};
use crate::driver::DriverRegistry;
use crate::history::HistoryRecorder;
use crate::report::RunReport;
use crate::workload::{CqlSession, MockCqlSession, WorkloadRegistry};

/// Where the CQL session for a single combination came from.
///
/// Used by the orchestrator to decide whether to dial the real cluster or
/// the in-process mock. The variant is exposed for unit tests so they can
/// assert the wiring without spinning a Docker cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionSource {
    /// In-process mock — no I/O. Used when no cluster has been provisioned
    /// or when running unit-style combinations.
    Mock,
    /// Real CQL session against the contact points listed.
    Real(Vec<String>),
}

/// Resolve the session source for a single combination.
///
/// When a real cluster has been provisioned (`cluster.is_some()`), we route
/// queries through `ScyllaCqlSession` against the cluster's contact points.
/// Otherwise (unit-level invocations, in-process orchestrator runs without
/// containers), we fall back to `MockCqlSession`.
///
/// Prior to Sprint 2, this function did not exist: `run_single_combination`
/// took an `Option<&ClusterInfo>` argument that it ignored, so the real
/// cluster path was unreachable. See `specs/in-process/sprint-02-jepsen-reactivation.md`
/// W2.1 / W2.2.
pub(crate) fn resolve_session_source(cluster: Option<&ClusterInfo>) -> SessionSource {
    match cluster {
        Some(c) => {
            let addrs: Vec<String> = c.nodes.iter().map(|n| n.cql_address()).collect();
            SessionSource::Real(addrs)
        }
        None => SessionSource::Mock,
    }
}

/// Result of a single test combination (one workload + one nemesis).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinationResult {
    pub workload: String,
    pub nemesis: String,
    pub topology: String,
    pub concurrency: String,
    pub driver: String,
    pub passed: bool,
    pub linearizability: Vec<CheckResult>,
    pub invariant_passed: bool,
    pub invariant_error: Option<String>,
    pub duration_secs: f64,
    pub op_count: usize,
}

/// Run the full test suite per the config.
pub async fn run(config: &RunConfig) -> Result<RunReport> {
    let topologies = config.topologies();
    let concurrency_levels = config.concurrency_levels();

    let nemesis_reg = resolve_nemesis_registry(config.tier);
    let nemesis_names: Vec<String> = match &config.nemesis {
        Some(n) => vec![n.clone()],
        None => nemesis_reg.names(),
    };

    let workload_names: Vec<String> = match &config.pattern {
        Some(p) => vec![p.clone()],
        None => {
            let wr = crate::workload::WorkloadRegistry::phase1();
            wr.names()
        }
    };

    let driver_reg = resolve_driver_registry(config.tier);
    let driver_names: Vec<String> = match &config.driver {
        Some(d) => vec![d.clone()],
        None => driver_reg.names(),
    };

    let mut results = Vec::new();

    for topology in &topologies {
        tracing::info!(?topology, "Provisioning cluster");

        // Provision the cluster for this topology.
        // For single-DC topologies (T1/T2) we use Docker Compose when
        // FERROSA_TEST_CONTAINERS is set; otherwise we fall back to a
        // no-op cluster (placeholder) so the orchestrator can still run
        // unit-level combinations without container infrastructure.
        let cluster_opt: Option<ClusterInfo> =
            if std::env::var("FERROSA_TEST_CONTAINERS").is_ok() && topology.node_count() <= 3 {
                match provision_docker_cluster(*topology).await {
                    Ok(c) => Some(c),
                    Err(e) => {
                        tracing::error!(
                            ?topology,
                            error = %e,
                            "Docker cluster provisioning failed; skipping topology"
                        );
                        continue;
                    }
                }
            } else {
                None
            };

        for concurrency in &concurrency_levels {
            for driver_name in &driver_names {
                for nemesis_name in &nemesis_names {
                    for workload_name in &workload_names {
                        let result = run_single_combination(
                            *topology,
                            nemesis_name,
                            workload_name,
                            driver_name,
                            *concurrency,
                            config,
                            cluster_opt.as_ref(),
                        )
                        .await;

                        match result {
                            Ok(r) => results.push(r),
                            Err(e) => {
                                tracing::error!(
                                    workload = workload_name.as_str(),
                                    nemesis = nemesis_name.as_str(),
                                    driver = driver_name.as_str(),
                                    error = %e,
                                    "Combination failed"
                                );
                                results.push(CombinationResult {
                                    workload: workload_name.clone(),
                                    nemesis: nemesis_name.clone(),
                                    topology: format!("{topology:?}"),
                                    concurrency: format!("{concurrency:?}"),
                                    driver: driver_name.clone(),
                                    passed: false,
                                    linearizability: vec![],
                                    invariant_passed: false,
                                    invariant_error: Some(e.to_string()),
                                    duration_secs: 0.0,
                                    op_count: 0,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Tear down the cluster after all combinations for this topology.
        if let Some(cluster) = cluster_opt {
            if let Err(e) = teardown_docker_cluster(cluster).await {
                tracing::warn!(?topology, error = %e, "cluster teardown failed");
            }
        }
    }

    let report = RunReport::from_results(&config.run_id, results);

    // Write report
    let report_dir = config.output_dir.join(&config.run_id);
    std::fs::create_dir_all(&report_dir)?;
    report.write_json(&report_dir.join("results.json"))?;
    report.write_html(&report_dir.join("report.html"))?;

    Ok(report)
}

/// Resolve nemesis registry based on tier.
fn resolve_nemesis_registry(tier: Tier) -> NemesisRegistry {
    match tier {
        Tier::Smoke => NemesisRegistry::phase1(),
        Tier::Standard => NemesisRegistry::phase2(),
        Tier::Full | Tier::Endurance => NemesisRegistry::full(),
        // Sprint 7 W7.11: tier-multi-dc uses the full registry
        // which includes the W7.11 dc-partition+dc-slow composed
        // nemesis required by the headline run.
        Tier::MultiDc => NemesisRegistry::full(),
    }
}

/// Resolve driver registry based on tier.
fn resolve_driver_registry(tier: Tier) -> DriverRegistry {
    match tier {
        Tier::Smoke => DriverRegistry::phase1(),
        _ => DriverRegistry::phase1(), // Phase 2: switch to phase2() once drivers are ready
    }
}

/// Run a single workload+nemesis combination.
///
/// Wires the full pipeline:
/// 1. Resolve workload from registry.
/// 2. Set up schema via CQL session.
/// 3. Run workload, recording history.
/// 4. Check linearizability.
/// 5. Check workload-specific invariants.
///
/// Uses `MockCqlSession` when no real cluster contact points are provided.
/// `cluster` is `Some` when a real Docker cluster has been provisioned.
async fn run_single_combination(
    topology: Topology,
    nemesis_name: &str,
    workload_name: &str,
    driver_name: &str,
    concurrency: Concurrency,
    config: &RunConfig,
    cluster: Option<&crate::docker_provision::ClusterInfo>,
) -> Result<CombinationResult> {
    let start = Instant::now();

    tracing::info!(
        workload = workload_name,
        nemesis = nemesis_name,
        driver = driver_name,
        ?concurrency,
        ?topology,
        "Running combination"
    );

    // Resolve workload.
    let registry = WorkloadRegistry::phase1();
    let workload = registry
        .get(workload_name)
        .ok_or_else(|| anyhow::anyhow!("unknown workload: {workload_name}"))?;

    // Resolve session source: real cluster when provisioned, mock otherwise.
    // Sprint 2 W2.2 — formerly the cluster argument was discarded and
    // `MockCqlSession` was used unconditionally.
    let session: Box<dyn CqlSession> = match resolve_session_source(cluster) {
        SessionSource::Real(addrs) => {
            tracing::info!(?addrs, "dialing real CQL cluster");
            Box::new(ScyllaCqlSession::connect(&addrs).await?)
        }
        SessionSource::Mock => {
            tracing::debug!("using MockCqlSession (no cluster provisioned)");
            Box::new(MockCqlSession)
        }
    };

    // Set up schema.
    workload.setup(session.as_ref()).await?;

    // Run workload for configured duration.
    let run_duration = Duration::from_secs(config.run_duration_secs());
    let mut recorder = HistoryRecorder::new(&format!("{driver_name}-{workload_name}"));
    workload
        .run(session.as_ref(), &mut recorder, run_duration)
        .await?;
    let history = recorder.finish();

    // Write history JSONL to output directory.
    let run_dir = config.output_dir.join(&config.run_id);
    std::fs::create_dir_all(&run_dir)?;
    let history_path = run_dir.join(format!("{topology:?}-{nemesis_name}-{workload_name}.jsonl"));
    history.to_jsonl(&history_path)?;

    // Check linearizability.
    let linearizability = check_linearizability(&history);
    let all_linear = linearizability.iter().all(|r| r.valid);

    // Check workload-specific invariants.
    let invariant_result = workload.check_invariant(&history);
    let invariant_passed = invariant_result.is_ok();
    let invariant_error = invariant_result.err().map(|e| e.to_string());

    let duration = start.elapsed();

    Ok(CombinationResult {
        workload: workload_name.into(),
        nemesis: nemesis_name.into(),
        topology: format!("{topology:?}"),
        concurrency: format!("{concurrency:?}"),
        driver: driver_name.into(),
        passed: all_linear && invariant_passed,
        linearizability,
        invariant_passed,
        invariant_error,
        duration_secs: duration.as_secs_f64(),
        op_count: history.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combination_result_serialization() {
        let r = CombinationResult {
            workload: "register".into(),
            nemesis: "partition-halves".into(),
            topology: "T1".into(),
            concurrency: "low".into(),
            driver: "rust".into(),
            passed: true,
            linearizability: vec![],
            invariant_passed: true,
            invariant_error: None,
            duration_secs: 1.5,
            op_count: 42,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: CombinationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.workload, "register");
        assert!(back.passed);
    }

    #[test]
    fn workload_names_from_registry() {
        let wr = crate::workload::WorkloadRegistry::phase1();
        let names = wr.names();
        assert!(!names.is_empty());
        assert!(names.contains(&"register".to_string()));
        assert!(names.contains(&"bank".to_string()));
    }

    // -----------------------------------------------------------------------
    // W2.1 / W2.2 — orchestrator must wire the real cluster when one is provided.
    //
    // Before Sprint 2 the orchestrator discarded its `_cluster` argument and
    // always used `MockCqlSession`, even when callers had spun up a real
    // Docker cluster. The bug lived at `orchestrator.rs:203`. The two tests
    // below pin the resolution helper that drives the wiring.
    // -----------------------------------------------------------------------

    /// W2.2: when a cluster is provided, the orchestrator must dial it.
    #[test]
    fn orchestrator_uses_real_cluster_when_provided() {
        use crate::docker_provision::NodeInfo as JepsenNodeInfo;

        let cluster = ClusterInfo {
            nodes: vec![
                JepsenNodeInfo {
                    host: "localhost",
                    cql_port: 19042,
                },
                JepsenNodeInfo {
                    host: "localhost",
                    cql_port: 19043,
                },
                JepsenNodeInfo {
                    host: "localhost",
                    cql_port: 19044,
                },
            ],
            compose_file: std::path::PathBuf::from("/tmp/fake.yml"),
        };

        let source = resolve_session_source(Some(&cluster));
        match source {
            SessionSource::Real(addrs) => {
                assert_eq!(
                    addrs,
                    vec![
                        "localhost:19042".to_string(),
                        "localhost:19043".to_string(),
                        "localhost:19044".to_string(),
                    ],
                    "real session must use the cluster's CQL contact points"
                );
            }
            SessionSource::Mock => panic!(
                "orchestrator must NOT use MockCqlSession when a real cluster is provided \
                 (Sprint 2 W2.2 — see specs/in-process/sprint-02-jepsen-reactivation.md)"
            ),
        }
    }

    /// W2.1: when no cluster is provided, the orchestrator falls back to mock.
    #[test]
    fn orchestrator_uses_mock_when_no_cluster_provided() {
        let source = resolve_session_source(None);
        assert_eq!(source, SessionSource::Mock);
    }

    #[tokio::test]
    async fn run_single_combination_completes() {
        // Verify that run_single_combination runs the full pipeline (workload setup,
        // run, linearizability check, invariant check) without panicking.
        //
        // Uses MockCqlSession so no real cluster is needed. The mock returns empty
        // rows for all queries, which means reads return Value(None). This is
        // consistent with a never-written register (model starts at None), so a
        // history with only reads would be linearizable. With writes also present,
        // the mock history may not be linearizable — the important invariant is
        // that the pipeline runs end-to-end without error, not that it passes.
        let dir = tempfile::tempdir().unwrap();
        let config = RunConfig {
            tier: Tier::Smoke,
            topology: None,
            nemesis: None,
            pattern: None,
            driver: None,
            concurrency: None,
            run_id: "test-run".into(),
            output_dir: dir.path().to_path_buf(),
            fly_regions: vec![],
            alert_webhook: None,
            output_json: false,
        };

        let result = run_single_combination(
            Topology::T1,
            "noop",
            "register",
            "rust",
            Concurrency::Low,
            &config,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.workload, "register");
        assert_eq!(result.nemesis, "noop");
        assert_eq!(result.driver, "rust");
        // History file should exist.
        let history_path = dir.path().join("test-run").join("T1-noop-register.jsonl");
        assert!(history_path.exists(), "history JSONL must be written");
    }
}
