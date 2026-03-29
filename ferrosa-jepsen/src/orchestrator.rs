use std::time::Instant;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::chaos::NemesisRegistry;
use crate::checker::{check_linearizability, CheckResult};
use crate::config::{Concurrency, RunConfig, Tier, Topology};
use crate::docker_provision::{provision_docker_cluster, teardown_docker_cluster, ClusterInfo};
use crate::driver::DriverRegistry;
use crate::history::{HistoryRecorder, Op, OpResult};
use crate::report::RunReport;

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
            if std::env::var("FERROSA_TEST_CONTAINERS").is_ok()
                && topology.node_count() <= 3
            {
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
/// `cluster` is `Some` when a real Docker cluster has been provisioned for
/// this topology; `None` in unit-test / placeholder mode.
async fn run_single_combination(
    topology: Topology,
    nemesis_name: &str,
    workload_name: &str,
    driver_name: &str,
    concurrency: Concurrency,
    _config: &RunConfig,
    _cluster: Option<&ClusterInfo>,
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

    // In full implementation:
    // 1. Setup workload schema via CQL session
    // 2. Start history recorder
    // 3. Start nemesis schedule (inject after 5s, heal after 15s, repeat)
    // 4. Run workload for 30s
    // 5. Stop and collect history
    // 6. Run linearizability checker
    // 7. Run invariant checker

    // For now, create a placeholder history to exercise the checker.
    let mut recorder = HistoryRecorder::new("orchestrator");
    recorder.invoke(Op::Write {
        key: "test".into(),
        value: 1,
    });
    recorder.complete(OpResult::Ok);
    let history = recorder.finish();

    let linearizability = check_linearizability(&history);
    let all_linear = linearizability.iter().all(|r| r.valid);

    let wr = crate::workload::WorkloadRegistry::phase1();
    let invariant_result = if let Some(wl) = wr.get(workload_name) {
        wl.check_invariant(&history)
    } else {
        Ok(())
    };
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

    #[tokio::test]
    async fn run_single_combination_passes() {
        let config = RunConfig {
            tier: Tier::Smoke,
            topology: None,
            nemesis: None,
            pattern: None,
            driver: None,
            concurrency: None,
            run_id: "test-run".into(),
            output_dir: std::path::PathBuf::from("/tmp/jepsen-test"),
            fly_regions: vec![],
            alert_webhook: None,
            output_json: false,
        };

        let result = run_single_combination(
            Topology::T1,
            "partition-halves",
            "register",
            "rust",
            Concurrency::Low,
            &config,
            None,
        )
        .await
        .unwrap();

        assert!(result.passed);
        assert_eq!(result.workload, "register");
        assert_eq!(result.nemesis, "partition-halves");
        assert!(result.op_count > 0);
    }
}
