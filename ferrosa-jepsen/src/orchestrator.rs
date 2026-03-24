use std::time::Instant;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::chaos::NemesisRegistry;
use crate::checker::{check_linearizability, CheckResult};
use crate::config::{RunConfig, Tier, Topology};
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
    let nemesis_reg = NemesisRegistry::phase1();

    let nemesis_names: Vec<String> = match &config.nemesis {
        Some(n) => vec![n.clone()],
        None => match config.tier {
            Tier::Smoke => nemesis_reg.names(),
            _ => nemesis_reg.names(), // Phase 2+ will expand
        },
    };

    let workload_names: Vec<String> = match &config.pattern {
        Some(p) => vec![p.clone()],
        None => {
            let wr = crate::workload::WorkloadRegistry::phase1();
            wr.names()
        }
    };

    let mut results = Vec::new();

    for topology in &topologies {
        tracing::info!(?topology, "Provisioning cluster");

        // In real execution, we'd provision here via FerrosCluster::provision.
        for nemesis_name in &nemesis_names {
            for workload_name in &workload_names {
                let result =
                    run_single_combination(*topology, nemesis_name, workload_name, config).await;

                match result {
                    Ok(r) => results.push(r),
                    Err(e) => {
                        tracing::error!(
                            workload = workload_name.as_str(),
                            nemesis = nemesis_name.as_str(),
                            error = %e,
                            "Combination failed"
                        );
                        results.push(CombinationResult {
                            workload: workload_name.clone(),
                            nemesis: nemesis_name.clone(),
                            topology: format!("{topology:?}"),
                            concurrency: "low".into(),
                            driver: "rust".into(),
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

    let report = RunReport::from_results(&config.run_id, results);

    // Write report
    let report_dir = config.output_dir.join(&config.run_id);
    std::fs::create_dir_all(&report_dir)?;
    report.write_json(&report_dir.join("results.json"))?;
    report.write_html(&report_dir.join("report.html"))?;

    Ok(report)
}

/// Run a single workload+nemesis combination.
async fn run_single_combination(
    topology: Topology,
    nemesis_name: &str,
    workload_name: &str,
    _config: &RunConfig,
) -> Result<CombinationResult> {
    let start = Instant::now();

    tracing::info!(
        workload = workload_name,
        nemesis = nemesis_name,
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
        concurrency: "low".into(),
        driver: "rust".into(),
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

        let result = run_single_combination(Topology::T1, "partition-halves", "register", &config)
            .await
            .unwrap();

        assert!(result.passed);
        assert_eq!(result.workload, "register");
        assert_eq!(result.nemesis, "partition-halves");
        assert!(result.op_count > 0);
    }
}
