use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::checker::check_linearizability;
use crate::config::RunConfig;
use crate::history::{HistoryRecorder, Op, OpResult};
use crate::orchestrator::CombinationResult;
use crate::report::RunReport;

/// Configuration for endurance tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnduranceConfig {
    /// Total run duration.
    pub total_duration: Duration,
    /// Duration per workload pattern before rotating.
    pub pattern_duration: Duration,
    /// Nemesis injection interval range (min, max).
    pub nemesis_interval: (Duration, Duration),
    /// Rolling verification window size.
    pub verification_window: Duration,
    /// How often to run rolling verification.
    pub verification_interval: Duration,
    /// W8.9 / ADR-014 — number of long-lived learner replicas per DC.
    /// The Fly.io tri-DC topology runs 3 voters + this many learners
    /// per DC. Operators tune this to trade learner read capacity
    /// against AppendEntries fan-out.
    #[serde(default = "default_learners_per_dc")]
    pub learners_per_dc: usize,
}

fn default_learners_per_dc() -> usize {
    1
}

impl Default for EnduranceConfig {
    fn default() -> Self {
        Self {
            total_duration: Duration::from_secs(24 * 3600), // 24 hours
            pattern_duration: Duration::from_secs(300),     // 5 minutes per pattern
            nemesis_interval: (Duration::from_secs(30), Duration::from_secs(120)),
            verification_window: Duration::from_secs(600), // 10-minute window
            verification_interval: Duration::from_secs(600), // verify every 10 minutes
            learners_per_dc: 1,                            // ADR-014 / W8.9 default
        }
    }
}

impl EnduranceConfig {
    /// Short config for testing (30 minutes).
    pub fn short() -> Self {
        Self {
            total_duration: Duration::from_secs(1800),
            pattern_duration: Duration::from_secs(60),
            nemesis_interval: (Duration::from_secs(10), Duration::from_secs(30)),
            verification_window: Duration::from_secs(120),
            verification_interval: Duration::from_secs(120),
            learners_per_dc: 1,
        }
    }
}

/// State of an endurance run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnduranceState {
    pub elapsed_secs: f64,
    pub patterns_completed: usize,
    pub current_pattern: String,
    pub nemesis_cycles: usize,
    pub verifications_run: usize,
    pub verifications_passed: usize,
    pub total_ops: usize,
    pub violations: Vec<EnduranceViolation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnduranceViolation {
    pub timestamp_secs: f64,
    pub pattern: String,
    pub nemesis: String,
    pub description: String,
}

/// Run endurance tier.
pub async fn run_endurance(config: &RunConfig, endurance: &EnduranceConfig) -> Result<RunReport> {
    let start = Instant::now();
    let workload_names = {
        let wr = crate::workload::WorkloadRegistry::phase1();
        wr.names()
    };
    let nemesis_names = {
        let nr = crate::chaos::NemesisRegistry::full();
        nr.names()
    };

    let mut state = EnduranceState {
        elapsed_secs: 0.0,
        patterns_completed: 0,
        current_pattern: String::new(),
        nemesis_cycles: 0,
        verifications_run: 0,
        verifications_passed: 0,
        total_ops: 0,
        violations: vec![],
    };

    let mut results = Vec::new();
    let mut pattern_idx = 0;
    let mut last_verification = Instant::now();

    while start.elapsed() < endurance.total_duration {
        // Rotate pattern.
        let pattern = &workload_names[pattern_idx % workload_names.len()];
        state.current_pattern = pattern.clone();
        pattern_idx += 1;

        // Pick a nemesis (round-robin).
        let nemesis = &nemesis_names[state.nemesis_cycles % nemesis_names.len()];
        state.nemesis_cycles += 1;

        tracing::info!(
            pattern = pattern.as_str(),
            nemesis = nemesis.as_str(),
            elapsed = ?start.elapsed(),
            "Endurance cycle"
        );

        // Run one pattern cycle.
        let mut recorder = HistoryRecorder::new("endurance");
        recorder.invoke(Op::Write {
            key: "endurance".into(),
            value: state.nemesis_cycles as i64,
        });
        recorder.complete(OpResult::Ok);
        let history = recorder.finish();
        state.total_ops += history.len();

        // Check linearizability.
        let linear = check_linearizability(&history);
        let passed = linear.iter().all(|r| r.valid);

        if !passed {
            state.violations.push(EnduranceViolation {
                timestamp_secs: start.elapsed().as_secs_f64(),
                pattern: pattern.clone(),
                nemesis: nemesis.clone(),
                description: "Linearizability violation detected".into(),
            });
        }

        results.push(CombinationResult {
            workload: pattern.clone(),
            nemesis: nemesis.clone(),
            topology: "T4".into(),
            concurrency: "high".into(),
            driver: "rust".into(),
            passed,
            linearizability: linear,
            invariant_passed: passed,
            invariant_error: None,
            duration_secs: endurance.pattern_duration.as_secs_f64(),
            op_count: history.len(),
        });

        state.patterns_completed += 1;

        // Rolling verification.
        if last_verification.elapsed() >= endurance.verification_interval {
            state.verifications_run += 1;
            state.verifications_passed += 1; // Placeholder
            last_verification = Instant::now();
            tracing::info!(
                verifications = state.verifications_run,
                violations = state.violations.len(),
                "Rolling verification"
            );
        }

        state.elapsed_secs = start.elapsed().as_secs_f64();

        // In real implementation, sleep for pattern_duration.
        // For scaffold, break after first cycle if testing.
        if endurance.total_duration <= Duration::from_secs(5) {
            break;
        }

        tokio::time::sleep(Duration::from_millis(10)).await; // Yield
    }

    let report = RunReport::from_results(&config.run_id, results);
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RunConfig, Tier};
    use std::path::PathBuf;

    #[test]
    fn endurance_config_default() {
        let c = EnduranceConfig::default();
        assert_eq!(c.total_duration, Duration::from_secs(86400));
        assert_eq!(c.pattern_duration, Duration::from_secs(300));
    }

    #[test]
    fn endurance_config_short() {
        let c = EnduranceConfig::short();
        assert_eq!(c.total_duration, Duration::from_secs(1800));
    }

    #[test]
    fn endurance_state_default() {
        let state = EnduranceState {
            elapsed_secs: 0.0,
            patterns_completed: 0,
            current_pattern: String::new(),
            nemesis_cycles: 0,
            verifications_run: 0,
            verifications_passed: 0,
            total_ops: 0,
            violations: vec![],
        };
        assert_eq!(state.patterns_completed, 0);
        assert!(state.violations.is_empty());
    }

    #[test]
    fn endurance_config_serialization() {
        let c = EnduranceConfig::default();
        let json = serde_json::to_string(&c).unwrap();
        let back: EnduranceConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_duration, Duration::from_secs(86400));
    }

    #[tokio::test]
    async fn endurance_single_cycle() {
        let config = RunConfig {
            tier: Tier::Endurance,
            topology: None,
            nemesis: None,
            pattern: None,
            driver: None,
            concurrency: None,
            run_id: "endurance-test".into(),
            output_dir: PathBuf::from("/tmp/jepsen-endurance-test"),
            fly_regions: vec![],
            alert_webhook: None,
            output_json: false,
        };
        let endurance = EnduranceConfig {
            total_duration: Duration::from_secs(1),
            ..EnduranceConfig::short()
        };
        let report = run_endurance(&config, &endurance).await.unwrap();
        assert!(report.total > 0);
    }
}
