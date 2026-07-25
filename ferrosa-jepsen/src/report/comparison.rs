use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::RunReport;

/// Comparison between two runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunComparison {
    pub run_a: String,
    pub run_b: String,
    pub regressions: Vec<ComparisonEntry>,
    pub fixes: Vec<ComparisonEntry>,
    pub unchanged: usize,
    pub new_in_b: usize,
    pub removed_in_b: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonEntry {
    pub workload: String,
    pub nemesis: String,
    pub status_a: String,
    pub status_b: String,
}

/// Compare two run reports.
pub fn compare(report_a: &RunReport, report_b: &RunReport) -> RunComparison {
    let mut regressions = Vec::new();
    let mut fixes = Vec::new();
    let mut unchanged = 0;

    // Build index of run_a results by (workload, nemesis).
    let a_map: std::collections::HashMap<(String, String), bool> = report_a
        .results
        .iter()
        .map(|r| ((r.workload.clone(), r.nemesis.clone()), r.passed))
        .collect();

    let b_map: std::collections::HashMap<(String, String), bool> = report_b
        .results
        .iter()
        .map(|r| ((r.workload.clone(), r.nemesis.clone()), r.passed))
        .collect();

    for (key, passed_b) in &b_map {
        if let Some(passed_a) = a_map.get(key) {
            if *passed_a && !passed_b {
                regressions.push(ComparisonEntry {
                    workload: key.0.clone(),
                    nemesis: key.1.clone(),
                    status_a: "PASS".into(),
                    status_b: "FAIL".into(),
                });
            } else if !passed_a && *passed_b {
                fixes.push(ComparisonEntry {
                    workload: key.0.clone(),
                    nemesis: key.1.clone(),
                    status_a: "FAIL".into(),
                    status_b: "PASS".into(),
                });
            } else {
                unchanged += 1;
            }
        }
    }

    let new_in_b = b_map.keys().filter(|k| !a_map.contains_key(k)).count();
    let removed_in_b = a_map.keys().filter(|k| !b_map.contains_key(k)).count();

    RunComparison {
        run_a: report_a.run_id.clone(),
        run_b: report_b.run_id.clone(),
        regressions,
        fixes,
        unchanged,
        new_in_b,
        removed_in_b,
    }
}

/// Load a report from a JSON file.
pub fn load_report(path: &Path) -> Result<RunReport> {
    let json = std::fs::read_to_string(path)?;
    let report: RunReport = serde_json::from_str(&json)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::CombinationResult;

    fn make_result(workload: &str, nemesis: &str, passed: bool) -> CombinationResult {
        CombinationResult {
            workload: workload.into(),
            nemesis: nemesis.into(),
            topology: "T1".into(),
            concurrency: "low".into(),
            driver: "rust".into(),
            passed,
            linearizability: vec![],
            invariant_passed: passed,
            invariant_error: if passed { None } else { Some("bad".into()) },
            setup_error: None,
            duration_secs: 1.0,
            op_count: 10,
        }
    }

    #[test]
    fn compare_runs_regression() {
        let a = RunReport::from_results("run-a", vec![make_result("reg", "part", true)]);
        let b = RunReport::from_results("run-b", vec![make_result("reg", "part", false)]);
        let cmp = compare(&a, &b);
        assert_eq!(cmp.regressions.len(), 1);
        assert_eq!(cmp.fixes.len(), 0);
        assert_eq!(cmp.unchanged, 0);
    }

    #[test]
    fn compare_runs_fix() {
        let a = RunReport::from_results("run-a", vec![make_result("reg", "part", false)]);
        let b = RunReport::from_results("run-b", vec![make_result("reg", "part", true)]);
        let cmp = compare(&a, &b);
        assert_eq!(cmp.regressions.len(), 0);
        assert_eq!(cmp.fixes.len(), 1);
    }

    #[test]
    fn compare_runs_unchanged() {
        let a = RunReport::from_results("run-a", vec![make_result("reg", "part", true)]);
        let b = RunReport::from_results("run-b", vec![make_result("reg", "part", true)]);
        let cmp = compare(&a, &b);
        assert_eq!(cmp.regressions.len(), 0);
        assert_eq!(cmp.fixes.len(), 0);
        assert_eq!(cmp.unchanged, 1);
    }

    #[test]
    fn compare_runs_new_and_removed() {
        let a = RunReport::from_results("run-a", vec![make_result("old", "part", true)]);
        let b = RunReport::from_results("run-b", vec![make_result("new", "part", true)]);
        let cmp = compare(&a, &b);
        assert_eq!(cmp.new_in_b, 1);
        assert_eq!(cmp.removed_in_b, 1);
    }

    #[test]
    fn load_report_roundtrip() {
        let report = RunReport::from_results("test-load", vec![make_result("a", "b", true)]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("report.json");
        report.write_json(&path).unwrap();
        let loaded = load_report(&path).unwrap();
        assert_eq!(loaded.run_id, "test-load");
        assert_eq!(loaded.total, 1);
    }
}
