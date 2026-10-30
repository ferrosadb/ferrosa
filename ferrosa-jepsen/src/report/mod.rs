pub mod anomaly;
pub mod comparison;
pub mod timeline;

use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::orchestrator::CombinationResult;

/// Complete report for a test run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub run_id: String,
    pub timestamp: String,
    pub results: Vec<CombinationResult>,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    /// Runs whose consistency profile could not support either claim: neither a pass nor a failure.
    #[serde(default)]
    pub not_tested: usize,
}

impl RunReport {
    pub fn from_results(run_id: &str, results: Vec<CombinationResult>) -> Self {
        let total = results.len();
        // A run whose profile cannot support a claim is neither a pass nor a failure. Counting it as
        // either would over-claim (a weak configuration called green) or under-claim (the semantics
        // of that configuration called a defect).
        //
        // Each count is taken by construction rather than derived by subtraction. Deriving `failed`
        // as `total - passed - not_tested` assumed the three sets were disjoint, and they are not:
        // a result can be `passed` and inconclusive at the same time, and the subtraction then
        // underflows and panics the report. Counting the classes directly keeps the partition exact
        // whatever the inputs say.
        let not_tested = results.iter().filter(|r| r.is_inconclusive()).count();
        let passed = results
            .iter()
            .filter(|r| r.passed && !r.is_inconclusive())
            .count();
        let failed = results
            .iter()
            .filter(|r| !r.passed && !r.is_inconclusive())
            .count();
        debug_assert_eq!(
            total,
            passed + failed + not_tested,
            "a run must be exactly one of passed, failed, or not tested"
        );

        Self {
            run_id: run_id.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            results,
            total,
            passed,
            failed,
            not_tested,
        }
    }

    /// True when nothing failed. Inconclusive runs are not failures — failing them would punish a
    /// deliberate configuration — but they are not confirmations either; see
    /// [`Self::fully_confirmed`].
    pub fn all_passed(&self) -> bool {
        self.failed == 0
    }

    /// True only when every run was confirmed at its profile. This is what a green badge may mean.
    pub fn fully_confirmed(&self) -> bool {
        self.failed == 0 && self.not_tested == 0
    }

    pub fn failures(&self) -> Vec<&CombinationResult> {
        self.results.iter().filter(|r| !r.passed).collect()
    }

    /// Write JSON report.
    pub fn write_json(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Write HTML report.
    pub fn write_html(&self, path: &Path) -> Result<()> {
        let mut html = String::new();
        html.push_str("<!DOCTYPE html><html><head>");
        html.push_str("<meta charset='utf-8'>");
        html.push_str(&format!("<title>ferrosa-jepsen: {}</title>", self.run_id));
        html.push_str("<style>");
        html.push_str("body { font-family: monospace; margin: 2em; }");
        html.push_str("table { border-collapse: collapse; width: 100%; }");
        html.push_str("th, td { border: 1px solid #333; padding: 8px; text-align: left; }");
        html.push_str("th { background: #222; color: #eee; }");
        html.push_str(".pass { background: #d4edda; }");
        html.push_str(".fail { background: #f8d7da; }");
        html.push_str("h1 { color: #333; }");
        html.push_str(".summary { font-size: 1.2em; margin: 1em 0; }");
        html.push_str("</style></head><body>");

        html.push_str(&format!("<h1>ferrosa-jepsen run: {}</h1>", self.run_id));
        html.push_str(&format!(
            "<p class='summary'>{} / {} passed",
            self.passed, self.total
        ));
        if self.failed > 0 {
            html.push_str(&format!(" — <strong>{} FAILED</strong>", self.failed));
        }
        html.push_str("</p>");
        html.push_str(&format!("<p>Generated: {}</p>", self.timestamp));

        // Results table
        html.push_str("<table><tr>");
        html.push_str("<th>Workload</th><th>Nemesis</th><th>Topology</th>");
        html.push_str("<th>Result</th><th>Ops</th><th>Duration</th><th>Details</th></tr>");

        for r in &self.results {
            let class = if r.passed { "pass" } else { "fail" };
            let status = if r.passed { "PASS" } else { "FAIL" };
            html.push_str(&format!("<tr class='{class}'>"));
            html.push_str(&format!("<td>{}</td>", r.workload));
            html.push_str(&format!("<td>{}</td>", r.nemesis));
            html.push_str(&format!("<td>{}</td>", r.topology));
            html.push_str(&format!("<td>{status}</td>"));
            html.push_str(&format!("<td>{}</td>", r.op_count));
            html.push_str(&format!("<td>{:.1}s</td>", r.duration_secs));

            if let Some(err) = &r.invariant_error {
                html.push_str(&format!("<td>{err}</td>"));
            } else {
                html.push_str("<td></td>");
            }
            html.push_str("</tr>");
        }

        html.push_str("</table></body></html>");
        std::fs::write(path, html)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            profile: Default::default(),
            // An unset assurance means "not tested", which is the right default in production and
            // the wrong one in a fixture: a fixture has to say what it is an example of.
            assurance: if passed {
                crate::checker::profile::Assurance::Confirmed
            } else {
                crate::checker::profile::Assurance::Refuted
            },
        }
    }

    #[test]
    fn report_from_results() {
        let results = vec![make_result("a", "b", true), make_result("c", "d", false)];
        let report = RunReport::from_results("test-001", results);
        assert_eq!(report.total, 2);
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 1);
        assert!(!report.all_passed());
        assert_eq!(report.failures().len(), 1);
    }

    /// An inconclusive run is neither a pass nor a failure, and the three counts must partition the
    /// total. A passed run that is also inconclusive used to underflow the subtraction that derived
    /// `failed`, which panicked the report instead of reporting it.
    #[test]
    fn an_inconclusive_run_is_neither_a_pass_nor_a_failure() {
        let mut inconclusive = make_result("e", "f", true);
        inconclusive.assurance = crate::checker::profile::Assurance::NotTested(
            crate::checker::profile::NotTested::ReadsDoNotOverlapWrites,
        );

        let results = vec![
            make_result("a", "b", true),
            make_result("c", "d", false),
            inconclusive,
        ];
        let report = RunReport::from_results("test-003", results);

        assert_eq!(report.total, 3);
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(report.not_tested, 1);
        assert_eq!(
            report.passed + report.failed + report.not_tested,
            report.total,
            "every run is exactly one of passed, failed, or not tested"
        );
    }

    /// The exact input that panicked the smoke tier: results that never set an assurance, so they
    /// carry the `NotTested` default. `total - passed - not_tested` underflowed here because every
    /// one of them counted as both passed and not tested.
    #[test]
    fn a_report_of_unassured_runs_does_not_underflow() {
        let results = vec![make_result("a", "b", true), make_result("c", "d", true)];
        let mut results = results;
        for r in &mut results {
            r.assurance = Default::default();
        }

        let report = RunReport::from_results("test-004", results);

        assert_eq!(report.total, 2);
        assert_eq!(report.passed, 0, "an untested run is not a pass");
        assert_eq!(report.failed, 0, "an untested run is not a failure");
        assert_eq!(report.not_tested, 2);
        assert_eq!(
            report.passed + report.failed + report.not_tested,
            report.total
        );
    }

    #[test]
    fn report_all_passed() {
        let results = vec![make_result("a", "b", true), make_result("c", "d", true)];
        let report = RunReport::from_results("test-002", results);
        assert!(report.all_passed());
        assert!(report.failures().is_empty());
    }

    #[test]
    fn report_json_roundtrip() {
        let report = RunReport::from_results("test-001", vec![]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("results.json");
        report.write_json(&path).unwrap();
        let json = std::fs::read_to_string(&path).unwrap();
        let back: RunReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back.run_id, "test-001");
    }

    #[test]
    fn report_html_generation() {
        let report = RunReport::from_results("test-001", vec![]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("report.html");
        report.write_html(&path).unwrap();
        let html = std::fs::read_to_string(&path).unwrap();
        assert!(html.contains("ferrosa-jepsen"));
        assert!(html.contains("test-001"));
    }

    #[test]
    fn report_html_contains_results() {
        let results = vec![
            make_result("register", "partition-halves", true),
            make_result("bank", "kill-minority", false),
        ];
        let report = RunReport::from_results("test-003", results);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("report.html");
        report.write_html(&path).unwrap();
        let html = std::fs::read_to_string(&path).unwrap();
        assert!(html.contains("register"));
        assert!(html.contains("partition-halves"));
        assert!(html.contains("PASS"));
        assert!(html.contains("FAIL"));
        assert!(html.contains("bad")); // invariant_error
    }
}
