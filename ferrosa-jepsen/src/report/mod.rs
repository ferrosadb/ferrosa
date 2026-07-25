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
}

impl RunReport {
    pub fn from_results(run_id: &str, results: Vec<CombinationResult>) -> Self {
        let total = results.len();
        let passed = results.iter().filter(|r| r.passed).count();
        let failed = total - passed;

        Self {
            run_id: run_id.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            results,
            total,
            passed,
            failed,
        }
    }

    pub fn all_passed(&self) -> bool {
        self.failed == 0
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
