use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Alert payload sent to webhooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertPayload {
    pub run_id: String,
    pub tier: String,
    pub status: AlertStatus,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub violations: Vec<String>,
    pub report_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertStatus {
    Pass,
    Fail,
}

/// Send an alert to a webhook URL.
pub async fn send_alert(webhook_url: &str, payload: &AlertPayload) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .post(webhook_url)
        .json(payload)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !response.status().is_success() {
        tracing::warn!(
            status = %response.status(),
            "Webhook returned non-success status"
        );
    } else {
        tracing::info!("Alert sent to webhook");
    }

    Ok(())
}

/// Create an alert payload from a run report.
pub fn alert_from_report(report: &crate::report::RunReport, tier: &str) -> AlertPayload {
    // Label each failure by class so an alert (and the auto-filed issue built
    // from it) never presents a setup/infrastructure failure as a correctness
    // violation — see issue #303.
    let violations: Vec<String> = report
        .failures()
        .iter()
        .map(|f| match &f.setup_error {
            Some(setup) => format!(
                "{}/{}: SETUP FAILED (no invariant evaluated): {setup}",
                f.workload, f.nemesis
            ),
            None => format!(
                "{}/{}: INVARIANT VIOLATED: {:?}",
                f.workload, f.nemesis, f.invariant_error
            ),
        })
        .collect();

    AlertPayload {
        run_id: report.run_id.clone(),
        tier: tier.into(),
        status: if report.all_passed() {
            AlertStatus::Pass
        } else {
            AlertStatus::Fail
        },
        total: report.total,
        passed: report.passed,
        failed: report.failed,
        violations,
        report_url: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::CombinationResult;
    use crate::report::RunReport;

    #[test]
    fn alert_payload_serialization() {
        let p = AlertPayload {
            run_id: "test".into(),
            tier: "smoke".into(),
            status: AlertStatus::Pass,
            total: 10,
            passed: 10,
            failed: 0,
            violations: vec![],
            report_url: None,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"status\":\"Pass\""));
    }

    #[test]
    fn alert_from_passing_report() {
        let report = RunReport::from_results("test-pass", vec![]);
        let alert = alert_from_report(&report, "smoke");
        assert_eq!(alert.run_id, "test-pass");
        assert_eq!(alert.tier, "smoke");
        assert!(matches!(alert.status, AlertStatus::Pass));
        assert!(alert.violations.is_empty());
    }

    #[test]
    fn alert_from_failing_report() {
        let results = vec![CombinationResult {
            workload: "register".into(),
            nemesis: "partition".into(),
            topology: "T1".into(),
            concurrency: "low".into(),
            driver: "rust".into(),
            passed: false,
            linearizability: vec![],
            invariant_passed: false,
            invariant_error: Some("invariant violated".into()),
            setup_error: None,
            duration_secs: 1.0,
            op_count: 10,
        }];
        let report = RunReport::from_results("test-fail", results);
        let alert = alert_from_report(&report, "full");
        assert!(matches!(alert.status, AlertStatus::Fail));
        assert_eq!(alert.failed, 1);
        assert_eq!(alert.violations.len(), 1);
        assert!(
            alert.violations[0].contains("INVARIANT VIOLATED"),
            "a real invariant failure must be labelled as one: {}",
            alert.violations[0]
        );
    }

    /// Issue #303: a combination that never RAN (cluster/DDL/driver setup error)
    /// must not be reported as a correctness violation. It still fails the run,
    /// but the alert — and the issue auto-filed from it — must say so plainly,
    /// because "violated an invariant" sends triage after a correctness bug that
    /// was never observed.
    #[test]
    fn alert_labels_setup_failure_distinctly_from_invariant_violation() {
        let results = vec![CombinationResult {
            workload: "bank".into(),
            nemesis: "dc-partition+dc-slow".into(),
            topology: "T3".into(),
            concurrency: "low".into(),
            driver: "rust".into(),
            passed: false,
            linearizability: vec![],
            invariant_passed: false,
            // No invariant was evaluated — the combination died in setup.
            invariant_error: None,
            setup_error: Some(
                "executing query `CREATE KEYSPACE ...`: Failed to await schema agreement".into(),
            ),
            duration_secs: 0.0,
            op_count: 0,
        }];
        let report = RunReport::from_results("test-setup-fail", results);
        let alert = alert_from_report(&report, "multi-dc");

        assert!(matches!(alert.status, AlertStatus::Fail), "still a failure");
        assert_eq!(alert.violations.len(), 1);
        let v = &alert.violations[0];
        assert!(
            v.contains("SETUP FAILED") && v.contains("no invariant evaluated"),
            "setup failure must be labelled as such: {v}"
        );
        assert!(
            !v.contains("INVARIANT VIOLATED"),
            "must NOT claim an invariant was violated: {v}"
        );
    }
}
