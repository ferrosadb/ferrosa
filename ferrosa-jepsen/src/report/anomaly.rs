use crate::orchestrator::CombinationResult;

/// Format a counterexample as HTML.
pub fn format_anomaly_html(result: &CombinationResult) -> String {
    let mut html = String::new();
    html.push_str("<div class='anomaly'>");
    html.push_str(&format!(
        "<h3>{} / {}</h3>",
        result.workload, result.nemesis
    ));

    if let Some(err) = &result.invariant_error {
        html.push_str(&format!("<p class='error'>{err}</p>"));
    }

    for check in &result.linearizability {
        if !check.valid {
            html.push_str(&format!(
                "<p>Key <code>{}</code>: NOT linearizable ({} ops)</p>",
                check.key, check.total_ops
            ));
            if let Some(ref cx) = check.counterexample {
                html.push_str("<pre class='counterexample'>");
                html.push_str(&cx.explanation);
                html.push_str("</pre>");
            }
        }
    }

    html.push_str("</div>");
    html
}

/// Format all anomalies from a set of results.
pub fn format_all_anomalies_html(results: &[CombinationResult]) -> String {
    let failures: Vec<&CombinationResult> = results.iter().filter(|r| !r.passed).collect();

    if failures.is_empty() {
        return "<p class='all-pass'>All checks passed.</p>".into();
    }

    let mut html = format!("<h2>{} Anomalies Detected</h2>", failures.len());
    for f in &failures {
        html.push_str(&format_anomaly_html(f));
    }
    html
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::CheckResult;
    use crate::orchestrator::CombinationResult;

    fn make_result(passed: bool) -> CombinationResult {
        CombinationResult {
            workload: "register".into(),
            nemesis: "partition-halves".into(),
            topology: "T1".into(),
            concurrency: "low".into(),
            driver: "rust".into(),
            passed,
            linearizability: vec![],
            invariant_passed: passed,
            invariant_error: if passed {
                None
            } else {
                Some("invariant violated".into())
            },
            duration_secs: 1.0,
            op_count: 10,
        }
    }

    #[test]
    fn format_all_pass() {
        let html = format_all_anomalies_html(&[]);
        assert!(html.contains("All checks passed"));
    }

    #[test]
    fn format_single_failure() {
        let result = make_result(false);
        let html = format_anomaly_html(&result);
        assert!(html.contains("register"));
        assert!(html.contains("partition-halves"));
        assert!(html.contains("invariant violated"));
    }

    #[test]
    fn format_with_linearizability_failure() {
        let result = CombinationResult {
            workload: "register".into(),
            nemesis: "partition-halves".into(),
            topology: "T1".into(),
            concurrency: "low".into(),
            driver: "rust".into(),
            passed: false,
            linearizability: vec![CheckResult {
                valid: false,
                key: "x".into(),
                total_ops: 5,
                counterexample: Some(crate::checker::Counterexample {
                    operations: vec![],
                    explanation: "stale read detected".into(),
                }),
                check_duration_ms: 1,
            }],
            invariant_passed: false,
            invariant_error: None,
            duration_secs: 1.0,
            op_count: 5,
        };
        let html = format_anomaly_html(&result);
        assert!(html.contains("NOT linearizable"));
        assert!(html.contains("stale read detected"));
    }

    #[test]
    fn format_multiple_failures() {
        let results = vec![make_result(true), make_result(false), make_result(false)];
        let html = format_all_anomalies_html(&results);
        assert!(html.contains("2 Anomalies Detected"));
    }
}
