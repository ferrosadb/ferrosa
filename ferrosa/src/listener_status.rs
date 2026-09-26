//! Module: Health of the client listeners that start in background tasks.
//! Correctness: Correct when a listener that failed to bind (or stopped) is
//!   reported as failed until it is marked up again, the report is deterministic
//!   (sorted by name), and marking is idempotent and never panics.
//! Last revised: 2026-09-26
//! Last changed: New module — Postgres, SPARQL, graph HTTP and Bolt each log a bind
//!   failure and let the node carry on, so `/readyz` answered 200 with a client
//!   listener missing. The failure is now recorded here, exported as
//!   `ferrosa_listener_up`, and gates `/readyz`.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// Which background listeners have failed, and why.
#[derive(Default)]
pub struct ListenerStatus {
    listeners: Mutex<BTreeMap<&'static str, Option<String>>>,
}

impl ListenerStatus {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<&'static str, Option<String>>> {
        // A panic while holding this lock only interrupted a map insert; the map is
        // still valid, and health reporting must keep working.
        self.listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record that `name` is serving.
    pub fn mark_up(&self, name: &'static str) {
        self.lock().insert(name, None);
    }

    /// Record that `name` failed (could not bind, or its server exited) with `reason`.
    pub fn mark_failed(&self, name: &'static str, reason: impl std::fmt::Display) {
        self.lock().insert(name, Some(reason.to_string()));
    }

    /// The listeners currently failed, as `(name, reason)`, sorted by name.
    pub fn failed(&self) -> Vec<(&'static str, String)> {
        self.lock()
            .iter()
            .filter_map(|(name, failure)| failure.clone().map(|reason| (*name, reason)))
            .collect()
    }

    /// Prometheus text: `ferrosa_listener_up{listener="..."}` is 1 or 0 for every
    /// listener that has been marked.
    pub fn render_prometheus(&self, out: &mut String) {
        use std::fmt::Write;
        out.push_str(
            "# HELP ferrosa_listener_up 1 when a background client listener is serving, 0 when it failed to bind or exited.\n\
             # TYPE ferrosa_listener_up gauge\n",
        );
        for (name, failure) in self.lock().iter() {
            // Writing into a String cannot fail.
            let _ = writeln!(
                out,
                "ferrosa_listener_up{{listener=\"{name}\"}} {}",
                u8::from(failure.is_none())
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_status_reports_no_failures() {
        let status = ListenerStatus::default();
        assert!(status.failed().is_empty());
        let mut out = String::new();
        status.render_prometheus(&mut out);
        assert!(out.contains("# TYPE ferrosa_listener_up gauge"), "{out}");
        assert!(
            !out.contains("ferrosa_listener_up{"),
            "no samples yet: {out}"
        );
    }

    #[test]
    fn a_failed_listener_is_reported_with_its_reason_until_it_is_marked_up() {
        let status = ListenerStatus::default();
        status.mark_failed("sparql", "Address already in use (os error 98)");
        assert_eq!(
            status.failed(),
            vec![("sparql", "Address already in use (os error 98)".to_string())]
        );
        let mut out = String::new();
        status.render_prometheus(&mut out);
        assert!(
            out.contains("ferrosa_listener_up{listener=\"sparql\"} 0\n"),
            "{out}"
        );

        status.mark_up("sparql");
        assert!(status.failed().is_empty(), "recovery clears the failure");
        let mut out = String::new();
        status.render_prometheus(&mut out);
        assert!(
            out.contains("ferrosa_listener_up{listener=\"sparql\"} 1\n"),
            "{out}"
        );
    }

    #[test]
    fn the_report_is_sorted_and_covers_every_marked_listener() {
        let status = ListenerStatus::default();
        status.mark_up("postgres");
        status.mark_failed("graph_http", "denied");
        status.mark_failed("bolt", "in use");
        let names: Vec<_> = status.failed().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["bolt", "graph_http"]);
        let mut out = String::new();
        status.render_prometheus(&mut out);
        let samples: Vec<_> = out
            .lines()
            .filter(|l| l.starts_with("ferrosa_listener_up{"))
            .collect();
        assert_eq!(
            samples,
            vec![
                "ferrosa_listener_up{listener=\"bolt\"} 0",
                "ferrosa_listener_up{listener=\"graph_http\"} 0",
                "ferrosa_listener_up{listener=\"postgres\"} 1",
            ]
        );
    }

    #[test]
    fn marking_twice_keeps_the_latest_reason() {
        let status = ListenerStatus::default();
        status.mark_failed("postgres", "first");
        status.mark_failed("postgres", "second");
        assert_eq!(status.failed(), vec![("postgres", "second".to_string())]);
    }
}
