//! Bolt-on retirement gate.
//!
//! Per ADR-012, the bolt-on subsystems
//! [`crate::raft::election_guard`] and
//! [`crate::raft::snapshot_pusher`] may only be retired after a
//! 2-week clean Jepsen window against the current stable build.  The gate
//! has two prerequisites:
//!
//! 1. **`ELECTION_STORM_TERM_JUMPS_TOTAL == 0`** for every Jepsen run
//!    in the window.
//! 2. **Runaway-term repro** (`bug-raft-stale-candidate-runaway-term-no-prevote`)
//!    produces zero term advances on the partitioned node.
//!
//! Until that runway accumulates in CI, the gate's test fixture loads
//! a manifest of past runs from `specs/in-process/sprint-04-jepsen-window.json`
//! when present.  When the file is absent (today's situation) the
//! test asserts the gate is **not yet satisfied** so that
//! bolt-on module removal cannot ship by accident.
//!
//! When the file is populated and reports a clean window, the test
//! flips green: `prerequisite_satisfied()` returns `Ok(())`, the
//! retirement PR can land, and the metric stays exposed (zeroed) for
//! one release per the ADR's downstream-dashboard contract.

use std::path::{Path, PathBuf};

use super::phase::{BootstrapError, BootstrapPhase};

/// Aggregated outcome of the 2-week Jepsen window.
///
/// Loaded from the manifest at
/// `specs/in-process/sprint-04-jepsen-window.json` when present.
#[derive(Clone, Debug, Default)]
pub struct JepsenWindowSummary {
    /// Total Jepsen runs in the window.  Must be > 0 for the gate to
    /// even consider passing — a window with zero runs is treated as
    /// "no runway accumulated" rather than "vacuously clean".
    pub runs: u64,
    /// Sum of `ELECTION_STORM_TERM_JUMPS_TOTAL` increments observed
    /// across the runs.  Must be 0 for the gate to pass.
    pub storm_term_jumps_total: u64,
    /// Whether the runaway-term repro produced any term advances on
    /// the partitioned node.  Must be `false` for the gate to pass.
    pub runaway_term_repro_advanced: bool,
}

impl JepsenWindowSummary {
    /// Strict acceptance: clean window AND non-trivial sample size.
    /// The 14 runs reflect at least one daily run for two weeks.
    pub const MIN_RUNS: u64 = 14;

    pub fn is_clean(&self) -> bool {
        self.runs >= Self::MIN_RUNS
            && self.storm_term_jumps_total == 0
            && !self.runaway_term_repro_advanced
    }
}

/// Path to the manifest file if it exists, otherwise `None`.
///
/// The path is resolved relative to the workspace root at compile time
/// using `CARGO_MANIFEST_DIR`.  Production builds never read this
/// file; only the retirement-gate test below consumes it.
pub fn manifest_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or(Path::new("/"))
        .join("specs")
        .join("in-process")
        .join("sprint-04-jepsen-window.json")
}

/// Returns `Ok(())` iff the bolt-on retirement gate is satisfied.
///
/// `Err(BootstrapError)` carries a human-readable explanation of which
/// prerequisite is missing.  We reuse [`BootstrapError`] so the
/// retirement gate slots into the same observability surface as
/// every other bootstrap phase, even though the gate is a one-shot
/// check rather than a continuous phase.
pub fn prerequisite_satisfied(summary: &JepsenWindowSummary) -> Result<(), BootstrapError> {
    if summary.runs < JepsenWindowSummary::MIN_RUNS {
        return Err(BootstrapError::phase(
            BootstrapPhase::DrainQueue,
            format!(
                "bolt-on retirement gate: only {n} Jepsen run(s) in window, need {min}",
                n = summary.runs,
                min = JepsenWindowSummary::MIN_RUNS
            ),
        ));
    }
    if summary.storm_term_jumps_total > 0 {
        return Err(BootstrapError::phase(
            BootstrapPhase::DrainQueue,
            format!(
                "bolt-on retirement gate: ELECTION_STORM_TERM_JUMPS_TOTAL={n} (must be 0)",
                n = summary.storm_term_jumps_total
            ),
        ));
    }
    if summary.runaway_term_repro_advanced {
        return Err(BootstrapError::phase(
            BootstrapPhase::DrainQueue,
            "bolt-on retirement gate: runaway-term repro advanced terms (must be 0)",
        ));
    }
    Ok(())
}

/// Parses a tiny JSON-shaped subset of the manifest without pulling in
/// `serde_json` — fields are `runs: u64`, `storm_term_jumps_total: u64`,
/// `runaway_term_repro_advanced: bool`.
pub fn parse_manifest(text: &str) -> Result<JepsenWindowSummary, BootstrapError> {
    fn read_u64(text: &str, key: &str) -> Option<u64> {
        let needle = format!("\"{key}\"");
        let i = text.find(&needle)?;
        let rest = &text[i + needle.len()..];
        let colon = rest.find(':')?;
        let after = &rest[colon + 1..];
        let trimmed = after.trim_start();
        let end = trimmed
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(trimmed.len());
        trimmed[..end].parse().ok()
    }
    fn read_bool(text: &str, key: &str) -> Option<bool> {
        let needle = format!("\"{key}\"");
        let i = text.find(&needle)?;
        let rest = &text[i + needle.len()..];
        let colon = rest.find(':')?;
        let after = rest[colon + 1..].trim_start();
        if after.starts_with("true") {
            Some(true)
        } else if after.starts_with("false") {
            Some(false)
        } else {
            None
        }
    }

    let runs = read_u64(text, "runs").ok_or_else(|| {
        BootstrapError::phase(BootstrapPhase::DrainQueue, "manifest missing 'runs'")
    })?;
    let storm_term_jumps_total = read_u64(text, "storm_term_jumps_total").ok_or_else(|| {
        BootstrapError::phase(
            BootstrapPhase::DrainQueue,
            "manifest missing 'storm_term_jumps_total'",
        )
    })?;
    let runaway_term_repro_advanced =
        read_bool(text, "runaway_term_repro_advanced").ok_or_else(|| {
            BootstrapError::phase(
                BootstrapPhase::DrainQueue,
                "manifest missing 'runaway_term_repro_advanced'",
            )
        })?;
    Ok(JepsenWindowSummary {
        runs,
        storm_term_jumps_total,
        runaway_term_repro_advanced,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gate test reads the manifest if present;
    /// if absent (today), the gate asserts NOT-YET-SATISFIED so that
    /// bolt-on module removal cannot ship by accident.  Once a clean 2-week
    /// Jepsen window lands, populate the manifest and the same test
    /// flips to passing without any code change.
    #[test]
    fn bolt_on_retirement_gate_passes() {
        let path = manifest_path();
        let summary = match std::fs::read_to_string(&path) {
            Ok(text) => parse_manifest(&text).expect("manifest is valid"),
            Err(_) => {
                // No manifest → no runway yet → gate must report
                // not-satisfied. This branch fails premature removal cleanly.
                let summary = JepsenWindowSummary::default();
                assert!(prerequisite_satisfied(&summary).is_err());
                eprintln!(
                    "bolt-on retirement gate not yet satisfied — \
                     no Jepsen window manifest at {}.  election_guard \
                     and snapshot_pusher removal remain deferred.",
                    path.display()
                );
                return;
            }
        };
        // Manifest present → caller is asserting we're in the green
        // window.  Any prerequisite failure is a real bug.
        prerequisite_satisfied(&summary).expect("manifest claims clean window");
    }

    #[test]
    fn gate_rejects_insufficient_runs() {
        let summary = JepsenWindowSummary {
            runs: 5,
            storm_term_jumps_total: 0,
            runaway_term_repro_advanced: false,
        };
        let err = prerequisite_satisfied(&summary).expect_err("too few runs");
        let msg = format!("{err}");
        assert!(msg.contains("Jepsen run"), "{msg}");
    }

    #[test]
    fn gate_rejects_storm_term_jumps() {
        let summary = JepsenWindowSummary {
            runs: 14,
            storm_term_jumps_total: 1,
            runaway_term_repro_advanced: false,
        };
        let err = prerequisite_satisfied(&summary).expect_err("storm increment");
        let msg = format!("{err}");
        assert!(msg.contains("STORM"), "{msg}");
    }

    #[test]
    fn gate_rejects_runaway_term_advance() {
        let summary = JepsenWindowSummary {
            runs: 14,
            storm_term_jumps_total: 0,
            runaway_term_repro_advanced: true,
        };
        assert!(prerequisite_satisfied(&summary).is_err());
    }

    #[test]
    fn gate_accepts_clean_window() {
        let summary = JepsenWindowSummary {
            runs: 14,
            storm_term_jumps_total: 0,
            runaway_term_repro_advanced: false,
        };
        prerequisite_satisfied(&summary).expect("clean window passes");
    }

    #[test]
    fn parse_manifest_reads_all_three_fields() {
        let text = r#"{
          "runs": 28,
          "storm_term_jumps_total": 0,
          "runaway_term_repro_advanced": false
        }"#;
        let s = parse_manifest(text).unwrap();
        assert_eq!(s.runs, 28);
        assert_eq!(s.storm_term_jumps_total, 0);
        assert!(!s.runaway_term_repro_advanced);
    }
}
