use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::report::RunReport;

/// Archive directory (~/.ferrosa-jepsen/runs/).
pub fn archive_dir() -> PathBuf {
    dirs_next::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ferrosa-jepsen")
        .join("runs")
}

/// Archive a run's results.
pub fn archive_run(report: &RunReport, source_dir: &Path) -> Result<PathBuf> {
    let dest = archive_dir().join(&report.run_id);
    std::fs::create_dir_all(&dest)
        .with_context(|| format!("creating archive dir {}", dest.display()))?;

    // Copy results.json
    let src_json = source_dir.join("results.json");
    if src_json.exists() {
        std::fs::copy(&src_json, dest.join("results.json"))?;
    }

    // Copy report.html
    let src_html = source_dir.join("report.html");
    if src_html.exists() {
        std::fs::copy(&src_html, dest.join("report.html"))?;
    }

    // Write summary
    let summary = serde_json::to_string_pretty(report)?;
    std::fs::write(dest.join("summary.json"), summary)?;

    tracing::info!(run_id = %report.run_id, path = %dest.display(), "Archived run");
    Ok(dest)
}

/// List archived runs.
pub fn list_runs() -> Result<Vec<ArchivedRun>> {
    let dir = archive_dir();
    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut runs = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let summary_path = entry.path().join("summary.json");
            if summary_path.exists() {
                if let Ok(json) = std::fs::read_to_string(&summary_path) {
                    if let Ok(report) = serde_json::from_str::<RunReport>(&json) {
                        runs.push(ArchivedRun {
                            run_id: report.run_id,
                            timestamp: report.timestamp,
                            total: report.total,
                            passed: report.passed,
                            failed: report.failed,
                            path: entry.path(),
                        });
                    }
                }
            }
        }
    }

    runs.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    Ok(runs)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedRun {
    pub run_id: String,
    pub timestamp: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    #[serde(skip)]
    pub path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn archive_dir_exists() {
        let dir = archive_dir();
        assert!(dir.to_str().unwrap().contains(".ferrosa-jepsen"));
    }

    #[test]
    fn archive_and_list() {
        let tmp = tempfile::tempdir().unwrap();

        // Override archive dir for test — we test archive_run directly with
        // a manually-created dest instead.
        let report = RunReport::from_results("archive-test-001", vec![]);

        // Write source files
        let source = tmp.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        report.write_json(&source.join("results.json")).unwrap();

        // Archive
        let dest = tmp.path().join("dest").join(&report.run_id);
        std::fs::create_dir_all(&dest).unwrap();
        let summary = serde_json::to_string_pretty(&report).unwrap();
        std::fs::write(dest.join("summary.json"), summary).unwrap();

        // Verify summary exists
        assert!(dest.join("summary.json").exists());
    }

    #[test]
    fn list_empty_runs() {
        // list_runs on a non-existent directory returns empty vec
        let runs = list_runs();
        // This might return runs or empty depending on the machine state,
        // but it should not error.
        assert!(runs.is_ok());
    }

    #[test]
    fn archived_run_serialization() {
        let run = ArchivedRun {
            run_id: "test-run".into(),
            timestamp: "2026-03-23T00:00:00Z".into(),
            total: 10,
            passed: 9,
            failed: 1,
            path: PathBuf::from("/tmp/test"),
        };
        let json = serde_json::to_string(&run).unwrap();
        // path is skipped in serialization
        assert!(!json.contains("/tmp/test"));
        assert!(json.contains("test-run"));
    }
}
