use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Result from a Knossos linearizability check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnossosResult {
    pub valid: bool,
    pub model: String,
    pub algorithm: String,
    pub ok_count: usize,
    pub fail_count: usize,
    pub info_count: usize,
    pub anomalies: Vec<KnossosAnomaly>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnossosAnomaly {
    pub anomaly_type: String,
    pub description: String,
}

/// Invoke Knossos via the Jepsen Clojure project.
///
/// Requires: leiningen installed, ferrosa-jepsen/jepsen/ project available.
pub struct KnossosChecker {
    jepsen_dir: std::path::PathBuf,
}

impl KnossosChecker {
    pub fn new(jepsen_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            jepsen_dir: jepsen_dir.into(),
        }
    }

    /// Check a history file for linearizability using Knossos.
    pub fn check(&self, history_path: &Path) -> Result<KnossosResult> {
        let output = Command::new("lein")
            .args([
                "run",
                "test",
                "--test",
                "register",
                "--history-file",
                &history_path.display().to_string(),
            ])
            .current_dir(&self.jepsen_dir)
            .output()
            .context("Failed to run Knossos checker (is lein installed?)")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Try to parse JSON from stdout even on failure.
            if let Ok(result) = Self::parse_output(&output.stdout) {
                return Ok(result);
            }
            anyhow::bail!("Knossos checker failed: {}", stderr);
        }

        Self::parse_output(&output.stdout)
    }

    /// Parse Jepsen/Knossos text output, extracting key fields.
    fn parse_output(stdout: &[u8]) -> Result<KnossosResult> {
        let text = String::from_utf8_lossy(stdout);

        // Jepsen outputs results in Clojure data notation; we parse key fields by searching
        // for known keywords.
        let valid = text.contains(":valid? true");

        let ok_count = Self::extract_count(&text, ":ok-count");
        let fail_count = Self::extract_count(&text, ":fail-count");
        let info_count = Self::extract_count(&text, ":info-count");

        let anomalies = if valid {
            vec![]
        } else {
            vec![KnossosAnomaly {
                anomaly_type: "linearizability-violation".into(),
                description: "See Jepsen output for full analysis".into(),
            }]
        };

        Ok(KnossosResult {
            valid,
            model: "cas-register".into(),
            algorithm: "wgl".into(),
            ok_count,
            fail_count,
            info_count,
            anomalies,
        })
    }

    fn extract_count(text: &str, key: &str) -> usize {
        text.find(key)
            .and_then(|pos| {
                let after = &text[pos + key.len()..];
                let trimmed = after.trim_start();
                let num_str: String = trimmed.chars().take_while(|c| c.is_ascii_digit()).collect();
                num_str.parse().ok()
            })
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_output() {
        let output = b":valid? true\n:ok-count 42\n:fail-count 0\n:info-count 3";
        let result = KnossosChecker::parse_output(output).unwrap();
        assert!(result.valid);
        assert_eq!(result.ok_count, 42);
        assert_eq!(result.fail_count, 0);
        assert_eq!(result.info_count, 3);
        assert!(result.anomalies.is_empty());
    }

    #[test]
    fn parse_invalid_output() {
        let output = b":valid? false\n:ok-count 10\n:fail-count 2";
        let result = KnossosChecker::parse_output(output).unwrap();
        assert!(!result.valid);
        assert_eq!(result.anomalies.len(), 1);
    }

    #[test]
    fn extract_count_works() {
        assert_eq!(
            KnossosChecker::extract_count(":ok-count 42 :fail-count 3", ":ok-count"),
            42
        );
        assert_eq!(
            KnossosChecker::extract_count(":ok-count 42 :fail-count 3", ":fail-count"),
            3
        );
        assert_eq!(
            KnossosChecker::extract_count("no match here", ":ok-count"),
            0
        );
    }
}
