//! Module: Run the `elle-checker` project on an Elle-native EDN history and report its verdict.
//! Correctness: The verdict is the checker's exit code alone — 0 valid, 1 invalid or `:unknown` —
//! and any other exit, a missing or unparseable anomalies file, or a checker that cannot start
//! is an error, never a verdict (`specs/jepsen-evidence` D2, D3). Nothing is read from stdout.
//! Last revised: 2026-10-31
//! Last changed: Replaced the stdout scrape (`":valid? true"` substring, lower-cased anomaly
//! names) with the exit code and a parse of the anomalies file's top-level keys.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

/// The variable naming the elle-checker project directory.
pub const DIR_VAR: &str = "ELLE_CHECKER_DIR";

/// The elle-checker project directory when [`DIR_VAR`] is unset, relative to the repository root.
pub const DEFAULT_DIR: &str = "ferrosa-jepsen/elle-checker";

/// How many trailing lines of the checker's output a checker error carries.
const ERROR_TAIL_LINES: usize = 20;

/// Result from an Elle consistency check that ran to a verdict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElleResult {
    /// `true` only when the checker exited 0, which it does only on a definitive `:valid? true`.
    pub valid: bool,
    pub consistency_model: String,
    /// Anomaly names in Elle's own spelling without the leading colon (`G0-realtime`,
    /// `G-single`), from the keys of the anomalies file. Empty when the file is `nil`.
    pub anomaly_types: Vec<String>,
}

/// Runs the `elle-checker` Leiningen project (`lein run <history.edn>`).
pub struct ElleChecker {
    checker_dir: PathBuf,
}

impl ElleChecker {
    pub fn new(checker_dir: impl Into<PathBuf>) -> Self {
        Self {
            checker_dir: checker_dir.into(),
        }
    }

    /// The checker from the environment ([`DIR_VAR`]). A value that is not valid Unicode is
    /// refused rather than treated as unset, so it can never fall back to the default silently.
    pub fn from_env() -> Result<Self, String> {
        match std::env::var(DIR_VAR) {
            Err(std::env::VarError::NotUnicode(raw)) => {
                Err(format!("{DIR_VAR}={raw:?} is not valid Unicode"))
            }
            Ok(v) => Self::from_vars(|key| (key == DIR_VAR).then(|| v.clone())),
            Err(std::env::VarError::NotPresent) => Self::from_vars(|_| None),
        }
    }

    /// The checker from a variable lookup, read the way `ConsistencyProfile::from_vars` reads
    /// its variables: unset takes [`DEFAULT_DIR`], and a set value that is not a path is refused
    /// naming the variable.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        match var(DIR_VAR) {
            None => Ok(Self::new(DEFAULT_DIR)),
            Some(v) if v.trim().is_empty() => {
                Err(format!("{DIR_VAR}={v:?} is not a directory path"))
            }
            Some(v) => Ok(Self::new(v)),
        }
    }

    pub fn checker_dir(&self) -> &Path {
        &self.checker_dir
    }

    /// Check an Elle-native EDN history for strict serializability.
    ///
    /// `Ok` carries a verdict; `Err` means the checker did not produce one (D2), which a caller
    /// must record as a setup failure rather than as either verdict.
    pub fn check(&self, history_path: &Path) -> Result<ElleResult> {
        if !self.checker_dir.join("project.clj").is_file() {
            bail!(
                "{} is not an elle-checker project (no project.clj); set {DIR_VAR} \
                 (default {DEFAULT_DIR}, relative to the repository root)",
                self.checker_dir.display()
            );
        }
        // The checker runs in its own directory, so the history must be named absolutely.
        let history = std::fs::canonicalize(history_path)
            .with_context(|| format!("Elle history {} is not readable", history_path.display()))?;
        let anomalies_path = anomalies_path_for(&history);
        remove_stale(&anomalies_path)?;

        let output = run_lein(&self.checker_dir, &history)?;
        let valid = match output.status.code() {
            Some(0) => true,
            Some(1) => false,
            other => bail!(
                "elle-checker exited {other:?}, which is neither a verdict (0 valid, 1 invalid) \
                 nor a clean failure:\n{}",
                output_tail(&output)
            ),
        };
        let edn = std::fs::read_to_string(&anomalies_path).with_context(|| {
            format!(
                "elle-checker exited {} but wrote no readable anomalies file {}:\n{}",
                u8::from(!valid),
                anomalies_path.display(),
                output_tail(&output)
            )
        })?;
        let anomaly_types = anomaly_types(&edn).map_err(|e| {
            anyhow!(
                "elle-checker anomalies file {} is unparseable: {e}",
                anomalies_path.display()
            )
        })?;
        Ok(ElleResult {
            valid,
            consistency_model: "strict-serializable".into(),
            anomaly_types,
        })
    }
}

/// Where elle-checker writes its anomalies for a history (`<history>.anomalies.edn`).
fn anomalies_path_for(history: &Path) -> PathBuf {
    let mut name = history.as_os_str().to_owned();
    name.push(".anomalies.edn");
    PathBuf::from(name)
}

/// Remove an anomalies file left by an earlier run, so a checker that writes none this time
/// cannot be read as having written the stale one.
fn remove_stale(anomalies_path: &Path) -> Result<()> {
    match std::fs::remove_file(anomalies_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| {
            format!(
                "cannot remove stale anomalies file {}",
                anomalies_path.display()
            )
        }),
    }
}

/// Run `lein run <history>` in the checker directory, naming the setup a missing `lein` needs.
fn run_lein(checker_dir: &Path, history: &Path) -> Result<Output> {
    Command::new("lein")
        .arg("run")
        .arg(history)
        .current_dir(checker_dir)
        .output()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                anyhow!(
                    "`lein` is not on PATH, so the Elle checker cannot run. Install Leiningen \
                     2.x and a JDK 17+ (macOS: `brew install leiningen openjdk@17`), then run \
                     `lein deps` in {}",
                    checker_dir.display()
                )
            } else {
                anyhow!(e).context(format!(
                    "failed to start `lein run` in {}",
                    checker_dir.display()
                ))
            }
        })
}

/// The last lines of the checker's stdout and stderr, for a checker error.
fn output_tail(output: &Output) -> String {
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(ERROR_TAIL_LINES)..].join("\n")
}

/// Anomaly names: the keyword keys of the top-level EDN map elle-checker writes (`pr-str` of
/// Elle's `:anomalies`), without their colon. `nil` means no anomalies. Only the top level is
/// read; the cycles inside each value are skipped, so a keyword nested in a cycle is never
/// mistaken for an anomaly type.
fn anomaly_types(edn: &str) -> Result<Vec<String>, String> {
    let text = edn.trim();
    if text == "nil" {
        return Ok(Vec::new());
    }
    let mut scanner = Scanner::new(text);
    scanner.expect('{')?;
    let mut names = Vec::new();
    loop {
        scanner.skip_whitespace();
        match scanner.peek() {
            None => return Err("unterminated map".into()),
            Some('}') => {
                scanner.pos += 1;
                break;
            }
            Some(_) => names.push(scanner.read_keyword_key()?),
        }
        scanner.skip_whitespace();
        if matches!(scanner.peek(), None | Some('}')) {
            return Err(format!("key :{} has no value", names[names.len() - 1]));
        }
        scanner.skip_form()?;
    }
    scanner.skip_whitespace();
    if scanner.pos != scanner.chars.len() {
        return Err("text after the top-level map".into());
    }
    Ok(names)
}

/// A forward-only reader over EDN text that skips whole forms.
struct Scanner {
    chars: Vec<char>,
    pos: usize,
}

impl Scanner {
    fn new(text: &str) -> Self {
        Self {
            chars: text.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn expect(&mut self, want: char) -> Result<(), String> {
        self.skip_whitespace();
        match self.peek() {
            Some(c) if c == want => {
                self.pos += 1;
                Ok(())
            }
            other => Err(format!("expected {want:?}, found {other:?}")),
        }
    }

    /// EDN treats commas as whitespace.
    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|c| c.is_whitespace() || c == ',') {
            self.pos += 1;
        }
    }

    fn read_keyword_key(&mut self) -> Result<String, String> {
        let atom = self.read_atom();
        match atom.strip_prefix(':') {
            Some(name) if !name.is_empty() => Ok(name.to_string()),
            _ => Err(format!("anomaly key {atom:?} is not a keyword")),
        }
    }

    fn read_atom(&mut self) -> String {
        let start = self.pos;
        while self.peek().is_some_and(|c| !is_delimiter(c)) {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }

    /// Skip one form of any kind: a string, a collection, a character literal, or an atom.
    fn skip_form(&mut self) -> Result<(), String> {
        match self.peek() {
            Some('"') => self.skip_string(),
            Some('(' | '[' | '{') => self.skip_collection(),
            Some('#') if self.chars.get(self.pos + 1) == Some(&'{') => {
                self.pos += 1;
                self.skip_collection()
            }
            Some(c @ (')' | ']' | '}')) => Err(format!("unexpected {c:?}")),
            Some('\\') => {
                // A character literal: the backslash, at least one character, then any name.
                self.pos += 2;
                self.read_atom();
                Ok(())
            }
            Some(_) => {
                self.read_atom();
                Ok(())
            }
            None => Err("expected a form, found the end".into()),
        }
    }

    fn skip_string(&mut self) -> Result<(), String> {
        self.pos += 1;
        while let Some(c) = self.peek() {
            self.pos += if c == '\\' { 2 } else { 1 };
            if c == '"' {
                return Ok(());
            }
        }
        Err("unterminated string".into())
    }

    /// Skip a balanced collection. Every step consumes input, so it ends within the text's length.
    fn skip_collection(&mut self) -> Result<(), String> {
        let mut depth = 0usize;
        while let Some(c) = self.peek() {
            match c {
                '"' => {
                    self.skip_string()?;
                    continue;
                }
                '\\' => self.pos += 1,
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => {
                    depth -= 1;
                    if depth == 0 {
                        self.pos += 1;
                        return Ok(());
                    }
                }
                _ => {}
            }
            self.pos += 1;
        }
        Err("unterminated collection".into())
    }
}

fn is_delimiter(c: char) -> bool {
    c.is_whitespace() || matches!(c, ',' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | ';')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elle_result_serialization() {
        let r = ElleResult {
            valid: true,
            consistency_model: "strict-serializable".into(),
            anomaly_types: vec![],
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: ElleResult = serde_json::from_str(&json).unwrap();
        assert!(back.valid);
        assert_eq!(back.consistency_model, "strict-serializable");
    }

    /// The committed elle-checker project, independent of the test's working directory.
    fn elle_checker_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("elle-checker")
    }

    /// Copy a committed control history into a fresh directory, so the anomalies file the
    /// checker writes beside it never lands in the source tree.
    fn control_history(name: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("create a temp dir for the control history");
        let src = elle_checker_dir().join("test/histories").join(name);
        let dst = dir.path().join(name);
        std::fs::copy(&src, &dst)
            .unwrap_or_else(|e| panic!("copy control history {}: {e}", src.display()));
        (dir, dst)
    }

    /// A stand-in checker project whose main prints `stdout`, writes `anomalies` (if any) beside
    /// the history it was given (its last argument), and exits with `exit_code`. It depends
    /// only on Clojure, which `lein deps` in the real elle-checker has already fetched.
    fn fake_checker(stdout: &str, anomalies: Option<&str>, exit_code: i32) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("create a temp dir for the fake checker");
        std::fs::write(
            dir.path().join("project.clj"),
            "(defproject fake-elle \"0\" :dependencies [[org.clojure/clojure \"1.12.0\"]] \
             :main fake.core)\n",
        )
        .expect("write fake project.clj");
        std::fs::create_dir_all(dir.path().join("src/fake")).expect("create fake src dir");
        let spit = match anomalies {
            Some(a) => format!("(spit (str (last args) \".anomalies.edn\") {a:?})"),
            None => String::new(),
        };
        std::fs::write(
            dir.path().join("src/fake/core.clj"),
            format!(
                "(ns fake.core (:gen-class))\n(defn -main [& args] {spit} (println {stdout:?}) \
                 (flush) (System/exit {exit_code}))\n"
            ),
        )
        .expect("write fake core.clj");
        dir
    }

    /// JE-2 (D3): the verdict is elle-checker's exit code, against both committed controls.
    /// The legal history is valid with no anomaly; the realtime-violating one is invalid and
    /// names its realtime anomaly in Elle's own upper-case spelling.
    #[test]
    fn elle_verdict_comes_from_the_exit_code() {
        let checker = ElleChecker::new(elle_checker_dir());

        let (_valid_dir, valid_path) = control_history("known-valid.edn");
        let valid = checker
            .check(&valid_path)
            .unwrap_or_else(|e| panic!("the elle-checker must run here: {e:#}"));
        assert!(valid.valid, "known-valid.edn must be valid: {valid:?}");
        assert!(
            valid.anomaly_types.is_empty(),
            "a valid history names no anomaly: {valid:?}"
        );

        let (_invalid_dir, invalid_path) = control_history("known-invalid-realtime.edn");
        let invalid = checker
            .check(&invalid_path)
            .unwrap_or_else(|e| panic!("the elle-checker must run here: {e:#}"));
        assert!(
            !invalid.valid,
            "known-invalid-realtime.edn must be invalid: {invalid:?}"
        );
        assert!(
            invalid.anomaly_types.iter().any(|a| a == "G0-realtime"),
            "the realtime anomaly must be named: {invalid:?}"
        );
    }

    /// Mirrors Lean `Jepsen.a_scraped_verdict_passes_an_invalid_result`: an invalid verdict
    /// whose output carries a nested `:valid? true` is still invalid, because exit 1 says so.
    #[test]
    fn a_nested_valid_true_in_the_output_does_not_pass_an_invalid_exit() {
        let fake = fake_checker(
            "{:valid? false, :anomaly-types (:G-single), :sub {:valid? true}}",
            Some("{:G-single [{:cycle []}]}"),
            1,
        );
        let (_dir, history) = control_history("known-valid.edn");
        let result = ElleChecker::new(fake.path())
            .check(&history)
            .unwrap_or_else(|e| panic!("the fake checker must run here: {e:#}"));
        assert!(!result.valid, "exit 1 is invalid whatever the output says");
        assert_eq!(result.anomaly_types, vec!["G-single".to_string()]);
    }

    /// D2/D3: an exit other than 0 or 1 is a checker error, never a verdict.
    #[test]
    fn an_exit_other_than_zero_or_one_is_a_checker_error() {
        let fake = fake_checker(":valid? true", Some("nil"), 3);
        let (_dir, history) = control_history("known-valid.edn");
        let outcome = ElleChecker::new(fake.path()).check(&history);
        assert!(
            outcome.is_err(),
            "exit 3 must be a checker error, not a verdict: {outcome:?}"
        );
    }

    /// D2: a checker that reports no anomalies file has produced output we cannot read, which
    /// is a checker error rather than a verdict without anomalies.
    #[test]
    fn a_missing_anomalies_file_is_a_checker_error() {
        let fake = fake_checker("valid? false", None, 1);
        let (_dir, history) = control_history("known-valid.edn");
        let outcome = ElleChecker::new(fake.path()).check(&history);
        assert!(
            outcome.is_err(),
            "no anomalies file must be a checker error: {outcome:?}"
        );
    }

    /// A directory with no project.clj is refused before anything runs.
    #[test]
    fn a_directory_that_is_not_the_checker_project_is_refused() {
        let empty = tempfile::tempdir().unwrap();
        let (_dir, history) = control_history("known-valid.edn");
        let error = ElleChecker::new(empty.path())
            .check(&history)
            .expect_err("a directory without project.clj is not a checker");
        assert!(error.to_string().contains(DIR_VAR), "{error}");
    }

    /// Mirrors Lean `Jepsen.a_lower_cased_anomaly_search_finds_nothing`: Elle names anomalies
    /// in upper case, and the parse reports them in that spelling.
    #[test]
    fn anomaly_names_keep_elles_upper_case_spelling() {
        assert_eq!(
            anomaly_types("{:G0 [] :G1c [] :G-single []}").unwrap(),
            vec!["G0", "G1c", "G-single"]
        );
    }

    #[test]
    fn nil_anomalies_name_none() {
        assert!(anomaly_types("nil\n").unwrap().is_empty());
        assert!(anomaly_types("{}").unwrap().is_empty());
    }

    /// Keywords inside a cycle (`:type :ok`, `:steps (...)`) are values, not anomaly types.
    #[test]
    fn only_top_level_keys_are_anomaly_types() {
        let edn = r#"{:G0-realtime [{:cycle [{:type :ok, :value [[:append 1 1]]}],
                      :steps ({:type :realtime, :a' {:f :txn}} {:type :ww, :s "a } b \" c"}),
                      :type :G0-realtime}],
                      :G2 #{:x \} :y}}"#;
        assert_eq!(anomaly_types(edn).unwrap(), vec!["G0-realtime", "G2"]);
    }

    #[test]
    fn unparseable_anomalies_are_refused() {
        for bad in [
            "",
            "{:G0 []",
            "{:G0}",
            "[:G0 []]",
            "{\"G0\" []}",
            "{:G0 [} ]}",
            "{:G0 []} trailing",
        ] {
            assert!(anomaly_types(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn checker_dir_is_read_like_the_profile() {
        let unset = ElleChecker::from_vars(|_| None).unwrap();
        assert_eq!(unset.checker_dir(), Path::new(DEFAULT_DIR));

        let set = ElleChecker::from_vars(|k| (k == DIR_VAR).then(|| "/opt/elle".into())).unwrap();
        assert_eq!(set.checker_dir(), Path::new("/opt/elle"));

        let error = ElleChecker::from_vars(|k| (k == DIR_VAR).then(|| "  ".into()))
            .err()
            .expect("a blank directory is refused");
        assert!(error.contains(DIR_VAR), "{error}");
    }
}
