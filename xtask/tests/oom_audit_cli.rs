//! End-to-end acceptance for the `p0-oom-audit` CLI contract.
//!
//! These run the real binary against a throwaway audit root, because the two
//! things that matter here are only observable at the process boundary:
//!
//! * warn-ahead prints a warning naming owner/rule/path/expiry/days remaining,
//!   and does NOT change the exit code — an entry that still has days left must
//!   never fail a build. A red `main` is how the 2026-09-30 batch of nine
//!   entries ambushed CI on the day it expired; the warning has to be loud
//!   enough to act on and harmless enough to ship with.
//! * an EXPIRED entry still fails `--enforce`, so the warning did not replace
//!   the failure half of the contract.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The allow entry every fixture uses: one 2026-10-12 expiry, so a `--today` of
/// 2026-09-30 is 12 days out (inside the default 21-day window) and a `--today`
/// of 2026-10-13 is one day past it.
fn allow_toml() -> &'static str {
    r#"
[[allow]]
path = "ferrosa-storage/src/store.rs"
rule = "returns-vec-partition-or-row"
reason = "fixture: exercises the warn-ahead contract"
owner = "storage"
expires = "2026-10-12"
"#
}

/// Two entries: one already expired (a FINDING) and one merely expiring soon (a
/// WARNING). The pair is what proves warnings cannot excuse a finding.
fn allow_toml_expired_and_expiring() -> &'static str {
    r#"
[[allow]]
path = "ferrosa-storage/src/store.rs"
rule = "returns-vec-partition-or-row"
reason = "fixture: an expired entry is a finding"
owner = "storage"
expires = "2026-09-01"

[[allow]]
path = "ferrosa-storage/src/engine.rs"
rule = "returns-vec-partition-or-row"
reason = "fixture: an entry inside the warn-ahead window is only a warning"
owner = "storage"
expires = "2026-10-12"
"#
}

/// A scratch audit root: a workspace manifest whose only member is classified,
/// an allowlist, and no audited crates (so the only output is allowlist
/// diagnostics). `label` keeps parallel tests off each other's files.
struct AuditRoot {
    dir: PathBuf,
}

impl AuditRoot {
    fn new(label: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "ferrosa-xtask-oom-audit-{}-{label}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("specs/p0-oom-guard")).expect("create fixture root");
        std::fs::write(
            dir.join("Cargo.toml"),
            "[workspace]\nmembers = [\"xtask\"]\n",
        )
        .expect("write fixture manifest");
        std::fs::write(
            dir.join("specs/p0-oom-guard/oom-audit-allow.toml"),
            allow_toml(),
        )
        .expect("write fixture allowlist");
        Self { dir }
    }

    fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for AuditRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Run `xtask p0-oom-audit <extra args>` against `root`.
fn audit(root: &AuditRoot, extra: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_xtask"));
    cmd.arg("p0-oom-audit")
        .arg("--root")
        .arg(root.path())
        .args(extra);
    cmd.output().expect("run the xtask binary")
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn warn_ahead_prints_an_actionable_warning_but_still_exits_zero_under_enforce() {
    let root = AuditRoot::new("warn-ahead");
    // 12 days before expiry: inside the default 21-day warn-ahead window, and
    // the entry has NOT expired.
    let out = audit(&root, &["--enforce", "--today", "2026-09-30"]);
    let text = stdout(&out);

    assert!(
        out.status.success(),
        "--enforce must stay green while the entry is unexpired (a warning is not \
         a failure); stdout={text:?} stderr={:?}",
        stderr(&out)
    );
    assert!(
        out.status.code() == Some(0),
        "a warning must never change the exit code, got {:?}",
        out.status.code()
    );
    for needle in [
        "warning",
        "storage",
        "returns-vec-partition-or-row",
        "ferrosa-storage/src/store.rs",
        "2026-10-12",
        "12",
    ] {
        assert!(
            text.contains(needle),
            "the warning must name `{needle}`; stdout={text:?}"
        );
    }
    assert!(
        !text.contains("expired\n") && !stderr(&out).contains("FAIL"),
        "an unexpired entry must not be reported as a failure; stdout={text:?} \
         stderr={:?}",
        stderr(&out)
    );
}

#[test]
fn expired_entry_still_fails_enforce() {
    let root = AuditRoot::new("expired");
    // One day past the expiry: the failure half of the contract, unchanged.
    let out = audit(&root, &["--enforce", "--today", "2026-10-13"]);
    assert!(
        !out.status.success(),
        "an expired entry must still fail --enforce; stdout={:?} stderr={:?}",
        stdout(&out),
        stderr(&out)
    );
    assert!(
        stdout(&out).contains("expired-allow-entry"),
        "the expired entry must still be reported as a finding; stdout={:?}",
        stdout(&out)
    );
    assert!(
        !stdout(&out).contains("expiring-allow-entry"),
        "an expired entry is a failure, not a warning; stdout={:?}",
        stdout(&out)
    );
}

/// INVARIANT (a) at the process boundary: the audit is a safety control, so
/// warn-ahead must never weaken the verdict. A run that ALSO prints warnings
/// still exits 1, and the widening of the warn window cannot change that.
#[test]
fn invariant_warnings_do_not_change_the_exit_code_under_enforce() {
    let root = AuditRoot::new("invariant-exit-code");
    std::fs::write(
        root.path().join("specs/p0-oom-guard/oom-audit-allow.toml"),
        allow_toml_expired_and_expiring(),
    )
    .expect("write the expired+expiring fixture");

    for window in ["0", "21", "3650"] {
        let out = audit(
            &root,
            &[
                "--enforce",
                "--today",
                "2026-09-30",
                "--warn-within",
                window,
            ],
        );
        let text = stdout(&out);
        assert!(
            !out.status.success(),
            "an expired entry must fail --enforce regardless of --warn-within {window}; \
             stdout={text:?} stderr={:?}",
            stderr(&out)
        );
        assert!(
            text.contains("expired-allow-entry"),
            "the finding must still be reported with --warn-within {window}; stdout={text:?}"
        );
        assert!(
            !text.contains("warning: [expired"),
            "warnings must not be emitted for an expired entry; stdout={text:?}"
        );
    }

    // With the default window the same run prints a warning for the OTHER
    // entry — and still fails. Warnings are advisory, never a waiver.
    let out = audit(&root, &["--enforce", "--today", "2026-09-30"]);
    assert!(
        !out.status.success(),
        "the run must still fail; stdout={:?}",
        stdout(&out)
    );
    assert!(
        stdout(&out).contains("expiring-allow-entry")
            && stdout(&out).contains("expired-allow-entry"),
        "both the warning and the finding must be visible; stdout={:?}",
        stdout(&out)
    );
}

/// INVARIANT (e)/(c) for the nightly: the scheduled job runs ONLY the audit —
/// no test suite, no image build. Pinned here so the workflow cannot grow into
/// a second full nightly by accident. (The YAML side is pinned in tests/ci.)
#[test]
fn invariant_audit_is_the_only_cargo_command_the_nightly_runs() {
    let workflow = include_str!("../../.github/workflows/oom-audit-daily.yml");
    // Lines that actually INVOKE cargo: the command after a `run:` key, so
    // prose that merely mentions a command is not mistaken for a second compile.
    let invocations: Vec<&str> = workflow
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("run:").map(str::trim))
        .filter(|l| l.starts_with("cargo "))
        .collect();
    assert_eq!(
        invocations.len(),
        1,
        "the nightly must invoke cargo exactly once (the audit); found {invocations:?}"
    );
    assert!(
        invocations[0]
            .contains("cargo run --quiet -p xtask --all-features -- p0-oom-audit --enforce"),
        "and it must be the enforced audit: {:?}",
        invocations[0]
    );
    assert!(
        !workflow.contains("cargo test") && !workflow.contains("cargo build"),
        "the nightly must not run the test suite or build images"
    );
}

#[test]
fn warn_within_zero_disables_the_warning_without_touching_findings() {
    let root = AuditRoot::new("warn-within-zero");
    // Same run date, window narrowed to today only: the 12-day-out entry is
    // not warned about, and the run is still green.
    let out = audit(
        &root,
        &["--enforce", "--today", "2026-09-30", "--warn-within", "0"],
    );
    assert!(
        out.status.success(),
        "narrowing the window must not fail the run; stdout={:?} stderr={:?}",
        stdout(&out),
        stderr(&out)
    );
    assert!(
        !stdout(&out).contains("expiring-allow-entry"),
        "a 12-day-out entry is outside a 0-day window; stdout={:?}",
        stdout(&out)
    );
}

#[test]
fn warn_within_widens_to_catch_the_entry() {
    let root = AuditRoot::new("warn-within-wide");
    let out = audit(&root, &["--today", "2026-09-30", "--warn-within", "30"]);
    assert!(out.status.success(), "warn mode always exits 0");
    assert!(
        stdout(&out).contains("expiring-allow-entry"),
        "--warn-within 30 must catch a 12-day-out entry; stdout={:?}",
        stdout(&out)
    );
}

#[test]
fn a_non_numeric_warn_within_is_a_loud_error() {
    let root = AuditRoot::new("warn-within-bad");
    let out = audit(&root, &["--today", "2026-09-30", "--warn-within", "soon"]);
    assert!(
        !out.status.success(),
        "a bad --warn-within must fail loud, not be silently ignored; stderr={:?}",
        stderr(&out)
    );
    assert!(
        stderr(&out).contains("--warn-within"),
        "the error must name the offending flag; stderr={:?}",
        stderr(&out)
    );
}

#[test]
fn warn_ahead_defaults_to_twenty_one_days() {
    let root = AuditRoot::new("default-window");
    // Exactly 21 days out: inside the default window (inclusive bound), so no
    // flag is needed for the entry to be surfaced.
    std::fs::write(
        root.path().join("specs/p0-oom-guard/oom-audit-allow.toml"),
        allow_toml().replace("2026-10-12", "2026-10-21"),
    )
    .expect("rewrite fixture allowlist");
    let out = audit(&root, &["--today", "2026-09-30"]);
    assert!(out.status.success(), "warn mode exits 0");
    let text = stdout(&out);
    assert!(
        text.contains("expiring-allow-entry") && text.contains("21"),
        "a 21-day-out entry must be warned about by default; stdout={text:?}"
    );
}

#[test]
fn a_malformed_expiry_is_reported_as_a_warning_not_a_finding() {
    let root = AuditRoot::new("bad-date");
    std::fs::write(
        root.path().join("specs/p0-oom-guard/oom-audit-allow.toml"),
        allow_toml().replace("2026-10-12", "2026-12-1"),
    )
    .expect("rewrite fixture allowlist");
    let out = audit(&root, &["--enforce", "--today", "2026-09-30"]);
    assert!(
        out.status.success(),
        "a malformed expiry is a latent hole, not a failure of the tree under audit; \
         stdout={:?} stderr={:?}",
        stdout(&out),
        stderr(&out)
    );
    let text = stdout(&out);
    assert!(
        text.contains("unparseable-allow-expiry") && text.contains("2026-12-1"),
        "the malformed date must be reported with the offending value; stdout={text:?}"
    );
}
