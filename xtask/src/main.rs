//! `xtask` — repo automation. Currently hosts the `p0-oom-audit` static AST
//! audit (see `specs/p0-oom-guard/blueprint.md`, Layer 1).
//!
//! Usage:
//!   cargo run -p xtask -- p0-oom-audit [--enforce] [--today YYYY-MM-DD]
//!
//! Default = warn mode: prints findings, exits 0.
//! `--enforce` = exits 1 if any non-whitelisted findings remain.

mod oom_audit;

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Context;

use oom_audit::{
    audit_paths, coverage_findings, expired_allow_findings, Allowlist, Finding, AUDIT_CRATES,
    DEFAULT_TODAY,
};

const ALLOW_FILE: &str = "specs/p0-oom-guard/oom-audit-allow.toml";

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("xtask: error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<ExitCode> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first() else {
        eprintln!(
            "usage: xtask <command>\n  commands: p0-oom-audit [--enforce] [--today YYYY-MM-DD]"
        );
        return Ok(ExitCode::FAILURE);
    };
    match cmd.as_str() {
        "p0-oom-audit" => p0_oom_audit(&args[1..]),
        other => {
            eprintln!("xtask: unknown command `{other}`");
            Ok(ExitCode::FAILURE)
        }
    }
}

/// Parse `--enforce`, `--today YYYY-MM-DD` and `--root <path>` from the
/// subcommand args. `--root` audits another checkout (defaults to this repo).
fn parse_audit_args(args: &[String]) -> anyhow::Result<(bool, String, Option<PathBuf>)> {
    let mut enforce = false;
    let mut today = DEFAULT_TODAY.to_string();
    let mut root_override = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--enforce" => enforce = true,
            "--today" => {
                i += 1;
                today = args
                    .get(i)
                    .context("--today requires a YYYY-MM-DD argument")?
                    .clone();
            }
            "--root" => {
                i += 1;
                root_override = Some(PathBuf::from(
                    args.get(i).context("--root requires a path argument")?,
                ));
            }
            other => anyhow::bail!("p0-oom-audit: unexpected argument `{other}`"),
        }
        i += 1;
    }
    Ok((enforce, today, root_override))
}

/// Repo root: the workspace dir this binary was built in (cargo sets CARGO_MANIFEST_DIR
/// to the xtask crate; its parent is the workspace root).
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn p0_oom_audit(args: &[String]) -> anyhow::Result<ExitCode> {
    let (enforce, today, root_override) = parse_audit_args(args)?;
    let root = root_override.unwrap_or_else(repo_root);

    let allow_path = root.join(ALLOW_FILE);
    let allow = Allowlist::load(&allow_path)
        .with_context(|| format!("loading allowlist {}", allow_path.display()))?;

    let roots: Vec<PathBuf> = AUDIT_CRATES
        .iter()
        .map(|c| root.join(c).join("src"))
        .collect();

    let mut findings = audit_paths(&roots, &allow);
    findings.extend(expired_allow_findings(&allow, &today));
    // Coverage is part of the verdict: an unclassified crate is a blind spot,
    // and a blind spot must not be reported as a clean run.
    findings.extend(coverage_findings(&root));
    findings.sort_by(|a, b| (a.path.as_str(), a.line).cmp(&(b.path.as_str(), b.line)));

    print_findings(&findings, &root);

    let mode = if enforce { "enforce" } else { "warn" };
    eprintln!(
        "\np0-oom-audit: {} finding(s) [{} mode, today={}]",
        findings.len(),
        mode,
        today
    );

    if enforce && !findings.is_empty() {
        eprintln!("p0-oom-audit: FAIL — non-whitelisted findings present");
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

fn print_findings(findings: &[Finding], root: &std::path::Path) {
    if findings.is_empty() {
        println!("p0-oom-audit: no findings");
        return;
    }
    let root_str = root.to_string_lossy();
    for f in findings {
        // Show paths relative to the repo root for readable output.
        let rel = f.path.strip_prefix(root_str.as_ref()).unwrap_or(&f.path);
        let rel = rel.trim_start_matches('/');
        println!("{}:{}  [{}]  {}", rel, f.line, f.rule, f.message);
    }
}
