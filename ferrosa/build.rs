//! Embed the commit this build came from.
//!
//! A Sentry event that names only a version cannot answer the question actually
//! asked when a report arrives: is this the build that already has the fix?
//! Nightly cuts several builds per version.
//!
//! `FERROSA_BUILD_SHA` from the environment wins — a release is built from a
//! tarball or container with no `.git` to ask, and CI knows what it checked
//! out. `unknown` is the honest last resort: a stale SHA would be believed.

use std::process::Command;

fn main() {
    let sha = std::env::var("FERROSA_BUILD_SHA")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .or_else(git_short_sha)
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=FERROSA_BUILD_SHA={sha}");
    println!("cargo:rerun-if-env-changed=FERROSA_BUILD_SHA");
    println!("cargo:rerun-if-changed=../.git/HEAD");
}

fn git_short_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if sha.is_empty() {
        return None;
    }
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .is_some_and(|out| !out.stdout.is_empty());
    Some(if dirty { format!("{sha}-dirty") } else { sha })
}
