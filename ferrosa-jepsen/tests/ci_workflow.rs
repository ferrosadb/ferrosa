//! Sprint 2 W2.13 — CI workflow assertions.
//!
//! Pins the existence and shape of the `jepsen-smoke` CI job so a future
//! refactor of `.github/workflows/ci.yml` doesn't accidentally drop the
//! Jepsen smoke run. We don't validate the YAML syntax (that's CI's job)
//! — we just assert the keywords are present.

use std::path::PathBuf;

fn ci_yaml_path() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .expect("CARGO_MANIFEST_DIR must be set under cargo test");
    manifest_dir.join("../.github/workflows/ci.yml")
}

#[test]
fn jepsen_smoke_job_exists_in_ci_yaml() {
    let path = ci_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

    assert!(
        yaml.contains("jepsen-smoke"),
        ".github/workflows/ci.yml must define a `jepsen-smoke` job (Sprint 2 W2.13). \
         Without it, every PR would still skip ferrosa-jepsen and the structural-invariant \
         check could regress unnoticed."
    );

    // The job must run with FERROSA_TEST_CONTAINERS=1 so the Docker-backed
    // path is exercised. The test_policy in CLAUDE.md panics for missing
    // infra, which prevents accidental green runs without the env var.
    assert!(
        yaml.contains("FERROSA_TEST_CONTAINERS"),
        "jepsen-smoke job must export FERROSA_TEST_CONTAINERS=1 so ferrosa-jepsen \
         tests panic loudly instead of silently no-oping"
    );
}

/// W2.13 negative companion: the legacy `--exclude ferrosa-jepsen` flag
/// must remain in the *general-purpose* `test` job (so the Docker-free
/// stages don't try to spin Docker). Asserts both behaviors.
#[test]
fn legacy_test_job_still_excludes_ferrosa_jepsen() {
    let path = ci_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

    assert!(
        yaml.contains("--exclude ferrosa-jepsen"),
        "the general-purpose `test` job must keep `--exclude ferrosa-jepsen` so it \
         doesn't try to spin Docker. The new jepsen-smoke job runs the suite separately."
    );
}
