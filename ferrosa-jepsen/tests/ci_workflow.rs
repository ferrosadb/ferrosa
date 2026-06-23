//! Sprint 2 W2.13 — CI workflow assertions.
//!
//! Pins the existence and shape of the `jepsen-smoke` CI job so a future
//! refactor of `.github/workflows/ci.yml` doesn't accidentally drop the
//! Jepsen smoke run. We don't validate the YAML syntax (that's CI's job)
//! — we just assert the keywords are present.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .expect("CARGO_MANIFEST_DIR must be set under cargo test");
    manifest_dir.join("..")
}

fn ci_yaml_path() -> PathBuf {
    repo_root().join(".github/workflows/ci.yml")
}

fn multi_dc_nightly_yaml_path() -> PathBuf {
    repo_root().join(".github/workflows/jepsen-multi-dc-nightly.yml")
}

fn step_body<'a>(yaml: &'a str, step_name: &str) -> &'a str {
    let marker = format!("- name: {step_name}");
    let start = yaml
        .find(&marker)
        .unwrap_or_else(|| panic!("workflow step `{step_name}` not found"));
    let rest = &yaml[start..];
    let end = rest.find("\n      - name:").unwrap_or(rest.len());
    &rest[..end]
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

#[test]
fn multi_dc_nightly_workload_invokes_current_run_subcommand() {
    let path = multi_dc_nightly_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let step = step_body(&yaml, "Run tier-multi-dc 1h bank workload");

    // 12-factor: the run job executes the PREBUILT driver binary (downloaded as
    // an artifact from the build-driver job), via the `run` subcommand. It must
    // NOT compile (`cargo run`) — nothing is built in the Docker/run job.
    assert!(
        step.contains(
            "./driver/ferrosa-jepsen \\\n            run \\\n            --tier multi-dc"
        ),
        "nightly multi-DC workload must run the prebuilt ./driver/ferrosa-jepsen via \
         the `run` subcommand; step was:\n{step}"
    );
    assert!(
        !step.contains("cargo run"),
        "nightly multi-DC run job must NOT compile (no `cargo run`); it runs the \
         prebuilt driver artifact. step was:\n{step}"
    );
    assert!(
        !step.contains("--run-id"),
        "ferrosa-jepsen no longer accepts --run-id on the run command"
    );
}

#[test]
fn multi_dc_nightly_workload_pipeline_fails_loudly_with_tee() {
    let path = multi_dc_nightly_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let step = step_body(&yaml, "Run tier-multi-dc 1h bank workload");

    assert!(
        step.contains("set -o pipefail"),
        "workload step pipes cargo output through tee and must set pipefail so \
         cargo failures are not hidden; step was:\n{step}"
    );
    assert!(
        step.contains("2>&1 | tee tier-multi-dc.log"),
        "workload step must preserve tier-multi-dc.log artifact capture"
    );
}
