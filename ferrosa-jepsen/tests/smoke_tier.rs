//! C3.7: Smoke tier end-to-end tests.
//!
//! Two test levels:
//!
//! 1. `smoke_tier_config_produces_combinations` — pure unit test, no containers.
//!    Verifies that the smoke tier `RunConfig` resolves to the expected topologies
//!    and concurrency levels.
//!
//! 2. `smoke_tier_end_to_end` — full pipeline test.
//!    Requires `FERROSA_TEST_CONTAINERS=1` and a running Docker/Podman runtime.
//!    Provisions a 3-node cluster, runs the register workload with the noop
//!    nemesis, checks linearizability, and asserts all combinations pass.

use std::path::PathBuf;

use ferrosa_jepsen::config::{Concurrency, RunConfig, Tier, Topology};

/// C3.7 (unit): Smoke tier config resolves to the expected topologies and
/// concurrency levels without requiring any infrastructure.
#[test]
fn smoke_tier_config_produces_combinations() {
    let config = RunConfig {
        tier: Tier::Smoke,
        topology: None,
        nemesis: None,
        pattern: None,
        driver: None,
        concurrency: None,
        run_id: "smoke-unit".into(),
        output_dir: PathBuf::from("/tmp/ferrosa-jepsen-smoke-unit"),
        fly_regions: vec![],
        alert_webhook: None,
        output_json: false,
    };

    // Smoke tier must use T1 (3-node) topology.
    let topologies = config.topologies();
    assert!(
        topologies.contains(&Topology::T1),
        "smoke tier must include T1 topology; got: {topologies:?}"
    );
    assert_eq!(
        topologies.len(),
        1,
        "smoke tier must use exactly one topology; got: {topologies:?}"
    );

    // Smoke tier must use Low concurrency.
    let concurrency_levels = config.concurrency_levels();
    assert!(
        concurrency_levels.contains(&Concurrency::Low),
        "smoke tier must include Low concurrency; got: {concurrency_levels:?}"
    );
    assert_eq!(
        concurrency_levels.len(),
        1,
        "smoke tier must use exactly one concurrency level; got: {concurrency_levels:?}"
    );

    // Smoke tier run duration is short (5s).
    assert_eq!(
        config.run_duration_secs(),
        5,
        "smoke tier run duration must be 5 seconds"
    );

    // Smoke tier uses phase1 nemesis registry which includes noop.
    let nemesis_reg = ferrosa_jepsen::chaos::NemesisRegistry::phase1();
    assert!(
        nemesis_reg.names().contains(&"noop".to_string()),
        "phase1 nemesis registry must include 'noop'"
    );

    // Smoke tier uses phase1 workload registry which includes register.
    let workload_reg = ferrosa_jepsen::workload::WorkloadRegistry::phase1();
    assert!(
        workload_reg.names().contains(&"register".to_string()),
        "phase1 workload registry must include 'register'"
    );
}

/// C3.7 (E2E): Full smoke tier pipeline — provision cluster, run register
/// workload with noop nemesis, check linearizability, assert all pass.
///
/// Requires:
///   FERROSA_TEST_CONTAINERS=1  — start Docker/Podman Desktop first
///   A Docker/Podman runtime with compose support.
///
/// Run with:
///   FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen --test smoke_tier smoke_tier_end_to_end -- --nocapture
#[tokio::test]
#[ignore = "requires FERROSA_TEST_CONTAINERS=1 + Docker daemon"]
async fn smoke_tier_end_to_end() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!(
            "FERROSA_TEST_CONTAINERS not set — start Docker/Podman Desktop, \
             then re-run with FERROSA_TEST_CONTAINERS=1 \
             cargo test -p ferrosa-jepsen --test smoke_tier smoke_tier_end_to_end"
        );
    }

    let dir = tempfile::tempdir().expect("create temp output dir");

    let run_id = format!("smoke-e2e-{}", &uuid::Uuid::new_v4().to_string()[..8],);

    let config = RunConfig {
        tier: Tier::Smoke,
        // Pin to T1 + noop nemesis + register workload + rust driver for reliability.
        topology: Some(Topology::T1),
        nemesis: Some("noop".to_string()),
        pattern: Some("register".to_string()),
        driver: Some("rust".to_string()),
        concurrency: Some(Concurrency::Low),
        run_id: run_id.clone(),
        output_dir: dir.path().to_path_buf(),
        fly_regions: vec![],
        alert_webhook: None,
        output_json: false,
    };

    let report = ferrosa_jepsen::orchestrator::run(&config)
        .await
        .expect("smoke tier run must succeed");

    assert!(
        report.total > 0,
        "smoke tier must run at least one combination; got 0"
    );

    assert!(
        report.all_passed(),
        "smoke tier must pass on a healthy cluster with noop nemesis. \
         Failures:\n{failures}",
        failures = report
            .failures()
            .iter()
            .map(|f| format!(
                "  {}/{}: {}",
                f.workload,
                f.nemesis,
                f.invariant_error
                    .as_deref()
                    .unwrap_or("linearizability violation")
            ))
            .collect::<Vec<_>>()
            .join("\n"),
    );

    // At least one history JSONL file must have been written.
    let history_files: Vec<_> = std::fs::read_dir(dir.path().join(&run_id))
        .expect("output directory must exist")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "jsonl")
                .unwrap_or(false)
        })
        .collect();

    assert!(
        !history_files.is_empty(),
        "smoke tier must write at least one history JSONL file"
    );

    eprintln!(
        "smoke_tier_end_to_end PASSED: {}/{} combinations passed, {} history files written",
        report.passed,
        report.total,
        history_files.len(),
    );
}
