//! Smoke tests for the ferrosa-loadgen binary.
//!
//! These tests run the actual binary as a subprocess and verify:
//! 1. It starts and exits cleanly
//! 2. Output includes all expected report sections
//! 3. Resource leak detection runs and reports
//! 4. Exit code is 0 for passing tests

use std::process::Command;

fn require_loadgen_binary() {
    if std::env::var("FERROSA_TEST_LOADGEN").is_err() {
        panic!(
            "FERROSA_TEST_LOADGEN not set — these tests spawn cargo run as a subprocess \
             and take several minutes to compile. Run with FERROSA_TEST_LOADGEN=1 for local testing."
        );
    }
}

fn cargo_bin() -> Command {
    let mut cmd = Command::new(env!("CARGO"));
    cmd.args(["run", "-p", "ferrosa-loadgen", "--"]);
    cmd
}

#[test]
#[ignore = "requires FERROSA_TEST_LOADGEN=1; spawns cargo run subprocess (takes minutes)"]
fn binary_list_profiles() {
    require_loadgen_binary();
    let output = cargo_bin()
        .arg("--list-profiles")
        .output()
        .expect("failed to run binary");

    assert!(output.status.success(), "exit code should be 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("read_heavy"), "should list read_heavy");
    assert!(stdout.contains("balanced"), "should list balanced");
    assert!(stdout.contains("write_heavy"), "should list write_heavy");
    assert!(
        stdout.contains("compaction_stress"),
        "should list compaction_stress"
    );
}

#[test]
#[ignore = "requires FERROSA_TEST_LOADGEN=1; spawns cargo run subprocess (takes minutes)"]
fn binary_help() {
    require_loadgen_binary();
    let output = cargo_bin()
        .arg("--help")
        .output()
        .expect("failed to run binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--tui"), "should document --tui flag");
    assert!(
        stdout.contains("--profile"),
        "should document --profile flag"
    );
    assert!(
        stdout.contains("--duration"),
        "should document --duration flag"
    );
}

#[test]
#[ignore = "requires FERROSA_TEST_LOADGEN=1; spawns cargo run subprocess (takes minutes)"]
fn binary_short_load_test() {
    require_loadgen_binary();
    let dir = tempfile::tempdir().expect("create temp dir");

    // Use read_heavy profile — lightest workload. Duration must be long enough
    // for the resource monitor to collect 5+ samples (4 warmup + 1 baseline,
    // at 500ms intervals = ~3s minimum, plus engine startup time).
    let output = cargo_bin()
        .args([
            "--profile",
            "read_heavy",
            "--duration",
            "10",
            "--data-dir",
            dir.path().to_str().unwrap(),
        ])
        .output()
        .expect("failed to run binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Print output for debugging if test fails.
    if !output.status.success() {
        eprintln!("STDOUT:\n{stdout}");
        eprintln!("STDERR:\n{stderr}");
    }

    // Verify report sections are present regardless of exit code.
    // The binary always prints a report, even if integrity fails.
    assert!(
        stdout.contains("Throughput"),
        "report should have Throughput section"
    );
    assert!(
        stdout.contains("Latency"),
        "report should have Latency section"
    );
    assert!(
        stdout.contains("Storage"),
        "report should have Storage section"
    );
    assert!(
        stdout.contains("Memory (RSS)"),
        "report should have Memory section"
    );
    assert!(
        stdout.contains("Integrity"),
        "report should have Integrity section"
    );
    assert!(
        stdout.contains("Resource Leak Detection"),
        "report should have Resource Leak Detection section"
    );
    assert!(
        stdout.contains("Leak verdict:"),
        "report should include leak verdict"
    );

    // Should NOT have been aborted due to resource limits.
    assert!(
        !stdout.contains("ABORTED"),
        "short test should not trigger resource abort"
    );
    // Exit code 2 = resource abort, which must not happen.
    assert_ne!(
        output.status.code(),
        Some(2),
        "should not exit 2 (resource abort)"
    );
}

#[test]
#[ignore = "requires FERROSA_TEST_LOADGEN=1; spawns cargo run subprocess (takes minutes)"]
fn binary_write_heavy_no_resource_abort() {
    require_loadgen_binary();
    let dir = tempfile::tempdir().expect("create temp dir");

    let output = cargo_bin()
        .args([
            "--profile",
            "write_heavy",
            "--duration",
            "10",
            "--data-dir",
            dir.path().to_str().unwrap(),
        ])
        .output()
        .expect("failed to run binary");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Resource leak detection should be present in report.
    assert!(
        stdout.contains("Leak verdict"),
        "resource report should be present"
    );

    // Should NOT have been aborted due to resource limits.
    assert!(
        !stdout.contains("ABORTED"),
        "write_heavy should not trigger resource abort"
    );
    assert_ne!(
        output.status.code(),
        Some(2),
        "should not exit 2 (resource abort)"
    );
}
