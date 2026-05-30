//! Regression checks for live-infrastructure test gating.
//!
//! Live infra tests must not turn missing prerequisites into passing cargo-test
//! bodies. They are either absent from the default test target via the
//! `live-infra-tests` feature or they run and panic loudly on missing infra.

use std::path::Path;

fn crate_file(relative: &str) -> String {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest.join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn assert_feature_gated(source: &str, test_name: &str) {
    let marker = format!("async fn {test_name}(");
    let byte_index = source
        .find(&marker)
        .unwrap_or_else(|| panic!("missing live infra test {test_name}"));
    let prefix = &source[..byte_index];
    let nearby_attrs = prefix.lines().rev().take(4).collect::<Vec<_>>().join("\n");
    assert!(
        nearby_attrs.contains(r#"#[cfg(feature = "live-infra-tests")]"#),
        "{test_name} must be behind #[cfg(feature = \"live-infra-tests\")] so default cargo test does not report a missing-infra body as passed; attrs were:\n{nearby_attrs}"
    );
}

#[test]
fn live_infra_tests_are_feature_gated_not_false_passes() {
    let cases = [
        ("src/cluster.rs", "provision_t1_cluster"),
        ("src/cql_session.rs", "rust_driver_connects_to_cluster"),
        (
            "src/docker_provision.rs",
            "orchestrator_docker_cluster_provision",
        ),
        ("src/docker_provision.rs", "orchestrator_cluster_teardown"),
        (
            "src/driver/rust_driver.rs",
            "rust_driver_connects_to_cluster",
        ),
        ("src/firecracker.rs", "provision_single_vm"),
        ("src/ssh.rs", "ssh_execute_command"),
        ("src/ssh.rs", "ssh_upload_file"),
    ];

    for (file, test_name) in cases {
        let source = crate_file(file);
        assert_feature_gated(&source, test_name);
        assert!(
            !source.contains("skipping "),
            "{file} must not print skip-style messages from Rust test bodies; use feature gating plus panic-on-missing-infra instead"
        );
    }
}
