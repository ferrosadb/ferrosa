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

fn postgres_jepsen_compose_path() -> PathBuf {
    repo_root().join("ferrosa-jepsen/tests/docker/jepsen-cluster.yml")
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
fn postgres_jepsen_compose_advertises_host_reachable_cql_ports() {
    let path = postgres_jepsen_compose_path();
    let compose =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let parsed: serde_yaml::Value =
        serde_yaml::from_str(&compose).expect("parse PostgreSQL Jepsen compose YAML");
    let services = parsed
        .get("services")
        .and_then(serde_yaml::Value::as_mapping)
        .expect("services mapping");

    for (node, port) in [("node1", 49042), ("node2", 49043), ("node3", 49044)] {
        let service = services
            .get(serde_yaml::Value::String(node.into()))
            .unwrap_or_else(|| panic!("service {node} missing"));
        let environment = service
            .get("environment")
            .and_then(serde_yaml::Value::as_mapping)
            .unwrap_or_else(|| panic!("{node} missing environment"));
        let actual = environment
            .get(serde_yaml::Value::String("FERROSA_CQL_BROADCAST".into()))
            .and_then(serde_yaml::Value::as_str);

        assert_eq!(
            actual,
            Some(format!("127.0.0.1:{port}").as_str()),
            "{node} must advertise the CQL endpoint reachable by the host workload driver"
        );
    }
}

#[test]
fn postgres_fault_workflow_waits_for_all_nodes_to_form_before_creating_role() {
    let path = ci_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let readiness = step_body(&yaml, "Wait for PostgreSQL cluster formation");
    let readiness_position = yaml
        .find("- name: Wait for PostgreSQL cluster formation")
        .expect("PostgreSQL cluster readiness step exists");
    let role_position = yaml
        .find("- name: Create the test CQL role")
        .expect("test CQL role setup step exists");

    assert!(
        readiness.contains("/api/cluster/ring")
            && readiness.contains("Normal")
            && readiness.contains("SECONDS + 120"),
        "PostgreSQL fault setup must wait for the three-node ring to form; step was:\n{readiness}"
    );
    assert!(
        readiness_position < role_position,
        "the test role must be created only after cluster formation is verified"
    );
}

/// CQL-T467ci-01: `mode == "cluster"` and a Normal ring do not mean DDL is
/// cluster-routed: the node keeps `DdlPath::Direct` until Raft has a leader,
/// and `ALTER KEYSPACE system_auth` on that path is refused. The readiness wait
/// must therefore also require the live DDL path to be `cluster`.
#[test]
fn postgres_fault_workflow_waits_for_cluster_ddl_path_before_system_auth_alter() {
    let path = ci_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let readiness = step_body(&yaml, "Wait for PostgreSQL cluster formation");

    assert!(
        readiness.contains(".ddl_path == \"cluster\""),
        "the wait must require every node's DDL path to be cluster; step was:\n{readiness}"
    );
}

#[test]
fn postgres_fault_workflow_replicates_system_auth_before_role_creation() {
    let path = ci_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let auth_setup = step_body(&yaml, "Replicate PostgreSQL roles to all nodes");
    let auth_position = yaml
        .find("- name: Replicate PostgreSQL roles to all nodes")
        .expect("system_auth replication setup step exists");
    let role_position = yaml
        .find("- name: Create the test CQL role")
        .expect("test CQL role setup step exists");

    assert!(
        auth_setup.contains("ALTER KEYSPACE system_auth")
            && auth_setup.contains("'replication_factor': 3"),
        "PostgreSQL role lookup must find the test user on every node; step was:\n{auth_setup}"
    );
    assert!(
        auth_position < role_position,
        "system_auth replication must be configured before creating the test role"
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

    // This is one bounded, named correctness experiment—not the entire
    // workload × nemesis matrix. Without these filters each combination gets
    // the full JEPSEN_RUN_DURATION_SECS budget and a nominal 10-minute nightly
    // cannot complete in its 45-minute job window.
    assert!(
        step.contains("--pattern bank"),
        "nightly multi-DC must run only the bank workload; step was:\n{step}"
    );
    assert!(
        step.contains("--nemesis dc-partition+dc-slow"),
        "nightly multi-DC must run its named WAN nemesis; step was:\n{step}"
    );
    assert!(
        step.contains("FERROSA_TEST_CLUSTER_NODES"),
        "nightly multi-DC must provide the six already-running T3 CQL endpoints so \
         the driver cannot fall back to MockCqlSession; step was:\n{step}"
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

#[test]
fn multi_dc_nightly_preflights_each_node_directly_over_cql_and_http() {
    let path = multi_dc_nightly_yaml_path();
    let yaml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let step = step_body(&yaml, "Bring up T3 (3+3 dual-DC) topology");

    assert!(
        !yaml.contains("- name: Install cqlsh")
            && !step.contains("cqlsh --")
            && !step.contains("pip install cqlsh"),
        "the topology preflight must use socket-pinned checks; cqlsh discovers and load-balances across peers"
    );
    assert!(
        step.contains("29042|29090|")
            && step.contains("29043|29091|")
            && step.contains("29044|29092|")
            && step.contains("29142|29190|")
            && step.contains("29143|29191|")
            && step.contains("29144|29192|"),
        "the topology step must pair every node's host-mapped CQL and HTTP endpoints; step was:\n{step}"
    );
    assert!(
        step.contains("socket.create_connection")
            && step.contains("OPTIONS_OPCODE = 0x05")
            && step.contains("SUPPORTED_OPCODE = 0x06")
            && step.contains("response_version != 0x84")
            && step.contains("supported_key_count"),
        "each direct CQL socket must complete a native-protocol v4 OPTIONS/SUPPORTED handshake; step was:\n{step}"
    );
    assert!(
        step.contains("/api/cluster/status")
            && step.contains("expected_host_id")
            && step.contains("mode")
            && step.contains("cluster"),
        "each direct endpoint must report its configured host ID and cluster mode; step was:\n{step}"
    );
    assert!(
        step.contains("/api/cluster/ring")
            && step.contains("expected_ring_ids")
            && step.contains("Normal"),
        "each node must see exactly its three expected DC ring members in Normal state; step was:\n{step}"
    );
    assert!(
        step.contains("preflight_deadline=$((SECONDS + 120))")
            && step.contains("while (( SECONDS < preflight_deadline )); do")
            && step.contains("T3 topology did not converge within 120s"),
        "direct status and ring validation must share a bounded convergence retry; step was:\n{step}"
    );
    assert!(
        !step.contains("schema_version"),
        "pre-DDL schema UUIDs are process-local and must not gate topology readiness; step was:\n{step}"
    );
}
