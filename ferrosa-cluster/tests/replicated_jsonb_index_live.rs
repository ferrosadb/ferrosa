//! Live-infra check for FM-70 / t_1f2741a0: after `CREATE INDEX` on node 1, an
//! indexed query coordinated by node 3 returns every matching row.
//!
//! The in-process twin is `replicated_create_index.rs`; this one needs a real
//! cluster because the failure is cluster-shaped (DDL applied on one node,
//! reads landing on another, no restart in between).
//!
//! Gating (repo test policy): behind the `live-infra-tests` feature, and with
//! the feature on but `FERROSA_TEST_CLUSTER_NODES` unset it `panic!`s with setup
//! instructions instead of passing silently.
//!
//! ```bash
//! FERROSA_TEST_CLUSTER_NODES=10.0.0.1:9042,10.0.0.2:9042,10.0.0.3:9042 \
//!   cargo test -p ferrosa-cluster --features live-infra-tests \
//!   --test replicated_jsonb_index_live -- --nocapture
//! ```
//!
//! The index here is on a plain text column: the replication path under test
//! (`ddl_path::build_replicated_index`) is index-type agnostic, and a jsonb
//! column type does not exist yet. Swap the column type once it does.

#![cfg(feature = "live-infra-tests")]

use std::process::Command;

const SETUP: &str = "live-infra-tests is enabled but FERROSA_TEST_CLUSTER_NODES is not set.\n\
    Point it at a running 3-node ferrosa cluster as comma-separated CQL addresses:\n\
      FERROSA_TEST_CLUSTER_NODES=host1:9042,host2:9042,host3:9042 \\\n\
        cargo test -p ferrosa-cluster --features live-infra-tests \\\n\
        --test replicated_jsonb_index_live -- --nocapture\n\
    `cqlsh` must be on PATH.";

fn nodes() -> Vec<(String, String)> {
    let raw = std::env::var("FERROSA_TEST_CLUSTER_NODES").unwrap_or_else(|_| panic!("{SETUP}"));
    let nodes: Vec<(String, String)> = raw
        .split(',')
        .map(|addr| match addr.trim().rsplit_once(':') {
            Some((host, port)) => (host.to_string(), port.to_string()),
            None => (addr.trim().to_string(), "9042".to_string()),
        })
        .collect();
    assert!(
        nodes.len() >= 3,
        "FERROSA_TEST_CLUSTER_NODES must list at least 3 nodes, got {}",
        nodes.len()
    );
    nodes
}

/// Run one CQL statement on one node through `cqlsh`, panicking with the
/// node and statement on any failure. Returns stdout.
fn cql(node: &(String, String), statement: &str) -> String {
    let out = Command::new("cqlsh")
        .args([&node.0, &node.1, "-e", statement])
        .output()
        .unwrap_or_else(|e| panic!("could not run cqlsh ({e}); it must be on PATH"));
    assert!(
        out.status.success(),
        "cqlsh on {}:{} failed for `{statement}`: {}",
        node.0,
        node.1,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn replicated_jsonb_index_builds_on_every_node() {
    let nodes = nodes();
    let (first, third) = (&nodes[0], &nodes[2]);

    cql(
        first,
        "CREATE KEYSPACE IF NOT EXISTS fm70_live WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': 3}",
    );
    cql(
        first,
        "CREATE TABLE IF NOT EXISTS fm70_live.docs (id text PRIMARY KEY, body text)",
    );
    for (id, body) in [("a", "alice"), ("b", "alice"), ("c", "bob")] {
        cql(
            first,
            &format!("INSERT INTO fm70_live.docs (id, body) VALUES ('{id}', '{body}')"),
        );
    }
    cql(
        first,
        "CREATE INDEX IF NOT EXISTS fm70_docs_body ON fm70_live.docs (body)",
    );

    // No restart, no waiting on anything but the DDL: node 3 must answer.
    let rows = cql(third, "SELECT id FROM fm70_live.docs WHERE body = 'alice'");
    let ids: Vec<&str> = rows
        .lines()
        .map(str::trim)
        .filter(|l| *l == "a" || *l == "b")
        .collect();
    assert_eq!(
        ids.len(),
        2,
        "node 3 must return both matching rows after CREATE INDEX on node 1; got:\n{rows}"
    );
}
