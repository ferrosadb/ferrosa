//! Gap 11 — 3-node cluster topology invariants.
//!
//! A correctly-formed ferrosa cluster must satisfy three observable
//! invariants visible to any CQL driver:
//!
//! 1. **Distinct host_ids** — every node has its own UUID.
//! 2. **Distinct broadcast addresses** — drivers must be able to tell
//!    nodes apart for connection pooling and load balancing.
//! 3. **Distinct token sets that cover the ring** — under
//!    `SimpleStrategy` with `replication_factor=1`, every key is owned
//!    by exactly one node and the union of all tokens covers the entire
//!    `i64` range so the partitioner can route every key.
//!
//! These invariants are what NoSQLBench (and any cassandra-driver
//! client) relies on to spread load. Without them the cluster behaves
//! like single-node + standbys — which is what the diabolical 3-node
//! benchmark caught (one node held 1.4 GiB of memtable, two were
//! effectively empty).  See
//! `ferrosa-nosqlbench/docs/initial-gaps-found.md`.
//!
//! The test is gated on `FERROSA_TEST_CONTAINERS=1` per the project's
//! test policy; without containers it panics with setup instructions
//! instead of silently passing.
//!
//! ## Running locally
//!
//! ```bash
//! # bring up the 3-node cluster
//! ../scripts/test-cluster-up-ci.sh
//!
//! # run the test
//! FERROSA_TEST_CONTAINERS=1 \
//!   cargo test -p ferrosa-jepsen --features live-infra-tests \
//!   --test cluster_topology_invariants \
//!   -- --nocapture
//! ```

#![cfg(feature = "live-infra-tests")]

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

/// Hosts the test cluster's CQL endpoints listen on (mirrors
/// `scripts/test-cluster-up-ci.sh`).
const CLUSTER_HOSTS: &[&str] = &["127.0.0.1:9042", "127.0.0.1:9043", "127.0.0.1:9044"];

/// Helper: spawn `cqlsh` against `host:port` and capture `key=value`
/// pairs from a single-row `SELECT host_id, broadcast_address, tokens
/// FROM system.local`.  Returns `None` if cqlsh isn't installed or the
/// node is unreachable — callers panic with a setup hint.
fn query_local(host: SocketAddr) -> Option<NodeIdentity> {
    let output = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "host",
            "cassandra:5.0",
            "cqlsh",
            "--no-color",
            &host.ip().to_string(),
            &host.port().to_string(),
            "-e",
            "SELECT host_id, broadcast_address, tokens FROM system.local;",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_cqlsh_local_row(&stdout)
}

#[derive(Debug)]
struct NodeIdentity {
    host_id: String,
    broadcast_address: String,
    tokens: Vec<String>,
}

fn parse_cqlsh_local_row(stdout: &str) -> Option<NodeIdentity> {
    // cqlsh emits a 4-line table:
    //  host_id | broadcast_address | tokens
    // --------+-------------------+--------
    //  <uuid> | <inet>            | {...}
    let mut data = None;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('-') || trimmed.contains("host_id") {
            continue;
        }
        if trimmed.contains('|') {
            data = Some(trimmed.to_string());
            break;
        }
    }
    let row = data?;
    let cells: Vec<&str> = row.split('|').map(|s| s.trim()).collect();
    if cells.len() < 3 {
        return None;
    }
    let tokens_raw = cells[2]
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim();
    let tokens: Vec<String> = if tokens_raw.is_empty() {
        Vec::new()
    } else {
        tokens_raw
            .split(',')
            .map(|t| t.trim().trim_matches('\'').to_string())
            .collect()
    };
    Some(NodeIdentity {
        host_id: cells[0].to_string(),
        broadcast_address: cells[1].to_string(),
        tokens,
    })
}

fn require_containers(test_name: &str) {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!(
            "FERROSA_TEST_CONTAINERS not set — bring the test cluster up first:\n\
             \n\
             scripts/test-cluster-up-ci.sh\n\
             FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen \\\n\
                 --features live-infra-tests \\\n\
                 --test cluster_topology_invariants {test_name} -- --nocapture\n"
        );
    }
}

/// Gap 11.1: each node in a 3-node cluster must report a distinct
/// `host_id` in `system.local`.  Pre-fix, two of three nodes shared
/// `11111111-1111-1111-1111-111111111111`, masking them as the same
/// physical node to the driver.
#[test]
fn three_node_cluster_reports_three_distinct_host_ids() {
    require_containers("three_node_cluster_reports_three_distinct_host_ids");
    std::thread::sleep(Duration::from_secs(2));
    let identities: Vec<NodeIdentity> = CLUSTER_HOSTS
        .iter()
        .map(|h| {
            let addr: SocketAddr = h.parse().expect("parse cluster host");
            query_local(addr)
                .unwrap_or_else(|| panic!("could not query {h} system.local — is the cluster up?"))
        })
        .collect();
    let unique: HashSet<&str> = identities.iter().map(|i| i.host_id.as_str()).collect();
    assert_eq!(
        unique.len(),
        3,
        "expected 3 distinct host_ids across the cluster, got {unique:?} from {identities:?}"
    );
}

/// Gap 11.2: each node must report a distinct `broadcast_address`.
/// Pre-fix, all three reported `127.0.0.1`, making the driver unable to
/// distinguish them for load-balancing.
#[test]
fn three_node_cluster_reports_three_distinct_broadcast_addresses() {
    require_containers("three_node_cluster_reports_three_distinct_broadcast_addresses");
    std::thread::sleep(Duration::from_secs(2));
    let identities: Vec<NodeIdentity> = CLUSTER_HOSTS
        .iter()
        .map(|h| {
            let addr: SocketAddr = h.parse().expect("parse cluster host");
            query_local(addr)
                .unwrap_or_else(|| panic!("could not query {h} system.local — is the cluster up?"))
        })
        .collect();
    let unique: HashSet<&str> = identities
        .iter()
        .map(|i| i.broadcast_address.as_str())
        .collect();
    assert_eq!(
        unique.len(),
        3,
        "expected 3 distinct broadcast_addresses, got {unique:?} from {identities:?}"
    );
}

/// Gap 11.3: tokens across the 3 nodes must form a distinct, non-empty
/// set that covers the full i64 ring.  Pre-fix every node reported
/// `tokens=['0']`, so all data hashed to one owner.
#[test]
fn three_node_cluster_tokens_are_distinct_and_cover_ring() {
    require_containers("three_node_cluster_tokens_are_distinct_and_cover_ring");
    std::thread::sleep(Duration::from_secs(2));
    let identities: Vec<NodeIdentity> = CLUSTER_HOSTS
        .iter()
        .map(|h| {
            let addr: SocketAddr = h.parse().expect("parse cluster host");
            query_local(addr)
                .unwrap_or_else(|| panic!("could not query {h} system.local — is the cluster up?"))
        })
        .collect();
    let all_tokens: Vec<&str> = identities
        .iter()
        .flat_map(|i| i.tokens.iter().map(String::as_str))
        .collect();
    let unique: HashSet<&str> = all_tokens.iter().copied().collect();
    assert!(
        all_tokens.len() >= 3,
        "expected at least 3 tokens total across the cluster, got {all_tokens:?}"
    );
    assert_eq!(
        unique.len(),
        all_tokens.len(),
        "expected every token to be unique across nodes, but {all_tokens:?} \
         has duplicates → cluster behaves as single-owner. (full identities: {identities:?})"
    );
    // The union of token ranges must reach both ends of the i64 ring.
    // Cassandra's Murmur3Partitioner range is i64::MIN..=i64::MAX; with
    // vnodes set, several tokens per node distribute coverage.  With a
    // single-token-per-node config, three tokens divide the ring into
    // three arcs and that's still a valid cover.
    let parsed: Result<Vec<i64>, _> = all_tokens.iter().map(|t| t.parse::<i64>()).collect();
    let parsed = parsed.expect("tokens must parse as i64");
    assert!(
        parsed.iter().any(|t| *t < 0) && parsed.iter().any(|t| *t > 0),
        "tokens must span negative and positive halves of the i64 ring, got {parsed:?}"
    );
}
