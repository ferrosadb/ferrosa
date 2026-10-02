//! Live-infra reproduction: a node that missed DDL while it was stopped must
//! converge on the cluster's schema when it comes back.
//!
//! ## The failure
//!
//! Restart one node of a 3-node cluster, create a table while it is down, then
//! bring it back. The restarted node comes up holding a stale schema: it never
//! learns the table it missed, so `SELECT` against that table on that node fails
//! with "unconfigured table" (or "keyspace not found") indefinitely -- not for a
//! moment during propagation, but permanently.
//!
//! It is permanent because schema handling on rejoin is **push-only**. Each node
//! offers its own local schema to the cluster, so a node that is *missing* a
//! table re-offers the tables it already has (each a no-op) and has no path to
//! learn the ones it lacks. Nothing ever sends it the cluster's schema.
//!
//! The consequence reaches further than one missing table: a CQL client that
//! performs a statement needing cluster-wide schema agreement compares every
//! node's schema version and waits for them to match. One divergent node can
//! never match, so every fresh session that does such a statement times out,
//! even when the statement itself is a satisfied `IF NOT EXISTS`. A process
//! holding a long-lived session keeps working, which masks the condition.
//!
//! ## Why this is a live test
//!
//! The failure is cluster-shaped: real nodes, real RPC, a real restart, and a
//! real divergence observed across processes. An in-process test cannot express
//! "this node's schema is stale relative to that node's".
//!
//! ## Gating (repo test policy)
//!
//! Behind the `live-infra-tests` feature. With the feature on but
//! `FERROSA_TEST_CLUSTER_NODES` unset it `panic!`s with setup instructions
//! rather than passing silently.
//!
//! ```bash
//! scripts/test-cluster-up.sh --keep      # brings up a 3-node cluster, prints the addresses
//! FERROSA_TEST_CLUSTER_NODES=127.0.0.1:30042,127.0.0.1:30043,127.0.0.1:30044 \
//!   cargo test -p ferrosa-cluster --features live-infra-tests \
//!   --test cluster_schema_convergence_live -- --nocapture
//! ```
//!
//! `cqlsh` and `podman` must be on PATH. The node container to restart is
//! DISCOVERED from running podman containers by node-name suffix, because the
//! compose project name is a convention that differs between the local up-script
//! and CI and is not exported to the test. `FERROSA_TEST_CLUSTER_PROJECT`
//! overrides the discovery when set.

#![cfg(feature = "live-infra-tests")]

use std::process::Command;
use std::time::{Duration, Instant};

const SETUP: &str = "live-infra-tests is enabled but FERROSA_TEST_CLUSTER_NODES is not set.\n\
    Bring up a 3-node cluster and point the test at it:\n\
      scripts/test-cluster-up.sh --keep\n\
      FERROSA_TEST_CLUSTER_NODES=127.0.0.1:30042,127.0.0.1:30043,127.0.0.1:30044 \\\n\
        cargo test -p ferrosa-cluster --features live-infra-tests \\\n\
        --test cluster_schema_convergence_live -- --nocapture\n\
    `cqlsh` and `podman` must be on PATH.";

/// CQL addresses, in node order (index 0 is node 1).
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

/// The index of the node container to restart (node 3, 1-based).
const NODE_INDEX: usize = 3;

/// Find the container backing node `NODE_INDEX`, without being told the project.
///
/// The project name is a local convention -- the local up-script uses one, CI
/// another (`ferrosa-test-ci`) -- and neither exports it to the test. Only
/// `FERROSA_TEST_CLUSTER_NODES` is exported. So the container is discovered by
/// asking podman for running containers whose name ends in the node suffix,
/// accepting either separator, rather than guessing the project.
fn node_container() -> String {
    // An explicit override still wins, for a deliberately unusual setup.
    if let Ok(project) = std::env::var("FERROSA_TEST_CLUSTER_PROJECT") {
        for candidate in [
            format!("{project}_node{NODE_INDEX}_1"),
            format!("{project}-node{NODE_INDEX}-1"),
        ] {
            if container_exists(&candidate) {
                return candidate;
            }
        }
        panic!(
            "FERROSA_TEST_CLUSTER_PROJECT={project} was given but neither \
             {project}_node{NODE_INDEX}_1 nor {project}-node{NODE_INDEX}-1 exists"
        );
    }

    let out = Command::new("podman")
        .args(["ps", "--format", "{{.Names}}"])
        .output()
        .unwrap_or_else(|e| panic!("could not run podman ps: {e}"));
    assert!(
        out.status.success(),
        "podman ps failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let names = String::from_utf8_lossy(&out.stdout);

    // `ferrosa-test-w1_node3_1` (compose underscore) or `ferrosa-test-ci-node3-1`.
    let suffixes = [
        format!("_node{NODE_INDEX}_1"),
        format!("-node{NODE_INDEX}-1"),
    ];
    let matches: Vec<&str> = names
        .lines()
        .map(str::trim)
        .filter(|n| suffixes.iter().any(|s| n.ends_with(s.as_str())))
        .collect();

    match matches.as_slice() {
        [] => panic!(
            "no running container for node {NODE_INDEX}. Expected one whose name \
             ends in _node{NODE_INDEX}_1 or -node{NODE_INDEX}-1. Running: {:?}",
            names.lines().map(str::trim).collect::<Vec<_>>()
        ),
        [only] => (*only).to_string(),
        many => panic!(
            "node {NODE_INDEX} is ambiguous, {} containers match: {many:?}",
            many.len()
        ),
    }
}

fn container_exists(name: &str) -> bool {
    Command::new("podman")
        .args(["container", "exists", name])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn podman(args: &[&str]) -> String {
    let out = Command::new("podman")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("could not run podman {args:?}: {e}"));
    assert!(
        out.status.success(),
        "podman {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Run one CQL statement on one node, returning `(succeeded, combined output)`.
///
/// Deliberately does NOT panic on failure: the reproduction needs to assert that
/// a statement *fails*, and which way it failed is the evidence.
fn try_cql(node: &(String, String), statement: &str) -> (bool, String) {
    let out = Command::new("cqlsh")
        .args([&node.0, &node.1, "-e", statement])
        .output()
        .unwrap_or_else(|e| panic!("could not run cqlsh ({e}); it must be on PATH"));
    let mut combined = String::from_utf8_lossy(&out.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), combined)
}

fn cql(node: &(String, String), statement: &str) -> String {
    let (ok, out) = try_cql(node, statement);
    assert!(
        ok,
        "cqlsh on {}:{} failed for `{statement}`: {out}",
        node.0, node.1
    );
    out
}

/// True while a table is visible on that node.
fn table_is_visible(node: &(String, String), keyspace: &str, table: &str) -> bool {
    let (ok, _) = try_cql(node, &format!("SELECT * FROM {keyspace}.{table}"));
    ok
}

/// Wait until the node answers CQL again after a restart.
fn wait_until_serving(node: &(String, String), timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if try_cql(node, "SELECT release_version FROM system.local").0 {
            return true;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    false
}

/// Restarts a stopped node on drop, including while unwinding from a panic.
///
/// A reproduction that stops a cluster node MUST put it back even when it
/// fails. Otherwise a failing test leaves the cluster short a node, and the next
/// run (or the next person) inherits a degraded fixture and cannot tell why.
struct RestartOnDrop {
    container: String,
    started: bool,
}

impl RestartOnDrop {
    fn new(container: String) -> Self {
        Self {
            container,
            started: false,
        }
    }

    fn start(&mut self) {
        let _ = Command::new("podman")
            .args(["start", &self.container])
            .status();
        self.started = true;
    }
}

impl Drop for RestartOnDrop {
    fn drop(&mut self) {
        if !self.started {
            let _ = Command::new("podman")
                .args(["start", &self.container])
                .status();
        }
    }
}

/// Run a statement, retrying while the cluster refuses for a reason that is
/// expected to clear.
///
/// Taking a node down makes the internode lane reconnect, and a statement issued
/// during that window is refused with `net: lane is reconnecting; retry later`.
/// That is transient and is not the behaviour under test, so the reproduction
/// waits it out rather than failing on it. Bounded, so a statement that is
/// genuinely refused still fails the test.
fn cql_retrying(node: &(String, String), statement: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        let (ok, out) = try_cql(node, statement);
        if ok {
            return out;
        }
        let transient = out.contains("lane is reconnecting")
            || out.contains("retry later")
            || out.contains("NoHostAvailable");
        if !transient || Instant::now() >= deadline {
            panic!(
                "cqlsh on {}:{} failed for `{statement}` after waiting {timeout:?}: {out}",
                node.0, node.1
            );
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// A node that missed a `CREATE TABLE` while it was stopped must see that table
/// after it comes back, without operator action.
///
/// Invariant: a table created while this node was down is visible on it once it
/// rejoins. A node that re-offers only the schema it already has can never
/// satisfy this, which is the bug.
///
/// `#[ignore]` because it needs a live cluster, matching the repo's convention
/// for cluster-gated tests (`--ignored` selects it). The cluster must be brought
/// up with a small `FERROSA_RAFT_SNAPSHOT_LOGS`, which the compose file sets, so
/// the missed DDL can be pushed outside the retained log.
#[test]
#[ignore = "needs a live cluster (FERROSA_TEST_CLUSTER_NODES); run with -- --ignored"]
fn rejoining_node_learns_a_table_created_while_it_was_down() {
    let nodes = nodes();
    // `nodes[1]` is node 2 -- deliberately NOT `nodes[2]`, which is the node this
    // test stops. Confirming "the cluster applied the DDL" has to read from a
    // node that is still up.
    let (first, other) = (&nodes[0], &nodes[1]);

    // The keyspace name MUST be unique per run.
    //
    // A fixed name makes this test lie. A previous run that failed before its
    // cleanup leaves the table cluster-wide, so `IF NOT EXISTS` becomes a no-op
    // and the restarted node already holds the schema from the earlier run --
    // the test then passes without exercising rejoin at all. Observed exactly
    // that: a "pass" whose node log showed the node already knew the table at
    // boot. Uniqueness makes it impossible for this node to have seen it before.
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let keyspace = format!("rejoin_conv_{unique}");
    let table = "created_while_down";

    // Stop the node BEFORE any of the DDL, so the keyspace AND the table are both
    // genuinely new to it. The guard puts it back on any exit, including a panic.
    let stopped = node_container();
    eprintln!("stopping {stopped} (node 3) to make it miss the DDL");
    podman(&["stop", &stopped]);
    let mut guard = RestartOnDrop::new(stopped.clone());

    cql_retrying(
        first,
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 3}}"
        ),
        Duration::from_secs(90),
    );

    // Create the table while it is down, and confirm the cluster applied it.
    cql_retrying(
        first,
        &format!("CREATE TABLE IF NOT EXISTS {keyspace}.{table} (id int PRIMARY KEY, v text)"),
        Duration::from_secs(90),
    );
    // The cluster applied it -- asserted on a node that is still up.
    cql(other, &format!("SELECT * FROM {keyspace}.{table}"));

    // Push the log PAST its snapshot window so the entries above are snapshotted
    // and PURGED.
    //
    // This is the part that makes the test real. With a short outage the missed
    // entries are still in the retained log, and a restarted node simply replays
    // them -- it converges with no schema-sync path involved, so the test would
    // pass against the unfixed code and prove nothing. Production diverged
    // permanently precisely because the entries that created the missing tables
    // had long since been purged.
    //
    // The cluster is brought up with a deliberately small window
    // (FERROSA_RAFT_SNAPSHOT_LOGS), so a handful of DDL entries is enough to
    // force a snapshot and a purge rather than requiring a thousand.
    eprintln!("filling the raft log past its snapshot window so the entries are purged");
    for i in 0..24 {
        cql_retrying(
            first,
            &format!(
                "CREATE TABLE IF NOT EXISTS {keyspace}.filler_{i} (id int PRIMARY KEY, v text)"
            ),
            Duration::from_secs(90),
        );
    }
    // Give the leader time to take the snapshot and purge below it.
    std::thread::sleep(Duration::from_secs(10));

    // Bring it back.
    eprintln!("starting {stopped} again");
    guard.start();
    let node3 = &nodes[2];
    assert!(
        wait_until_serving(node3, Duration::from_secs(120)),
        "node 3 did not start answering CQL within 120s of the restart"
    );

    // Give the rejoin and any schema catch-up a bounded window to complete
    // before calling it divergent. Generous on purpose: this test is about
    // PERMANENT divergence, so waiting past any plausible propagation delay is
    // what makes a failure meaningful rather than flaky.
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut visible = table_is_visible(node3, &keyspace, table);
    while !visible && Instant::now() < deadline {
        std::thread::sleep(Duration::from_secs(3));
        visible = table_is_visible(node3, &keyspace, table);
    }

    let (_, detail) = try_cql(node3, &format!("SELECT * FROM {keyspace}.{table}"));
    // Best-effort cleanup; the unique name means a failure to clean up cannot
    // poison a later run.
    let _ = try_cql(first, &format!("DROP KEYSPACE IF EXISTS {keyspace}"));

    assert!(
        visible,
        "node 3 rejoined but never learned `{keyspace}.{table}`, which was \
         created while it was stopped. Schema handling on rejoin is push-only: \
         the node re-offers the schema it already has and is never sent the \
         cluster's, so the divergence is permanent. Observed: {detail}"
    );
}
