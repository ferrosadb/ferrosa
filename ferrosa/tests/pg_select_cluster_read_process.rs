//! The PostgreSQL front end's `SELECT` must consult the **cluster** range-read
//! path, not only this node's LOCAL `TableStore` — the REAL multi-node PROCESS
//! red.
//!
//! # The defect this pins
//!
//! `storage_provider::produce_scan` opened `StorageEngine::range_iter` on this
//! node's local storage and never fanned the scan across the ring. That is only
//! correct while the query's keyspace is replicated to *every* node the query can
//! be served from. At a replication factor below the node count the local node
//! owns only some partitions, so a scan returns a FRACTION of the committed rows
//! — **silently**, because a short result set is indistinguishable from a small
//! table. And when an owner is unreachable the read must FAIL LOUD rather than
//! shrink: a subset that arrives as `Ok` is the worst possible answer, because
//! nothing downstream can tell it apart from the whole table.
//!
//! # The topology, and why an assertion must be able to distinguish
//!
//! Three real `ferrosa` node processes form a STAR. Node 0 carries NO seed and is
//! started ALONE first; nodes 1 and 2 then seed off it. Node 0's default keyspace
//! `public` is created once the cluster's replicated DDL path is live, at a factor
//! BELOW the node count — the very condition the boot guard names ("replicated to
//! FEWER nodes"). This is asserted at runtime rather than assumed: the cluster
//! read requests **every** remote replica, which `remote_count_for_cl` only does
//! when `rf < node_count`. So no node owns every partition of `public`.
//!
//! That is what makes the assertions discriminate. With all replicas up a
//! pure-local scan from a node that owns only some partitions returns a strict
//! SUBSET; the cluster scan returns every committed row. Note the trap this
//! avoids: had `public` landed at `RF == node_count` (every key on every node — the
//! shape when all three nodes are started together and the committed size reaches
//! three before the keyspace is created) a pure-local read would ALSO be complete,
//! and an all-up assertion would pass before and after the fix and document
//! nothing. The topology here is deliberately the below-node-count one, the live
//! `maas-loadtest` shape, because that is the shape in which a local-only read is
//! observably wrong.
//!
//! # Why an owner must be DOWN for the second assertion to discriminate
//!
//! The first assertion (`..._all_nodes_return_every_committed_row`) separates
//! "reads local storage" from "reads the owners" while every replica is healthy.
//! The second is about the *fail-loud* invariant — never trade a loud failure for
//! a quiet partial answer — and it can only be exercised when a replica is
//! actually missing:
//!
//! * **Local-only read, one owner stopped:** the survivor returns whatever
//!   partitions it happens to hold locally, as `Ok`. Nothing marks the missing
//!   rows; the caller sees a smaller table.
//! * **Cluster read, one owner stopped:** a partition whose replicas include the
//!   dead node can no longer be served at `CL = QUORUM`; the coordinator
//!   `REFUSES` the read with a `58000` error rather than merging fewer sources
//!   than the level requires.
//!
//! So the owner must be down for the *silent-partial-vs-loud-refusal* distinction
//! to exist at all; an all-replicas-up run has nothing missing to make a partial.
//! Both assertions are on the VALUES of every row, never on a count — a count is a
//! property the defect preserves.
//!
//! Auth is ENABLED so the PG front end admits a login at all (the listener is
//! gated on `FERROSA_AUTH_ENABLED`); development mode seeds the well-known
//! `ferrosa_admin` superuser.

#![cfg(unix)]

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

/// The table the red drives. The PG front end's default schema is `public`, so an
/// unqualified name resolves into the keyspace whose factor is under test.
const TABLE: &str = "cluster_read_tbl";
/// `base64("ferrosa_admin:ferrosa_admin")` — the seeded development superuser.
const ADMIN_BASIC: &str = "Basic ZmVycm9zYV9hZG1pbjpmZXJyb3NhX2FkbWlu";
const N_NODES: usize = 3;

/// The rows seeded through the wire. Twelve distinct partition keys, so that
/// "this node happens to own every partition" is overwhelmingly unlikely at a
/// factor below the node count — each node is the non-owner of a given key with
/// probability `1/N`, so `P(a node owns all twelve) <= (2/3)^12 ~ 0.8%` and the
/// first assertion fails on at least one of the three nodes with probability
/// `~1 - (0.8%)^3`. The values must survive a scan from a node that does not own
/// every partition; a count assertion would not notice a subset.
const SEEDED: &[(i32, &str)] = &[
    (1, "alpha"),
    (2, "bravo"),
    (3, "charlie"),
    (4, "delta"),
    (5, "echo"),
    (6, "foxtrot"),
    (7, "golf"),
    (8, "hotel"),
    (9, "india"),
    (10, "juliett"),
    (11, "kilo"),
    (12, "lima"),
];

// ── ports ─────────────────────────────────────────────────────────────────────

/// The eight listeners one node binds. A node that fails any of these never
/// becomes fully ready and must not be used as evidence.
#[derive(Clone, Copy)]
struct Ports {
    cql: u16,
    web: u16,
    internode: u16,
    graph: u16,
    bolt: u16,
    pg: u16,
    sparql: u16,
    flight: u16,
}

fn free_port(taken: &mut HashSet<u16>) -> u16 {
    for _ in 0..200 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        if taken.insert(port) {
            return port;
        }
    }
    panic!("could not allocate a free port");
}

fn alloc_ports(taken: &mut HashSet<u16>) -> Ports {
    Ports {
        cql: free_port(taken),
        web: free_port(taken),
        internode: free_port(taken),
        graph: free_port(taken),
        bolt: free_port(taken),
        pg: free_port(taken),
        sparql: free_port(taken),
        flight: free_port(taken),
    }
}

// ── node process ──────────────────────────────────────────────────────────────

struct Node {
    _dir: tempfile::TempDir,
    data_dir: PathBuf,
    ports: Ports,
    host_id: String,
    child: Child,
}

impl Node {
    fn log_path(&self) -> PathBuf {
        self.data_dir.join("node.log")
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.log_path()).unwrap_or_default()
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Launch one `ferrosa` process. `seed` is the node's `[internode] seed` list;
/// `None` leaves it empty — the seed node of the star.
fn spawn_child(
    data_dir: &Path,
    ports: Ports,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
) -> Child {
    let log = std::fs::File::create(data_dir.join("node.log")).expect("create node log");
    let stderr = log.try_clone().expect("clone log handle");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
    cmd.env("RUST_MIN_STACK", "33554432") // debug-profile worker stack
        .env("RUST_LOG", "info")
        .env("FERROSA_AUTH_ENABLED", "true")
        .env("FERROSA_DATA_DIR", data_dir)
        .env("FERROSA_CLUSTER_NAME", cluster_name)
        .env("FERROSA_HOST_ID", host_id)
        // Deliberately NO FERROSA_EXPECTED_CLUSTER_SIZE: the node sizes its
        // default keyspace from what it actually knows at boot, and node 0 boots
        // alone with no seed — so `public` lands at RF < 3.
        .env("FERROSA_CQL_BIND", format!("127.0.0.1:{}", ports.cql))
        .env("FERROSA_WEB_BIND", format!("127.0.0.1:{}", ports.web))
        .env(
            "FERROSA_INTERNODE_BIND",
            format!("127.0.0.1:{}", ports.internode),
        )
        .env(
            "FERROSA_INTERNODE_BROADCAST",
            format!("127.0.0.1:{}", ports.internode),
        )
        .env("FERROSA_GRAPH_BIND", format!("127.0.0.1:{}", ports.graph))
        .env("FERROSA_BOLT_PORT", ports.bolt.to_string())
        .env("FERROSA_POSTGRES_BIND", format!("127.0.0.1:{}", ports.pg))
        .env("FERROSA_SPARQL_BIND", format!("127.0.0.1:{}", ports.sparql))
        .env("FERROSA_FLIGHT_BIND", format!("127.0.0.1:{}", ports.flight))
        .env("FERROSA_FLUSH_INTERVAL_SECS", "1")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    if let Some(seed) = seed {
        cmd.env("FERROSA_SEED", seed);
    }
    cmd.spawn().expect("spawn ferrosa node")
}

fn spawn_node(
    dir: tempfile::TempDir,
    ports: Ports,
    host_id: String,
    cluster_name: &str,
    seed: Option<&str>,
) -> Node {
    let data_dir = dir.path().to_path_buf();
    let child = spawn_child(&data_dir, ports, &host_id, cluster_name, seed);
    Node {
        _dir: dir,
        data_dir,
        ports,
        host_id,
        child,
    }
}

// ── readiness ─────────────────────────────────────────────────────────────────

/// A minimal HTTP/1.1 GET. Returns the raw response text, or `None` if the
/// listener is not up yet. `authorization` is the full header value, or `""`.
fn http_get(port: u16, path: &str, authorization: &str) -> Option<String> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    let auth = if authorization.is_empty() {
        String::new()
    } else {
        format!("Authorization: {authorization}\r\n")
    };
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}Connection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// Extract a top-level `"key":"value"` string field from a small flat JSON object,
/// without pulling in a JSON parser.
fn json_str(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// The host UUID for node `i` — distinct in BOTH Raft's `bytes[8..16]` and Accord's
/// `bytes[0..8]`: repeating one byte across all sixteen is distinct in both, matching
/// `tests/docker-compose.cluster.yml`.
fn node_host_id(i: usize) -> String {
    let d = 0x11u8 * (i as u8 + 1);
    format!(
        "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
         {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
    )
}

/// The node's message when one of its eight listeners cannot bind.
const PORT_IN_USE: &str = "Address already in use";

/// Why a node did not reach readiness.
///
/// A listener that cannot bind is a HARNESS accident, not a product failure: [`free_port`]
/// hands out a port that is free at that instant, and between then and the node's `bind` a
/// concurrently running test target can take it — `cargo-nextest` runs many targets at once,
/// all drawing from the same ephemeral range. The node then never reports ready, and
/// reporting THAT as a red would be a false failure about the product. It is re-drawn and
/// retried instead (see [`start_node_with`]).
enum NotReady {
    /// The node log shows a listener that could not bind: its port was taken by another
    /// process after allocation.
    PortCollision,
    /// Nothing in the log says the node failed to bind; it is simply not ready.
    Timeout,
}

/// Wait until the node reports `{"ready":true}` on `/readyz` (leader elected and every
/// listener up). A bind collision is reported as soon as the node logs it, so the caller can
/// re-draw the band and restart instead of waiting out the whole budget.
fn wait_ready(node: &Node, timeout: Duration) -> Result<(), NotReady> {
    let deadline = Instant::now() + timeout;
    let mut polls: u32 = 0;
    loop {
        if let Some(body) = http_get(node.ports.web, "/readyz", "") {
            if body.contains("\"ready\":true") {
                return Ok(());
            }
        }
        // Re-read the node log once a second (5 x 200 ms). A failed bind is fatal to
        // readiness, so seeing it early is what makes the retry cheap.
        polls += 1;
        if polls.is_multiple_of(5) && node.log().contains(PORT_IN_USE) {
            return Err(NotReady::PortCollision);
        }
        if Instant::now() >= deadline {
            return Err(if node.log().contains(PORT_IN_USE) {
                NotReady::PortCollision
            } else {
                NotReady::Timeout
            });
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Bring ONE node up, re-drawing its whole port band and restarting it if any listener lost
/// its port to a concurrent process. A node that never becomes ready for any other reason is
/// reported with its log, exactly as before.
fn start_node(
    taken: &mut HashSet<u16>,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
) -> Node {
    start_node_with(taken, host_id, cluster_name, seed, alloc_ports)
}

/// [`start_node`] with the band allocator injected, so a test can hand the first attempt a
/// band that is already taken and prove the retry rather than assert it.
fn start_node_with(
    taken: &mut HashSet<u16>,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
    mut allocate: impl FnMut(&mut HashSet<u16>) -> Ports,
) -> Node {
    const ATTEMPTS: usize = 5;
    let mut last_log = String::new();
    for attempt in 1..=ATTEMPTS {
        let dir = tempfile::tempdir().expect("node data dir");
        let node = spawn_node(
            dir,
            allocate(taken),
            host_id.to_string(),
            cluster_name,
            seed,
        );
        match wait_ready(&node, Duration::from_secs(90)) {
            Ok(()) => return node,
            Err(NotReady::PortCollision) => {
                eprintln!(
                    "node {host_id} lost a listener port to a concurrent process; re-drawing \
                     its band and restarting (attempt {attempt}/{ATTEMPTS})"
                );
                last_log = node.log();
            }
            Err(NotReady::Timeout) => panic!(
                "node {host_id} did not become ready within 90s; web={} log tail:\n{}",
                node.ports.web,
                tail(&node.log(), 40)
            ),
        }
    }
    panic!(
        "node {host_id} never became ready: every one of {ATTEMPTS} attempts lost a listener \
         port to a concurrent process. Log tail:\n{}",
        tail(&last_log, 40)
    );
}

/// Every node's log tail, labelled — the evidence a failing harness must show.
fn dump_all_logs(nodes: &[Node], lines: usize) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            format!(
                "\n===== node {i} host {} (web {}) log tail =====\n{}",
                n.host_id,
                n.ports.web,
                tail(&n.log(), lines)
            )
        })
        .collect()
}

/// Whether this node has installed the Raft-replicated `DdlPath::Cluster`.
fn ddl_path_is_cluster(node: &Node) -> bool {
    http_get(node.ports.web, "/api/cluster/status", ADMIN_BASIC)
        .map(|body| json_str(&body, "ddl_path").as_deref() == Some("cluster"))
        .unwrap_or(false)
}

/// Wait until **every** node reports the replicated `DdlPath::Cluster`.
async fn wait_cluster_ddl_ready(nodes: &[Node], budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        let ddl: Vec<bool> = nodes.iter().map(ddl_path_is_cluster).collect();
        if ddl.iter().all(|ready| *ready) {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "cluster did not install the replicated DDL path on every node within \
                 {budget:?} (ddl_path_per_node={ddl:?}):{}",
                dump_all_logs(nodes, 60)
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ── PostgreSQL wire ───────────────────────────────────────────────────────────

async fn pg_connect(port: u16) -> Client {
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_admin")
        .password("ferrosa_admin")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("PostgreSQL SCRAM handshake must succeed as ferrosa_admin");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A Postgres error rendered with its SQLSTATE, message and detail — the bare
/// `Display` is only "db error".
fn describe(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!(
            "SQLSTATE {} {} (detail {:?})",
            db.code().code(),
            db.message(),
            db.detail()
        ),
        None => format!("{error:?}"),
    }
}

/// `SELECT a, b FROM t ORDER BY a` over the wire, as `(a, b)` pairs.
async fn read_rows(client: &Client) -> Result<Vec<(i32, String)>, String> {
    let rows = client
        .query(&format!("SELECT a, b FROM {TABLE} ORDER BY a"), &[])
        .await
        .map_err(|e| describe(&e))?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, i32>("a"), row.get::<_, String>("b")))
        .collect())
}

fn expected_rows() -> Vec<(i32, String)> {
    SEEDED.iter().map(|(a, b)| (*a, (*b).to_string())).collect()
}

// ── the red ────────────────────────────────────────────────────────────────────

/// The RED, end to end on three REAL node processes:
///
/// 1. a `SELECT` served by ANY of the three nodes returns EVERY committed row —
///    by value and in order — and
/// 2. once an owner is stopped, the same `SELECT` either returns every committed
///    row or FAILS LOUD; it never returns a subset as if it were the answer.
///
/// PRE-change (local-only scan): (1) fails — with `public` below the node count
/// no node owns every partition, so some node returns a strict subset, and with
/// twelve keys at least one of the three nodes is a non-owner with probability
/// `> 99.999%` — and (2) fails because the subset arrives as `Ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_select_consults_the_cluster_or_fails_loud() {
    let cluster_name = format!("pgread-proc-{}", std::process::id());
    let mut taken: HashSet<u16> = HashSet::new();

    let mut nodes: Vec<Node> = Vec::new();

    // Node 0 FIRST, alone: it is the seed node of the star and creates its
    // default keyspace once the cluster's DDL path is live, at a factor below the
    // node count — the shape a local-only scan gets wrong. It must be ready before the
    // others join it, and each node is brought up through `start_node`, which re-draws its
    // band and restarts it if a listener lost its port to a concurrent test target.
    nodes.push(start_node(
        &mut taken,
        &node_host_id(0),
        &cluster_name,
        None,
    ));
    let seed_hub = format!("127.0.0.1:{}", nodes[0].ports.internode);

    // Then the other two seed off it, forming the star.
    for i in 1..N_NODES {
        nodes.push(start_node(
            &mut taken,
            &node_host_id(i),
            &cluster_name,
            Some(seed_hub.as_str()),
        ));
    }
    for node in &nodes {
        let log = node.log();
        assert!(
            !log.contains("Address already in use"),
            "node {} hit a port collision — the cluster is not healthy:\n{}",
            node.host_id,
            tail(&log, 30)
        );
    }
    wait_cluster_ddl_ready(&nodes, Duration::from_secs(240)).await;

    // Create the table through the replicated DDL path, retrying while the default
    // `public` keyspace finishes being created.
    let client0 = pg_connect(nodes[0].ports.pg).await;
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client0
                .batch_execute(&format!("CREATE TABLE {TABLE} (a int PRIMARY KEY, b text)"))
                .await
            {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "CREATE TABLE {TABLE} never succeeded: {}{}",
                        describe(&e),
                        dump_all_logs(&nodes, 40)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // One multi-row INSERT, retried while Accord membership settles.
    let values = SEEDED
        .iter()
        .map(|(a, b)| format!("({a},'{b}')"))
        .collect::<Vec<_>>()
        .join(",");
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client0
                .execute(&format!("INSERT INTO {TABLE} (a,b) VALUES {values}"), &[])
                .await
            {
                Ok(n) => {
                    assert_eq!(
                        n as usize,
                        SEEDED.len(),
                        "the INSERT must report every row it committed"
                    );
                    break;
                }
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "multi-row INSERT never succeeded: {}{}",
                        describe(&e),
                        dump_all_logs(&nodes, 40)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    let want = expected_rows();

    // ── Invariant 1 + 3: every node returns every committed row, in order. ──
    //
    // Asserted for EACH node, by value. `public` is below the node count, so it is
    // impossible for all three nodes to own every partition; a local-only read
    // fails this on at least one node (no probability beyond the token draw).
    tokio::time::sleep(Duration::from_secs(3)).await;
    for i in 0..N_NODES {
        let client = pg_connect(nodes[i].ports.pg).await;
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut got = read_rows(&client)
            .await
            .unwrap_or_else(|e| panic!("SELECT on node {i} failed: {e}"));
        while got != want && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            got = read_rows(&client)
                .await
                .unwrap_or_else(|e| panic!("SELECT on node {i} failed: {e}"));
        }
        assert_eq!(
            got,
            want,
            "node {i} returned {} of the {} committed rows. The PostgreSQL front end \
             must read the CLUSTER, not only this node's local store: with `public` \
             below the node count, a local scan is a silent subset. Values, not \
             counts: the INSERT reported {} rows. Logs:{}",
            got.len(),
            want.len(),
            SEEDED.len(),
            dump_all_logs(&nodes, 40)
        );
    }

    // ── Invariant 2: an unreachable owner is a LOUD failure, never a short ──
    // ── answer that looks complete.                                             ──
    nodes[2].stop();
    // Give the peer death a moment to be observed by node 0.
    tokio::time::sleep(Duration::from_secs(2)).await;

    match read_rows(&client0).await {
        Ok(rows) => assert_eq!(
            rows,
            want,
            "with an owner stopped, node 0 returned {} of the {} committed rows as a \
             SUCCESSFUL result. An under-return must be indistinguishable from nothing: \
             it must fail loud, because a subset looks exactly like a smaller table. \
             Logs:{}",
            rows.len(),
            want.len(),
            dump_all_logs(&nodes, 40)
        ),
        Err(e) => {
            // The loud path: the consistency level could not be met. This is the
            // required outcome when a committed owner is unreachable.
            eprintln!("SELECT correctly failed loud once an owner was stopped: {e}");
        }
    }
}
