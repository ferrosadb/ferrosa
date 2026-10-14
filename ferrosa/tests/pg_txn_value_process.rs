//! Two SIMULTANEOUS transactions, on REAL node processes over the wire: a commit that
//! reports success must carry the value it wrote.
//!
//! # The defect this pins (failure mode RT-3)
//!
//! Measured on the live three-node cluster: two transactions committed back to back, one
//! peer was refused `40001`, and the OTHER peer's COMMIT returned success with no error —
//! yet its row read back PRESENT with the updated column NULL. The row survived and the
//! CELL was lost, which is the failure a caller cannot detect: "proceeds" is not "takes
//! effect", and a lost cell looks exactly like a value nobody wrote.
//!
//! # Why this level, and not the in-process fixture
//!
//! `ferrosa-postgres/tests/m1_join_live.rs` has an equivalent two-client fixture and it is
//! GREEN — measured 3/3. That is the finding, not a contradiction: the in-process two-node
//! Accord fixture does NOT reproduce the defect, so the harness level was measuring itself.
//! This test spawns REAL `ferrosa` processes and drives them with real `tokio_postgres`
//! clients, which is the level at which the defect was actually observed. If it goes green
//! too, that is itself evidence about which layer drops the cell.
//!
//! # Shape (mirrors the live reproducer, not invented here)
//!
//! Three nodes in a star (node 0 boots alone, 1 and 2 seed off it), a table with an INLINE
//! declared key (`aid int PRIMARY KEY`, so the storage key IS the column in the WHERE), both
//! client connections to the SAME node — which is what the live probe did, since all client
//! connections landed on one node.
//!
//! The assertions are stated WITHOUT assuming an ordering, because the commit race decides
//! which peer is refused. Every peer that reports success must have its VALUE readable, and
//! a refusal must be loud and named.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

mod common;
use common::ports::PortAllocator;

const TABLE: &str = "txn_rt3";
/// `base64("ferrosa_admin:ferrosa_admin")` — the seeded development superuser.
const ADMIN_BASIC: &str = "Basic ZmVycm9zYV9hZG1pbjpmZXJyb3NhX2FkbWlu";
const N_NODES: usize = 3;

// ── ports ─────────────────────────────────────────────────────────────────────

/// The eight listeners one node binds. A node that fails any of these never becomes fully
/// ready and must not be used as evidence.
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

/// A band of eight ports for one node, from the shared allocator whose ports sit
/// BELOW the kernel's ephemeral range (`tests/common/ports.rs`): a port inside that
/// range can already be held by any outbound connection in the VM, which is how a
/// node's SPARQL listener lost its port in CI on 2026-10-13.
fn alloc_ports(alloc: &mut PortAllocator) -> Ports {
    Ports {
        cql: alloc.get(),
        web: alloc.get(),
        internode: alloc.get(),
        graph: alloc.get(),
        bolt: alloc.get(),
        pg: alloc.get(),
        sparql: alloc.get(),
        flight: alloc.get(),
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
    fn log(&self) -> String {
        std::fs::read_to_string(self.data_dir.join("node.log")).unwrap_or_default()
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

/// Launch one `ferrosa` process. `seed` is the node's seed list; `None` leaves it empty —
/// the seed node of the star.
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
    cmd.env("RUST_MIN_STACK", "33554432")
        .env("RUST_LOG", "info")
        .env("FERROSA_AUTH_ENABLED", "true")
        .env("FERROSA_DATA_DIR", data_dir)
        .env("FERROSA_CLUSTER_NAME", cluster_name)
        .env("FERROSA_HOST_ID", host_id)
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

/// A minimal HTTP/1.1 GET. Returns the raw response text, or `None` if the listener is not
/// up yet. `header` is an optional extra header line, already terminated.
fn http_get(port: u16, path: &str, header: Option<&str>) -> Option<String> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    let extra = header.map(|h| format!("{h}\r\n")).unwrap_or_default();
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{extra}Connection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// Extract a top-level `"key":"value"` string field from a small flat JSON object, without
/// pulling in a JSON parser.
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
/// A listener that cannot bind is a HARNESS accident, not a product failure:
/// [`alloc_ports`] hands out a port that is free at that instant, and between then and the
/// node's `bind` a concurrently running process can take it. The band it draws from sits
/// BELOW the kernel's ephemeral range (`common::ports`), so the kernel can no longer hand
/// a node's port to an outbound connection; this retry is the second line of defence
/// against a process that binds inside the band. The node never reports ready when it
/// loses a port, and reporting THAT as a red would be a false failure about the product.
/// It is re-drawn and retried instead (see [`start_node_with`]).
enum NotReady {
    /// The node log shows a listener that could not bind: its port was taken by another
    /// process after allocation.
    PortCollision,
    /// Nothing in the log says the node failed to bind; it is simply not ready.
    Timeout,
}

/// Wait until the node reports `{"ready":true}` on `/readyz` (leader elected and every
/// listener up). A bind collision is reported as soon as the node logs it, so the caller
/// can re-draw the band and restart instead of waiting out the whole budget.
fn wait_ready(node: &Node, timeout: Duration) -> Result<(), NotReady> {
    let deadline = Instant::now() + timeout;
    let mut polls: u32 = 0;
    loop {
        if let Some(body) = http_get(node.ports.web, "/readyz", None) {
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

/// Bring ONE node up, re-drawing its whole port band and restarting it if any listener
/// lost its port to a concurrent process. A node that never becomes ready for any other
/// reason is reported with its log, exactly as before.
fn start_node(
    alloc: &mut PortAllocator,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
) -> Node {
    start_node_with(alloc, host_id, cluster_name, seed, alloc_ports)
}

/// [`start_node`] with the band allocator injected, so a test can hand the first attempt
/// a band that is already taken and prove the retry rather than assert it.
fn start_node_with(
    alloc: &mut PortAllocator,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
    mut allocate: impl FnMut(&mut PortAllocator) -> Ports,
) -> Node {
    const ATTEMPTS: usize = 5;
    let mut last_log = String::new();
    for attempt in 1..=ATTEMPTS {
        let dir = tempfile::tempdir().expect("node data dir");
        let node = spawn_node(
            dir,
            allocate(alloc),
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
         port to a concurrent process. Last log tail:\n{}",
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
    let auth = format!("Authorization: {ADMIN_BASIC}");
    http_get(node.ports.web, "/api/cluster/status", Some(&auth))
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

/// A Postgres error rendered with its SQLSTATE, message and detail — the bare `Display` is
/// only "db error".
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

/// The value of one row, read through the given client. An empty result and a NULL cell are
/// both rendered explicitly, because they mean different things and both differ from a
/// value — the ambiguity that made the live finding hard to read.
async fn read_value(client: &Client, aid: i32) -> String {
    let rows = client
        .query(&format!("SELECT v FROM {TABLE} WHERE aid = {aid}"), &[])
        .await
        .unwrap_or_else(|e| panic!("SELECT v WHERE aid={aid} failed: {}", describe(&e)));
    match rows.first() {
        None => "<no row>".to_string(),
        Some(row) => row
            .try_get::<_, Option<i32>>(0)
            .map(|v| match v {
                Some(v) => v.to_string(),
                None => "<NULL cell>".to_string(),
            })
            .unwrap_or_else(|e| format!("<undecodable: {e}>")),
    }
}

// ── the red ────────────────────────────────────────────────────────────────────

/// The RED, end to end on three REAL node processes:
///
/// 1. a single transaction's committed write is readable BY VALUE through a fresh
///    connection (the positive control — without it, the assertions below prove nothing), and
/// 2. two simultaneous transactions, both COMMITs in flight, each end with the value it
///    wrote readable — every peer that reports success carries its value.
///
/// MEASURED (2026-10-11): GREEN — 1/1 in 12.5s, positive control passing. The pair behaved
/// exactly as the live cluster did (one peer refused loudly with SQLSTATE 40001, the other
/// reporting success) EXCEPT that the survivor's value read back correctly. So the lost-cell
/// defect does NOT reproduce on a healthy three-process local cluster, and this test — while a
/// real regression guard for the value-on-success contract — does NOT hold RT-3 down.
///
/// Two levels are now measured green (this, and the in-process fixture 3/3) while the live
/// cross-host cluster produced the NULL cell once. The defect is therefore conditional on some
/// live condition not yet identified, and RT-3's next step is a LIVE re-verification on a
/// healthy cluster: decide whether the live observation was a state artifact of a degraded
/// cluster, rather than chasing more local reproduction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_transactions_each_carry_the_value_they_committed() {
    let cluster_name = format!("txnrt3-proc-{}", std::process::id());
    let mut alloc = PortAllocator::new();

    // Node 0 boots ALONE and must be ready before the others join it (it is the star's seed
    // hub). Each node is brought up through `start_node`, which re-draws its band and
    // restarts it if any listener lost its port to a concurrent test target — the harness
    // accident that made this test red in CI without the product ever being reached.
    let mut nodes: Vec<Node> = vec![start_node(
        &mut alloc,
        &node_host_id(0),
        &cluster_name,
        None,
    )];
    let seed_hub = format!("127.0.0.1:{}", nodes[0].ports.internode);
    for i in 1..N_NODES {
        nodes.push(start_node(
            &mut alloc,
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

    let pg = nodes[0].ports.pg;
    let setup = pg_connect(pg).await;
    let create_sql = format!("CREATE TABLE {TABLE} (aid int PRIMARY KEY, v int)");
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match setup.batch_execute(&create_sql).await {
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
    let seed_sql = format!("INSERT INTO {TABLE} (aid, v) VALUES (1, 100), (2, 200)");
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match setup.execute(&seed_sql, &[]).await {
                Ok(n) => {
                    assert_eq!(n, 2, "the INSERT must report every row it committed");
                    break;
                }
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "seed INSERT never succeeded: {}{}",
                        describe(&e),
                        dump_all_logs(&nodes, 40)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // ── Positive control: one transaction's commit IS readable, by value, elsewhere. ──
    // Without this, the pair assertions below cannot distinguish "the cell is lost" from
    // "this harness can never read a value at all".
    let single = pg_connect(pg).await;
    single.batch_execute("BEGIN").await.expect("BEGIN");
    single
        .execute(&format!("UPDATE {TABLE} SET v = 105 WHERE aid = 1"), &[])
        .await
        .expect("buffered UPDATE inside the txn");
    single
        .batch_execute("COMMIT")
        .await
        .expect("the single transaction commits");
    let reader = pg_connect(pg).await;
    let got = read_value(&reader, 1).await;
    assert_eq!(
        got,
        "105",
        "positive control failed: a committed write must be readable BY VALUE through a \
         fresh connection (aid=1 read back as {got}). Nothing below can mean anything \
         until this holds. Logs:{}",
        dump_all_logs(&nodes, 40)
    );

    // ── The pair: two simultaneous transactions, DISJOINT keys, both COMMITs in flight. ──
    let ca = pg_connect(pg).await;
    let cb = pg_connect(pg).await;
    ca.batch_execute("BEGIN").await.expect("BEGIN on A");
    cb.batch_execute("BEGIN").await.expect("BEGIN on B");
    ca.execute(&format!("UPDATE {TABLE} SET v = 111 WHERE aid = 1"), &[])
        .await
        .expect("A buffers its update");
    cb.execute(&format!("UPDATE {TABLE} SET v = 222 WHERE aid = 2"), &[])
        .await
        .expect("B buffers its update");

    let (out_a, out_b) = tokio::join!(ca.batch_execute("COMMIT"), cb.batch_execute("COMMIT"),);
    let a_ok = out_a.is_ok();
    let b_ok = out_b.is_ok();
    for (peer, outcome) in [("A", &out_a), ("B", &out_b)] {
        match outcome {
            Ok(()) => eprintln!("peer {peer}: COMMIT reported success"),
            Err(e) => eprintln!("peer {peer}: COMMIT refused with {}", describe(e)),
        }
    }
    assert!(
        a_ok || b_ok,
        "two disjoint writers must not BOTH be refused — that is no progress at all. Logs:{}",
        dump_all_logs(&nodes, 40)
    );

    // THE INVARIANT (RT-3): a commit that reports success carries the VALUE it wrote.
    // Read back through a connection that is NOT either of the two writers.
    let v1 = read_value(&reader, 1).await;
    let v2 = read_value(&reader, 2).await;
    if a_ok {
        assert_eq!(
            v1,
            "111",
            "A reported COMMIT success and its row must carry the value it wrote; aid=1 read \
             back as {v1}. A row present with a NULL cell is the live failure mode RT-3. Logs:{}",
            dump_all_logs(&nodes, 40)
        );
    }
    if b_ok {
        assert_eq!(
            v2,
            "222",
            "B reported COMMIT success and its row must carry the value it wrote; aid=2 read \
             back as {v2}. A row present with a NULL cell is the live failure mode RT-3. Logs:{}",
            dump_all_logs(&nodes, 40)
        );
    }
}

// ── the harness's own failure mode ──────────────────────────────────────────────

/// A listener port taken by a concurrent process must cost a RETRY, never a red.
///
/// This is the exact signature that made the test above fail in CI without the product ever
/// being exercised — every CI failure of it so far reads:
///
/// ```text
/// node 11111111-1111-1111-1111-111111111111 did not become ready within 90s; web=46617 log tail:
///   ... ERROR ferrosa: SPARQL HTTP server failed e=Address already in use (os error 98)
/// ```
///
/// `Test + Coverage` runs every test target in parallel, so an ephemeral port handed out by
/// [`free_port`] can be taken between that moment and the node's `bind`. The first attempt
/// here is handed a band whose SPARQL port this test holds OPEN, so a band that is not
/// re-drawn cannot produce a ready node.
#[test]
fn a_port_taken_by_a_concurrent_process_is_redrawn_and_retried() {
    let cluster_name = format!("txnrt3-collide-{}", std::process::id());
    let mut alloc = PortAllocator::new();
    // A listener held for the whole test: this port is genuinely occupied, exactly as the
    // CI thief leaves the port it took.
    // Hold a port from the harness's OWN band: that is the class of loss the retry
    // exists for now that allocation is out of the kernel's ephemeral range, and it
    // must be re-drawn rather than reused.
    let stolen_port = (common::ports::BAND_START..=common::ports::BAND_END)
        .find(|p| !common::ports::reserved_in_process(*p) && common::ports::port_is_free(*p))
        .expect("a free port in the harness band");
    // Bound for the whole test — dropping it would release the port the retry
    // must be seen to step over.
    let _hold = std::net::TcpListener::bind(("127.0.0.1", stolen_port)).expect("hold a band port");

    let mut collided = false;
    let node = start_node_with(&mut alloc, &node_host_id(0), &cluster_name, None, |alloc| {
        let mut ports = alloc_ports(alloc);
        if !collided {
            collided = true;
            ports.sparql = stolen_port;
        }
        ports
    });

    assert!(
        !node.log().contains(PORT_IN_USE),
        "the node that came up must have bound every one of its eight listeners; log tail:\n{}",
        tail(&node.log(), 30)
    );
}
