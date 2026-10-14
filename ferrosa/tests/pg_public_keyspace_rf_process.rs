//! The `public` keyspace's replication factor must cover the cluster the node can
//! actually be read from — the REAL multi-node PROCESS red.
//!
//! # The defect this pins
//!
//! The PostgreSQL front end answers EVERY `SELECT` from this node's LOCAL
//! `TableStore` (`storage_provider::produce_scan` never fans a range read across
//! the ring). That is only correct when the query's keyspace is replicated to
//! every node it can be read from. `main` sized the auto-created PostgreSQL default
//! keyspace `public` from the `[internode] seed` list ALONE (`seeds + 1`). A node
//! that carries no seed — the live `maas-loadtest` cluster's nodes, and the SEED
//! node of any star topology — therefore got `RF=1`: one owner per partition.
//!
//! The visible symptom is the `INSERT 0 2` / one-row read: a two-row INSERT commits
//! both rows (`INSERT 0 2` is honest), but a `SELECT` served by a node that owns
//! only one of the two partitions returns a FRACTION of them — the write is
//! complete, the read is not. It is not data loss.
//!
//! # The red
//!
//! Three real `ferrosa` node processes form a STAR: node 0 carries NO seed and
//! nodes 1 and 2 seed off it. Every node is given
//! `FERROSA_EXPECTED_CLUSTER_SIZE=3`. The client connects to node 0 — the node with
//! no configured seed, i.e. the live shape.
//!
//! * On PRE-change code the seed-only rule gives node 0 `seeds + 1 = 1`, so its
//!   `public` is RF=1 and a scan from node 0 returns only the partitions node 0
//!   owns — fewer than were committed.
//! * On FIXED code the derivation is `max(seeds + 1, expected, committed) = 3`, so
//!   `public` is RF=3 and the SAME select returns every committed row.
//!
//! The assertions are on the VALUES of every row, never on a count: a count check
//! is satisfied by the defect (it returned `INSERT 0 2` while persisting one
//! visible row). Several rows are seeded rather than two, because RF=1 from one
//! coordinator would still return all of them with probability `(1/3)^n` — the
//! brief's two-row repro is only ~11% likely to false-green, so the red uses a
//! handful and asserts the whole set.
//!
//! The topology mirrors `tests/docker-compose.cluster.yml`'s per-node port bands and
//! host-id shape. Auth is ENABLED so the PG front end admits a login at all (the
//! listener is gated on `FERROSA_AUTH_ENABLED`); development mode seeds the
//! well-known `ferrosa_admin` superuser.

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

/// The table the red drives. The PG front end's default schema is `public`, so an
/// unqualified name resolves into the keyspace whose factor is under test.
const TABLE: &str = "rf_proc_tbl";
/// `base64("ferrosa_admin:ferrosa_admin")` — the seeded development superuser.
const ADMIN_BASIC: &str = "Basic ZmVycm9zYV9hZG1pbjpmZXJyb3NhX2FkbWlu";
const N_NODES: usize = 3;
/// The cluster size the operator declares. This is the knob the fix reads and the
/// seed-only rule ignored. Held equal to the real node count so FIXED code makes
/// `public` complete for this star.
const EXPECTED_CLUSTER_SIZE: &str = "3";

/// The rows seeded through the wire. Values must survive a scan from a node that
/// does not own every partition; a count assertion would not notice when they do
/// not. A handful, so an RF=1 coordinator returning only its own third fails with
/// overwhelming probability rather than the ~1/9 a two-row pair would carry.
const SEEDED: &[(i32, &str)] = &[(1, "x"), (2, "y"), (3, "z"), (4, "w"), (5, "v")];

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
    fn log_path(&self) -> PathBuf {
        self.data_dir.join("node.log")
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
/// `None` leaves it empty — the seed node of the star, and the shape the live
/// cluster runs.
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
        // The operator declares the intended cluster size. The defect is that the
        // seed-only rule never read it; the fix makes the derivation its max.
        .env("FERROSA_EXPECTED_CLUSTER_SIZE", EXPECTED_CLUSTER_SIZE)
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
/// listener is not up yet.
fn http_get(port: u16, path: &str, authorization: Option<&str>) -> Option<String> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    let auth = authorization
        .map(|a| format!("Authorization: {a}\r\n"))
        .unwrap_or_default();
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

/// The message a readiness timeout must carry, as a pure function so its SHAPE is
/// testable without a cluster.
///
/// The `/readyz` body is the only artifact that separates "the readiness condition
/// is genuinely unmet" — the body names it under `waiting_for` (raft_leader, a
/// schema-registration wait, the expected-cluster-size topology gate) — from "this
/// test's three node processes were starved on a contended runner", where the node
/// is plainly alive in its own log. Discarding that body, as this harness used to,
/// turns every occurrence into a fresh investigation of a failure that never says
/// what it was waiting for.
fn readiness_timeout_message(
    host_id: &str,
    web_port: u16,
    timeout: Duration,
    body: Option<&str>,
    log_tail: &str,
) -> String {
    let verdict = match body {
        None => "no /readyz response at all (its listener never came up)".to_string(),
        Some(raw) => {
            // `http_get` returns the whole response; the JSON body follows the header block.
            let json = raw.rsplit("\r\n\r\n").next().unwrap_or(raw).trim();
            match json_str(json, "waiting_for") {
                Some(w) => format!("waiting_for={w}  body: {json}"),
                None => format!("no waiting_for field  body: {json}"),
            }
        }
    };
    format!(
        "node {host_id} did not become ready within {timeout:?}; web={web_port}\n  \
         /readyz: {verdict}\n  log tail:\n{log_tail}"
    )
}

/// Wait until the node reports `{"ready":true}` on `/readyz` (leader elected and
/// every listener up). Panics — naming the condition it was waiting on — if it
/// never does.
fn wait_ready(node: &Node, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut last_body: Option<String> = None;
    loop {
        if let Some(body) = http_get(node.ports.web, "/readyz", None) {
            if body.contains("\"ready\":true") {
                return;
            }
            last_body = Some(body);
        }
        if Instant::now() >= deadline {
            // One last probe: the freshest body is the one that names the condition.
            let body = http_get(node.ports.web, "/readyz", None).or(last_body);
            let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
            panic!(
                "{}",
                readiness_timeout_message(
                    &node.host_id,
                    node.ports.web,
                    timeout,
                    body.as_deref(),
                    &tail(&log, 40)
                )
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// A readiness timeout must NAME the condition it was waiting on, and must say so
/// even when the listener never answered. The `/readyz` body is the artifact that
/// distinguishes a genuinely unmet condition from a starved runner (t_64dd3703),
/// so pin its presence in the message.
#[test]
fn a_readiness_timeout_names_the_condition_the_node_is_waiting_on() {
    let with_body = "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\n\r\n\
                     {\"ready\":false,\"waiting_for\":\"raft_leader\"}";
    let msg = readiness_timeout_message(
        "11111111-1111-1111-1111-111111111111",
        38059,
        Duration::from_secs(90),
        Some(with_body),
        "log line A\nlog line B",
    );
    assert!(msg.contains("waiting_for=raft_leader"), "{msg}");
    assert!(msg.contains("did not become ready within 90s"), "{msg}");
    assert!(msg.contains("log line B"), "{msg}");
    assert!(
        !msg.contains("HTTP/1.1"),
        "the header block is not the evidence: {msg}"
    );

    let no_body = readiness_timeout_message("h", 1, Duration::from_secs(90), None, "tail");
    assert!(no_body.contains("no /readyz response at all"), "{no_body}");
    assert!(no_body.contains("tail"), "{no_body}");
}

/// Every node's log tail, labelled — the evidence a failing harness must show.
fn dump_all_logs(nodes: &[Node], lines: usize) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let log = std::fs::read_to_string(n.log_path()).unwrap_or_default();
            format!(
                "\n===== node {i} host {} (web {}) log tail =====\n{}",
                n.host_id,
                n.ports.web,
                tail(&log, lines)
            )
        })
        .collect()
}

/// The node reporting `ferrosa_raft_is_leader 1` on the unauthenticated
/// `/metrics` endpoint.
fn find_leader(nodes: &[Node]) -> Option<usize> {
    nodes.iter().position(|node| {
        http_get(node.ports.web, "/metrics", None)
            .map(|body| {
                body.lines()
                    .any(|line| line.trim() == "ferrosa_raft_is_leader 1")
            })
            .unwrap_or(false)
    })
}

/// Whether this node has installed the Raft-replicated `DdlPath::Cluster`.
fn ddl_path_is_cluster(node: &Node) -> bool {
    http_get(node.ports.web, "/api/cluster/status", Some(ADMIN_BASIC))
        .map(|body| json_str(&body, "ddl_path").as_deref() == Some("cluster"))
        .unwrap_or(false)
}

/// Wait until a Raft leader has been elected AND **every** node reports the
/// replicated `DdlPath::Cluster`; return the leader's index. On timeout it dumps
/// every node's log tail.
async fn wait_cluster_ddl_ready(nodes: &[Node], budget: Duration) -> usize {
    let deadline = Instant::now() + budget;
    loop {
        let ddl: Vec<bool> = nodes.iter().map(ddl_path_is_cluster).collect();
        let leader = find_leader(nodes);
        if let Some(leader) = leader {
            if ddl.iter().all(|ready| *ready) {
                return leader;
            }
        }
        if Instant::now() >= deadline {
            let logs: String = nodes
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    let log = std::fs::read_to_string(n.log_path()).unwrap_or_default();
                    format!(
                        "\n===== node {i} host {} (web {}) ddl_path={} log tail =====\n{}",
                        n.host_id,
                        n.ports.web,
                        ddl[i],
                        tail(&log, 80)
                    )
                })
                .collect();
            panic!(
                "cluster did not elect a Raft leader and install the replicated DDL path on \
                 every node within {budget:?} (leader={leader:?}, ddl_path_per_node={ddl:?}):\
                 {logs}"
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

// ── the test ──────────────────────────────────────────────────────────────────

/// The RED: on a node that carries no seed, `public` must be replicated across the
/// whole cluster, so a wire `SELECT` returns EVERY committed row by value.
///
/// PRE-change this fails: node 0's `public` is RF=1 and the scan returns only node
/// 0's third of the rows. POST-change it passes: `max(seeds, expected, committed)=3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_keyspace_covers_a_cluster_formed_without_a_configured_seed() {
    let cluster_name = format!("rf-proc-{}", std::process::id());
    let mut alloc = PortAllocator::new();

    // Every node binds its own band of eight ports before any is started, so the
    // star can be wired: node 0 has NO seed and nodes 1 and 2 seed off it.
    let all_ports: Vec<Ports> = (0..N_NODES).map(|_| alloc_ports(&mut alloc)).collect();
    let seed_hub = format!("127.0.0.1:{}", all_ports[0].internode);

    let mut nodes: Vec<Node> = Vec::new();
    for (i, ports) in all_ports.iter().enumerate() {
        let dir = tempfile::tempdir().expect("node data dir");
        // Host ids must be distinct in BOTH halves: Raft's `uuid_to_node_id` reads
        // bytes[8..16] (little-endian) and Accord reads bytes[0..8] (big-endian).
        // `0x11/0x22/0x33` repeated across all sixteen bytes is distinct in both,
        // matching `tests/docker-compose.cluster.yml`.
        let d = 0x11u8 * (i as u8 + 1);
        let host_id = format!(
            "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
             {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
        );
        // node 0 is the seed HUB and carries NO seed itself — the live shape. The
        // other two point at it so the cluster forms.
        let seed = if i == 0 {
            None
        } else {
            Some(seed_hub.as_str())
        };
        nodes.push(spawn_node(dir, *ports, host_id, &cluster_name, seed));
    }
    for node in &nodes {
        wait_ready(node, Duration::from_secs(90));
    }
    for node in &nodes {
        let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
        assert!(
            !log.contains("Address already in use"),
            "node {} hit a port collision — the cluster is not healthy:\n{}",
            node.host_id,
            tail(&log, 30)
        );
    }

    // Formation must complete: a Raft leader plus `DdlPath::Cluster` everywhere.
    let _leader = wait_cluster_ddl_ready(&nodes, Duration::from_secs(240)).await;

    // The client talks to node 0 — the node with no configured seed, i.e. exactly
    // the node whose `public` the seed-only rule sized at RF=1.
    let client = pg_connect(nodes[0].ports.pg).await;

    // Create the table through the replicated DDL path, retrying while the default
    // `public` keyspace is still being created at boot.
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client
                .batch_execute(&format!(
                    "CREATE TABLE {TABLE} (a int PRIMARY KEY, b text, c numeric(10,2))"
                ))
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

    // One multi-row INSERT — the statement whose reported count is honest but whose
    // rows a local-only scan can lose. Retry the first one while Accord membership
    // settles ("Accord quorum unavailable" right after bootstrap).
    let values = SEEDED
        .iter()
        .map(|(a, b)| format!("({a},'{b}',{a}.5)"))
        .collect::<Vec<_>>()
        .join(",");
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client
                .execute(&format!("INSERT INTO {TABLE} (a,b,c) VALUES {values}"), &[])
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

    // Accord apply is asynchronous; give it a bounded window, then assert.
    let want: Vec<(i32, String)> = SEEDED.iter().map(|(a, b)| (*a, (*b).to_string())).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut got = read_rows(&client)
        .await
        .unwrap_or_else(|e| panic!("SELECT of the seeded rows failed: {e}"));
    while got != want && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        got = read_rows(&client)
            .await
            .unwrap_or_else(|e| panic!("SELECT of the seeded rows failed: {e}"));
    }

    assert_eq!(
        got,
        want,
        "a scan served by node 0 (no configured seed) returned {} of the {} committed rows. \
         The PostgreSQL front end reads only local storage, so `public` must be replicated \
         to the whole cluster — not sized from the seed list alone. Values, not counts: the \
         INSERT reported {} rows. \
         See `postgres_default_keyspace_replication_factor_for_cluster`. Logs:{}",
        got.len(),
        want.len(),
        SEEDED.len(),
        dump_all_logs(&nodes, 40)
    );
}
