//! jsonb is a FIRST-CLASS data type on a REAL multi-node cluster (T-300 gate
//! REMOVED). The claim being proved — "jsonb works clustered with no flag, no
//! capability check and no operator action" — is a cluster behaviour, so unit
//! tests cannot establish it. This test spawns **three real `ferrosa` node
//! processes**, forms a real Raft cluster, and drives DDL and a write over the
//! PostgreSQL wire (there is no `cqlsh` on the host):
//!
//! 1. a `jsonb`-carrying table is CREATEd on the leader and a jsonb value
//!    INSERTed, with no flag, env var or capability input;
//! 2. a **non-coordinator** node serves the table and the row — which requires
//!    BOTH the schema AND the data to have replicated to it (a node cannot
//!    answer for a table it does not hold);
//! 3. after restarting that node the row is still served, so nothing about the
//!    jsonb schema depended on process-local state.
//!
//! The topology mirrors `tests/docker-compose.cluster.yml`: every node seeds off
//! the other two (a full mesh), so the auto-created `public` keyspace is RF=3.
//! Each node binds its OWN band of eight ports (`ferrosa-node-isolation`).
//!
//! Gating (repo test policy): like the neighbouring `*_replica_process` harnesses
//! this test spawns its own nodes, so it runs in the normal suite by default and is
//! NOT `#[ignore]`d -- a new waiver would be a landmine for future runs. It needs
//! no external cluster, so the cluster job's `--ignored` selection filters it out.
//! Run just it with:
//!   cargo test -p ferrosa --test jsonb_first_class_cluster_process

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

/// The jsonb-carrying table. The PG front end's default schema is `public`.
const TABLE: &str = "jsonb_first_class";
const DOC: &str = "{\"msg\": \"hello\", \"n\": 7}";
const N_NODES: usize = 3;

// ── ports ─────────────────────────────────────────────────────────────────────

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

    fn restart(&mut self, cluster_name: &str, seed: Option<&str>) {
        self.stop();
        self.child = spawn_child(
            &self.data_dir,
            self.ports,
            &self.host_id,
            cluster_name,
            seed,
        );
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

fn warm_node_binary() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        Command::new(env!("CARGO_BIN_EXE_ferrosa"))
            .arg("--version")
            .stdin(Stdio::null())
            .output()
            .expect("warm the node binary");
    });
}

fn spawn_child(
    data_dir: &Path,
    ports: Ports,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
) -> Child {
    warm_node_binary();
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
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    if let Some(seed) = seed {
        cmd.env("FERROSA_SEED", seed);
    }
    cmd.spawn().expect("spawn ferrosa node")
}

// ── readiness ─────────────────────────────────────────────────────────────────

const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const PROBE_READ_TIMEOUT: Duration = Duration::from_millis(500);

fn http_get(port: u16, path: &str, authorization: Option<&str>) -> Result<String, String> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, PROBE_CONNECT_TIMEOUT)
        .map_err(|e| format!("not listening: connect failed ({e})"))?;
    stream
        .set_read_timeout(Some(PROBE_READ_TIMEOUT))
        .map_err(|e| format!("cannot set the probe read timeout: {e}"))?;
    let auth = authorization
        .map(|value| format!("Authorization: {value}"))
        .unwrap_or_default();
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}Connection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("request write failed: {e}"))?;
    let mut body = String::new();
    stream
        .read_to_string(&mut body)
        .map_err(|e| format!("reading the answer failed: {e}"))?;
    Ok(body)
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn dump_logs(nodes: &[Node], lines: usize) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let log = std::fs::read_to_string(n.log_path()).unwrap_or_default();
            format!(
                "\n===== node {i} (web {}) log tail =====\n{}",
                n.ports.web,
                tail(&log, lines)
            )
        })
        .collect()
}

/// How long a node may produce NO new log output before it counts as STALLED rather
/// than slow: a node that is still starting keeps writing, a hung one goes quiet.
const READY_STALL_BACKSTOP: Duration = Duration::from_secs(30);

/// Ceiling on the progress extension below, so a node that chatters forever still
/// fails instead of hanging the run.
const READY_HARD_CAP: Duration = Duration::from_secs(300);

fn wait_ready(node: &mut Node, timeout: Duration) {
    let started = Instant::now();
    let hard_cap = started + READY_HARD_CAP;
    let mut deadline = started + timeout;
    // The budget is spent on SILENCE, not on a wall clock: any new output resets it.
    // That is the rule the engine's own barrier already uses ("The barrier must
    // decide on that PROGRESS, not on a wall clock", quorum_availability.rs), and it
    // is what separates a node starved by the full parallel suite from one that is
    // genuinely hung -- the latter stops writing and still fails at the nominal
    // budget, so this does not lengthen any assertion.
    let mut seen_len = 0u64;
    let mut progressed_at = started;
    loop {
        if let Some(status) = node.child.try_wait().expect("try_wait") {
            let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
            panic!(
                "node {} exited before ready: {status}; log tail:\n{}",
                node.host_id,
                tail(&log, 40)
            );
        }
        if let Ok(raw) = http_get(node.ports.web, "/readyz", None) {
            if raw.contains("\"ready\":true") {
                return;
            }
        }
        let len = std::fs::metadata(node.log_path())
            .map(|m| m.len())
            .unwrap_or(0);
        if len > seen_len {
            seen_len = len;
            progressed_at = Instant::now();
            if deadline < hard_cap {
                deadline = Instant::now() + timeout;
            }
        }
        let stalled = progressed_at.elapsed() >= READY_STALL_BACKSTOP;
        if (Instant::now() >= deadline && stalled) || Instant::now() >= hard_cap {
            let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
            panic!(
                "node {} did not become ready within {timeout:?}; log tail:\n{}",
                node.host_id,
                tail(&log, 40)
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The node reporting `ferrosa_raft_is_leader 1` on `/metrics`.
fn find_leader(nodes: &[Node]) -> Option<usize> {
    nodes.iter().position(|node| {
        http_get(node.ports.web, "/metrics", None)
            .ok()
            .map(|body| {
                body.lines()
                    .any(|line| line.trim() == "ferrosa_raft_is_leader 1")
            })
            .unwrap_or(false)
    })
}

/// Wait until a Raft leader is elected AND every node reports ready on `/readyz`
/// -- the signal the existing real-process harness waits on -- so the CREATE this
/// test issues next really replicates instead of landing on one node.
async fn wait_cluster_ddl_ready(nodes: &[Node], budget: Duration) -> usize {
    let deadline = Instant::now() + budget;
    loop {
        // Readiness is the signal the existing real-process harness waits on: a node
        // answers `{"ready":true}` on /readyz once a leader is elected and it has joined.
        // Requiring the replicated DDL path on EVERY node instead deadlocks this wait --
        // one leader is elected, but per-node knowledge of it need not converge, and
        // that convergence is not what this test is about.
        let all_ready = nodes.iter().all(|n| {
            http_get(n.ports.web, "/readyz", None)
                .ok()
                .map(|raw| raw.contains("\"ready\":true"))
                .unwrap_or(false)
        });
        let leader = find_leader(nodes);
        if let Some(leader) = leader {
            if all_ready {
                return leader;
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "cluster did not elect a Raft leader and become ready on every node \
                 within {budget:?} (leader={leader:?}){}",
                dump_logs(nodes, 60)
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

fn describe(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("SQLSTATE {} {}", db.code().code(), db.message()),
        None => format!("{error:?}"),
    }
}

/// `SELECT id FROM TABLE` on one node's own PostgreSQL listener. Reading the
/// table here requires the node to hold BOTH the table's schema and its rows.
async fn read_ids(client: &Client) -> Result<Vec<i32>, String> {
    let rows = client
        .query(&format!("SELECT id FROM {TABLE}"), &[])
        .await
        .map_err(|e| describe(&e))?;
    Ok(rows.iter().map(|r| r.get::<_, i32>("id")).collect())
}

// ── the test ──────────────────────────────────────────────────────────────────

/// A jsonb-carrying table's SCHEMA and DATA replicate across a real 3-node
/// cluster and read back from a non-coordinator, with no flag or capability
/// input.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jsonb_schema_and_data_replicate_across_a_real_cluster() {
    let cluster_name = format!("jsonb-first-class-{}", std::process::id());
    let mut alloc = PortAllocator::new();

    let all_ports: Vec<Ports> = (0..N_NODES).map(|_| alloc_ports(&mut alloc)).collect();
    let internode: Vec<String> = all_ports
        .iter()
        .map(|p| format!("127.0.0.1:{}", p.internode))
        .collect();
    let seeds_for = |i: usize| -> String {
        internode
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| a.clone())
            .collect::<Vec<_>>()
            .join(",")
    };

    let mut nodes: Vec<Node> = Vec::new();
    for (i, ports) in all_ports.iter().enumerate() {
        let dir = tempfile::tempdir().expect("node data dir");
        // Distinct in BOTH halves of the host id: Raft reads bytes[8..16],
        // Accord reads bytes[0..8], so a single distinguishing group is not
        // enough (see `drop_table_replica_process.rs`).
        let d = 0x22u8 * (i as u8 + 1);
        let host_id = format!(
            "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
             {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
        );
        nodes.push(Node {
            data_dir: dir.path().to_path_buf(),
            ports: *ports,
            host_id: host_id.clone(),
            child: spawn_child(
                dir.path(),
                *ports,
                &host_id,
                &cluster_name,
                Some(&seeds_for(i)),
            ),
            _dir: dir,
        });
    }
    for node in nodes.iter_mut() {
        wait_ready(node, Duration::from_secs(90));
    }

    let leader = wait_cluster_ddl_ready(&nodes, Duration::from_secs(240)).await;

    // (1) CREATE a jsonb-carrying table and write a jsonb value, on the leader,
    //     through the SAME replicated DDL path CQL uses, via PG. No flag, no
    //     capability input, no operator action.
    let client = pg_connect(nodes[leader].ports.pg).await;
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client
                .batch_execute(&format!(
                    "CREATE TABLE {TABLE} (id int PRIMARY KEY, doc jsonb)"
                ))
                .await
            {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "CREATE TABLE {TABLE} with a jsonb column never succeeded: {}{}",
                        describe(&e),
                        dump_logs(&nodes, 40)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client
                .execute(
                    &format!("INSERT INTO {TABLE} (id, doc) VALUES (1, '{DOC}')"),
                    &[],
                )
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "INSERT of a jsonb row never succeeded: {}{}",
                        describe(&e),
                        dump_logs(&nodes, 40)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // (2) A NON-coordinator node serves the table and the row. It cannot answer
    //     for a table it does not hold, so a successful read is evidence that
    //     both the jsonb SCHEMA and the DATA replicated to it.
    let non_coordinator = (0..N_NODES)
        .find(|i| *i != leader)
        .expect("a non-leader node");
    let reader = pg_connect(nodes[non_coordinator].ports.pg).await;
    let ids = read_ids(&reader).await.unwrap_or_else(|e| {
        panic!("node {non_coordinator} must serve the replicated jsonb table: {e}")
    });
    assert_eq!(
        ids,
        vec![1],
        "node {non_coordinator} must serve the row written on node {leader}"
    );

    // (3) Restart the non-coordinator and re-read: the replicated jsonb schema
    //     and row survive a process restart (nothing process-local).
    let seed = seeds_for(non_coordinator);
    let restarted = &mut nodes[non_coordinator];
    restarted.restart(&cluster_name, Some(&seed));
    wait_ready(restarted, Duration::from_secs(90));
    let reader = pg_connect(restarted.ports.pg).await;
    // `/readyz` means the node is up and has joined, not that it can already serve
    // this table: a read the instant it flips ready can time out (CL=ONE,
    // received=0). The invariant is that the replicated row SURVIVES the restart,
    // so retry inside a deadline and still fail loud if it never becomes servable.
    let deadline = Instant::now() + Duration::from_secs(60);
    let ids = loop {
        match read_ids(&reader).await {
            Ok(ids) => break ids,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("after restart the jsonb table must still be served: {e}"),
        }
    };
    assert_eq!(
        ids,
        vec![1],
        "the replicated jsonb schema and row must survive a restart of the node"
    );
}
