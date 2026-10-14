//! The owner's COMMIT GUARANTEE, pinned on **three real `ferrosa` node processes**
//! with a real `SIGKILL`:
//!
//! > "if the transaction was committed the data must be durable and needs to be
//! > repaired that's the guarantee we are making if the client thinks they
//! > committed we have to persist the data"
//!
//! # The four invariants
//!
//! * **G1 — a client that observed COMMIT success has its data on a quorum.**
//!   Every write this test makes is a real client statement whose success is
//!   *required*: a refusal or an abandon at any measured write FAILS the test,
//!   because there is no guarantee to check for a client that was told it did
//!   not commit. Immediately after each success — with NO waiting — at least a
//!   quorum (`RF / 2 + 1`) of the replicas must serve the row, read at `ONE`
//!   through each replica in turn.
//! * **G2 — a replica that acknowledged an apply keeps it.** The set of rows
//!   the replica to be killed serves from its OWN replicas is recorded before
//!   the kill; after the crash and restart it must still be a superset of that
//!   set. Nothing is ever rolled back by the restart.
//! * **G3 — replicas that diverge about committed data converge.** Rows
//!   committed while one replica is dead must reappear on that replica once it
//!   rejoins. Asserted on a BOUNDED POLL: convergence may take time, but it must
//!   happen.
//! * **G4 — any replica the cluster serves a read from finds the committed
//!   data.** Every read here is pinned to the reading node's OWN replicas, so
//!   `node 3 has the row` is a claim about node 3 and not about the coordinator
//!   having asked somebody else.
//!
//! # Why the read is pinned, and how
//!
//! The PostgreSQL front end reads through the cluster range-read path
//! (`WritePath::range_read_stream_all_with`) and the level it reads at is
//! chosen by [`ferrosa_postgres::cluster_read::READ_CONSISTENCY_ENV`]
//! (`FERROSA_PG_READ_CONSISTENCY`); this test sets it to `ONE` on every node.
//! With the keyspace's replication factor spanning the ring,
//! `remote_count_for_cl(One, rf, node_count, remotes)` returns **0** remote
//! replicas (`ferrosa-cluster/src/coordinator/range_read_stream.rs`), so the
//! coordinator is served from its own local storage alone — which it is
//! impossible to observe from outside the process without this knob, and which
//! a `QUORUM` read (the default) cannot show, because a quorum read through
//! node N is satisfied by N's peers.
//!
//! `QUORUM` therefore stays the product default; `ONE` is opt-in and this test
//! is the reason it exists. The knob is INSTRUMENTATION, and the assertion only
//! means what it says while the read really is local, so the test asserts the
//! ring each node sees holds all three replicas before it reads.
//!
//! # Layout
//!
//! A star on localhost: node 0 carries no seed and boots alone, nodes 1 and 2
//! seed off it. Every node is told `FERROSA_EXPECTED_CLUSTER_SIZE=3`, so the
//! auto-created PostgreSQL default keyspace `public` is `RF = 3` on every node
//! (see `postgres_default_keyspace_replication_factor_for_cluster`) and every
//! node owns every token range — the shape in which a `ONE` read is a local
//! read. Each node binds its own band of eight ports and each is brought up
//! through [`start_node`], which re-draws the band and retries if a listener
//! loses its port to a concurrently running test target.
//!
//! The repair cadence is shortened (`FERROSA_AUTO_REPAIR_INTERVAL_SECS=1`) so the
//! PRODUCTION anti-entropy scheduler runs inside the test. That shortens a
//! cadence, not a bound: the convergence assertion below is still a bounded poll
//! that must succeed, and it never widens anything.

#![cfg(unix)]

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

mod common;
use common::ports::PortAllocator;

/// `base64("ferrosa_admin:ferrosa_admin")` — the seeded development superuser.
const ADMIN_BASIC: &str = "Basic ZmVycm9zYV9hZG1pbjpmZXJyb3NhX2FkbWlu";
const N_NODES: usize = 3;
/// The declared cluster size. It is one of the three sizes `public`'s factor is
/// the max of, and it is what makes the factor 3 even though node 0 boots alone.
const EXPECTED_CLUSTER_SIZE: &str = "3";
/// `RF / 2 + 1` for `RF = 3`: the number of replicas that must hold a row the
/// client was told committed.
const QUORUM: usize = 2;

/// The line a replica that ran no session must emit: it names the DESIGN (one
/// elected initiator per range) instead of reading as a failed verification.
const NOT_THE_INITIATOR_LINE: &str = "auto-repair: another live replica is the elected initiator";

/// The elected initiator's two POSITIVE claims. Both mean "every range comparison
/// against every live replica found the same leaf hashes", so both are checked
/// against the restarted replica's actual contents.
const CONVERGED_LINE: &str = "auto-repair: table converged (no divergence)";
const DIVERGENCE_RECONCILED_LINE: &str = "auto-repair: DIVERGENCE reconciled for table";

/// The rows committed while every replica is up, before the kill.
const BEFORE_FAULT: std::ops::RangeInclusive<i32> = 1..=8;
/// The rows committed while one replica is dead. A three-node quorum of two must
/// still accept them.
const DURING_FAULT: std::ops::RangeInclusive<i32> = 9..=14;
/// The replica killed and restarted: index into the node vector, i.e. node 2.
const FAULT_NODE: usize = 2;

/// How long the test will wait for replica propagation, for the restarted
/// replica to rejoin, and for anti-entropy to converge. These are BOUNDED POLLS
/// on the system's own condition, not fixed sleeps: each returns as soon as the
/// condition holds, and any of them exhausting its budget is a failure.
const PROPAGATION_BUDGET: Duration = Duration::from_secs(30);
const REJOIN_BUDGET: Duration = Duration::from_secs(90);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(180);

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
    /// Kept alive for the whole test; its directory survives a restart of
    /// `child`, which is what makes the crash a crash and not a wipe.
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

    /// Wait until this node answers `/readyz` with `{"ready":true}`.
    fn wait_ready(&self, budget: Duration) {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(body) = http_get(self.ports.web, "/readyz", "") {
                if body.contains("\"ready\":true") {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "node {} (web {}) never became ready within {budget:?}; log tail:\n{}",
                self.host_id,
                self.ports.web,
                tail(&self.log(), 40)
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// The number of ring entries this node currently sees, from the
    /// authenticated cluster ring endpoint — the precondition for a `ONE` read
    /// being served from this node's own replicas alone.
    fn ring_node_count(&self) -> Option<usize> {
        let body = http_get(self.ports.web, "/api/cluster/ring", ADMIN_BASIC)?;
        Some(body.matches("\"node_id\":").count())
    }

    /// Kill this node's process the way a machine loses power to it: `SIGKILL`,
    /// no chance to flush, no shutdown path.
    ///
    /// Returns the signal the process died from, so the test can PROVE the death
    /// was the one it meant (`std::process::Child::kill` is `SIGKILL` on Unix) and
    /// not, say, an exit from a failed bind.
    fn kill_hard(&mut self) -> std::process::ExitStatus {
        self.child.kill().expect("SIGKILL the node process");
        self.child.wait().expect("reap the killed node process")
    }

    /// Start this node's process again against the SAME data directory: the
    /// restart half of the fault.
    fn restart(&mut self, cluster_name: &str, seed: Option<&str>) {
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
        let _ = self.child.kill();
        let _ = self.child.wait();
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
        // `public`'s replication factor is the max of the seed-derived size, the
        // size the operator declares, and the committed size. Node 0 boots alone,
        // so without this it would create `public` at RF=1 and no node would own
        // every partition — a `ONE` read would then not be a local read at all.
        .env("FERROSA_EXPECTED_CLUSTER_SIZE", EXPECTED_CLUSTER_SIZE)
        // THE INSTRUMENT: read at `ONE`, so the coordinator answers from its own
        // replicas. `QUORUM` is the product default and stays so.
        .env("FERROSA_PG_READ_CONSISTENCY", "ONE")
        // Run the production anti-entropy scheduler inside the test's lifetime.
        .env("FERROSA_AUTO_REPAIR_INTERVAL_SECS", "1")
        // Flush quickly so committed rows reach SSTables, not just memtables.
        .env("FERROSA_FLUSH_INTERVAL_SECS", "1")
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

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// The host UUID for node `i` — distinct in BOTH Raft's `bytes[8..16]` and
/// Accord's `bytes[0..8]`: repeating one byte across all sixteen is distinct in
/// both, matching `tests/docker-compose.cluster.yml`.
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
/// A listener that cannot bind is a HARNESS accident, not a product failure: [`alloc_ports`]
/// hands out a port that is free at that instant, and between then and the node's `bind` a
/// concurrently running process can take it. The band it draws from sits BELOW the kernel's
/// ephemeral range (`common::ports`), so the kernel can no longer hand a node's port to an
/// outbound connection; this retry is the second line of defence against a process that
/// binds inside the band. The node never reports ready when it loses a port, and reporting
/// THAT as a red would be a false failure about the product. It is re-drawn and retried instead.
enum NotReady {
    PortCollision,
    Timeout,
}

fn wait_ready(node: &Node, timeout: Duration) -> Result<(), NotReady> {
    let deadline = Instant::now() + timeout;
    let mut polls: u32 = 0;
    loop {
        if let Some(body) = http_get(node.ports.web, "/readyz", "") {
            if body.contains("\"ready\":true") {
                return Ok(());
            }
        }
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

/// Bring the whole three-node star up, re-drawing EVERY band and restarting the
/// whole cluster if any node's listener lost its port to a concurrent process.
///
/// The whole cluster is brought up before any readiness is waited for, and the
/// bands are drawn first, because a node's seed is another node's internode port:
/// a per-node retry would leave nodes 1 and 2 pointed at a hub port that no longer
/// exists. It is also why readiness must be waited for with all three up — every
/// node is told `FERROSA_EXPECTED_CLUSTER_SIZE=3`, and a node alone does not report
/// ready at the declared size.
fn start_cluster(alloc: &mut PortAllocator, cluster_name: &str) -> Vec<Node> {
    const ATTEMPTS: usize = 5;
    let mut last_log = String::new();
    for attempt in 1..=ATTEMPTS {
        let all_ports: Vec<Ports> = (0..N_NODES).map(|_| alloc_ports(alloc)).collect();
        let seed_hub = format!("127.0.0.1:{}", all_ports[0].internode);

        let mut nodes: Vec<Node> = Vec::with_capacity(N_NODES);
        for (i, ports) in all_ports.iter().enumerate() {
            let dir = tempfile::tempdir().expect("node data dir");
            let data_dir = dir.path().to_path_buf();
            let host_id = node_host_id(i);
            let seed = if i == 0 {
                None
            } else {
                Some(seed_hub.as_str())
            };
            let child = spawn_child(&data_dir, *ports, &host_id, cluster_name, seed);
            nodes.push(Node {
                _dir: dir,
                data_dir,
                ports: *ports,
                host_id,
                child,
            });
        }

        let mut collided = false;
        for node in &nodes {
            match wait_ready(node, Duration::from_secs(90)) {
                Ok(()) => {}
                Err(NotReady::PortCollision) => {
                    eprintln!(
                        "node {} lost a listener port to a concurrent process; re-drawing every \
                         band and restarting the cluster (attempt {attempt}/{ATTEMPTS})",
                        node.host_id
                    );
                    collided = true;
                    break;
                }
                Err(NotReady::Timeout) => panic!(
                    "node {} did not become ready within 90s; web={} log tail:\n{}",
                    node.host_id,
                    node.ports.web,
                    tail(&node.log(), 40)
                ),
            }
        }
        if !collided {
            return nodes;
        }
        last_log = nodes
            .iter()
            .map(|n| format!("node {}:\n{}", n.host_id, tail(&n.log(), 20)))
            .collect::<Vec<_>>()
            .join("\n");
    }
    panic!(
        "the cluster never came up: every one of {ATTEMPTS} attempts lost a listener port to a \
         concurrent process. Last logs:\n{last_log}"
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
        .map(|body| body.contains("\"ddl_path\":\"cluster\""))
        .unwrap_or(false)
}

/// Wait until **every** node reports the replicated `DdlPath::Cluster` — the
/// precondition for a `CREATE TABLE` to reach every node.
async fn wait_cluster_ddl_ready(nodes: &[Node], budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        let ddl: Vec<bool> = nodes.iter().map(ddl_path_is_cluster).collect();
        if ddl.iter().all(|ready| *ready) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not install the replicated DDL path on every node within {budget:?} \
             (ddl_path_per_node={ddl:?}):{}",
            dump_all_logs(nodes, 60)
        );
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

/// Every row of the table as `(id, value)` pairs, in id order.
///
/// A failure is an `Err` and never an empty vector: "the table read failed" and
/// "the table is empty" are different findings.
async fn read_rows(client: &Client, table: &str) -> Result<Vec<(i32, String)>, String> {
    let rows = client
        .query(&format!("SELECT id, v FROM {table} ORDER BY id"), &[])
        .await
        .map_err(|e| describe(&e))?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, i32>("id"), row.get::<_, String>("v")))
        .collect())
}

/// The set of ids the table serves.
async fn served_ids(client: &Client, table: &str) -> Result<BTreeSet<i32>, String> {
    Ok(read_rows(client, table)
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect())
}

/// Read this node's own replicas, retrying on a transient error, and report the
/// id set it serves.
async fn served_ids_through(node: &Node, table: &str) -> Result<BTreeSet<i32>, String> {
    let client = pg_connect(node.ports.pg).await;
    served_ids(&client, table).await
}

/// Poll until `node` serves at least `want`, or the budget runs out. Returns the
/// last set observed either way, so the caller asserts on the VALUES.
async fn poll_until_served(
    node: &Node,
    table: &str,
    want: &BTreeSet<i32>,
    budget: Duration,
) -> BTreeSet<i32> {
    let deadline = Instant::now() + budget;
    let mut last = BTreeSet::new();
    loop {
        match served_ids_through(node, table).await {
            Ok(ids) => {
                last = ids;
                if last == *want {
                    return last;
                }
            }
            Err(error) => eprintln!(
                "node {} read failed while waiting for convergence ({error}); retrying",
                node.host_id
            ),
        }
        if Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Commit one row through a real client and REQUIRE the client to observe a
/// successful commit.
///
/// This is the whole point of the guarantee: a client that is told it did not
/// commit is owed nothing, so a refusal here is a test failure rather than a
/// tolerated outcome. The error text is carried into the assertion.
async fn commit_row(client: &Client, table: &str, id: i32, value: &str) -> Result<(), String> {
    let tag = client
        .execute(
            &format!("INSERT INTO {table} (id, v) VALUES ({id}, '{value}')"),
            &[],
        )
        .await
        .map_err(|e| describe(&e))?;
    if tag != 1 {
        return Err(format!(
            "the INSERT committed {tag} rows for id={id}; a statement that reports what it did \
             not do is not a commit this guarantee can be checked against"
        ));
    }
    Ok(())
}

// ── the test ──────────────────────────────────────────────────────────────────

/// G1, G2, G3 and G4, end to end on three REAL node processes.
///
/// The order matters and is deliberate: the fault is applied to a replica that
/// is *known* to be serving the committed rows (step 4), so "it came back" is a
/// claim about data that was demonstrably there and not about an empty node
/// that was always going to look incomplete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_row_survives_a_sigkill_and_is_repaired_onto_the_restarted_replica() {
    let cluster_name = format!("commit-guarantee-{}", std::process::id());
    // A per-run table name, so nothing a previous run left behind can satisfy an
    // assertion (a fresh data directory already prevents that; this makes it
    // impossible rather than merely unlikely).
    let table = format!("cg_{}", std::process::id());
    let run = std::process::id();
    let value = |id: i32| format!("run{run}:{id}");

    let mut alloc = PortAllocator::new();

    // ── 0. The cluster. The whole star is brought up together: every node
    //      declares `FERROSA_EXPECTED_CLUSTER_SIZE=3`, so (correctly) none reports
    //      ready until it sees the declared size, and the star is wired from the
    //      bands drawn before any node starts. A bind collision re-draws every
    //      band and restarts the lot, exactly as in the single-node case.
    let mut nodes = start_cluster(&mut alloc, &cluster_name);
    let seed_hub = format!("127.0.0.1:{}", nodes[0].ports.internode);
    for node in &nodes {
        assert!(
            !node.log().contains(PORT_IN_USE),
            "node {} hit a port collision — the cluster is not healthy:\n{}",
            node.host_id,
            tail(&node.log(), 30)
        );
        assert!(
            !node.log().contains("undersized"),
            "node {} booted with an undersized `public` keyspace, so a ONE read is not the \
             local read this test needs:\n{}",
            node.host_id,
            tail(&node.log(), 30)
        );
    }
    wait_cluster_ddl_ready(&nodes, Duration::from_secs(240)).await;

    // The instrument's precondition, asserted rather than assumed: for a ONE
    // read to be answered from this node's own replicas, the node must see the
    // whole replica set of every range. A node that sees fewer has no local
    // partitions to answer with and would be a different measurement.
    for node in &nodes {
        let count = node.ring_node_count();
        assert_eq!(
            count,
            Some(N_NODES),
            "node {} sees ring={count:?}, so a ONE read through it is not pinned to its own \
             replicas. Logs:{}",
            node.host_id,
            dump_all_logs(&nodes, 30)
        );
    }

    // ── 1. The table, through the replicated DDL path.
    let admin = pg_connect(nodes[0].ports.pg).await;
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match admin
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id int PRIMARY KEY, v text)"
                ))
                .await
            {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "CREATE TABLE {table} never succeeded: {}{}",
                        describe(&e),
                        dump_all_logs(&nodes, 40)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // ── 2. The membership-settling write. Right after bootstrap a write can be
    //      refused while Accord's participant set fills in, and a retry there
    //      says nothing about the guarantee. It is done ONCE, here, so that every
    //      write below is measured.
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match commit_row(
                &admin,
                &table,
                *BEFORE_FAULT.start(),
                &value(*BEFORE_FAULT.start()),
            )
            .await
            {
                Ok(()) => break,
                Err(e) => {
                    if Instant::now() >= deadline {
                        panic!(
                            "the membership-settling write never committed: {e}{}",
                            dump_all_logs(&nodes, 40)
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // ── 3. G1: every measured commit must be OBSERVED as a success, and a
    //      quorum must hold the row immediately afterwards — no waiting.
    let mut committed: BTreeSet<i32> = BTreeSet::new();
    committed.insert(*BEFORE_FAULT.start());
    for id in BEFORE_FAULT.start() + 1..=*BEFORE_FAULT.end() {
        let value = value(id);
        commit_row(&admin, &table, id, &value)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "the client did NOT observe a commit for id={id}, so there is no guarantee to \
                 check for it: {e}{}",
                    dump_all_logs(&nodes, 40)
                )
            });
        committed.insert(id);

        // Immediately, with no propagation window: the replicas that acked must
        // serve it from their own stores. This is the durability half of G1 — a
        // row the client was told was committed sitting on fewer than a quorum is
        // exactly the failure the guarantee forbids.
        let mut hold: Vec<usize> = Vec::new();
        for (i, node) in nodes.iter().enumerate() {
            let ids = served_ids_through(node, &table)
                .await
                .unwrap_or_else(|e| panic!("node {i} could not be read at all: {e}"));
            if ids.contains(&id) {
                hold.push(i);
            }
        }
        assert!(
            hold.len() >= QUORUM,
            "id={id} was reported committed to the client but only {hold:?} of {N_NODES} \
             replicas serve it from their own stores (a quorum of {QUORUM} is required). \
             A commit that is not durable on a quorum is the whole guarantee failing. Logs:{}",
            dump_all_logs(&nodes, 40)
        );
    }

    // ── 4. G4: every node serves every committed row from its OWN replicas. This
    //      is a bounded poll and not an immediate check, because a quorum commit
    //      does not by itself promise the replica outside the quorum has applied
    //      it yet; the window being waited out is that propagation, and it must
    //      close.
    for (i, node) in nodes.iter().enumerate() {
        let served = poll_until_served(node, &table, &committed, PROPAGATION_BUDGET).await;
        eprintln!(
            "step 2 (all replicas up): node {i} host {} serves ids {:?} of committed {:?}",
            node.host_id,
            served.iter().collect::<Vec<_>>(),
            committed.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            served,
            committed,
            "node {i} (host {}) serves {served:?} but {committed:?} was committed. A read through \
             a node that does not hold a committed row is the guarantee failing at that node. \
             Logs:{}",
            node.host_id,
            dump_all_logs(&nodes, 40)
        );
    }
    // G2's baseline: what the replica about to be killed holds in its OWN store.
    let before_fault_set = served_ids_through(&nodes[FAULT_NODE], &table)
        .await
        .expect("the fault replica must be readable before the fault");
    assert!(
        !before_fault_set.is_empty(),
        "the replica to be killed must be serving committed rows, or the fault proves nothing"
    );

    // ── 5. The fault: a real SIGKILL. Not a graceful stop, not a shutdown, not a
    //      mock: the process gets no notice and no chance to flush.
    let status = nodes[FAULT_NODE].kill_hard();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(9),
        "node {FAULT_NODE} was expected to die by SIGKILL and instead exited {status:?}; the \
         fault under test did not happen"
    );
    assert!(
        nodes[FAULT_NODE].child.try_wait().ok().flatten().is_some(),
        "the killed node must be reaped before the cluster is asked to cope without it"
    );

    // ── 6. G1 under the fault: a three-node quorum of two must still accept
    //      commits. Every one of these is measured, exactly as above.
    for id in DURING_FAULT {
        let value = value(id);
        commit_row(&admin, &table, id, &value)
            .await
            .unwrap_or_else(|e| {
                panic!(
                "with one of {N_NODES} replicas dead the cluster refused a commit that a quorum \
                 of {QUORUM} can still accept, so the client did NOT observe success for \
                 id={id}: {e}{}",
                dump_all_logs(&nodes, 40)
            )
            });
        committed.insert(id);

        let mut hold: Vec<usize> = Vec::new();
        for (i, node) in nodes.iter().enumerate().filter(|(i, _)| *i != FAULT_NODE) {
            let ids = served_ids_through(node, &table)
                .await
                .unwrap_or_else(|e| panic!("surviving node {i} could not be read: {e}"));
            if ids.contains(&id) {
                hold.push(i);
            }
        }
        assert!(
            hold.len() >= QUORUM,
            "id={id} was reported committed while a replica was dead but only {hold:?} of the \
             survivors serve it — the commit was not durable on a quorum. Logs:{}",
            dump_all_logs(&nodes, 40)
        );
    }

    // ── 7. The restarted replica. Same data directory, same ports, same host id:
    //      a restart, not a replacement.
    nodes[FAULT_NODE].restart(&cluster_name, Some(seed_hub.as_str()));
    nodes[FAULT_NODE].wait_ready(REJOIN_BUDGET);
    {
        // The instrument's precondition again, now on the restarted node: it must
        // see the whole replica set before its ONE read means "from my own store".
        let deadline = Instant::now() + REJOIN_BUDGET;
        loop {
            if nodes[FAULT_NODE].ring_node_count() == Some(N_NODES) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the restarted node never saw all {N_NODES} replicas, so a ONE read through it \
                 would not be pinned to its own store. Logs:{}",
                dump_all_logs(&nodes, 40)
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    // ── 8. G2 + G3 + G4: everything the restarted replica held before the crash
    //      is still there (nothing was rolled back), and everything committed
    //      while it was dead has come back to it. Read at ONE through the
    //      restarted replica itself, on a bounded poll: convergence may take
    //      time, but it must happen.
    let after = poll_until_served(&nodes[FAULT_NODE], &table, &committed, CONVERGENCE_BUDGET).await;
    assert!(
        after.is_superset(&before_fault_set),
        "the restart ROLLED BACK rows the replica was serving before the crash: it held \
         {before_fault_set:?} and now holds {after:?} (G2 — an acked apply is never undone). \
         Logs:{}",
        dump_all_logs(&nodes, 40)
    );
    assert_eq!(
        after,
        committed,
        "the restarted replica serves {after:?} but {committed:?} was committed and every row \
         must be repaired back onto it (G3 — divergence converges; G4 — a read from this replica \
         finds the data). Missing: {:?}. Logs:{}",
        committed.difference(&after).collect::<Vec<_>>(),
        dump_all_logs(&nodes, 60)
    );

    // ── 9. The reporting must be honest, on the SAME processes that just served
    //      the read above.
    //
    // (a) The repair path must actually run and say what it found, and any
    //     POSITIVE claim it makes must be backed by the data. Only the replica
    //     elected initiator for a range runs a session, and only it can say
    //     anything about agreement; when it reports the table converged (no
    //     divergence) it is asserting that every range comparison against every
    //     live replica, including the restarted one, found the same leaf hashes —
    //     so the restarted replica must serve every committed row at that moment.
    //     A positive claim is only reachable after the restart's transport lane is
    //     whole again, so this is a bounded poll on the system's own condition and
    //     not a race with the rejoin.
    let deadline = Instant::now() + CONVERGENCE_BUDGET;
    let mut claimer: Option<usize> = None;
    loop {
        for (i, node) in nodes.iter().enumerate() {
            let log = node.log();
            if log.contains(CONVERGED_LINE) || log.contains(DIVERGENCE_RECONCILED_LINE) {
                claimer = Some(i);
                break;
            }
        }
        if claimer.is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "after {CONVERGENCE_BUDGET:?} of repair cycles the elected initiator never reported a \
             positive outcome for the table, so the repair path cannot say whether the restarted \
             replica agrees with it. Logs:{}",
            dump_all_logs(&nodes, 60)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let claimer = claimer.expect("the loop returns only when a claimer exists");
    // Read the restarted replica AGAIN, in the same breath as the claim.
    let now = served_ids_through(&nodes[FAULT_NODE], &table)
        .await
        .unwrap_or_else(|e| panic!("node {FAULT_NODE} could not be read: {e}"));
    assert_eq!(
        now,
        committed,
        "node {claimer} reported a positive repair outcome for the table while the restarted \
         replica serves {now:?} of {committed:?}: a convergence claim the data does not support. \
         Logs:{}",
        dump_all_logs(&nodes, 60)
    );

    // (b) A replica that verifies nothing must say WHY. One initiator per range is
    //     elected, so every other replica runs no session by design; its silence
    //     must name that design rather than read as a failure ("replica agreement
    //     NOT verified"), which is what misled an investigation.
    let logs: Vec<String> = nodes.iter().map(|n| n.log()).collect();
    let honest_silence = logs
        .iter()
        .filter(|log| log.contains(NOT_THE_INITIATOR_LINE))
        .count();
    assert!(
        honest_silence >= 1,
        "not one of the {N_NODES} replicas reported WHY it ran no session. One initiator per \
         range is elected, so at least one replica here verifies nothing on purpose — and its \
         log must say that, not leave its silence to be read as a failure. Logs:{}",
        dump_all_logs(&nodes, 60)
    );
    for (i, log) in logs.iter().enumerate() {
        assert!(
            !log.contains("no repair sessions attempted"),
            "node {i} still reports an empty repair cycle in the form that misled an \
             investigation: `no repair sessions attempted for table (no peers or no initiated \
             ranges); replica agreement NOT verified`. The condition is right; the report is \
             not. Logs:{}",
            dump_all_logs(&nodes, 60)
        );
    }
}
