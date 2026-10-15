//! The GUARANTEED REPLICA DELETE, pinned on **three real `ferrosa` node
//! processes** — forge t_15b75033, `ferrosa-cluster` FMEA CL-38.
//!
//! > "the full guaranteed replica delete, to make sure we do not have rows
//! > resurrected"
//!
//! # What this covers that the in-process guard cannot
//!
//! `ferrosa-cql/tests/plain_delete_after_lwt.rs` (branch
//! `fix/plain-delete-after-lwt`) pins the same defect at the consensus /
//! apply / merge level with in-process nodes and a `Direct` write path — so no
//! write in that fixture is ever *replicated*. This test closes the remaining
//! half at the process boundary: three real `ferrosa` binaries, a real
//! replicated keyspace at **RF 3**, real CQL statements through the real client
//! path, and an assertion that reads **each replica's own store**, not a
//! quorum that a peer could satisfy.
//!
//! # THIS IS A REGRESSION GUARD, NOT A RED
//!
//! Every case here is expected to be GREEN on `main`. The defect — the Accord
//! applier stamped LWT cells with the HLC's **nanoseconds** while every plain
//! write stamps **microseconds**, so an LWT row sat ~1000x in the future and no
//! plain `DELETE` could shadow it — was fixed on 2026-10-08 by `34acf0ef`
//! (`fix(accord): stamp Accord cells in microseconds, with legacy rows
//! readable`), an ancestor of `main`. The fix divides the agreed HLC time by
//! 1000 (`ferrosa_cluster::accord::apply::accord_cell_timestamp`) and maps
//! values already on disk (`ferrosa_common::cell_ts::normalize_cell_ts`).
//!
//! A guard that is green on the fixed tree proves nothing on its own, so it is
//! proved NON-VACUOUS by a MIRROR CONTROL: revert those two functions to the
//! pre-fix behaviour (raw nanoseconds; identity normalisation), re-run, and
//! require cases B, C and E to go RED — row still readable from every replica —
//! while case A stays green. Case A is in this file as the POSITIVE CONTROL: a
//! harness that deleted nothing at all would satisfy every absence assertion
//! vacuously.
//!
//! # The cases
//!
//! | # | Sequence | On the fixed tree | Under the mirror control |
//! |---|---|---|---|
//! | A | INSERT (plain), plain DELETE | row gone | **still green** (positive control) |
//! | B | INSERT (plain), `UPDATE ... IF version=1` reporting `[applied]=true`, plain DELETE | row gone | **row survives** |
//! | C | `INSERT ... IF NOT EXISTS` reporting `[applied]=true`, plain DELETE | row gone | **row survives** |
//! | E | as B, plain DELETE 3 s later | row gone | **row survives** — so not a sub-second race |
//!
//! Each case asserts the EFFECT and not the status: the row must be readable at
//! its expected VALUES from **every** replica before the delete (the positive
//! control that makes the absence assertion non-vacuous), and after the plain
//! `DELETE` is acknowledged it must be unreadable **from each replica
//! individually**, asserting the values read back (row absent), never a lone
//! count.
//!
//! # No resurrection — how the window is made real
//!
//! After the delete the test does not stop at one read. It (1) re-reads every
//! replica's own store; (2) issues a **`CL=ALL` full scan through every node**,
//! which fans the read out over all three replicas and compares their digests —
//! the read path that performs read repair when replicas disagree; (3) fires the
//! node's own anti-entropy trigger,
//! `POST /api/cluster/repair?keyspace=…&table=…&rf=3`, which runs a real
//! `RepairCoordinator::repair_table` against every peer that replicates the
//! table, and requires the summary to show at least one successful session (a
//! repair that ran nothing is not evidence); and (4) RE-READS every replica and
//! asserts the row is STILL absent. The production anti-entropy scheduler also
//! runs inside the test's lifetime (`FERROSA_AUTO_REPAIR_INTERVAL_SECS=1`).
//!
//! # How a read is pinned to ONE replica's store
//!
//! Two independent facts, both asserted rather than assumed:
//!
//! * the client session for node *i* is pinned to node *i* with
//!   `SingleTargetLoadBalancingPolicy`, so node *i* is the coordinator of every
//!   statement it serves; and
//! * every read is issued at `Consistency::One`. With RF == node count (3 == 3)
//!   every node owns every token range, and
//!   `remote_count_for_cl(One, rf, node_count, remotes)` returns **0**
//!   (`ferrosa-cluster/src/coordinator/range_read_stream.rs`), so a `CL=ONE`
//!   range read is served from the coordinator's own storage alone. The test
//!   asserts the precondition — every node sees all three ring entries — before
//!   it reads, and a `QUORUM` read could not show this, because a quorum read
//!   through node *N* is satisfied by *N*'s peers.
//!
//! The plain `DELETE` is issued at `Consistency::All`: its acknowledgement then
//! means *every replica applied the tombstone*, which is what makes the
//! immediate per-replica absence assertion exact. The replication/hint path that
//! a `QUORUM`-acked plain `DELETE` would additionally need (a non-quorum replica
//! that has not yet received the tombstone) is deliberately NOT claimed by this
//! guard — see the crate README.
//!
//! # Layout
//!
//! A full mesh on localhost: every node seeds off the other two, so the cluster
//! is RF 3 with every node owning every token range. The keyspace is created
//! explicitly at `replication_factor: 3` rather than inferred. Each node binds
//! its own band of eight ports from the shared allocator whose ports sit BELOW
//! the kernel's ephemeral range (`tests/common/ports.rs`); a node that loses one
//! fails loud rather than being read as evidence.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::{Consistency, Statement};
use scylla::value::{CqlValue, Row};
use uuid::Uuid;

mod common;
use common::ports::PortAllocator;

/// `base64("ferrosa_admin:ferrosa_admin")` — the seeded development superuser.
/// Required for `POST /api/cluster/repair` (the `/api/*` router is behind the
/// Basic-auth middleware) and for the CQL login, because auth is ENABLED.
const ADMIN_BASIC: &str = "Basic ZmVycm9zYV9hZG1pbjpmZXJyb3NhX2FkbWlu";
const ADMIN_CQL_USER: &str = "ferrosa_admin";
const ADMIN_CQL_PASSWORD: &str = "ferrosa_admin";

const N_NODES: usize = 3;
/// The keyspace is created at this factor explicitly, so "RF 3" is a stated
/// fact of the fixture and not an inference about how the auto-created
/// `public` keyspace derives its size from the seed list.
const KEYSPACE: &str = "pdl";
const TABLE: &str = "t";
const REPLICATION_FACTOR: usize = 3;
/// The one row under test. A literal UUID keeps the statement text identical
/// across nodes and across runs, so nothing a previous run left behind can
/// satisfy an assertion.
const ROW_ID: &str = "11111111-2222-3333-4444-555555555555";

/// How long the test will wait for a condition the system itself owns:
/// replication of a write to every replica, the cluster's DDL path, and the
/// post-delete convergence of a tombstone. Each is a BOUNDED POLL, not a fixed
/// sleep; exhausting one is a failure.
const PROPAGATION_BUDGET: Duration = Duration::from_secs(60);
const FORMATION_BUDGET: Duration = Duration::from_secs(240);
const READY_BUDGET: Duration = Duration::from_secs(90);
const REPAIR_BUDGET: Duration = Duration::from_secs(120);

/// The node process's own report that one of its eight listeners could not
/// bind. A harness accident, never a product failure.
const PORT_IN_USE: &str = "Address already in use";

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
/// BELOW the kernel's ephemeral range (`tests/common/ports.rs`): a port inside
/// that range can already be held by any outbound connection in the VM, which is
/// how a node's SPARQL listener lost its port in CI on 2026-10-13.
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
    /// Kept alive for the whole test; its directory holds the node's data.
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

    /// The number of ring entries this node currently sees — the precondition
    /// for a `CL=ONE` read being served from this node's own replicas alone.
    fn ring_node_count(&self) -> Option<usize> {
        http_get(self.ports.web, "/api/cluster/ring", Some(ADMIN_BASIC))
            .ok()
            .map(|body| body.matches("\"node_id\":").count())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Exec the node binary once so the OS has it mapped, validated and warm.
///
/// macOS's first exec of a freshly written binary stalls inside `_dyld_start`
/// (code-signature validation plus a cold page-in), and that stall lands inside
/// every timed window in this file. It is paid once here, before any window
/// opens. `--version` prints one line and exits before the node does any setup.
fn warm_node_binary() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        match Command::new(env!("CARGO_BIN_EXE_ferrosa"))
            .arg("--version")
            .stdin(Stdio::null())
            .output()
        {
            Ok(out) => eprintln!(
                "harness: warmed the node binary ({})",
                String::from_utf8_lossy(&out.stdout).trim()
            ),
            Err(e) => panic!("could not exec the node binary to warm it: {e}"),
        }
    });
}

/// Launch one `ferrosa` process. `seed` is the node's internode seed list: a
/// node that names the other two gives a full mesh, and the cluster resolves to
/// RF 3 with every node owning every token range.
fn spawn_child(
    data_dir: &Path,
    ports: Ports,
    host_id: &str,
    cluster_name: &str,
    seed: &str,
) -> Child {
    warm_node_binary();
    let log = std::fs::File::create(data_dir.join("node.log")).expect("create node log");
    let stderr = log.try_clone().expect("clone log handle");

    Command::new(env!("CARGO_BIN_EXE_ferrosa"))
        .env("RUST_MIN_STACK", "33554432") // debug-profile worker stack
        .env("RUST_LOG", "info")
        .env("FERROSA_AUTH_ENABLED", "true")
        .env("FERROSA_DATA_DIR", data_dir)
        .env("FERROSA_CLUSTER_NAME", cluster_name)
        .env("FERROSA_HOST_ID", host_id)
        .env("FERROSA_SEED", seed)
        // Run the PRODUCTION anti-entropy scheduler inside the test's lifetime,
        // so a repair pass over the table really happens after the delete. This
        // shortens a cadence, never a bound.
        .env("FERROSA_AUTO_REPAIR_INTERVAL_SECS", "1")
        // Flush quickly so committed rows and tombstones reach SSTables rather
        // than living only in a memtable — the merge/repair path is the subject.
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
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn ferrosa node")
}

// ── readiness ─────────────────────────────────────────────────────────────────

const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const PROBE_READ_TIMEOUT: Duration = Duration::from_millis(500);
/// The authenticated probe pays a bcrypt `cost=12`; 5 s is the bound
/// `ferrosa-cluster/src/ddl_path.rs` puts on its own DDL waits.
const AUTH_PROBE_READ_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_TAIL_LINES: usize = 40;
const NO_FATAL_LINE: &str = "none — the log has no FATAL/ERROR line";

/// A minimal HTTP/1.1 request. `authorization` is a full header value, or
/// `None` for the unauthenticated probes. Returns the whole response, status
/// line included, or a reason no answer arrived.
fn http_request(
    port: u16,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    read_timeout: Duration,
) -> Result<String, String> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, PROBE_CONNECT_TIMEOUT)
        .map_err(|e| format!("not listening: connect failed ({e})"))?;
    stream
        .set_read_timeout(Some(read_timeout))
        .map_err(|e| format!("cannot set the probe read timeout: {e}"))?;
    let auth = authorization
        .map(|a| format!("Authorization: {a}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}Content-Length: 0\r\n\
         Connection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("request write failed: {e}"))?;
    let mut body = String::new();
    stream
        .read_to_string(&mut body)
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                format!("no answer within {read_timeout:?} — the read timed out")
            }
            _ => format!("reading the answer failed: {e}"),
        })?;
    Ok(body)
}

/// A `GET` bounded by the budget that matches the request it makes.
///
/// An UNAUTHENTICATED probe (`/readyz`, `/metrics`) answers in single-digit
/// milliseconds even loaded, so 500 ms is generous. An AUTHENTICATED probe
/// (`/api/cluster/ring`, `/api/cluster/status`, `/api/cluster/repair`) sits
/// behind the console's Basic-auth middleware and pays a bcrypt `cost=12`
/// comparison — measured at 0.62 s on a cold cache here, ~0.5 ms on a hit — and
/// the cache is invalidated by every schema mutation, so a bootstrapping cluster
/// churns it. Under the shared 500 ms the ring probe timed out and
/// `ring_node_count` read `None` for a ring that in fact held all three nodes,
/// which the test then reported as a product fact it had never observed
/// (t_c8625592's trap, re-derived here the first time this harness ran).
fn http_get(port: u16, path: &str, authorization: Option<&str>) -> Result<String, String> {
    let read_timeout = if authorization.is_some() {
        AUTH_PROBE_READ_TIMEOUT
    } else {
        PROBE_READ_TIMEOUT
    };
    http_request(port, "GET", path, authorization, read_timeout)
}

/// A `POST` with an empty body — the shape `/api/cluster/repair` takes.
fn http_post(port: u16, path: &str, authorization: Option<&str>) -> Result<String, String> {
    http_request(port, "POST", path, authorization, AUTH_PROBE_READ_TIMEOUT)
}

/// Extract a top-level `"key":"value"` string field from a small flat JSON
/// object, without pulling in a JSON parser.
fn json_str(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Extract a top-level `"key":<number>` field.
fn json_num(body: &str, key: &str) -> Option<u64> {
    let needle = format!("\"{key}\":");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Whether a log line says the node was failing.
fn is_fatal_line(line: &str) -> bool {
    line.contains("FATAL:") || line.contains(" ERROR ") || line.starts_with("ERROR ")
}

/// The FIRST line in `log` that says why the node stopped — a 40-line tail can
/// start after the cause.
fn first_fatal_line(log: &str) -> Option<&str> {
    log.lines().find(|line| is_fatal_line(line))
}

/// Every node's log tail, labelled, each led by its first fatal/ERROR line.
fn dump_all_logs(nodes: &[Node], lines: usize) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let log = n.log();
            format!(
                "\n===== node {i} host {} (web {}) first fatal/ERROR line: {}; log tail =====\n{}",
                n.host_id,
                n.ports.web,
                first_fatal_line(&log).unwrap_or(NO_FATAL_LINE),
                tail(&log, lines)
            )
        })
        .collect()
}

/// Split a raw HTTP response into its status code and body.
fn parse_status(raw: &str) -> Option<u16> {
    raw.lines().next()?.split_whitespace().nth(1)?.parse().ok()
}

/// Why a node did not reach readiness.
///
/// A listener that cannot bind is a HARNESS accident, not a product failure:
/// [`alloc_ports`] hands out a port that is free at that instant, and between
/// then and the node's `bind` a concurrently running test process can take it.
/// The band it draws from sits BELOW the kernel's ephemeral range
/// (`common::ports`), so the kernel cannot hand a node's port to an outbound
/// connection; this is the second line of defence. The node never reports ready
/// when it loses a port, and reporting THAT as a red would be a false failure
/// about the product; it is re-drawn and retried instead. Every other reason
/// fails the test with the node's own evidence.
enum NotReady {
    PortCollision,
    Failure(String),
}

/// Wait until the node reports `{"ready":true}` on `/readyz`.
///
/// A node that has already EXITED fails the wait at once, naming the exit
/// status and the node's own first fatal line: polling a corpse for the whole
/// budget hides the cause (t_4e9a3e61). An alive-but-never-ready node fails
/// naming the CONDITION it was reporting (`HTTP 503 from /readyz`), because
/// throwing that answer away makes a patient node look like a dead one.
fn wait_ready(node: &mut Node, timeout: Duration) -> Result<(), NotReady> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_condition;
    loop {
        if let Some(status) = node.child.try_wait().expect("try_wait on the node") {
            let log = node.log();
            return Err(NotReady::Failure(format!(
                "node {} exited {:?} into the readiness wait: {status}; web={}\n  \
                 first fatal/ERROR line: {}\n  log tail:\n{}",
                node.host_id,
                started.elapsed(),
                node.ports.web,
                first_fatal_line(&log).unwrap_or(NO_FATAL_LINE),
                tail(&log, LOG_TAIL_LINES)
            )));
        }
        match http_get(node.ports.web, "/readyz", None) {
            Ok(raw) if raw.contains("\"ready\":true") => return Ok(()),
            Ok(raw) => {
                last_condition = match parse_status(&raw) {
                    Some(status) => format!("HTTP {status} from /readyz"),
                    None => "an unparseable /readyz answer".to_string(),
                };
            }
            Err(why) => last_condition = format!("/readyz: {why}"),
        }
        if Instant::now() >= deadline {
            let log = node.log();
            if log.contains(PORT_IN_USE) {
                return Err(NotReady::PortCollision);
            }
            return Err(NotReady::Failure(format!(
                "node {} did not become ready within {timeout:?}; web={}; node reported: \
                 {last_condition}\n  first fatal/ERROR line: {}\n  log tail:\n{}",
                node.host_id,
                node.ports.web,
                first_fatal_line(&log).unwrap_or(NO_FATAL_LINE),
                tail(&log, LOG_TAIL_LINES)
            )));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The host UUID for node `i`.
///
/// It must be distinct in BOTH of the two halves the node id is derived from:
/// Raft's `uuid_to_node_id` reads `bytes[8..16]` (little-endian) and Accord's
/// numeric node id reads `bytes[0..8]` (big-endian). Repeating one byte across
/// all sixteen bytes — the shape `tests/docker-compose.cluster.yml` ships — is
/// distinct in both; a single distinguishing group at either end is not, and
/// the two collision modes look like broken consensus rather than a bad id.
fn node_host_id(i: usize) -> String {
    let d = 0x11u8 * (i as u8 + 1);
    format!(
        "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
         {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
    )
}

fn node_uuid(i: usize) -> Uuid {
    Uuid::parse_str(&node_host_id(i)).expect("node host id parses as a UUID")
}

/// Whether this node has installed the Raft-replicated `DdlPath::Cluster`.
fn ddl_path_is_cluster(node: &Node) -> bool {
    http_get(node.ports.web, "/api/cluster/status", Some(ADMIN_BASIC))
        .ok()
        .and_then(|raw| json_str(&raw, "ddl_path"))
        .map(|path| path == "cluster")
        .unwrap_or(false)
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

/// Wait until a Raft leader exists AND every node has installed the replicated
/// DDL path — the preconditions for a `CREATE` to reach every node.
async fn wait_cluster_ddl_ready(nodes: &[Node], budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        let leader = find_leader(nodes);
        let ddl: Vec<bool> = nodes.iter().map(ddl_path_is_cluster).collect();
        if leader.is_some() && ddl.iter().all(|ready| *ready) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the cluster did not elect a Raft leader and install the replicated DDL path on \
             every node within {budget:?} (leader={leader:?}, ddl_path_per_node={ddl:?}), so a \
             CREATE would not reach every node:{}",
            dump_all_logs(nodes, 60)
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ── the cluster ───────────────────────────────────────────────────────────────

/// Three real node processes plus one CQL session PINNED to each of them.
struct Cluster {
    nodes: Vec<Node>,
    /// `sessions[i]` is coordinated by `nodes[i]` and only by it: a statement
    /// sent through it is served from node `i`'s own storage at `CL=ONE`.
    sessions: Vec<Session>,
}

/// Bring up the three-node full mesh and a pinned client session per node.
async fn start_cluster(cluster_name: &str) -> Cluster {
    const ATTEMPTS: usize = 5;
    let mut alloc = PortAllocator::new();

    for attempt in 1..=ATTEMPTS {
        // All ports are drawn BEFORE any node starts: a node's seed is another
        // node's internode port, so a per-node retry would strand its followers.
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

        let mut nodes: Vec<Node> = Vec::with_capacity(N_NODES);
        for (i, ports) in all_ports.iter().enumerate() {
            let dir = tempfile::tempdir().expect("node data dir");
            let data_dir = dir.path().to_path_buf();
            let host_id = node_host_id(i);
            let child = spawn_child(&data_dir, *ports, &host_id, cluster_name, &seeds_for(i));
            nodes.push(Node {
                _dir: dir,
                data_dir,
                ports: *ports,
                host_id,
                child,
            });
        }

        let mut collided = false;
        let mut failure: Option<String> = None;
        for node in &mut nodes {
            match wait_ready(node, READY_BUDGET) {
                Ok(()) => {}
                Err(NotReady::PortCollision) => {
                    collided = true;
                    break;
                }
                Err(NotReady::Failure(why)) => {
                    failure = Some(why);
                    break;
                }
            }
        }
        if let Some(why) = failure {
            panic!("{why}\n{}", dump_all_logs(&nodes, 60));
        }
        if collided {
            eprintln!(
                "attempt {attempt}/{ATTEMPTS}: a node lost a listener port to a concurrent \
                 process; re-drawing every band and restarting the cluster"
            );
            continue;
        }

        // Every node must be fully up, and must see the whole ring: a `CL=ONE`
        // read is only a local read while the coordinator owns every range.
        for node in &nodes {
            assert!(
                !node.log().contains(PORT_IN_USE),
                "node {} hit a port collision — the cluster is not healthy:\n{}",
                node.host_id,
                tail(&node.log(), 30)
            );
        }
        let addresses: Vec<String> = nodes
            .iter()
            .map(|n| format!("127.0.0.1:{}", n.ports.cql))
            .collect();
        let sessions = connect_pinned_sessions(&addresses, &nodes).await;
        return Cluster { nodes, sessions };
    }
    panic!("the cluster never came up cleanly in {ATTEMPTS} attempts");
}

/// Connect one CQL session per node, each PINNED to that node.
///
/// Topology discovery has to complete before the target node can be resolved
/// from the session's own metadata, so the session is built with the ordinary
/// policy and then remapped to a `SingleTargetLoadBalancingPolicy` naming the
/// target node — the pattern `ferrosa-jepsen/src/cql_session.rs` uses for
/// coordinator affinity.
async fn connect_pinned_sessions(addresses: &[String], nodes: &[Node]) -> Vec<Session> {
    let deadline = Instant::now() + PROPAGATION_BUDGET;
    let mut sessions = Vec::with_capacity(nodes.len());
    for (i, node) in nodes.iter().enumerate() {
        let want = node_uuid(i);
        let mut profile_handle = ExecutionProfile::builder()
            .consistency(Consistency::One)
            .build()
            .into_handle();
        let session = SessionBuilder::new()
            .known_nodes(addresses)
            .user(ADMIN_CQL_USER, ADMIN_CQL_PASSWORD)
            .default_execution_profile_handle(profile_handle.clone())
            .pool_size(PoolSize::PerHost(
                NonZeroUsize::new(1).expect("1 is nonzero"),
            ))
            .build()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "the CQL session for node {i} (host {}) could not be built: {e:?}\n{}",
                    node.host_id,
                    dump_all_logs(nodes, 40)
                )
            });
        // The pinned node must be in the session's OWN metadata, or the policy
        // resolves nothing and every statement fails with an empty plan.
        let target = loop {
            let found = session
                .get_cluster_state()
                .get_nodes_info()
                .iter()
                .find(|n| n.host_id == want)
                .cloned();
            if let Some(target) = found {
                break target;
            }
            assert!(
                Instant::now() < deadline,
                "node {i} (host id {want}) never appeared in the CQL session's cluster \
                 metadata within {PROPAGATION_BUDGET:?}, so no read can be pinned to its own \
                 storage. Nodes in metadata: {:?}\n{}",
                session
                    .get_cluster_state()
                    .get_nodes_info()
                    .iter()
                    .map(|n| n.host_id.to_string())
                    .collect::<Vec<_>>(),
                dump_all_logs(nodes, 40)
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let policy = SingleTargetLoadBalancingPolicy::new(NodeIdentifier::Node(target), None);
        let profile = ExecutionProfile::builder()
            .consistency(Consistency::One)
            .load_balancing_policy(policy)
            .build();
        profile_handle.map_to_another_profile(profile);
        sessions.push(session);
    }
    sessions
}

// ── CQL ───────────────────────────────────────────────────────────────────────

/// Run a statement at an explicit consistency, discarding any rows.
async fn cql_exec(session: &Session, query: &str, cl: Consistency) -> Result<(), String> {
    let mut stmt = Statement::new(query);
    stmt.set_consistency(cl);
    session
        .query_unpaged(stmt, &[])
        .await
        .map(|_| ())
        .map_err(|e| format!("`{query}` failed: {e:?}"))
}

/// Run a conditional statement and return the `[applied]` flag the server
/// reported. The flag is the statement's own claim, which is why the cases
/// assert it rather than assuming the write landed.
async fn cql_applied(session: &Session, query: &str) -> Result<bool, String> {
    let rows = cql_rows(session, query, Consistency::Quorum).await?;
    let row = rows
        .first()
        .ok_or_else(|| format!("`{query}` returned no [applied] row"))?;
    let applied = row.columns.first().cloned().flatten().ok_or_else(|| {
        format!("`{query}` returned a null first column; an LWT must report [applied]: {row:?}")
    })?;
    match applied {
        CqlValue::Boolean(applied) => Ok(applied),
        other => Err(format!(
            "`{query}` returned {other:?} as its first column; an LWT must report a boolean \
             [applied]"
        )),
    }
}

/// Run a statement and return its rows.
async fn cql_rows(session: &Session, query: &str, cl: Consistency) -> Result<Vec<Row>, String> {
    let mut stmt = Statement::new(query);
    stmt.set_consistency(cl);
    let result = session
        .query_unpaged(stmt, &[])
        .await
        .map_err(|e| format!("`{query}` failed: {e:?}"))?;
    let rows_result = match result.into_rows_result() {
        Ok(rows) => rows,
        // A statement that returns no result set (a plain INSERT or DELETE)
        // is not a failure; it simply has no [applied] row to read.
        Err(_) => return Ok(Vec::new()),
    };
    rows_result
        .rows::<Row>()
        .map_err(|e| format!("`{query}` rows could not be deserialized: {e:?}"))?
        .collect::<Result<Vec<Row>, _>>()
        .map_err(|e| format!("`{query}` row read failed: {e:?}"))
}

/// The `(version, payload)` the table serves for `ROW_ID`, read through
/// `session` — which is pinned to ONE node, so at `CL=ONE` the answer comes from
/// that node's own storage alone.
///
/// A failure is an `Err` and never `None`: "the read failed" and "the row is
/// absent" are different findings.
async fn read_row(session: &Session, cl: Consistency) -> Result<Option<(i64, String)>, String> {
    let query = format!("SELECT id, version, payload FROM {KEYSPACE}.{TABLE}");
    for row in cql_rows(session, &query, cl).await? {
        let id = match row.columns.first().cloned().flatten() {
            Some(CqlValue::Uuid(id)) => id.to_string(),
            Some(other) => return Err(format!("id column is {other:?}, not a uuid")),
            None => return Err("id column is null".to_string()),
        };
        if id != ROW_ID {
            continue;
        }
        let version = match row.columns.get(1).cloned().flatten() {
            Some(CqlValue::BigInt(v)) => v,
            Some(other) => return Err(format!("version column is {other:?}, not a bigint")),
            None => return Err("version column is null".to_string()),
        };
        let payload = match row.columns.get(2).cloned().flatten() {
            Some(CqlValue::Text(t)) => t,
            Some(other) => return Err(format!("payload column is {other:?}, not text")),
            None => return Err("payload column is null".to_string()),
        };
        return Ok(Some((version, payload)));
    }
    Ok(None)
}

/// Poll until node `i`'s OWN store serves `want`, or the budget runs out.
/// Returns the last value observed either way, so the caller asserts on VALUES.
async fn poll_row_from_replica(
    cluster: &Cluster,
    i: usize,
    want: Option<(i64, String)>,
    budget: Duration,
) -> Result<Option<(i64, String)>, String> {
    let deadline = Instant::now() + budget;
    loop {
        match read_row(&cluster.sessions[i], Consistency::One).await {
            Ok(seen) if seen == want => return Ok(seen),
            Ok(seen) => {
                // The values are what the caller asserts on, so the last value
                // observed is returned rather than a bare timeout.
                if Instant::now() >= deadline {
                    return Ok(seen);
                }
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The row must be readable, at exactly `want`, from EVERY replica's own store.
async fn assert_present_on_every_replica(cluster: &Cluster, case: &str, want: (i64, String)) {
    for i in 0..cluster.nodes.len() {
        let seen = poll_row_from_replica(cluster, i, Some(want.clone()), PROPAGATION_BUDGET)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "{case}: node {i} (host {}) could not be read at all: {e}\n{}",
                    cluster.nodes[i].host_id,
                    dump_all_logs(&cluster.nodes, 40)
                )
            });
        assert_eq!(
            seen,
            Some(want.clone()),
            "{case}: precondition — node {i} (host {}) must serve the row at {want:?} BEFORE \
             the plain DELETE, or the assertion after it says nothing. It serves {seen:?}. \
             Logs:{}",
            cluster.nodes[i].host_id,
            dump_all_logs(&cluster.nodes, 40)
        );
    }
}

/// The row must be absent from EVERY replica's own store.
async fn assert_absent_from_every_replica(cluster: &Cluster, case: &str, phase: &str) {
    for i in 0..cluster.nodes.len() {
        let seen = read_row(&cluster.sessions[i], Consistency::One)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "{case}/{phase}: node {i} (host {}) could not be read at all: {e}\n{}",
                    cluster.nodes[i].host_id,
                    dump_all_logs(&cluster.nodes, 40)
                )
            });
        assert!(
            seen.is_none(),
            "{case}/{phase}: node {i} (host {}) STILL SERVES the row from its own storage: \
             {seen:?}. A plain DELETE was acknowledged at CL=ALL, so every replica applied the \
             tombstone — a row that outlives it is the resurrection this guard exists for. \
             Logs:{}",
            cluster.nodes[i].host_id,
            dump_all_logs(&cluster.nodes, 60)
        );
    }
}

/// A `CL=ALL` full scan through every node.
///
/// At `CL=ALL` the coordinator must gather from every replica
/// (`remote_count_for_cl` returns the full remote count), so this read compares
/// all three replicas' digests — the read path that performs read repair when
/// replicas disagree — and its merged answer shows the row if ANY replica still
/// held it. A surviving row found only here is the read-repair resurrection the
/// guard is about.
async fn assert_absent_at_cl_all_through_every_node(cluster: &Cluster, case: &str, phase: &str) {
    for i in 0..cluster.nodes.len() {
        let seen = read_row(&cluster.sessions[i], Consistency::All)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "{case}/{phase}: the CL=ALL read through node {i} (host {}) failed: {e}\n{}",
                    cluster.nodes[i].host_id,
                    dump_all_logs(&cluster.nodes, 40)
                )
            });
        assert!(
            seen.is_none(),
            "{case}/{phase}: a CL=ALL read through node {i} (host {}) returns {seen:?}. At \
             CL=ALL the coordinator gathers from every replica, so a replica still holding the \
             row — or read repair re-materialising one over the plain tombstone — shows up \
             here. Logs:{}",
            cluster.nodes[i].host_id,
            dump_all_logs(&cluster.nodes, 60)
        );
    }
}

/// Fire the node's own anti-entropy trigger against every peer that replicates
/// the table, and require the summary to show that a repair session really ran.
///
/// `POST /api/cluster/repair?keyspace=…&table=…&rf=3` builds a
/// `LocalRepairExecutor` and runs `RepairCoordinator::repair_table`. A summary
/// with zero sessions or zero successes is not evidence that the table was
/// repaired, so it is a failure and not a pass.
async fn run_anti_entropy_repair(cluster: &Cluster, case: &str) {
    let path =
        format!("/api/cluster/repair?keyspace={KEYSPACE}&table={TABLE}&rf={REPLICATION_FACTOR}");
    for (i, node) in cluster.nodes.iter().enumerate() {
        let raw = http_post(node.ports.web, &path, Some(ADMIN_BASIC)).unwrap_or_else(|e| {
            panic!(
                "{case}/repair: the repair trigger on node {i} (host {}) did not answer: {e}",
                node.host_id
            )
        });
        let status = parse_status(&raw).unwrap_or(0);
        assert!(
            status == 200,
            "{case}/repair: node {i} (host {}) answered HTTP {status} to the anti-entropy \
             trigger, so no repair pass was run:\n{raw}\n{}",
            node.host_id,
            dump_all_logs(&cluster.nodes, 40)
        );
        let ok = json_num(&raw, "sessions_ok").unwrap_or_else(|| {
            panic!(
                "{case}/repair: node {i} (host {}) answered HTTP 200 without a session summary, \
                 so the repair pass cannot be attested:\n{raw}",
                node.host_id
            )
        });
        let total = json_num(&raw, "sessions_total").unwrap_or(0);
        eprintln!(
            "{case}/repair: node {i} ran {ok} successful repair sessions of {total} against its \
             peers"
        );
        assert!(
            ok > 0,
            "{case}/repair: node {i} (host {}) ran {ok} of {total} repair sessions successfully, \
             so this anti-entropy pass verified nothing and cannot attest that the deleted row \
             stayed gone.\n{raw}\n{}",
            node.host_id,
            dump_all_logs(&cluster.nodes, 40)
        );
    }
}

/// The read-repair window: give read repair and anti-entropy every chance the
/// harness owns, then require the row to STILL be absent from every replica.
async fn no_resurrection_window(cluster: &Cluster, case: &str) {
    // (1) The replicas' own stores, immediately after the acknowledgement.
    assert_absent_from_every_replica(cluster, case, "immediately after the plain DELETE").await;

    // (2) A CL=ALL read through every node — all three replicas' digests
    //     compared on every replica's behalf, so a divergent copy anywhere (or a
    //     read repair that chose it) surfaces here.
    assert_absent_at_cl_all_through_every_node(cluster, case, "after a CL=ALL read").await;

    // (3) A real anti-entropy pass, driven by the node's own trigger. This is
    //     the window commit_guarantee uses the production scheduler for; here
    //     it is fired on demand as well, so a repair that DID find divergence
    //     could still re-materialise a row before the assertion below.
    run_anti_entropy_repair(cluster, case).await;

    // (4) The production scheduler has been ticking at 1 Hz throughout; a repair
    //     cycle that ran can only help a resurrection, never prevent one. Give
    //     it a bounded moment to have run at least once over the table, then
    //     re-read every replica.
    let deadline = Instant::now() + REPAIR_BUDGET;
    loop {
        let claimed = cluster.nodes.iter().any(|n| {
            let log = n.log();
            log.contains("auto-repair: table converged (no divergence)")
                || log.contains("auto-repair: DIVERGENCE reconciled for table")
        });
        if claimed || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // (5) RE-READ every replica and require the row to STILL be absent.
    assert_absent_from_every_replica(
        cluster,
        case,
        "after the read-repair and anti-entropy window",
    )
    .await;
}

// ── fixture ───────────────────────────────────────────────────────────────────

/// Bring up the cluster, the RF-3 keyspace and the table, and assert the
/// preconditions every later read depends on.
async fn fixture(case: &str) -> Cluster {
    let cluster_name = format!("replica-delete-{}-{}", case, std::process::id());
    let cluster = start_cluster(&cluster_name).await;

    // The instrument's precondition, asserted rather than assumed: for a CL=ONE
    // read to be answered from the coordinator's own replicas, the node must see
    // the whole replica set of every range. Pinned per node on a bounded poll,
    // because the membership a node reports right after bootstrap can still be
    // filling in.
    for (i, node) in cluster.nodes.iter().enumerate() {
        let deadline = Instant::now() + FORMATION_BUDGET;
        loop {
            if node.ring_node_count() == Some(N_NODES) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{case}: node {i} (host {}) never saw all {N_NODES} ring entries within \
                 {FORMATION_BUDGET:?}, so a CL=ONE read through it would not be pinned to its \
                 own replicas. Last count: {:?}. Logs:{}",
                node.host_id,
                node.ring_node_count(),
                dump_all_logs(&cluster.nodes, 40)
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    wait_cluster_ddl_ready(&cluster.nodes, FORMATION_BUDGET).await;

    // The keyspace is created at RF 3 explicitly, so "every replica" is a stated
    // property of the fixture. A CREATE that the cluster refused is a failure,
    // never a silent downgrade to a smaller factor.
    let deadline = Instant::now() + FORMATION_BUDGET;
    loop {
        let out = cql_exec(
            &cluster.sessions[0],
            &format!(
                "CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = \
                 {{'class': 'SimpleStrategy', 'replication_factor': {REPLICATION_FACTOR}}}"
            ),
            Consistency::Quorum,
        )
        .await;
        match out {
            Ok(()) => break,
            Err(e) if Instant::now() < deadline => {
                eprintln!("{case}: CREATE KEYSPACE not accepted yet ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!(
                "{case}: CREATE KEYSPACE never succeeded: {e}\n{}",
                dump_all_logs(&cluster.nodes, 40)
            ),
        }
    }

    let deadline = Instant::now() + FORMATION_BUDGET;
    loop {
        let out = cql_exec(
            &cluster.sessions[0],
            &format!(
                "CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} \
                 (id uuid PRIMARY KEY, version bigint, payload text)"
            ),
            Consistency::Quorum,
        )
        .await;
        match out {
            Ok(()) => break,
            Err(e) if Instant::now() < deadline => {
                eprintln!("{case}: CREATE TABLE not accepted yet ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!(
                "{case}: CREATE TABLE never succeeded: {e}\n{}",
                dump_all_logs(&cluster.nodes, 40)
            ),
        }
    }

    // The replicated DDL must have reached every replica before a per-replica
    // read can mean anything, so poll each replica's OWN store until it can
    // answer the read at all.
    for i in 0..cluster.nodes.len() {
        let deadline = Instant::now() + FORMATION_BUDGET;
        loop {
            match read_row(&cluster.sessions[i], Consistency::One).await {
                Ok(_) => break,
                Err(e) if Instant::now() < deadline => {
                    eprintln!(
                        "{case}: node {i} cannot read {KEYSPACE}.{TABLE} yet ({e}); retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => panic!(
                    "{case}: node {i} (host {}) never learned the replicated schema: {e}\n{}",
                    cluster.nodes[i].host_id,
                    dump_all_logs(&cluster.nodes, 40)
                ),
            }
        }
    }

    // Any row a previous run left behind would make the absence assertions
    // vacuous; clear it before the case starts.
    cql_exec(
        &cluster.sessions[0],
        &format!("DELETE FROM {KEYSPACE}.{TABLE} WHERE id = {ROW_ID}"),
        Consistency::All,
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: the pre-case cleanup DELETE failed: {e}"));

    cluster
}

/// A plain `DELETE` at `CL=ALL`, acknowledged only when every replica applied
/// the tombstone. The acknowledgement is what "guaranteed replica delete" is
/// measured against, so it is required to succeed and the statement's own
/// success is never the assertion — the reads after it are.
async fn plain_delete(cluster: &Cluster, case: &str) {
    cql_exec(
        &cluster.sessions[0],
        &format!("DELETE FROM {KEYSPACE}.{TABLE} WHERE id = {ROW_ID}"),
        Consistency::All,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "{case}: the plain DELETE was refused at CL=ALL, so there is no acknowledged delete \
             to check: {e}\n{}",
            dump_all_logs(&cluster.nodes, 40)
        )
    });
    eprintln!("{case}: the plain DELETE was acknowledged at CL=ALL");
}

// ── the cases ─────────────────────────────────────────────────────────────────

/// Case A — the POSITIVE CONTROL.
///
/// Both writes are plain, so this pins that the harness really can delete at
/// all: a fixture that never deleted anything would satisfy every absence
/// assertion in this file vacuously.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn case_a_a_plain_delete_removes_a_plainly_inserted_row_from_every_replica() {
    let case = "case A (plain INSERT, plain DELETE)";
    let cluster = fixture("case-a").await;

    cql_exec(
        &cluster.sessions[0],
        &format!(
            "INSERT INTO {KEYSPACE}.{TABLE} (id, version, payload) VALUES ({ROW_ID}, 1, 'p1')"
        ),
        Consistency::Quorum,
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: the plain INSERT was refused: {e}"));

    let want = (1i64, "p1".to_string());
    assert_present_on_every_replica(&cluster, case, want).await;

    plain_delete(&cluster, case).await;
    no_resurrection_window(&cluster, case).await;
}

/// Case B — the row's last write is a conditional `UPDATE ... IF version=1`
/// that reported `[applied]=true`. This is the reported shape: an LWT writes
/// the row, then a plain `DELETE` is acknowledged and the row must be gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn case_b_a_plain_delete_removes_a_row_last_written_by_a_conditional_update_everywhere() {
    let case = "case B (plain INSERT, UPDATE ... IF version=1, plain DELETE)";
    let cluster = fixture("case-b").await;

    cql_exec(
        &cluster.sessions[0],
        &format!(
            "INSERT INTO {KEYSPACE}.{TABLE} (id, version, payload) VALUES ({ROW_ID}, 1, 'p1')"
        ),
        Consistency::Quorum,
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: the plain INSERT was refused: {e}"));

    let applied = cql_applied(
        &cluster.sessions[0],
        &format!(
            "UPDATE {KEYSPACE}.{TABLE} SET version = 2, payload = 'p2' WHERE id = {ROW_ID} \
             IF version = 1"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: the conditional UPDATE could not be evaluated: {e}"));
    assert!(
        applied,
        "{case}: the conditional UPDATE reported [applied]=false, so the row's last write is not \
         the LWT this case is about and the DELETE below would prove nothing"
    );
    eprintln!("{case}: the conditional UPDATE reported [applied]=true");

    assert_present_on_every_replica(&cluster, case, (2i64, "p2".to_string())).await;

    plain_delete(&cluster, case).await;
    no_resurrection_window(&cluster, case).await;
}

/// Case C — the row was written by `INSERT ... IF NOT EXISTS` itself, which
/// Accord replicated to every replica. Nothing is seeded beforehand, so the
/// LWT is the only write the row ever had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn case_c_a_plain_delete_removes_a_row_written_by_insert_if_not_exists_everywhere() {
    let case = "case C (INSERT ... IF NOT EXISTS, plain DELETE)";
    let cluster = fixture("case-c").await;

    let applied = cql_applied(
        &cluster.sessions[0],
        &format!(
            "INSERT INTO {KEYSPACE}.{TABLE} (id, version, payload) VALUES ({ROW_ID}, 1, 'p1') \
             IF NOT EXISTS"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: the conditional INSERT could not be evaluated: {e}"));
    assert!(
        applied,
        "{case}: INSERT ... IF NOT EXISTS reported [applied]=false, so the row under test was not \
         written by the LWT"
    );
    eprintln!("{case}: INSERT ... IF NOT EXISTS reported [applied]=true");

    assert_present_on_every_replica(&cluster, case, (1i64, "p1".to_string())).await;

    plain_delete(&cluster, case).await;
    no_resurrection_window(&cluster, case).await;
}

/// Case E — the anti-race control.
///
/// Identical to case B except that the plain `DELETE` is issued 3 s after the
/// LWT. A sub-second ordering race cannot explain a surviving row here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn case_e_a_plain_delete_three_seconds_after_the_lwt_removes_it_everywhere() {
    let case = "case E (UPDATE ... IF version=1, 3 s, plain DELETE)";
    let cluster = fixture("case-e").await;

    cql_exec(
        &cluster.sessions[0],
        &format!(
            "INSERT INTO {KEYSPACE}.{TABLE} (id, version, payload) VALUES ({ROW_ID}, 1, 'p1')"
        ),
        Consistency::Quorum,
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: the plain INSERT was refused: {e}"));

    let applied = cql_applied(
        &cluster.sessions[0],
        &format!(
            "UPDATE {KEYSPACE}.{TABLE} SET version = 2, payload = 'p2' WHERE id = {ROW_ID} \
             IF version = 1"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("{case}: the conditional UPDATE could not be evaluated: {e}"));
    assert!(
        applied,
        "{case}: the conditional UPDATE reported [applied]=false"
    );

    assert_present_on_every_replica(&cluster, case, (2i64, "p2".to_string())).await;

    eprintln!("{case}: waiting 3 s before the plain DELETE");
    tokio::time::sleep(Duration::from_secs(3)).await;

    plain_delete(&cluster, case).await;
    no_resurrection_window(&cluster, case).await;
}
