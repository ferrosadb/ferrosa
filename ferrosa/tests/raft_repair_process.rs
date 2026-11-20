//! t_6f2ba27a, the REAL process acceptance test for the Raft repair installer.
//!
//! A node that stops while an apply group is mid-effect restarts with its
//! recovery journal in `SessionArmed`. That node used to quarantine its Raft
//! group and stay at `waiting_for="consensus_runtime"` forever: preparation
//! recorded `Prepared` and nothing consumed it.
//!
//! This test spawns three real `ferrosa` processes, writes a table and a row,
//! stops one follower and rewrites its `<raft>/<dc>.recovery.json` to the state
//! that crash leaves, then creates a second table on the surviving two. On
//! restart the victim must fetch a committed snapshot from a peer, install it,
//! become ready, keep its own rows and serve the table it missed.
//!
//! The harness (ports, spawn, readiness, logs) is the one
//! `drop_table_replica_process.rs` carries; each process test binary keeps its
//! own copy, as the others do.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

mod common;
use common::ports::{log_shows_port_collision, PortAllocator};

/// Written before the victim stops, so its repair has local rows to keep.
const TABLE_BEFORE: &str = "repair_before";
/// Created while the victim is down: only a repair from a peer delivers it.
const TABLE_AFTER: &str = "repair_after";
const N_NODES: usize = 3;
/// `base64("ferrosa_admin:ferrosa_admin")` — the seeded superuser, development mode.
const ADMIN_BASIC: &str = "Basic ZmVycm9zYV9hZG1pbjpmZXJyb3NhX2FkbWlu";

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
    /// Kept alive for the whole test; its directory survives across a restart of
    /// `child`.
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

    /// Restart this node's process in place, reusing its data directory.
    fn restart(&mut self, cluster_name: &str, seed: Option<&str>) {
        self.stop();
        self.child = spawn_child(
            &self.data_dir,
            self.ports,
            &self.host_id,
            cluster_name,
            seed,
            &[],
        );
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Exec the node binary once so the OS has it mapped, validated and warm.
///
/// Every `cargo test` invocation RELINKS the 336 MB `ferrosa` binary: the
/// crate's build script re-runs on each build, because `build.rs` watches
/// `../.git/HEAD` and in a git worktree `../.git` is a *file*, not a directory,
/// so the watched path never exists and cargo treats it as always changed
/// (`Compiling ferrosa v0.22.0` on every run). macOS's first exec of a freshly
/// written binary then stalls ~7 s inside `_dyld_start` — code-signature
/// validation plus a cold page-in of the image (measured: 7.08 s for a fresh
/// copy, 0.03 s for the next exec; 1.6 s after a bare `touch`).
///
/// That stall lands inside EVERY timed window in this file and reads as a node
/// that is slow to boot. It is a build/host artifact, not node behaviour, so it
/// is paid once here, before any window opens — the readiness wait's 5 s
/// detector above all (t_4e9a3e61). `--version` prints one line and exits before
/// the node does any setup.
fn warm_node_binary() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let started = Instant::now();
        match Command::new(env!("CARGO_BIN_EXE_ferrosa"))
            .arg("--version")
            .stdin(Stdio::null())
            .output()
        {
            Ok(out) => eprintln!(
                "harness: warmed the node binary in {:.2?} ({}) — the first exec of a \
                 freshly linked debug binary stalls in dyld on this host",
                started.elapsed(),
                String::from_utf8_lossy(&out.stdout).trim()
            ),
            Err(e) => panic!("could not exec the node binary to warm it: {e}"),
        }
    });
}

/// Launch one `ferrosa` process against an existing data directory. `extra_env`
/// is the caller's addition to the node's environment (the fault-injection seam
/// the harness tests use); it is applied last, so a test can override anything.
fn spawn_child(
    data_dir: &Path,
    ports: Ports,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
    extra_env: &[(&str, &str)],
) -> Child {
    warm_node_binary();
    let log = std::fs::File::create(data_dir.join("node.log")).expect("create node log");
    let stderr = log.try_clone().expect("clone log handle");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
    cmd.env("RUST_MIN_STACK", "33554432") // debug-profile worker stack
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
        // Flush quickly so the seeded rows reach SSTables before the DROP: the
        // resurrection this test guards is orphaned SSTables on disk, not rows
        // still in a memtable.
        .env("FERROSA_FLUSH_INTERVAL_SECS", "1")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    if let Some(seed) = seed {
        cmd.env("FERROSA_SEED", seed);
    }
    cmd.envs(extra_env.iter().copied());
    cmd.spawn().expect("spawn ferrosa node")
}

fn spawn_node(
    dir: tempfile::TempDir,
    ports: Ports,
    host_id: String,
    cluster_name: &str,
    seed: Option<&str>,
) -> Node {
    spawn_node_with_env(dir, ports, host_id, cluster_name, seed, &[])
}

/// [`spawn_node`] with extra environment for the node process.
fn spawn_node_with_env(
    dir: tempfile::TempDir,
    ports: Ports,
    host_id: String,
    cluster_name: &str,
    seed: Option<&str>,
    extra_env: &[(&str, &str)],
) -> Node {
    let data_dir = dir.path().to_path_buf();
    let child = spawn_child(&data_dir, ports, &host_id, cluster_name, seed, extra_env);
    Node {
        _dir: dir,
        data_dir,
        ports,
        host_id,
        child,
    }
}

// ── startup: re-drawing a band that a concurrent process took ────────────────

/// How many times a band is re-drawn before the failure is believed.
const STARTUP_ATTEMPTS: usize = 5;

/// The seed list for node `i` of an already-running mesh: every OTHER node's
/// internode address. Used when a node is RESTARTED mid-test, so it rejoins the
/// full mesh rather than the star.
fn mesh_seed(nodes: &[Node], i: usize) -> String {
    nodes
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != i)
        .map(|(_, node)| format!("127.0.0.1:{}", node.ports.internode))
        .collect::<Vec<_>>()
        .join(",")
}

/// Bring the whole `N_NODES` mesh up: every node's ports are allocated up front
/// so each can be given the others as seeds, and the WHOLE set is re-drawn and
/// restarted if any listener loses its port — a partial retry would change the
/// mesh the seeds describe.
fn start_mesh_cluster(alloc: &mut PortAllocator, cluster_name: &str) -> Vec<Node> {
    let mut last_failure = String::new();
    for attempt in 1..=STARTUP_ATTEMPTS {
        // Allocate every node's ports up front, so each node can be given the
        // others as seeds. Every node seeds off the other two (a full mesh): that
        // makes the auto-created `public` keyspace RF=3 on every node — its
        // replication factor is `seeds + 1` — so an Accord write has two remote
        // replicas to reach a quorum from. With the docker-compose star (node 0
        // has no seed) the keyspace is RF=1 and an Accord write that resolves to
        // the coordinator's own host fails "unknown peer: <own host_id>" and
        // never commits.
        let all_ports: Vec<Ports> = (0..N_NODES).map(|_| alloc_ports(alloc)).collect();
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

        let mut spawned: Vec<Node> = Vec::new();
        for (i, ports) in all_ports.iter().enumerate() {
            let dir = tempfile::tempdir().expect("node data dir");
            // A host id must be distinct in BOTH halves, because the node id is
            // derived from it two different ways and the halves are disjoint:
            //
            //  * Raft's `uuid_to_node_id` reads `bytes[8..16]` (the LAST two groups,
            //    little-endian) — three host ids that differ only in the FIRST groups
            //    collapse to one Raft node id, the voters cannot form a group, and no
            //    leader is ever elected ("never installed the replicated DDL path /
            //    elected a leader").
            //  * Accord derives its numeric node id from `bytes[0..8]` (the FIRST
            //    groups, big-endian) — host ids differing only in the LAST group
            //    collide onto one Accord node id and every write fails "unknown peer"
            //    against a self-send (trap 2 below).
            //
            // `0x11, 0x22, 0x33` repeated across all sixteen bytes matches the host
            // ids `tests/docker-compose.cluster.yml` ships and is distinct in both
            // halves. A single distinguishing group — either end — is not enough.
            let d = 0x11u8 * (i as u8 + 1);
            let host_id = format!(
                "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
                 {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
            );
            spawned.push(spawn_node(
                dir,
                *ports,
                host_id,
                cluster_name,
                Some(&seeds_for(i)),
            ));
        }

        let mut collision: Option<String> = None;
        for node in spawned.iter_mut() {
            if let Err(message) = wait_ready_watching(node, Duration::from_secs(90), true) {
                if log_shows_port_collision(&message) {
                    collision = Some(message);
                    break;
                }
                panic!("node {} did not become ready:\n{message}", node.host_id);
            }
        }
        match collision {
            None => return spawned,
            Some(message) => {
                eprintln!(
                    "a node lost a listener port to a concurrent process; re-drawing the whole \
                     band and restarting the cluster (attempt {attempt}/{STARTUP_ATTEMPTS})"
                );
                last_failure = message;
            }
        }
    }
    panic!(
        "the cluster never came up: all {STARTUP_ATTEMPTS} attempts lost a listener port to a \
         concurrent process. Last failure:\n{last_failure}"
    );
}

// ── readiness ─────────────────────────────────────────────────────────────────

/// The connect timeout for a readiness probe.
///
/// A loopback listener that is UP accepts at once, so this only bounds the "not
/// listening yet" case. Short on purpose — see [`PROBE_READ_TIMEOUT`].
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// The read timeout for an UNAUTHENTICATED readiness probe (`/readyz`,
/// `/metrics`).
///
/// A live `/readyz` answers in single-digit milliseconds even with the node
/// under contention, so this is generous for a real answer and still short
/// enough that an exit DURING a stalled probe costs well under the 5 s detector
/// budget of `a_node_that_dies_at_startup_fails_the_readiness_wait_at_once`. The
/// former 3 s read let a listener that had accepted but was shutting down hold
/// each poll for the full 3 s — one to two such rounds are what stretched that
/// test to 5.6-8.7 s (t_4e9a3e61).
///
/// This bound is for the UNAUTHENTICATED probes only. The authenticated status
/// probe pays a bcrypt and gets its own, larger budget — see
/// [`AUTH_PROBE_READ_TIMEOUT`].
const PROBE_READ_TIMEOUT: Duration = Duration::from_millis(500);

/// The read timeout for an AUTHENTICATED probe — `GET /api/cluster/status`,
/// which sits behind the web console's Basic-auth middleware.
///
/// This is a different request from the unauthenticated `/readyz` probe that
/// [`PROBE_READ_TIMEOUT`] bounds, and 500 ms is wrong for it. The middleware
/// verifies the credential with a bcrypt `cost=12` comparison
/// (`ferrosa/src/web/auth.rs`): measured at **0.62 s** on a cold `AuthCache` on
/// this host, ~0.5 ms on a hit. The cache is keyed on the live schema `Arc` and
/// invalidated by any schema mutation, so while a cluster bootstraps the entry
/// churns — the failing runs' node logs carried an `AuthSuccess` every ~1.78 s —
/// and a probe pays the full bcrypt. Under the former 500 ms budget every such
/// probe timed out and `ddl_path_is_cluster` reported `false` for a node whose
/// DDL path was in fact `cluster`, so the panic asserted a product fact the
/// harness never observed (t_c8625592).
///
/// 5 s is the bound `ferrosa-cluster/src/ddl_path.rs` puts on every other wait on
/// the DDL path, and ~8x the measured cold bcrypt — headroom for the loaded hosts
/// the real-node tests share. It bounds ONE probe, not the wait: the 240 s
/// formation budget in `wait_cluster_ddl_ready` is unchanged.
const AUTH_PROBE_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A minimal HTTP/1.1 GET bounded by `read_timeout`.
///
/// Returns the raw response text, or a human-readable reason no answer arrived.
/// The reason is kept rather than collapsed into `None` so a caller can tell a
/// probe that TIMED OUT from an answer that said something — collapsing those
/// two is what let a timed-out probe be reported as `ddl_path=false`
/// (t_c8625592). `authorization` is a full header value terminated with CRLF, or
/// `None` for the unauthenticated probes.
fn http_get(
    port: u16,
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
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}Connection: close\r\n\r\n");
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

/// Extract a top-level `"key":"value"` string field from a small flat JSON object,
/// without pulling in a JSON parser.
fn json_str(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// How many trailing log lines a failing harness prints.
const LOG_TAIL_LINES: usize = 40;

/// The line a diagnostic must not leave out: what a tail-only dump cannot show.
const NO_FATAL_LINE: &str = "none — the log has no FATAL/ERROR line";

/// Whether a log line says the node was failing: the synchronous `FATAL:` line
/// `lifecycle` writes when a thread panics (and the bare one it also writes to
/// stderr), or a line carrying the `ERROR` level.
///
/// The level is surrounded by spaces because tracing prints the timestamp and
/// the target first (`2026-…Z ERROR ferrosa_cluster::pair::ddl: …`).
fn is_fatal_line(line: &str) -> bool {
    line.contains("FATAL:") || line.contains(" ERROR ") || line.starts_with("ERROR ")
}

/// The FIRST line in `log` that says why the node stopped.
///
/// The tail alone is not the evidence. In the merge-queue failure of PR #551
/// (job 115370033050) the node's last line was 74 s before the harness gave up,
/// so the 40 lines it printed were the shutdown — the cause (the `ERROR
/// PairSchemaSync REFUSED` storm) sat above them. The first fatal/ERROR line is
/// the one that names the failure (t_4e9a3e61).
fn first_fatal_line(log: &str) -> Option<&str> {
    log.lines().find(|line| is_fatal_line(line))
}

/// The last `/readyz` answer the harness saw, so a wait that expires can name
/// the CONDITION the node was reporting rather than only the process state.
///
/// An alive-but-unready node answers `503` with a body saying what it is waiting
/// for (`{"ready":false,"waiting_for":"raft_leader"}`). The wait this harness
/// replaced threw that body away, so a node patiently reporting its condition
/// was indistinguishable from a dead one — and CI could never have shown which
/// condition it was (t_4e9a3e61).
struct Probe {
    /// HTTP status of the answer.
    status: u16,
    /// The body — the node's own statement of why it is not ready.
    body: String,
}

/// Split a raw HTTP response — [`http_get`] returns the whole thing, status line
/// and headers included — into its status code and body.
fn parse_probe(raw: &str) -> Option<Probe> {
    let (head, body) = raw.split_once("\r\n\r\n")?;
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some(Probe {
        status,
        body: body.trim().to_string(),
    })
}

/// The condition a `/readyz` body names, or the body itself when it names none.
///
/// `waiting_for` is the field the node uses to say what it is waiting on. It is
/// present on every not-ready answer (`ferrosa/src/web/readiness.rs`) and is the
/// thing a failing harness has to quote.
fn probe_condition(body: &str) -> String {
    match json_str(body, "waiting_for") {
        Some(condition) => format!("waiting_for=\"{condition}\""),
        None => body.chars().take(200).collect(),
    }
}

/// The body of every readiness failure this harness reports: a headline, the
/// CONDITION the node reported (its `/readyz` answer, or that it exited), the
/// node's first fatal/ERROR line, and its log tail.
fn readiness_failure_message(headline: &str, condition: &str, log: &str) -> String {
    format!(
        "{headline}\n  node reported: {condition}\n  first fatal/ERROR line: {}\n  log tail (last {LOG_TAIL_LINES} lines):\n{}",
        first_fatal_line(log).unwrap_or(NO_FATAL_LINE),
        tail(log, LOG_TAIL_LINES)
    )
}

/// What the harness must say when the node process is GONE: the exit status,
/// when it went, and the node's own first fatal line. Waiting out the budget on
/// a process that already exited hides the cause and wastes the round.
fn node_exit_message(
    host_id: &str,
    web_port: u16,
    status: &ExitStatus,
    waited: Duration,
    log: &str,
) -> String {
    readiness_failure_message(
        &format!(
            "node {host_id} exited {waited:?} into the readiness wait: {status}; web={web_port}"
        ),
        "the process is gone — it can report no /readyz condition",
        log,
    )
}

/// What the harness must say when the node is still running but never answered
/// ready: the budget it spent, the CONDITION the node itself reported on
/// `/readyz` (or that it never answered at all), and its first fatal line
/// beside the tail.
fn readiness_timeout_message(
    host_id: &str,
    web_port: u16,
    timeout: Duration,
    last_probe: Option<&Probe>,
    log: &str,
) -> String {
    let condition = match last_probe {
        Some(probe) => format!(
            "HTTP {} from /readyz: {}",
            probe.status,
            probe_condition(&probe.body)
        ),
        None => {
            format!("/readyz on web={web_port} never answered — connection refused or timed out")
        }
    };
    readiness_failure_message(
        &format!("node {host_id} did not become ready within {timeout:?}; web={web_port}"),
        &condition,
        log,
    )
}

/// The failure message if the node process is already GONE, else `None`.
fn node_gone_message(node: &mut Node, started: Instant) -> Option<String> {
    let status = node.child.try_wait().expect("try_wait on the node")?;
    let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
    Some(node_exit_message(
        &node.host_id,
        node.ports.web,
        &status,
        started.elapsed(),
        &log,
    ))
}

/// Wait for readiness, RETURNING the failure text instead of panicking, so a
/// caller can tell the two ways this fails apart:
///
/// * a listener that could not take its port — a harness accident, re-drawn and
///   retried by [`spawn_node_redrawing`] and [`start_mesh_cluster`];
/// * a node that is alive but never ready — a product failure, reported.
fn wait_ready_outcome(node: &mut Node, timeout: Duration) -> Result<(), String> {
    wait_ready_watching(node, timeout, false)
}

/// Readiness wait with a LOG WATCH: a node whose log already shows that a listener
/// could not take its port is reported AT ONCE, instead of at the deadline.
///
/// The retry paths ( [`spawn_node_redrawing`], [`start_mesh_cluster`] ) want this:
/// a lost port is re-drawn in about a second rather than after the whole budget,
/// which is what makes a collision cheap to survive on a loaded runner — where a
/// node can take seconds just to REACH its listeners. Note the CI failure that
/// prompted this: with the check only at the deadline, a node that took ~5 s to
/// bind had not yet logged anything about the taken port when a short budget
/// expired, so the collision looked like a product failure.
///
/// The strict tests do NOT want it: a node that is alive but never ready has to be
/// seen to spend its whole budget, so `wait_ready` keeps the un-watched semantics
/// and that guard stays honest.
fn wait_ready_watching(
    node: &mut Node,
    timeout: Duration,
    watch_for_lost_port: bool,
) -> Result<(), String> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_probe: Option<Probe> = None;
    let mut polls: u32 = 0;
    loop {
        if let Some(message) = node_gone_message(node, started) {
            return Err(message);
        }
        // Every 5 polls (~500 ms): has the node already said a listener could not
        // take its port? A failed bind is terminal for that listener, so this is
        // not a race with recovery.
        if watch_for_lost_port {
            polls += 1;
            if polls.is_multiple_of(5) {
                let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
                if log_shows_port_collision(&log) {
                    return Err(format!(
                        "node {} lost a listener port: a port in its band was taken by a \
                         concurrent process after allocation, so it can never become ready. \
                         log tail:\n{}",
                        node.host_id,
                        tail(&log, 20)
                    ));
                }
            }
        }
        let raw = http_get(node.ports.web, "/readyz", None, PROBE_READ_TIMEOUT);
        // An exit DURING the probe must not wait for the next tick. The probe
        // is bounded (a live /readyz answers in milliseconds; a listener that
        // accepted and then stalled is cut off by the read timeout), so this
        // keeps the check within about one probe of the exit.
        if let Some(message) = node_gone_message(node, started) {
            return Err(message);
        }
        if let Ok(raw) = raw {
            if raw.contains("\"ready\":true") {
                return Ok(());
            }
            if let Some(probe) = parse_probe(&raw) {
                last_probe = Some(probe);
            }
        }
        if Instant::now() >= deadline {
            let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
            return Err(readiness_timeout_message(
                &node.host_id,
                node.ports.web,
                timeout,
                last_probe.as_ref(),
                &log,
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Every node's log tail, labelled — the evidence a failing harness must show.
/// Each block also leads with that node's first fatal/ERROR line, because a tail
/// can start after the cause (t_4e9a3e61).
fn dump_all_logs(nodes: &[Node], lines: usize) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let log = std::fs::read_to_string(n.log_path()).unwrap_or_default();
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

/// The node reporting `ferrosa_raft_is_leader 1` on the unauthenticated
/// `/metrics` endpoint — the Raft leader that a DDL `client_write` will apply on.
fn find_leader(nodes: &[Node]) -> Option<usize> {
    nodes.iter().position(|node| {
        http_get(node.ports.web, "/metrics", None, PROBE_READ_TIMEOUT)
            .ok()
            .map(|body| {
                body.lines()
                    .any(|line| line.trim() == "ferrosa_raft_is_leader 1")
            })
            .unwrap_or(false)
    })
}

/// One node's answer to the AUTHENTICATED `GET /api/cluster/status` probe.
///
/// A `bool` here conflated two different facts — "this node is NOT on the cluster
/// DDL path" and "this probe never got an answer" — so a probe cut off by a cold
/// bcrypt was reported as `ddl_path=false`, a product fact the harness had not
/// observed (t_c8625592).
#[derive(Debug)]
enum DdlProbe {
    /// The node answered and reports `"ddl_path":"cluster"`.
    Cluster,
    /// The node answered, but its DDL path is something else. The string is the
    /// path the node itself named.
    Other(String),
    /// No usable answer inside [`AUTH_PROBE_READ_TIMEOUT`]: the listener was not
    /// up, the read timed out under a cold bcrypt, or the answer named no
    /// `ddl_path`. This is NOT evidence about the node's DDL path.
    NoAnswer(String),
}

impl DdlProbe {
    fn is_cluster(&self) -> bool {
        matches!(self, DdlProbe::Cluster)
    }

    /// How this probe's outcome reads in a failure, in terms that do not
    /// overstate what was observed.
    fn describe(&self) -> String {
        match self {
            DdlProbe::Cluster => "cluster".to_string(),
            DdlProbe::Other(path) => format!("not cluster (ddl_path=\"{path}\")"),
            DdlProbe::NoAnswer(why) => {
                format!("NO ANSWER ({why}) — not evidence about the DDL path")
            }
        }
    }
}

/// Probe whether this node has installed the Raft-replicated `DdlPath::Cluster`.
///
/// The read budget is [`AUTH_PROBE_READ_TIMEOUT`], not [`PROBE_READ_TIMEOUT`]:
/// this request is authenticated and can pay a bcrypt `cost=12`.
fn ddl_path_probe(node: &Node) -> DdlProbe {
    match http_get(
        node.ports.web,
        "/api/cluster/status",
        Some(ADMIN_BASIC),
        AUTH_PROBE_READ_TIMEOUT,
    ) {
        Ok(raw) => match json_str(&raw, "ddl_path") {
            Some(path) if path == "cluster" => DdlProbe::Cluster,
            Some(path) => DdlProbe::Other(path),
            None => DdlProbe::NoAnswer(format!(
                "answered {} but named no ddl_path",
                parse_probe(&raw)
                    .map(|p| format!("HTTP {}", p.status))
                    .unwrap_or_else(|| "an unparseable response".to_string())
            )),
        },
        Err(why) => DdlProbe::NoAnswer(why),
    }
}

/// Wait until a Raft leader has been elected AND **every** node reports the
/// replicated `DdlPath::Cluster`; return the leader's index.
///
/// Both conditions are polled per node on a generous budget (formation is not a
/// timed production limit — a flat 60 s that is too short only ever costs a
/// round). On timeout it dumps **every** node's log tail — and it distinguishes a
/// node that ANSWERED "not cluster" from a probe that never answered, so the
/// failure never asserts a product fact the harness did not observe.
async fn wait_cluster_ddl_ready(nodes: &[Node], budget: Duration) -> usize {
    let deadline = Instant::now() + budget;
    loop {
        let ddl: Vec<DdlProbe> = nodes.iter().map(ddl_path_probe).collect();
        let leader = find_leader(nodes);
        if let Some(leader) = leader {
            if ddl.iter().all(DdlProbe::is_cluster) {
                return leader;
            }
        }
        if Instant::now() >= deadline {
            let per_node: String = ddl
                .iter()
                .enumerate()
                .map(|(i, probe)| format!("  node {i}: {}", probe.describe()))
                .collect::<Vec<_>>()
                .join("\n");
            let silent = ddl
                .iter()
                .filter(|probe| matches!(probe, DdlProbe::NoAnswer(_)))
                .count();
            let logs: String = nodes
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    let log = std::fs::read_to_string(n.log_path()).unwrap_or_default();
                    format!(
                        "\n===== node {i} host {} (web {}) ddl_path: {}; log tail =====\n{}",
                        n.host_id,
                        n.ports.web,
                        ddl[i].describe(),
                        tail(&log, 80)
                    )
                })
                .collect();
            panic!(
                "cluster did not elect a Raft leader and install the replicated DDL path on \
                 every node within {budget:?} (leader={leader:?});\nper node:\n{per_node}\n\
                 {silent} of {N_NODES} probes produced NO ANSWER — a probe that timed out \
                 (a cold bcrypt can exceed any probe budget) is NOT evidence that the node \
                 is off the cluster DDL path.{logs}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// A probe that got no answer must NOT be reported as "not cluster": the failure
/// has to distinguish "never answered" from "answered: not cluster", or a
/// timed-out probe asserts a product fact the harness never observed — the bug
/// that made a real node read `ddl_path=false` for a node whose path was
/// `cluster` (t_c8625592).
#[test]
fn a_probe_that_never_answered_is_not_reported_as_not_cluster() {
    let timed_out = DdlProbe::NoAnswer(format!(
        "no answer within {AUTH_PROBE_READ_TIMEOUT:?} — the read timed out"
    ));
    let refused = DdlProbe::NoAnswer("not listening: connect failed (Connection refused)".into());
    let answered = DdlProbe::Other("pair".to_string());

    assert!(!timed_out.is_cluster() && !refused.is_cluster() && !answered.is_cluster());

    let t = timed_out.describe();
    let a = answered.describe();
    assert_ne!(
        t, a,
        "a timed-out probe must read differently from a real 'not cluster' answer"
    );
    assert!(
        t.contains("timed out"),
        "the probe must say it TIMED OUT, not merely report false: {t}"
    );
    assert!(
        t.contains("NO ANSWER") && t.contains("not evidence"),
        "a probe with no answer must not claim the node is off the path: {t}"
    );
    assert!(
        a.contains("not cluster") && a.contains("pair"),
        "an answer must quote the path the node named: {a}"
    );
    assert_eq!(DdlProbe::Cluster.describe(), "cluster");
    assert!(DdlProbe::Cluster.is_cluster());
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
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection ended: {error}");
        }
    });
    client
}

/// A Postgres error rendered with its SQLSTATE, message and detail.
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

/// Run `sql` until it succeeds or `budget` runs out. DDL and the first writes
/// after bootstrap are briefly refused while the cluster settles.
async fn execute_until_ok(client: &Client, sql: &str, budget: Duration, nodes: &[Node]) {
    let deadline = Instant::now() + budget;
    loop {
        match client.batch_execute(sql).await {
            Ok(()) => return,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "`{sql}` never succeeded: {}{}",
                    describe(&e),
                    dump_all_logs(nodes, 40)
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// The `v` values in `table`, read through `port`, once a read succeeds and
/// returns `expected`; panics with every log when it never does.
async fn wait_for_value(port: u16, table: &str, expected: &str, nodes: &[Node]) {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut last = String::new();
    while Instant::now() < deadline {
        let client = pg_connect(port).await;
        match client.query(&format!("SELECT v FROM {table}"), &[]).await {
            Ok(rows) => {
                let values: Vec<String> = rows.iter().map(|row| row.get("v")).collect();
                if values.iter().any(|v| v == expected) {
                    return;
                }
                last = format!("rows {values:?}");
            }
            Err(e) => last = describe(&e),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!(
        "port {port} never served {expected:?} from {table}; last: {last}{}",
        dump_all_logs(nodes, 60)
    );
}

// ── the crash seam ────────────────────────────────────────────────────────────

/// The one recovery journal under a stopped node's data directory.
fn recovery_journal(data_dir: &Path) -> PathBuf {
    let raft = data_dir.join("raft");
    let journals: Vec<PathBuf> = std::fs::read_dir(&raft)
        .unwrap_or_else(|e| panic!("read {}: {e}", raft.display()))
        .map(|entry| entry.expect("raft dir entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".recovery.json"))
        })
        .collect();
    assert_eq!(
        journals.len(),
        1,
        "expected one recovery journal under {}, found {journals:?}",
        raft.display()
    );
    journals.into_iter().next().unwrap()
}

/// Leave the journal exactly as a process killed inside an apply group does:
/// the session armed, its physical effects unaccounted for.
fn arm_interrupted_session(journal: &Path) {
    let mut record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(journal).expect("read the recovery journal"))
            .expect("the recovery journal is JSON");
    record["phase"] = serde_json::Value::from("SessionArmed");
    record["reason"] = serde_json::Value::from("process stopped inside an apply group");
    std::fs::write(journal, serde_json::to_vec(&record).unwrap())
        .expect("rewrite the recovery journal");
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_stopped_mid_apply_repairs_from_a_peer_and_rejoins() {
    let cluster_name = format!("raft-repair-{}", std::process::id());
    let mut alloc = PortAllocator::new();
    let mut nodes = start_mesh_cluster(&mut alloc, &cluster_name);
    let leader = wait_cluster_ddl_ready(&nodes, Duration::from_secs(240)).await;
    let budget = Duration::from_secs(120);

    let client = pg_connect(nodes[leader].ports.pg).await;
    execute_until_ok(
        &client,
        &format!("CREATE TABLE {TABLE_BEFORE} (id int PRIMARY KEY, v text)"),
        budget,
        &nodes,
    )
    .await;
    execute_until_ok(
        &client,
        &format!("INSERT INTO {TABLE_BEFORE} (id, v) VALUES (1, 'kept')"),
        budget,
        &nodes,
    )
    .await;

    let victim = (leader + 1) % N_NODES;
    wait_for_value(nodes[victim].ports.pg, TABLE_BEFORE, "kept", &nodes).await;
    nodes[victim].stop();
    let journal = recovery_journal(&nodes[victim].data_dir);
    arm_interrupted_session(&journal);

    execute_until_ok(
        &client,
        &format!("CREATE TABLE {TABLE_AFTER} (id int PRIMARY KEY, v text)"),
        budget,
        &nodes,
    )
    .await;
    execute_until_ok(
        &client,
        &format!("INSERT INTO {TABLE_AFTER} (id, v) VALUES (1, 'delivered')"),
        budget,
        &nodes,
    )
    .await;

    let seed = mesh_seed(&nodes, victim);
    nodes[victim].restart(&cluster_name, Some(&seed));
    if let Err(message) = wait_ready_outcome(&mut nodes[victim], Duration::from_secs(180)) {
        panic!(
            "the repaired node never became ready:\n{message}{}",
            dump_all_logs(&nodes, 60)
        );
    }
    let log = std::fs::read_to_string(nodes[victim].log_path()).unwrap_or_default();
    assert!(
        log.contains("Raft repair complete"),
        "the victim became ready without running the repair, so the crash seam did \
         not take:\n{}",
        tail(&log, 60)
    );
    wait_for_value(nodes[victim].ports.pg, TABLE_AFTER, "delivered", &nodes).await;
    wait_for_value(nodes[victim].ports.pg, TABLE_BEFORE, "kept", &nodes).await;
}
