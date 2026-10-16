//! Invariant **ALL REPLICAS AGREE** for `DROP TABLE` (forge t_c8625592) —
//! the REAL multi-node PROCESS test.
//!
//! The in-process cluster test (`ferrosa-cluster/tests/drop_table_replica_agreement.rs`)
//! drives three real Raft voters with real engines but elides the process
//! boundary. This test closes that gap: it spawns **three real `ferrosa` node
//! processes**, forms a real cluster, and drives `DROP TABLE` over the
//! **PostgreSQL wire** (there is no `cqlsh` on this host). One node's table
//! removal actually fails — its `sstables/public.drop_proc_tbl` directory is
//! made unremovable with the same order-independent POSIX mode-000 seam the
//! storage tests use — and the assertions are at the *process* level:
//!
//! 1. the client receives an **ERROR** for the refused DROP, not success;
//! 2. the applying node **does not serve** the dropped rows afterwards;
//! 3. the rows do **not come back after restarting that node**.
//!
//! RED before the fix: `ddl_path::execute_via_raft` discarded `RaftResponse::data`
//! and returned `Ok` for a DROP the applying node had refused, so the client was
//! told the DROP succeeded (assertion 1 fails). See forge t_c8625592,
//! `ferrosa-cluster` FMEA CL-47, `ferrosa-storage` FMEA ST-92.
//!
//! The topology mirrors `tests/docker-compose.cluster.yml`: node 0 is the seed,
//! nodes 1 and 2 seed off node 0. Every node binds its OWN band of eight ports
//! (CQL/web/internode/graph/bolt/postgres/sparql/flight) — a shared port is a
//! spurious `Address already in use` that looks like a code bug (see the
//! `ferrosa-node-isolation` notes). Auth is ENABLED so the PG front end accepts a
//! login at all (the PG listener is gated on `FERROSA_AUTH_ENABLED`, not the
//! deprecated `FERROSA_AUTH_DISABLED`); development mode seeds the well-known
//! `ferrosa_admin` superuser.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

mod common;
use common::ports::{log_shows_port_collision, PortAllocator};

/// The dropped table. The PG front end's default schema is `public`.
const TABLE: &str = "drop_proc_tbl";
/// A value ONLY the pre-DROP rows carry. Its reappearance anywhere is the
/// resurrection this test exists to catch.
const OLD_VALUE: &str = "OLD_SURVIVOR_ROW";
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
    fn table_dir(&self) -> PathBuf {
        self.data_dir
            .join("sstables")
            .join(format!("public.{TABLE}"))
    }

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

/// What one node's startup needs, as a struct rather than a positional list: a
/// swapped `host_id`/`seed` at a call site is invisible in review.
struct StartupSpec<'a> {
    host_id: &'a str,
    cluster_name: &'a str,
    seed: Option<&'a str>,
    extra_env: &'a [(&'a str, &'a str)],
    /// The readiness budget for ONE attempt.
    ready_timeout: Duration,
}

/// Bring ONE node up, re-drawing its whole port band and restarting it if a
/// listener lost its port to a concurrent process.
///
/// [`alloc_ports`] hands out a port that is free at that instant. The band sits
/// below the kernel's ephemeral floor, so the kernel cannot hand it to an
/// outbound connection, but nothing stops ANOTHER test process binding inside
/// the band: `common::ports` leases each process a disjoint slice by file lock,
/// and a process that cannot take a lease falls back to a slice keyed on its own
/// process id. A node that loses a port never reports ready, and reporting that
/// as a red would be a false failure about the product — so it is re-drawn.
///
/// The `allocate` seam lets a test hand the first attempt a band that is already
/// taken, so the retry is PROVEN rather than asserted (see
/// `a_port_taken_by_a_concurrent_process_is_redrawn_and_retried`).
fn spawn_node_redrawing(
    alloc: &mut PortAllocator,
    spec: StartupSpec<'_>,
    mut allocate: impl FnMut(&mut PortAllocator) -> Ports,
) -> Node {
    let mut last_failure = String::new();
    for attempt in 1..=STARTUP_ATTEMPTS {
        let dir = tempfile::tempdir().expect("node data dir");
        let mut node = spawn_node_with_env(
            dir,
            allocate(alloc),
            spec.host_id.to_string(),
            spec.cluster_name,
            spec.seed,
            spec.extra_env,
        );
        match wait_ready_watching(&mut node, spec.ready_timeout, true) {
            Ok(()) => return node,
            Err(message) => {
                if !log_shows_port_collision(&message) {
                    panic!(
                        "node {} did not become ready (attempt {attempt}/{STARTUP_ATTEMPTS}):\n{message}",
                        spec.host_id
                    );
                }
                eprintln!(
                    "node {} lost a listener port to a concurrent process; re-drawing its band \
                     and restarting (attempt {attempt}/{STARTUP_ATTEMPTS})",
                    spec.host_id
                );
                last_failure = message;
            }
        }
    }
    panic!(
        "node {} never became ready: all {STARTUP_ATTEMPTS} attempts lost a listener port to a \
         concurrent process. Last failure:\n{last_failure}",
        spec.host_id
    );
}

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

/// Wait until the node reports `{"ready":true}` on `/readyz` (leader elected and
/// every listener up).
///
/// Two failure modes, and each is named with what the node reported:
///
/// * the process has EXITED — it can never report ready, and polling it costs
///   the whole budget (the last line of that node's log was 74 s before the
///   panic in the merge-queue failure of PR #551, job 115370033050). The child is
///   checked on every poll and its exit fails the wait at once.
/// * the node is ALIVE but never ready — its `/readyz` body says what it is
///   waiting for, and the failure quotes that condition rather than leaving the
///   reader with a tail. An alive node reporting `waiting_for` used to look
///   exactly like a dead one (t_4e9a3e61).
///
/// The exit check runs BOTH sides of the probe. A probe is bounded by
/// [`PROBE_CONNECT_TIMEOUT`] and [`PROBE_READ_TIMEOUT`], so an exit during one
/// can delay the check by at most one probe — never by a whole poll tick, which
/// is what the former 3 s read cost (5.6-8.7 s to notice a dead child).
fn wait_ready(node: &mut Node, timeout: Duration) {
    if let Err(message) = wait_ready_outcome(node, timeout) {
        panic!("{message}");
    }
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
        let _ = connection.await;
    });
    client
}

/// `SELECT id, v FROM TABLE`. A relation-not-exist error is returned as `Err`
/// (also "not serving the dropped rows").
async fn read_rows(client: &Client) -> Result<Vec<(i32, String)>, String> {
    let rows = client
        .query(&format!("SELECT id, v FROM {TABLE}"), &[])
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, i32>("id"), row.get::<_, String>("v")))
        .collect())
}

fn serves_old(rows: &[(i32, String)]) -> bool {
    rows.iter().any(|(_, v)| v == OLD_VALUE)
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

// ── filesystem seam ───────────────────────────────────────────────────────────

/// Restores the directory mode so the tempdir can be cleaned up.
struct RestoreDirMode(PathBuf);

impl Drop for RestoreDirMode {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(&self.0) {
            let mut p = md.permissions();
            p.set_mode(0o755);
            let _ = std::fs::set_permissions(&self.0, p);
        }
    }
}

/// Make the table's SSTable directory **unremovable** so the node's
/// `remove_dir_all` cannot complete — the order-independent form of "the DROP's
/// removal failed". Guarantees the directory exists and is non-empty first.
fn sabotage_table_dir(table_dir: &Path) -> RestoreDirMode {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(table_dir).expect("create table dir");
    // A sentinel makes the directory non-empty even if the engine has not flushed
    // any SSTable yet, so the refused drop has something it cannot remove.
    std::fs::write(table_dir.join("drop-sabotage.sentinel"), b"unremovable")
        .expect("write sentinel");
    let mut perms = std::fs::metadata(table_dir).unwrap().permissions();
    perms.set_mode(0o000);
    std::fs::set_permissions(table_dir, perms).expect("chmod 000 table dir");
    RestoreDirMode(table_dir.to_path_buf())
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_drop_on_a_real_node_process_fails_loud_and_never_resurrects() {
    let cluster_name = format!("drop-proc-{}", std::process::id());
    let mut alloc = PortAllocator::new();

    let mut nodes = start_mesh_cluster(&mut alloc, &cluster_name);

    // Every node must be fully up; a half-bound cluster invalidates everything.
    for node in &nodes {
        let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
        assert!(
            !log.contains("Address already in use"),
            "node {} hit a port collision — the cluster is not healthy:\n{}",
            node.host_id,
            tail(&log, 30)
        );
    }

    // The DDL must route through the Raft-replicated path: wait (generously, and
    // per node) until a Raft leader exists and every node has installed
    // `DdlPath::Cluster`. On timeout the helper dumps every node's log.
    let leader = wait_cluster_ddl_ready(&nodes, Duration::from_secs(240)).await;

    // Create the table through the SAME replicated DDL path CQL uses, via PG.
    // Retry while the default `public` keyspace is being created at boot.
    let client = pg_connect(nodes[leader].ports.pg).await;
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client
                .batch_execute(&format!(
                    "CREATE TABLE {TABLE} (id int PRIMARY KEY, v text)"
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

    // Seed distinctive rows — one statement each, since a multi-row INSERT is
    // refused fail-loud on a cluster. The first insert doubles as the wait for
    // full Accord membership: right after bootstrap the peer set can still be
    // incomplete, so a write is briefly refused with "Accord quorum unavailable".
    for id in 1..=3 {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match client
                .execute(
                    &format!("INSERT INTO {TABLE} (id, v) VALUES ({id}, '{OLD_VALUE}')"),
                    &[],
                )
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "INSERT row {id} never succeeded: {}{}",
                        describe(&e),
                        dump_all_logs(&nodes, 40)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // Let the fast flush interval land the rows in SSTables, then confirm the
    // rows are readable and the applied table's directory is on disk.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let pre = read_rows(&client)
        .await
        .unwrap_or_else(|e| panic!("SELECT of seeded rows failed: {e}"));
    assert!(
        serves_old(&pre),
        "precondition: the seeded rows must be readable before the DROP, got {pre:?}"
    );
    assert!(
        nodes[leader].table_dir().exists(),
        "precondition: the applied table's directory must exist on disk before the DROP"
    );

    // Sabotage EVERY node's table directory, so whichever node applies the DROP
    // (and every follower that replays it) cannot remove this table's SSTables.
    let guards: Vec<RestoreDirMode> = nodes
        .iter()
        .map(|n| sabotage_table_dir(&n.table_dir()))
        .collect();

    // (1) The refused DROP must FAIL LOUD to the client, not report success.
    let drop_result = client.batch_execute(&format!("DROP TABLE {TABLE}")).await;
    assert!(
        drop_result.is_err(),
        "a DROP the applying node refused must surface an ERROR to the client, \
         but the PostgreSQL DROP TABLE reported success"
    );
    eprintln!(
        "refused DROP surfaced to the client as: {}",
        describe(drop_result.as_ref().unwrap_err())
    );

    // The removal never completed: the directory (with its sentinel) is still on
    // disk, which is what makes the next assertions meaningful.
    assert!(
        nodes[leader].table_dir().exists(),
        "the refused removal must have left the table directory on disk"
    );

    // (2) No node may serve the dropped rows afterwards.
    for node in &nodes {
        let c = pg_connect(node.ports.pg).await;
        match read_rows(&c).await {
            Ok(rows) => assert!(
                !serves_old(&rows),
                "node {} still serves the dropped table's rows: {rows:?}",
                node.host_id
            ),
            Err(_) => { /* relation does not exist — also not serving the rows */ }
        }
    }

    // Restore the directory modes so the node can start and sweep on restart.
    drop(guards);

    // (3) Restart the node that APPLIED the drop; the rows must not come back.
    let seed = mesh_seed(&nodes, leader);
    let restarted = &mut nodes[leader];
    restarted.restart(&cluster_name, Some(&seed));
    wait_ready(restarted, Duration::from_secs(90));

    let c = pg_connect(restarted.ports.pg).await;
    match read_rows(&c).await {
        Ok(rows) => assert!(
            !serves_old(&rows),
            "after restarting the applying node, the dropped rows came back: {rows:?}"
        ),
        Err(_) => { /* relation does not exist — the durable drop held */ }
    }
}

// ── a node that dies on its way up ─────────────────────────────────────────────

/// A readiness failure must quote the node's FIRST fatal/ERROR line, not only a
/// tail that can start after the cause. In the merge-queue failure of PR #551 the
/// dumped tail began at the node's shutdown and the `PairSchemaSync REFUSED`
/// storm that named the trouble sat above it (t_4e9a3e61).
#[test]
fn a_readiness_failure_leads_with_the_first_fatal_line_not_only_the_tail() {
    // 60 lines, with the ERROR second: the 40-line tail cannot contain it.
    let mut log = String::from(
        "2026-10-13T15:13:00.000000Z  INFO ferrosa: ferrosa starting\n\
         2026-10-13T15:13:01.000000Z ERROR ferrosa_cluster::pair::ddl: PairSchemaSync REFUSED: \
         this node is the pair primary and its schema is authoritative peer=33333333\n",
    );
    for n in 0..58 {
        log.push_str(&format!(
            "2026-10-13T15:13:02.000000Z  INFO ferrosa_net::lane_actor: filler {n}\n"
        ));
    }
    log.push_str(
        "2026-10-13T15:13:16.000000Z  INFO ferrosa_net::lane_actor: lane actor: shutting down lane=Data\n",
    );

    let tail_lines: Vec<&str> = log.lines().rev().take(LOG_TAIL_LINES).collect();
    assert_eq!(
        log.lines()
            .filter(|l| l.contains("PairSchemaSync REFUSED"))
            .count(),
        1,
        "the fixture holds exactly one fatal line"
    );
    assert!(
        !tail_lines
            .iter()
            .any(|l| l.contains("PairSchemaSync REFUSED")),
        "the fixture must put the ERROR above the tail, or this test proves nothing"
    );

    let message = readiness_failure_message("headline", "waiting_for=\"raft_leader\"", &log);
    assert!(
        message.contains(
            "first fatal/ERROR line: 2026-10-13T15:13:01.000000Z ERROR \
             ferrosa_cluster::pair::ddl: PairSchemaSync REFUSED"
        ),
        "the first fatal line is the evidence, not the tail:\n{message}"
    );
    assert!(
        message.contains("lane actor: shutting down lane=Data"),
        "the tail is still printed beside it:\n{message}"
    );

    let silent = readiness_failure_message("headline", "HTTP 503 from /readyz", "INFO a\nINFO b\n");
    assert!(
        silent.contains(NO_FATAL_LINE),
        "a log with nothing fatal in it must say so:\n{silent}"
    );
}

/// A node exit is the one readiness failure the harness can settle at once: it
/// must name the exit status, when it happened, and the node's first fatal line.
#[test]
fn a_node_exit_is_named_with_its_status_and_its_first_fatal_line() {
    use std::os::unix::process::ExitStatusExt;
    // The shape `fatal_exit(70, ..)` after an injected worker panic leaves.
    let status = ExitStatus::from_raw(70 << 8);
    let log = "2026-10-13T17:10:06.639956Z FATAL: thread 'fault-injected' panicked at \
               ferrosa/src/main.rs:1863:27: injected fault: worker thread panic\n\
               2026-10-13T17:10:06.671163Z ferrosa exiting code=70 reason=injected fault\n";

    let message = node_exit_message(
        "11111111-1111-1111-1111-111111111111",
        33385,
        &status,
        Duration::from_millis(700),
        log,
    );
    assert!(
        message.contains(
            "node 11111111-1111-1111-1111-111111111111 exited 700ms into the \
                          readiness wait: exit status: 70"
        ),
        "the headline names the exit status and when it happened:\n{message}"
    );
    assert!(
        message.contains("FATAL: thread 'fault-injected' panicked at"),
        "and quotes the node's own first fatal line:\n{message}"
    );

    let timed_out = readiness_timeout_message("h", 1, Duration::from_secs(90), None, "INFO a\n");
    assert!(
        timed_out.contains("did not become ready within 90s"),
        "a timeout still says what it waited for:\\n{timed_out}"
    );
    assert!(
        timed_out.contains("/readyz on web=1 never answered"),
        "a node that never answered says so, instead of inventing a condition:\\n{timed_out}"
    );

    // The condition the node reported is what a probe-shaped failure must name.
    let probe = parse_probe(
        "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 41\r\n\r\n\
         {\"ready\":false,\"waiting_for\":\"raft_leader\"}",
    )
    .expect("an HTTP response parses");
    assert_eq!(probe.status, 503);
    let waiting =
        readiness_timeout_message("h", 1, Duration::from_secs(90), Some(&probe), "INFO a\n");
    assert!(
        waiting.contains("HTTP 503 from /readyz: waiting_for=\"raft_leader\""),
        "the timeout must quote the /readyz condition the node reported:\\n{waiting}"
    );
}

/// The OTHER readiness failure the harness must name: the node is ALIVE and
/// reporting a condition it never satisfies.
///
/// The wait this harness replaced threw the `/readyz` body away, so a node
/// patiently reporting `waiting_for` was indistinguishable from a dead one and
/// CI could never show which condition was being waited on (t_4e9a3e61). The
/// requirement is not "fail when the process is gone" but "fail naming THE
/// CONDITION the node reports".
///
/// The node here can never be ready: the PostgreSQL port it must bind is already
/// held, so its `postgres` listener records a bind failure and `/readyz` answers
/// `503 {"ready":false,"waiting_for":"listeners",…}` for the node's whole life.
/// A failed listener is checked before every other readiness condition
/// (`ferrosa/src/web/readiness.rs`), and a failed background listener is not
/// fatal — the node keeps running, which is the case this test exists for.
#[test]
fn a_node_alive_but_never_ready_fails_the_wait_naming_its_condition() {
    let mut alloc = PortAllocator::new();
    let ports = alloc_ports(&mut alloc);
    let dir = tempfile::tempdir().expect("node data dir");
    // Held for the whole test: the node can never bind it.
    let _held_pg = std::net::TcpListener::bind(("127.0.0.1", ports.pg))
        .expect("occupy the node's PostgreSQL port");
    let host_id = "11111111-1111-1111-1111-111111111111".to_string();
    let mut node = spawn_node_with_env(dir, ports, host_id, "alive-never-ready", None, &[]);

    // Bounded poll for the node to be UP and reporting a not-ready condition.
    // Waiting for the condition before calling `wait_ready` is what makes this
    // deterministic: a failed listener, once recorded, gates every later answer.
    let observe_deadline = Instant::now() + Duration::from_secs(45);
    let mut observed: Option<String> = None;
    while Instant::now() < observe_deadline {
        if let Some(status) = node.child.try_wait().expect("try_wait on the node") {
            panic!("the never-ready fixture node exited before reporting a condition: {status}");
        }
        if let Ok(raw) = http_get(node.ports.web, "/readyz", None, PROBE_READ_TIMEOUT) {
            if !raw.contains("\"ready\":true") {
                if let Some(condition) =
                    parse_probe(&raw).and_then(|p| json_str(&p.body, "waiting_for"))
                {
                    observed = Some(condition);
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let observed = observed.expect(
        "the node must report a /readyz waiting condition within 45s \
         (is the PostgreSQL listener still started when its port is held?)",
    );

    let budget = Duration::from_secs(10);
    let started = Instant::now();
    let failure = catch_unwind(AssertUnwindSafe(|| {
        wait_ready(&mut node, budget);
    }))
    .expect_err("a node that is alive but never ready must not pass the readiness wait");
    let elapsed = started.elapsed();
    let message = panic_text(failure);
    eprintln!("readiness wait failed after {elapsed:?} naming condition {observed:?}:\n{message}");

    assert!(
        node.child
            .try_wait()
            .expect("try_wait on the node")
            .is_none(),
        "the node must still be ALIVE when the wait fails — this is the \
         alive-but-never-ready case, not the dead-process one:\n{message}"
    );
    assert!(
        elapsed >= budget,
        "the wait must spend its whole budget on a node that is up but never ready \
         (it took {elapsed:?}):\n{message}"
    );
    assert!(
        message.contains(&observed),
        "the failure must NAME the condition the node reported ({observed:?}):\n{message}"
    );
    assert!(
        message.contains("waiting_for"),
        "the failure must quote the /readyz condition, not only the process state:\n{message}"
    );
    assert!(
        !message.contains("exited"),
        "an alive node did not exit; the failure must not claim it did:\n{message}"
    );
}

/// The panic message as text, so a test can assert on what the harness SAID and
/// not merely that it panicked.
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => message.to_string(),
            Err(_) => "<non-string panic payload>".to_string(),
        },
    }
}

/// The harness must fail the moment the node process is GONE, naming the exit
/// status and the node's own first fatal line — not poll a corpse for the whole
/// budget and then print a tail that starts after the cause.
///
/// PR #551 was ejected from the merge queue by this test burning its full 90 s
/// readiness budget on a process that had already exited: the last line of the
/// log it dumped was 74 s before the panic (job 115370033050, t_4e9a3e61).
///
/// The node here fails at startup on purpose, through the binary's hidden
/// fault-injection seam: a worker thread panics, `lifecycle` writes the
/// synchronous `FATAL:` line, and the process exits 70.
#[test]
fn a_node_that_dies_at_startup_fails_the_readiness_wait_at_once() {
    let mut alloc = PortAllocator::new();
    let ports = alloc_ports(&mut alloc);
    let dir = tempfile::tempdir().expect("node data dir");
    let host_id = "11111111-1111-1111-1111-111111111111".to_string();
    let mut node = spawn_node_with_env(
        dir,
        ports,
        host_id,
        "drop-dead-startup",
        None,
        // RUST_BACKTRACE=0 for THIS child on purpose. The workflow sets
        // RUST_BACKTRACE=1 globally, and symbolising a backtrace out of a debug
        // binary is expensive: measured on one host, exec -> injected panic ->
        // exit is ~32 ms without a backtrace and ~170 ms with one (5.3x), which
        // on a loaded CI runner becomes seconds. Nothing here asserts on the std
        // backtrace — the assertions quote the synchronous `FATAL:` line
        // `lifecycle` writes — so this removes an unrelated cost from a
        // measurement of DETECTION, not of symbolisation.
        &[
            ("FERROSA_TEST_FAULT_INJECT", "panic-thread"),
            ("RUST_BACKTRACE", "0"),
        ],
    );

    let started = Instant::now();
    let failure = catch_unwind(AssertUnwindSafe(|| {
        wait_ready(&mut node, Duration::from_secs(90));
    }))
    .expect_err("a node process that has exited must not pass the readiness wait");
    let elapsed = started.elapsed();
    let message = panic_text(failure);
    eprintln!("readiness wait failed after {elapsed:?}:\n{message}");

    // The property is DETECTION, and the budget is 90 s: this bound only has to
    // be far below it, because the regression it guards is a wait that polls a
    // corpse until the budget runs out (the merge-queue failure of PR #551 spent
    // 74 s). It must NOT be tightened to the observed number: `elapsed` also
    // contains the CHILD's own exec-and-die cost, which is milliseconds on a warm
    // host and SECONDS on a loaded CI runner. A 5 s bound here failed a run at
    // 5.325 s where the child itself took 4.119 s to die and was detected 0.6 s
    // later — i.e. the detection was fine and the bound was measuring the runner.
    // 30 s keeps the guard (a missing child check burns the whole 90 s, and the
    // message assertions below prove the failure came from the child-gone path,
    // not from the deadline) while absorbing runner variance.
    assert!(
        elapsed < Duration::from_secs(30),
        "the wait must fail as soon as the process is gone, not spend its budget \
         (it took {elapsed:?}):\n{message}"
    );
    assert!(
        message.contains("exited") && message.contains("exit status"),
        "the failure must name the exit status:\n{message}"
    );
    assert!(
        message.contains("70"),
        "the exit status must be the node's (70 for the injected panic):\n{message}"
    );
    assert!(
        message.contains("FATAL:") && message.contains("injected fault: worker thread panic"),
        "the failure must quote the node's first fatal line:\n{message}"
    );
    assert!(
        !message.contains("did not become ready within"),
        "a node that exited did not merely time out:\n{message}"
    );
}

/// The re-draw is PROVEN, not asserted: hand the first attempt a band whose
/// PostgreSQL port a concurrent process is holding, and the node must still come
/// up — on a different band.
///
/// A held listener is exactly what another test process binding inside the band
/// looks like from here, and the node's own log says so: the failed bind is
/// recorded, the node reports a not-ready condition rather than ready, and the
/// harness must re-draw instead of reporting a product failure.
#[test]
fn a_port_taken_by_a_concurrent_process_is_redrawn_and_retried() {
    let mut alloc = PortAllocator::new();
    let taken = alloc_ports(&mut alloc);
    // Held for the whole test: the FIRST attempt's node can never bind it.
    let _held = std::net::TcpListener::bind(("127.0.0.1", taken.pg))
        .expect("occupy a port in the first attempt's band");

    let started = Instant::now();
    let mut allocations = 0usize;
    let node = spawn_node_redrawing(
        &mut alloc,
        StartupSpec {
            host_id: "77777777-7777-7777-7777-777777777777",
            cluster_name: &format!("redraw-{}", std::process::id()),
            seed: None,
            extra_env: &[],
            // The standard budget: with the log watch, a lost port is detected in
            // about a second, so this covers a slow-but-healthy boot on a loaded
            // runner rather than being burned on the node that cannot bind.
            ready_timeout: Duration::from_secs(90),
        },
        |alloc| {
            allocations += 1;
            if allocations == 1 {
                taken
            } else {
                alloc_ports(alloc)
            }
        },
    );

    let elapsed = started.elapsed();
    assert!(
        allocations > 1,
        "the first attempt must have been handed the taken band, or this test proves nothing"
    );
    assert!(
        elapsed < Duration::from_secs(60),
        "the lost port must be detected as soon as the node logs it, not by burning the \
         budget: the first attempt took {elapsed:?}"
    );
    assert_ne!(
        node.ports.pg, taken.pg,
        "the retry must be on a DIFFERENT band, not the port it already lost"
    );
    let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
    assert!(
        !log.contains(common::ports::PORT_IN_USE),
        "the node that came up must not itself have lost a port:\n{}",
        tail(&log, 30)
    );
}
