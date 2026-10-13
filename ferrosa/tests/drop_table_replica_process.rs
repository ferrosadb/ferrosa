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

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

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

/// Every port this test binary has handed out, process-wide.
///
/// `#[test]` functions run on separate threads of one process, each with its
/// own `taken` set, so a per-test set alone lets two tests draw the SAME port:
/// [`free_port`] binds `:0`, reads the port and drops the listener, and the
/// port is then free for the other test to draw too. The global set closes that
/// window. The sibling never-ready fixture is where it showed: its node's
/// PostgreSQL listener reported `Address already in use` under load, because a
/// port another test had been handed was still bound (t_4e9a3e61).
fn reserved_ports() -> &'static std::sync::Mutex<HashSet<u16>> {
    static RESERVED: std::sync::OnceLock<std::sync::Mutex<HashSet<u16>>> =
        std::sync::OnceLock::new();
    RESERVED.get_or_init(|| std::sync::Mutex::new(HashSet::new()))
}

/// Whether a connection to `port` is REFUSED — i.e. nothing is listening on it
/// right now.
///
/// The bind-then-drop in [`free_port`] proves the port was free a moment ago;
/// this proves nothing holds it NOW. A stale listener (a container runtime's
/// proxy, a peer test's node) answers the connect, and this node's own bind
/// would then fail `Address already in use` — a harness accident reported as a
/// product red.
fn port_is_refused(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_err()
}

/// A port no other test in this process holds and nothing is listening on.
///
/// Retries on every collision rather than taking the first ephemeral port the
/// kernel offers: a port can be spoken for by this process, or already bound by
/// something outside it.
fn free_port(taken: &mut HashSet<u16>) -> u16 {
    for _ in 0..500 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        // Genuinely free: refused now (nothing outside holds it) and not
        // claimed by another test or an earlier allocation in this one.
        if !port_is_refused(port) {
            continue;
        }
        if !taken.insert(port) {
            continue;
        }
        if !reserved_ports().lock().unwrap().insert(port) {
            continue;
        }
        return port;
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

// ── readiness ─────────────────────────────────────────────────────────────────

/// The connect timeout for a readiness probe.
///
/// A loopback listener that is UP accepts at once, so this only bounds the "not
/// listening yet" case. Short on purpose — see [`PROBE_READ_TIMEOUT`].
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// The read timeout for a readiness probe.
///
/// A live `/readyz` answers in single-digit milliseconds even with the node
/// under contention, so this is generous for a real answer and still short
/// enough that an exit DURING a stalled probe costs well under the 5 s detector
/// budget of `a_node_that_dies_at_startup_fails_the_readiness_wait_at_once`. The
/// former 3 s read let a listener that had accepted but was shutting down hold
/// each poll for the full 3 s — one to two such rounds are what stretched that
/// test to 5.6-8.7 s (t_4e9a3e61).
const PROBE_READ_TIMEOUT: Duration = Duration::from_millis(500);

/// A minimal HTTP/1.1 GET. Returns the raw response text, or `None` if the
/// listener is not up yet (or does not answer inside the probe timeouts).
fn http_get(port: u16, path: &str, authorization: Option<&str>) -> Option<String> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, PROBE_CONNECT_TIMEOUT).ok()?;
    stream.set_read_timeout(Some(PROBE_READ_TIMEOUT)).ok()?;
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
    let started = Instant::now();
    let deadline = started + timeout;
    let mut last_probe: Option<Probe> = None;
    loop {
        if let Some(message) = node_gone_message(node, started) {
            panic!("{message}");
        }
        let raw = http_get(node.ports.web, "/readyz", None);
        // An exit DURING the probe must not wait for the next tick. The probe
        // is bounded (a live /readyz answers in milliseconds; a listener that
        // accepted and then stalled is cut off by the read timeout), so this
        // keeps the check within about one probe of the exit.
        if let Some(message) = node_gone_message(node, started) {
            panic!("{message}");
        }
        if let Some(raw) = raw {
            if raw.contains("\"ready\":true") {
                return;
            }
            if let Some(probe) = parse_probe(&raw) {
                last_probe = Some(probe);
            }
        }
        if Instant::now() >= deadline {
            let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
            panic!(
                "{}",
                readiness_timeout_message(
                    &node.host_id,
                    node.ports.web,
                    timeout,
                    last_probe.as_ref(),
                    &log
                )
            );
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
/// replicated `DdlPath::Cluster`; return the leader's index.
///
/// Both conditions are polled per node on a generous budget (formation is not a
/// timed production limit — a flat 60 s that is too short only ever costs a
/// round). On timeout it dumps **every** node's log tail: a harness that fails
/// without showing why is worth nothing.
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
    let mut taken: HashSet<u16> = HashSet::new();

    // Allocate every node's ports up front, so each node can be given the others
    // as seeds. Every node seeds off the other two (a full mesh): that makes the
    // auto-created `public` keyspace RF=3 on every node — its replication factor
    // is `seeds + 1` — so an Accord write has two remote replicas to reach a
    // quorum from. With the docker-compose star (node 0 has no seed) the keyspace
    // is RF=1 and an Accord write that resolves to the coordinator's own host
    // fails "unknown peer: <own host_id>" and never commits.
    let all_ports: Vec<Ports> = (0..N_NODES).map(|_| alloc_ports(&mut taken)).collect();
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
        let node = spawn_node(dir, *ports, host_id, &cluster_name, Some(&seeds_for(i)));
        nodes.push(node);
    }
    for node in nodes.iter_mut() {
        wait_ready(node, Duration::from_secs(90));
    }

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
    let seed = seeds_for(leader);
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
    let mut taken: HashSet<u16> = HashSet::new();
    let ports = alloc_ports(&mut taken);
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
        if let Some(raw) = http_get(node.ports.web, "/readyz", None) {
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
    let mut taken: HashSet<u16> = HashSet::new();
    let ports = alloc_ports(&mut taken);
    let dir = tempfile::tempdir().expect("node data dir");
    let host_id = "11111111-1111-1111-1111-111111111111".to_string();
    let mut node = spawn_node_with_env(
        dir,
        ports,
        host_id,
        "drop-dead-startup",
        None,
        &[("FERROSA_TEST_FAULT_INJECT", "panic-thread")],
    );

    let started = Instant::now();
    let failure = catch_unwind(AssertUnwindSafe(|| {
        wait_ready(&mut node, Duration::from_secs(90));
    }))
    .expect_err("a node process that has exited must not pass the readiness wait");
    let elapsed = started.elapsed();
    let message = panic_text(failure);
    eprintln!("readiness wait failed after {elapsed:?}:\n{message}");

    assert!(
        elapsed < Duration::from_secs(5),
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
