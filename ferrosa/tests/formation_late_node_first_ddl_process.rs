//! The first DDL after a fresh 3-node formation must succeed — and be visible on
//! the node that ran it — even when one node starts late. REAL multi-node PROCESS
//! test (forge t_0cfbb511).
//!
//! # The defect this pins
//!
//! A coordinator e2e started three nodes as a STAR (node 0 carries no seed, nodes
//! 1 and 2 seed off it) and one member, a microVM, came up about a second after
//! the others. The client waited until node 0 answered CQL and `system.peers`
//! listed both peers, then sent `CREATE KEYSPACE`. Four runs in five it got
//! `server error: DDL unavailable: peer lost`, and every statement after it found
//! no keyspace.
//!
//! Nothing was lost. `system.peers` lists a peer as soon as it is connected, but
//! a node only publishes the Raft-backed DDL path once a leader is elected.
//! Between the two the node sits in formation (`DdlPath::Forming`), and the CQL
//! router answered every DDL in that window with the degraded-pair "peer lost"
//! message: a false cause, returned at once, never retried and never applied. A
//! late member stretches that window from about half a second to two or three
//! seconds, which is why the late start made the failure likely rather than rare.
//!
//! # The test
//!
//! For each delay in a sweep, start node 0 and node 2 together and node 1 that
//! much later. Poll node 0 tightly until it answers CQL and lists two peers —
//! the e2e's own readiness signal — and send the DDL the instant it does. The
//! assertions:
//!
//! 1. `CREATE KEYSPACE` and `CREATE TABLE` on node 0 succeed;
//! 2. node 0 can read the new table immediately (read-your-writes);
//! 3. every other node can read it within a bound.
//!
//! The sweep's per-delay outcome is printed as a table either way, so a red run is
//! also the failure-rate measurement. `FERROSA_FORMATION_SWEEP_RUNS` raises the
//! runs per delay (default 1); `FERROSA_FORMATION_SWEEP_DELAYS_MS` replaces the
//! delay list (comma-separated milliseconds). `FERROSA_FORMATION_KEEP_LOGS=<dir>`
//! keeps every failing run's full node logs, and the sweep summary, under that
//! directory.
//!
//! Gating (repo test policy): like the neighbouring `*_process` harnesses it
//! spawns its own nodes and needs no external infrastructure, so it runs in the
//! normal suite and is not `#[ignore]`d. Run just it with:
//!   cargo test -p ferrosa --test formation_late_node_first_ddl_process -- --nocapture

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::ports::PortAllocator;

/// Delays (ms) by which node 1 starts after nodes 0 and 2.
const DEFAULT_DELAYS_MS: &[u64] = &[0, 500, 1000, 1500, 2000, 3000];

/// How long a node may take to bind its CQL listener.
const CQL_LISTEN_BUDGET: Duration = Duration::from_secs(120);

/// How long node 0 may take to list both peers after the last node starts.
const PEERS_BUDGET: Duration = Duration::from_secs(120);

/// How long a non-coordinator may take to apply the replicated schema.
const PROPAGATION_BUDGET: Duration = Duration::from_secs(20);

/// Client read timeout for one CQL frame. Above the server's own DDL bounds, so a
/// slow DDL is reported by the server rather than cut off here.
const FRAME_TIMEOUT: Duration = Duration::from_secs(40);

// ── ports and processes ───────────────────────────────────────────────────────

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

struct Node {
    _dir: tempfile::TempDir,
    data_dir: PathBuf,
    ports: Ports,
    child: Child,
}

impl Node {
    fn log(&self) -> String {
        std::fs::read_to_string(self.data_dir.join("node.log")).unwrap_or_default()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Best-effort teardown of a child this test spawned; a kill of an
        // already-exited child fails and there is nothing left to stop.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Host id of node `index` in run `run`.
///
/// The highest host id seeds the Raft group and usually leads it. Even runs give
/// that to node 2; odd runs give it to node 1, the LATE node, as in the e2e run
/// where the microVM member won the election. Distinct in both halves: Raft
/// reads bytes[8..16], Accord bytes[0..8].
fn host_id(index: usize, run: usize) -> String {
    let rank = if run % 2 == 1 {
        [0u8, 2, 1][index]
    } else {
        index as u8
    };
    let d = (0x11u8 * (rank + 1)).wrapping_add((run % 16) as u8);
    format!(
        "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
         {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
    )
}

fn spawn_node(ports: Ports, host_id: &str, cluster: &str, seed: Option<u16>) -> Node {
    let dir = tempfile::tempdir().expect("node data dir");
    let log = std::fs::File::create(dir.path().join("node.log")).expect("create node log");
    let stderr = log.try_clone().expect("clone log handle");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
    cmd.env("RUST_MIN_STACK", "33554432")
        .env("RUST_LOG", "info")
        .env("FERROSA_AUTH_ENABLED", "false")
        .env("FERROSA_DATA_DIR", dir.path())
        .env("FERROSA_CLUSTER_NAME", cluster)
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
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    if let Some(seed) = seed {
        cmd.env("FERROSA_SEED", format!("127.0.0.1:{seed}"));
    }
    let child = cmd.spawn().expect("spawn ferrosa node");
    Node {
        data_dir: dir.path().to_path_buf(),
        _dir: dir,
        ports,
        child,
    }
}

// ── a minimal CQL v4 client ───────────────────────────────────────────────────

const OP_ERROR: u8 = 0x00;
const OP_STARTUP: u8 = 0x01;
const OP_READY: u8 = 0x02;
const OP_QUERY: u8 = 0x07;
const OP_RESULT: u8 = 0x08;

fn frame(opcode: u8, body: &[u8]) -> Vec<u8> {
    let mut buf = vec![0x04, 0x00, 0x00, 0x00, opcode];
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(body);
    buf
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 9];
    stream.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header[5..9].try_into().expect("4 bytes")) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    Ok((header[4], body))
}

fn cql_connect(port: u16) -> std::io::Result<TcpStream> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(250))?;
    stream.set_read_timeout(Some(FRAME_TIMEOUT))?;
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    for s in ["CQL_VERSION", "3.0.0"] {
        body.extend_from_slice(&(s.len() as u16).to_be_bytes());
        body.extend_from_slice(s.as_bytes());
    }
    stream.write_all(&frame(OP_STARTUP, &body))?;
    let (op, body) = read_frame(&mut stream)?;
    if op != OP_READY {
        return Err(std::io::Error::other(format!(
            "STARTUP answered opcode {op:#x}: {}",
            decode_error(&body)
        )));
    }
    Ok(stream)
}

fn decode_error(body: &[u8]) -> String {
    if body.len() < 6 {
        return format!("malformed error body: {body:?}");
    }
    let code = i32::from_be_bytes(body[0..4].try_into().expect("4 bytes"));
    let len = u16::from_be_bytes(body[4..6].try_into().expect("2 bytes")) as usize;
    let msg = body
        .get(6..6 + len)
        .and_then(|b| std::str::from_utf8(b).ok())
        .unwrap_or("<invalid utf8>");
    format!("0x{code:04x} {msg}")
}

/// Run one statement. `Ok(body)` is a RESULT frame's body; `Err` is the server's
/// error (code and message) or a transport failure, as text.
fn cql_query(stream: &mut TcpStream, query: &str) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    body.extend_from_slice(&(query.len() as i32).to_be_bytes());
    body.extend_from_slice(query.as_bytes());
    body.extend_from_slice(&0x0004u16.to_be_bytes()); // QUORUM
    body.push(0);
    stream
        .write_all(&frame(OP_QUERY, &body))
        .map_err(|e| format!("send failed: {e}"))?;
    match read_frame(stream) {
        Ok((OP_RESULT, body)) => Ok(body),
        Ok((OP_ERROR, body)) => Err(decode_error(&body)),
        Ok((op, _)) => Err(format!("unexpected opcode {op:#x}")),
        Err(e) => Err(format!("no answer within {FRAME_TIMEOUT:?}: {e}")),
    }
}

fn take<'a>(body: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], String> {
    let out = body
        .get(*pos..*pos + n)
        .ok_or_else(|| format!("RESULT body truncated at {pos}"))?;
    *pos += n;
    Ok(out)
}

fn read_i32(body: &[u8], pos: &mut usize) -> Result<i32, String> {
    Ok(i32::from_be_bytes(
        take(body, pos, 4)?.try_into().expect("4 bytes"),
    ))
}

fn skip_string(body: &[u8], pos: &mut usize) -> Result<(), String> {
    let len = u16::from_be_bytes(take(body, pos, 2)?.try_into().expect("2 bytes")) as usize;
    take(body, pos, len).map(|_| ())
}

/// Row count of a RESULT/Rows body (simple column types only).
fn row_count(body: &[u8]) -> Result<usize, String> {
    let mut pos = 0;
    let kind = read_i32(body, &mut pos)?;
    if kind != 0x0002 {
        return Err(format!("expected a Rows result, got kind {kind:#x}"));
    }
    let flags = read_i32(body, &mut pos)?;
    let columns = read_i32(body, &mut pos)? as usize;
    if flags & 0x0002 != 0 {
        let len = read_i32(body, &mut pos)?;
        take(body, &mut pos, len.max(0) as usize)?;
    }
    if flags & 0x0004 == 0 {
        let global = flags & 0x0001 != 0;
        if global {
            skip_string(body, &mut pos)?;
            skip_string(body, &mut pos)?;
        }
        for _ in 0..columns {
            if !global {
                skip_string(body, &mut pos)?;
                skip_string(body, &mut pos)?;
            }
            skip_string(body, &mut pos)?;
            take(body, &mut pos, 2)?;
        }
    }
    Ok(read_i32(body, &mut pos)? as usize)
}

// ── one run ───────────────────────────────────────────────────────────────────

/// What one formation run produced.
enum Outcome {
    /// DDL succeeded and the table was readable everywhere.
    Ok { ready_to_ddl_ms: u128 },
    /// The first DDL was refused.
    DdlFailed(String),
    /// DDL reported success but node 0 could not read the table at once.
    NotVisibleOnCoordinator(String),
    /// DDL reported success but another node never saw the table.
    NotPropagated(String),
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok { ready_to_ddl_ms } => write!(f, "ok (DDL took {ready_to_ddl_ms} ms)"),
            Self::DdlFailed(e) => write!(f, "DDL refused: {e}"),
            Self::NotVisibleOnCoordinator(e) => {
                write!(f, "DDL ok but node 0 cannot read the table: {e}")
            }
            Self::NotPropagated(e) => write!(f, "DDL ok but not propagated: {e}"),
        }
    }
}

fn wait_cql_listening(node: &mut Node, budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = node.child.try_wait().expect("try_wait") {
            panic!(
                "node exited before CQL listened: {status}\n{}",
                tail(&node.log(), 40)
            );
        }
        if let Ok(mut s) = cql_connect(node.ports.cql) {
            if cql_query(&mut s, "SELECT release_version FROM system.local").is_ok() {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "CQL on {} never answered within {budget:?}\n{}",
            node.ports.cql,
            tail(&node.log(), 40)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Poll node 0 every 20 ms until `system.peers` lists both peers: the e2e's own
/// readiness signal, observed as early as a client could observe it.
fn wait_two_peers(node: &Node) -> TcpStream {
    let deadline = Instant::now() + PEERS_BUDGET;
    loop {
        let last;
        match cql_connect(node.ports.cql) {
            Ok(mut s) => match cql_query(&mut s, "SELECT peer FROM system.peers") {
                Ok(body) if row_count(&body) == Ok(2) => return s,
                Ok(body) => last = format!("rows {:?}", row_count(&body)),
                Err(e) => last = e,
            },
            Err(e) => last = e.to_string(),
        }
        assert!(
            Instant::now() < deadline,
            "node 0 never listed two peers within {PEERS_BUDGET:?} (last: {last})\n{}",
            tail(&node.log(), 60)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn table_readable(port: u16, ks: &str) -> Result<(), String> {
    let mut s = cql_connect(port).map_err(|e| e.to_string())?;
    cql_query(&mut s, &format!("SELECT v FROM {ks}.t WHERE k = 0")).map(|_| ())
}

fn wait_table_readable(port: u16, ks: &str, budget: Duration) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    loop {
        match table_readable(port, ks) {
            Ok(()) => return Ok(()),
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn run_ddl(stream: &mut TcpStream, ks: &str) -> Result<(), String> {
    cql_query(
        stream,
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = \
             {{'class':'SimpleStrategy','replication_factor':3}}"
        ),
    )?;
    cql_query(
        stream,
        &format!("CREATE TABLE IF NOT EXISTS {ks}.t (k int PRIMARY KEY, v text)"),
    )
    .map(|_| ())
}

fn start_cluster(delay: Duration, run: usize) -> Vec<Node> {
    let cluster = format!("late-ddl-{}-{run}", std::process::id());
    let mut alloc = PortAllocator::new();
    let ports: Vec<Ports> = (0..3).map(|_| alloc_ports(&mut alloc)).collect();
    let seed = Some(ports[0].internode);
    let mut n0 = spawn_node(ports[0], &host_id(0, run), &cluster, None);
    wait_cql_listening(&mut n0, CQL_LISTEN_BUDGET);
    let n2 = spawn_node(ports[2], &host_id(2, run), &cluster, seed);
    std::thread::sleep(delay);
    let n1 = spawn_node(ports[1], &host_id(1, run), &cluster, seed);
    vec![n0, n1, n2]
}

fn one_run(delay: Duration, run: usize) -> (Outcome, Vec<Node>) {
    let nodes = start_cluster(delay, run);
    let mut stream = wait_two_peers(&nodes[0]);
    let ready = Instant::now();
    let ks = format!("late_ddl_{run}");
    if let Err(e) = run_ddl(&mut stream, &ks) {
        return (Outcome::DdlFailed(e), nodes);
    }
    let ready_to_ddl_ms = ready.elapsed().as_millis();
    if let Err(e) = table_readable(nodes[0].ports.cql, &ks) {
        // Say where the DDL did land: a table only some node holds was applied
        // outside the replicated log.
        let holders: Vec<usize> = (0..nodes.len())
            .filter(|i| table_readable(nodes[*i].ports.cql, &ks).is_ok())
            .collect();
        return (
            Outcome::NotVisibleOnCoordinator(format!("{e}; nodes that can read it: {holders:?}")),
            nodes,
        );
    }
    for (i, node) in nodes.iter().enumerate().skip(1) {
        if let Err(e) = wait_table_readable(node.ports.cql, &ks, PROPAGATION_BUDGET) {
            return (Outcome::NotPropagated(format!("node {i}: {e}")), nodes);
        }
    }
    (Outcome::Ok { ready_to_ddl_ms }, nodes)
}

// ── reporting ─────────────────────────────────────────────────────────────────

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    match std::env::var(name) {
        Ok(v) => v
            .split(',')
            .map(|s| {
                s.trim()
                    .parse()
                    .unwrap_or_else(|e| panic!("{name}={v}: {e}"))
            })
            .collect(),
        Err(_) => default.to_vec(),
    }
}

fn sweep_runs() -> usize {
    match std::env::var("FERROSA_FORMATION_SWEEP_RUNS") {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|e| panic!("FERROSA_FORMATION_SWEEP_RUNS={v}: {e}")),
        Err(_) => 1,
    }
}

/// Copy every node's full log into `$FERROSA_FORMATION_KEEP_LOGS/<label>/` when
/// that variable names a directory, so a failing run can be read in full.
fn keep_logs(nodes: &[Node], label: &str) {
    let Ok(root) = std::env::var("FERROSA_FORMATION_KEEP_LOGS") else {
        return;
    };
    let dir = PathBuf::from(root).join(label);
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    for (i, node) in nodes.iter().enumerate() {
        let to = dir.join(format!("node{i}.log"));
        std::fs::write(&to, node.log()).unwrap_or_else(|e| panic!("write {}: {e}", to.display()));
    }
}

fn node_logs(nodes: &[Node]) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| format!("\n===== node {i} log tail =====\n{}", tail(&n.log(), 60)))
        .collect()
}

#[test]
fn first_ddl_after_formation_with_a_late_node_succeeds_and_is_visible() {
    let delays = env_list("FERROSA_FORMATION_SWEEP_DELAYS_MS", DEFAULT_DELAYS_MS);
    let runs = sweep_runs();
    let mut table = Vec::new();
    let mut first_failure: Option<String> = None;
    let mut runs_log = Vec::new();
    for (d, delay_ms) in delays.iter().enumerate() {
        let mut failed = 0usize;
        for r in 0..runs {
            let run = d * runs + r;
            let (outcome, nodes) = one_run(Duration::from_millis(*delay_ms), run);
            let line = format!("formation sweep: delay={delay_ms}ms run={r} outcome={outcome}");
            eprintln!("{line}");
            runs_log.push(line);
            if !matches!(outcome, Outcome::Ok { .. }) {
                failed += 1;
                keep_logs(&nodes, &format!("delay{delay_ms}-run{r}"));
                if first_failure.is_none() {
                    first_failure = Some(format!(
                        "delay={delay_ms}ms run={r}: {outcome}{}",
                        node_logs(&nodes)
                    ));
                }
            }
        }
        table.push(format!("  delay {delay_ms:>5} ms: {failed}/{runs} failed"));
    }
    eprintln!("formation sweep summary:\n{}", table.join("\n"));
    if let Ok(root) = std::env::var("FERROSA_FORMATION_KEEP_LOGS") {
        let path = PathBuf::from(root).join("summary.txt");
        std::fs::create_dir_all(path.parent().expect("summary dir"))
            .unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
        std::fs::write(
            &path,
            format!("{}\n{}\n", runs_log.join("\n"), table.join("\n")),
        )
        .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    }
    if let Some(failure) = first_failure {
        panic!(
            "the first DDL after formation failed:\n{}\nfirst failure: {failure}",
            table.join("\n")
        );
    }
}

/// The runner's own decode must hold, or a red could be a client bug.
#[test]
fn row_count_decodes_a_rows_result_and_rejects_a_truncated_one() {
    let mut body = Vec::new();
    body.extend_from_slice(&2i32.to_be_bytes()); // Rows
    body.extend_from_slice(&1i32.to_be_bytes()); // global table spec
    body.extend_from_slice(&1i32.to_be_bytes()); // one column
    for s in ["system", "peers", "peer"] {
        body.extend_from_slice(&(s.len() as u16).to_be_bytes());
        body.extend_from_slice(s.as_bytes());
    }
    body.extend_from_slice(&0x0010u16.to_be_bytes()); // inet
    body.extend_from_slice(&2i32.to_be_bytes());
    assert_eq!(row_count(&body), Ok(2));
    assert!(row_count(&body[..body.len() - 2]).is_err());
}
