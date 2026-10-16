//! A node that cannot bind a listener must say WHICH listener and WHICH address.
//!
//! The cluster harnesses wait on `/readyz`. When a node dies at a bind instead,
//! the only evidence a reader gets is the node's own fatal line -- and in CI it
//! read `server error: I/O error: Address already in use (os error 98)`, naming
//! neither the listener nor the port. That forces whoever reads a red to guess
//! which of the node's eight listeners collided, which is how a port collision
//! gets mistaken for a hang.
//!
//! Each case holds one listener's port and asserts the node's log names that
//! listener and that address. Not every listener's failure is fatal, so the wait
//! is bounded and the node is always reaped; the log is the evidence either way.

mod common;

use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::ports::PortAllocator;

/// How long to give a node to exit on its own before reaping it.
const EXIT_BUDGET: Duration = Duration::from_secs(30);

struct Run {
    log: String,
    exited_on_its_own: bool,
}

/// Spawn a node with every listener on its own allocated port, except `listener`
/// which is forced onto `taken_port` (already held by the caller).
fn run_node_with_held_port(listener: &str, taken_port: u16) -> Run {
    let mut alloc = PortAllocator::new();
    let bindings = [
        ("FERROSA_WEB_BIND", "web"),
        ("FERROSA_CQL_BIND", "cql"),
        ("FERROSA_INTERNODE_BIND", "internode"),
        ("FERROSA_GRAPH_BIND", "graph"),
        ("FERROSA_POSTGRES_BIND", "postgres"),
        ("FERROSA_SPARQL_BIND", "sparql"),
        ("FERROSA_FLIGHT_BIND", "flight"),
    ];

    let dir = tempfile::tempdir().expect("node data dir");
    let log_path = dir.path().join("node.log");
    let log = std::fs::File::create(&log_path).expect("create node log");
    let err = log.try_clone().expect("clone log handle");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
    cmd.env("FERROSA_DATA_DIR", dir.path())
        .env("RUST_LOG", "info")
        .env("FERROSA_AUTH_ENABLED", "true")
        .env("FERROSA_CLUSTER_NAME", "bind-failure-probe")
        .env("FERROSA_HOST_ID", "11111111-1111-1111-1111-111111111111")
        .env("FERROSA_BOLT_PORT", alloc.get().to_string())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err));
    for (var, name) in bindings {
        let port = if name == listener {
            taken_port
        } else {
            alloc.get()
        };
        cmd.env(var, format!("127.0.0.1:{port}"));
    }
    let internode_broadcast = if listener == "internode" {
        taken_port
    } else {
        alloc.get()
    };
    cmd.env(
        "FERROSA_INTERNODE_BROADCAST",
        format!("127.0.0.1:{internode_broadcast}"),
    );

    let mut child = cmd.spawn().expect("spawn a ferrosa node");
    let exited_on_its_own = wait_bounded(&mut child, EXIT_BUDGET);
    Run {
        log: read_log(&log_path),
        exited_on_its_own,
    }
}

/// Wait up to `budget` for the child to exit; if it is still running, kill and
/// reap it. Returns whether it exited on its own.
fn wait_bounded(child: &mut Child, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        match child.try_wait().expect("try_wait on the node") {
            Some(_) => return true,
            None => std::thread::sleep(Duration::from_millis(200)),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    false
}

fn read_log(path: &Path) -> String {
    let mut text = String::new();
    std::fs::File::open(path)
        .expect("open node log")
        .read_to_string(&mut text)
        .expect("read node log");
    text
}

/// The invariant: the node's log names the listener and the address it could not
/// bind, whether or not the failure was fatal.
fn assert_names(run: &Run, needle: &str, port: u16, what: &str) {
    let addr = format!("127.0.0.1:{port}");
    println!("--- {what}: exited_on_its_own={}", run.exited_on_its_own);
    for line in run.log.lines() {
        if line.contains(&addr) || line.contains(needle) {
            println!("    {line}");
        }
    }
    // One line must carry all three: the failure, the listener, the address.
    let names_it = run.log.lines().any(|l| {
        l.contains(&addr) && l.contains(needle) && (l.contains("fail") || l.contains("ERROR"))
    });
    assert!(
        names_it,
        "a failure line must name {what} ('{needle}') and the address it could not bind \
         ({addr}); log was:\n{}",
        run.log
    );
}

#[test]
fn a_taken_web_port_is_named_in_the_fatal_line() {
    let port = PortAllocator::new().get();
    let _held = std::net::TcpListener::bind(("127.0.0.1", port)).expect("hold the web port");
    let run = run_node_with_held_port("web", port);
    assert_names(&run, "web console", port, "the web console listener");
}

#[test]
fn a_taken_postgres_port_is_named_in_the_log() {
    let port = PortAllocator::new().get();
    let _held = std::net::TcpListener::bind(("127.0.0.1", port)).expect("hold the postgres port");
    let run = run_node_with_held_port("postgres", port);
    assert_names(&run, "ostgres", port, "the PostgreSQL listener");
}

#[test]
fn a_taken_cql_port_is_named_in_the_log() {
    let port = PortAllocator::new().get();
    let _held = std::net::TcpListener::bind(("127.0.0.1", port)).expect("hold the cql port");
    let run = run_node_with_held_port("cql", port);
    assert_names(&run, "CQL", port, "the CQL listener");
}
