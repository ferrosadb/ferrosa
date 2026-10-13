//! Real-entry-point guard for the 2026-10-10 boot decoupling.
//!
//! The internode listener must come up within a bounded time of process start
//! even when the configured object store never answers. Before the decoupling,
//! `ferrosa/src/main.rs` awaited `validate_object_store_access` (then the CAS
//! probe, the upload replay, and the cold-start bootstrap) *before* binding the
//! internode listener, with `FERROSA_S3_REQUEST_TIMEOUT_SECS` defaulting to 900 s
//! — so a restarting node was off the internode lane for the whole window. That
//! is what turned a brief leaderless window into the 2026-10-10 35-minute outage.
//!
//! This spawns a real `ferrosa` process against a black-holed S3 endpoint (a TCP
//! listener that accepts and never replies) and asserts the internode port
//! accepts a connection within [`BOUND`]. Measured on the pre-decoupling binary,
//! the port never opens within the cap: the object-store client hangs in
//! `validate_object_store_access` for its ~180 s retry window. The node is a
//! cold start (empty data dir), so the object-store path it reaches first is the
//! strongest form of the defect.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Well below the object store's ~180 s retry window and the 900 s default
/// request timeout; far above the ~1 s the local boot actually takes (measured:
/// the node reaches the object-store client ~0.3 s after `ferrosa starting`).
const BOUND: Duration = Duration::from_secs(30);

/// A TCP listener that accepts connections and never replies, holding them open.
struct Blackhole {
    port: u16,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Blackhole {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind blackhole");
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut held: Vec<TcpStream> = Vec::new();
            while !stop_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => held.push(stream), // never respond
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            port,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for Blackhole {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

struct Node {
    _dir: tempfile::TempDir,
    data_dir: PathBuf,
    internode: u16,
    child: Child,
}

impl Node {
    fn log_path(&self) -> PathBuf {
        self.data_dir.join("node.log")
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_node(blackhole_s3_port: u16) -> Node {
    let dir = tempfile::tempdir().expect("node data dir");
    let data_dir = dir.path().to_path_buf();
    let internode = free_port();
    let cql = free_port();
    let web = free_port();
    let graph = free_port();
    let bolt = free_port();
    let pg = free_port();
    let sparql = free_port();
    let flight = free_port();

    let log = std::fs::File::create(data_dir.join("node.log")).expect("create node log");
    let stderr = log.try_clone().expect("clone log handle");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
    cmd.env("RUST_MIN_STACK", "33554432")
        .env("RUST_LOG", "info")
        .env("FERROSA_AUTH_ENABLED", "true")
        .env("FERROSA_DATA_DIR", &data_dir)
        .env(
            "FERROSA_CLUSTER_NAME",
            format!("boot-decouple-{}", std::process::id()),
        )
        .env("FERROSA_HOST_ID", "11111111-1111-1111-1111-111111111111")
        .env("FERROSA_CQL_BIND", format!("127.0.0.1:{cql}"))
        .env("FERROSA_WEB_BIND", format!("127.0.0.1:{web}"))
        .env("FERROSA_INTERNODE_BIND", format!("127.0.0.1:{internode}"))
        .env(
            "FERROSA_INTERNODE_BROADCAST",
            format!("127.0.0.1:{internode}"),
        )
        .env("FERROSA_GRAPH_BIND", format!("127.0.0.1:{graph}"))
        .env("FERROSA_BOLT_PORT", bolt.to_string())
        .env("FERROSA_POSTGRES_BIND", format!("127.0.0.1:{pg}"))
        .env("FERROSA_SPARQL_BIND", format!("127.0.0.1:{sparql}"))
        .env("FERROSA_FLIGHT_BIND", format!("127.0.0.1:{flight}"))
        // Object store: a black-holed S3 endpoint that never answers.
        .env(
            "FERROSA_S3_ENDPOINT",
            format!("http://127.0.0.1:{blackhole_s3_port}"),
        )
        .env("FERROSA_S3_BUCKET", "boot-decouple-bucket")
        .env("FERROSA_S3_ACCESS_KEY_ID", "probe")
        .env("FERROSA_S3_SECRET_ACCESS_KEY", "probe")
        .env("FERROSA_S3_ALLOW_HTTP", "true")
        // Not required: the node must still come up on the lane.
        .env("FERROSA_S3_REQUIRED", "false")
        // The window the defect made the node wait out. Must NOT be needed.
        .env("FERROSA_S3_REQUEST_TIMEOUT_SECS", "900")
        // Ensure no local-store override sneaks in.
        .env_remove("FERROSA_LOCAL_STORE_PATH")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));

    let child = cmd.spawn().expect("spawn ferrosa node");
    Node {
        _dir: dir,
        data_dir,
        internode,
        child,
    }
}

fn internode_accepts(port: u16) -> bool {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok()
}

#[test]
fn internode_lane_comes_up_in_bounded_time_while_object_store_is_black_holed() {
    let blackhole = Blackhole::start();
    let mut node = spawn_node(blackhole.port);

    let start = Instant::now();
    let mut listened = None;
    while start.elapsed() < BOUND {
        if node.child.try_wait().expect("wait node").is_some() {
            break; // node exited early — do not spin
        }
        if internode_accepts(node.internode) {
            listened = Some(start.elapsed());
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
    let tail = {
        let all: Vec<&str> = log.lines().collect();
        all[all.len().saturating_sub(40)..].join("\n")
    };

    assert!(
        listened.is_some(),
        "the internode lane must be reachable within {BOUND:?} regardless of object-store \
         health; it was not. The node is still blocked on the boot-time object-store work \
         before binding the listener (the 2026-10-10 outage shape).\n--- node log tail ---\n{tail}"
    );
    let listened = listened.unwrap();
    assert!(
        log.contains("internode server listening"),
        "the listener bound but the node never logged it; log tail:\n{tail}"
    );

    // Also assert the node is genuinely slow-but-alive on the object store, not
    // that it skipped S3 entirely: it must still be running (not exited) and the
    // black hole must have been contacted.
    assert!(
        node.child.try_wait().expect("wait node").is_none(),
        "the node must remain up (degraded on the object store), not exit"
    );

    eprintln!(
        "internode listened after {listened:?} (bound {BOUND:?}); object-store work still \
         pending in the background"
    );
    drop(node);
}
