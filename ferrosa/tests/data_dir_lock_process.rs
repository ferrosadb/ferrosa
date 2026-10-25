//! Two processes must never share one data directory (P0, t_34e20fa6).
//!
//! On 2026-10-25 an installer VM ran a legacy launchd job beside the real one,
//! both on one `ferrosa.toml` and one `data_dir`. The duplicate replayed the
//! same commit log segment 179 times, removed the live node's compaction
//! staging directory (which then killed it with "failed to remove stale
//! compaction staging dir ... No such file or directory"), and writes that had
//! been acknowledged and read back were gone after the next reboot.
//!
//! These tests run the real binary (`CARGO_BIN_EXE_ferrosa`) twice on one
//! temporary data directory, every listener on an ephemeral loopback port, and
//! assert the second process refuses to start, names the holder, and leaves
//! the first one's files alone.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// How long a node may take to boot or to exit.
const DEADLINE: Duration = Duration::from_secs(120);

/// The log line a node writes once every listener is up.
const READY: &str = "ferrosa ready; waiting for SIGINT or SIGTERM";

/// The exit code a node uses when another process holds its data directory.
const EXIT_DATA_DIR_LOCKED: i32 = 75;

struct Node {
    child: Child,
    log_dir: PathBuf,
}

impl Node {
    /// Start the binary on `data_dir`, logging to its own `log_dir`.
    fn start(root: &Path, data_dir: &Path, name: &str) -> Self {
        let config = root.join("ferrosa.toml");
        if !config.exists() {
            std::fs::write(&config, "").expect("write empty config");
        }
        let log_dir = root.join(format!("logs-{name}"));
        let child = Command::new(env!("CARGO_BIN_EXE_ferrosa"))
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", root)
            .env("RUST_LOG", "info")
            .env("FERROSA_CONFIG", &config)
            .env("FERROSA_DATA_DIR", data_dir)
            .env("FERROSA_LOG_DIR", &log_dir)
            .env("FERROSA_WEB_BIND", "127.0.0.1:0")
            .env("FERROSA_CQL_BIND", "127.0.0.1:0")
            .env("FERROSA_GRAPH_BIND", "127.0.0.1:0")
            .env("FERROSA_BOLT_PORT", "0")
            .env("FERROSA_SPARQL_BIND", "127.0.0.1:0")
            .env("FERROSA_POSTGRES_BIND", "127.0.0.1:0")
            .env("FERROSA_FLIGHT_BIND", "127.0.0.1:0")
            .env("FERROSA_INTERNODE_BIND", "127.0.0.1:0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the ferrosa binary");
        Self { child, log_dir }
    }

    fn log(&self) -> String {
        let path = self.log_dir.join("ferrosa.log");
        match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => panic!("read {}: {e}", path.display()),
        }
    }

    /// Wait for the process to exit; panic past the deadline.
    fn wait(&mut self) -> ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return status;
            }
            if started.elapsed() > DEADLINE {
                panic!(
                    "ferrosa did not exit within {DEADLINE:?}; log:\n{}",
                    self.log()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Wait until the node logs that it is ready; panic past the deadline or
    /// if it exits first.
    fn wait_ready(&mut self) {
        let started = Instant::now();
        while !self.log().contains(READY) {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                panic!(
                    "ferrosa exited ({status}) before it was ready; stderr:\n{}\nlog:\n{}",
                    self.stderr(),
                    self.log()
                );
            }
            assert!(
                started.elapsed() < DEADLINE,
                "not ready within {DEADLINE:?}; log:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn is_running(&mut self) -> bool {
        self.child.try_wait().expect("try_wait").is_none()
    }

    fn sigterm(&self) {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("run kill");
        assert!(sent.success(), "kill -TERM {pid} failed");
    }

    fn stderr(&mut self) -> String {
        use std::io::Read;
        let mut out = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            pipe.read_to_string(&mut out).expect("read stderr");
        }
        out
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Never leave a node running after a failed assertion. Teardown is
        // best-effort: a node that already exited makes kill fail, which is
        // the outcome wanted.
        if let Ok(None) = self.child.try_wait() {
            if let Err(e) = self.child.kill() {
                eprintln!("teardown: kill ferrosa pid {}: {e}", self.child.id());
            }
            if let Err(e) = self.child.wait() {
                eprintln!("teardown: wait ferrosa pid {}: {e}", self.child.id());
            }
        }
    }
}

/// Every file and directory under `dir`, relative, sorted.
fn tree(dir: &Path) -> Vec<String> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            let rel = path
                .strip_prefix(base)
                .expect("under base")
                .display()
                .to_string();
            out.push(rel);
            if entry.file_type().expect("file type").is_dir() {
                walk(base, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// A second process on a live node's data directory exits non-zero, names
/// the directory and the holder's pid, and touches none of the first one's
/// files; the first keeps running and shuts down cleanly.
#[test]
fn second_process_on_one_data_dir_refuses_and_touches_nothing() {
    let root = TempDir::new().expect("temp dir");
    let data_dir = root.path().join("data");

    let mut first = Node::start(root.path(), &data_dir, "first");
    first.wait_ready();

    // The staging dir the duplicate removed on 2026-10-25, and a sentinel
    // nothing in ferrosa knows about.
    let staging = data_dir.join("compaction");
    assert!(
        staging.is_dir(),
        "a running node has its compaction staging dir {}",
        staging.display()
    );
    let sentinel = data_dir.join("sentinel-do-not-touch");
    std::fs::write(&sentinel, b"first node's file").expect("write sentinel");
    let before = tree(&data_dir);

    let mut second = Node::start(root.path(), &data_dir, "second");
    let status = second.wait();
    let stderr = second.stderr();
    assert_eq!(
        status.code(),
        Some(EXIT_DATA_DIR_LOCKED),
        "the second process must refuse the locked data dir; stderr:\n{stderr}\nlog:\n{}",
        second.log()
    );
    let data_dir_text = data_dir.display().to_string();
    assert!(
        stderr.contains(&data_dir_text),
        "the refusal names the data dir {data_dir_text}; stderr:\n{stderr}"
    );
    let holder = format!("pid {}", first.child.id());
    assert!(
        stderr.contains(&holder),
        "the refusal names the holder ({holder}); stderr:\n{stderr}"
    );
    assert!(
        !second.log().contains("replay"),
        "the refused process must not replay the commit log; log:\n{}",
        second.log()
    );

    assert!(first.is_running(), "the first node keeps running");
    assert!(
        staging.is_dir(),
        "the staging dir survives the second attempt"
    );
    assert_eq!(
        std::fs::read(&sentinel).expect("sentinel survives"),
        b"first node's file"
    );
    assert_eq!(
        tree(&data_dir),
        before,
        "the second process must not create, remove or rename anything in the data dir"
    );

    first.sigterm();
    let status = first.wait();
    assert_eq!(
        status.code(),
        Some(0),
        "the first node shuts down cleanly; log:\n{}",
        first.log()
    );
}

/// The lock file stays on disk after a node exits. It must not block the next
/// start: the lock is the kernel's, held by an open descriptor, not the file's
/// existence.
#[test]
fn lock_file_left_by_an_exited_node_does_not_block_the_next_start() {
    let root = TempDir::new().expect("temp dir");
    let data_dir = root.path().join("data");

    let mut first = Node::start(root.path(), &data_dir, "first");
    first.wait_ready();
    first.sigterm();
    assert_eq!(first.wait().code(), Some(0), "log:\n{}", first.log());
    let lock = data_dir.join("ferrosa.lock");
    assert!(
        lock.is_file(),
        "the lock file outlives its holder: {}",
        lock.display()
    );

    let mut next = Node::start(root.path(), &data_dir, "next");
    next.wait_ready();
    let recorded = std::fs::read_to_string(&lock).expect("read lock file");
    assert_eq!(
        recorded.trim(),
        next.child.id().to_string(),
        "the new holder records its own pid"
    );
    next.sigterm();
    assert_eq!(next.wait().code(), Some(0), "log:\n{}", next.log());
}
