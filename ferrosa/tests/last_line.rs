//! A dying node must always leave a last line (outage 2026-10-10, F3/D4).
//!
//! On 2026-10-10 node2 and node3 exited with no final log line: the log
//! writer is non-blocking, and `process::exit` / `abort()` skip the guard
//! that drains it, so operators could not tell a crash from a kill.
//!
//! These tests run the real binary (`CARGO_BIN_EXE_ferrosa`) in a temporary
//! data and log directory, with every listener on an ephemeral loopback port,
//! and read `ferrosa.log` after it exits. Faults are injected through the
//! hidden `FERROSA_TEST_FAULT_INJECT` variable.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// How long a node may take to boot or to exit.
const DEADLINE: Duration = Duration::from_secs(120);

struct Node {
    child: Child,
    dir: TempDir,
}

impl Node {
    /// Start the binary in a fresh directory. `fault` sets the hidden fault
    /// injector.
    fn start(fault: Option<&str>) -> Self {
        Self::start_with(fault, &[])
    }

    /// [`Node::start`] with extra environment.
    fn start_with(fault: Option<&str>, extra_env: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = dir.path().join("ferrosa.toml");
        std::fs::write(&config, "").expect("write empty config");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
        cmd.env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", dir.path())
            .env("RUST_LOG", "info")
            .env("FERROSA_CONFIG", &config)
            .env("FERROSA_DATA_DIR", dir.path().join("data"))
            .env("FERROSA_LOG_DIR", dir.path().join("logs"))
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
            .stderr(Stdio::piped());
        if let Some(fault) = fault {
            cmd.env("FERROSA_TEST_FAULT_INJECT", fault);
        }
        cmd.envs(extra_env.iter().copied());
        let child = cmd.spawn().expect("spawn the ferrosa binary");
        Self { child, dir }
    }

    fn log_path(&self) -> PathBuf {
        self.dir.path().join("logs").join("ferrosa.log")
    }

    fn log(&self) -> String {
        read_log(&self.log_path())
    }

    /// Wait for the process to exit; panic past the deadline.
    fn wait(&mut self) -> ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return status;
            }
            if started.elapsed() > DEADLINE {
                let _ = self.child.kill(); // the assertion below reports the hang
                panic!(
                    "ferrosa did not exit within {DEADLINE:?}; log:\n{}",
                    self.log()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Wait until the log contains `needle`; panic past the deadline.
    fn wait_for_log(&mut self, needle: &str) {
        let started = Instant::now();
        while !self.log().contains(needle) {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                panic!(
                    "ferrosa exited ({status}) before logging {needle:?}; stderr:\n{}\nlog:\n{}",
                    self.stderr(),
                    self.log()
                );
            }
            assert!(
                started.elapsed() < DEADLINE,
                "no {needle:?} within {DEADLINE:?}; log:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
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
        // Never leave a node running after a failed assertion.
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn read_log(path: &Path) -> String {
    match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => panic!("read {}: {e}", path.display()),
    }
}

fn last_line(log: &str) -> &str {
    log.lines().rfind(|l| !l.trim().is_empty()).unwrap_or("")
}

/// T-F3-1: a panic on a non-consensus thread leaves a FATAL line carrying
/// the panic message in the log file, and the fatal exit that follows is the
/// last line.
#[test]
fn panic_on_any_thread_writes_a_final_log_line() {
    let mut node = Node::start(Some("panic-thread"));
    let status = node.wait();
    let log = node.log();
    assert_eq!(status.code(), Some(70), "log:\n{log}");
    let fatal = log
        .lines()
        .find(|l| l.contains("FATAL: thread 'fault-injected' panicked at"))
        .unwrap_or_else(|| panic!("no FATAL panic line in the log file:\n{log}"));
    assert!(
        fatal.contains("injected fault: worker thread panic"),
        "the FATAL line carries the panic message: {fatal}"
    );
    assert!(
        last_line(&log).contains("ferrosa exiting code=70 reason="),
        "last line: {:?}",
        last_line(&log)
    );
}

/// T-F3-2: `fatal_exit(3, "x")` drains the buffered log COMPLETELY and leaves
/// `ferrosa exiting code=3 reason=x` as the last line.
///
/// The writer is slowed to 100 us per line (hidden
/// `FERROSA_TEST_LOG_WRITE_DELAY_US`), so 20,000 queued lines take seconds to
/// drain, the way they do on a loaded machine. The full workspace run under
/// load lost all but ~209 of them: `WorkerGuard::drop` waits at most 100 ms to
/// queue its shutdown and 1 s for the writer (tracing-appender 0.2.5
/// `non_blocking.rs`), then gives up silently.
#[test]
fn fatal_exit_flushes_and_logs_reason() {
    let mut node = Node::start_with(
        Some("fatal-exit"),
        &[("FERROSA_TEST_LOG_WRITE_DELAY_US", "100")],
    );
    let status = node.wait();
    let log = node.log();
    assert_eq!(status.code(), Some(3), "log:\n{log}");
    // The fault queues 20,000 lines and an ERROR through the non-blocking
    // writer an instant before the exit: they are in the file only if the
    // exit drained the buffer.
    let buffered = log
        .lines()
        .filter(|l| l.contains("injected fault: buffered line "))
        .count();
    let tail: Vec<&str> = log.lines().rev().take(3).collect();
    assert_eq!(
        buffered, 20_000,
        "every queued line reaches the file before the exit line; last lines: {tail:?}"
    );
    assert!(
        log.contains(r#"fatal: exiting code=3 reason="x""#),
        "the ERROR queued last was flushed too; last lines: {tail:?}"
    );
    assert!(
        !log.contains("log drain"),
        "a complete drain reports no shortfall; last lines: {tail:?}"
    );
    assert!(
        last_line(&log).ends_with("ferrosa exiting code=3 reason=x"),
        "last line: {:?}",
        last_line(&log)
    );
}

/// T-F3-3: SIGTERM is logged by name, the node drains, and the last line is
/// the orderly exit.
#[test]
fn sigterm_logs_the_signal_name() {
    let mut node = Node::start(None);
    node.wait_for_log("ferrosa ready; waiting for SIGINT or SIGTERM");
    let pid = node.child.id().to_string();
    let sent = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("run kill");
    assert!(sent.success(), "kill -TERM {pid} failed");
    let status = node.wait();
    let log = node.log();
    assert_eq!(status.code(), Some(0), "log:\n{log}");
    assert!(
        log.contains("received SIGTERM"),
        "the signal is named:\n{log}"
    );
    assert!(
        last_line(&log).ends_with("ferrosa exiting code=0 reason=shutdown after SIGTERM"),
        "last line: {:?}",
        last_line(&log)
    );
}
