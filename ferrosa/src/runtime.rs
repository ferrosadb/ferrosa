//! Subsystem runtime manager.
//!
//! Each subsystem gets its own tokio runtime so work on one path cannot
//! starve another.  The main runtime is supervisor-only.
//! Correctness: a consensus panic is recorded before unwinding continues; the
//! process remains alive and client-facing gates fail closed.
//! Last revised: 2026-08-27
//! Last changed: Replaced process abort with bounded consensus supervision.

use std::sync::Arc;
use std::time::Duration;

/// Resolve a positive-`usize` runtime tunable from an env value, falling back to
/// `default` when unset, unparseable, or non-positive.
///
/// Pure (the env read happens at the call site) so it is testable without racy
/// `set_var` in parallel tests.
fn resolve_positive_usize(env_val: Option<String>, default: usize) -> usize {
    env_val
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or(default)
}

/// Detected CPU parallelism, falling back to a single core.
fn detected_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Holds dedicated tokio runtimes for each subsystem.
///
/// Created once at startup and threaded through the initialization sequence.
/// Each runtime is `Arc`-wrapped so handles can be cheaply cloned into
/// subsystem components.
pub struct RuntimeManager {
    /// Raft consensus: openraft tasks, Raft lane IO, vote/heartbeat handlers.
    /// Must NEVER run bootstrap streaming, S3 sync, or data-path handlers.
    pub raft: Arc<tokio::runtime::Runtime>,
    /// Internode data path: read/write forwarding, bootstrap streaming, repair.
    pub data: Arc<tokio::runtime::Runtime>,
    /// Client CQL protocol path: accept loop, connection handlers, request dispatch.
    pub cql: Arc<tokio::runtime::Runtime>,
    /// Low-priority service work: seed retries, web/graph/sparql listeners,
    /// periodic maintenance coordinators, and one-shot warnings.
    pub background: Arc<tokio::runtime::Runtime>,
}

impl RuntimeManager {
    /// Build all subsystem runtimes.
    pub fn new() -> Self {
        fn worker_threads(env_key: &str, default: usize) -> usize {
            resolve_positive_usize(std::env::var(env_key).ok(), default)
        }
        fn max_blocking(env_key: &str, default: usize) -> usize {
            resolve_positive_usize(std::env::var(env_key).ok(), default)
        }

        // Explicit blocking-pool ceilings (t_88223ad0). Left unset, tokio defaults
        // to 512 blocking threads per runtime; a full-table ALLOW FILTERING scan
        // then admits hundreds of blocking producers that oversubscribe the cores
        // and starve raft heartbeats into a CheckQuorum step-down. These ceilings
        // are a cores-derived BACKSTOP; the tight per-scan CPU bound is the
        // ferrosa-sched pool (cores - reserved), which scan producers route
        // through (T0.3). The raft runtime is intentionally NOT capped — consensus
        // must never be throttled. cql keeps the default (client handlers are not
        // the scan-blocking offenders).
        let cores = detected_cores();
        let data_max_blocking = (cores * 8).max(8);
        let background_max_blocking = (cores * 2).max(4);

        let raft = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_RAFT_RUNTIME_THREADS", 8))
                .thread_name("raft-rt")
                .enable_all()
                .build()
                .expect("raft runtime"),
        );

        let data = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_DATA_RUNTIME_THREADS", 8))
                .max_blocking_threads(max_blocking("FERROSA_DATA_MAX_BLOCKING", data_max_blocking))
                .thread_name("data-rt")
                .enable_all()
                .build()
                .expect("data runtime"),
        );

        let cql = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_CQL_RUNTIME_THREADS", 8))
                .thread_name("cql-rt")
                .enable_all()
                .build()
                .expect("cql runtime"),
        );

        let background = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_BACKGROUND_RUNTIME_THREADS", 2))
                .max_blocking_threads(max_blocking(
                    "FERROSA_BACKGROUND_MAX_BLOCKING",
                    background_max_blocking,
                ))
                .thread_name("background-rt")
                .enable_all()
                .build()
                .expect("background runtime"),
        );

        Self {
            raft,
            data,
            cql,
            background,
        }
    }

    /// Pin the subsystem runtimes to the process lifetime so they are never
    /// dropped on the `#[tokio::main]` async stack.
    ///
    /// Dropping a tokio `Runtime` from within an async context panics
    /// ("Cannot drop a runtime in a context where blocking is not allowed").
    /// `main` holds this `RuntimeManager` (and its `Arc<Runtime>` clones flow
    /// into `ModeController`, `PeerManager`, spawned tasks, …) as locals on the
    /// async stack. On *any* early-error return — a listener bind failing, a
    /// startup step returning `Err(_)` — those locals unwind and the last
    /// surviving `Arc<Runtime>` clone drops in async context, firing that panic
    /// *before* the real error is reported and masking it (issue #172, which
    /// fixed the same trap for the S3 upload runtime).
    ///
    /// Leaking one extra `Arc` clone of each runtime keeps every strong count
    /// ≥ 1 for the life of the process, so no `Runtime::drop` ever runs on the
    /// async stack. The runtimes must live until exit anyway; the OS reclaims
    /// them. Call this once, immediately after [`RuntimeManager::new`].
    pub fn leak_for_process_lifetime(&self) {
        std::mem::forget(self.raft.clone());
        std::mem::forget(self.data.clone());
        std::mem::forget(self.cql.clone());
        std::mem::forget(self.background.clone());
    }

    /// Graceful shutdown in reverse dependency order.
    #[allow(dead_code)] // Will be called from shutdown path.
    pub fn shutdown_all(self, timeout: Duration) {
        if let Ok(rt) = Arc::try_unwrap(self.data) {
            rt.shutdown_timeout(timeout);
        }
        if let Ok(rt) = Arc::try_unwrap(self.cql) {
            rt.shutdown_timeout(timeout);
        }
        if let Ok(rt) = Arc::try_unwrap(self.background) {
            rt.shutdown_timeout(timeout);
        }
        if let Ok(rt) = Arc::try_unwrap(self.raft) {
            rt.shutdown_timeout(timeout);
        }
    }
}

/// The runtime whose panic means this node can no longer be a cluster member.
const CONSENSUS_RUNTIME_THREAD: &str = "raft-rt";

/// Did a panic originate on the dedicated consensus runtime?
///
/// A Rust panic unwinds one thread. For most of them that is the right scope —
/// CQL already wraps request handling in `catch_unwind` so a bad request kills
/// a connection, not a node. For consensus it is exactly wrong: when the raft
/// runtime dies the node stops replicating, loses its RaftAppendEntries
/// handler, and keeps answering CQL with whatever stale state it holds.
///
/// That happened here (2026-08-20, node1): a panic inside openraft left the
/// process alive for hours, logging `no handler registered` every 3.5 seconds
/// while serving `keyspace 'agent_memory' not found` to every client. launchd's
/// `KeepAlive { Crashed = true }` never fired because nothing crashed.
///
/// Matching is exact. `raft-log-store` is the sled blocking pool and has its
/// own error path.
pub(crate) fn is_consensus_runtime(thread_name: Option<&str>) -> bool {
    thread_name == Some(CONSENSUS_RUNTIME_THREAD)
}

/// Record a consensus panic in bounded shared state and return to the caller.
///
/// Kept separate from the process-global hook so the survival contract can be
/// tested without racing other tests' panic hooks.
fn record_consensus_panic(
    health: &ferrosa_cluster::ConsensusHealth,
    thread_name: Option<&str>,
    payload: &str,
    location: Option<(&str, u32, u32)>,
) -> bool {
    if !is_consensus_runtime(thread_name) {
        return false;
    }
    match location {
        Some((file, line, column)) => health.fail(
            "raft-runtime-panic",
            format_args!(
                "thread={} at {file}:{line}:{column}: {payload}",
                thread_name.unwrap_or("unnamed")
            ),
        ),
        None => health.fail(
            "raft-runtime-panic",
            format_args!(
                "thread={} at <unknown>: {payload}",
                thread_name.unwrap_or("unnamed")
            ),
        ),
    }
}

/// Install bounded supervision for a consensus-runtime panic.
///
/// The process deliberately remains alive: readiness closes, new and existing
/// CQL data operations return typed retriable errors, while protocol health
/// remains responsive for diagnosis. Consensus output is deliberately capped;
/// the prior hook is chained only for unrelated panics whose normal handling
/// this supervisor must not change.
pub fn install_consensus_panic_hook(health: Arc<ferrosa_cluster::ConsensusHealth>) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let current = std::thread::current();
        let name = current.name();
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload");
        let location = info
            .location()
            .map(|location| (location.file(), location.line(), location.column()));
        let consensus_thread = is_consensus_runtime(name);
        let first_failure = record_consensus_panic(&health, name, payload, location);
        if consensus_thread {
            if !first_failure {
                return;
            }
            let detail = health
                .failure()
                .map(ferrosa_cluster::ConsensusFailure::detail)
                .unwrap_or("consensus failure detail unavailable");
            eprintln!(
                "FATAL: consensus runtime failed; process remains alive in fail-closed mode; \
readiness=503; CQL data operations=OVERLOADED; detail={detail}"
            );
        } else {
            previous(info);
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A panic on the consensus runtime must fail the consensus health gate
    /// without terminating the process.
    ///
    /// Observed on node1 of the local three-node cluster, 2026-08-20. The raft
    /// thread panicked inside openraft:
    ///
    ///     thread 'raft-rt' panicked at raft_core.rs:769:35:
    ///     index out of bounds: the len is 0 but the index is 18446744073709551615
    ///
    /// A panic unwinds only its own thread, so the process stayed alive. What
    /// died with that thread was the node's participation in the cluster: the
    /// RaftAppendEntries handler went with it, and the leader logged
    ///
    ///     WARN no handler registered msg_type=RaftAppendEntries
    ///
    /// every 3.5 seconds for hours, into a file nobody was reading. The node
    /// kept accepting CQL connections the whole time and answered every query
    /// with `keyspace 'agent_memory' not found`, because it could no longer
    /// receive schema. A live endpoint returning a wrong answer is worse than a
    /// dead one: clients cannot fail over from it.
    ///
    /// The corrected contract keeps the diagnostic surface alive but makes
    /// readiness and CQL data operations fail closed from shared health state.
    #[test]
    fn a_panic_on_the_consensus_runtime_fails_health_and_returns() {
        let health = std::sync::Arc::new(ferrosa_cluster::ConsensusHealth::new());

        let first_failure = record_consensus_panic(
            &health,
            Some("raft-rt"),
            "index out of bounds: len is 0",
            Some(("raft_core.rs", 769, 35)),
        );

        assert!(
            first_failure,
            "the exact consensus runtime must be supervised"
        );
        assert!(
            !record_consensus_panic(&health, Some("raft-rt"), "repeat", None),
            "repeat panics must not own another FATAL emission"
        );
        assert!(!health.is_healthy());
        let failure = health.failure().expect("bounded diagnostic is retained");
        assert!(failure.detail().contains("raft_core.rs:769:35"));
        assert!(failure.detail().contains("len is 0"));
        assert!(failure.detail().len() <= 1024, "diagnostic must be bounded");
        // Reaching this assertion is the process-survival contract: the
        // recorder returns instead of aborting or panicking.
        assert_eq!(health.failure_count(), 2);
    }

    /// Exercise the real process-global hook in a child test process. The old
    /// implementation aborted here; the child now joins the panicked Raft
    /// worker, observes failed health, and exits successfully.
    #[test]
    fn consensus_panic_hook_keeps_child_process_alive() {
        const CHILD_ENV: &str = "FERROSA_TEST_CONSENSUS_PANIC_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            let health = Arc::new(ferrosa_cluster::ConsensusHealth::new());
            install_consensus_panic_hook(health.clone());
            for attempt in 0..2 {
                let result = std::thread::Builder::new()
                    .name(CONSENSUS_RUNTIME_THREAD.into())
                    .spawn(move || panic!("synthetic bounded consensus failure {attempt}"))
                    .expect("spawn named consensus worker")
                    .join();
                assert!(
                    result.is_err(),
                    "the worker panic must still unwind its thread"
                );
            }
            assert!(!health.is_healthy(), "the hook must close shared health");
            assert_eq!(health.failure_count(), 2);
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::tests::consensus_panic_hook_keeps_child_process_alive",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .expect("launch isolated panic-hook test process");
        assert!(
            output.status.success(),
            "consensus panic must not abort the process: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stderr.matches("FATAL: consensus runtime failed").count(),
            1,
            "only the first failure source may emit the bounded operator diagnostic: {stderr}"
        );
        assert!(
            stderr.len() <= 1_280,
            "the isolated operator diagnostic must remain bounded: {} bytes",
            stderr.len()
        );
    }

    /// Runtimes whose panic is survivable must NOT take the node down.
    ///
    /// CQL already wraps request handling in catch_unwind, so one bad request
    /// kills a connection rather than a process. Aborting on those would turn a
    /// contained fault into an outage — the opposite mistake, and an easy one
    /// to make while fixing the first.
    #[test]
    fn a_panic_on_a_request_runtime_is_not_fatal() {
        assert!(!is_consensus_runtime(Some("cql-rt")));
        assert!(!is_consensus_runtime(Some("data-rt")));
        assert!(!is_consensus_runtime(Some("background-rt")));
    }

    /// An unnamed thread is not assumed fatal. Tokio names its workers, so an
    /// unnamed panic is something else entirely, and guessing would make every
    /// unrelated library panic an outage.
    #[test]
    fn an_unnamed_thread_is_not_fatal() {
        assert!(!is_consensus_runtime(None));
        assert!(!is_consensus_runtime(Some("")));
    }

    /// Matching must be exact. A substring match on "raft" would catch
    /// "raft-log-store", which is a blocking pool for sled IO -- a panic there
    /// is a storage error, not a consensus failure.
    #[test]
    fn matching_is_exact_not_a_substring() {
        assert!(
            !is_consensus_runtime(Some("raft-log-store")),
            "the sled blocking pool is not the consensus runtime"
        );
        assert!(!is_consensus_runtime(Some("raft-rt-something-else")));
    }

    /// T0.2 (t_88223ad0): the runtime tunable parser prefers a valid env value
    /// over the default and falls back safely on unset / non-positive /
    /// unparseable input. Pure — no `set_var`, so no cross-test env races.
    #[test]
    fn resolve_positive_usize_prefers_valid_env_over_default() {
        assert_eq!(resolve_positive_usize(Some("16".into()), 8), 16);
        assert_eq!(resolve_positive_usize(None, 8), 8, "unset uses default");
        assert_eq!(
            resolve_positive_usize(Some("0".into()), 8),
            8,
            "non-positive falls back to default"
        );
        assert_eq!(
            resolve_positive_usize(Some("garbage".into()), 8),
            8,
            "unparseable falls back to default"
        );
        assert_eq!(
            resolve_positive_usize(Some("  4  ".into()), 8),
            4,
            "surrounding whitespace is trimmed"
        );
    }

    /// The data/background blocking ceilings scale with cores and never collapse
    /// below their floors, so a small node still admits enough blocking I/O.
    #[test]
    fn blocking_ceilings_scale_with_cores_and_have_floors() {
        // Mirrors the derivation in `new()`.
        for cores in [1usize, 2, 4, 8, 16] {
            let data = (cores * 8).max(8);
            let background = (cores * 2).max(4);
            assert!(data >= 8, "data ceiling floors at 8 (cores={cores})");
            assert!(
                background >= 4,
                "background ceiling floors at 4 (cores={cores})"
            );
            assert!(
                data < 512,
                "data ceiling must be well below tokio's 512 default"
            );
            assert!(
                data >= background,
                "data path needs at least as much as background"
            );
        }
        assert_eq!(
            detected_cores().max(1),
            detected_cores(),
            "detected cores is >= 1"
        );
    }

    /// Regression (issue #172): dropping a `RuntimeManager` — and therefore its
    /// `Arc<Runtime>` fields — inside an async context must NOT panic once the
    /// runtimes have been pinned for the process lifetime. Before the fix, an
    /// early-error return from the async `#[tokio::main]` dropped the last
    /// `Arc<Runtime>` on the async stack, firing "Cannot drop a runtime in a
    /// context where blocking is not allowed" and masking the real error.
    ///
    /// This test runs on the default current-thread test runtime (an async
    /// context); the `drop(rm)` below would panic without
    /// `leak_for_process_lifetime`.
    #[tokio::test]
    async fn dropping_manager_in_async_context_does_not_panic_after_leak() {
        // Keep the runtimes tiny — we only care about the drop behavior.
        std::env::set_var("FERROSA_RAFT_RUNTIME_THREADS", "1");
        std::env::set_var("FERROSA_DATA_RUNTIME_THREADS", "1");
        std::env::set_var("FERROSA_CQL_RUNTIME_THREADS", "1");
        std::env::set_var("FERROSA_BACKGROUND_RUNTIME_THREADS", "1");

        let rm = RuntimeManager::new();
        rm.leak_for_process_lifetime();
        // Would panic in this async context if the leak did not keep a strong
        // ref alive; reaching the assertion means no panic occurred.
        drop(rm);

        std::env::remove_var("FERROSA_RAFT_RUNTIME_THREADS");
        std::env::remove_var("FERROSA_DATA_RUNTIME_THREADS");
        std::env::remove_var("FERROSA_CQL_RUNTIME_THREADS");
        std::env::remove_var("FERROSA_BACKGROUND_RUNTIME_THREADS");
    }
}
