//! Subsystem runtime manager.
//!
//! Each subsystem gets its own tokio runtime so work on one path cannot
//! starve another.  The main runtime is supervisor-only.
//! Correctness: a consensus panic is recorded before unwinding continues; the
//! process remains alive and client-facing gates fail closed.
//!
//! # Thread budget
//!
//! The four subsystem runtimes below used to default to raft=8, data=8, cql=8,
//! background=2 — **26 async worker threads on a 4-vCPU host**. That is a
//! 6.5× oversubscription of the CPU before a single blocking-pool task or
//! supervisor runtime is counted. On a CPU-scarce host the scheduler is then
//! forced to timeshare the workers, and the CQL runtime's liveness task can miss
//! its 100 ms tick by seconds — recorded as a `ferrosa_sched_runtime_stall_*`
//! event and seen by clients as a connect/query timeout.
//!
//! [`RuntimeManager::new`] now derives the *combined* worker count for the four
//! subsystem runtimes from `available_parallelism()`, distributing it by weight,
//! so the fixed runtimes stay within the host's parallelism instead of
//! oversubscribing it. `FERROSA_RUNTIME_WORKER_BUDGET` overrides the total; the
//! four per-runtime vars become relative weights (their sum seeds the default
//! budget and caps it from above).
//! Last revised: 2026-10-07
//! Last changed: Bounded the subsystem worker/blocking thread budget to the
//!   host's parallelism (the runtime-stall fix).

use std::sync::Arc;
use std::time::Duration;

/// Total async worker budget for the four subsystem runtimes.
///
/// Overrides the default (which is `available_parallelism`). The four
/// per-runtime env vars below still set the *relative* weights even when this is
/// set, so the budget is the total and the vars are the split.
const ENV_WORKER_BUDGET: &str = "FERROSA_RUNTIME_WORKER_BUDGET";

/// Default weights (also the historical worker counts) for each subsystem
/// runtime. They seed the default budget and define the split of whatever
/// budget applies.
const DEFAULT_RAFT_THREADS: usize = 8;
const DEFAULT_DATA_THREADS: usize = 8;
const DEFAULT_CQL_THREADS: usize = 8;
const DEFAULT_BACKGROUND_THREADS: usize = 2;

/// Minimum total worker budget: one worker per subsystem runtime. tokio refuses
/// to build a multi-thread runtime with zero workers and the process has four,
/// so a budget can never apply below this and still produce a running process.
const MIN_WORKER_BUDGET: usize = 4;

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

/// The resolved thread plan for the four subsystem runtimes — the pure output of
/// [`plan_threads`], before any runtime is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ThreadPlan {
    /// Total async worker budget across all four subsystem runtimes.
    worker_budget: usize,
    raft_workers: usize,
    data_workers: usize,
    cql_workers: usize,
    background_workers: usize,
    /// `max_blocking_threads` for the cql runtime. Previously left at tokio's
    /// 512 default; now a cores-derived ceiling.
    cql_max_blocking: usize,
    data_max_blocking: usize,
    background_max_blocking: usize,
}

/// Split `budget` worker threads across the four subsystem runtimes.
///
/// Deterministic proportional (largest-remainder) allocation by `weights`, with
/// two guarantees: every runtime gets at least one worker (tokio requires it),
/// and the parts always sum to exactly `budget`. `budget` is assumed `>=
/// MIN_WORKER_BUDGET` (enforced by [`plan_threads`]); the "at least one" pass
/// then always has a larger holder to draw from.
///
/// `partition_worker_budget(26, [8, 8, 8, 2]) == [8, 8, 8, 2]`, so a host with
/// ≥26 detected cores reproduces the historical per-runtime defaults exactly.
fn partition_worker_budget(budget: usize, weights: [usize; 4]) -> [usize; 4] {
    let total: usize = weights.iter().sum::<usize>().max(1);
    let mut out = [0usize; 4];
    let mut assigned = 0usize;
    for (i, &w) in weights.iter().enumerate() {
        out[i] = budget.saturating_mul(w) / total;
        assigned += out[i];
    }
    // Largest-remainder pass: hand the integer-division leftover to the runtimes
    // with the biggest fractional share (ties broken by larger weight, then
    // lower index) so the sum lands exactly on the budget.
    let mut leftover = budget.saturating_sub(assigned);
    while leftover > 0 {
        let mut best = 0usize;
        let mut best_frac = i64::MIN;
        let mut best_weight = 0usize;
        for i in 0..4 {
            let frac =
                (budget.saturating_mul(weights[i])) as i64 - (out[i].saturating_mul(total)) as i64;
            if frac > best_frac || (frac == best_frac && weights[i] > best_weight) {
                best_frac = frac;
                best_weight = weights[i];
                best = i;
            }
        }
        out[best] += 1;
        leftover -= 1;
    }
    // tokio will not build a multi-thread runtime with zero workers, so give any
    // empty runtime one worker and take it from the largest holder. Safe because
    // `budget >= 4` guarantees the largest holder has more than one.
    for i in 0..4 {
        if out[i] == 0 {
            let donor = (0..4).max_by_key(|&j| out[j]).unwrap_or(0);
            debug_assert!(
                out[donor] > 1,
                "budget below MIN_WORKER_BUDGET reached partition_worker_budget"
            );
            if out[donor] > 1 {
                out[donor] -= 1;
                out[i] = 1;
            }
        }
    }
    out
}

/// Compute the subsystem thread plan from the detected core count and an env
/// lookup. Pure: `env` is injected so the policy is testable without racing
/// `std::env::set_var` in parallel tests.
fn plan_threads(cores: usize, env: &dyn Fn(&str) -> Option<String>) -> ThreadPlan {
    let cores = cores.max(1);

    let weights = [
        resolve_positive_usize(env("FERROSA_RAFT_RUNTIME_THREADS"), DEFAULT_RAFT_THREADS),
        resolve_positive_usize(env("FERROSA_DATA_RUNTIME_THREADS"), DEFAULT_DATA_THREADS),
        resolve_positive_usize(env("FERROSA_CQL_RUNTIME_THREADS"), DEFAULT_CQL_THREADS),
        resolve_positive_usize(
            env("FERROSA_BACKGROUND_RUNTIME_THREADS"),
            DEFAULT_BACKGROUND_THREADS,
        ),
    ];

    // What the operator asked for in total. The budget never inflates beyond it,
    // so a deliberately tiny configuration (e.g. every runtime set to 1) stays
    // tiny instead of being scaled back up.
    let configured_sum: usize = weights.iter().sum();

    // Default budget = the host's parallelism, so the four fixed runtimes share
    // the cores instead of oversubscribing them. An explicit
    // FERROSA_RUNTIME_WORKER_BUDGET replaces the total; either way it is clamped
    // to `[MIN_WORKER_BUDGET, configured_sum]`.
    let budget = resolve_positive_usize(env(ENV_WORKER_BUDGET), cores)
        .min(configured_sum)
        .max(MIN_WORKER_BUDGET);

    let counts = partition_worker_budget(budget, weights);

    // Blocking-pool ceilings. Left unset, tokio defaults to 512 blocking threads
    // per runtime; a maintenance burst (fsync storms, S3 sync, a full-range
    // Merkle RPC) then admits hundreds of extra threads that oversubscribe a
    // CPU-scarce host. The data and background ceilings were already bounded
    // (t_88223ad0); the cql pool was left uncapped and is bounded here to the
    // same cores-derived backstop. The raft runtime is intentionally NOT capped —
    // consensus must never be throttled.
    let data_max_blocking = (cores * 8).max(8);
    let background_max_blocking = (cores * 2).max(4);
    let cql_max_blocking = (cores * 8).max(8);

    ThreadPlan {
        worker_budget: budget,
        raft_workers: counts[0],
        data_workers: counts[1],
        cql_workers: counts[2],
        background_workers: counts[3],
        cql_max_blocking,
        data_max_blocking,
        background_max_blocking,
    }
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
        Self::new_with_plan(plan_threads(detected_cores(), &|key| {
            std::env::var(key).ok()
        }))
    }

    fn new_with_plan(plan: ThreadPlan) -> Self {
        tracing::info!(
            worker_budget = plan.worker_budget,
            raft_workers = plan.raft_workers,
            data_workers = plan.data_workers,
            cql_workers = plan.cql_workers,
            background_workers = plan.background_workers,
            "runtime: subsystem worker budget bounded to the host's parallelism \
             (set {} to override)",
            ENV_WORKER_BUDGET
        );

        let raft = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(plan.raft_workers)
                .thread_name("raft-rt")
                .enable_all()
                .build()
                .expect("raft runtime"),
        );

        let data = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(plan.data_workers)
                .max_blocking_threads(plan.data_max_blocking)
                .thread_name("data-rt")
                .enable_all()
                .build()
                .expect("data runtime"),
        );

        let cql = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(plan.cql_workers)
                .max_blocking_threads(plan.cql_max_blocking)
                .thread_name("cql-rt")
                .enable_all()
                .build()
                .expect("cql runtime"),
        );

        let background = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(plan.background_workers)
                .max_blocking_threads(plan.background_max_blocking)
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

    /// Test accessor: the plan [`RuntimeManager::new`] would build on this host.
    #[cfg(test)]
    fn current_plan() -> ThreadPlan {
        plan_threads(detected_cores(), &|key| std::env::var(key).ok())
    }
}

/// Portable live-thread census for the deterministic isolation test. A tokio
/// multi-thread runtime spawns its worker threads eagerly at build time, so the
/// process's live OS-thread count immediately after [`RuntimeManager::new`] is a
/// direct, non-timing measurement of how many workers it actually created.
#[cfg(test)]
mod proc_threads {
    /// Number of live OS threads in this process, or `None` when the platform
    /// probe is unavailable (the caller must then skip its assertion rather than
    /// pass vacuously).
    pub fn live_thread_count() -> Option<usize> {
        #[cfg(target_os = "linux")]
        {
            // One directory entry per task; `/proc/self/task` includes the
            // calling thread itself.
            std::fs::read_dir("/proc/self/task")
                .ok()
                .map(|entries| entries.filter_map(|e| e.ok()).count())
        }
        #[cfg(target_os = "macos")]
        {
            macos::task_thread_count()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            None
        }
    }

    #[cfg(target_os = "macos")]
    mod macos {
        use std::os::raw::{c_int, c_uint};

        type MachPortT = c_uint;

        extern "C" {
            static mach_task_self_: MachPortT;
            fn task_threads(
                task: MachPortT,
                act_list: *mut *mut MachPortT,
                act_list_cnt: *mut c_uint,
            ) -> c_int;
            fn vm_deallocate(target_task: MachPortT, address: usize, size: usize) -> c_int;
        }

        /// Count this task's threads via `task_threads`, releasing the returned
        /// port array (the mach API does not free it).
        pub fn task_thread_count() -> Option<usize> {
            let mut list: *mut MachPortT = std::ptr::null_mut();
            let mut count: c_uint = 0;
            // SAFETY: `mach_task_self_` is the current task port; `task_threads`
            // writes a freshly allocated array of `count` thread ports into
            // `list`, which we free with `vm_deallocate` before returning.
            unsafe {
                let task = mach_task_self_;
                if task_threads(task, &mut list, &mut count) != 0 {
                    return None;
                }
                let bytes = (count as usize) * std::mem::size_of::<MachPortT>();
                vm_deallocate(task, list as usize, bytes);
            }
            (count > 0).then_some(count as usize)
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
    use serial_test::serial;

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

    /// The four runtimes' baseline weights (they seed the budget and cap it).
    fn default_weights() -> [usize; 4] {
        [
            DEFAULT_RAFT_THREADS,
            DEFAULT_DATA_THREADS,
            DEFAULT_CQL_THREADS,
            DEFAULT_BACKGROUND_THREADS,
        ]
    }

    /// The core regression, at the policy level: on a 4-vCPU host (the nightly
    /// runner, `node1.log:6` `cpus=4`) the four subsystem runtimes must not
    /// spawn the historical 26 workers. They must share the host's parallelism.
    ///
    /// This is the pure decision behind the measured OS-thread test below; there
    /// the same bound is checked against the live thread count so it cannot pass
    /// vacuously.
    #[test]
    fn worker_budget_is_bounded_by_parallelism_not_the_26_thread_default() {
        let plan = plan_threads(4, &|_| None);
        assert_eq!(
            plan.worker_budget, 4,
            "a 4-vCPU host budgets 4 subsystem workers"
        );
        assert_eq!(
            plan.raft_workers + plan.data_workers + plan.cql_workers + plan.background_workers,
            4,
            "the plan splits the budget exactly, never the 26-thread default"
        );
        assert!(
            plan.raft_workers > 0 && plan.cql_workers > 0,
            "raft and cql must each keep a worker on a small host"
        );
    }

    /// On a host with at least as many cores as the historical default total,
    /// the split reproduces the old per-runtime counts exactly — the change only
    /// bites where the old defaults would have oversubscribed.
    #[test]
    fn budget_at_or_above_26_reproduces_the_historical_default_split() {
        let plan = plan_threads(32, &|_| None);
        assert_eq!(plan.worker_budget, 26);
        assert_eq!(
            [
                plan.raft_workers,
                plan.data_workers,
                plan.cql_workers,
                plan.background_workers
            ],
            default_weights()
        );
    }

    /// The budget never inflates beyond what was configured: an operator who
    /// shrinks every runtime keeps a small process, even on a large host.
    #[test]
    fn worker_budget_never_exceeds_the_configured_total() {
        let env = |key: &str| match key {
            "FERROSA_RAFT_RUNTIME_THREADS"
            | "FERROSA_DATA_RUNTIME_THREADS"
            | "FERROSA_CQL_RUNTIME_THREADS"
            | "FERROSA_BACKGROUND_RUNTIME_THREADS" => Some("1".to_string()),
            _ => None,
        };
        let plan = plan_threads(64, &env);
        assert_eq!(plan.worker_budget, 4, "1+1+1+1 configured → budget 4");
        assert_eq!(
            plan.raft_workers + plan.data_workers + plan.cql_workers + plan.background_workers,
            4
        );
    }

    /// An explicit `FERROSA_RUNTIME_WORKER_BUDGET` overrides the default total,
    /// and the split always sums to it.
    #[test]
    fn explicit_worker_budget_overrides_the_default_and_sums_exactly() {
        let env = |key: &str| (key == ENV_WORKER_BUDGET).then(|| "10".to_string());
        let plan = plan_threads(64, &env);
        assert_eq!(plan.worker_budget, 10);
        assert_eq!(
            plan.raft_workers + plan.data_workers + plan.cql_workers + plan.background_workers,
            10,
            "the four runtimes share exactly the budget"
        );
    }

    /// The partition is total (sums to the budget) and floor-safe (no runtime
    /// left with zero workers, which tokio cannot build) across the whole range
    /// a real host can produce.
    #[test]
    fn partition_always_sums_to_budget_and_gives_every_runtime_a_worker() {
        let weights = default_weights();
        for budget in MIN_WORKER_BUDGET..=256 {
            let parts = partition_worker_budget(budget, weights);
            assert_eq!(
                parts.iter().sum::<usize>(),
                budget,
                "budget {budget} must split exactly"
            );
            assert!(
                parts.iter().all(|&w| w >= 1),
                "budget {budget} left a runtime with no worker: {parts:?}"
            );
        }
    }

    /// The data/background blocking ceilings scale with cores and never collapse
    /// below their floors, so a small node still admits enough blocking I/O.
    #[test]
    fn blocking_ceilings_scale_with_cores_and_have_floors() {
        // Mirrors the derivation in `plan_threads`.
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

    /// The cql blocking pool was left at tokio's uncapped 512 default; it is now
    /// bounded to the same cores-derived backstop as the data path.
    #[test]
    fn cql_blocking_pool_is_no_longer_uncapped() {
        for cores in [1usize, 2, 4, 8, 16] {
            let plan = plan_threads(cores, &|_| None);
            assert!(
                plan.cql_max_blocking < 512,
                "cql blocking pool must be bounded below tokio's 512 default (cores={cores})"
            );
            assert!(
                plan.cql_max_blocking >= 8,
                "cql blocking pool keeps a usable floor (cores={cores})"
            );
        }
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
    #[serial]
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

    /// Deterministic, non-timing proof of the oversubscription fix: build the
    /// real `RuntimeManager` in a child process and count the process's live OS
    /// threads. A tokio multi-thread runtime spawns its worker threads eagerly at
    /// build time, so this measures the actual worker threads, not an inferred
    /// number.
    ///
    /// The child forces `FERROSA_RUNTIME_WORKER_BUDGET=4` so the expected bound
    /// is host-independent (CI is 4-vCPU, this dev host is 18). The parent
    /// asserts the measured thread count stays within that budget plus a small
    /// constant for the process's non-runtime threads (main thread, the test
    /// harness's own thread, and tokio/jemalloc internals).
    ///
    /// ## Measured numbers (this machine, 18-vCPU macOS; the budget var makes
    /// the expectation host-independent)
    ///
    /// Same child, same input, only the runtime plan differs:
    /// * pre-fix revision (`origin/main`): the budget var does not exist, so the
    ///   four runtimes spawn the 26-thread default and the child reports the
    ///   harness's threads **+ 26 workers** — the oversubscription that starves
    ///   a 4-vCPU host. **RED**.
    /// * fixed revision: the child reports the harness's threads **+ 4 workers**
    ///   (budget=4 exactly split 1/1/1/1). **GREEN**.
    ///
    /// The exact counts are recorded on the board; the assertion below is the
    /// durable bound.
    #[test]
    #[serial]
    fn subsystem_worker_threads_stay_within_the_host_budget() {
        const CHILD_ENV: &str = "FERROSA_TEST_RT_BUDGET_CHILD";
        const MEASURED_TAG: &str = "RT_BUDGET_MEASURED";
        /// The budget the parent forces on the child, so the expected OS-thread
        /// count is a fixed number rather than whatever this host detects.
        const EXPECTED_BUDGET: usize = 4;
        /// Non-runtime threads allowed on top of the worker budget: the child's
        /// main thread, the harness thread running this test, plus slack for
        /// tokio/jemalloc internals. Deliberately small so a return to the
        /// 26-thread default (a ~+22 overshoot) cannot slip under it.
        const NON_WORKER_SLACK: usize = 8;

        if std::env::var_os(CHILD_ENV).is_some() {
            let plan = RuntimeManager::current_plan();
            let rm = RuntimeManager::new();
            // The worker threads exist now; the runtimes are idle, so no blocking
            // threads have been admitted.
            let measured = proc_threads::live_thread_count()
                .expect("os thread census must be available on this platform");
            println!(
                "{MEASURED_TAG} budget={} measured={} cores={}",
                plan.worker_budget,
                measured,
                detected_cores(),
            );
            drop(rm);
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::tests::subsystem_worker_threads_stay_within_the_host_budget",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .env(ENV_WORKER_BUDGET, EXPECTED_BUDGET.to_string())
            .env_remove("FERROSA_RAFT_RUNTIME_THREADS")
            .env_remove("FERROSA_DATA_RUNTIME_THREADS")
            .env_remove("FERROSA_CQL_RUNTIME_THREADS")
            .env_remove("FERROSA_BACKGROUND_RUNTIME_THREADS")
            .output()
            .expect("launch isolated thread-census test process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let line = stdout
            .lines()
            .find(|l| l.contains(MEASURED_TAG))
            .unwrap_or_else(|| {
                panic!(
                    "child did not report a thread census; stdout=\n{stdout}\nstderr=\n{}",
                    String::from_utf8_lossy(&output.stderr)
                )
            });
        let field = |name: &str| -> usize {
            line.split_whitespace()
                .find_map(|kv| kv.strip_prefix(name))
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("missing `{name}` in census line: {line}"))
        };
        let budget = field("budget=");
        let measured = field("measured=");
        // The plan must honour the forced budget — otherwise the thread-count
        // bound below is measuring the wrong expectation.
        assert_eq!(
            budget, EXPECTED_BUDGET,
            "the plan ignored {ENV_WORKER_BUDGET}: {line}"
        );
        let bound = EXPECTED_BUDGET + NON_WORKER_SLACK;
        assert!(
            measured <= bound,
            "subsystem runtimes spawned {measured} OS threads but the budget is \
             {EXPECTED_BUDGET} (+{NON_WORKER_SLACK} slack) = {bound}; the four runtimes \
             are oversubscribing the host again ({line})"
        );
    }
}
