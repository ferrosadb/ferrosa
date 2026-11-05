//! Explicit spawn targets for subsystem-owned work, with bounded admission
//! for blocking jobs.
//!
//! # Why blocking work is admitted against a capacity
//!
//! tokio's blocking pool has a thread ceiling but its *queue* has no capacity:
//! once every blocking thread is busy, `spawn_blocking` keeps accepting
//! closures and parks them in an unbounded queue. Each queued closure owns its
//! payload (a Raft snapshot's bytes, a write batch, a partition list), so under
//! sustained load that queue is a memory leak with a queue in front of it.
//!
//! [`TaskPool::spawn_blocking`] therefore awaits an owned permit from the
//! pool's [`Semaphore`] *before* handing the closure to tokio. The permit is
//! moved into the closure and dropped when the closure returns or unwinds, so a
//! panicking job releases its slot too. A caller that finds the pool full waits
//! (backpressure travels to the async caller) instead of enqueueing.
//!
//! # Why the bound lives in a process-wide registry keyed by pool name
//!
//! `TaskPool::current("name")` is constructed ad hoc at call sites — often a
//! fresh value per call (`coordinator/write.rs`, `raft/state_machine.rs`). A
//! semaphore owned by each `TaskPool` value would therefore bound nothing. The
//! semaphore is instead looked up by name in a process-wide registry, so every
//! `TaskPool` that names the same pool shares one bound, whatever runtime it
//! targets. A single shared default bound was rejected: it would let one busy
//! subsystem (repair merkle builds, say) starve an unrelated latency-critical
//! one (the Raft log store) of admission.
//!
//! # Capacity
//!
//! The default is `4 × available_parallelism`, clamped to `[8, 128]`
//! ([`default_blocking_capacity`]). Blocking work here is mostly disk and
//! object-store I/O (sled flushes, fsyncs, SSTable reads), where running a few
//! jobs per core keeps the device queue full; the clamp keeps a 1-core
//! container usable and keeps the sum across the ~15 named pools in the same
//! range as tokio's default 512 blocking threads.
//!
//! Overrides, read once when a pool is first used:
//! - `FERROSA_BLOCKING_POOL_CAPACITY_<NAME>` — one pool; `<NAME>` is the pool
//!   name upper-cased with `-` replaced by `_` (`raft-log-store` →
//!   `FERROSA_BLOCKING_POOL_CAPACITY_RAFT_LOG_STORE`).
//! - `FERROSA_BLOCKING_POOL_CAPACITY` — every pool without its own override.
//!
//! A value that is not a positive integer is rejected with an `error!` line
//! naming the variable and the value, and resolution falls through to the next
//! source (global override, then the default). It never silently defaults.
//!
//! # Observability
//!
//! Each pool counts the callers that had to wait (`waits_total`), the callers
//! waiting now (`waiting`) and the jobs holding a permit (`in_flight`); see
//! [`TaskPool::blocking_stats`] and [`blocking_pool_stats`]. Saturation is
//! logged on its EDGES: one `warn!` when a caller first has to wait, one
//! `info!` when the last waiter is admitted — not a line per wait.
//!
//! # Closed semaphore
//!
//! The semaphores are private to this module and never closed. A closed one is
//! a programming error, so `spawn_blocking` panics with the pool name rather
//! than falling back to unbounded submission.

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Global capacity override for every blocking pool without its own override.
pub const BLOCKING_POOL_CAPACITY_ENV: &str = "FERROSA_BLOCKING_POOL_CAPACITY";

const DEFAULT_JOBS_PER_CORE: usize = 4;
const DEFAULT_MIN_CAPACITY: usize = 8;
const DEFAULT_MAX_CAPACITY: usize = 128;

/// Explicit spawn target for subsystem-owned async work.
///
/// `TaskPool::current` is a named fallback for code paths that do not yet
/// receive a dedicated runtime. Production hot paths should prefer
/// `TaskPool::runtime` so work lands on a tunable subsystem runtime.
///
/// Blocking work submitted through [`TaskPool::spawn_blocking`] is admitted
/// against the named pool's capacity; see the module docs.
#[derive(Clone)]
pub struct TaskPool {
    name: &'static str,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl TaskPool {
    pub fn current(name: &'static str) -> Self {
        Self {
            name,
            runtime: None,
        }
    }

    pub fn runtime(name: &'static str, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self {
            name,
            runtime: Some(runtime),
        }
    }

    pub fn from_optional_runtime(
        name: &'static str,
        runtime: Option<Arc<tokio::runtime::Runtime>>,
    ) -> Self {
        match runtime {
            Some(runtime) => Self::runtime(name, runtime),
            None => Self::current(name),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn runtime_ref(&self) -> Option<&tokio::runtime::Runtime> {
        self.runtime.as_deref()
    }

    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if let Some(runtime) = &self.runtime {
            runtime.spawn(future)
        } else {
            tokio::spawn(future)
        }
    }

    /// Run `f` on a blocking thread once this pool has capacity for it.
    ///
    /// Waits for a permit from the pool's process-wide semaphore BEFORE the
    /// closure reaches tokio, so at most `capacity` closures of this pool are
    /// running or queued in tokio at once. The permit is released when `f`
    /// returns or panics. Dropping the returned handle detaches the job; it
    /// keeps its permit until it finishes.
    ///
    /// # Panics
    ///
    /// If the pool's semaphore has been closed. It never is: that would be a
    /// programming error in this module, and running unbounded instead would
    /// hide it.
    pub async fn spawn_blocking<F, R>(&self, f: F) -> tokio::task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let permit = admission_for(self.name).acquire().await;
        let job = move || {
            let _permit = permit;
            f()
        };
        if let Some(runtime) = &self.runtime {
            runtime.spawn_blocking(job)
        } else {
            tokio::task::spawn_blocking(job)
        }
    }

    /// Admission counters for this pool (shared with every `TaskPool` of the
    /// same name). Registers the pool if it has not been used yet.
    pub fn blocking_stats(&self) -> BlockingPoolStats {
        admission_for(self.name).stats()
    }
}

/// A snapshot of one blocking pool's admission state, for export as metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingPoolStats {
    pub name: &'static str,
    /// The most jobs that may hold a permit at once.
    pub capacity: usize,
    /// Jobs holding a permit: running, or queued inside tokio.
    pub in_flight: usize,
    /// Callers waiting for a permit right now.
    pub waiting: usize,
    /// Callers that ever had to wait for a permit.
    pub waits_total: u64,
    /// True between a saturation edge and the matching recovery edge.
    pub saturated: bool,
}

/// Admission counters for every blocking pool used so far, sorted by name.
pub fn blocking_pool_stats() -> Vec<BlockingPoolStats> {
    let mut stats: Vec<BlockingPoolStats> = lock_registry()
        .values()
        .map(|admission| admission.stats())
        .collect();
    stats.sort_by_key(|s| s.name);
    stats
}

/// Fix a pool's capacity before its first use.
///
/// Fails if the pool is already registered with a different capacity: its
/// semaphore exists and callers may hold permits from it, so it cannot be
/// resized safely. Re-configuring with the same capacity is a no-op.
pub fn configure_blocking_capacity(
    name: &'static str,
    capacity: NonZeroUsize,
) -> Result<(), BlockingPoolAlreadyRegistered> {
    let mut registry = lock_registry();
    if let Some(existing) = registry.get(name) {
        if existing.capacity == capacity.get() {
            return Ok(());
        }
        return Err(BlockingPoolAlreadyRegistered {
            name,
            existing: existing.capacity,
            requested: capacity.get(),
        });
    }
    registry.insert(name, Arc::new(Admission::new(name, capacity)));
    Ok(())
}

/// [`configure_blocking_capacity`] was called for a pool already in use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingPoolAlreadyRegistered {
    pub name: &'static str,
    pub existing: usize,
    pub requested: usize,
}

impl std::fmt::Display for BlockingPoolAlreadyRegistered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "blocking pool {:?} is already registered with capacity {}; cannot set {}",
            self.name, self.existing, self.requested
        )
    }
}

impl std::error::Error for BlockingPoolAlreadyRegistered {}

/// A capacity override that is not a positive integer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapacityParseError {
    Empty,
    Zero,
    NotANumber(String),
}

impl std::fmt::Display for CapacityParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty value; expected a positive integer"),
            Self::Zero => write!(f, "0 would admit no blocking work; expected >= 1"),
            Self::NotANumber(raw) => write!(f, "{raw:?} is not a positive integer"),
        }
    }
}

impl std::error::Error for CapacityParseError {}

/// Parse a capacity override. Surrounding whitespace is ignored.
pub fn parse_blocking_capacity(raw: &str) -> Result<NonZeroUsize, CapacityParseError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CapacityParseError::Empty);
    }
    let value: usize = trimmed
        .parse()
        .map_err(|_| CapacityParseError::NotANumber(trimmed.to_string()))?;
    NonZeroUsize::new(value).ok_or(CapacityParseError::Zero)
}

/// The per-pool override variable for `name`.
pub fn per_pool_capacity_env(name: &str) -> String {
    let suffix: String = name
        .chars()
        .map(|c| match c {
            '-' | '.' | ' ' => '_',
            c => c.to_ascii_uppercase(),
        })
        .collect();
    format!("{BLOCKING_POOL_CAPACITY_ENV}_{suffix}")
}

/// `4 × available_parallelism`, clamped to `[8, 128]`; see the module docs.
pub fn default_blocking_capacity() -> NonZeroUsize {
    let cores = match std::thread::available_parallelism() {
        Ok(n) => n.get(),
        Err(error) => {
            tracing::warn!(
                %error,
                "available_parallelism unknown; sizing blocking pools for 1 core"
            );
            1
        }
    };
    let capacity = cores
        .saturating_mul(DEFAULT_JOBS_PER_CORE)
        .clamp(DEFAULT_MIN_CAPACITY, DEFAULT_MAX_CAPACITY);
    NonZeroUsize::new(capacity).expect("clamped to a positive minimum")
}

/// Resolve a pool's capacity: per-pool override, then global, then default.
/// Every rejected override is logged; see the module docs.
fn resolve_capacity(name: &str, lookup: impl Fn(&str) -> Option<String>) -> NonZeroUsize {
    for var in [
        per_pool_capacity_env(name),
        BLOCKING_POOL_CAPACITY_ENV.to_string(),
    ] {
        let Some(raw) = lookup(&var) else {
            continue;
        };
        match parse_blocking_capacity(&raw) {
            Ok(capacity) => return capacity,
            Err(error) => tracing::error!(
                pool = name,
                variable = %var,
                value = %raw,
                %error,
                "rejected blocking-pool capacity override; falling through to the next source"
            ),
        }
    }
    default_blocking_capacity()
}

fn env_lookup(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(raw)) => Some(raw.to_string_lossy().into_owned()),
    }
}

type Registry = Mutex<HashMap<&'static str, Arc<Admission>>>;

fn lock_registry() -> std::sync::MutexGuard<'static, HashMap<&'static str, Arc<Admission>>> {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    // Nothing panics while the lock is held (get/insert only; capacity is
    // resolved outside it), so poisoning is a bug worth crashing on.
    REGISTRY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("blocking-pool registry mutex poisoned")
}

fn admission_for(name: &'static str) -> Arc<Admission> {
    if let Some(existing) = lock_registry().get(name) {
        return Arc::clone(existing);
    }
    let capacity = resolve_capacity(name, env_lookup);
    let mut registry = lock_registry();
    let admission = registry.entry(name).or_insert_with(|| {
        tracing::debug!(pool = name, capacity, "registered blocking pool");
        Arc::new(Admission::new(name, capacity))
    });
    Arc::clone(admission)
}

/// One named pool's bound and counters.
struct Admission {
    name: &'static str,
    capacity: usize,
    semaphore: Arc<Semaphore>,
    waits_total: AtomicU64,
    waiting: AtomicUsize,
    saturated: AtomicBool,
}

impl Admission {
    fn new(name: &'static str, capacity: NonZeroUsize) -> Self {
        Self {
            name,
            capacity: capacity.get(),
            semaphore: Arc::new(Semaphore::new(capacity.get())),
            waits_total: AtomicU64::new(0),
            waiting: AtomicUsize::new(0),
            saturated: AtomicBool::new(false),
        }
    }

    async fn acquire(&self) -> OwnedSemaphorePermit {
        match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => return permit,
            Err(TryAcquireError::NoPermits) => {}
            Err(TryAcquireError::Closed) => self.closed(),
        }
        // Counted for the life of the wait, including a caller cancelled
        // mid-wait: the guard's drop records the exit either way.
        let _waiter = Waiter::enter(self);
        match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_closed) => self.closed(),
        }
    }

    fn closed(&self) -> ! {
        panic!(
            "blocking pool {:?}: admission semaphore closed; refusing to run blocking work unbounded",
            self.name
        )
    }

    fn stats(&self) -> BlockingPoolStats {
        BlockingPoolStats {
            name: self.name,
            capacity: self.capacity,
            in_flight: self
                .capacity
                .saturating_sub(self.semaphore.available_permits()),
            waiting: self.waiting.load(Ordering::Acquire),
            waits_total: self.waits_total.load(Ordering::Relaxed),
            saturated: self.saturated.load(Ordering::Acquire),
        }
    }
}

/// A caller waiting for a permit. Records the saturation and recovery edges.
struct Waiter<'a> {
    admission: &'a Admission,
}

impl<'a> Waiter<'a> {
    fn enter(admission: &'a Admission) -> Self {
        admission.waits_total.fetch_add(1, Ordering::Relaxed);
        admission.waiting.fetch_add(1, Ordering::AcqRel);
        if !admission.saturated.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                pool = admission.name,
                capacity = admission.capacity,
                "blocking pool saturated; callers now wait for admission"
            );
        }
        Self { admission }
    }
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        let admission = self.admission;
        let was = admission.waiting.fetch_sub(1, Ordering::AcqRel);
        if was == 1 && admission.saturated.swap(false, Ordering::AcqRel) {
            tracing::info!(
                pool = admission.name,
                capacity = admission.capacity,
                waits_total = admission.waits_total.load(Ordering::Relaxed),
                "blocking pool recovered; no caller is waiting for admission"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test capacity must be non-zero")
    }

    /// Poll `cond` until it holds, panicking with `what` after 5 s.
    async fn wait_for(what: &str, cond: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn at_most_capacity_blocking_jobs_are_admitted() {
        const POOL: &str = "test-bound-two-of-five";
        configure_blocking_capacity(POOL, nz(2)).expect("fresh pool");
        let pool = TaskPool::current(POOL);
        let entered = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = crossbeam_channel::unbounded::<()>();

        let callers: Vec<_> = (0..5)
            .map(|_| {
                let pool = pool.clone();
                let entered = Arc::clone(&entered);
                let release_rx = release_rx.clone();
                tokio::spawn(async move {
                    let handle = pool
                        .spawn_blocking(move || {
                            entered.fetch_add(1, Ordering::AcqRel);
                            release_rx.recv().expect("release sender alive");
                        })
                        .await;
                    handle.await.expect("blocking job must not panic");
                })
            })
            .collect();

        wait_for("two jobs to start", || entered.load(Ordering::Acquire) >= 2).await;
        wait_for("three callers to wait", || {
            pool.blocking_stats().waiting == 3
        })
        .await;
        // Give an unbounded pool every chance to start the other three.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            entered.load(Ordering::Acquire),
            2,
            "more blocking closures started than the pool's capacity"
        );
        let stats = pool.blocking_stats();
        assert_eq!(stats.in_flight, 2, "{stats:?}");
        assert_eq!(stats.waiting, 3, "{stats:?}");
        assert!(stats.waits_total >= 3, "{stats:?}");
        assert!(stats.saturated, "saturation edge not recorded: {stats:?}");

        for _ in 0..5 {
            release_tx.send(()).expect("jobs alive");
        }
        for caller in callers {
            caller.await.expect("caller task");
        }
        assert_eq!(entered.load(Ordering::Acquire), 5);
        let stats = pool.blocking_stats();
        assert_eq!((stats.in_flight, stats.waiting), (0, 0), "{stats:?}");
        assert!(!stats.saturated, "recovery edge not recorded: {stats:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pools_with_the_same_name_share_one_bound() {
        const POOL: &str = "test-shared-bound";
        configure_blocking_capacity(POOL, nz(1)).expect("fresh pool");
        let first = TaskPool::current(POOL);
        let second = TaskPool::current(POOL);
        let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(1);

        let holder = first
            .spawn_blocking(move || release_rx.recv().expect("release sender alive"))
            .await;
        let blocked = tokio::time::timeout(
            Duration::from_millis(200),
            second.spawn_blocking(|| "second"),
        )
        .await;
        assert!(
            blocked.is_err(),
            "a second TaskPool of the same name was admitted past the shared bound"
        );

        let second_job =
            tokio::spawn(async move { second.spawn_blocking(|| "second").await.await });
        wait_for("the second instance to wait", || {
            first.blocking_stats().waiting == 1
        })
        .await;
        release_tx.send(()).expect("holder alive");
        holder.await.expect("holder job");
        let output = second_job.await.expect("second caller");
        assert_eq!(output.expect("second job"), "second");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_job_releases_its_permit() {
        const POOL: &str = "test-panic-release";
        configure_blocking_capacity(POOL, nz(1)).expect("fresh pool");
        let pool = TaskPool::current(POOL);

        let panicked = pool
            .spawn_blocking(|| panic!("deliberate test panic"))
            .await
            .await;
        assert!(
            panicked.expect_err("the job panicked").is_panic(),
            "expected a panic JoinError"
        );
        assert_eq!(pool.blocking_stats().in_flight, 0);

        let later = tokio::time::timeout(Duration::from_secs(5), pool.spawn_blocking(|| 7))
            .await
            .expect("a later job must be admitted after a panic released the permit");
        assert_eq!(later.await.expect("later job"), 7);
    }

    #[test]
    fn capacity_override_parsing() {
        assert_eq!(parse_blocking_capacity("8"), Ok(nz(8)));
        assert_eq!(parse_blocking_capacity(" 32\n"), Ok(nz(32)));
        assert_eq!(parse_blocking_capacity("0"), Err(CapacityParseError::Zero));
        assert_eq!(parse_blocking_capacity(""), Err(CapacityParseError::Empty));
        assert_eq!(
            parse_blocking_capacity("  "),
            Err(CapacityParseError::Empty)
        );
        assert_eq!(
            parse_blocking_capacity("-1"),
            Err(CapacityParseError::NotANumber("-1".into()))
        );
        assert_eq!(
            parse_blocking_capacity("lots"),
            Err(CapacityParseError::NotANumber("lots".into()))
        );
    }

    #[test]
    fn capacity_resolution_order_and_rejection() {
        let per_pool = per_pool_capacity_env("raft-log-store");
        assert_eq!(per_pool, "FERROSA_BLOCKING_POOL_CAPACITY_RAFT_LOG_STORE");

        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |var: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == var)
                    .map(|(_, v)| v.to_string())
            }
        };
        let default = default_blocking_capacity();

        assert_eq!(resolve_capacity("raft-log-store", env(&[])), default);
        assert_eq!(
            resolve_capacity("raft-log-store", env(&[(BLOCKING_POOL_CAPACITY_ENV, "16")])),
            nz(16)
        );
        assert_eq!(
            resolve_capacity(
                "raft-log-store",
                env(&[
                    ("FERROSA_BLOCKING_POOL_CAPACITY_RAFT_LOG_STORE", "3"),
                    (BLOCKING_POOL_CAPACITY_ENV, "16"),
                ])
            ),
            nz(3),
            "the per-pool override wins"
        );
        assert_eq!(
            resolve_capacity(
                "raft-log-store",
                env(&[
                    ("FERROSA_BLOCKING_POOL_CAPACITY_RAFT_LOG_STORE", "0"),
                    (BLOCKING_POOL_CAPACITY_ENV, "16"),
                ])
            ),
            nz(16),
            "a rejected per-pool override falls through to the global one"
        );
        assert_eq!(
            resolve_capacity(
                "raft-log-store",
                env(&[(BLOCKING_POOL_CAPACITY_ENV, "many")])
            ),
            default,
            "a rejected global override falls through to the default"
        );
    }

    #[test]
    fn default_capacity_is_clamped() {
        let capacity = default_blocking_capacity().get();
        assert!(
            (DEFAULT_MIN_CAPACITY..=DEFAULT_MAX_CAPACITY).contains(&capacity),
            "{capacity}"
        );
    }

    #[test]
    fn reconfiguring_a_registered_pool_is_refused() {
        const POOL: &str = "test-reconfigure";
        configure_blocking_capacity(POOL, nz(4)).expect("fresh pool");
        configure_blocking_capacity(POOL, nz(4)).expect("same capacity is a no-op");
        assert_eq!(
            configure_blocking_capacity(POOL, nz(5)),
            Err(BlockingPoolAlreadyRegistered {
                name: POOL,
                existing: 4,
                requested: 5,
            })
        );
        assert!(blocking_pool_stats().iter().any(|s| s.name == POOL));
    }
}
