//! Tests for [`super`]: the flusher and the maintenance loop are restarted
//! within their intensity, observable while impaired, and escalate past it.
//!
//! Background: 2026-10-02, node2. `storage-flush` panicked in the SSTable
//! encoder, the maintenance loop logged `flush task panicked e=channel closed`
//! once, and from then on the node flushed, compacted and persisted nothing
//! while `/readyz` answered 200 (t_7681b32b).

use super::*;
use std::sync::atomic::Ordering;

const PERIOD: Duration = Duration::from_secs(3600);

fn intensity(max_restarts: u32) -> RestartIntensity {
    RestartIntensity {
        max_restarts,
        period: PERIOD,
    }
}

/// A supervisor whose escalation only counts, so a test can observe it.
fn flush_supervisor(
    max_restarts: u32,
    stall_deadline: Duration,
) -> (FlushSupervisor, Arc<SupervisionStatus>, Arc<AtomicU64>) {
    let status = Arc::new(SupervisionStatus::default());
    let escalations = Arc::new(AtomicU64::new(0));
    let supervisor = FlushSupervisor::new(
        status.clone(),
        intensity(max_restarts),
        stall_deadline,
        Arc::new(EscalationPolicy::Record(escalations.clone())),
    );
    (supervisor, status, escalations)
}

fn ok() -> ferrosa_common::Result<()> {
    Ok(())
}

// ---------------------------------------------------------------------------
// (a) a panicking flush is restarted and flushing resumes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_panicking_flush_is_restarted_and_flushing_resumes() {
    let (mut flusher, status, escalations) = flush_supervisor(3, Duration::from_secs(30));

    let run = flusher
        .run("storage-flush", || panic!("encoder exploded"))
        .await;
    assert_eq!(run, FlushRun::Panicked);
    assert_eq!(status.failures(Child::StorageFlush, FailureKind::Panic), 1);
    assert_eq!(
        status.restarts(Child::StorageFlush),
        1,
        "a panic is followed by a restart"
    );

    let run = flusher.run("storage-flush", ok).await;
    assert_eq!(run, FlushRun::Flushed, "the next attempt runs and flushes");
    assert!(
        status.impaired().is_empty(),
        "a successful flush clears the impaired state"
    );
    assert_eq!(escalations.load(Ordering::SeqCst), 0, "within intensity");
}

// ---------------------------------------------------------------------------
// (c) the impaired state is observable: health report and metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_panicked_flush_is_reported_impaired_with_its_cause() {
    let (mut flusher, status, _) = flush_supervisor(3, Duration::from_secs(30));
    flusher
        .run("storage-flush", || panic!("complex col_idx 8 path=None"))
        .await;

    let impaired = status.impaired();
    assert_eq!(impaired.len(), 1, "{impaired:?}");
    assert_eq!(impaired[0].0, "storage_flush");
    assert!(
        impaired[0].1.contains("complex col_idx 8 path=None"),
        "the panic message is the reason: {impaired:?}"
    );

    let mut metrics = String::new();
    status.render_prometheus(&mut metrics);
    assert!(
        metrics.contains("ferrosa_supervised_task_up{task=\"storage_flush\"} 0\n"),
        "{metrics}"
    );
    assert!(
        metrics.contains(
            "ferrosa_supervised_task_failures_total{task=\"storage_flush\",kind=\"panic\"} 1\n"
        ),
        "{metrics}"
    );
    assert!(
        metrics.contains("ferrosa_supervised_task_restarts_total{task=\"storage_flush\"} 1\n"),
        "{metrics}"
    );
    assert!(
        metrics.contains("ferrosa_supervised_task_up{task=\"maintenance_loop\"} 1\n"),
        "every supervised task is exported, healthy ones as 1: {metrics}"
    );
}

/// A flush that RETURNS an error is impaired until one succeeds, but is not a
/// crash: it never counts toward the restart intensity.
#[tokio::test]
async fn a_failing_flush_is_impaired_until_one_succeeds_and_never_escalates() {
    let (mut flusher, status, escalations) = flush_supervisor(1, Duration::from_secs(30));
    for _ in 0..10 {
        let run = flusher
            .run("storage-flush", || {
                Err(ferrosa_common::Error::Io(std::io::Error::other(
                    "disk full",
                )))
            })
            .await;
        assert_eq!(run, FlushRun::Failed);
    }
    assert_eq!(status.failures(Child::StorageFlush, FailureKind::Error), 10);
    assert!(status.impaired()[0].1.contains("disk full"));
    assert_eq!(escalations.load(Ordering::SeqCst), 0);

    flusher.run("storage-flush", ok).await;
    assert!(status.impaired().is_empty());
}

// ---------------------------------------------------------------------------
// (d) exceeding the restart intensity escalates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exceeding_the_flush_restart_intensity_escalates() {
    let (mut flusher, _status, escalations) = flush_supervisor(3, Duration::from_secs(30));
    for attempt in 1..=3 {
        flusher
            .run("storage-flush", || panic!("deterministic"))
            .await;
        assert_eq!(
            escalations.load(Ordering::SeqCst),
            0,
            "{attempt} panics are within an intensity of 3"
        );
    }
    flusher
        .run("storage-flush", || panic!("deterministic"))
        .await;
    assert_eq!(
        escalations.load(Ordering::SeqCst),
        1,
        "the 4th panic inside the period exceeds max_restarts=3"
    );
}

#[test]
fn failures_older_than_the_period_leave_the_window() {
    let mut window = IntensityWindow::new(RestartIntensity {
        max_restarts: 2,
        period: Duration::from_secs(60),
    });
    let start = Instant::now();
    assert_eq!(window.record(start), 1);
    assert_eq!(window.record(start + Duration::from_secs(10)), 2);
    assert_eq!(
        window.record(start + Duration::from_secs(65)),
        2,
        "the failure at t=0 aged out of the 60s period"
    );
    assert_eq!(window.record(start + Duration::from_secs(66)), 3);
}

// ---------------------------------------------------------------------------
// a hung flush must not wedge the maintenance loop, and must be reported
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_hung_flush_is_reported_stalled_without_blocking_the_caller() {
    let (mut flusher, status, _) = flush_supervisor(5, Duration::from_millis(50));
    let (release, gate) = std::sync::mpsc::channel::<()>();

    let started = Instant::now();
    let run = flusher
        .run("storage-flush", move || {
            gate.recv().expect("test releases the flush");
            Ok(())
        })
        .await;
    assert_eq!(run, FlushRun::Stalled);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the caller gets control back at the stall deadline"
    );
    assert!(flusher.is_busy());
    assert_eq!(status.failures(Child::StorageFlush, FailureKind::Stall), 1);
    assert!(
        status.impaired()[0].1.contains("stalled"),
        "{:?}",
        status.impaired()
    );

    let spawned_while_busy = Arc::new(AtomicU64::new(0));
    let counter = spawned_while_busy.clone();
    let run = flusher
        .run("storage-flush", move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
    assert_eq!(
        run,
        FlushRun::Busy,
        "no second flush thread while one is in flight"
    );

    release.send(()).expect("release the hung flush");
    // Bounded wait for the released thread to report.
    for _ in 0..200 {
        let run = flusher.run("storage-flush", ok).await;
        if run != FlushRun::Busy {
            assert_eq!(run, FlushRun::Flushed);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!flusher.is_busy());
    assert_eq!(spawned_while_busy.load(Ordering::SeqCst), 0);
    assert!(
        status.impaired().is_empty(),
        "the hung flush finished and a new one succeeded"
    );
}

#[tokio::test]
async fn a_flush_hung_for_good_escalates() {
    let (mut flusher, status, escalations) = flush_supervisor(2, Duration::from_millis(20));
    let (_never_released, gate) = std::sync::mpsc::channel::<()>();
    let run = flusher
        .run("storage-flush", move || {
            // Returns only when the test ends and drops the sender.
            let _ = gate.recv();
            Ok(())
        })
        .await;
    assert_eq!(run, FlushRun::Stalled);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(flusher.run("storage-flush", ok).await, FlushRun::Busy);
    assert!(
        status.failures(Child::StorageFlush, FailureKind::Stall) >= 3,
        "one stall is recorded per elapsed deadline"
    );
    assert_eq!(
        escalations.load(Ordering::SeqCst),
        1,
        "a flush that never returns exceeds the intensity and escalates once"
    );
}

// ---------------------------------------------------------------------------
// the maintenance loop itself is supervised
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_panicking_maintenance_loop_is_restarted_then_escalates() {
    let status = Arc::new(SupervisionStatus::default());
    let escalations = Arc::new(AtomicU64::new(0));
    let starts = Arc::new(AtomicU64::new(0));
    let counter = starts.clone();
    supervise(
        Child::MaintenanceLoop,
        status.clone(),
        intensity(2),
        Arc::new(EscalationPolicy::Record(escalations.clone())),
        move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            async move { panic!("maintenance tick {n} panicked") }
        },
    )
    .await;

    assert_eq!(
        starts.load(Ordering::SeqCst),
        3,
        "started, then restarted twice"
    );
    assert_eq!(status.restarts(Child::MaintenanceLoop), 2);
    assert_eq!(
        status.failures(Child::MaintenanceLoop, FailureKind::Panic),
        3
    );
    assert_eq!(escalations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_maintenance_loop_that_returns_is_a_failure() {
    let status = Arc::new(SupervisionStatus::default());
    let escalations = Arc::new(AtomicU64::new(0));
    supervise(
        Child::MaintenanceLoop,
        status.clone(),
        intensity(0),
        Arc::new(EscalationPolicy::Record(escalations.clone())),
        || async {},
    )
    .await;
    assert_eq!(
        status.failures(Child::MaintenanceLoop, FailureKind::Exit),
        1
    );
    assert_eq!(escalations.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// (d) production escalation: sync the commit log, then crash with context
// ---------------------------------------------------------------------------

/// Run the real `AbortProcess` escalation in a child test process: it must
/// sync the commit log, print a FATAL line naming the task and its last
/// failure, and die by SIGABRT (a crash, which `KeepAlive { Crashed }`
/// restarts — a clean `exit(1)` is not restarted on the native cluster).
#[test]
fn production_escalation_syncs_the_commit_log_and_aborts_with_context() {
    const CHILD_ENV: &str = "FERROSA_TEST_SUPERVISOR_ABORT_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(
            ferrosa_storage::StorageEngine::new(
                ferrosa_storage::StorageEngineConfig::test_config(dir.path()),
                None,
            )
            .expect("engine"),
        );
        let status = Arc::new(SupervisionStatus::default());
        let mut flusher = FlushSupervisor::new(
            status,
            intensity(0),
            Duration::from_secs(30),
            Arc::new(EscalationPolicy::AbortProcess { engine }),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(flusher.run("storage-flush", || panic!("poisoned memtable")));
        unreachable!("AbortProcess must not return");
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "supervisor::tests::production_escalation_syncs_the_commit_log_and_aborts_with_context",
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .output()
        .expect("launch the escalation child");
    let stderr = String::from_utf8_lossy(&output.stderr);
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        output.status.signal(),
        Some(libc_sigabrt()),
        "escalation must abort (SIGABRT): {:?}\n{stderr}",
        output.status
    );
    let fatal = stderr
        .lines()
        .find(|line| line.starts_with("FATAL: supervised task"))
        .unwrap_or_else(|| panic!("no FATAL line:\n{stderr}"));
    assert!(fatal.contains("task=storage_flush"), "{fatal}");
    assert!(fatal.contains("poisoned memtable"), "{fatal}");
    assert!(fatal.contains("commit_log_sync=ok"), "{fatal}");
}

fn libc_sigabrt() -> i32 {
    6
}

// ---------------------------------------------------------------------------
// P0-6 (t_88479cda): the commit log's fsync thread is supervised
// ---------------------------------------------------------------------------

use ferrosa_storage::commitlog::SyncHealthSnapshot;

fn healthy_sync() -> SyncHealthSnapshot {
    SyncHealthSnapshot {
        has_sync_thread: true,
        dead: false,
        failing: false,
        panics: 0,
        sync_failures: 0,
        restarts: 0,
        unsynced_for: None,
        stall_deadline: Duration::from_secs(2),
        last_failure: None,
    }
}

/// A sync thread whose health the test sets; a restart revives it.
struct FakeSync {
    health: ArcSwap<SyncHealthSnapshot>,
    restarts: AtomicU64,
}

impl FakeSync {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            health: ArcSwap::from_pointee(healthy_sync()),
            restarts: AtomicU64::new(0),
        })
    }

    fn set(&self, update: impl FnOnce(&mut SyncHealthSnapshot)) {
        let mut next = self.health.load_full().as_ref().clone();
        update(&mut next);
        self.health.store(Arc::new(next));
    }
}

impl CommitLogSyncTarget for FakeSync {
    fn sync_health(&self) -> SyncHealthSnapshot {
        self.health.load_full().as_ref().clone()
    }

    fn restart_sync(&self) -> ferrosa_common::Result<bool> {
        if !self.health.load().dead {
            return Ok(false);
        }
        self.restarts.fetch_add(1, Ordering::SeqCst);
        self.set(|h| {
            h.dead = false;
            h.restarts += 1;
        });
        Ok(true)
    }
}

use arc_swap::ArcSwap;

fn sync_supervisor<T: CommitLogSyncTarget>(
    target: Arc<T>,
    max_restarts: u32,
) -> (
    CommitLogSyncSupervisor<T>,
    Arc<SupervisionStatus>,
    Arc<AtomicU64>,
) {
    let status = Arc::new(SupervisionStatus::default());
    let escalations = Arc::new(AtomicU64::new(0));
    let supervisor = CommitLogSyncSupervisor::new(
        target,
        status.clone(),
        intensity(max_restarts),
        Arc::new(EscalationPolicy::Record(escalations.clone())),
    );
    (supervisor, status, escalations)
}

fn metrics(status: &SupervisionStatus) -> String {
    let mut out = String::new();
    status.render_prometheus(&mut out);
    out
}

#[test]
fn a_dead_commit_log_sync_thread_is_reported_restarted_and_recovers() {
    let target = FakeSync::new();
    let (mut supervisor, status, escalations) = sync_supervisor(target.clone(), 3);
    supervisor.check();
    assert!(status.impaired().is_empty(), "healthy at start");

    target.set(|h| {
        h.dead = true;
        h.panics = 1;
        h.unsynced_for = Some(Duration::from_millis(5));
        h.last_failure = Some("commitlog-periodic-sync panicked: boom".into());
    });
    supervisor.check();

    assert_eq!(target.restarts.load(Ordering::SeqCst), 1, "restarted");
    assert_eq!(status.failures(Child::CommitLogSync, FailureKind::Panic), 1);
    assert_eq!(status.restarts(Child::CommitLogSync), 1);
    let impaired = status.impaired();
    assert_eq!(impaired.len(), 1, "{impaired:?}");
    assert_eq!(impaired[0].0, "commitlog_sync");
    assert!(impaired[0].1.contains("boom"), "{impaired:?}");
    let text = metrics(&status);
    assert!(
        text.contains("ferrosa_supervised_task_up{task=\"commitlog_sync\"} 0\n"),
        "readiness and the metric flip while the restarted thread has not synced: {text}"
    );
    assert!(
        text.contains(
            "ferrosa_supervised_task_failures_total{task=\"commitlog_sync\",kind=\"panic\"} 1\n"
        ),
        "{text}"
    );

    // The restarted thread syncs what the dead one left.
    target.set(|h| h.unsynced_for = None);
    supervisor.check();
    assert!(status.impaired().is_empty(), "recovered");
    assert!(metrics(&status).contains("ferrosa_supervised_task_up{task=\"commitlog_sync\"} 1\n"));
    assert_eq!(escalations.load(Ordering::SeqCst), 0);
}

#[test]
fn a_stalled_commit_log_sync_counts_one_stall_per_deadline_and_escalates() {
    let target = FakeSync::new();
    let (mut supervisor, status, escalations) = sync_supervisor(target.clone(), 2);

    target.set(|h| h.unsynced_for = Some(Duration::from_millis(2500)));
    supervisor.check();
    assert_eq!(status.failures(Child::CommitLogSync, FailureKind::Stall), 1);
    assert!(
        status.impaired()[0]
            .1
            .contains("past the 2000ms stall deadline"),
        "{:?}",
        status.impaired()
    );
    supervisor.check();
    assert_eq!(
        status.failures(Child::CommitLogSync, FailureKind::Stall),
        1,
        "the same deadline is not counted twice"
    );
    assert_eq!(
        target.restarts.load(Ordering::SeqCst),
        0,
        "a stall is not a death"
    );

    target.set(|h| h.unsynced_for = Some(Duration::from_millis(6100)));
    supervisor.check();
    assert_eq!(status.failures(Child::CommitLogSync, FailureKind::Stall), 3);
    assert_eq!(
        escalations.load(Ordering::SeqCst),
        1,
        "three stalls exceed max_restarts=2"
    );
    assert!(
        !status.impaired().is_empty(),
        "an escalated child stays impaired"
    );
}

#[test]
fn commit_log_sync_deaths_past_the_intensity_escalate_without_a_restart() {
    let target = FakeSync::new();
    let (mut supervisor, _status, escalations) = sync_supervisor(target.clone(), 1);
    for death in 1..=2u64 {
        target.set(|h| {
            h.dead = true;
            h.panics = death;
        });
        supervisor.check();
    }
    assert_eq!(escalations.load(Ordering::SeqCst), 1);
    assert_eq!(
        target.restarts.load(Ordering::SeqCst),
        1,
        "the death that exceeds the intensity escalates instead of restarting"
    );
}

#[test]
fn a_failing_commit_log_fsync_is_impaired_until_one_succeeds() {
    let target = FakeSync::new();
    let (mut supervisor, status, escalations) = sync_supervisor(target.clone(), 1);
    target.set(|h| {
        h.failing = true;
        h.sync_failures = 4;
        h.unsynced_for = Some(Duration::from_millis(20));
        h.last_failure = Some("fsync failed: EIO".into());
    });
    supervisor.check();
    assert_eq!(status.failures(Child::CommitLogSync, FailureKind::Error), 1);
    assert!(status.impaired()[0].1.contains("EIO"));
    assert_eq!(
        escalations.load(Ordering::SeqCst),
        0,
        "an error alone is not a crash"
    );

    target.set(|h| {
        h.failing = false;
        h.unsynced_for = None;
    });
    supervisor.check();
    assert!(status.impaired().is_empty());
}

/// End to end against the real commit log in its production mode
/// (Periodic): a panicked sync thread refuses writes, the supervisor reports
/// it and restarts it, and acks resume with every acked write on disk.
#[test]
fn the_supervisor_restarts_a_real_dead_commit_log_sync_thread() {
    use ferrosa_storage::commitlog::{CommitLog, CommitLogConfig, Mutation, SyncStrategyConfig};
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(
        CommitLog::new(CommitLogConfig {
            segment_size: 1024 * 1024,
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: Duration::from_millis(5),
            },
            ..CommitLogConfig::test_config(dir.path())
        })
        .unwrap(),
    );
    let mutation = |table: &str| {
        Mutation::new(
            "ks".into(),
            table.into(),
            ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"k".to_vec())),
            Vec::new(),
            1,
        )
    };
    let wait = |what: &str, condition: &dyn Fn() -> bool| {
        let started = Instant::now();
        while !condition() {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "timed out: {what}"
            );
            std::thread::yield_now();
        }
    };
    let (mut supervisor, status, escalations) = sync_supervisor(log.clone(), 3);

    log.append(&mutation("before")).unwrap();
    log.inject_sync_panic();
    log.append(&mutation("wakes_the_panic")).unwrap();
    wait("the sync thread to die", &|| log.sync_health().dead);
    let refused = log.append(&mutation("while_dead"));
    assert!(
        matches!(
            refused,
            Err(ferrosa_common::Error::CommitLogNotDurable { .. })
        ),
        "{refused:?}"
    );

    supervisor.check();
    assert_eq!(status.failures(Child::CommitLogSync, FailureKind::Panic), 1);
    assert_eq!(status.restarts(Child::CommitLogSync), 1);
    assert!(
        !log.sync_health().dead,
        "the supervisor restarted the thread"
    );

    log.append(&mutation("after_restart"))
        .expect("acks resume after the supervised restart");
    wait("the restarted thread to sync", &|| {
        log.sync_health().unsynced_for.is_none()
    });
    supervisor.check();
    assert!(status.impaired().is_empty(), "{:?}", status.impaired());
    assert_eq!(escalations.load(Ordering::SeqCst), 0);
}
