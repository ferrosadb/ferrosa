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
