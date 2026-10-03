//! Module: the storage maintenance loop.
//! Responsibility: periodic and urgent memtable flushes, compaction polling,
//!   commit-log GC, schema persistence and S3 sync, run under supervision.
//! Correctness: the loop is a child of `supervisor::supervise` (restarted if it
//!   panics or returns) and every flush is a `FlushSupervisor` attempt (a panic
//!   is restarted on the next tick, a hang is reported as a stall and no longer
//!   wedges the loop). State that must survive a loop restart, the last
//!   persisted schema version, lives in the context, not the loop.
//! Last revised: 2026-10-03
//! Last changed: Extracted from `main` and supervised (t_7681b32b). Before, a
//!   flush panic was one ERROR line, a hung flush blocked every arm of the loop
//!   forever, the loop's own JoinHandle was dropped, and `/readyz` said 200.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::supervisor::{
    self, EscalationPolicy, FlushRun, FlushSupervisor, RestartIntensity, SupervisionStatus,
};

/// Tick periods for the maintenance loop.
#[derive(Clone, Copy, Debug)]
pub struct MaintenanceIntervals {
    pub flush: Duration,
    pub urgent_flush: Duration,
    pub urgent_s3_sync: Duration,
}

/// Everything the maintenance loop needs; cloned into each loop incarnation.
#[derive(Clone)]
pub struct MaintenanceContext {
    pub engine: Arc<ferrosa_storage::StorageEngine>,
    pub schema: Arc<ferrosa_schema::Schema>,
    pub data_dir: String,
    pub intervals: MaintenanceIntervals,
    /// The schema version last persisted (or present at startup). Shared
    /// across loop restarts so a restart does not skip a pending persist.
    pub last_persisted_schema: Arc<ArcSwap<uuid::Uuid>>,
    pub supervision: Arc<SupervisionStatus>,
    pub intensity: RestartIntensity,
    pub flush_stall_deadline: Duration,
    pub escalation: Arc<EscalationPolicy>,
}

/// Run the maintenance loop under supervision until the process exits.
pub async fn run_supervised(ctx: MaintenanceContext) {
    supervisor::supervise(
        supervisor::Child::MaintenanceLoop,
        ctx.supervision.clone(),
        ctx.intensity,
        ctx.escalation.clone(),
        move || run_maintenance_loop(ctx.clone()),
    )
    .await;
}

/// One incarnation of the loop. Never returns on its own; the supervisor
/// treats a return as a failure.
async fn run_maintenance_loop(ctx: MaintenanceContext) {
    let mut flusher = FlushSupervisor::new(
        ctx.supervision.clone(),
        ctx.intensity,
        ctx.flush_stall_deadline,
        ctx.escalation.clone(),
    );
    let has_s3 = ctx.engine.has_s3();
    let mut flush_interval = tokio::time::interval(ctx.intervals.flush);
    let mut urgent_flush_interval = tokio::time::interval(ctx.intervals.urgent_flush);
    let mut urgent_s3_sync_interval = tokio::time::interval(ctx.intervals.urgent_s3_sync);
    let mut compact_interval = tokio::time::interval(Duration::from_secs(10));
    // Persist schema snapshot + flush all memtables to S3 every 30s.
    let mut schema_sync_interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = flush_interval.tick() => periodic_flush(&ctx, &mut flusher, has_s3).await,
            _ = compact_interval.tick() => {
                ctx.engine.poll_compactions().await;
            }
            _ = ctx.engine.wait_for_compaction_retry_wakeup() => {
                ctx.engine.poll_compactions().await;
            }
            _ = urgent_flush_interval.tick() => {
                // Leave the request pending while a stalled flush is in flight;
                // the periodic tick settles it and the next urgent tick runs.
                if !flusher.is_busy() && ctx.engine.take_flush_request() {
                    let engine = ctx.engine.clone();
                    flusher
                        .run("storage-flush-urgent", move || engine.flush_if_needed())
                        .await;
                }
            }
            _ = urgent_s3_sync_interval.tick(), if has_s3 => {
                if ctx.engine.take_s3_sync_request() {
                    let engine = ctx.engine.clone();
                    spawn_s3_sync("s3-sync-urgent", move || async move {
                        match engine.sync_sstables_to_s3().await {
                            Ok(n) => tracing::info!(
                                count = n,
                                "urgent S3 SSTable sync completed after write backpressure"
                            ),
                            Err(e) => tracing::warn!(%e, "urgent S3 SSTable sync failed"),
                        }
                    });
                }
            }
            _ = schema_sync_interval.tick() => schema_sync(&ctx, &mut flusher, has_s3).await,
        }
    }
}

/// Periodic flush, then S3 sync and commit-log GC.
async fn periodic_flush(ctx: &MaintenanceContext, flusher: &mut FlushSupervisor, has_s3: bool) {
    // Run flush on a dedicated OS thread so SSTable encoding, compression, and
    // disk I/O do not consume Tokio's shared blocking pool.
    let engine = ctx.engine.clone();
    let run = flusher
        .run("storage-flush", move || engine.flush_if_needed())
        .await;
    if matches!(run, FlushRun::Busy | FlushRun::Stalled) {
        // A hung flush may hold storage locks; GC and S3 sync could block on
        // them and wedge this loop. They run again once the flush settles.
        return;
    }

    // After flush, sync new SSTables to S3 on a dedicated thread so HTTP
    // uploads don't starve the runtime that handles Raft RPCs.
    if has_s3 {
        let engine = ctx.engine.clone();
        spawn_s3_sync("s3-sync", move || async move {
            match engine.sync_sstables_to_s3().await {
                Ok(n) if n > 0 => tracing::info!(count = n, "synced SSTables to S3"),
                Err(e) => tracing::warn!(%e, "S3 SSTable sync failed"),
                _ => {}
            }
        });
    }

    // Commit log GC: discard segments with no remaining dirty tables.
    match ctx.engine.discard_completed_commit_log_segments() {
        Ok(n) if n > 0 => tracing::debug!(segments = n, "commit log GC cleaned up segments"),
        Err(e) => tracing::warn!(%e, "commit log GC failed"),
        _ => {}
    }
}

/// Flush everything, then persist the schema snapshot locally and to S3, when
/// the schema changed since the last persist.
async fn schema_sync(ctx: &MaintenanceContext, flusher: &mut FlushSupervisor, has_s3: bool) {
    let snap = ctx.schema.snapshot();
    if !crate::should_persist_schema(snap.version, **ctx.last_persisted_schema.load()) {
        return;
    }
    // Flush all memtables before persisting schema so SSTables on disk match
    // the schema snapshot.
    let engine = ctx.engine.clone();
    let run = flusher
        .run("storage-schema-flush", move || engine.flush_all())
        .await;
    if run != FlushRun::Flushed {
        // Don't persist schema without data; retry next tick. The flush
        // supervisor already reported the failure.
        tracing::warn!(outcome = ?run, "pre-schema-persist flush did not complete; skipping schema persist");
        return;
    }

    // Always persist schema locally for restart recovery.
    if let Err(e) = crate::persist_schema_locally(Path::new(&ctx.data_dir), &ctx.schema) {
        tracing::error!(%e, "failed to persist authoritative local schema snapshot");
    }

    if has_s3 {
        let engine = ctx.engine.clone();
        let schema = ctx.schema.clone();
        spawn_s3_sync("s3-schema-sync", move || async move {
            match engine.sync_sstables_to_s3().await {
                Ok(n) if n > 0 => tracing::info!(count = n, "pre-schema-persist S3 sync"),
                Err(e) => tracing::warn!(%e, "pre-schema-persist S3 sync failed"),
                _ => {}
            }
            crate::persist_schema_to_s3(&engine, &schema).await;
        });
    }

    ctx.last_persisted_schema.store(Arc::new(snap.version));
}

/// Run one S3 sync round on its own thread with a current-thread runtime.
///
/// A failure to start the round is logged and skipped; the next tick retries.
/// Before, a spawn failure was discarded (`let _ =`) and a runtime build
/// failure panicked the thread, both invisible outside stderr.
fn spawn_s3_sync<F, Fut>(thread_name: &'static str, work: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    let spawned = std::thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(work()),
                Err(e) => tracing::error!(
                    thread = thread_name,
                    %e,
                    "could not build the S3 sync runtime; this round is skipped and the next tick retries"
                ),
            }
        });
    if let Err(e) = spawned {
        tracing::error!(
            thread = thread_name,
            %e,
            "could not spawn the S3 sync thread; this round is skipped and the next tick retries"
        );
    }
}
