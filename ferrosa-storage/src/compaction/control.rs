//! Coordinate compaction admission, cancellation and completion at task boundaries.
//! Correctness: table pauses invalidate stale submissions and finish only after claims release.
//! Last revised: 2026-09-27
//! Last changed: Add table-scoped cancellation and invalidatable submission tickets.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ferrosa_common::{CancelReason, CancelToken};
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::metadata::CompactionTask;
use crate::TableId;

#[derive(Clone, Default)]
pub(crate) struct TaskTracker {
    state: Arc<Mutex<State>>,
    pub(crate) changed: Arc<Notify>,
}

#[derive(Default)]
struct State {
    closed: bool,
    claims: HashSet<String>,
    tasks: HashMap<String, TaskInfo>,
    tables: HashMap<TableId, TableGate>,
}

#[derive(Default)]
struct TableGate {
    invalidated: Arc<AtomicBool>,
    pauses: usize,
}

struct TaskInfo {
    table_id: TableId,
    cancel: CancelToken,
    input_bytes: u64,
}

/// Capture before selecting inputs; pauses permanently invalidate old tickets.
pub(crate) struct SubmissionTicket {
    invalidated: Arc<AtomicBool>,
}

/// Keeps submissions paused until the destructive operation has finished.
/// Dropping a cancelled async caller also releases its pause.
#[must_use = "hold the pause until the table mutation has completed"]
pub struct TableCompactionPause {
    tracker: TaskTracker,
    table_id: TableId,
}

/// Snapshot of an operator cancellation request; completion is asynchronous.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CompactionStopReport {
    pub matched_tasks: usize,
    pub already_cancelled_tasks: usize,
}

impl TaskTracker {
    pub(crate) fn submission_ticket(&self, table_id: &TableId) -> Option<SubmissionTicket> {
        let mut state = self.state.lock();
        if state.closed {
            return None;
        }
        if let Some(gate) = state.tables.get(table_id) {
            return (gate.pauses == 0).then(|| SubmissionTicket {
                invalidated: Arc::clone(&gate.invalidated),
            });
        }
        let gate = TableGate::default();
        let ticket = SubmissionTicket {
            invalidated: Arc::clone(&gate.invalidated),
        };
        state.tables.insert(table_id.clone(), gate);
        Some(ticket)
    }

    pub(crate) fn try_register(
        &self,
        task: &CompactionTask,
        ticket: &SubmissionTicket,
    ) -> Option<CancelToken> {
        let first = task.inputs.first()?;
        let mut state = self.state.lock();
        if state.closed
            || ticket.invalidated.load(Ordering::Acquire)
            || !state.tables.get(&task.table_id).is_some_and(|gate| {
                gate.pauses == 0 && Arc::ptr_eq(&gate.invalidated, &ticket.invalidated)
            })
            || task
                .inputs
                .iter()
                .any(|input| state.claims.contains(&input_key(task, &input.id)))
        {
            return None;
        }
        let cancel = CancelToken::new();
        for input in &task.inputs {
            state.claims.insert(input_key(task, &input.id));
        }
        state.tasks.insert(
            input_key(task, &first.id),
            TaskInfo {
                table_id: task.table_id.clone(),
                cancel: cancel.clone(),
                input_bytes: task
                    .inputs
                    .iter()
                    .fold(0u64, |sum, input| sum.saturating_add(input.size_bytes)),
            },
        );
        Some(cancel)
    }

    pub(crate) fn pause_table(
        &self,
        table_id: &TableId,
        reason: CancelReason,
    ) -> TableCompactionPause {
        let mut state = self.state.lock();
        let gate = state.tables.entry(table_id.clone()).or_default();
        gate.invalidated.store(true, Ordering::Release);
        gate.pauses += 1;
        for task in state
            .tasks
            .values()
            .filter(|task| &task.table_id == table_id)
        {
            task.cancel.cancel(reason);
        }
        TableCompactionPause {
            tracker: self.clone(),
            table_id: table_id.clone(),
        }
    }

    pub(crate) fn release(&self, task: &CompactionTask) {
        let mut state = self.state.lock();
        for input in &task.inputs {
            state.claims.remove(&input_key(task, &input.id));
        }
        if let Some(first) = task.inputs.first() {
            state.tasks.remove(&input_key(task, &first.id));
        }
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) fn request_operator_stop(&self, table_id: Option<&TableId>) -> CompactionStopReport {
        let state = self.state.lock();
        let mut report = CompactionStopReport::default();
        for task in state
            .tasks
            .values()
            .filter(|task| table_id.is_none_or(|table| table == &task.table_id))
        {
            report.matched_tasks += 1;
            report.already_cancelled_tasks += usize::from(task.cancel.is_cancelled());
            task.cancel.cancel(CancelReason::Operator);
        }
        report
    }

    pub(crate) fn cancel_all(&self, reason: CancelReason) {
        let mut state = self.state.lock();
        if reason == CancelReason::Shutdown {
            state.closed = true;
        }
        for task in state.tasks.values() {
            task.cancel.cancel(reason);
        }
    }

    pub(crate) fn cancel_largest_for_disk_reserve(&self) -> Option<u64> {
        let state = self.state.lock();
        if state
            .tasks
            .values()
            .any(|task| task.cancel.reason() == Some(CancelReason::DiskReserve))
        {
            return None;
        }
        let (key, task) = state
            .tasks
            .iter()
            .filter(|(_, task)| !task.cancel.is_cancelled())
            .max_by_key(|(_, task)| task.input_bytes)?;
        task.cancel.cancel(CancelReason::DiskReserve);
        tracing::warn!(task = %key, input_bytes = task.input_bytes,
            "compaction: cancelling largest task to reclaim disk reserve");
        Some(task.input_bytes)
    }

    /// Whether any queued, running or awaiting-finalization task is registered
    /// for `table_id`. A task stays registered until its claim is released, so
    /// `false` means the table's compaction work has fully settled.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn table_has_tasks(&self, table_id: &TableId) -> bool {
        self.state
            .lock()
            .tasks
            .values()
            .any(|task| &task.table_id == table_id)
    }

    #[cfg(test)]
    fn table_gate_count(&self) -> usize {
        self.state.lock().tables.len()
    }
}

impl TableCompactionPause {
    pub(crate) fn is_drained(&self) -> bool {
        !self
            .tracker
            .state
            .lock()
            .tasks
            .values()
            .any(|task| task.table_id == self.table_id)
    }
}

impl Drop for TableCompactionPause {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock();
        if let Some(gate) = state.tables.get_mut(&self.table_id) {
            gate.pauses -= 1;
            if gate.pauses == 0 {
                // Tickets retain the invalidation flag across DROP + re-create.
                state.tables.remove(&self.table_id);
            }
        }
    }
}

fn input_key(task: &CompactionTask, input: &str) -> String {
    format!("{}:{input}", task.table_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::metadata::{CompactionTask, SSTableMetadata};
    use crate::TableId;
    use ferrosa_common::{CancelReason, CancelToken};

    fn task(table: &str) -> CompactionTask {
        CompactionTask {
            inputs: vec![SSTableMetadata {
                id: "1".into(),
                path: "unused".into(),
                size_bytes: 100,
                min_token: 0,
                max_token: 1,
                min_timestamp: 0,
                max_timestamp: 1,
                partition_count: 1,
                legacy_format: false,
            }],
            output_dir: "unused".into(),
            schema: ferrosa_common::schema::TableSchema {
                keyspace: "ks".into(),
                table: table.into(),
                key_type: "UTF8Type".into(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![],
                extensions: Default::default(),
            },
            table_id: TableId::new("ks", table),
            purge: None,
        }
    }

    #[test]
    fn cancel_source_tracker_pause_waits_for_release_and_rejects_stale_tickets() {
        let tracker = TaskTracker::default();
        let work = task("table");
        let old = tracker.submission_ticket(&work.table_id).unwrap();
        let cancel: CancelToken = tracker.try_register(&work, &old).unwrap();
        let pause = tracker.pause_table(&work.table_id, CancelReason::Truncated);
        assert_eq!(cancel.reason(), Some(CancelReason::Truncated));
        assert!(!pause.is_drained());
        assert!(tracker.submission_ticket(&work.table_id).is_none());
        tracker.release(&work);
        assert!(pause.is_drained());
        drop(pause);
        assert!(tracker.try_register(&work, &old).is_none());
        let fresh = tracker.submission_ticket(&work.table_id).unwrap();
        assert!(tracker.try_register(&work, &fresh).is_some());
        tracker.release(&work);
    }

    #[test]
    fn cancel_source_tracker_drop_recreate_rejects_precomputed_work_without_tombstones() {
        let tracker = TaskTracker::default();
        let work = task("table");
        let old = tracker.submission_ticket(&work.table_id).unwrap();
        std::thread::scope(|scope| {
            let (continue_tx, continue_rx) = crossbeam_channel::bounded(0);
            let tracker_ref = &tracker;
            let work_ref = &work;
            let late = scope.spawn(move || {
                continue_rx.recv().unwrap();
                tracker_ref.try_register(work_ref, &old).is_some()
            });
            let pause = tracker.pause_table(&work.table_id, CancelReason::TableDropped);
            assert!(pause.is_drained());
            drop(pause);
            assert_eq!(tracker.table_gate_count(), 0);
            let recreated = tracker.submission_ticket(&work.table_id).unwrap();
            continue_tx.send(()).unwrap();
            assert!(!late.join().unwrap());
            assert!(tracker.try_register(&work, &recreated).is_some());
            tracker.release(&work);
        });
    }

    #[test]
    fn cancel_source_operator_scopes_requests_and_keeps_admission_open() {
        let tracker = TaskTracker::default();
        let first = task("first");
        let other = task("other");
        let first_ticket = tracker.submission_ticket(&first.table_id).unwrap();
        let other_ticket = tracker.submission_ticket(&other.table_id).unwrap();
        let first_cancel = tracker.try_register(&first, &first_ticket).unwrap();
        let other_cancel = tracker.try_register(&other, &other_ticket).unwrap();
        assert_eq!(
            tracker.request_operator_stop(Some(&first.table_id)),
            CompactionStopReport {
                matched_tasks: 1,
                already_cancelled_tasks: 0,
            }
        );
        assert_eq!(first_cancel.reason(), Some(CancelReason::Operator));
        assert!(!other_cancel.is_cancelled());
        other_cancel.cancel(CancelReason::DiskReserve);
        assert_eq!(
            tracker.request_operator_stop(None),
            CompactionStopReport {
                matched_tasks: 2,
                already_cancelled_tasks: 2,
            }
        );
        assert_eq!(other_cancel.reason(), Some(CancelReason::DiskReserve));
        tracker.release(&first);
        assert!(tracker.try_register(&first, &first_ticket).is_some());
        tracker.release(&first);
        tracker.release(&other);
        assert_eq!(
            tracker.request_operator_stop(None),
            CompactionStopReport::default()
        );
    }

    #[test]
    fn cancel_source_tracker_shutdown_rejects_new_and_precomputed_work() {
        let tracker = TaskTracker::default();
        let work = task("table");
        let ticket = tracker.submission_ticket(&work.table_id).unwrap();
        let cancel = tracker.try_register(&work, &ticket).unwrap();
        tracker.cancel_all(CancelReason::Shutdown);
        assert_eq!(cancel.reason(), Some(CancelReason::Shutdown));
        tracker.release(&work);
        assert!(tracker.submission_ticket(&work.table_id).is_none());
        assert!(tracker.try_register(&work, &ticket).is_none());
    }

    #[test]
    fn cancel_source_tracker_ticket_cannot_admit_another_table() {
        let tracker = TaskTracker::default();
        let work = task("table");
        let other = task("other");
        let ticket = tracker.submission_ticket(&work.table_id).unwrap();
        assert!(tracker.try_register(&other, &ticket).is_none());
        let _other_ticket = tracker.submission_ticket(&other.table_id).unwrap();
        assert!(tracker.try_register(&other, &ticket).is_none());
    }

    #[test]
    fn cancel_source_tracker_nested_pauses_keep_other_tables_running() {
        let tracker = TaskTracker::default();
        let work = task("table");
        let other = task("other");
        let ticket = tracker.submission_ticket(&work.table_id).unwrap();
        let other_ticket = tracker.submission_ticket(&other.table_id).unwrap();
        let cancel = tracker.try_register(&work, &ticket).unwrap();
        let other_cancel = tracker.try_register(&other, &other_ticket).unwrap();
        let first = tracker.pause_table(&work.table_id, CancelReason::TableDropped);
        let second = tracker.pause_table(&work.table_id, CancelReason::Truncated);
        assert!(cancel.is_cancelled());
        assert!(!other_cancel.is_cancelled());
        tracker.release(&work);
        assert!(second.is_drained());
        drop(first);
        assert!(tracker.submission_ticket(&work.table_id).is_none());
        drop(second);
        assert!(tracker.submission_ticket(&work.table_id).is_some());
        tracker.release(&other);
    }
}
