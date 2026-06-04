//! The quarantine → targeted-refill port.
//!
//! When the controller quarantines a corrupt generation, the rows in that
//! generation are gone locally. To refill them *promptly* from a healthy
//! replica — rather than waiting for the next full anti-entropy cycle — the
//! controller calls a [`RepairTrigger`]. This is a **port** (FMEA #10): the
//! storage crate sits below the cluster layer in the dependency graph, so it
//! cannot drive a real cross-node repair itself. The real implementation lives
//! in the binary (it owns the `AutoRepairScheduler` / `RepairCoordinator`); the
//! storage crate only depends on this trait.
//!
//! The default implementation is [`NoopRepairTrigger`], which keeps existing
//! tests and single-node deployments working without any cluster wiring: a
//! single-node engine never quarantines (FMEA #1), so a no-op refill is never
//! actually reached for it; on a cluster, the binary supplies the real trigger.

use crate::TableId;

/// Port the controller uses to request a prompt, targeted refill of the token
/// ranges whose data was just quarantined.
///
/// `request_refill` must be cheap and non-blocking — it *schedules* a repair,
/// it does not run one. The real impl typically enqueues a targeted
/// `repair_table` over the affected ranges on the cluster's repair scheduler.
/// Failure to enqueue must be logged loudly by the implementation; the periodic
/// repair cycle is the backstop (design Q3 / FMEA #10).
pub trait RepairTrigger: Send + Sync {
    /// Schedule a targeted refill of `table`'s `ranges` (`[start, end)` token
    /// bounds) from a healthy replica. Called once per successful quarantine.
    fn request_refill(&self, table: &TableId, ranges: &[(i64, i64)]);
}

/// The default, do-nothing [`RepairTrigger`].
///
/// Used when no cluster-layer trigger is wired (single-node, and existing
/// tests). It logs at `debug` so a misconfigured cluster deployment — one that
/// quarantines but never wired a real trigger — is still observable, without
/// being noisy on single-node where this path is never reached.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopRepairTrigger;

impl RepairTrigger for NoopRepairTrigger {
    fn request_refill(&self, table: &TableId, ranges: &[(i64, i64)]) {
        tracing::debug!(
            keyspace = %table.keyspace,
            table = %table.table,
            ranges = ?ranges,
            "self-heal: no-op RepairTrigger — refill not scheduled (no cluster trigger wired); \
             periodic repair cycle is the backstop"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// Recorded `(table, ranges)` of the most recent refill request.
    type RecordedRefill = Option<(TableId, Vec<(i64, i64)>)>;

    /// A trigger that records every refill request for assertions.
    #[derive(Default)]
    struct RecordingTrigger {
        calls: AtomicUsize,
        last: Mutex<RecordedRefill>,
    }

    impl RepairTrigger for RecordingTrigger {
        fn request_refill(&self, table: &TableId, ranges: &[(i64, i64)]) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last.lock().unwrap() = Some((table.clone(), ranges.to_vec()));
        }
    }

    #[test]
    fn noop_trigger_is_inert() {
        let t = NoopRepairTrigger;
        // Must not panic and must accept any input shape.
        t.request_refill(&TableId::new("ks", "t"), &[(i64::MIN, i64::MAX)]);
        t.request_refill(&TableId::new("ks", "t"), &[]);
    }

    #[test]
    fn trigger_is_object_safe_and_records_request() {
        let rec = Arc::new(RecordingTrigger::default());
        let dynamic: Arc<dyn RepairTrigger> = rec.clone();
        dynamic.request_refill(&TableId::new("ks", "t"), &[(10, 20), (30, 40)]);
        assert_eq!(rec.calls.load(Ordering::SeqCst), 1);
        let last = rec.last.lock().unwrap().clone().unwrap();
        assert_eq!(last.0, TableId::new("ks", "t"));
        assert_eq!(last.1, vec![(10, 20), (30, 40)]);
    }
}
