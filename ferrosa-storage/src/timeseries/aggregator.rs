//! TimeSeriesAggregator (WriteObserver) and ConsolidationWorker.
//!
//! The aggregator inserts into ring buffers inline on the write path and
//! sends consolidation tasks to an async worker via a bounded channel.

use crate::commitlog::config::TableId;

use super::ring::RingEntry;

/// A task sent from the inline write path to the async consolidation worker.
#[derive(Debug, Clone)]
pub enum ConsolidationTask {
    /// Normal boundary crossing -- window data copied from ring.
    BoundaryCrossed {
        table_id: TableId,
        partition_key: Vec<u8>,
        window_entries: Vec<RingEntry>,
        window_start_ts: i64,
    },
    /// Late data detected -- requires disk read to reconstruct window.
    LateData {
        table_id: TableId,
        partition_key: Vec<u8>,
        window_start_ts: i64,
        late_timestamp: i64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    #[test]
    fn consolidation_task_boundary_crossed() {
        let task = ConsolidationTask::BoundaryCrossed {
            table_id: TableId::new("ks", "sensor_1s"),
            partition_key: b"sensor-1".to_vec(),
            window_entries: vec![RingEntry {
                timestamp: 1_000_000,
                values: SmallVec::from_slice(&[42.0]),
            }],
            window_start_ts: 0,
        };

        if let ConsolidationTask::BoundaryCrossed {
            table_id,
            partition_key,
            window_entries,
            window_start_ts,
        } = &task
        {
            assert_eq!(table_id.keyspace, "ks");
            assert_eq!(table_id.table, "sensor_1s");
            assert_eq!(partition_key, b"sensor-1");
            assert_eq!(window_entries.len(), 1);
            assert_eq!(*window_start_ts, 0);
        } else {
            panic!("expected BoundaryCrossed");
        }
    }

    #[test]
    fn consolidation_task_late_data() {
        let task = ConsolidationTask::LateData {
            table_id: TableId::new("ks", "sensor_1s"),
            partition_key: b"sensor-1".to_vec(),
            window_start_ts: 0,
            late_timestamp: 5_000_000,
        };

        if let ConsolidationTask::LateData { late_timestamp, .. } = &task {
            assert_eq!(*late_timestamp, 5_000_000);
        } else {
            panic!("expected LateData");
        }
    }
}
