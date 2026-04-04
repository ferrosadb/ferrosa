//! Stub virtual tables for future observability features.
//!
//! These tables return empty results but ensure the schema exists and
//! CQL queries against them do not fail. Full implementations are
//! tracked in their respective task IDs.

use ferrosa_common::DataType;
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Generic stub virtual table that returns empty rows.
///
/// Used for virtual tables whose full implementation is deferred.
pub struct StubVirtualTable {
    table_name: &'static str,
    keyspace: &'static str,
    columns: Vec<VirtualColumnDef>,
    pk_columns: Vec<usize>,
}

impl StubVirtualTable {
    fn new(
        table_name: &'static str,
        keyspace: &'static str,
        columns: Vec<VirtualColumnDef>,
        pk_columns: Vec<usize>,
    ) -> Self {
        Self {
            table_name,
            keyspace,
            columns,
            pk_columns,
        }
    }
}

impl VirtualTable for StubVirtualTable {
    fn name(&self) -> &str {
        self.table_name
    }

    fn keyspace(&self) -> &str {
        self.keyspace
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &self.pk_columns
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        // TODO: Full implementation pending for this virtual table.
        vec![]
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

// ---------------------------------------------------------------------------
// Stub table constructors for each deferred task
// ---------------------------------------------------------------------------

/// T-20: Slow query log — `system_observability.slow_queries`
pub fn slow_queries_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "slow_queries",
        "system_observability",
        vec![
            col("query_id", DataType::BigInt),
            col("query_text", DataType::Text),
            col("keyspace", DataType::Text),
            col("duration_ms", DataType::BigInt),
            col("client_address", DataType::Text),
            col("timestamp_ms", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-21: Compaction history — `system_observability.compaction_history`
pub fn compaction_history_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "compaction_history",
        "system_observability",
        vec![
            col("id", DataType::BigInt),
            col("keyspace", DataType::Text),
            col("table_name", DataType::Text),
            col("started_at_ms", DataType::BigInt),
            col("completed_at_ms", DataType::BigInt),
            col("input_sstables", DataType::Int),
            col("output_sstables", DataType::Int),
            col("bytes_read", DataType::BigInt),
            col("bytes_written", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-22: Raft state — `system_observability.raft_state`
pub fn raft_state_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "raft_state",
        "system_observability",
        vec![
            col("node_id", DataType::Text),
            col("role", DataType::Text),
            col("term", DataType::BigInt),
            col("committed_index", DataType::BigInt),
            col("applied_index", DataType::BigInt),
            col("leader_id", DataType::Text),
        ],
        vec![0],
    )
}

/// T-23: Repair status — `system_observability.repair_status`
pub fn repair_status_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "repair_status",
        "system_observability",
        vec![
            col("repair_id", DataType::BigInt),
            col("keyspace", DataType::Text),
            col("table_name", DataType::Text),
            col("state", DataType::Text),
            col("started_at_ms", DataType::BigInt),
            col("progress_pct", DataType::Int),
        ],
        vec![0],
    )
}

/// T-24: Hint status — `system_observability.hint_status`
pub fn hint_status_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "hint_status",
        "system_observability",
        vec![
            col("target_node", DataType::Text),
            col("pending_hints", DataType::BigInt),
            col("oldest_hint_ms", DataType::BigInt),
            col("delivered_total", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-25: S3 upload queue — `system_observability.s3_upload_queue`
pub fn s3_upload_queue_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "s3_upload_queue",
        "system_observability",
        vec![
            col("queue_depth", DataType::BigInt),
            col("oldest_age_secs", DataType::BigInt),
            col("upload_errors_total", DataType::BigInt),
            col("bytes_pending", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-26: Gossip state — `system_observability.gossip_state`
pub fn gossip_state_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "gossip_state",
        "system_observability",
        vec![
            col("peer_id", DataType::Text),
            col("state", DataType::Text),
            col("heartbeat_gen", DataType::BigInt),
            col("last_seen_ms", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-29: Resource quotas — `system_observability.resource_quotas`
pub fn resource_quotas_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "resource_quotas",
        "system_observability",
        vec![
            col("role", DataType::Text),
            col("max_connections", DataType::Int),
            col("max_requests_per_sec", DataType::Int),
            col("max_bytes_per_sec", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-31: Latency histograms — `system_observability.latency_histograms`
pub fn latency_histograms_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "latency_histograms",
        "system_observability",
        vec![
            col("operation", DataType::Text),
            col("p50_us", DataType::BigInt),
            col("p95_us", DataType::BigInt),
            col("p99_us", DataType::BigInt),
            col("max_us", DataType::BigInt),
            col("count", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-32: Cache stats — `system_observability.cache_stats`
pub fn cache_stats_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "cache_stats",
        "system_observability",
        vec![
            col("cache_name", DataType::Text),
            col("size_bytes", DataType::BigInt),
            col("capacity_bytes", DataType::BigInt),
            col("hits", DataType::BigInt),
            col("misses", DataType::BigInt),
            col("evictions", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-35: Network stats — `system_observability.network_stats`
pub fn network_stats_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "network_stats",
        "system_observability",
        vec![
            col("peer_address", DataType::Text),
            col("lane", DataType::Text),
            col("bytes_sent", DataType::BigInt),
            col("bytes_received", DataType::BigInt),
            col("messages_sent", DataType::BigInt),
            col("messages_received", DataType::BigInt),
            col("errors", DataType::BigInt),
        ],
        vec![0, 1],
    )
}

/// T-37: Accord transaction stats — `system_observability.accord_stats`
pub fn accord_stats_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "accord_stats",
        "system_observability",
        vec![
            col("total_txns", DataType::BigInt),
            col("committed", DataType::BigInt),
            col("aborted", DataType::BigInt),
            col("in_flight", DataType::BigInt),
            col("avg_latency_us", DataType::BigInt),
        ],
        vec![0],
    )
}

/// T-38: UDF execution stats — `system_observability.udf_stats`
pub fn udf_stats_stub() -> StubVirtualTable {
    StubVirtualTable::new(
        "udf_stats",
        "system_observability",
        vec![
            col("function_name", DataType::Text),
            col("keyspace", DataType::Text),
            col("invocations", DataType::BigInt),
            col("total_duration_us", DataType::BigInt),
            col("errors", DataType::BigInt),
        ],
        vec![0, 1],
    )
}

/// Register all stub virtual tables into a registry.
pub fn register_all_stubs(registry: &ferrosa_schema::VirtualTableRegistry) {
    use std::sync::Arc;

    let stubs: Vec<Box<dyn VirtualTable>> = vec![
        Box::new(slow_queries_stub()),
        Box::new(compaction_history_stub()),
        Box::new(raft_state_stub()),
        Box::new(repair_status_stub()),
        Box::new(hint_status_stub()),
        Box::new(s3_upload_queue_stub()),
        Box::new(gossip_state_stub()),
        Box::new(resource_quotas_stub()),
        Box::new(latency_histograms_stub()),
        Box::new(cache_stats_stub()),
        Box::new(network_stats_stub()),
        Box::new(accord_stats_stub()),
        Box::new(udf_stats_stub()),
    ];

    for stub in stubs {
        registry.register(Arc::from(stub));
    }
}

/// Helper to create a column definition.
fn col(name: &str, data_type: DataType) -> VirtualColumnDef {
    VirtualColumnDef {
        name: name.to_string(),
        data_type,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::VirtualTableRegistry;

    #[test]
    fn stub_tables_return_empty_results() {
        let stubs: Vec<Box<dyn VirtualTable>> = vec![
            Box::new(slow_queries_stub()),
            Box::new(compaction_history_stub()),
            Box::new(raft_state_stub()),
            Box::new(repair_status_stub()),
            Box::new(hint_status_stub()),
            Box::new(s3_upload_queue_stub()),
            Box::new(gossip_state_stub()),
            Box::new(resource_quotas_stub()),
            Box::new(latency_histograms_stub()),
            Box::new(cache_stats_stub()),
            Box::new(network_stats_stub()),
            Box::new(accord_stats_stub()),
            Box::new(udf_stats_stub()),
        ];

        for stub in &stubs {
            assert_eq!(stub.keyspace(), "system_observability");
            assert!(
                stub.read(None).is_empty(),
                "stub {} should be empty",
                stub.name()
            );
            assert!(
                !stub.columns().is_empty(),
                "stub {} should have columns",
                stub.name()
            );
        }
    }

    #[test]
    fn register_all_stubs_populates_registry() {
        let registry = VirtualTableRegistry::new();
        register_all_stubs(&registry);

        // All 13 stub tables should be registered.
        let tables = registry.list("system_observability");
        assert_eq!(tables.len(), 13);
    }

    #[test]
    fn stub_tables_queryable_by_name() {
        let registry = VirtualTableRegistry::new();
        register_all_stubs(&registry);

        let expected_names = [
            "slow_queries",
            "compaction_history",
            "raft_state",
            "repair_status",
            "hint_status",
            "s3_upload_queue",
            "gossip_state",
            "resource_quotas",
            "latency_histograms",
            "cache_stats",
            "network_stats",
            "accord_stats",
            "udf_stats",
        ];

        for name in &expected_names {
            let table = registry.get("system_observability", name);
            assert!(table.is_some(), "missing stub table: {name}");
            assert!(table.unwrap().read(None).is_empty());
        }
    }
}
