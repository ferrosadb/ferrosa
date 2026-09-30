//! FM-70 / t_1f2741a0: a replicated `CREATE INDEX` must BUILD the index on
//! every node that applies it, not only record it in that node's schema.
//!
//! Before the fix only the node whose CQL session ran the statement wired the
//! index into its storage engine. Every other node listed the index in
//! `Schema`, so the planner chose it there, and the node either answered from
//! an index it did not have or refused the read. The index appeared on a
//! follower only after a restart.
//!
//! This drives the real Raft path: three voters, each with its own
//! `StorageEngine` and `Schema`; the DDL is proposed once, on the leader, and
//! every node applies it from its own log.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::raft_harness::TestCluster;
use ferrosa_cluster::raft::{RaftCommand, RaftOp, RaftResponse};
use ferrosa_common::key::DecoratedKey;
use ferrosa_common::{CellValue, PartitionKey};
use ferrosa_index::{IndexKey, IndexType};
use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::TableId;
use indexmap::IndexMap;

const KS: &str = "fm70_ks";
const TBL: &str = "fm70_tbl";
const IDX: &str = "fm70_by_value";

fn command(op: RaftOp) -> RaftCommand {
    RaftCommand {
        op,
        schema_version: uuid::Uuid::new_v4(),
    }
}

fn keyspace() -> KeyspaceMetadata {
    let mut options = HashMap::new();
    options.insert("replication_factor".to_string(), "1".to_string());
    KeyspaceMetadata {
        name: KS.to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options,
        },
    }
}

fn column(name: &str, kind: ColumnKind, ty: &str) -> (String, ColumnMetadata) {
    (
        name.to_string(),
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: ty.to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    )
}

fn table() -> TableMetadata {
    let columns: IndexMap<String, ColumnMetadata> = [
        column("id", ColumnKind::PartitionKey, "text"),
        column("value", ColumnKind::Regular, "text"),
    ]
    .into_iter()
    .collect();
    TableMetadata {
        keyspace: KS.to_string(),
        name: TBL.to_string(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: vec!["id".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: Default::default(),
        extensions: HashMap::new(),
        is_system: false,
    }
}

fn index() -> IndexMetadata {
    IndexMetadata {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        name: IDX.to_string(),
        index_type: IndexType::BTree,
        target_columns: vec!["value".to_string()],
        filter_predicate: None,
        options: HashMap::new(),
    }
}

fn row(value: &[u8], ts: i64) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Commit `op` through the leader and return the log index it was committed
/// at, taken from the `client_write` response itself.
///
/// Not read from raft metrics: those are published asynchronously, so right
/// after `client_write` returns they can still name the previous entry, and a
/// caller waiting on that index would stop one entry early.
async fn propose(cluster: &TestCluster, leader_id: u64, op: RaftOp) -> u64 {
    let raft = {
        let nodes = cluster.nodes();
        nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node present")
            .raft
            .clone()
    };
    let response = raft
        .client_write(command(op))
        .await
        .unwrap_or_else(|e| panic!("client_write on leader {leader_id}: {e:?}"));
    assert!(
        matches!(response.data, RaftResponse::Ok),
        "the leader must apply the DDL cleanly: {:?}",
        response.data
    );
    response.log_id.index
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_create_index_builds_on_receiving_node() {
    let cluster = TestCluster::with_voters_and_engines(3).await;
    let leader_id = cluster
        .wait_for_all_voters_leader(Duration::from_secs(10))
        .await
        .expect("all voters should agree on a leader");

    // Schema first, so every node has the table before it holds any data.
    propose(&cluster, leader_id, RaftOp::CreateKeyspace(keyspace())).await;
    let after_table = propose(&cluster, leader_id, RaftOp::CreateTable(Box::new(table()))).await;
    wait_all_applied(&cluster, after_table).await;

    // Every replica holds the same rows, written before the index exists.
    let tid = TableId::new(KS, TBL);
    for node in cluster.nodes().iter() {
        let engine = node.engine.as_ref().expect("engine-backed node");
        for (pk, value, ts) in [
            ("a", "alice", 1000),
            ("b", "alice", 1001),
            ("c", "bob", 1002),
        ] {
            engine
                .write(
                    &tid,
                    &DecoratedKey::new(PartitionKey::new(pk.as_bytes().to_vec())),
                    row(value.as_bytes(), ts),
                    ts,
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "seed row on node {} (leader {leader_id}): {e}",
                        node.node_id
                    )
                });
        }
    }

    // CREATE INDEX is proposed ONCE. Every node learns of it only by applying
    // its own copy of the log; none runs a CQL session.
    let after_index = propose(&cluster, leader_id, RaftOp::CreateIndex(index())).await;
    wait_all_applied(&cluster, after_index).await;

    for node in cluster.nodes().iter() {
        let engine = node.engine.as_ref().expect("engine-backed node");
        assert!(
            engine.declares_index(&tid, IDX),
            "node {} applied the replicated CREATE INDEX but did not build it \
             (schema lists it, storage does not)",
            node.node_id
        );
        assert!(
            engine.index_is_current(&tid, IDX),
            "node {} built the index but does not report it current",
            node.node_id
        );
        let mut hits = 0_usize;
        engine
            .read_by_index_each(&tid, IDX, &IndexKey(b"alice".to_vec()), &mut |_partition| {
                hits += 1;
                std::ops::ControlFlow::Continue(())
            })
            .unwrap_or_else(|e| {
                panic!(
                    "node {} could not answer the indexed read: {e}",
                    node.node_id
                )
            });
        assert_eq!(
            hits, 2,
            "node {}: an indexed read must return every matching row",
            node.node_id
        );
    }
}

async fn wait_all_applied(cluster: &TestCluster, index: u64) {
    let rafts: Vec<_> = cluster
        .nodes()
        .iter()
        .map(|n| (n.node_id, n.raft.clone()))
        .collect();
    for (node_id, raft) in rafts {
        assert!(
            ferrosa_cluster::ddl_path::wait_for_local_apply(&raft, index, Duration::from_secs(10))
                .await,
            "node {node_id} did not apply log index {index} in time"
        );
    }
}
