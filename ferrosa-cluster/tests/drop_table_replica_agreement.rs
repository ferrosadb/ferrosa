//! Invariant **ALL REPLICAS AGREE** for `DROP TABLE` (forge t_c8625592).
//!
//! The storage half of the fix (branch `fix/drop-table-no-resurrection`) makes
//! a DROP whose SSTable removal fails *fail loud* and never reload the
//! survivors. This test guards the **cluster** half: a replica whose apply of a
//! replicated `DropTable` refuses must not let the DROP be reported to the
//! client as success.
//!
//! The failure mode: `RaftState::apply_command` removes the table from the
//! *in-memory* schema and then calls `engine.unregister_table`. The openraft
//! state machine folds that refusal into a `RaftResponse::Error`, but the
//! client-facing cluster route (`ddl_path::execute_via_raft`) discarded
//! `resp.data` and returned `Ok` regardless — so a DROP that did not take effect
//! on the node that applied it was reported to the client as done.
//!
//! This drives the **real** Raft path: three voters, each with its own real
//! `StorageEngine` and `Schema`; the DROP is applied by the real state machine
//! on the real engine. The applied node's table directory is made unremovable
//! with the same order-independent POSIX mode-000 seam the storage tests use.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::raft_harness::TestCluster;
use ferrosa_cluster::ddl_path::execute_via_raft;
use ferrosa_cluster::pair::ddl::DdlOperation;
use ferrosa_cluster::raft::{RaftCommand, RaftOp, RaftResponse};
use ferrosa_common::key::DecoratedKey;
use ferrosa_common::{CellValue, PartitionKey};
use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::TableId;
use indexmap::IndexMap;

const KS: &str = "drop_agree_ks";
const TBL: &str = "drop_agree_tbl";

fn command(op: RaftOp) -> RaftCommand {
    RaftCommand {
        op,
        schema_version: uuid::Uuid::new_v4(),
    }
}

fn keyspace() -> KeyspaceMetadata {
    let mut options = HashMap::new();
    options.insert("replication_factor".to_string(), "3".to_string());
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

fn row(value: &[u8], ts: i64) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

async fn propose_on_node(
    cluster: &TestCluster,
    node_id: u64,
    op: RaftOp,
) -> Result<RaftResponse, String> {
    let raft = cluster
        .raft_for_node_id(node_id)
        .unwrap_or_else(|| panic!("node {node_id} present"));
    match raft.client_write(command(op)).await {
        Ok(resp) => Ok(resp.data),
        Err(e) => Err(format!("{e:?}")),
    }
}

/// Restores a table directory's mode so the tempdir stays cleanable.
#[cfg(unix)]
struct RestoreDirMode(std::path::PathBuf);
#[cfg(unix)]
impl Drop for RestoreDirMode {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(&self.0) {
            let mut p = md.permissions();
            p.set_mode(0o755);
            let _ = std::fs::set_permissions(&self.0, p);
        }
    }
}

/// Make `table_dir` unreadable so `remove_dir_all` cannot even enumerate it —
/// the order-independent form of "the removal did not complete".
#[cfg(unix)]
fn make_undeletable(table_dir: &std::path::Path) -> RestoreDirMode {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(table_dir).unwrap().permissions();
    perms.set_mode(0o000);
    std::fs::set_permissions(table_dir, perms).unwrap();
    RestoreDirMode(table_dir.to_path_buf())
}

/// A DROP that the applying node refused must surface to the client — not be
/// reported as success — and the refusal must never let that node serve the
/// dropped rows, nor resurrect them on a same-name re-create.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_drop_on_the_applying_node_surfaces_and_never_resurrects() {
    let cluster = TestCluster::with_voters_and_engines(3).await;
    let leader_id = cluster
        .wait_for_all_voters_leader(Duration::from_secs(10))
        .await
        .expect("all voters should agree on a leader");

    // Schema first, so every node has the table before it holds any data.
    propose_on_node(&cluster, leader_id, RaftOp::CreateKeyspace(keyspace()))
        .await
        .expect("CreateKeyspace applies");
    propose_on_node(&cluster, leader_id, RaftOp::CreateTable(Box::new(table())))
        .await
        .expect("CreateTable applies");

    let tid = TableId::new(KS, TBL);
    for node in cluster.nodes().iter() {
        let engine = node.engine.as_ref().expect("engine-backed node");
        for i in 0..3 {
            let pk = format!("old{i}");
            engine
                .write(
                    &tid,
                    &DecoratedKey::new(PartitionKey::new(pk.into_bytes())),
                    row(b"stale", 1),
                    1,
                )
                .unwrap_or_else(|e| panic!("seed row on node {}: {e}", node.node_id));
        }
        engine.flush(&tid).expect("flush seed rows");
    }

    // Sabotage the node that will APPLY the DROP: its removal of this table's
    // SSTables will fail. The leader is chosen so the refusal lands on the node
    // whose apply determines the client-visible result.
    let leader_raft = cluster.raft_for_node_id(leader_id).expect("leader raft");
    let table_dir = {
        let nodes = cluster.nodes();
        let leader = nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node present");
        let engine = leader.engine.as_ref().expect("engine-backed leader");
        let table_dir = engine.data_dir().join("sstables").join(tid.to_string());
        assert!(
            table_dir.exists(),
            "precondition: the table directory exists on disk before the DROP"
        );
        table_dir
    };

    // Everything below runs with the table directory unreadable, so the DROP's
    // removal cannot complete. The guard restores the mode when it drops.
    {
        let _restore = make_undeletable(&table_dir);

        // The DROP, through the real client-facing cluster route. It is refused
        // on this node, so it must NOT be reported as success.
        let drop = execute_via_raft(
            &leader_raft,
            DdlOperation::DropTable {
                keyspace: KS.to_string(),
                table: TBL.to_string(),
            },
        )
        .await;
        assert!(
            drop.is_err(),
            "a DROP that the applying node refused must fail loud to the client, \
             but the cluster route reported success (returned {drop:?})"
        );

        // (b) The refused node must not serve the dropped rows afterwards, even
        // though its removal did not complete.
        {
            let nodes = cluster.nodes();
            let leader = nodes
                .iter()
                .find(|n| n.node_id == leader_id)
                .expect("leader node present");
            let engine = leader.engine.as_ref().expect("engine-backed leader");
            assert_eq!(
                engine.count_range(&tid, None, None).expect("count"),
                0,
                "the refused node still serves the dropped table's rows"
            );
        }
    }

    // The removal never completed: the dropped incarnation's SSTable directory
    // is still physically on disk. It must nevertheless never be served again —
    // not now, and not through a same-name re-create.
    assert!(
        table_dir.exists(),
        "precondition: the refused removal must have left the directory on disk"
    );
    {
        let nodes = cluster.nodes();
        let leader = nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node present");
        let engine = leader.engine.as_ref().expect("engine-backed leader");
        assert_eq!(
            engine.count_range(&tid, None, None).expect("count"),
            0,
            "rows physically present on disk must not be served after the refused DROP"
        );
    }

    // A same-name re-create must sweep the orphaned directory and hold only the
    // new rows, never the dropped incarnation's survivors.
    propose_on_node(&cluster, leader_id, RaftOp::CreateTable(Box::new(table())))
        .await
        .expect("same-name CreateTable applies");
    {
        let nodes = cluster.nodes();
        let leader = nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node present");
        let engine = leader.engine.as_ref().expect("engine-backed leader");
        for i in 0..2 {
            let pk = format!("new{i}");
            engine
                .write(
                    &tid,
                    &DecoratedKey::new(PartitionKey::new(pk.into_bytes())),
                    row(b"fresh", 100),
                    100,
                )
                .expect("write new row");
        }
        assert_eq!(
            engine.count_range(&tid, None, None).expect("count"),
            2,
            "a same-name re-create must hold ONLY the new rows; the dropped \
             table's rows came back"
        );
    }
}
