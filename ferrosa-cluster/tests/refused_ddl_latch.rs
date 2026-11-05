//! A *refused* DDL must not fence the replica that refused it.
//!
//! `apply_command` collects local effects that a committed command could not
//! carry out into `ApplyError`s, and `finish_pending_apply` latches the first
//! storage failure it is handed — every later apply then re-reports it and the
//! node stops serving. That latch is right for a failure that leaves this
//! replica's durable state disagreeing with the log. It is wrong for a *refusal*:
//! the entry is committed, canonical metadata agrees on every replica, and only
//! this node's copy of one table's bytes is unusable.
//!
//! These tests drive the real Raft path on real engines and assert the boundary
//! from the outside: the client sees a statement refusal rather than a Raft
//! fault, the node keeps applying, and the repair is a DDL rather than a
//! restart. `drop_table_replica_agreement.rs` covers the table-level DROP;
//! this file covers the keyspace-level DROP and the CREATE that trips over the
//! directory such a DROP could not remove.

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

fn command(op: RaftOp) -> RaftCommand {
    RaftCommand {
        op,
        schema_version: uuid::Uuid::new_v4(),
    }
}

fn keyspace_named(name: &str) -> KeyspaceMetadata {
    let mut options = HashMap::new();
    options.insert("replication_factor".to_string(), "3".to_string());
    KeyspaceMetadata {
        name: name.to_string(),
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

fn table_named(keyspace: &str, table: &str) -> TableMetadata {
    let columns: IndexMap<String, ColumnMetadata> = [
        column("id", ColumnKind::PartitionKey, "text"),
        column("value", ColumnKind::Regular, "text"),
    ]
    .into_iter()
    .collect();
    TableMetadata {
        keyspace: keyspace.to_string(),
        name: table.to_string(),
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

/// A `DROP KEYSPACE` whose table unregistration is refused must not fence the
/// node.
///
/// `DropKeyspace` pushes the same `ApplyError::EngineUnregisterTable` variant
/// `DropTable` does, so the partition that spares the table-level DROP should
/// spare this route too. This proves the shared variant really is shared
/// behaviour rather than assuming it: the same unremovable directory must be
/// reported to the client as a statement refusal, must not be served as success,
/// and must leave the Core applying.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_drop_keyspace_surfaces_and_leaves_the_core_serving() {
    const KS: &str = "drop_ks_agree_ks";
    const TBL: &str = "drop_ks_agree_tbl";

    let cluster = TestCluster::with_voters_and_engines(3).await;
    let leader_id = cluster
        .wait_for_all_voters_leader(Duration::from_secs(10))
        .await
        .expect("all voters should agree on a leader");

    propose_on_node(
        &cluster,
        leader_id,
        RaftOp::CreateKeyspace(keyspace_named(KS)),
    )
    .await
    .expect("CreateKeyspace applies");
    propose_on_node(
        &cluster,
        leader_id,
        RaftOp::CreateTable(Box::new(table_named(KS, TBL))),
    )
    .await
    .expect("CreateTable applies");

    let tid = TableId::new(KS, TBL);
    let leader_raft = cluster.raft_for_node_id(leader_id).expect("leader raft");
    let engine = {
        let nodes = cluster.nodes();
        let leader = nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node present");
        let engine = leader
            .engine
            .as_ref()
            .expect("engine-backed leader")
            .clone();
        for i in 0..3 {
            engine
                .write(
                    &tid,
                    &DecoratedKey::new(PartitionKey::new(format!("old{i}").into_bytes())),
                    row(b"stale", 1),
                    1,
                )
                .unwrap_or_else(|e| panic!("seed row on leader: {e}"));
        }
        engine.flush(&tid).expect("flush seed rows");
        engine
    };
    let table_dir = engine.data_dir().join("sstables").join(tid.to_string());
    assert!(
        table_dir.exists(),
        "precondition: the table directory exists"
    );

    {
        let _restore = make_undeletable(&table_dir);

        let drop =
            execute_via_raft(&leader_raft, &DdlOperation::DropKeyspace(KS.to_string())).await;
        let refusal = drop.expect_err(
            "a DROP KEYSPACE whose table unregistration was refused must fail loud \
             to the client, but the cluster route reported success",
        );
        // A statement refusal carrying the state machine's reason — not a Raft
        // fault. A `RaftError` here means the apply fence treated an undroppable
        // directory as a storage failure and latched the machine.
        assert!(
            matches!(
                &refusal,
                ferrosa_cluster::ClusterError::Internal(reason)
                    if reason.contains("refused on the applying node")
                        && reason.contains("unregister_table")
            ),
            "a refused DROP KEYSPACE must surface as the DDL path's statement \
             refusal, not as a Raft fault (got {refusal:?})"
        );

        let running = leader_raft.metrics().borrow().running_state.clone();
        assert!(
            running.is_ok(),
            "the RaftCore must survive a DROP KEYSPACE its applying node refused, \
             but it is no longer running: {running:?}"
        );

        // The keyspace is gone from canonical metadata on this node, so its rows
        // must not be served even though the bytes survived.
        assert_eq!(
            engine.count_range(&tid, None, None).expect("count"),
            0,
            "the node that refused the removal still serves the dropped rows"
        );
    }

    // The node still applies: a subsequent DDL on the same Core commits and is
    // visible to the state machine it applies to.
    let next_name = "drop_ks_agree_after_refusal";
    let next = propose_on_node(
        &cluster,
        leader_id,
        RaftOp::CreateKeyspace(keyspace_named(next_name)),
    )
    .await
    .expect("a subsequent DDL must still apply after the refused DROP KEYSPACE");
    assert!(
        matches!(next, RaftResponse::Ok),
        "a subsequent DDL must apply cleanly, got {next:?}"
    );

    let state = {
        let nodes = cluster.nodes();
        let node = nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node present");
        node.state_machine.snapshot_state().await
    };
    assert!(
        state.keyspaces.contains_key(next_name),
        "the keyspace created after the refusal must be in applied state"
    );
    assert!(
        !state.keyspaces.contains_key(KS),
        "the refused DROP KEYSPACE must still have removed the keyspace metadata"
    );

    let running = leader_raft.metrics().borrow().running_state.clone();
    assert!(
        running.is_ok(),
        "the Core must still be running after the refused DROP KEYSPACE: {running:?}"
    );
}
