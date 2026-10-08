//! LWTs on rows an older build wrote with NANOSECOND cell stamps
//! (t_cf637b6e).
//!
//! Before the fix, Accord stamped every cell, liveness and deletion marker with
//! the HLC's `t.time` — nanoseconds — while everything else wrote
//! microseconds. #532 moved new Accord cells to microseconds and bounded the
//! as-of-`t` read in microseconds, and every CAS on a row LWT-written before
//! the upgrade broke: its cells sat above the bound and the row read as
//! absent. #532's tests only used rows written by the new code.
//!
//! So every test here STARTS from data the old build left behind (a raw
//! SSTable whose header minimum is >= 1e18, a replayed commit log, an
//! in-process nanosecond write) and runs real Accord transactions over TCP
//! between two nodes backed by real storage engines.

use std::sync::Arc;

use ferrosa_cluster::accord::apply::StorageReader;
use ferrosa_cluster::accord::handlers::{AccordHandler, AccordState};
use ferrosa_cluster::accord::state_machine::AccordStateMachine;
use ferrosa_cluster::accord::{AccordCoordinatorDriver, AccordDriverError, ConditionGate};
use ferrosa_common::accord::{HybridLogicalClock, Timestamp};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::{HandlerRegistry, PeerId};
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::accord::sync_writer::MockSyncWriter;
use ferrosa_storage::legacy_ns_fixtures::{
    legacy_ns, open_engine, seed_legacy, wall_now_us, LegacySeed,
};
use ferrosa_storage::{Mutation, StorageEngine, TableId};

const KS: &str = "legacy_lwt_ks";
const TABLE: &str = "legacy_lwt_t";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TABLE.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn tid() -> TableId {
    TableId::new(KS, TABLE)
}

fn key(pk: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(pk.as_bytes().to_vec()))
}

fn v_row(value: i32, ts: i64) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(value.to_be_bytes().to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

fn mutation_bytes(pk: &str, value: i32, cell_ts: i64) -> Vec<u8> {
    let m = Mutation::new(
        KS.to_string(),
        TABLE.to_string(),
        key(pk),
        vec![v_row(value, cell_ts)],
        cell_ts,
    );
    let mut buf = vec![0u8; m.serialized_size()];
    m.serialize_into(&mut buf);
    buf
}

fn decode_v(bytes: &[u8]) -> Option<i32> {
    let m = Mutation::deserialize_from(bytes).expect("read-row bytes decode");
    let row = m.rows.first()?;
    let v = row.cells.first()?.1.value.clone()?;
    Some(i32::from_be_bytes(v[..4].try_into().ok()?))
}

/// The value currently stored, read with the production reader at a far
/// future `t`.
fn engine_v(engine: &Arc<StorageEngine>, pk: &str) -> Option<i32> {
    let reader = ferrosa_cluster::accord::EngineStorageReader::new(engine.clone());
    let far_future = Timestamp {
        epoch: u64::MAX,
        time: u64::MAX,
        seq: u32::MAX,
        node: 0,
    };
    let bytes = reader
        .read_row_at(KS, TABLE, pk.as_bytes(), far_future)
        .expect("engine read")?;
    decode_v(&bytes)
}

fn if_v_eq_gate(expected: i32) -> ConditionGate {
    Box::new(move |row: Option<&[u8]>| match row {
        Some(bytes) if !bytes.is_empty() => decode_v(bytes) == Some(expected),
        _ => false,
    })
}

fn if_not_exists_gate() -> ConditionGate {
    Box::new(|row: Option<&[u8]>| !matches!(row, Some(bytes) if !bytes.is_empty()))
}

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _: PeerId) {}
    fn on_peer_disconnected(&self, _: PeerId) {}
    fn on_peer_suspected(&self, _: PeerId) {}
    fn on_peer_recovered(&self, _: uuid::Uuid) {}
    fn on_peer_failed(&self, _: uuid::Uuid) {}
}

struct Node {
    host_id: uuid::Uuid,
    node_id: u64,
    peer_manager: Arc<PeerManager>,
    server: Arc<RpcServer>,
    accord_state: AccordState,
    addr: std::net::SocketAddr,
    engine: Arc<StorageEngine>,
}

/// An Accord node over `engine`, serving RPC on a real TCP port.
async fn start_node(host_id: uuid::Uuid, engine: Arc<StorageEngine>) -> Node {
    let b = host_id.as_bytes();
    let node_id = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    let applier = Arc::new(ferrosa_cluster::accord::EngineStorageApplier::new(
        engine.clone(),
    ));
    let reader = Arc::new(ferrosa_cluster::accord::EngineStorageReader::new(
        engine.clone(),
    ));
    let accord_state: AccordState = Arc::new(parking_lot::Mutex::new(
        AccordStateMachine::with_applier_and_reader(
            node_id,
            Arc::new(MockSyncWriter::new()),
            applier,
            reader,
        ),
    ));
    let registry = Arc::new(HandlerRegistry::new());
    let handler = Arc::new(AccordHandler::new(accord_state.clone(), node_id));
    for msg in [
        MsgType::AccordPreAccept,
        MsgType::AccordAccept,
        MsgType::AccordCommit,
        MsgType::AccordRead,
        MsgType::AccordApply,
        MsgType::AccordRecover,
    ] {
        registry.register(msg, handler.clone());
    }
    let net_cfg = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    };
    let server = Arc::new(RpcServer::new(net_cfg.clone(), host_id, registry));
    let addr = server.start_and_get_addr().await.expect("bind");
    let peer_manager = Arc::new(PeerManager::new(
        Arc::new(net_cfg),
        host_id,
        Arc::new(NoopListener),
    ));
    Node {
        host_id,
        node_id,
        peer_manager,
        server,
        accord_state,
        addr,
        engine,
    }
}

/// Two connected nodes over the given engines.
async fn cluster(a: Arc<StorageEngine>, b: Arc<StorageEngine>) -> (Node, Node) {
    let coord = start_node(uuid::Uuid::from_bytes([0xA1; 16]), a).await;
    let replica = start_node(uuid::Uuid::from_bytes([0xB2; 16]), b).await;
    coord
        .peer_manager
        .ensure_peer(replica.host_id, &replica.addr.to_string())
        .await
        .expect("coord -> replica");
    replica
        .peer_manager
        .ensure_peer(coord.host_id, &coord.addr.to_string())
        .await
        .expect("replica -> coord");
    (coord, replica)
}

async fn shutdown(nodes: [&Node; 2]) {
    for n in nodes {
        n.server
            .shutdown(std::time::Duration::from_millis(100))
            .await;
    }
}

/// Run one conditional LWT on `pk` from `coord`, the way the CQL layer does
/// (`ReadRow` read vote + the statement's condition gate). The HLC is the
/// production one: `t.time` is wall-clock nanoseconds.
async fn lwt(
    coord: &Node,
    replica: &Node,
    pk: &str,
    value: i32,
    gate: ConditionGate,
) -> Result<(), AccordDriverError> {
    let clock = HybridLogicalClock::new(coord.node_id, 0);
    AccordCoordinatorDriver::new(
        coord.node_id,
        vec![coord.host_id, replica.host_id],
        Arc::clone(&coord.peer_manager),
        false,
        &clock,
        pk.as_bytes().to_vec(),
        mutation_bytes(pk, value, wall_now_us()),
    )
    .with_local_accord_state(coord.accord_state.clone())
    .with_read_predicate(ferrosa_cluster::accord::ReadPredicate::ReadRow {
        keyspace: KS.to_string(),
        table: TABLE.to_string(),
    })
    .with_local_applier(Arc::new(
        ferrosa_cluster::accord::EngineStorageApplier::new(coord.engine.clone()),
    ))
    .with_local_reader(Arc::new(ferrosa_cluster::accord::EngineStorageReader::new(
        coord.engine.clone(),
    )))
    .with_condition_gate(gate)
    .run_transaction()
    .await
    .map(|_| ())
}

/// Both replicas hold `v` for `pk` as the old build wrote it: an LWT agreed a
/// second ago, stamped in nanoseconds.
fn seed_both(
    seed: LegacySeed,
    pk: &str,
    v: i32,
) -> (
    Arc<StorageEngine>,
    Arc<StorageEngine>,
    [tempfile::TempDir; 2],
) {
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let stamp = legacy_ns(wall_now_us() - 1_000_000);
    let a = seed_legacy(
        da.path(),
        &schema(),
        seed,
        vec![(key(pk), vec![v_row(v, stamp)])],
    );
    let b = seed_legacy(
        db.path(),
        &schema(),
        seed,
        vec![(key(pk), vec![v_row(v, stamp)])],
    );
    (a, b, [da, db])
}

// ---------------------------------------------------------------------------
// Tests 1 and 2: CAS on a legacy row applies, and its value wins LWW — also
// after flush and reread.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cas_update_if_on_a_legacy_row_applies_and_wins() {
    for seed in LegacySeed::ALL {
        let (a, b, dirs) = seed_both(seed, "head", 50);
        assert_eq!(engine_v(&a, "head"), Some(50), "{seed:?}: seeded");
        let (coord, replica) = cluster(a, b).await;

        lwt(&coord, &replica, "head", 77, if_v_eq_gate(50))
            .await
            .unwrap_or_else(|e| panic!("{seed:?}: UPDATE ... IF v = 50 on a legacy row: {e:?}"));
        for n in [&coord, &replica] {
            assert_eq!(
                engine_v(&n.engine, "head"),
                Some(77),
                "{seed:?}: the CAS value beats the legacy cell"
            );
            n.engine.flush(&tid()).unwrap();
            assert_eq!(
                engine_v(&n.engine, "head"),
                Some(77),
                "{seed:?}: and still after flush"
            );
        }

        // A second CAS chained on the first, as fmem's revision head does.
        lwt(&coord, &replica, "head", 78, if_v_eq_gate(77))
            .await
            .unwrap_or_else(|e| panic!("{seed:?}: the next CAS: {e:?}"));
        assert_eq!(engine_v(&coord.engine, "head"), Some(78), "{seed:?}");
        shutdown([&coord, &replica]).await;

        // Reread from disk after a restart.
        drop((coord, replica));
        for dir in &dirs {
            let engine = open_engine(dir.path(), &schema());
            assert_eq!(
                engine_v(&engine, "head"),
                Some(78),
                "{seed:?}: after restart"
            );
        }
    }
}

/// A CAS whose condition does not hold on a legacy row is refused with the
/// legacy row's real value (the bug made every condition read "absent").
#[tokio::test]
async fn a_failed_cas_on_a_legacy_row_reports_its_value() {
    for seed in LegacySeed::ALL {
        let (a, b, _dirs) = seed_both(seed, "head", 50);
        let (coord, replica) = cluster(a, b).await;
        match lwt(&coord, &replica, "head", 77, if_v_eq_gate(49)).await {
            Err(AccordDriverError::ConditionNotMet { current_row }) => assert_eq!(
                decode_v(&current_row),
                Some(50),
                "{seed:?}: the current row is the legacy row"
            ),
            other => panic!("{seed:?}: expected ConditionNotMet, got {other:?}"),
        }
        assert_eq!(engine_v(&coord.engine, "head"), Some(50), "{seed:?}");
        shutdown([&coord, &replica]).await;
    }
}

// ---------------------------------------------------------------------------
// Test 5: INSERT ... IF NOT EXISTS on a legacy row is refused.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_if_not_exists_on_a_legacy_row_is_refused() {
    for seed in LegacySeed::ALL {
        let (a, b, _dirs) = seed_both(seed, "row", 50);
        let (coord, replica) = cluster(a, b).await;
        match lwt(&coord, &replica, "row", 1, if_not_exists_gate()).await {
            Err(AccordDriverError::ConditionNotMet { current_row }) => {
                assert_eq!(decode_v(&current_row), Some(50), "{seed:?}")
            }
            other => panic!("{seed:?}: IF NOT EXISTS on a legacy row must not apply: {other:?}"),
        }
        for n in [&coord, &replica] {
            assert_eq!(engine_v(&n.engine, "row"), Some(50), "{seed:?}: no write");
        }
        shutdown([&coord, &replica]).await;
    }
}

// ---------------------------------------------------------------------------
// Test 6: the read at `t` includes legacy cells written before `t`.
// ---------------------------------------------------------------------------

#[test]
fn a_read_at_t_includes_legacy_cells_written_before_t() {
    for seed in LegacySeed::ALL {
        let dir = tempfile::tempdir().unwrap();
        let stamp = legacy_ns(wall_now_us() - 1_000_000);
        let engine = seed_legacy(
            dir.path(),
            &schema(),
            seed,
            vec![(key("k"), vec![v_row(50, stamp)])],
        );
        let reader = ferrosa_cluster::accord::EngineStorageReader::new(engine);
        let t = HybridLogicalClock::new(1, 0).now();
        let bytes = reader
            .read_row_at(KS, TABLE, b"k", t)
            .unwrap()
            .unwrap_or_else(|| panic!("{seed:?}: a row written before t is part of the row at t"));
        assert_eq!(decode_v(&bytes), Some(50), "{seed:?}");
    }
}

// ---------------------------------------------------------------------------
// Test 17: rows a #532 build stamped in microseconds keep working.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_row_lwt_written_in_microseconds_by_532_still_cas() {
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // #532 stamped Accord cells with t.time / 1000, and they were flushed.
    let stamp = wall_now_us() - 1_000_000;
    let seed = |dir: &std::path::Path| {
        let engine = open_engine(dir, &schema());
        engine
            .write(&tid(), &key("head"), v_row(50, stamp), stamp)
            .unwrap();
        engine.flush(&tid()).unwrap();
        engine
    };
    let (coord, replica) = cluster(seed(da.path()), seed(db.path())).await;
    lwt(&coord, &replica, "head", 77, if_v_eq_gate(50))
        .await
        .expect("CAS on a row #532 stamped in microseconds");
    for n in [&coord, &replica] {
        assert_eq!(engine_v(&n.engine, "head"), Some(77));
    }
    shutdown([&coord, &replica]).await;
}

// ---------------------------------------------------------------------------
// Test 12: an old-style stamper next to the new code converges after repair.
// ---------------------------------------------------------------------------

/// Node A wrote LWTs the old way (nanoseconds) while node B, already upgraded,
/// took plain writes. Once both run the fixed build, repair must converge both
/// keys to the write that is newer in REAL time: `lwt-later` (A's LWT is
/// newer) and `plain-later` (B's plain write is newer).
#[test]
fn a_mixed_version_pair_converges_after_repair() {
    let t0 = wall_now_us();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = seed_legacy(
        da.path(),
        &schema(),
        LegacySeed::Sstable,
        vec![
            (key("lwt-later"), vec![v_row(1, legacy_ns(t0 + 2_000))]),
            (key("plain-later"), vec![v_row(1, legacy_ns(t0))]),
        ],
    );
    let b = open_engine(db.path(), &schema());
    b.write(&tid(), &key("lwt-later"), v_row(2, t0 + 1_000), t0 + 1_000)
        .unwrap();
    b.write(
        &tid(),
        &key("plain-later"),
        v_row(2, t0 + 1_000),
        t0 + 1_000,
    )
    .unwrap();

    let read_all = |engine: &StorageEngine| -> Vec<ferrosa_sstable::types::Partition> {
        ["lwt-later", "plain-later"]
            .iter()
            .map(|pk| engine.read(&tid(), &key(pk)).unwrap().unwrap())
            .collect()
    };
    let plan = ferrosa_cluster::repair::diff_partition_sets(&read_all(&a), &read_all(&b));
    let apply = |engine: &StorageEngine, parts: &[ferrosa_sstable::types::Partition]| {
        for p in parts {
            for row in &p.rows {
                let ts = row.primary_key_liveness.timestamp;
                engine.write(&tid(), &p.key, row.clone(), ts).unwrap();
            }
        }
    };
    apply(&b, &plan.a_to_b);
    apply(&a, &plan.b_to_a);

    for (name, engine) in [("A", &a), ("B", &b)] {
        assert_eq!(
            engine_v(engine, "lwt-later"),
            Some(1),
            "{name}: the LWT is newer in real time"
        );
        assert_eq!(
            engine_v(engine, "plain-later"),
            Some(2),
            "{name}: the plain write is newer in real time"
        );
    }
    let again = ferrosa_cluster::repair::diff_partition_sets(&read_all(&a), &read_all(&b));
    assert!(
        again.a_to_b.is_empty() && again.b_to_a.is_empty(),
        "a second repair pass finds nothing to stream"
    );
}
