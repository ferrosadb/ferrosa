//! A plain `DELETE` must remove a row whose last write came from an LWT
//! (`IF` / `IF NOT EXISTS`) — forge t_15b75033, a reported P0 silent wrong
//! answer ("plain DELETE is acknowledged but silently not applied to a row last
//! written by an LWT").
//!
//! # The five cases the report names
//!
//! | # | Sequence | Reported outcome |
//! |---|---|---|
//! | A | INSERT, then plain DELETE | row gone (OK) |
//! | B | INSERT, `UPDATE ... IF version=1` applied, then plain DELETE | **row survives** |
//! | C | `INSERT ... IF NOT EXISTS` applied, then plain DELETE | **row survives** |
//! | D | as B, but `DELETE ... IF EXISTS` | row gone |
//! | E | as B, plain DELETE 3 s later | **row survives** — so not a sub-second race |
//!
//! Every case here is one test, and case A is the POSITIVE CONTROL: a harness
//! that deleted nothing at all would satisfy every "the row is gone" assertion
//! otherwise. Each case also asserts the row EXISTS immediately before its
//! DELETE, so no assertion is vacuous.
//!
//! # Harness level
//!
//! This is the CLUSTER harness level, driven through the real client path. Two
//! in-process nodes are stood up the way this repo's cluster harnesses are
//! (`tests/result_cursor_forwarding.rs`, `ferrosa-cluster/tests/accord_lwt_concurrent.rs`):
//! a real `StorageEngine` each, a real internode `RpcServer` + `PeerManager` on
//! loopback, a real `AccordStateMachine` whose applier persists through that
//! node's OWN engine, and the `AccordHandler` registered for all ten Accord
//! message types exactly as `controller/cluster.rs` registers it in production.
//! Every statement is a real CQL statement routed through `router::route`, so
//! the LWT goes PreAccept → Accept → Commit → Apply over real TCP and the plain
//! DELETE goes through the CQL delete path.
//!
//! The one thing the fixture does NOT reproduce is the replication of a PLAIN
//! write to both replicas (its `WritePath` is `Direct`): the plain INSERT is
//! seeded on both nodes at a pinned `USING TIMESTAMP`, so the two replicas hold
//! byte-identical rows and the LWT read-vote can reach its F+1 agreement. The
//! LWT itself needs no such help — Accord replicates it, which is why case C
//! (whose only write IS the LWT) seeds nothing.
//!
//! # Why this is green, and what would make it red
//!
//! RED before 2026-10-08 (`34acf0ef fix(accord): stamp Accord cells in
//! microseconds, with legacy rows readable`): the Accord applier stamped every
//! cell, liveness and deletion with the HLC's **nanoseconds** (t_277e2bf9), while
//! a plain write stamps microseconds. An LWT row therefore sat ~1000x in the
//! future and no plain DELETE could shadow it. That one mechanism reproduces all
//! five reported cases at once — including why D worked: a conditional DELETE is
//! itself an LWT, so its tombstone was stamped in the SAME nanosecond domain and
//! ordered against the row by Accord's agreed `t`, whereas a plain DELETE was
//! three orders of magnitude below it. The fix divides the agreed HLC time by
//! 1000 (`accord::apply::accord_cell_timestamp`), and `cell_ts::normalize_cell_ts`
//! maps values already on disk. This test pins the behaviour so neither can
//! regress. See `ferrosa-cluster` FMEA CL-38 / CL-74.
//!
//! # Ports
//!
//! The internode listener takes `127.0.0.1:0` and is used only after
//! `start_and_get_addr` returns, so the bind and the first use are the same
//! instant and there is no window for a source port to be taken — the
//! in-process convention this repo already uses, not the real-process harness's
//! fixed band (`ferrosa/tests/common/ports.rs`).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_common::accord::HybridLogicalClock;
use ferrosa_cql::router::{route, route_select_raw, RequestContext, RouteResult, SharedState};
use ferrosa_cql::test_util::{standalone_for_test_with, StandaloneOptions};
use ferrosa_cql::types::CqlValue;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::HandlerRegistry;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_schema::auth::role::AuthContext;
use uuid::Uuid;

const KS: &str = "pdl";
const TABLE: &str = "t";

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _peer: (Uuid, SocketAddr)) {}
    fn on_peer_disconnected(&self, _peer: (Uuid, SocketAddr)) {}
    fn on_peer_suspected(&self, _peer: (Uuid, SocketAddr)) {}
    fn on_peer_recovered(&self, _peer_id: Uuid) {}
    fn on_peer_failed(&self, _peer_id: Uuid) {}
}

struct Node {
    id: Uuid,
    state: Arc<SharedState>,
    peers: Arc<PeerManager>,
    rpc_addr: SocketAddr,
    /// Held for the node's whole life: the RPC server, its handler registry and
    /// the data directory the engine owns.
    _held: (Arc<HandlerRegistry>, Arc<RpcServer>, tempfile::TempDir),
}

/// Accord's numeric node id: the host UUID's FIRST 8 bytes, big-endian — the
/// same derivation `router::route_lwt_via_accord` uses. A node id is also a Raft
/// id, derived from the LAST 8 bytes; `Uuid::new_v4` gives 16 random bytes, so
/// both halves are distinct across the two nodes.
fn accord_node_id(host: Uuid) -> u64 {
    u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap())
}

async fn node() -> Node {
    let id = Uuid::new_v4();
    let dir = tempfile::tempdir().unwrap();
    let node_id = accord_node_id(id);
    let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
    let net = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    };
    let peers = Arc::new(PeerManager::new(
        Arc::new(net.clone()),
        id,
        Arc::new(NoopListener),
    ));
    let state = standalone_for_test_with(
        dir.path(),
        StandaloneOptions {
            flush_threshold_bytes: 1 << 30,
            host_id: Some(id),
            peer_manager: Some(peers.clone()),
            // Without a shared HLC `route` refuses the Accord path outright.
            accord_clock: Some(clock.clone()),
            ..StandaloneOptions::default()
        },
    );
    // A real Accord replica: its applier persists through THIS node's engine,
    // and its reader answers the LWT read-vote from the same engine.
    let accord_state: ferrosa_cluster::accord::AccordState = Arc::new(parking_lot::Mutex::new(
        ferrosa_cluster::accord::state_machine::build_accord_state_machine(
            node_id,
            Arc::new(ferrosa_storage::accord::sync_writer::MockSyncWriter::new()),
            state.engine.clone(),
            Some(clock),
        ),
    ));
    ferrosa_cluster::accord::publish_accord_state(&state.accord_state, accord_state.clone())
        .expect("publish local Accord state");
    let registry = Arc::new(HandlerRegistry::new());
    let handler = Arc::new(ferrosa_cluster::accord::AccordHandler::new(
        accord_state,
        node_id,
    ));
    for msg_type in [
        MsgType::AccordPreAccept,
        MsgType::AccordPreAcceptV2,
        MsgType::AccordAccept,
        MsgType::AccordCommit,
        MsgType::AccordRead,
        MsgType::AccordApply,
        MsgType::AccordApplyV2,
        MsgType::AccordApplyV2Capnp,
        MsgType::AccordApplyV2Region,
        MsgType::AccordRecover,
    ] {
        registry.register(msg_type, handler.clone());
    }
    let rpc = Arc::new(RpcServer::new(net, id, registry.clone()));
    let rpc_addr = rpc.start_and_get_addr().await.unwrap();
    // Cluster mode. `route_decision` keys the Accord path on this being
    // anything but `Standalone`; the ring is deliberately empty, so
    // `accord_replicas_for_key` returns `None` and the LWT replica set falls
    // back to the connected peers plus this node — a stable two-replica set.
    let ring = Arc::new(ArcSwap::from_pointee(
        ferrosa_cluster::ring::TokenRing::new(),
    ));
    state
        .cluster_state
        .store(Arc::new(ferrosa_cluster::ClusterStateHolder::Cluster(
            ferrosa_cluster::RaftClusterState::new(ring, node_id),
        )));
    Node {
        id,
        state,
        peers,
        rpc_addr,
        _held: (registry, rpc, dir),
    }
}

fn auth() -> AuthContext {
    AuthContext {
        role: "cassandra".into(),
        is_superuser: true,
        must_change_password: false,
    }
}

/// A real client request context: `LOCAL_QUORUM`, protocol v4, and NO serial
/// consistency — `route_decision` keys the LWT decision on the STATEMENT, not
/// on that optional protocol field, so an `IF` routes to Accord regardless.
fn ctx<'a>(auth: &'a AuthContext, ks: &'a Option<String>) -> RequestContext<'a> {
    RequestContext {
        auth,
        current_keyspace: ks,
        consistency: ConsistencyLevel::LocalQuorum,
        serial_consistency: None,
        paging: ferrosa_cql::paging::PagingParams::default(),
        client_address: "127.0.0.1:1".into(),
        protocol_version: 4,
    }
}

async fn run(node: &Node, cql: &str) -> Result<(), String> {
    let auth = auth();
    let ks = Some(KS.to_string());
    match route(
        &node.state,
        &ctx(&auth, &ks),
        ferrosa_cql::parser::parse(cql).unwrap(),
    )
    .await
    {
        Ok(RouteResult::Result(_)) => Ok(()),
        Ok(_) => Err(format!("{cql}: unexpected route result")),
        Err(e) => Err(format!("{cql}: {e}")),
    }
}

/// The row as the CQL client sees it, or `None` when the table holds no row for
/// the key. This is the VALUE the acceptance criterion is about.
async fn read_row(node: &Node) -> Option<(String, i64)> {
    let auth = auth();
    let ks = Some(KS.to_string());
    let stmt = match ferrosa_cql::parser::parse(&format!(
        "SELECT payload, version FROM {KS}.{TABLE} WHERE id = 1"
    ))
    .unwrap()
    {
        ferrosa_cql::ast::Statement::Select(s) => s,
        _ => unreachable!(),
    };
    let raw = route_select_raw(&node.state, &ctx(&auth, &ks), &stmt)
        .await
        .expect("SELECT must succeed");
    let row = raw.rows.first()?;
    let payload = match &row[0] {
        Some(CqlValue::Text(t)) => t.clone(),
        other => panic!("payload: {other:?}"),
    };
    let version = match &row[1] {
        Some(CqlValue::Bigint(v)) => *v,
        other => panic!("version: {other:?}"),
    };
    Some((payload, version))
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// Two nodes, both holding the schema, connected to each other. The fixture has
/// no replication coordinator, so each node is given the schema locally.
async fn cluster() -> (Node, Node) {
    let a = node().await;
    let b = node().await;
    for (from, to) in [(&a, &b), (&b, &a)] {
        from.peers
            .ensure_peer(to.id, &to.rpc_addr.to_string())
            .await
            .expect("connect the peers");
    }
    for n in [&a, &b] {
        run(
            n,
            &format!(
                "CREATE KEYSPACE {KS} WITH replication = \
                 {{'class': 'SimpleStrategy', 'replication_factor': 2}}"
            ),
        )
        .await
        .expect("CREATE KEYSPACE");
        run(
            n,
            &format!(
                "CREATE TABLE {KS}.{TABLE} (id int PRIMARY KEY, version bigint, payload text)"
            ),
        )
        .await
        .expect("CREATE TABLE");
    }
    (a, b)
}

/// Seed a byte-identical row on both replicas at a pinned `USING TIMESTAMP`.
/// The pin is what makes the two read-votes agree without a replication
/// coordinator: the read-vote compares serialized row BYTES, which include the
/// cell timestamps.
async fn seed_on_both(a: &Node, b: &Node) {
    let t0 = now_micros();
    for n in [a, b] {
        run(
            n,
            &format!(
                "INSERT INTO {KS}.{TABLE} (id, version, payload) VALUES (1, 1, 'p1') \
                 USING TIMESTAMP {t0}"
            ),
        )
        .await
        .expect("seed the row on both replicas");
    }
}

/// Assert the row survived as `expect`, then run `delete`, then assert it is
/// GONE. The pre-delete assertion is what keeps the post-delete one from being
/// vacuous.
async fn assert_plain_delete_removes_it(a: &Node, expect: (String, i64), delete: &str) {
    assert_eq!(
        read_row(a).await.as_ref(),
        Some(&expect),
        "the row must exist before the plain DELETE, or the assertion after it says nothing"
    );
    run(a, delete)
        .await
        .expect("the plain DELETE must be accepted");
    assert_eq!(
        read_row(a).await,
        None,
        "a plain DELETE was acknowledged but the row is still served"
    );
}

/// Case A — the POSITIVE CONTROL. Both writes are plain, so this pins that the
/// harness really can delete at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_delete_removes_a_plainly_inserted_row() {
    let (a, b) = cluster().await;
    seed_on_both(&a, &b).await;
    assert_plain_delete_removes_it(
        &a,
        ("p1".to_string(), 1),
        &format!("DELETE FROM {KS}.{TABLE} WHERE id = 1"),
    )
    .await;
}

/// Case B — the row's last write is a conditional `UPDATE ... IF version=1`
/// that reported `[applied]=true`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_delete_removes_a_row_last_written_by_a_conditional_update() {
    let (a, b) = cluster().await;
    seed_on_both(&a, &b).await;
    run(
        &a,
        &format!("UPDATE {KS}.{TABLE} SET version = 2, payload = 'p2' WHERE id = 1 IF version = 1"),
    )
    .await
    .expect("the conditional UPDATE must apply");
    assert_plain_delete_removes_it(
        &a,
        ("p2".to_string(), 2),
        &format!("DELETE FROM {KS}.{TABLE} WHERE id = 1"),
    )
    .await;
}

/// Case C — the row was written by `INSERT ... IF NOT EXISTS` itself, which
/// Accord replicated to both replicas. Nothing is seeded here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_delete_removes_a_row_written_by_insert_if_not_exists() {
    let (a, _b) = cluster().await;
    run(
        &a,
        &format!(
            "INSERT INTO {KS}.{TABLE} (id, version, payload) VALUES (1, 1, 'p1') IF NOT EXISTS"
        ),
    )
    .await
    .expect("INSERT ... IF NOT EXISTS must apply");
    assert_plain_delete_removes_it(
        &a,
        ("p1".to_string(), 1),
        &format!("DELETE FROM {KS}.{TABLE} WHERE id = 1"),
    )
    .await;
}

/// Case E — the anti-race control. Identical to case B except that the plain
/// DELETE is issued 3 s after the LWT. A sub-second ordering race cannot explain
/// a surviving row here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_delete_three_seconds_after_the_lwt_still_removes_the_row() {
    let (a, b) = cluster().await;
    seed_on_both(&a, &b).await;
    run(
        &a,
        &format!("UPDATE {KS}.{TABLE} SET version = 2, payload = 'p2' WHERE id = 1 IF version = 1"),
    )
    .await
    .expect("the conditional UPDATE must apply");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_plain_delete_removes_it(
        &a,
        ("p2".to_string(), 2),
        &format!("DELETE FROM {KS}.{TABLE} WHERE id = 1"),
    )
    .await;
}

/// Case D — the report's own control that already worked: a CONDITIONAL delete
/// (itself an LWT) removes the row. It is here so a regression that broke the
/// LWT write path in the other direction is caught too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conditional_delete_removes_a_row_last_written_by_a_conditional_update() {
    let (a, b) = cluster().await;
    seed_on_both(&a, &b).await;
    run(
        &a,
        &format!("UPDATE {KS}.{TABLE} SET version = 2, payload = 'p2' WHERE id = 1 IF version = 1"),
    )
    .await
    .expect("the conditional UPDATE must apply");
    assert_plain_delete_removes_it(
        &a,
        ("p2".to_string(), 2),
        &format!("DELETE FROM {KS}.{TABLE} WHERE id = 1 IF EXISTS"),
    )
    .await;
}
