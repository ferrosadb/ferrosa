//! A paged ORDER BY whose next page reaches a different coordinator.
//!
//! Drivers treat `paging_state` as portable. scylla-rust-driver keeps an
//! iterator on its first node only until that node's connection fails, then
//! retries the remaining pages on the next node in its plan with the same
//! paging state. A result cursor lives on the node that built it, so every
//! other node must forward the page request to that owner over internode and
//! relay the reply — and must refuse by name when it cannot.
//!
//! Each test builds two in-process nodes, A and B: a standalone storage
//! engine each, a real internode `RpcServer` + `PeerManager` on loopback, and
//! B connected to A. Only A holds rows; B has the schema. A cursor built on A
//! and paged through B can only return rows if B forwards to A.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cql::ast::{SelectStatement, Statement};
use ferrosa_cql::result_cursor::{
    ResultCursorConfig, ResultCursorPageHandler, ResultCursorRegistry,
};
use ferrosa_cql::router::{route, route_select_raw, RequestContext, SharedState};
use ferrosa_cql::test_util::{standalone_for_test_with, StandaloneOptions};
use ferrosa_cql::types::CqlValue;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::HandlerRegistry;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_schema::auth::role::AuthContext;
use uuid::Uuid;

const ROWS: usize = 230;
const PAGE: i32 = 50;
const QUERY: &str = "SELECT id, v FROM fw.t ORDER BY v";

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
    registry: Arc<HandlerRegistry>,
    rpc: Arc<RpcServer>,
    rpc_addr: SocketAddr,
    _dir: tempfile::TempDir,
}

fn net_config(capabilities: u32) -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised_capabilities: capabilities,
        ..NetConfig::default()
    }
}

/// A node advertising `capabilities`, serving forwarded cursor pages.
async fn node(capabilities: u32, cursor_config: ResultCursorConfig) -> Node {
    let id = Uuid::new_v4();
    let dir = tempfile::tempdir().unwrap();
    let net = net_config(capabilities);
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
            cursor_config,
            ..StandaloneOptions::default()
        },
    );
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(
        MsgType::ResultCursorPage,
        Arc::new(ResultCursorPageHandler::new(state.result_cursors.clone())),
    );
    let rpc = Arc::new(RpcServer::new(net, id, registry.clone()));
    let rpc_addr = rpc.start_and_get_addr().await.unwrap();
    Node {
        id,
        state,
        peers,
        registry,
        rpc,
        rpc_addr,
        _dir: dir,
    }
}

fn superuser() -> AuthContext {
    AuthContext {
        role: "cassandra".into(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn ctx<'a>(
    auth: &'a AuthContext,
    ks: &'a Option<String>,
    client: &str,
    paging_state: Option<Vec<u8>>,
) -> RequestContext<'a> {
    RequestContext {
        auth,
        current_keyspace: ks,
        consistency: ConsistencyLevel::One,
        serial_consistency: None,
        paging: ferrosa_cql::paging::PagingParams {
            page_size: Some(PAGE),
            paging_state,
        },
        client_address: client.to_string(),
        protocol_version: 4,
    }
}

async fn run(state: &SharedState, cql: &str) {
    let auth = superuser();
    let ks = Some("fw".to_string());
    let c = ctx(&auth, &ks, "setup:0", None);
    route(state, &c, ferrosa_cql::parser::parse(cql).unwrap())
        .await
        .unwrap_or_else(|e| panic!("{cql}: {e}"));
}

/// Schema on both nodes; rows (`v` a permutation of `0..ROWS`) on `owner` only.
async fn seed(owner: &Node, other: &Node) {
    for n in [owner, other] {
        run(
            &n.state,
            "CREATE KEYSPACE fw WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        )
        .await;
        run(&n.state, "CREATE TABLE fw.t (id int PRIMARY KEY, v int)").await;
    }
    for id in 0..ROWS {
        let v = (id * 7) % ROWS;
        run(
            &owner.state,
            &format!("INSERT INTO fw.t (id, v) VALUES ({id}, {v})"),
        )
        .await;
    }
}

/// Connect `from`'s peer manager to `to`'s internode server.
async fn connect(from: &Node, to: &Node) {
    from.peers
        .ensure_peer(to.id, &to.rpc_addr.to_string())
        .await
        .unwrap();
}

fn select() -> SelectStatement {
    match ferrosa_cql::parser::parse(QUERY).unwrap() {
        Statement::Select(s) => s,
        other => panic!("expected select, got {other:?}"),
    }
}

async fn page(
    node: &Node,
    client: &str,
    paging_state: Option<Vec<u8>>,
) -> Result<(Vec<i32>, Option<Vec<u8>>), String> {
    let auth = superuser();
    let ks = Some("fw".to_string());
    let c = ctx(&auth, &ks, client, paging_state);
    let raw = route_select_raw(&node.state, &c, &select())
        .await
        .map_err(|e| e.to_string())?;
    let v_idx = raw.column_names.iter().position(|c| c == "v").unwrap();
    let vs = raw
        .rows
        .iter()
        .map(|r| match &r[v_idx] {
            Some(CqlValue::Int(v)) => *v,
            other => panic!("expected int v, got {other:?}"),
        })
        .collect();
    Ok((vs, raw.paging_state))
}

fn assert_every_row_once_in_order(got: &[i32]) {
    assert_eq!(
        got,
        (0..ROWS as i32).collect::<Vec<_>>(),
        "every row exactly once, in ORDER BY order"
    );
}

async fn two_nodes() -> (Node, Node) {
    let a = node(
        ferrosa_net::handshake::LOCAL_CAPABILITIES,
        ResultCursorConfig::default(),
    )
    .await;
    let b = node(
        ferrosa_net::handshake::LOCAL_CAPABILITIES,
        ResultCursorConfig::default(),
    )
    .await;
    seed(&a, &b).await;
    connect(&b, &a).await;
    connect(&a, &b).await;
    (a, b)
}

/// (a) Page 1 on A, page 2 onward on B (alternating back to A): every row is
/// returned exactly once and in order. B holds no rows, so anything it
/// returns came from A's cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_fetched_through_another_coordinator_is_forwarded_to_the_owner() {
    let (a, b) = two_nodes().await;
    let (mut got, mut next) = page(&a, "client:1", None).await.unwrap();
    let mut via_b = 0;
    for i in 0..100 {
        let Some(token) = next.take() else { break };
        let target = if i % 3 == 2 { &a } else { &b };
        via_b += usize::from(std::ptr::eq(target, &b));
        let (rows, after) = page(target, "client:2", Some(token)).await.unwrap();
        got.extend(rows);
        next = after;
    }
    assert!(via_b >= 2, "most pages must go through B");
    assert_every_row_once_in_order(&got);
    assert_eq!(
        a.state.result_cursors.stats().open,
        0,
        "the exhausted cursor is gone"
    );
    a.rpc.shutdown(Duration::from_millis(50)).await;
    b.rpc.shutdown(Duration::from_millis(50)).await;
}

/// (b) A's client connection closes after page 1 (the driver's connection
/// broke). Within the close grace the page is still served, via B and via A.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_connections_cursor_is_still_served_within_the_grace() {
    let (a, b) = two_nodes().await;
    let (mut got, mut next) = page(&a, "client:1", None).await.unwrap();
    assert_eq!(a.state.result_cursors.close_owner("client:1"), 1);

    let (rows, after) = page(&b, "client:2", next.take()).await.unwrap();
    got.extend(rows);
    next = after;
    let (rows, after) = page(&a, "client:3", next.take()).await.unwrap();
    got.extend(rows);
    next = after;
    for _ in 0..100 {
        let Some(token) = next.take() else { break };
        let (rows, after) = page(&b, "client:2", Some(token)).await.unwrap();
        got.extend(rows);
        next = after;
    }
    assert_every_row_once_in_order(&got);
    a.rpc.shutdown(Duration::from_millis(50)).await;
    b.rpc.shutdown(Duration::from_millis(50)).await;
}

/// (c) After the close grace the cursor is gone, and a page fetched through
/// B gets the owner's named error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_the_grace_a_forwarded_page_gets_the_owners_named_error() {
    let (a, b) = two_nodes().await;
    let (_, next) = page(&a, "client:1", None).await.unwrap();
    a.state.result_cursors.close_owner("client:1");
    let grace = a.state.result_cursors.config().close_grace;
    let past = Instant::now() + grace + Duration::from_secs(1);
    assert_eq!(a.state.result_cursors.sweep_expired_at(past), 1);

    let err = page(&b, "client:2", next).await.unwrap_err();
    assert!(err.contains("no longer holds"), "{err}");
    assert!(
        err.contains(&a.id.to_string()),
        "the error names the owner: {err}"
    );
    a.rpc.shutdown(Duration::from_millis(50)).await;
    b.rpc.shutdown(Duration::from_millis(50)).await;
}

/// (d) The owner restarted (same host id, fresh registry): a page fetched
/// through B gets the named restart error, not rows and not a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_the_owner_restarts_a_forwarded_page_gets_the_named_error() {
    let (a, b) = two_nodes().await;
    let (_, next) = page(&a, "client:1", None).await.unwrap();
    let restarted = Arc::new(ResultCursorRegistry::new(
        ResultCursorConfig::default(),
        a.id,
    ));
    a.registry.register(
        MsgType::ResultCursorPage,
        Arc::new(ResultCursorPageHandler::new(restarted)),
    );
    let err = page(&b, "client:2", next).await.unwrap_err();
    assert!(err.contains("before it restarted"), "{err}");
    a.rpc.shutdown(Duration::from_millis(50)).await;
    b.rpc.shutdown(Duration::from_millis(50)).await;
}

/// The owner is gone: a page fetched through B fails with an error naming
/// the owner, promptly — never an empty "last" page.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_the_owner_is_gone_a_forwarded_page_fails_by_name() {
    let (a, b) = two_nodes().await;
    let (_, next) = page(&a, "client:1", None).await.unwrap();
    let a_id = a.id;
    a.rpc.shutdown(Duration::from_millis(50)).await;
    b.peers.remove_peer(a_id).await;
    let started = Instant::now();
    let err = page(&b, "client:2", next).await.unwrap_err();
    assert!(err.contains(&a_id.to_string()), "{err}");
    assert!(err.contains("cannot reach"), "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "no lane-timeout wait"
    );
    b.rpc.shutdown(Duration::from_millis(50)).await;
}

/// Mixed versions, new coordinator → old owner. An owner that did not
/// advertise `CAP_RESULT_CURSOR_PAGE` is never sent the new message type (an
/// old node drops the whole connection on an unknown type byte): the client
/// gets a named error and the internode connection stays up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_without_the_capability_is_never_sent_the_request() {
    let a = node(0, ResultCursorConfig::default()).await;
    let b = node(
        ferrosa_net::handshake::LOCAL_CAPABILITIES,
        ResultCursorConfig::default(),
    )
    .await;
    seed(&a, &b).await;
    connect(&b, &a).await;
    assert_eq!(b.peers.peer_capabilities(a.id).await, Some(0));

    let (_, next) = page(&a, "client:1", None).await.unwrap();
    let err = page(&b, "client:2", next.clone()).await.unwrap_err();
    assert!(err.contains("runs a version that cannot"), "{err}");
    assert!(
        b.peers.has_live_peer(a.id),
        "the internode connection must survive"
    );
    // The cursor is untouched on its owner, so the page is still served there.
    let (rows, _) = page(&a, "client:1", next).await.unwrap();
    assert_eq!(rows.len(), PAGE as usize);
    a.rpc.shutdown(Duration::from_millis(50)).await;
    b.rpc.shutdown(Duration::from_millis(50)).await;
}

/// (e) The scylla-driver retry path at the protocol level: page 1 over a real
/// CQL connection to A, then the connection BREAKS (dropped mid-iteration),
/// and the driver retries the remaining pages on B with the same paging
/// state. The iteration completes with every row once, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broken_connection_retry_on_another_node_completes_the_iteration() {
    use ferrosa_cql::client::CqlClient;
    use ferrosa_cql::server::{CqlServer, ServerConfig};

    let (a, b) = two_nodes().await;
    let cql_config = || ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        auth_disabled: true,
        ..ServerConfig::default()
    };
    let server_a = CqlServer::new(cql_config(), a.state.clone());
    let server_b = CqlServer::new(cql_config(), b.state.clone());
    let addr_a = server_a.start_background().await.unwrap();
    let addr_b = server_b.start_background().await.unwrap();

    let decode = |rows: &[ferrosa_cql::client::ResultRow]| -> Vec<i32> {
        rows.iter()
            .map(|r| {
                let bytes = r.columns[1].as_ref().expect("v is never null");
                i32::from_be_bytes(bytes.as_slice().try_into().unwrap())
            })
            .collect()
    };

    let mut on_a = CqlClient::connect(addr_a).await.unwrap();
    let first = on_a.query_page(QUERY, PAGE, None).await.unwrap();
    let mut got = decode(&first.rows);
    let mut next = first.paging_state;
    assert!(next.is_some());
    // BrokenConnection: the socket to A goes away mid-iteration.
    drop(on_a);
    let deadline = Instant::now() + Duration::from_secs(5);
    while a.state.result_cursors.stats().closed == 0 {
        assert!(
            Instant::now() < deadline,
            "A never ran the connection-close guard for its cursor"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        a.state.result_cursors.stats().parked,
        1,
        "the close grace keeps the cursor for the retry"
    );

    let mut on_b = CqlClient::connect(addr_b).await.unwrap();
    for _ in 0..100 {
        let Some(token) = next.take() else { break };
        let page = on_b.query_page(QUERY, PAGE, Some(&token)).await.unwrap();
        got.extend(decode(&page.rows));
        next = page.paging_state;
    }
    assert_every_row_once_in_order(&got);
    a.rpc.shutdown(Duration::from_millis(50)).await;
    b.rpc.shutdown(Duration::from_millis(50)).await;
}
