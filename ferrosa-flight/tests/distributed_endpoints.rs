//! `GetFlightInfo` distributed-endpoint behavior (W-002).
//!
//! On a multi-range cluster topology, `get_flight_info` must return one
//! `FlightEndpoint` per token range, each carrying a DISTINCT token-bounded
//! `SELECT` ticket and the advertised Flight location(s) of the owning
//! replica(s). On a standalone topology (no ring) it must return exactly one
//! endpoint redeeming the original command — the prior behavior.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::FlightDescriptor;
use tonic::Request;

use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::TokenRing;
use ferrosa_cql::router::{route, RequestContext, SharedState};
use ferrosa_flight::service::FerrosaFlight;
use ferrosa_flight::token::{self, Claims};

const KEY: &[u8] = b"flight-test-signing-key";

fn superuser_token() -> String {
    token::issue(
        KEY,
        &Claims {
            role: "cassandra".to_string(),
            is_superuser: true,
            expires_at: 4_102_444_800,
        },
    )
}

fn bearer<T>(msg: T, token: &str) -> Request<T> {
    let mut req = Request::new(msg);
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    req
}

fn superuser() -> ferrosa_schema::AuthContext {
    ferrosa_schema::AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

async fn exec(state: &SharedState, cql: &str) {
    let auth = superuser();
    let ks: Option<String> = None;
    let ctx = RequestContext {
        auth: &auth,
        current_keyspace: &ks,
        consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
        serial_consistency: None,
        paging: ferrosa_cql::paging::PagingParams::default(),
        client_address: String::new(),
    };
    let stmt = ferrosa_cql::parser::parse(cql).unwrap_or_else(|e| panic!("parse {cql:?}: {e}"));
    route(state, &ctx, stmt)
        .await
        .unwrap_or_else(|e| panic!("route {cql:?}: {e}"));
}

fn ring_node(addr: &str) -> NodeInfo {
    NodeInfo {
        host_id: uuid::Uuid::new_v4(),
        addr: addr.to_string(),
        data_center: "dc1".to_string(),
        rack: "rack1".to_string(),
        state: NodeState::Normal,
        cql_broadcast: None,
    }
}

async fn seeded_state() -> (Arc<SharedState>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    exec(
        &state,
        "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
    )
    .await;
    exec(&state, "CREATE TABLE ks.t (id int PRIMARY KEY, name text)").await;
    exec(&state, "INSERT INTO ks.t (id, name) VALUES (1, 'alice')").await;
    (state, dir)
}

#[tokio::test]
async fn standalone_returns_single_endpoint() {
    let (state, _dir) = seeded_state().await;
    let svc = FerrosaFlight::new(Arc::clone(&state), KEY.to_vec());

    let info = svc
        .get_flight_info(bearer(
            FlightDescriptor::new_cmd("SELECT id, name FROM ks.t"),
            &superuser_token(),
        ))
        .await
        .expect("get_flight_info ok")
        .into_inner();

    assert_eq!(
        info.endpoint.len(),
        1,
        "standalone topology → exactly one endpoint"
    );
    // The single endpoint redeems the original command.
    let ticket = info.endpoint[0]
        .ticket
        .as_ref()
        .expect("endpoint has a ticket");
    assert_eq!(
        ticket.ticket.as_ref(),
        b"SELECT id, name FROM ks.t",
        "self endpoint carries the original command"
    );
}

#[tokio::test]
async fn multi_range_topology_returns_endpoint_per_range() {
    let (state, _dir) = seeded_state().await;

    // Install a 3-node ring so the table spans multiple token ranges.
    let mut ring = TokenRing::new();
    ring.add_node(1, ring_node("10.0.0.1:7000"));
    ring.add_node(2, ring_node("10.0.0.2:7000"));
    ring.add_node(3, ring_node("10.0.0.3:7000"));
    ring.assign_tokens(1, &[0]);
    ring.assign_tokens(2, &[100]);
    ring.assign_tokens(3, &[200]);
    state.core.mode_controller.set_token_ring(Arc::new(ring));

    let svc = FerrosaFlight::new(Arc::clone(&state), KEY.to_vec());

    let info = svc
        .get_flight_info(bearer(
            FlightDescriptor::new_cmd("SELECT id, name FROM ks.t"),
            &superuser_token(),
        ))
        .await
        .expect("get_flight_info ok")
        .into_inner();

    assert!(
        info.endpoint.len() > 1,
        "multi-range topology → more than one endpoint, got {}",
        info.endpoint.len()
    );

    // Every endpoint carries a DISTINCT, token-bounded ticket.
    let mut tickets = HashSet::new();
    for ep in &info.endpoint {
        let t = ep.ticket.as_ref().expect("endpoint has ticket");
        let cql = std::str::from_utf8(&t.ticket).expect("utf8 ticket");
        assert!(
            cql.contains("token(id) >") && cql.contains("token(id) <="),
            "range ticket carries token bounds: {cql}"
        );
        assert!(
            tickets.insert(cql.to_string()),
            "tickets are distinct: {cql}"
        );
    }

    // At least one endpoint advertises a replica Flight location.
    let with_location = info
        .endpoint
        .iter()
        .filter(|ep| !ep.location.is_empty())
        .count();
    assert!(
        with_location > 0,
        "endpoints advertise replica Flight locations"
    );
    // Locations look like Flight gRPC URIs on the ring's hosts.
    for ep in &info.endpoint {
        for loc in &ep.location {
            assert!(
                loc.uri.contains("10.0.0."),
                "location points at a ring host: {}",
                loc.uri
            );
        }
    }
}
