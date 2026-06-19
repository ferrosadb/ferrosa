//! Tests for the four minor Flight RPCs: `list_flights`, `poll_flight_info`,
//! `do_action`, and `list_actions`. Each mirrors the read-path harness: seed a
//! keyspace/table through the CQL engine, then drive the RPC with a bearer
//! token and assert on the real result (and that auth is enforced).

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{Action, Criteria, Empty, FlightDescriptor};
use futures::TryStreamExt;
use tonic::Request;

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
            expires_at: 4_102_444_800, // year 2100
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

fn ctx<'a>(auth: &'a ferrosa_schema::AuthContext, ks: &'a Option<String>) -> RequestContext<'a> {
    RequestContext {
        auth,
        current_keyspace: ks,
        consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
        serial_consistency: None,
        paging: ferrosa_cql::paging::PagingParams::default(),
        client_address: String::new(),
    }
}

async fn exec(state: &SharedState, cql: &str) {
    let auth = superuser();
    let ks: Option<String> = None;
    let stmt = ferrosa_cql::parser::parse(cql).unwrap_or_else(|e| panic!("parse {cql:?}: {e}"));
    route(state, &ctx(&auth, &ks), stmt)
        .await
        .unwrap_or_else(|e| panic!("route {cql:?}: {e}"));
}

async fn seeded_service() -> (FerrosaFlight, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    exec(
        &state,
        "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
    )
    .await;
    exec(&state, "CREATE TABLE ks.t (id int PRIMARY KEY, name text)").await;
    exec(&state, "CREATE TABLE ks.other (id int PRIMARY KEY)").await;
    (FerrosaFlight::new(Arc::clone(&state), KEY.to_vec()), dir)
}

// ---- list_flights ----------------------------------------------------------

#[tokio::test]
async fn list_flights_enumerates_user_tables() {
    let (svc, _dir) = seeded_service().await;

    let resp = svc
        .list_flights(bearer(Criteria::default(), &superuser_token()))
        .await
        .expect("list_flights ok");
    let flights: Vec<_> = resp.into_inner().try_collect().await.expect("collect");

    // The two user tables must both appear; system keyspaces must not.
    let tickets: Vec<String> = flights
        .iter()
        .map(|f| String::from_utf8(f.endpoint[0].ticket.as_ref().unwrap().ticket.to_vec()).unwrap())
        .collect();
    assert!(
        tickets.iter().any(|t| t == "SELECT * FROM ks.t"),
        "ks.t flight present; got {tickets:?}"
    );
    assert!(
        tickets.iter().any(|t| t == "SELECT * FROM ks.other"),
        "ks.other flight present; got {tickets:?}"
    );
    assert!(
        tickets.iter().all(|t| t.contains("FROM ks.")),
        "only user-keyspace tables; got {tickets:?}"
    );
    // Each flight carries a real Arrow schema (non-empty IPC schema bytes).
    assert!(
        flights.iter().all(|f| !f.schema.is_empty()),
        "each FlightInfo carries an encoded schema"
    );
}

#[tokio::test]
async fn list_flights_respects_criteria_prefix() {
    let (svc, _dir) = seeded_service().await;

    let criteria = Criteria {
        expression: b"ks.t".to_vec().into(),
    };
    let resp = svc
        .list_flights(bearer(criteria, &superuser_token()))
        .await
        .expect("list_flights ok");
    let flights: Vec<_> = resp.into_inner().try_collect().await.expect("collect");
    let tickets: Vec<String> = flights
        .iter()
        .map(|f| String::from_utf8(f.endpoint[0].ticket.as_ref().unwrap().ticket.to_vec()).unwrap())
        .collect();

    // "ks.t" is a prefix of both "ks.t" and ... but "ks.other" does NOT start
    // with "ks.t", so it must be excluded.
    assert!(
        tickets.iter().any(|t| t == "SELECT * FROM ks.t"),
        "prefix match keeps ks.t; got {tickets:?}"
    );
    assert!(
        tickets.iter().all(|t| t != "SELECT * FROM ks.other"),
        "prefix match excludes ks.other; got {tickets:?}"
    );
}

#[tokio::test]
async fn list_flights_requires_bearer() {
    let (svc, _dir) = seeded_service().await;
    match svc.list_flights(Request::new(Criteria::default())).await {
        Ok(_) => panic!("unauthenticated list_flights must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::Unauthenticated),
    }
}

// ---- poll_flight_info ------------------------------------------------------

#[tokio::test]
async fn poll_flight_info_returns_completed_info() {
    let (svc, _dir) = seeded_service().await;

    let descriptor = FlightDescriptor::new_cmd(b"SELECT id, name FROM ks.t".to_vec());
    let resp = svc
        .poll_flight_info(bearer(descriptor, &superuser_token()))
        .await
        .expect("poll_flight_info ok");
    let poll = resp.into_inner();

    let info = poll.info.expect("PollInfo carries a FlightInfo");
    assert!(!info.schema.is_empty(), "info carries an encoded schema");
    assert_eq!(info.endpoint.len(), 1, "single self-endpoint");
    // v1 has no long-running poll: the result is complete, so no continuation
    // descriptor is returned and progress is 1.0.
    assert!(
        poll.flight_descriptor.is_none(),
        "completed poll has no continuation descriptor"
    );
    assert_eq!(
        poll.progress,
        Some(1.0),
        "completed poll reports progress 1.0"
    );
}

#[tokio::test]
async fn poll_flight_info_requires_bearer() {
    let (svc, _dir) = seeded_service().await;
    let descriptor = FlightDescriptor::new_cmd(b"SELECT id, name FROM ks.t".to_vec());
    match svc.poll_flight_info(Request::new(descriptor)).await {
        Ok(_) => panic!("unauthenticated poll_flight_info must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::Unauthenticated),
    }
}

// ---- list_actions ----------------------------------------------------------

#[tokio::test]
async fn list_actions_advertises_known_actions() {
    let (svc, _dir) = seeded_service().await;

    let resp = svc
        .list_actions(bearer(Empty {}, &superuser_token()))
        .await
        .expect("list_actions ok");
    let actions: Vec<_> = resp.into_inner().try_collect().await.expect("collect");

    let types: Vec<&str> = actions.iter().map(|a| a.r#type.as_str()).collect();
    assert!(
        types.contains(&"server.info"),
        "server.info advertised; got {types:?}"
    );
    assert!(
        types.contains(&"token.validate"),
        "token.validate advertised; got {types:?}"
    );
    assert!(
        actions.iter().all(|a| !a.description.is_empty()),
        "every action carries a description"
    );
}

#[tokio::test]
async fn list_actions_requires_bearer() {
    let (svc, _dir) = seeded_service().await;
    match svc.list_actions(Request::new(Empty {})).await {
        Ok(_) => panic!("unauthenticated list_actions must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::Unauthenticated),
    }
}

// ---- do_action -------------------------------------------------------------

#[tokio::test]
async fn do_action_server_info_returns_version() {
    let (svc, _dir) = seeded_service().await;

    let action = Action {
        r#type: "server.info".to_string(),
        body: Vec::new().into(),
    };
    let resp = svc
        .do_action(bearer(action, &superuser_token()))
        .await
        .expect("do_action ok");
    let results: Vec<_> = resp.into_inner().try_collect().await.expect("collect");
    assert_eq!(results.len(), 1, "one result body");
    let body = String::from_utf8(results[0].body.to_vec()).unwrap();
    assert!(
        body.contains(env!("CARGO_PKG_VERSION")),
        "server.info reports the crate version; got {body:?}"
    );
    assert!(
        body.contains("ferrosa-flight"),
        "server.info names the service; got {body:?}"
    );
}

#[tokio::test]
async fn do_action_token_validate_reports_role() {
    let (svc, _dir) = seeded_service().await;

    let action = Action {
        r#type: "token.validate".to_string(),
        body: Vec::new().into(),
    };
    let resp = svc
        .do_action(bearer(action, &superuser_token()))
        .await
        .expect("do_action ok");
    let results: Vec<_> = resp.into_inner().try_collect().await.expect("collect");
    assert_eq!(results.len(), 1, "one result body");
    let body = String::from_utf8(results[0].body.to_vec()).unwrap();
    assert!(
        body.contains("cassandra"),
        "token.validate echoes the verified role; got {body:?}"
    );
}

#[tokio::test]
async fn do_action_rejects_unknown_type() {
    let (svc, _dir) = seeded_service().await;

    let action = Action {
        r#type: "no.such.action".to_string(),
        body: Vec::new().into(),
    };
    match svc.do_action(bearer(action, &superuser_token())).await {
        Ok(_) => panic!("unknown action type must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::InvalidArgument),
    }
}

#[tokio::test]
async fn do_action_requires_bearer() {
    let (svc, _dir) = seeded_service().await;
    let action = Action {
        r#type: "server.info".to_string(),
        body: Vec::new().into(),
    };
    match svc.do_action(Request::new(action)).await {
        Ok(_) => panic!("unauthenticated do_action must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::Unauthenticated),
    }
}
