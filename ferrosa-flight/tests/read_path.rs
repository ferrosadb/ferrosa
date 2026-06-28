//! End-to-end test of the Flight read path: drive DDL/DML through the CQL
//! engine, then redeem a `SELECT` via `do_get` (with bearer auth) and decode the
//! streamed Arrow back into record batches.

use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
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
            expires_at: 4_102_444_800, // year 2100 — comfortably in the future
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
        protocol_version: 4,
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
    exec(&state, "INSERT INTO ks.t (id, name) VALUES (1, 'alice')").await;
    exec(&state, "INSERT INTO ks.t (id, name) VALUES (2, 'bob')").await;
    (FerrosaFlight::new(Arc::clone(&state), KEY.to_vec()), dir)
}

#[tokio::test]
async fn do_get_streams_select_result_as_arrow() {
    let (svc, _dir) = seeded_service().await;

    let resp = svc
        .do_get(bearer(
            Ticket {
                ticket: "SELECT id, name FROM ks.t".into(),
            },
            &superuser_token(),
        ))
        .await
        .expect("do_get ok");

    let inner = resp
        .into_inner()
        .map_err(|s| FlightError::ExternalError(Box::new(s)));
    let batches: Vec<_> = FlightRecordBatchStream::new_from_flight_data(inner)
        .try_collect()
        .await
        .expect("decode flight stream");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "two inserted rows should round-trip");
    assert_eq!(batches[0].num_columns(), 2, "id + name columns");
    assert_eq!(batches[0].schema().field(0).name(), "id");
    assert_eq!(batches[0].schema().field(1).name(), "name");
}

#[tokio::test]
async fn do_get_requires_a_valid_bearer() {
    let (svc, _dir) = seeded_service().await;
    let ticket = || Ticket {
        ticket: "SELECT id, name FROM ks.t".into(),
    };

    // No token at all.
    match svc.do_get(Request::new(ticket())).await {
        Ok(_) => panic!("unauthenticated do_get must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::Unauthenticated),
    }

    // Token signed with the wrong key.
    let forged = token::issue(
        b"not-the-server-key",
        &Claims {
            role: "cassandra".to_string(),
            is_superuser: true,
            expires_at: 4_102_444_800,
        },
    );
    match svc.do_get(bearer(ticket(), &forged)).await {
        Ok(_) => panic!("forged token must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::Unauthenticated),
    }
}

#[tokio::test]
async fn do_get_streams_multiple_pages() {
    let dir = tempfile::tempdir().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    exec(
        &state,
        "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
    )
    .await;
    exec(&state, "CREATE TABLE ks.t (id int PRIMARY KEY, name text)").await;
    for i in 1..=3 {
        exec(
            &state,
            &format!("INSERT INTO ks.t (id, name) VALUES ({i}, 'n{i}')"),
        )
        .await;
    }

    // page_size = 1 forces paging: the result must arrive as multiple batches,
    // not one materialized batch.
    let svc = FerrosaFlight::new(state, KEY.to_vec()).with_page_size(1);
    let resp = svc
        .do_get(bearer(
            Ticket {
                ticket: "SELECT id, name FROM ks.t".into(),
            },
            &superuser_token(),
        ))
        .await
        .expect("do_get ok");

    let inner = resp
        .into_inner()
        .map_err(|s| FlightError::ExternalError(Box::new(s)));
    let batches: Vec<_> = FlightRecordBatchStream::new_from_flight_data(inner)
        .try_collect()
        .await
        .expect("decode flight stream");

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "all rows stream across pages");
    assert!(
        batches.len() >= 2,
        "page_size=1 should stream multiple batches, got {}",
        batches.len()
    );
}

#[tokio::test]
async fn do_get_rejects_non_select() {
    let (svc, _dir) = seeded_service().await;
    // Authenticated, but the command is not a SELECT.
    match svc
        .do_get(bearer(
            Ticket {
                ticket: "INSERT INTO ks.t (id, name) VALUES (3, 'x')".into(),
            },
            &superuser_token(),
        ))
        .await
    {
        Ok(_) => panic!("non-SELECT ticket must be rejected"),
        Err(e) => assert_eq!(e.code(), tonic::Code::InvalidArgument),
    }
}
