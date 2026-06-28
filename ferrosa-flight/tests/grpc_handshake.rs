//! Full-transport e2e: run the Flight service on a real tonic gRPC server,
//! connect a `FlightServiceClient`, perform a `Handshake` to obtain a bearer
//! token, then `do_get` a SELECT with that token and decode the Arrow stream.
//! Also asserts that an unauthenticated `do_get` is rejected over the wire.

use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, HandshakeRequest, Ticket};
use futures::{stream, TryStreamExt};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;

use ferrosa_common::{CqlType, CqlValue};
use ferrosa_cql::router::{route, RequestContext, SharedState};

async fn exec(state: &SharedState, cql: &str) {
    let auth = ferrosa_schema::AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    };
    let ks: Option<String> = None;
    let ctx = RequestContext {
        auth: &auth,
        current_keyspace: &ks,
        consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
        serial_consistency: None,
        paging: ferrosa_cql::paging::PagingParams::default(),
        client_address: String::new(),
        protocol_version: 4,
    };
    let stmt = ferrosa_cql::parser::parse(cql).unwrap_or_else(|e| panic!("parse {cql:?}: {e}"));
    route(state, &ctx, stmt)
        .await
        .unwrap_or_else(|e| panic!("route {cql:?}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_grpc_handshake_then_do_get() {
    // Seed a single-node keyspace/table with two rows.
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

    // Start the Flight server on an ephemeral port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = ferrosa_flight::server::flight_service(Arc::clone(&state), b"server-key".to_vec());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let mut client = FlightServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to flight server");

    // Handshake with the seeded default superuser -> bearer token.
    let handshake = stream::once(async {
        HandshakeRequest {
            protocol_version: 0,
            payload: b"cassandra\0cassandra".to_vec().into(),
        }
    });
    let mut hs_resp = client
        .handshake(handshake)
        .await
        .expect("handshake ok")
        .into_inner();
    let token_msg = hs_resp
        .message()
        .await
        .expect("handshake stream")
        .expect("a handshake response");
    let token = String::from_utf8(token_msg.payload.to_vec()).unwrap();
    assert!(!token.is_empty(), "handshake should issue a token");

    // Authenticated do_get streams the rows back as Arrow.
    let mut req = Request::new(Ticket {
        ticket: "SELECT id, name FROM ks.t".into(),
    });
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let data = client.do_get(req).await.expect("do_get ok").into_inner();
    let batches: Vec<_> = FlightRecordBatchStream::new_from_flight_data(
        data.map_err(|s| FlightError::ExternalError(Box::new(s))),
    )
    .try_collect()
    .await
    .expect("decode arrow stream");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "both rows round-trip over gRPC");

    // An unauthenticated do_get is rejected over the wire.
    let unauth = client
        .do_get(Request::new(Ticket {
            ticket: "SELECT id FROM ks.t".into(),
        }))
        .await;
    assert_eq!(
        unauth.unwrap_err().code(),
        tonic::Code::Unauthenticated,
        "missing bearer must be rejected"
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_put_writes_rows_then_do_get_reads_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    exec(
        &state,
        "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
    )
    .await;
    exec(&state, "CREATE TABLE ks.t (id int PRIMARY KEY, name text)").await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = ferrosa_flight::server::flight_service(Arc::clone(&state), b"server-key".to_vec());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let mut client = FlightServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    // Handshake for a bearer token.
    let handshake = stream::once(async {
        HandshakeRequest {
            protocol_version: 0,
            payload: b"cassandra\0cassandra".to_vec().into(),
        }
    });
    let token = String::from_utf8(
        client
            .handshake(handshake)
            .await
            .unwrap()
            .into_inner()
            .message()
            .await
            .unwrap()
            .unwrap()
            .payload
            .to_vec(),
    )
    .unwrap();

    // Build an Arrow batch (two rows) and encode it to FlightData.
    let batch = ferrosa_flight::convert::rows_to_record_batch(
        &["id".to_string(), "name".to_string()],
        &[CqlType::Int, CqlType::Varchar],
        &[
            vec![Some(CqlValue::Int(10)), Some(CqlValue::Text("ten".into()))],
            vec![
                Some(CqlValue::Int(20)),
                Some(CqlValue::Text("twenty".into())),
            ],
        ],
    )
    .unwrap();
    let flight_data: Vec<FlightData> = FlightDataEncoderBuilder::new()
        .build(stream::iter(vec![Ok::<_, FlightError>(batch)]))
        .try_collect()
        .await
        .unwrap();

    // DoPut the batch into ks.t.
    let mut put_req = Request::new(stream::iter(flight_data));
    put_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    put_req
        .metadata_mut()
        .insert("ferrosa-table", "ks.t".parse().unwrap());
    let put_resp = client.do_put(put_req).await.expect("do_put ok");
    let written = String::from_utf8(
        put_resp
            .into_inner()
            .message()
            .await
            .unwrap()
            .expect("a put result")
            .app_metadata
            .to_vec(),
    )
    .unwrap();
    assert_eq!(written, "2", "two rows written");

    // DoGet reads the written rows back.
    let mut get_req = Request::new(Ticket {
        ticket: "SELECT id, name FROM ks.t".into(),
    });
    get_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let data = client.do_get(get_req).await.unwrap().into_inner();
    let batches: Vec<_> = FlightRecordBatchStream::new_from_flight_data(
        data.map_err(|s| FlightError::ExternalError(Box::new(s))),
    )
    .try_collect()
    .await
    .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "both DoPut rows read back via DoGet");

    server.abort();
}
