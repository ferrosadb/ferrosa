//! Full-transport e2e for `DoExchange`: run the Flight service on a real tonic
//! gRPC server, handshake for a bearer token, open a bidirectional
//! `do_exchange` channel, push N Arrow `RecordBatch`es of rows into it, assert
//! we receive N acknowledgements (each carrying the rows-applied count in
//! `app_metadata`), then `do_get` the rows back and assert they were written.

use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, HandshakeRequest, Ticket};
use futures::{stream, StreamExt, TryStreamExt};
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

/// One (id, name) row as a single-row Arrow `RecordBatch`.
fn one_row_batch(id: i32, name: &str) -> arrow::record_batch::RecordBatch {
    ferrosa_flight::convert::rows_to_record_batch(
        &["id".to_string(), "name".to_string()],
        &[CqlType::Int, CqlType::Varchar],
        &[vec![
            Some(CqlValue::Int(id)),
            Some(CqlValue::Text(name.to_string())),
        ]],
    )
    .unwrap()
}

/// Encode `batches` into one FlightData stream (one schema message + one data
/// message per batch) — the wire form a real client sends to `do_exchange`.
async fn encode_batches(batches: Vec<arrow::record_batch::RecordBatch>) -> Vec<FlightData> {
    FlightDataEncoderBuilder::new()
        .build(stream::iter(batches.into_iter().map(Ok::<_, FlightError>)))
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_exchange_upserts_each_batch_and_acks() {
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

    // Handshake -> bearer token.
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

    // Three single-row batches -> one schema + three data messages on the wire.
    let inbound = encode_batches(vec![
        one_row_batch(100, "alpha"),
        one_row_batch(200, "beta"),
        one_row_batch(300, "gamma"),
    ])
    .await;
    let n_batches = 3;

    let mut req = Request::new(stream::iter(inbound));
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    req.metadata_mut()
        .insert("ferrosa-table", "ks.t".parse().unwrap());

    let acks: Vec<FlightData> = client
        .do_exchange(req)
        .await
        .expect("do_exchange ok")
        .into_inner()
        .map(|r| r.expect("ack stream item"))
        .collect()
        .await;

    assert_eq!(acks.len(), n_batches, "one ack per processed inbound batch");
    // Each ack carries the rows-applied count for its batch in app_metadata.
    for ack in &acks {
        let count = String::from_utf8(ack.app_metadata.to_vec()).expect("ack count utf8");
        assert_eq!(count, "1", "each batch applied exactly one row");
    }

    // The rows must actually be readable back via do_get.
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
    assert_eq!(rows, 3, "all three exchanged rows written and read back");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_exchange_requires_a_valid_bearer() {
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

    // No bearer token, with the table header present.
    let mut req = Request::new(stream::iter(
        encode_batches(vec![one_row_batch(1, "x")]).await,
    ));
    req.metadata_mut()
        .insert("ferrosa-table", "ks.t".parse().unwrap());

    // The unauthenticated status surfaces either on the call or on the first
    // stream poll, depending on transport timing.
    let outcome = client.do_exchange(req).await;
    let code = match outcome {
        Ok(resp) => resp
            .into_inner()
            .next()
            .await
            .expect("a stream item")
            .expect_err("unauthenticated exchange must error")
            .code(),
        Err(status) => status.code(),
    };
    assert_eq!(code, tonic::Code::Unauthenticated);

    server.abort();
}
