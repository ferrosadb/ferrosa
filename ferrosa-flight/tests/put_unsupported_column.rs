//! FL-T016 / FM-65 / JB-T4: a `DoPut` whose batch carries a value the generated
//! INSERT cannot render must fail loud, naming the column, and must not write a
//! row without that column. Runs over a real tonic transport.

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

/// Two rows: a renderable one, then one whose `score` is NaN (no CQL literal).
async fn poisoned_wire_batch() -> Vec<FlightData> {
    let batch = ferrosa_flight::convert::rows_to_record_batch(
        &["id".to_string(), "score".to_string()],
        &[CqlType::Int, CqlType::Double],
        &[
            vec![
                Some(CqlValue::Int(1)),
                Some(CqlValue::Double(1.5f64.to_bits())),
            ],
            vec![
                Some(CqlValue::Int(2)),
                Some(CqlValue::Double(f64::NAN.to_bits())),
            ],
        ],
    )
    .unwrap();
    FlightDataEncoderBuilder::new()
        .build(stream::iter([Ok::<_, FlightError>(batch)]))
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_put_unsupported_column_fails_loud() {
    let dir = tempfile::tempdir().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    exec(
        &state,
        "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
    )
    .await;
    exec(
        &state,
        "CREATE TABLE ks.t (id int PRIMARY KEY, score double)",
    )
    .await;

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

    let handshake = stream::once(async {
        HandshakeRequest {
            protocol_version: 0,
            payload: b"ferrosa_admin\0ferrosa_admin".to_vec().into(),
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

    let mut req = Request::new(stream::iter(poisoned_wire_batch().await));
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    req.metadata_mut()
        .insert("ferrosa-table", "ks.t".parse().unwrap());

    // The failure may surface on the call or on the first response poll.
    let status = match client.do_put(req).await {
        Err(s) => s,
        Ok(resp) => resp
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .expect_err("put with an unrenderable column must fail"),
    };
    assert!(
        status.message().contains("score"),
        "error must name the column, got: {}",
        status.message()
    );

    // Nothing from the poisoned batch may have been written.
    let mut get_req = Request::new(Ticket {
        ticket: "SELECT id, score FROM ks.t".into(),
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
    assert_eq!(rows, 0, "no row may be written from a failed put batch");

    server.abort();
}
