//! Flight gRPC server bootstrap.
//!
//! `flight_service` wraps [`FerrosaFlight`] in the generated
//! `FlightServiceServer` so callers can mount it on a `tonic` server (with
//! their own TLS / shutdown / incoming wiring); `serve` is the simple
//! bind-and-run path used by the binary.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;

use ferrosa_cql::router::SharedState;

use crate::service::FerrosaFlight;

/// Wrap the Flight service in its gRPC server adapter, ready to add to a
/// `tonic::transport::Server` (or serve over a custom incoming stream in tests).
pub fn flight_service(
    state: Arc<SharedState>,
    signing_key: Vec<u8>,
) -> FlightServiceServer<FerrosaFlight> {
    FlightServiceServer::new(FerrosaFlight::new(state, signing_key))
}

/// Serve a pre-configured Flight service (e.g. with key rotation / custom TTL).
pub async fn serve_service(
    addr: SocketAddr,
    service: FerrosaFlight,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await
}

/// Serve the Flight endpoint on `addr` until the future is dropped/cancelled.
///
/// Auth is enforced per-RPC (see [`crate::service`]); the endpoint is only
/// anonymous-safe because every read RPC requires a verified bearer token.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<SharedState>,
    signing_key: Vec<u8>,
) -> Result<(), tonic::transport::Error> {
    serve_service(addr, FerrosaFlight::new(state, signing_key)).await
}
