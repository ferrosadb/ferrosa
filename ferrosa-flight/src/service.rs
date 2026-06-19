//! Arrow Flight gRPC service — read path (`DoGet` / `GetFlightInfo` / `GetSchema`).
//!
//! A client places a CQL `SELECT` string in the Flight command/ticket; it is
//! executed by `ferrosa-cql` (`route_select_raw`) and the result is converted
//! to Arrow and streamed back. `DoPut` writes Arrow rows into the
//! `ferrosa-table` (`keyspace.table` metadata header) as CQL INSERTs.
//! `DoExchange` and `ListFlights` remain `Unimplemented`.
//!
//! Auth (decision D4): `Handshake` validates CQL credentials via
//! `Schema::authenticate` and returns a signed bearer token; every other RPC
//! requires `authorization: Bearer <token>`, and the request's `AuthContext` is
//! derived from the verified token claims — there is no anonymous access.
//!
//! `DoGet` streams the result one page at a time (FMEA F5), so peak memory is
//! bounded to a single page rather than the whole result set.

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, SchemaResult, Ticket,
};
use futures::{Stream, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use ferrosa_common::CqlValue;
use ferrosa_cql::ast::Statement;
use ferrosa_cql::router::{RequestContext, SharedState};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Lifetime of a bearer token issued by `Handshake`, in seconds.
const TOKEN_TTL_SECS: u64 = 3600;

/// Rows per page for `DoGet` streaming. Bounds peak memory to one page rather
/// than materializing the whole result set.
const DEFAULT_PAGE_SIZE: usize = 1024;

/// Ferrosa's Arrow Flight service. Executes CQL queries and streams Arrow.
pub struct FerrosaFlight {
    state: Arc<SharedState>,
    /// HMAC keys for bearer tokens. `[0]` is the current key (used to sign new
    /// tokens); all keys are accepted on verify, giving a rotation overlap
    /// window during which tokens signed by a just-retired key still validate.
    signing_keys: Vec<Vec<u8>>,
    /// Lifetime of issued tokens, seconds.
    token_ttl_secs: u64,
    /// Rows per `DoGet` page (peak-memory bound).
    page_size: usize,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl FerrosaFlight {
    /// Build the service over the shared CQL execution state and the current
    /// token-signing key.
    pub fn new(state: Arc<SharedState>, signing_key: Vec<u8>) -> Self {
        Self {
            state,
            signing_keys: vec![signing_key],
            token_ttl_secs: TOKEN_TTL_SECS,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    /// Add previous signing keys still accepted on verify (rotation overlap).
    /// New tokens are always signed with the current key (`new`'s key).
    pub fn with_previous_keys(mut self, previous: impl IntoIterator<Item = Vec<u8>>) -> Self {
        self.signing_keys.extend(previous);
        self
    }

    /// Override the issued-token TTL (seconds). Clamped to >= 1.
    pub fn with_token_ttl(mut self, secs: u64) -> Self {
        self.token_ttl_secs = secs.max(1);
        self
    }

    /// Override the `DoGet` page size (rows per streamed batch). Clamped to >= 1.
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size.max(1);
        self
    }

    /// Verify the `authorization: Bearer <token>` header and derive the
    /// request's `AuthContext`. Missing/invalid/expired -> `Unauthenticated`.
    // `Status` is tonic's standard (large) error type, used uniformly by every
    // RPC; boxing it here would just force unboxing at the `?` call sites.
    #[allow(clippy::result_large_err)]
    fn authenticate(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<ferrosa_schema::AuthContext, Status> {
        let header = metadata
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        let token = header
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid authorization header"))?
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("expected 'Bearer <token>'"))?;
        let claims = crate::token::verify_with_keys(&self.signing_keys, token, now_secs())
            .map_err(|e| Status::unauthenticated(format!("authentication failed: {e}")))?;
        Ok(ferrosa_schema::AuthContext {
            role: claims.role,
            is_superuser: claims.is_superuser,
            must_change_password: false,
        })
    }

    /// Parse a Flight command, requiring a `SELECT`.
    #[allow(clippy::result_large_err)] // tonic Status is the uniform RPC error type
    fn parse_select(cql: &str) -> Result<ferrosa_cql::ast::SelectStatement, Status> {
        match ferrosa_cql::parser::parse(cql)
            .map_err(|e| Status::invalid_argument(format!("CQL parse error: {e}")))?
        {
            Statement::Select(s) => Ok(s),
            _ => Err(Status::invalid_argument(
                "Flight command must be a SELECT statement",
            )),
        }
    }

    /// Execute a CQL SELECT under `auth` and convert one (small) page to a single
    /// Arrow batch — used by `GetFlightInfo`/`GetSchema`, which only need the
    /// schema, so this fetches just one page rather than the whole result.
    async fn query_to_batch(
        &self,
        cql: &str,
        auth: &ferrosa_schema::AuthContext,
    ) -> Result<RecordBatch, Status> {
        let select = Self::parse_select(cql)?;
        let ks: Option<String> = None;
        let ctx = RequestContext {
            auth,
            current_keyspace: &ks,
            consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
            serial_consistency: None,
            paging: ferrosa_cql::paging::PagingParams {
                page_size: Some(1),
                paging_state: None,
            },
            client_address: String::new(),
        };

        let raw = ferrosa_cql::router::route_select_raw(&self.state, &ctx, &select)
            .await
            .map_err(|e| Status::internal(format!("query execution failed: {e}")))?;

        crate::convert::rows_to_record_batch(&raw.column_names, &raw.column_types, &raw.rows)
            .map_err(|e| Status::internal(format!("Arrow conversion failed: {e}")))
    }
}

/// Paging cursor state for the streaming `DoGet`.
enum Page {
    /// Fetch the next page using this continuation (None on the first page).
    Fetch(Option<Vec<u8>>),
    /// All pages delivered.
    Done,
}

/// A CQL identifier: leading letter/underscore, then alphanumerics/underscores.
/// Used to reject injection via column/table names (which are interpolated into
/// generated INSERTs, unlike values which are escaped literals).
fn valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `keyspace.table`, both valid identifiers.
fn valid_table(t: &str) -> bool {
    matches!(t.split_once('.'), Some((ks, tbl)) if valid_ident(ks) && valid_ident(tbl))
}

/// Render a CQL value as a literal for a generated INSERT. Strings are escaped
/// (`'` -> `''`) and blobs hex-encoded, so interpolation is injection-safe.
/// `None` for NULL/non-finite-float/unsupported -> the column is omitted.
fn cql_literal(v: &CqlValue) -> Option<String> {
    Some(match v {
        CqlValue::Int(n) => n.to_string(),
        CqlValue::Bigint(n) | CqlValue::Counter(n) | CqlValue::Timestamp(n) => n.to_string(),
        CqlValue::Smallint(n) => n.to_string(),
        CqlValue::Tinyint(n) => n.to_string(),
        CqlValue::Boolean(b) => b.to_string(),
        CqlValue::Float(bits) => {
            let f = f32::from_bits(*bits);
            if !f.is_finite() {
                return None;
            }
            format!("{f}")
        }
        CqlValue::Double(bits) => {
            let f = f64::from_bits(*bits);
            if !f.is_finite() {
                return None;
            }
            format!("{f}")
        }
        CqlValue::Text(s) | CqlValue::Ascii(s) => format!("'{}'", s.replace('\'', "''")),
        CqlValue::Blob(b) => format!("0x{}", hex::encode(b)),
        _ => return None,
    })
}

/// Build an `INSERT INTO <table> (...) VALUES (...)` for one row, including only
/// the present, representable columns. `Ok(None)` if the row has nothing to
/// write. Errors on an invalid column identifier (injection guard).
#[allow(clippy::result_large_err)] // tonic Status is the uniform RPC error type
fn build_insert(
    table: &str,
    names: &[String],
    row: &[Option<CqlValue>],
) -> Result<Option<String>, Status> {
    let mut cols = Vec::new();
    let mut vals = Vec::new();
    for (name, cell) in names.iter().zip(row) {
        let Some(value) = cell else { continue };
        let Some(lit) = cql_literal(value) else {
            continue;
        };
        if !valid_ident(name) {
            return Err(Status::invalid_argument(format!(
                "invalid column identifier {name:?}"
            )));
        }
        cols.push(name.as_str());
        vals.push(lit);
    }
    if cols.is_empty() {
        return Ok(None);
    }
    Ok(Some(format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        cols.join(", "),
        vals.join(", ")
    )))
}

#[tonic::async_trait]
impl FlightService for FerrosaFlight {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<arrow_flight::PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    /// Stream the result of the CQL query carried in the ticket, one page at a
    /// time, so peak memory is bounded to a single page rather than the whole
    /// result. The first page is always emitted (even with zero rows) so the
    /// client still receives the schema.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let cql = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket is not valid UTF-8 CQL"))?;
        // Validate it is a SELECT up front so a bad command fails the call (not
        // mid-stream).
        let select = Self::parse_select(&cql)?;

        let state = Arc::clone(&self.state);
        let page_size = self.page_size as i32;

        let batches = futures::stream::unfold(Page::Fetch(None), move |page| {
            let state = Arc::clone(&state);
            let select = select.clone();
            let auth = auth.clone();
            async move {
                let cursor = match page {
                    Page::Done => return None,
                    Page::Fetch(cursor) => cursor,
                };
                let ks: Option<String> = None;
                let ctx = RequestContext {
                    auth: &auth,
                    current_keyspace: &ks,
                    consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
                    serial_consistency: None,
                    paging: ferrosa_cql::paging::PagingParams {
                        page_size: Some(page_size),
                        paging_state: cursor,
                    },
                    client_address: String::new(),
                };
                match ferrosa_cql::router::route_select_raw(&state, &ctx, &select).await {
                    Ok(page_res) => {
                        let next = match page_res.paging_state {
                            Some(c) => Page::Fetch(Some(c)),
                            None => Page::Done,
                        };
                        let batch = crate::convert::rows_to_record_batch(
                            &page_res.column_names,
                            &page_res.column_types,
                            &page_res.rows,
                        )
                        .map_err(|e| FlightError::ExternalError(e.to_string().into()));
                        Some((batch, next))
                    }
                    Err(e) => Some((
                        Err(FlightError::ExternalError(
                            format!("query execution failed: {e}").into(),
                        )),
                        Page::Done,
                    )),
                }
            }
        });

        // Schema is inferred from the first emitted batch (always present).
        let flight_data = FlightDataEncoderBuilder::new()
            .build(batches)
            .map_err(|e| Status::internal(format!("Flight encode error: {e}")));

        Ok(Response::new(Box::pin(flight_data) as Self::DoGetStream))
    }

    /// Return the schema (and a single self-endpoint) for a CQL command.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let descriptor = request.into_inner();
        let cql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor.cmd is not valid UTF-8 CQL"))?;
        let batch = self.query_to_batch(&cql, &auth).await?;
        let schema = batch.schema();

        // Single endpoint: the client redeems the same CQL ticket on this
        // connection (distributed per-token-range endpoints are W-002).
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: descriptor.cmd.clone(),
        });
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    /// Return just the Arrow schema for a CQL command.
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let cql = String::from_utf8(request.into_inner().cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor.cmd is not valid UTF-8 CQL"))?;
        let batch = self.query_to_batch(&cql, &auth).await?;
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let result = SchemaResult::try_from(arrow_flight::SchemaAsIpc::new(
            batch.schema().as_ref(),
            &options,
        ))
        .map_err(|e| Status::internal(format!("schema encode error: {e}")))?;
        Ok(Response::new(result))
    }

    /// Validate `username\0password` credentials and return a signed bearer
    /// token in the response payload for use on subsequent RPCs.
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut stream = request.into_inner();
        let msg = stream
            .message()
            .await?
            .ok_or_else(|| Status::unauthenticated("empty handshake stream"))?;

        let payload = msg.payload;
        let sep = payload
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| Status::unauthenticated("payload must be username\\0password"))?;
        let username = std::str::from_utf8(&payload[..sep])
            .map_err(|_| Status::unauthenticated("username is not valid UTF-8"))?;
        let password = std::str::from_utf8(&payload[sep + 1..])
            .map_err(|_| Status::unauthenticated("password is not valid UTF-8"))?;

        let auth = self
            .state
            .schema
            .authenticate(username, password)
            .map_err(|_| Status::unauthenticated("invalid credentials"))?;

        let claims = crate::token::Claims {
            role: auth.role,
            is_superuser: auth.is_superuser,
            expires_at: now_secs() + self.token_ttl_secs,
        };
        let token = crate::token::issue(&self.signing_keys[0], &claims);
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: token.into_bytes().into(),
        };
        Ok(Response::new(
            Box::pin(futures::stream::once(async move { Ok(response) })) as Self::HandshakeStream,
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }

    /// Write Arrow rows into `keyspace.table` (from the `ferrosa-table`
    /// metadata header). Each row becomes a CQL INSERT routed through the normal
    /// write path (key/token derivation, consistency, replication). Returns the
    /// number of rows written in the `PutResult` app metadata.
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let table = request
            .metadata()
            .get("ferrosa-table")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                Status::invalid_argument("missing 'ferrosa-table' metadata (keyspace.table)")
            })?
            .to_string();
        if !valid_table(&table) {
            return Err(Status::invalid_argument(
                "'ferrosa-table' must be keyspace.table identifiers",
            ));
        }

        let stream = request
            .into_inner()
            .map_err(|s| FlightError::ExternalError(Box::new(s)));
        let mut batches =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream);

        let mut written: i64 = 0;
        while let Some(batch) = batches
            .try_next()
            .await
            .map_err(|e| Status::invalid_argument(format!("Flight decode error: {e}")))?
        {
            let (names, rows) = crate::convert::record_batch_to_rows(&batch)
                .map_err(|e| Status::invalid_argument(format!("Arrow conversion failed: {e}")))?;
            for row in &rows {
                let Some(sql) = build_insert(&table, &names, row)? else {
                    continue;
                };
                let stmt = ferrosa_cql::parser::parse(&sql)
                    .map_err(|e| Status::internal(format!("generated CQL parse error: {e}")))?;
                let ks: Option<String> = None;
                let ctx = RequestContext {
                    auth: &auth,
                    current_keyspace: &ks,
                    consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
                    serial_consistency: None,
                    paging: ferrosa_cql::paging::PagingParams::default(),
                    client_address: String::new(),
                };
                ferrosa_cql::router::route(&self.state, &ctx, stmt)
                    .await
                    .map_err(|e| Status::internal(format!("write failed: {e}")))?;
                written += 1;
            }
        }

        let result = arrow_flight::PutResult {
            app_metadata: written.to_string().into_bytes().into(),
        };
        Ok(Response::new(
            Box::pin(futures::stream::once(async move { Ok(result) })) as Self::DoPutStream,
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "DoExchange not implemented (channel slice pending)",
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("DoAction not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("ListActions not implemented"))
    }
}
