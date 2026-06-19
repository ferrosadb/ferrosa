//! Arrow Flight gRPC service — read path (`DoGet` / `GetFlightInfo` / `GetSchema`).
//!
//! A client places a CQL `SELECT` string in the Flight command/ticket; it is
//! executed by `ferrosa-cql` (`route_select_raw`) and the result is converted
//! to Arrow and streamed back. `DoPut` writes Arrow rows into the
//! `ferrosa-table` (`keyspace.table` metadata header) as CQL INSERTs.
//! `DoExchange` is a bidirectional streaming upsert: it consumes the same
//! inbound Arrow batches as `DoPut` and emits one `FlightData` acknowledgement
//! per batch (rows-applied count in `app_metadata`). `ListFlights`,
//! `PollFlightInfo`, `DoAction`, and `ListActions` are implemented.
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

    /// Extract and validate the `ferrosa-table` (`keyspace.table`) write target
    /// from request metadata. Shared by `do_put` and `do_exchange`.
    #[allow(clippy::result_large_err)] // tonic Status is the uniform RPC error type
    fn target_table(&self, metadata: &tonic::metadata::MetadataMap) -> Result<String, Status> {
        let table = metadata
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
        Ok(table)
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

    /// Build the `FlightInfo` for a CQL command `descriptor`: its Arrow schema
    /// plus a single self-endpoint whose ticket is the same command (the client
    /// redeems it via `do_get`). Shared by `get_flight_info` and
    /// `poll_flight_info`.
    #[allow(clippy::result_large_err)] // tonic Status is the uniform RPC error type
    async fn flight_info_for(
        &self,
        descriptor: FlightDescriptor,
        auth: &ferrosa_schema::AuthContext,
    ) -> Result<FlightInfo, Status> {
        let cql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor.cmd is not valid UTF-8 CQL"))?;
        let batch = self.query_to_batch(&cql, auth).await?;
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: descriptor.cmd.clone(),
        });
        FlightInfo::new()
            .try_with_schema(&batch.schema())
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))
            .map(|info| info.with_descriptor(descriptor).with_endpoint(endpoint))
    }

    /// The `SELECT * FROM ks.t` commands for every queryable (non-system) table,
    /// sorted for determinism, optionally filtered to tables whose `ks.table`
    /// name starts with `prefix`.
    fn queryable_table_commands(&self, prefix: &str) -> Vec<String> {
        let snapshot = self.state.schema.snapshot();
        let mut tables: Vec<(String, String)> = snapshot
            .tables
            .keys()
            .filter(|(ks, _)| !ferrosa_schema::is_system_keyspace(ks))
            .filter(|(ks, tbl)| format!("{ks}.{tbl}").starts_with(prefix))
            .cloned()
            .collect();
        tables.sort();
        tables
            .into_iter()
            .map(|(ks, tbl)| format!("SELECT * FROM {ks}.{tbl}"))
            .collect()
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

/// A `DoExchange` acknowledgement for one applied batch: an otherwise-empty
/// `FlightData` carrying the decimal rows-applied count in `app_metadata`
/// (same count convention as `do_put`'s `PutResult.app_metadata`).
fn ack_flight_data(written: i64) -> FlightData {
    FlightData::new().with_app_metadata(written.to_string().into_bytes())
}

/// Write every row of one decoded Arrow batch into `table` via the normal CQL
/// write path (`do_put` / `do_exchange` share this). Returns the number of rows
/// applied. Fail-loud: a conversion, parse, or write error propagates as
/// `Status` — a batch is never silently dropped.
#[allow(clippy::result_large_err)] // tonic Status is the uniform RPC error type
async fn write_batch(
    state: &SharedState,
    auth: &ferrosa_schema::AuthContext,
    table: &str,
    batch: &RecordBatch,
) -> Result<i64, Status> {
    let (names, rows) = crate::convert::record_batch_to_rows(batch)
        .map_err(|e| Status::invalid_argument(format!("Arrow conversion failed: {e}")))?;
    let mut written: i64 = 0;
    for row in &rows {
        let Some(sql) = build_insert(table, &names, row)? else {
            continue;
        };
        let stmt = ferrosa_cql::parser::parse(&sql)
            .map_err(|e| Status::internal(format!("generated CQL parse error: {e}")))?;
        let ks: Option<String> = None;
        let ctx = RequestContext {
            auth,
            current_keyspace: &ks,
            consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
            serial_consistency: None,
            paging: ferrosa_cql::paging::PagingParams::default(),
            client_address: String::new(),
        };
        ferrosa_cql::router::route(state, &ctx, stmt)
            .await
            .map_err(|e| Status::internal(format!("write failed: {e}")))?;
        written += 1;
    }
    Ok(written)
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
        // Single endpoint: the client redeems the same CQL ticket on this
        // connection (distributed per-token-range endpoints are W-002).
        let info = self.flight_info_for(request.into_inner(), &auth).await?;
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

    /// Enumerate the queryable (non-system) tables as `FlightInfo` entries: one
    /// per table, each carrying its Arrow schema and a `SELECT * FROM ks.t`
    /// ticket that `do_get` accepts. An optional `Criteria.expression` (UTF-8)
    /// filters to tables whose `ks.table` name starts with that prefix.
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let prefix = String::from_utf8(request.into_inner().expression.to_vec())
            .map_err(|_| Status::invalid_argument("criteria expression is not valid UTF-8"))?;

        let mut flights = Vec::new();
        for cql in self.queryable_table_commands(&prefix) {
            let descriptor = FlightDescriptor::new_cmd(cql.into_bytes());
            flights.push(self.flight_info_for(descriptor, &auth).await?);
        }
        let stream = futures::stream::iter(flights.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream) as Self::ListFlightsStream))
    }

    /// Resolve a query descriptor to its `FlightInfo` (the same one
    /// `get_flight_info` produces) and report it complete: the result is ready
    /// immediately, so progress is `1.0` and there is no continuation
    /// descriptor (long-running async polling is W-002).
    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let info = self.flight_info_for(request.into_inner(), &auth).await?;
        let poll = PollInfo {
            info: Some(info),
            flight_descriptor: None,
            progress: Some(1.0),
            expiration_time: None,
        };
        Ok(Response::new(poll))
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
        let table = self.target_table(request.metadata())?;

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
            written += write_batch(&self.state, &auth, &table, &batch).await?;
        }

        let result = arrow_flight::PutResult {
            app_metadata: written.to_string().into_bytes().into(),
        };
        Ok(Response::new(
            Box::pin(futures::stream::once(async move { Ok(result) })) as Self::DoPutStream,
        ))
    }

    /// Bidirectional streaming upsert with per-batch acknowledgement. The client
    /// pushes a stream of `FlightData` (decoded to `RecordBatch`es); each batch's
    /// rows are written into the `ferrosa-table` (`keyspace.table` header) via the
    /// same write path as `do_put`, and for every processed batch the server emits
    /// one outbound `FlightData` whose `app_metadata` is the decimal rows-applied
    /// count for that batch. Fail-loud: a decode/conversion/write error ends the
    /// stream with a `Status` rather than silently dropping a batch.
    ///
    /// Out of scope (post-v1): a live-subscribe channel over `DoExchange`.
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let table = self.target_table(request.metadata())?;
        let state = Arc::clone(&self.state);

        let inbound = request
            .into_inner()
            .map_err(|s| FlightError::ExternalError(Box::new(s)));
        let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(inbound);

        // One ack per inbound batch. `unfold` keeps the upsert strictly ordered
        // (apply batch N, ack N, then poll batch N+1) and stops on the first
        // error, so a failure is never hidden behind a later "success" ack.
        let acks = futures::stream::unfold(batches, move |mut batches| {
            let auth = auth.clone();
            let table = table.clone();
            let state = Arc::clone(&state);
            async move {
                let batch = match batches.try_next().await {
                    Ok(Some(b)) => b,
                    Ok(None) => return None,
                    Err(e) => {
                        return Some((
                            Err(Status::invalid_argument(format!(
                                "Flight decode error: {e}"
                            ))),
                            batches,
                        ))
                    }
                };
                let ack = write_batch(&state, &auth, &table, &batch)
                    .await
                    .map(ack_flight_data);
                Some((ack, batches))
            }
        });

        Ok(Response::new(Box::pin(acks) as Self::DoExchangeStream))
    }

    /// Handle a supported action (see [`SUPPORTED_ACTIONS`]). `server.info`
    /// reports the service name + crate version; `token.validate` echoes the
    /// verified bearer identity. Both require auth; any other type is rejected
    /// with `InvalidArgument`.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let auth = self.authenticate(request.metadata())?;
        let action = request.into_inner();
        let body: Vec<u8> = match action.r#type.as_str() {
            "server.info" => format!(
                "{{\"service\":\"ferrosa-flight\",\"version\":\"{}\"}}",
                env!("CARGO_PKG_VERSION")
            )
            .into_bytes(),
            "token.validate" => format!(
                "{{\"role\":\"{}\",\"is_superuser\":{}}}",
                auth.role, auth.is_superuser
            )
            .into_bytes(),
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown action type {other:?}"
                )))
            }
        };
        let result = arrow_flight::Result { body: body.into() };
        Ok(Response::new(
            Box::pin(futures::stream::once(async move { Ok(result) })) as Self::DoActionStream,
        ))
    }

    /// Advertise the actions [`do_action`](Self::do_action) supports.
    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.authenticate(request.metadata())?;
        let actions: Vec<ActionType> = SUPPORTED_ACTIONS
            .iter()
            .map(|(r#type, description)| ActionType {
                r#type: (*r#type).to_string(),
                description: (*description).to_string(),
            })
            .collect();
        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream) as Self::ListActionsStream))
    }
}

/// Action types `do_action` handles, with their descriptions. `list_actions`
/// advertises exactly this set, so the two stay in lock-step.
const SUPPORTED_ACTIONS: &[(&str, &str)] = &[
    (
        "server.info",
        "Return the Flight service name and crate version as JSON.",
    ),
    (
        "token.validate",
        "Validate the presented bearer token and return its verified role.",
    ),
];
