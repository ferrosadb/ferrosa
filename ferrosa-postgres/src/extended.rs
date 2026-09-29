//! Per-connection extended-query session state: prepared statements + portals.
//!
//! The Postgres extended-query protocol (the path `tokio-postgres::query` and
//! every parameterized driver call uses) is a multi-message dance:
//!
//! ```text
//! Parse  ('P')  → prepare a named (or unnamed) statement from a SQL string
//! Bind   ('B')  → create a portal: a prepared statement + bound parameter values
//! Describe('D') → ask for the parameter / result-column shapes
//! Execute('E')  → run a portal, streaming DataRows
//! Sync   ('S')  → end the sequence; the server replies ReadyForQuery
//! Close  ('C')  → drop a named statement or portal
//! ```
//!
//! This module owns the per-connection [`Session`] store and the *pure* handlers
//! (Parse / Bind / Close / Describe-shape) that need no I/O. The async parts
//! (loading tables, executing a portal) live in the server's query loop, which
//! reads from this store. The empty-string name is the unnamed statement/portal.
//!
//! ## Error skipping (Postgres semantics)
//!
//! After any error inside an extended-query sequence, the backend ignores every
//! subsequent message until the next `Sync`, then emits `ReadyForQuery`. The
//! the `Session::error_pending` flag implements that skip; `Sync` clears it.

use std::collections::{HashMap, HashSet};

use ferrosa_sql::{
    parse_statement, DeleteStmt, InsertStmt, ScalarItem, ScalarValue, SelectStmt, Statement,
    UpdateStmt, Value as SqlValue,
};

use crate::messages::{BackendMessage, TransactionStatus};
use crate::mvcc::{MvccSnapshot, PgWrite};
use crate::query::{
    decode_param_checked, error_response, exec_error_response, row_description_fields,
};
use crate::result_stream::ResultStream;

/// What a prepared statement parses to: a table query, a no-`FROM` expression
/// query (`SELECT version()`, `SELECT 1`), or parameterized DML (`INSERT` /
/// `UPDATE` / `DELETE`). Transaction-control and session statements are handled
/// on the simple-query path, not prepared here. The statement variants are boxed
/// — `SelectStmt` (and, for size parity, the DML statements) are far larger than
/// the `Exprs` variant.
#[derive(Debug, Clone)]
pub enum PreparedKind {
    Select(Box<SelectStmt>),
    Exprs(Vec<ScalarItem>),
    Insert(Box<InsertStmt>),
    Update(Box<UpdateStmt>),
    Delete(Box<DeleteStmt>),
}

/// A prepared statement: the parsed query plus the client-declared parameter
/// type OIDs (used to decode bound values and to answer `Describe` for the
/// `ParameterDescription`). A `0` OID means "unspecified" (decode leniently).
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub parsed: PreparedKind,
    pub param_oids: Vec<i32>,
}

/// A bound portal: which statement it executes, the decoded parameter values,
/// and the result-column format codes the client requested.
#[derive(Debug, Clone)]
pub struct Portal {
    pub stmt_name: String,
    pub params: Vec<SqlValue>,
    pub result_formats: Vec<i16>,
}

/// Per-connection extended-query store.
pub struct Session {
    /// The authenticated role every statement on this connection is authorized
    /// against (t_e1c819ad). Set once at login; never defaulted.
    auth: ferrosa_schema::AuthContext,
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    /// Set when an error occurs mid-sequence; skip messages until `Sync`.
    error_pending: bool,
    /// Protocol-level transaction state, reported in every `ReadyForQuery`
    /// (`I`/`T`/`E`). Entering a `T` block starts PostgreSQL MVCC transaction
    /// state; Cassandra CQL transactions remain on Accord.
    txn: TransactionStatus,
    /// Explicit isolation mode for the current transaction. `None` means the
    /// session default.
    txn_isolation: Option<ferrosa_sql::IsolationLevel>,
    txn_snapshot: Option<MvccSnapshot>,
    txn_read_tables: HashSet<String>,
    /// Buffered DML write-set for the open `BEGIN`/`COMMIT` block. DML inside a
    /// transaction is BUFFERED here instead of applied; `COMMIT` drives the whole
    /// set through the PostgreSQL MVCC manager atomically; `ROLLBACK`/`end_txn` clears it
    /// so a discarded transaction never touches storage (FMEA PG-1).
    txn_writes: Vec<PgWrite>,
    /// Running queries of suspended portals (`Execute` with `max_rows` stopped
    /// short), keyed by portal name. Dropping an entry stops its executor.
    streams: HashMap<String, ResultStream>,
}

/// The format code (0 text / 1 binary) for parameter `i` under the Bind fan-out
/// rule: empty ⇒ all text; single ⇒ applies to all; else per-parameter.
fn param_format_for(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(0),
    }
}

impl Session {
    /// A fresh session for the authenticated role `auth`.
    pub fn new(auth: ferrosa_schema::AuthContext) -> Self {
        Self {
            auth,
            statements: HashMap::new(),
            portals: HashMap::new(),
            error_pending: false,
            txn: TransactionStatus::default(),
            txn_isolation: None,
            txn_snapshot: None,
            txn_read_tables: HashSet::new(),
            txn_writes: Vec::new(),
            streams: HashMap::new(),
        }
    }

    /// The role this session's statements are authorized against.
    pub fn auth(&self) -> &ferrosa_schema::AuthContext {
        &self.auth
    }

    /// Whether the session is currently skipping messages until the next `Sync`
    /// (an error occurred earlier in this extended sequence).
    pub fn is_error_pending(&self) -> bool {
        self.error_pending
    }

    /// Look up a portal by name (for the async Execute / Describe-portal paths).
    pub fn portal(&self, name: &str) -> Option<&Portal> {
        self.portals.get(name)
    }

    /// Look up a prepared statement by name.
    pub fn statement(&self, name: &str) -> Option<&PreparedStatement> {
        self.statements.get(name)
    }

    /// Overwrite a prepared statement's parameter type OIDs with the resolved
    /// (inferred) values. Called after `Describe('S')` so a subsequent `Bind`
    /// decodes binary parameters against the same OIDs the driver was told to
    /// serialize with. No-op if the statement is absent.
    pub fn set_param_oids(&mut self, name: &str, oids: Vec<i32>) {
        if let Some(stmt) = self.statements.get_mut(name) {
            stmt.param_oids = oids;
        }
    }

    /// Handle `Sync`: clear the error-skip flag. The caller then emits
    /// `ReadyForQuery`.
    ///
    /// Outside a transaction block the unit of work ends here, so suspended
    /// portals are released with it (PostgreSQL destroys them at the implicit
    /// commit). Inside a block they live until `Close`, a rebind of the same
    /// name, or the end of the session.
    pub fn on_sync(&mut self) {
        self.error_pending = false;
        if matches!(self.txn, TransactionStatus::Idle) {
            self.streams.clear();
        }
    }

    /// Take a suspended portal's running query, to continue it. The caller must
    /// [`Session::park_stream`] it again if it suspends once more.
    pub(crate) fn take_stream(&mut self, portal: &str) -> Option<ResultStream> {
        self.streams.remove(portal)
    }

    /// Park a query whose portal was suspended, so the next `Execute` resumes it.
    pub(crate) fn park_stream(&mut self, portal: String, stream: ResultStream) {
        self.streams.insert(portal, stream);
    }

    /// How many portals currently hold a running, suspended query.
    pub fn suspended_portals(&self) -> usize {
        self.streams.len()
    }

    /// The protocol transaction status to report in `ReadyForQuery`.
    pub fn txn_status(&self) -> TransactionStatus {
        self.txn
    }

    /// Whether the session is inside an open (non-failed) transaction block.
    pub fn in_txn(&self) -> bool {
        matches!(self.txn, TransactionStatus::InTransaction)
    }

    /// Whether the session is inside an aborted transaction block (only
    /// `COMMIT`/`ROLLBACK` are accepted until it ends — PG `25P02`).
    pub fn in_failed_txn(&self) -> bool {
        matches!(self.txn, TransactionStatus::Failed)
    }

    /// `BEGIN`: enter a transaction block and start a fresh empty write-set. A
    /// `BEGIN` while already in one keeps the session in-transaction (PG warns
    /// but stays `T`) and clears any buffered writes.
    pub(crate) fn begin_txn(
        &mut self,
        isolation: Option<ferrosa_sql::IsolationLevel>,
        snapshot: MvccSnapshot,
    ) {
        self.txn_writes.clear();
        self.txn_read_tables.clear();
        if matches!(self.txn, TransactionStatus::Idle) {
            self.txn = TransactionStatus::InTransaction;
            self.txn_isolation = isolation;
            self.txn_snapshot = Some(snapshot);
        }
    }

    /// `COMMIT`/`ROLLBACK`: leave the transaction block, back to idle, and drop
    /// the buffered write-set. After `end_txn` no buffered write survives, so a
    /// rolled-back (or committed) transaction never re-applies on the next one.
    pub fn end_txn(&mut self) {
        self.txn = TransactionStatus::Idle;
        self.txn_isolation = None;
        self.txn_snapshot = None;
        self.txn_read_tables.clear();
        self.txn_writes.clear();
    }

    pub fn txn_isolation(&self) -> Option<ferrosa_sql::IsolationLevel> {
        self.txn_isolation
    }

    /// Mutable handle to the open transaction's buffered write-set, for the DML
    /// path to push a PostgreSQL write into while in a `T` block.
    pub(crate) fn txn_writes_mut(&mut self) -> &mut Vec<PgWrite> {
        &mut self.txn_writes
    }

    pub(crate) fn txn_writes(&self) -> &[PgWrite] {
        &self.txn_writes
    }

    pub(crate) fn txn_snapshot(&self) -> Option<&MvccSnapshot> {
        self.txn_snapshot.as_ref()
    }

    pub(crate) fn txn_read_tables_mut(&mut self) -> &mut HashSet<String> {
        &mut self.txn_read_tables
    }

    pub(crate) fn take_txn_read_tables(&mut self) -> HashSet<String> {
        std::mem::take(&mut self.txn_read_tables)
    }

    /// Drain the buffered write-set, leaving it empty. Used by `COMMIT` to hand
    /// the whole set to the PostgreSQL MVCC commit path.
    pub(crate) fn take_txn_writes(&mut self) -> Vec<PgWrite> {
        std::mem::take(&mut self.txn_writes)
    }

    /// An error while executing a statement inside a transaction aborts it
    /// (`T` → `E`); a no-op outside a transaction.
    pub fn mark_txn_failed(&mut self) {
        if matches!(self.txn, TransactionStatus::InTransaction) {
            self.txn = TransactionStatus::Failed;
        }
    }

    /// Handle `Parse`: parse the SQL and store the prepared statement. On a
    /// parse error, set `error_pending` and return an `ErrorResponse` (42601) —
    /// no `ParseComplete`. On success return `ParseComplete`.
    pub fn on_parse(
        &mut self,
        stmt_name: String,
        query: &str,
        param_types: Vec<i32>,
    ) -> BackendMessage {
        let parsed = match parse_statement(query) {
            Ok(Statement::Select(select)) => PreparedKind::Select(select),
            Ok(Statement::SelectExprs(items)) => {
                // Parameterized expression selects need $N type inference with no
                // column to infer from — not supported via the extended protocol
                // yet. Fail loud rather than guess.
                if items
                    .iter()
                    .any(|it| matches!(it.value, ScalarValue::Param(_)))
                {
                    self.error_pending = true;
                    return error_response(
                        "0A000",
                        "$N parameters in expression selects are not supported via the \
                         extended-query protocol yet",
                    );
                }
                PreparedKind::Exprs(items)
            }
            // Parameterized DML: prepare the statement as-is. Bound `$N` values
            // are substituted at Execute (the declared `param_types` OIDs drive
            // the Bind-time decode; an unspecified OID decodes leniently). No
            // column-from-comparison inference like SELECT — Ecto declares the
            // param OIDs in Parse, which we honor.
            Ok(Statement::Insert(ins)) => PreparedKind::Insert(ins),
            Ok(Statement::Update(upd)) => PreparedKind::Update(upd),
            Ok(Statement::Delete(del)) => PreparedKind::Delete(del),
            Ok(_) => {
                // BEGIN/COMMIT/ROLLBACK/SET reach the backend via simple Query.
                self.error_pending = true;
                return error_response(
                    "0A000",
                    "only SELECT and INSERT/UPDATE/DELETE statements can be prepared via the \
                     extended-query protocol",
                );
            }
            Err(e) => {
                self.error_pending = true;
                return error_response("42601", &e.to_string());
            }
        };
        self.statements.insert(
            stmt_name,
            PreparedStatement {
                parsed,
                param_oids: param_types,
            },
        );
        BackendMessage::ParseComplete
    }

    /// Handle `Bind`: decode each parameter value against the prepared
    /// statement's declared OIDs (jsonb parameters are parsed under
    /// `jsonb_limits`, D14b) and store the portal. A missing prepared
    /// statement is a fail-loud error (26000, invalid_sql_statement_name); on
    /// success return `BindComplete`.
    #[allow(clippy::too_many_arguments)]
    pub fn on_bind(
        &mut self,
        portal: String,
        stmt_name: String,
        param_formats: &[i16],
        param_values: &[Option<Vec<u8>>],
        result_formats: Vec<i16>,
        jsonb_limits: &ferrosa_jsonb::Limits,
    ) -> BackendMessage {
        let Some(stmt) = self.statements.get(&stmt_name) else {
            self.error_pending = true;
            return error_response(
                "26000",
                &format!("prepared statement \"{stmt_name}\" does not exist"),
            );
        };

        if param_formats.len() > 1 && param_formats.len() != param_values.len() {
            self.error_pending = true;
            return error_response(
                "08P01",
                "Bind parameter format count must be zero, one, or match the parameter count",
            );
        }
        if param_formats.iter().any(|format| !matches!(format, 0 | 1))
            || result_formats.iter().any(|format| !matches!(format, 0 | 1))
        {
            self.error_pending = true;
            return error_response("08P01", "Bind format code must be 0 (text) or 1 (binary)");
        }

        let params: Result<Vec<SqlValue>, _> = param_values
            .iter()
            .enumerate()
            .map(|(i, bytes)| {
                let format = param_format_for(param_formats, i);
                // A declared OID is matched positionally; unspecified ⇒ 0.
                let oid = stmt.param_oids.get(i).copied().unwrap_or(0);
                decode_param_checked(format, oid, bytes.as_deref(), jsonb_limits)
            })
            .collect();
        let params = match params {
            Ok(params) => params,
            Err(err) => {
                self.error_pending = true;
                return error_response(err.sqlstate, &err.message);
            }
        };

        // Rebinding a name replaces the portal, and with it any suspended query.
        self.streams.remove(&portal);
        self.portals.insert(
            portal,
            Portal {
                stmt_name,
                params,
                result_formats,
            },
        );
        BackendMessage::BindComplete
    }

    /// Handle `Close`: drop the named statement (`S`) or portal (`P`). Always
    /// succeeds (closing an absent name is a no-op in Postgres) ⇒ `CloseComplete`.
    pub fn on_close(&mut self, kind: u8, name: &str) -> BackendMessage {
        match kind {
            b'S' => {
                self.statements.remove(name);
            }
            b'P' => {
                self.portals.remove(name);
                // Releases the running query of a suspended portal.
                self.streams.remove(name);
            }
            _ => {}
        }
        BackendMessage::CloseComplete
    }

    /// Record that an error occurred mid-sequence (skip until `Sync`) and return
    /// the given `ErrorResponse`. Used by the async handlers in the server loop.
    pub fn fail(&mut self, err: BackendMessage) -> BackendMessage {
        self.error_pending = true;
        err
    }

    /// Mark that an error occurred mid-sequence (skip until `Sync`) without
    /// constructing a response — for when the error message was produced
    /// elsewhere (e.g. by the shared result renderer).
    pub fn mark_error(&mut self) {
        self.error_pending = true;
    }
}

/// Build the `Describe('S')` reply for a prepared statement's result columns:
/// either a `RowDescription` (text-format here, the pre-Bind default) or
/// `NoData`. `columns` empty ⇒ `NoData`. Pure helper shared by the server loop.
pub fn describe_statement_rows(columns: &[ferrosa_sql::Column]) -> BackendMessage {
    if columns.is_empty() {
        BackendMessage::NoData
    } else {
        // Pre-Bind Describe reports text format (the portal's chosen result
        // formats aren't known until Bind).
        BackendMessage::RowDescription {
            fields: row_description_fields(columns, &[]),
        }
    }
}

/// Build the `Describe('P')` reply for a portal's result columns under its
/// chosen result formats: a `RowDescription`, or `NoData` if there are none.
pub fn describe_portal_rows(
    columns: &[ferrosa_sql::Column],
    result_formats: &[i16],
) -> BackendMessage {
    if columns.is_empty() {
        BackendMessage::NoData
    } else {
        BackendMessage::RowDescription {
            fields: row_description_fields(columns, result_formats),
        }
    }
}

/// Map a [`ferrosa_sql::ExecError`] from `describe` to a fail-loud error response
/// (re-exported convenience so the server loop need not reach into `query`).
pub fn describe_exec_error(err: &ferrosa_sql::ExecError) -> BackendMessage {
    exec_error_response(err)
}

/// Build a `ParameterDescription` from a prepared statement's declared OIDs.
pub fn parameter_description(param_oids: &[i32]) -> BackendMessage {
    BackendMessage::ParameterDescription {
        type_oids: param_oids.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_auth() -> ferrosa_schema::AuthContext {
        ferrosa_schema::AuthContext {
            role: "tester".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    #[test]
    fn parse_stores_statement_and_acks() {
        let mut s = Session::new(test_auth());
        let ack = s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![23]);
        assert!(matches!(ack, BackendMessage::ParseComplete));
        assert!(s.statement("st").is_some());
        assert_eq!(s.statement("st").unwrap().param_oids, vec![23]);
        assert!(!s.is_error_pending());
    }

    #[test]
    fn parse_error_sets_error_pending_and_no_parse_complete() {
        let mut s = Session::new(test_auth());
        let resp = s.on_parse("bad".into(), "SELCT garbage", vec![]);
        match resp {
            BackendMessage::ErrorResponse { fields } => {
                assert_eq!(fields[1], (b'C', "42601".to_string()));
            }
            other => panic!("expected ErrorResponse, got {other:?}"),
        }
        assert!(s.is_error_pending());
        assert!(s.statement("bad").is_none());
    }

    #[test]
    fn bind_decodes_binary_int_param_and_stores_portal() {
        let mut s = Session::new(test_auth());
        s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![23]);
        let ack = s.on_bind(
            String::new(), // unnamed portal
            "st".into(),   // statement
            &[1],          // binary param format
            &[Some(7i32.to_be_bytes().to_vec())],
            vec![1], // binary result format
            &crate::jsonb_wire::test_limits(),
        );
        assert!(matches!(ack, BackendMessage::BindComplete));
        let portal = s.portal("").expect("portal stored");
        assert_eq!(portal.stmt_name, "st");
        assert_eq!(portal.params, vec![SqlValue::Int(7)]);
        assert_eq!(portal.result_formats, vec![1]);
    }

    #[test]
    fn bind_missing_statement_fails_loud() {
        let mut s = Session::new(test_auth());
        let resp = s.on_bind(
            "".into(),
            "ghost".into(),
            &[],
            &[],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        assert!(matches!(
            resp,
            BackendMessage::ErrorResponse { ref fields } if fields[1] == (b'C', "26000".to_string())
        ));
        assert!(s.is_error_pending());
    }

    #[test]
    fn bind_rejects_malformed_value_instead_of_binding_null() {
        let mut s = Session::new(test_auth());
        s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![23]);

        let response = s.on_bind(
            "p".into(),
            "st".into(),
            &[0],
            &[Some(b"not-an-integer".to_vec())],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );

        assert!(matches!(
            response,
            BackendMessage::ErrorResponse { ref fields }
                if fields[1] == (b'C', "22P02".to_string())
        ));
        assert!(s.is_error_pending());
        assert!(
            s.portal("p").is_none(),
            "invalid values must not create a portal"
        );
    }

    /// Bind one parameter and return the ErrorResponse SQLSTATE, asserting the
    /// portal was not created and the session is in the error state.
    fn bind_error_code(oid: i32, format: i16, value: &[u8]) -> String {
        let mut s = Session::new(test_auth());
        s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![oid]);
        let response = s.on_bind(
            "p".into(),
            "st".into(),
            &[format],
            &[Some(value.to_vec())],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        assert!(s.is_error_pending(), "oid {oid} format {format}");
        assert!(s.portal("p").is_none(), "oid {oid} format {format}");
        match response {
            BackendMessage::ErrorResponse { fields } => fields[1].1.clone(),
            other => panic!("expected ErrorResponse, got {other:?}"),
        }
    }

    #[test]
    fn pg_param_parse_failure_is_22p02_not_null() {
        // Text format: int, uuid, timestamp (and the rest) are 22P02.
        assert_eq!(bind_error_code(23, 0, b"12x"), "22P02");
        assert_eq!(bind_error_code(2950, 0, b"not-a-uuid"), "22P02");
        assert_eq!(bind_error_code(1114, 0, b"2024-99-99 25:00:00"), "22P02");
        assert_eq!(bind_error_code(16, 0, b"maybe"), "22P02");
        assert_eq!(bind_error_code(25, 0, &[0xff, 0xfe]), "22P02");
        // Binary format: PG's invalid_binary_representation.
        assert_eq!(bind_error_code(23, 1, &[1, 2]), "22P03");
        assert_eq!(bind_error_code(2950, 1, &[0; 3]), "22P03");
        assert_eq!(bind_error_code(1114, 1, &[0; 4]), "22P03");
        // jsonb / json are mapped since T-161a: bad JSON is 22P02 (text and
        // binary), a bad version byte is 22P03.
        assert_eq!(bind_error_code(3802, 0, b"{"), "22P02");
        assert_eq!(bind_error_code(3802, 1, b"\x01{"), "22P02");
        assert_eq!(bind_error_code(3802, 1, b"\x02{}"), "22P03");
        assert_eq!(bind_error_code(114, 1, b"{"), "22P02");
        // An unmapped OID is refused at Bind, in both formats.
        assert_eq!(bind_error_code(4072, 0, b"$"), "42704");
        assert_eq!(bind_error_code(1184, 1, b"{}"), "42704");
    }

    #[test]
    fn bind_rejects_malformed_binary_value_with_binary_sqlstate() {
        let mut s = Session::new(test_auth());
        s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![23]);

        let response = s.on_bind(
            "p".into(),
            "st".into(),
            &[1],
            &[Some(vec![1])],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );

        assert!(matches!(
            response,
            BackendMessage::ErrorResponse { ref fields }
                if fields[1] == (b'C', "22P03".to_string())
        ));
        assert!(s.portal("p").is_none());
    }

    #[test]
    fn bind_rejects_parameter_format_count_mismatch() {
        let mut s = Session::new(test_auth());
        s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![23]);

        let response = s.on_bind(
            "p".into(),
            "st".into(),
            &[0, 1],
            &[Some(b"7".to_vec())],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );

        assert!(matches!(
            response,
            BackendMessage::ErrorResponse { ref fields }
                if fields[1] == (b'C', "08P01".to_string())
        ));
        assert!(s.portal("p").is_none());
    }

    #[test]
    fn close_removes_statement_and_portal() {
        let mut s = Session::new(test_auth());
        s.on_parse("st".into(), "SELECT id FROM users", vec![]);
        s.on_bind(
            "p".into(),
            "st".into(),
            &[],
            &[],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        assert!(matches!(
            s.on_close(b'P', "p"),
            BackendMessage::CloseComplete
        ));
        assert!(s.portal("p").is_none());
        assert!(matches!(
            s.on_close(b'S', "st"),
            BackendMessage::CloseComplete
        ));
        assert!(s.statement("st").is_none());
    }

    /// A running query for a portal, over a table far larger than the channel.
    async fn running_query() -> crate::result_stream::ResultStream {
        use ferrosa_sql::{Column, ColumnType, InMemoryTable, MapCatalog, RelSchema, Row};
        let schema = RelSchema::new(vec![Column::new("id", ColumnType::Int)]);
        let rows = (0..1_000)
            .map(|i| Row::new(vec![SqlValue::Int(i)]))
            .collect();
        let catalog = MapCatalog::new().with_table(
            "public",
            "t",
            std::sync::Arc::new(InMemoryTable::new(schema, rows)),
        );
        let Ok(Statement::Select(select)) = parse_statement("SELECT id FROM t") else {
            panic!("fixture query must parse to a SELECT");
        };
        crate::result_stream::open_stream(
            *select,
            catalog,
            crate::storage_provider::ScanFailure::default(),
            "public".to_string(),
            Vec::new(),
        )
        .await
        .expect("query starts")
    }

    /// `Close` on a portal releases its suspended query.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_releases_a_suspended_portals_query() {
        let mut s = Session::new(test_auth());
        s.park_stream("p".into(), running_query().await);
        assert_eq!(s.suspended_portals(), 1);
        s.on_close(b'P', "p");
        assert_eq!(s.suspended_portals(), 0);
        assert!(s.take_stream("p").is_none());
    }

    /// Rebinding a portal name replaces it, and its suspended query with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rebind_releases_a_suspended_portals_query() {
        let mut s = Session::new(test_auth());
        s.on_parse("st".into(), "SELECT id FROM users", vec![]);
        s.on_bind(
            "p".into(),
            "st".into(),
            &[],
            &[],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        s.park_stream("p".into(), running_query().await);
        s.on_bind(
            "p".into(),
            "st".into(),
            &[],
            &[],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        assert_eq!(s.suspended_portals(), 0);
    }

    /// Outside a transaction block `Sync` ends the unit of work and releases
    /// suspended portals; inside one they survive until closed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sync_releases_suspended_portals_only_outside_a_transaction() {
        let mut s = Session::new(test_auth());
        s.park_stream("p".into(), running_query().await);
        s.begin_txn(None, crate::mvcc::MvccManager::default().snapshot());
        s.on_sync();
        assert_eq!(s.suspended_portals(), 1, "a transaction keeps its portals");
        s.end_txn();
        s.on_sync();
        assert_eq!(
            s.suspended_portals(),
            0,
            "Sync outside a block releases them"
        );
    }

    #[test]
    fn sync_clears_error_pending() {
        let mut s = Session::new(test_auth());
        s.on_parse("bad".into(), "SELCT x", vec![]);
        assert!(s.is_error_pending());
        s.on_sync();
        assert!(!s.is_error_pending());
    }

    #[test]
    fn describe_statement_rows_nodata_when_empty() {
        assert!(matches!(
            describe_statement_rows(&[]),
            BackendMessage::NoData
        ));
        let cols = vec![ferrosa_sql::Column::new("id", ferrosa_sql::ColumnType::Int)];
        assert!(matches!(
            describe_statement_rows(&cols),
            BackendMessage::RowDescription { .. }
        ));
    }

    #[test]
    fn transaction_state_machine() {
        let mut s = Session::new(test_auth());
        // Starts idle.
        assert_eq!(s.txn_status(), TransactionStatus::Idle);
        assert!(!s.in_txn() && !s.in_failed_txn());

        // BEGIN -> in transaction.
        s.begin_txn(None, crate::mvcc::MvccManager::default().snapshot());
        assert_eq!(s.txn_status(), TransactionStatus::InTransaction);
        assert!(s.in_txn());

        // An error inside the txn aborts it (T -> E).
        s.mark_txn_failed();
        assert_eq!(s.txn_status(), TransactionStatus::Failed);
        assert!(s.in_failed_txn() && !s.in_txn());

        // ROLLBACK/COMMIT clears it back to idle.
        s.end_txn();
        assert_eq!(s.txn_status(), TransactionStatus::Idle);

        // mark_txn_failed is a no-op outside a transaction.
        s.mark_txn_failed();
        assert_eq!(s.txn_status(), TransactionStatus::Idle);

        // BEGIN while already in a transaction stays in-transaction.
        s.begin_txn(
            Some(ferrosa_sql::IsolationLevel::Serializable),
            crate::mvcc::MvccManager::default().snapshot(),
        );
        s.begin_txn(None, crate::mvcc::MvccManager::default().snapshot());
        assert_eq!(s.txn_status(), TransactionStatus::InTransaction);
        assert_eq!(
            s.txn_isolation(),
            Some(ferrosa_sql::IsolationLevel::Serializable)
        );
    }
}
