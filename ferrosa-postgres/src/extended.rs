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
//! [`Session::error_pending`] flag implements that skip; `Sync` clears it.

use std::collections::HashMap;

use ferrosa_sql::{parse, SelectStmt, Value as SqlValue};

use crate::messages::BackendMessage;
use crate::query::{decode_param, error_response, exec_error_response, row_description_fields};

/// A prepared statement: the parsed SELECT plus the client-declared parameter
/// type OIDs (used to decode bound values and to answer `Describe` for the
/// `ParameterDescription`). A `0` OID means "unspecified" (decode leniently).
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub parsed: SelectStmt,
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
#[derive(Default)]
pub struct Session {
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    /// Set when an error occurs mid-sequence; skip messages until `Sync`.
    error_pending: bool,
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
    pub fn new() -> Self {
        Self::default()
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
    pub fn on_sync(&mut self) {
        self.error_pending = false;
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
        match parse(query) {
            Ok(parsed) => {
                self.statements.insert(
                    stmt_name,
                    PreparedStatement {
                        parsed,
                        param_oids: param_types,
                    },
                );
                BackendMessage::ParseComplete
            }
            Err(e) => {
                self.error_pending = true;
                error_response("42601", &e.to_string())
            }
        }
    }

    /// Handle `Bind`: decode each parameter value against the prepared
    /// statement's declared OIDs and store the portal. A missing prepared
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
    ) -> BackendMessage {
        let Some(stmt) = self.statements.get(&stmt_name) else {
            self.error_pending = true;
            return error_response(
                "26000",
                &format!("prepared statement \"{stmt_name}\" does not exist"),
            );
        };

        let params: Vec<SqlValue> = param_values
            .iter()
            .enumerate()
            .map(|(i, bytes)| {
                let format = param_format_for(param_formats, i);
                // A declared OID is matched positionally; unspecified ⇒ 0.
                let oid = stmt.param_oids.get(i).copied().unwrap_or(0);
                decode_param(format, oid, bytes.as_deref())
            })
            .collect();

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

    #[test]
    fn parse_stores_statement_and_acks() {
        let mut s = Session::new();
        let ack = s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![23]);
        assert!(matches!(ack, BackendMessage::ParseComplete));
        assert!(s.statement("st").is_some());
        assert_eq!(s.statement("st").unwrap().param_oids, vec![23]);
        assert!(!s.is_error_pending());
    }

    #[test]
    fn parse_error_sets_error_pending_and_no_parse_complete() {
        let mut s = Session::new();
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
        let mut s = Session::new();
        s.on_parse("st".into(), "SELECT id FROM users WHERE id = $1", vec![23]);
        let ack = s.on_bind(
            String::new(), // unnamed portal
            "st".into(),   // statement
            &[1],          // binary param format
            &[Some(7i32.to_be_bytes().to_vec())],
            vec![1], // binary result format
        );
        assert!(matches!(ack, BackendMessage::BindComplete));
        let portal = s.portal("").expect("portal stored");
        assert_eq!(portal.stmt_name, "st");
        assert_eq!(portal.params, vec![SqlValue::Int(7)]);
        assert_eq!(portal.result_formats, vec![1]);
    }

    #[test]
    fn bind_missing_statement_fails_loud() {
        let mut s = Session::new();
        let resp = s.on_bind("".into(), "ghost".into(), &[], &[], vec![]);
        assert!(matches!(
            resp,
            BackendMessage::ErrorResponse { ref fields } if fields[1] == (b'C', "26000".to_string())
        ));
        assert!(s.is_error_pending());
    }

    #[test]
    fn close_removes_statement_and_portal() {
        let mut s = Session::new();
        s.on_parse("st".into(), "SELECT id FROM users", vec![]);
        s.on_bind("p".into(), "st".into(), &[], &[], vec![]);
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

    #[test]
    fn sync_clears_error_pending() {
        let mut s = Session::new();
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
}
