//! The host interface both runtimes use (the WIT `ferrosa:plpgsql/host` interface, as Rust).
//!
//! The host executes inside the **calling transaction**: what a `DO` block or a trigger
//! function writes commits or rolls back with the statement that ran it.

use std::sync::Arc;

/// Rows a runtime asks for in one `fetch`. A result is read in batches of this size, never
/// gathered whole.
pub const FETCH_BATCH: u32 = 64;

/// A SQL value as it crosses the host interface: its type OID and PostgreSQL's text form
/// (`None` is SQL NULL). Text I/O is lossless in PostgreSQL by design, so nothing is lost,
/// and PL/pgSQL never looks inside a value except to ask whether it is `true`.
///
/// The text is an `Arc<str>`: a variable's value is bound as a parameter while the variable
/// keeps it, and a notification payload is fanned out to every listener, so it is shared
/// rather than copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    /// The value's PostgreSQL type OID.
    pub type_oid: u32,
    /// The text form, or `None` for NULL.
    pub text: Option<Arc<str>>,
}

impl Value {
    /// SQL NULL of the given type.
    pub fn null(type_oid: u32) -> Self {
        Self {
            type_oid,
            text: None,
        }
    }

    /// A non-NULL value from its text form.
    pub fn text(type_oid: u32, text: impl Into<Arc<str>>) -> Self {
        Self {
            type_oid,
            text: Some(text.into()),
        }
    }

    /// PostgreSQL's reading of a condition: only boolean `true` is true. NULL and `false`
    /// are not, and a non-boolean is a type error the caller reports.
    pub fn is_true(&self) -> bool {
        self.type_oid == crate::OID_BOOL && self.text.as_deref() == Some("t")
    }

    /// `true` when this is SQL NULL.
    pub fn is_null(&self) -> bool {
        self.text.is_none()
    }
}

/// A failure the host (or a runtime) reports, with its SQLSTATE.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{sqlstate}: {message}")]
pub struct SqlError {
    /// PostgreSQL SQLSTATE.
    pub sqlstate: String,
    /// Message.
    pub message: String,
}

impl SqlError {
    /// An error with this SQLSTATE and message.
    pub fn new(sqlstate: &str, message: impl Into<String>) -> Self {
        Self {
            sqlstate: sqlstate.to_string(),
            message: message.into(),
        }
    }
}

impl From<crate::PlError> for SqlError {
    fn from(error: crate::PlError) -> Self {
        Self {
            sqlstate: error.sqlstate.to_string(),
            message: error.message,
        }
    }
}

/// An open query's handle, valid until `close` or the end of the call.
pub type CursorId = u32;

/// The database, as a PL/pgSQL runtime sees it.
pub trait Host {
    /// Start `sql` with `params` bound as `$1 … $n`; rows are read with [`Host::fetch`].
    fn query(&mut self, sql: &str, params: &[Value]) -> Result<CursorId, SqlError>;
    /// Up to `max` more rows of an open query; an empty batch means it is exhausted.
    fn fetch(&mut self, cursor: CursorId, max: u32) -> Result<Vec<Vec<Value>>, SqlError>;
    /// Stop an open query early (its remaining rows are never produced).
    fn close(&mut self, cursor: CursorId);
    /// Run a statement for its effect; the affected-row count.
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, SqlError>;
    /// Queue a notification in the calling transaction (delivered only if it commits).
    fn notify(&mut self, channel: &str, payload: &str) -> Result<(), SqlError>;
}

/// The trigger context a trigger function is called with. `new_row` / `old_row` hold the
/// values of the fields the function references, in the order of
/// [`crate::Program::new_fields`] / [`crate::Program::old_fields`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerCall {
    /// `TG_OP`: `INSERT`, `UPDATE` or `DELETE`.
    pub op: String,
    /// `NEW`'s referenced fields (absent for `DELETE`).
    pub new_row: Option<Vec<Value>>,
    /// `OLD`'s referenced fields (absent for `INSERT`).
    pub old_row: Option<Vec<Value>>,
}
