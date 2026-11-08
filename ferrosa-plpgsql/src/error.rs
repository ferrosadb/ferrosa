//! Parse-time refusals, each carrying the SQLSTATE the front end reports.

/// Why a PL/pgSQL body (or a front-end statement this crate parses) was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct PlError {
    /// The PostgreSQL SQLSTATE the front end answers with.
    pub sqlstate: &'static str,
    /// The human-readable reason, naming the construct.
    pub message: String,
}

impl PlError {
    /// `0A000`: a real PL/pgSQL construct this subset does not run. The construct is
    /// named so the caller knows what to change; it is never compiled to a no-op.
    pub fn unsupported(construct: &str) -> Self {
        Self {
            sqlstate: "0A000",
            message: format!("PL/pgSQL construct {construct} is not supported"),
        }
    }

    /// `42601`: the text is not valid PL/pgSQL (or not valid for this statement).
    pub fn syntax(message: impl Into<String>) -> Self {
        Self {
            sqlstate: "42601",
            message: message.into(),
        }
    }

    /// Any other SQLSTATE with its message.
    pub fn new(sqlstate: &'static str, message: impl Into<String>) -> Self {
        Self {
            sqlstate,
            message: message.into(),
        }
    }
}
