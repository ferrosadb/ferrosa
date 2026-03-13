//! CQL error types.
//!
//! Minimal stub — expanded with full error codes, Display, and
//! encode_body in Task 4.

use std::fmt;
use std::io;

/// CQL protocol error.
#[derive(Debug, Clone)]
pub enum CqlError {
    /// Protocol-level error (malformed frame, wrong version, bad opcode).
    Protocol(String),
    /// I/O error wrapper.
    Io(String),
}

impl fmt::Display for CqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(msg) => write!(f, "{msg}"),
            Self::Io(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for CqlError {}

impl From<io::Error> for CqlError {
    fn from(e: io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
