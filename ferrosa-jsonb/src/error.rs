//! Module: typed errors for jsonb limits and (later) parsing and validation.
//! Correctness: correct when every rejected value maps to one variant that names
//! the measured value and the bound it crossed; nothing is swallowed.
//! Last revised: 2026-09-28
//! Last changed: T-100 initial limit-related variants.

use thiserror::Error;

/// A jsonb value or argument crossed a limit, or was otherwise refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JsonbError {
    /// Input text exceeds the tunable `max_input_bytes`.
    #[error("jsonb input is {len} bytes, above the limit of {max} bytes")]
    InputTooLarge { len: usize, max: usize },
    /// Encoded value exceeds the tunable ingest limit or the read ceiling.
    #[error("jsonb encoded value is {len} bytes, above the limit of {max} bytes")]
    EncodedTooLarge { len: usize, max: usize },
    /// Nesting exceeds the tunable or hard depth.
    #[error("jsonb nesting depth {depth} exceeds the limit of {max}")]
    DepthExceeded { depth: u32, max: u32 },
    /// A number has too many digits before the decimal point (D14a).
    #[error("jsonb number has {digits} digits before the decimal point, above the limit of {max}")]
    DigitsBeforePointExceeded { digits: usize, max: usize },
    /// A number has too many digits after the decimal point (D14a).
    #[error("jsonb number has {digits} digits after the decimal point, above the limit of {max}")]
    DigitsAfterPointExceeded { digits: usize, max: usize },
    /// The text is not a JSON number lexeme; `offset` is the byte position.
    #[error("jsonb number lexeme is malformed at byte offset {offset}")]
    InvalidNumber { offset: usize },
    /// NaN or an infinity has no jsonb number form (D12a, FM-48).
    #[error("jsonb cannot represent a non-finite number")]
    NonFiniteNumber,
    /// A key list (for example an exists-list argument) is too long.
    #[error("jsonb key list has {len} keys, above the limit of {max}")]
    KeyListTooLong { len: usize, max: usize },
    /// A duplicate object key under `DuplicateKeyPolicy::Error` (D6b).
    #[error("jsonb object has a duplicate key")]
    DuplicateKey,
}

/// A limits configuration was refused at startup (D14b, D14d, FM-108).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LimitsError {
    /// A tunable was zero.
    #[error("jsonb limit `{name}` must be greater than zero")]
    Zero { name: &'static str },
    /// A tunable exceeds its compiled ceiling.
    #[error("jsonb limit `{name}` is {value}, above its hard ceiling of {ceiling}")]
    AboveCeiling {
        name: &'static str,
        value: u64,
        ceiling: u64,
    },
    /// A size tunable exceeds what the write path can hold (D14d).
    #[error(
        "jsonb limit `{name}` is {value} bytes, above the write-path maximum \
         (commit-log segment size) of {write_path_max} bytes"
    )]
    AboveWritePath {
        name: &'static str,
        value: u64,
        write_path_max: u64,
    },
    /// An environment override could not be parsed as an unsigned integer.
    #[error("environment variable {var} has value {raw:?}, which is not an unsigned integer")]
    InvalidEnv { var: &'static str, raw: String },
}
