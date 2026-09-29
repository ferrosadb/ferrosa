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
    /// `path` names the object (`$`, `$.a[0]`); the builder alone leaves it empty.
    #[error("jsonb object at {path:?} has a duplicate key")]
    DuplicateKey { path: String },
    /// The text is not RFC 8259 JSON; `offset` is the byte position (T-103).
    #[error("jsonb text is not valid JSON at byte offset {offset}: {reason}")]
    Syntax { offset: usize, reason: &'static str },
    /// The text is not valid UTF-8; `offset` is the first bad byte (T-103).
    #[error("jsonb text is not valid UTF-8 at byte offset {offset}")]
    InvalidUtf8 { offset: usize },
    /// A `\u0000` escape under `NulPolicy::Reject`: the target cannot hold a NUL
    /// (Postgres `text`, `22P05`). `offset` is the backslash of the escape.
    #[error(
        "jsonb text has a \\u0000 escape at byte offset {offset}, which the target cannot store"
    )]
    NulEscape { offset: usize },
    /// The node-wide in-flight parse budget cannot admit the request (JB-D5).
    #[error("jsonb in-flight budget refused {requested} bytes (budget {max})")]
    InflightBudgetExceeded { requested: usize, max: usize },
    /// The builder was driven out of order (T-102): `reason` names the misuse.
    #[error("jsonb builder misuse: {reason}")]
    BuilderMisuse { reason: &'static str },
    /// The first byte of a stored cell is not a known envelope (T-104, FM-07).
    #[error("jsonb cell has unknown envelope byte {byte:#04x}")]
    UnknownEnvelope { byte: u8 },
    /// A stored cell is malformed or not canonical (T-104, JB-T1).
    #[error("jsonb cell is invalid: {reason}")]
    InvalidEncoding { reason: EncodingFault },
    /// A reader accessor was applied to a value of another kind (T-104).
    #[error("jsonb value is not {expected}")]
    WrongKind { expected: &'static str },
}

/// Why a stored cell failed validation (T-104). Every variant is a distinct
/// structural fault; `NonCanonical` names the architecture rule (C1-C11) that a
/// structurally sound cell breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum EncodingFault {
    #[error("the cell ends before a required field")]
    Truncated,
    #[error("the metadata header is not Variant version 1 with sorted strings")]
    BadMetadataHeader,
    #[error("the dictionary is not strictly ascending by key bytes")]
    DictionaryNotSorted,
    #[error("an offset table is not monotone starting at zero")]
    OffsetsNotMonotonic,
    #[error("an offset or length reaches past its enclosing region")]
    OffsetOutOfBounds,
    #[error("a field id is not in the dictionary")]
    FieldIdOutOfRange,
    #[error("field ids are not strictly ascending")]
    FieldsNotSorted,
    #[error("primitive id {0} is not defined")]
    UnknownPrimitive(u8),
    #[error("primitive id {0} is excluded from jsonb (C10)")]
    ExcludedPrimitive(u8),
    #[error("a bigdecimal body is malformed")]
    BadBigDecimal,
    #[error("text is not valid UTF-8")]
    InvalidUtf8,
    #[error("bytes remain after the value")]
    TrailingBytes,
    #[error("breaks canonical rule {0}")]
    NonCanonical(&'static str),
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
