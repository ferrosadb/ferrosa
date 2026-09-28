//! Module: `ferrosa-jsonb`, the jsonb value model (D5a: ferrosa owns the codec).
//! Correctness: correct when every limit is enforced incrementally with a typed
//! error, tunables never exceed a compiled ceiling, and reads use only ceilings.
//! Last revised: 2026-09-28
//! Last changed: T-100 scaffold: error types, `Limits`, `HardCeilings`.
//!
//! Leaf crate: depends on no other ferrosa crate and on no arrow-rs crate.
//! Only [`error`] and [`limits`] exist so far; the value, codec and validator
//! arrive in later packets (T-101 onward).

#![forbid(unsafe_code)]

pub mod error;
pub mod limits;

pub use error::{JsonbError, LimitsError};
pub use limits::{
    DuplicateKeyPolicy, HardCeilings, Limits, LimitsConfig, HARD_MAX_DEPTH,
    HARD_MAX_DIGITS_AFTER_POINT, HARD_MAX_DIGITS_BEFORE_POINT, HARD_MAX_ENCODED_BYTES,
};
