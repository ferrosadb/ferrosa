//! Module: `JsonbValue`, the owned validated jsonb cell (T-106, D2a, D18).
//! Correctness: correct when the only ways to obtain one validate the bytes
//! against the fixed hard ceilings (never trusting the source), so every
//! accessor and every comparison may assume canonical bytes.
//! Last revised: 2026-09-28
//! Last changed: T-106 initial value type; traits by value.

use crate::builder::Encoded;
use crate::error::JsonbError;
use crate::reader::JsonbRef;

/// An owned, validated jsonb cell (envelope, metadata and root value).
///
/// `Eq`, `Ord`, `Hash` and `Debug` are by VALUE (D2a, D18), implemented in
/// `eq_hash` and `order`; the bytes may differ in number scale between equal
/// values, so byte equality is only a fast path. Serde lives in `serde_impl`.
#[derive(Clone)]
pub struct JsonbValue {
    bytes: Vec<u8>,
}

impl JsonbValue {
    /// Validate `bytes` (D4, D14b) and take ownership of them.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<JsonbValue, JsonbError> {
        JsonbRef::validate(&bytes)?;
        Ok(JsonbValue { bytes })
    }

    /// Validate a finished build and take its bytes.
    pub fn from_encoded(encoded: Encoded) -> Result<JsonbValue, JsonbError> {
        JsonbValue::from_bytes(encoded.bytes)
    }

    /// The canonical cell bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Give the canonical cell bytes back.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// The borrowed reader. Bytes were validated at construction, so an error
    /// here means memory corruption and is surfaced, never swallowed.
    pub fn view(&self) -> Result<JsonbRef<'_>, JsonbError> {
        JsonbRef::validate(&self.bytes)
    }
}
