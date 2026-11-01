//! The join between the Lean specification and the Rust implementation.
//!
//! [`REDS`] holds one row per red item in the Lean ledgers, generated into `src/reds.rs` by
//! `scripts/red_ledger.py`; CI's `red_ledger.py --check` fails when the table and the ledger drift.
//! A red is data, never a test: there is no ignored stub per claim. Closing one means writing the
//! test that pins the claim in the crate that owns the code, then flipping the Lean item to `.met`,
//! which drops the row and moves the count `tests/reds.rs` pins.

mod reds;

pub use reds::{Red, REDS};
