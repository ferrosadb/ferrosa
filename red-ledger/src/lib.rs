//! The join between the Lean specification and the Rust implementation.
//!
//! [`REDS`] holds one row per red item in the Lean ledgers, generated into `src/reds.rs` by
//! `scripts/red_ledger.py`; CI's `red_ledger.py --check` fails when the table and the ledger drift.
//! A red is data, never a test: there is no ignored stub per claim. Closing one means writing the
//! test that pins the claim in the crate that owns the code, then flipping the Lean item to `.met`,
//! which drops the row and moves the count `tests/reds.rs` pins.

// The crate is a data table and a join over it; it has no reason to reach the platform, so the
// absence of `unsafe` is enforced rather than merely true (docs/unsafe-policy.md §7).
#![forbid(unsafe_code)]

mod reds;

pub use reds::{Red, REDS};
