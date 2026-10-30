//! The join between the Lean specification and the Rust implementation.
//!
//! Every red item in the Lean ledgers has one test in `tests/reds.rs` carrying the same name. The
//! test is `#[ignore]`d while the claim is unmet: it documents what the code does not yet do, and
//! it is the target a fix has to make green. Fixing the code means deleting the `#[ignore]` and
//! filling in the assertion; closing the item in Lean means the generator stops emitting it. The
//! two sides are checked against each other by `cargo test -p red-ledger`.
