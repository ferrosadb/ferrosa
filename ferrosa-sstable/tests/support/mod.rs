//! Shared test-support modules for the T-035 writer test oracle.
//!
//! Each file directly under `tests/` compiles as its own integration-test
//! crate, so every test binary that needs this support code pulls it in
//! independently via `mod support;` (see `tests/oracle.rs` and
//! `tests/golden_regen.rs`).

// Each integration-test binary (`oracle.rs`, `golden_regen.rs`) compiles its
// own copy of this module tree and only uses part of its API, so plain
// `dead_code` warnings fire per-binary for whatever the OTHER binary uses.
// `-D warnings` in the crate's clippy gate would otherwise fail a binary for
// not calling every helper meant for its sibling.
#[allow(dead_code)]
pub mod generators;
#[allow(dead_code)]
pub mod golden;
#[allow(dead_code)]
pub mod legacy_writer;
