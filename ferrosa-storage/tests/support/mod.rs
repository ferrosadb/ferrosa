//! Shared test support for `ferrosa-storage` integration tests.
//!
//! Each `tests/*.rs` compiles as its own integration-test crate, so every test
//! binary that needs this support code pulls it in independently via
//! `mod support;` (see `tests/sidecar_golden.rs` and
//! `tests/sidecar_golden_regen.rs`).
//!
//! Keep it free of `#[test]`s: those would be collected and run by every
//! consumer.

// Each integration-test binary compiles its own copy of this module tree and
// only uses part of its API, so plain `dead_code` warnings fire per-binary for
// whatever the OTHER binary uses. `-D warnings` in the crate's clippy gate
// would otherwise fail a binary for not calling every helper meant for its
// sibling. Mirrors `ferrosa-sstable/tests/support/mod.rs`.
#[allow(dead_code)]
pub mod sidecar_golden;
