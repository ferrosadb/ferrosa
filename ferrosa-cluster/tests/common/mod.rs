//! Shared helpers for ferrosa-cluster integration tests.
//!
//! Modules here are reused across multiple `tests/*.rs` files; each
//! integration test compiles `mod common;` directly from `tests/`.

pub mod harness_slot;
pub mod raft_harness;
