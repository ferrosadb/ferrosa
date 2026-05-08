//! Library interface for `ferrosa-ctl` command implementations.
//!
//! Re-exports the `commands` module so integration tests can call
//! the command functions directly against a live test server.

pub mod auth;
pub mod commands;
pub mod tui;
