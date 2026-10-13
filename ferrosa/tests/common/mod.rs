//! Shared support for the real-process node harnesses.
//!
//! Rust compiles this module into every test binary that declares `mod common;`,
//! so anything here is linked into six different targets and each target uses
//! only a subset of it.
#![allow(dead_code)]

pub mod ports;
