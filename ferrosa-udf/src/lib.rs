//! WASM-sandboxed User-Defined Function execution for Ferrosa.
//!
//! This crate provides the [`UdfExecutor`] which compiles, caches, and
//! invokes WASM Component Model modules for CQL UDFs.  All Wasmtime
//! internals are encapsulated here — the CQL layer sees only the
//! `call()` method.

pub mod arena;
/// Inline AssemblyScript -> WASM compilation (feature `asc-udf`).
#[cfg(feature = "asc-udf")]
pub mod asc;
/// Componentize an asc core module to the `udf` WIT ABI (feature `asc-udf`).
#[cfg(feature = "asc-udf")]
pub mod component;
pub mod convert;
pub mod error;
pub mod executor;
pub mod sandbox;

pub use arena::UdfArena;
pub use error::UdfError;
pub use executor::{FunctionKind, StreamingAggregateInvocation, UdfExecutor};
pub use sandbox::SandboxConfig;
