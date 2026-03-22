//! Jepsen-style test infrastructure for Ferrosa.
//!
//! This module provides a deterministic linearizability checker and nemesis
//! framework built on top of the existing [`TestCluster`]. Unlike the real
//! Clojure Jepsen, this is a pure-Rust implementation focused on:
//!
//! - Programmatic cluster provisioning (3-node TestCluster)
//! - CQL-level client operations (read/write to a register)
//! - Nemesis controllers (partition, kill, slow, clock skew, pause)
//! - History recording with wall-clock timestamps
//! - Sequential consistency / linearizability checking

pub mod infrastructure;
pub mod register;
