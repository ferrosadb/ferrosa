//! Accord consensus protocol support for Ferrosa.
//!
//! This module contains the deterministic test harness ([`TestCluster`]) for
//! protocol-level testing of the Accord (EPaxos-family) consensus protocol.

pub mod test_cluster;

pub use test_cluster::{TestCluster, TestMessage, TestMessagePayload, TestReplica};
