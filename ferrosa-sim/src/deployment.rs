//! Cluster formation lifecycle, shared with `ferrosa-cluster`.
//!
//! This file used to hold a SECOND copy of `DeploymentMode`, with the comment
//! "the two enums are kept in lock-step manually". They were not. The mirror
//! had four variants where the real one has six -- no `DegradedPair`, no
//! `DegradedCluster` -- so the simulator could not represent a degraded
//! cluster, and therefore could not simulate the failure that matters most:
//! a node that has held a quorum losing one.
//!
//! Both now use the definition in `ferrosa-common`, which depends on neither
//! openraft nor sled, so the reason `ferrosa-sim` avoids `ferrosa-cluster`
//! (ADR-017) still holds.

pub use ferrosa_common::deployment_mode::DeploymentMode;
