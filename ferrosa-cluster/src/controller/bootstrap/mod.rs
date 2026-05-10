//! Bootstrap phase decomposition (Sprint 4 W4.1–W4.9).
//!
//! `transition_to_cluster` (in `controller/cluster.rs`) historically ran
//! ~700 lines of imperative orchestration to bring a Forming cluster up
//! to its first leader-elected, schema-replayed steady state. Sprint 4
//! decomposes that work into eight typed phases, each with explicit
//! [`precondition`] and [`postcondition`] checks. Errors carry the
//! offending [`BootstrapPhase`], so a caller (today only the spawned
//! bootstrap task; tomorrow a coordinator-driven phase runner) can log
//! and recover at the right granularity.
//!
//! ## Why a separate module
//!
//! Sprint 6 plans to extend `transition_to_cluster` for multi-Raft (one
//! Raft per DC). Sprint 4 keeps the existing in-place implementation
//! operational and exposes the phase types and pure helpers from this
//! module. A subsequent sprint will rewire the imperative path to
//! consume the typed phases. The decomposition is therefore additive
//! today: every phase is unit-testable in isolation, the
//! pre/post-condition signature is fixed, and `controller/cluster.rs`
//! continues to drive the live cluster.
//!
//! ## Phase ordering
//!
//! | # | Phase             | Purpose                                            |
//! |--:|-------------------|----------------------------------------------------|
//! | 1 | `DeliverInvites`  | Multicast `ClusterInvite` to every peer           |
//! | 2 | `EstablishPools`  | Ensure outbound pools live on `Lane::Raft`/`Data` |
//! | 3 | `CreateRaft`      | Construct `FerrosRaft`; publish to three sinks    |
//! | 4 | `WaitLeader`      | Block until any node sees a `current_leader`      |
//! | 5 | `ReplaySchema`    | Drive `state.schema_version` to leader's value    |
//! | 6 | `BootstrapStream` | Stream owning replicas of joining tokens          |
//! | 7 | `Promote`         | Mark every peer `NodeState::Normal`               |
//! | 8 | `DrainQueue`      | Replay queued DDL operations through cluster path |
//!
//! The matrix in `specs/raft-failure-mode-matrix.md` indexes failure
//! modes that touch each phase; the W4.15 integration tests exercise
//! S-01 through S-37 against the live `transition_to_cluster`.

pub mod phase;

pub use phase::{BootstrapError, BootstrapPhase};
