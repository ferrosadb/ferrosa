# ferrosa-cluster Phase 1 (Pair Mode) Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build pair mode — two-node synchronous replication with primary/secondary roles, write forwarding, catch-up, and switchover.

**Architecture:** A `PairCoordinator` owns the write path for both roles. Primary writes locally then replicates via `PairWriteForward` RPC. Secondary forwards writes to primary. A `PairNode` struct integrates coordinator, RPC handlers, and peer lifecycle. No Raft — pair mode uses deterministic primary election (higher host_id wins).

**Tech Stack:** Rust, tokio, ferrosa-net (RPC/peer management), ferrosa-storage (StorageEngine/Mutation/CommitLog), ferrosa-schema (ClusterState), arc-swap, bytes, uuid

**Spec:** `docs/superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md` Part 2
**Threat model:** `specs/threat-model-net-cluster.md` (T9, T11-T13)

---

## File Structure

```text
ferrosa-cluster/src/
├── lib.rs              — Module declarations and re-exports
├── error.rs            — ClusterError enum, Result type alias
├── config.rs           — ClusterConfig struct with env parsing
├── consistency.rs      — ConsistencyLevel enum, blockFor()
├── mode.rs             — DeploymentMode enum
├── state.rs            — ClusterState trait impl for pair mode
├── pair/
│   ├── mod.rs          — PairRole, PairState, elect_primary, re-exports
│   ├── node.rs         — PairNode integration struct, PeerEventListener
│   ├── coordinator.rs  — PairCoordinator write coordination
│   ├── handler.rs      — PairWriteForwardHandler RPC handler
│   ├── catchup.rs      — Catch-up protocol
│   └── switchover.rs   — Operator-initiated role swap
```

Also modified:

- `Cargo.toml` (workspace) — add `ferrosa-cluster` member
- `ferrosa-storage/src/engine.rs` — add public `replay_from()` accessor (T8)
- `ferrosa-storage/src/commitlog/mod.rs` — add `replay_from()` on CommitLog (T8)

---

## Chunk 1: Foundation

### Task 1: Scaffold crate with error types and config

**Files:**

- Create: `ferrosa-cluster/Cargo.toml`
- Create: `ferrosa-cluster/src/lib.rs`
- Create: `ferrosa-cluster/src/error.rs`
- Create: `ferrosa-cluster/src/config.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create `ferrosa-cluster/Cargo.toml`**

```toml
[package]
name = "ferrosa-cluster"
version = "0.1.0"
edition = "2021"

[dependencies]
arc-swap = "1.7"
async-trait = "0.1"
bytes = "1"
ferrosa-common = { path = "../ferrosa-common" }
ferrosa-net = { path = "../ferrosa-net" }
ferrosa-schema = { path = "../ferrosa-schema" }
ferrosa-storage = { path = "../ferrosa-storage" }
tokio = { version = "1", features = ["sync", "time", "rt", "macros"] }
tracing = "0.1"
uuid = { version = "1", features = ["v4"] }

[dev-dependencies]
proptest = "1"
tempfile = "3"
tokio = { version = "1", features = ["full", "test-util"] }
```

- [ ] **Step 2: Add `ferrosa-cluster` to workspace members**

In root `Cargo.toml`, add `"ferrosa-cluster"` to the `members` list (alphabetical order, after `ferrosa-cql`).

- [ ] **Step 3: Create `ferrosa-cluster/src/error.rs`**

```rust
use std::fmt;

/// Errors produced by ferrosa-cluster.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClusterError {
    /// Not enough replicas available to satisfy consistency level.
    Unavailable {
        consistency: String,
        required: usize,
        alive: usize,
    },
    /// Replicas alive but not enough ACKed the write in time.
    WriteTimeout {
        consistency: String,
        received: usize,
        required: usize,
    },
    /// Replicas alive but not enough responded to the read in time.
    ReadTimeout {
        consistency: String,
        received: usize,
        required: usize,
        data_present: bool,
    },
    /// Pair mode: primary is down, writes unavailable until operator promotes.
    PairWriteUnavailable,
    /// Operation requires primary role but this node is secondary.
    NotPrimary,
    /// Attempted mode transition that is not allowed.
    ModeTransitionRejected(String),
    /// Replication to peer failed.
    ReplicationFailed(String),
    /// Peer is too far behind; full catch-up or bootstrap required.
    CatchUpRequired,
    /// Underlying storage error.
    Storage(ferrosa_common::Error),
    /// Underlying network error.
    Net(ferrosa_net::error::NetError),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { consistency, required, alive } => {
                write!(f, "unavailable: CL={consistency}, required={required}, alive={alive}")
            }
            Self::WriteTimeout { consistency, received, required } => {
                write!(f, "write timeout: CL={consistency}, received={received}, required={required}")
            }
            Self::ReadTimeout { consistency, received, required, data_present } => {
                write!(
                    f,
                    "read timeout: CL={consistency}, received={received}, required={required}, data_present={data_present}"
                )
            }
            Self::PairWriteUnavailable => write!(f, "pair mode: primary unavailable"),
            Self::NotPrimary => write!(f, "this node is not the primary"),
            Self::ModeTransitionRejected(reason) => write!(f, "mode transition rejected: {reason}"),
            Self::ReplicationFailed(reason) => write!(f, "replication failed: {reason}"),
            Self::CatchUpRequired => write!(f, "peer requires full catch-up"),
            Self::Storage(e) => write!(f, "storage: {e}"),
            Self::Net(e) => write!(f, "net: {e}"),
            Self::Internal(msg) => write!(f, "internal: {msg}"),
        }
    }
}

impl std::error::Error for ClusterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
            Self::Net(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ferrosa_net::error::NetError> for ClusterError {
    fn from(e: ferrosa_net::error::NetError) -> Self {
        Self::Net(e)
    }
}

impl From<ferrosa_common::Error> for ClusterError {
    fn from(e: ferrosa_common::Error) -> Self {
        Self::Storage(e)
    }
}

pub type Result<T> = std::result::Result<T, ClusterError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        let e = ClusterError::Unavailable {
            consistency: "QUORUM".into(),
            required: 2,
            alive: 1,
        };
        assert!(e.to_string().contains("QUORUM"));

        let e = ClusterError::PairWriteUnavailable;
        assert!(e.to_string().contains("primary"));
    }

    #[test]
    fn net_error_conversion() {
        let net_err = ferrosa_net::error::NetError::Timeout("test".into());
        let cluster_err: ClusterError = net_err.into();
        assert!(matches!(cluster_err, ClusterError::Net(_)));
    }
}
```

- [ ] **Step 4: Create `ferrosa-cluster/src/config.rs`**

```rust
use std::path::PathBuf;

use crate::consistency::ConsistencyLevel;
use crate::mode::DeploymentMode;

/// Cluster configuration. Parsed from `FERROSA_*` environment variables.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Forced deployment mode. `None` means auto-detect from peer count.
    pub mode: Option<DeploymentMode>,
    /// Cluster name — must match across all nodes.
    pub cluster_name: String,
    /// This node's data center.
    pub data_center: String,
    /// This node's rack within the data center.
    pub rack: String,
    /// Number of virtual token ranges per node.
    pub num_tokens: u32,
    /// Default consistency level for queries that don't specify one.
    pub default_cl: ConsistencyLevel,
    /// Hinted handoff storage directory.
    pub hinted_handoff_dir: PathBuf,
    /// Maximum hint storage per peer in megabytes.
    pub hinted_handoff_max_mb: u64,
    /// Allow unapproved nodes to join (true for development).
    pub auto_join: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            mode: None,
            cluster_name: "ferrosa".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            num_tokens: 256,
            default_cl: ConsistencyLevel::Quorum,
            hinted_handoff_dir: PathBuf::from("data/hints"),
            hinted_handoff_max_mb: 1024,
            auto_join: false,
        }
    }
}

impl ClusterConfig {
    /// Parse configuration from environment variables.
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(mode) = std::env::var("FERROSA_CLUSTER_MODE") {
            config.mode = match mode.to_lowercase().as_str() {
                "standalone" => Some(DeploymentMode::Standalone),
                "pair" => Some(DeploymentMode::Pair),
                "cluster" => Some(DeploymentMode::Cluster),
                _ => None,
            };
        }
        if let Ok(name) = std::env::var("FERROSA_CLUSTER_NAME") {
            config.cluster_name = name;
        }
        if let Ok(dc) = std::env::var("FERROSA_DATA_CENTER") {
            config.data_center = dc;
        }
        if let Ok(rack) = std::env::var("FERROSA_RACK") {
            config.rack = rack;
        }
        if let Ok(tokens) = std::env::var("FERROSA_NUM_TOKENS") {
            if let Ok(n) = tokens.parse() {
                config.num_tokens = n;
            }
        }
        if let Ok(cl) = std::env::var("FERROSA_DEFAULT_CL") {
            if let Some(parsed) = ConsistencyLevel::from_str(&cl) {
                config.default_cl = parsed;
            }
        }
        if let Ok(dir) = std::env::var("FERROSA_HINTED_HANDOFF_DIR") {
            config.hinted_handoff_dir = PathBuf::from(dir);
        }
        if let Ok(max) = std::env::var("FERROSA_HINTED_HANDOFF_MAX_MB") {
            if let Ok(n) = max.parse() {
                config.hinted_handoff_max_mb = n;
            }
        }
        if let Ok(auto) = std::env::var("FERROSA_AUTO_JOIN") {
            config.auto_join = auto == "true" || auto == "1";
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = ClusterConfig::default();
        assert_eq!(config.cluster_name, "ferrosa");
        assert_eq!(config.data_center, "dc1");
        assert_eq!(config.rack, "rack1");
        assert_eq!(config.num_tokens, 256);
        assert_eq!(config.default_cl, ConsistencyLevel::Quorum);
        assert_eq!(config.hinted_handoff_max_mb, 1024);
        assert!(!config.auto_join);
    }
}
```

- [ ] **Step 5: Create `ferrosa-cluster/src/lib.rs`**

```rust
pub mod config;
pub mod consistency;
pub mod error;
pub mod mode;

pub use config::ClusterConfig;
pub use consistency::ConsistencyLevel;
pub use error::{ClusterError, Result};
pub use mode::DeploymentMode;
```

Note: `consistency` and `mode` modules will be created in Tasks 2 and 3. For now, create minimal stubs so the crate compiles:

`ferrosa-cluster/src/consistency.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyLevel {
    Quorum,
}

impl ConsistencyLevel {
    pub fn from_str(_s: &str) -> Option<Self> {
        None
    }
}
```

`ferrosa-cluster/src/mode.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    Standalone,
    Pair,
    Cluster,
}
```

- [ ] **Step 6: Verify it compiles**

Run: `cargo build -p ferrosa-cluster`
Expected: Compiles with no errors.

- [ ] **Step 7: Run tests**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass (error display + config defaults).

- [ ] **Step 8: Commit**

```bash
git add ferrosa-cluster/ Cargo.toml
git commit -m "feat(cluster): scaffold ferrosa-cluster crate with error types and config"
```

---

### Task 2: ConsistencyLevel with blockFor()

**Files:**

- Modify: `ferrosa-cluster/src/consistency.rs` (replace stub)

**Context:** The `ConsistencyLevel` enum defines how many replica ACKs are required before responding to the client. `blockFor(rf)` computes that number. This is standalone with no external dependencies — pure logic with property tests.

**Spec reference:** Part 2 → "Consistency Levels" table

- [ ] **Step 1: Write tests**

Replace the stub `consistency.rs` with the full implementation including tests:

```rust
/// CQL consistency levels for read and write operations.
///
/// Each level defines how many replica acknowledgements are required
/// before the coordinator responds to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsistencyLevel {
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalOne,
    LocalQuorum,
    EachQuorum,
}

impl ConsistencyLevel {
    /// Number of replicas that must acknowledge before responding.
    ///
    /// # Panics
    ///
    /// Panics if called with `EachQuorum` — use [`block_for_dc`] instead.
    pub fn block_for(&self, rf: usize) -> usize {
        match self {
            Self::One | Self::LocalOne => 1,
            Self::Two => 2.min(rf),
            Self::Three => 3.min(rf),
            Self::Quorum | Self::LocalQuorum => rf / 2 + 1,
            Self::All => rf,
            Self::EachQuorum => {
                panic!("use block_for_dc() for EACH_QUORUM");
            }
        }
    }

    /// Number of replicas per data center that must acknowledge.
    pub fn block_for_dc(&self, dc_rf: usize) -> usize {
        match self {
            Self::EachQuorum | Self::LocalQuorum => dc_rf / 2 + 1,
            _ => self.block_for(dc_rf),
        }
    }

    /// Parse from CQL string representation.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ONE" => Some(Self::One),
            "TWO" => Some(Self::Two),
            "THREE" => Some(Self::Three),
            "QUORUM" => Some(Self::Quorum),
            "ALL" => Some(Self::All),
            "LOCAL_ONE" => Some(Self::LocalOne),
            "LOCAL_QUORUM" => Some(Self::LocalQuorum),
            "EACH_QUORUM" => Some(Self::EachQuorum),
            _ => None,
        }
    }

    /// CQL string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::One => "ONE",
            Self::Two => "TWO",
            Self::Three => "THREE",
            Self::Quorum => "QUORUM",
            Self::All => "ALL",
            Self::LocalOne => "LOCAL_ONE",
            Self::LocalQuorum => "LOCAL_QUORUM",
            Self::EachQuorum => "EACH_QUORUM",
        }
    }
}

impl std::fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn block_for_one() {
        assert_eq!(ConsistencyLevel::One.block_for(3), 1);
        assert_eq!(ConsistencyLevel::One.block_for(1), 1);
    }

    #[test]
    fn block_for_quorum() {
        assert_eq!(ConsistencyLevel::Quorum.block_for(3), 2);
        assert_eq!(ConsistencyLevel::Quorum.block_for(5), 3);
        assert_eq!(ConsistencyLevel::Quorum.block_for(1), 1);
    }

    #[test]
    fn block_for_all() {
        assert_eq!(ConsistencyLevel::All.block_for(3), 3);
        assert_eq!(ConsistencyLevel::All.block_for(1), 1);
    }

    #[test]
    fn block_for_two_capped_at_rf() {
        assert_eq!(ConsistencyLevel::Two.block_for(1), 1);
        assert_eq!(ConsistencyLevel::Two.block_for(3), 2);
    }

    #[test]
    fn block_for_dc() {
        assert_eq!(ConsistencyLevel::EachQuorum.block_for_dc(3), 2);
        assert_eq!(ConsistencyLevel::LocalQuorum.block_for_dc(3), 2);
    }

    #[test]
    fn from_str_roundtrip() {
        for cl in &[
            ConsistencyLevel::One,
            ConsistencyLevel::Two,
            ConsistencyLevel::Three,
            ConsistencyLevel::Quorum,
            ConsistencyLevel::All,
            ConsistencyLevel::LocalOne,
            ConsistencyLevel::LocalQuorum,
            ConsistencyLevel::EachQuorum,
        ] {
            assert_eq!(ConsistencyLevel::from_str(cl.as_str()), Some(*cl));
        }
    }

    #[test]
    fn from_str_unknown_returns_none() {
        assert_eq!(ConsistencyLevel::from_str("INVALID"), None);
    }

    proptest! {
        #[test]
        fn block_for_never_exceeds_rf(rf in 1usize..=100) {
            for cl in &[
                ConsistencyLevel::One,
                ConsistencyLevel::Two,
                ConsistencyLevel::Three,
                ConsistencyLevel::Quorum,
                ConsistencyLevel::All,
                ConsistencyLevel::LocalOne,
                ConsistencyLevel::LocalQuorum,
            ] {
                let bf = cl.block_for(rf);
                prop_assert!(bf <= rf, "blockFor({:?}, {}) = {} exceeds RF", cl, rf, bf);
            }
        }

        #[test]
        fn quorum_is_majority(rf in 1usize..=100) {
            let bf = ConsistencyLevel::Quorum.block_for(rf);
            prop_assert!(bf > rf / 2, "QUORUM blockFor({}) = {} is not majority", rf, bf);
        }

        #[test]
        fn block_for_at_least_one(rf in 1usize..=100) {
            for cl in &[
                ConsistencyLevel::One,
                ConsistencyLevel::Two,
                ConsistencyLevel::Three,
                ConsistencyLevel::Quorum,
                ConsistencyLevel::All,
            ] {
                let bf = cl.block_for(rf);
                prop_assert!(bf >= 1, "blockFor({:?}, {}) = 0", cl, rf);
            }
        }
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass including proptest.

- [ ] **Step 3: Commit**

```bash
git add ferrosa-cluster/src/consistency.rs
git commit -m "feat(cluster): add ConsistencyLevel with blockFor() and property tests"
```

---

### Task 3: DeploymentMode and PairRole with PairState

**Files:**

- Modify: `ferrosa-cluster/src/mode.rs` (replace stub)
- Create: `ferrosa-cluster/src/pair/mod.rs`
- Modify: `ferrosa-cluster/src/lib.rs` (add `pair` module)

**Context:** `DeploymentMode` tracks Standalone/Pair/Cluster. `PairRole` tracks Primary/Secondary. Primary election uses deterministic host_id comparison (higher wins, per spec). `PairState` tracks peer connection status and replication position.

**Spec reference:** Part 2 → "Deployment Modes", "Pair Mode", "Primary election on first pair formation"

- [ ] **Step 1: Write `mode.rs`**

Replace the stub with the full implementation:

```rust
/// Deployment mode inferred from peer count or set explicitly.
///
/// Mode transitions are a one-way ratchet: Standalone → Pair → Cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    Standalone,
    Pair,
    Cluster,
}

impl DeploymentMode {
    /// Infer deployment mode from the number of peers (excluding self).
    pub fn from_peer_count(count: usize) -> Self {
        match count {
            0 => Self::Standalone,
            1 => Self::Pair,
            _ => Self::Cluster,
        }
    }

    /// Check if transitioning from `self` to `target` is allowed.
    /// Transitions are one-way: Standalone → Pair → Cluster.
    pub fn can_transition_to(&self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::Standalone, Self::Pair)
                | (Self::Standalone, Self::Cluster)
                | (Self::Pair, Self::Cluster)
        )
    }
}

impl std::fmt::Display for DeploymentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standalone => write!(f, "standalone"),
            Self::Pair => write!(f, "pair"),
            Self::Cluster => write!(f, "cluster"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_peer_count() {
        assert_eq!(DeploymentMode::from_peer_count(0), DeploymentMode::Standalone);
        assert_eq!(DeploymentMode::from_peer_count(1), DeploymentMode::Pair);
        assert_eq!(DeploymentMode::from_peer_count(2), DeploymentMode::Cluster);
        assert_eq!(DeploymentMode::from_peer_count(10), DeploymentMode::Cluster);
    }

    #[test]
    fn transitions_are_one_way() {
        assert!(DeploymentMode::Standalone.can_transition_to(DeploymentMode::Pair));
        assert!(DeploymentMode::Standalone.can_transition_to(DeploymentMode::Cluster));
        assert!(DeploymentMode::Pair.can_transition_to(DeploymentMode::Cluster));
        assert!(!DeploymentMode::Pair.can_transition_to(DeploymentMode::Standalone));
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Pair));
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Standalone));
    }
}
```

- [ ] **Step 2: Write `pair/mod.rs`**

```rust
pub mod coordinator;
pub mod handler;
pub mod node;
pub mod catchup;
pub mod switchover;

use std::net::SocketAddr;
use uuid::Uuid;

/// Role within a pair. Determined by host_id comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairRole {
    Primary,
    Secondary,
}

impl PairRole {
    /// Determine this node's role by comparing host_ids.
    /// The higher host_id becomes primary (deterministic, no consensus needed).
    pub fn elect(local_id: Uuid, peer_id: Uuid) -> Self {
        if local_id > peer_id {
            Self::Primary
        } else {
            Self::Secondary
        }
    }

    /// Return the opposite role.
    pub fn opposite(&self) -> Self {
        match self {
            Self::Primary => Self::Secondary,
            Self::Secondary => Self::Primary,
        }
    }
}

impl std::fmt::Display for PairRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Secondary => write!(f, "secondary"),
        }
    }
}

/// Tracks the state of the pair relationship.
pub struct PairState {
    /// This node's current role.
    pub role: PairRole,
    /// Peer's host_id.
    pub peer_host_id: Uuid,
    /// Peer's internode address.
    pub peer_addr: SocketAddr,
    /// Whether the peer is currently connected.
    pub connected: bool,
    /// Last commit log position successfully replicated to peer.
    /// `(segment_id, offset)`.
    pub last_replicated_position: Option<(u64, u64)>,
}

impl PairState {
    pub fn new(role: PairRole, peer_host_id: Uuid, peer_addr: SocketAddr) -> Self {
        Self {
            role,
            peer_host_id,
            peer_addr,
            connected: false,
            last_replicated_position: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elect_primary_higher_id_wins() {
        let high = Uuid::from_bytes([0xFF; 16]);
        let low = Uuid::from_bytes([0x00; 16]);

        assert_eq!(PairRole::elect(high, low), PairRole::Primary);
        assert_eq!(PairRole::elect(low, high), PairRole::Secondary);
    }

    #[test]
    fn role_opposite() {
        assert_eq!(PairRole::Primary.opposite(), PairRole::Secondary);
        assert_eq!(PairRole::Secondary.opposite(), PairRole::Primary);
    }

    #[test]
    fn pair_state_default_not_connected() {
        let state = PairState::new(
            PairRole::Primary,
            Uuid::new_v4(),
            "127.0.0.1:7000".parse().unwrap(),
        );
        assert!(!state.connected);
        assert!(state.last_replicated_position.is_none());
    }
}
```

Note: The submodules `coordinator`, `handler`, `node`, `catchup`, `switchover` will be created in later tasks. For now create empty files so the crate compiles:

```rust
// ferrosa-cluster/src/pair/coordinator.rs
// ferrosa-cluster/src/pair/handler.rs
// ferrosa-cluster/src/pair/node.rs
// ferrosa-cluster/src/pair/catchup.rs
// ferrosa-cluster/src/pair/switchover.rs
```

- [ ] **Step 3: Update `lib.rs`**

Add `pub mod pair;` to `ferrosa-cluster/src/lib.rs` and add re-exports:

```rust
pub mod config;
pub mod consistency;
pub mod error;
pub mod mode;
pub mod pair;

pub use config::ClusterConfig;
pub use consistency::ConsistencyLevel;
pub use error::{ClusterError, Result};
pub use mode::DeploymentMode;
pub use pair::PairRole;
```

- [ ] **Step 4: Verify it compiles and tests pass**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cluster/src/mode.rs ferrosa-cluster/src/pair/ ferrosa-cluster/src/lib.rs
git commit -m "feat(cluster): add DeploymentMode, PairRole, and PairState"
```

---

## Chunk 2: Write Path

### Task 4: PairCoordinator — write coordination for both roles

**Files:**

- Modify: `ferrosa-cluster/src/pair/coordinator.rs` (replace empty stub)

**Context:** The `PairCoordinator` owns the write path. It decides whether to write locally + replicate (primary) or forward to primary (secondary). It encodes/decodes `Mutation` for the wire using the existing `Mutation::serialize_into` / `Mutation::deserialize_from` from ferrosa-storage.

**Write flows:**

*Client → Primary:*

1. `coordinate_write()` writes to local `StorageEngine`
2. Encodes mutation, sends `PairWriteForward` to secondary via `PeerManager`
3. Secondary's handler applies locally, returns `PairWriteAck`
4. Returns success to CQL client

*Client → Secondary:*

1. `coordinate_write()` forwards to primary via `PairWriteForward`
2. Primary's handler writes locally + replicates to secondary (same flow as above)
3. Primary returns `PairWriteAck`
4. Returns success to CQL client (secondary already has data via handler)

**Key types from dependencies:**

- `ferrosa_storage::Mutation` — `serialized_size()`, `serialize_into(&mut [u8])`, `deserialize_from(&[u8])`
- `ferrosa_storage::TableId` — `{ keyspace: String, table: String }`
- `ferrosa_storage::StorageEngine` — `write(table_id, key, row, timestamp)`
- `ferrosa_net::Message::PairWriteForward(Bytes)` / `PairWriteAck(Bytes)`
- `ferrosa_net::codec::Lane::Data`
- `ferrosa_net::peer::PeerManager` — `send(host_id, msg, lane) -> Result<Message>`

- [ ] **Step 1: Write the coordinator**

```rust
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_storage::commitlog::TableId;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::Mutation;

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// Coordinates writes in pair mode.
///
/// Primary: writes locally, then replicates to secondary.
/// Secondary: forwards to primary (which writes + replicates back).
pub struct PairCoordinator {
    role: Arc<ArcSwap<PairRole>>,
    peer_host_id: Uuid,
    storage: Arc<StorageEngine>,
    peer_manager: Arc<PeerManager>,
}

impl PairCoordinator {
    pub fn new(
        role: Arc<ArcSwap<PairRole>>,
        peer_host_id: Uuid,
        storage: Arc<StorageEngine>,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            role,
            peer_host_id,
            storage,
            peer_manager,
        }
    }

    /// Route a write based on current role.
    pub async fn coordinate_write(&self, mutation: &Mutation) -> Result<()> {
        match **self.role.load() {
            PairRole::Primary => {
                self.apply_locally(mutation)?;
                self.replicate_to_peer(mutation).await?;
                Ok(())
            }
            PairRole::Secondary => self.forward_to_primary(mutation).await,
        }
    }

    /// Apply a mutation to local storage.
    pub(crate) fn apply_locally(&self, mutation: &Mutation) -> Result<()> {
        let table_id = TableId {
            keyspace: mutation.keyspace.clone(),
            table: mutation.table.clone(),
        };
        for row in &mutation.rows {
            self.storage
                .write(&table_id, &mutation.key, row.clone(), mutation.timestamp)
                .map_err(ClusterError::Storage)?;
        }
        Ok(())
    }

    /// Send a mutation to the peer and wait for ACK.
    pub(crate) async fn replicate_to_peer(&self, mutation: &Mutation) -> Result<()> {
        let body = encode_mutation(mutation)?;
        let resp = self
            .peer_manager
            .send(self.peer_host_id, Message::PairWriteForward(body), Lane::Data)
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairWriteAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairWriteAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Forward a write to the primary and wait for ACK.
    async fn forward_to_primary(&self, mutation: &Mutation) -> Result<()> {
        let body = encode_mutation(mutation)?;
        let resp = self
            .peer_manager
            .send(self.peer_host_id, Message::PairWriteForward(body), Lane::Data)
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairWriteAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairWriteAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Get current role.
    pub fn role(&self) -> PairRole {
        **self.role.load()
    }

    /// Update peer host_id (used during switchover).
    pub fn peer_host_id(&self) -> Uuid {
        self.peer_host_id
    }
}

/// Encode a Mutation into Bytes for the wire.
pub fn encode_mutation(mutation: &Mutation) -> Result<Bytes> {
    let size = mutation.serialized_size();
    let mut buf = vec![0u8; size];
    mutation.serialize_into(&mut buf);
    Ok(Bytes::from(buf))
}

/// Decode a Mutation from wire bytes.
pub fn decode_mutation(body: &[u8]) -> Result<Mutation> {
    Mutation::deserialize_from(body).map_err(ClusterError::Storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn test_mutation() -> Mutation {
        let key = DecoratedKey {
            token: Token(42),
            key: PartitionKey(vec![1, 2, 3]),
        };
        let row = Row {
            clustering: vec![10, 20],
            cells: vec![(0, CellValue::live(vec![100], 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        Mutation {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key,
            rows: vec![row],
            timestamp: 1000,
        }
    }

    #[test]
    fn encode_decode_mutation_roundtrip() {
        let mutation = test_mutation();
        let encoded = encode_mutation(&mutation).unwrap();
        let decoded = decode_mutation(&encoded).unwrap();

        assert_eq!(decoded.keyspace, mutation.keyspace);
        assert_eq!(decoded.table, mutation.table);
        assert_eq!(decoded.timestamp, mutation.timestamp);
        assert_eq!(decoded.rows.len(), mutation.rows.len());
    }
}
```

**Note:** `DeletionTime::LIVE` is a const and `LivenessInfo::with_timestamp(ts)` is the constructor. `CellValue::live(bytes, timestamp)` creates a live cell.

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p ferrosa-cluster`
Expected: Compiles. If `DeletionTime::live()` or `LivenessInfo::live()` don't exist, adjust the test helper to use struct literals.

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-cluster`
Expected: `encode_decode_mutation_roundtrip` passes.

- [ ] **Step 4: Commit**

```bash
git add ferrosa-cluster/src/pair/coordinator.rs
git commit -m "feat(cluster): add PairCoordinator with write coordination for both roles"
```

---

### Task 5: PairWriteForwardHandler — RPC handler for replicated writes

**Files:**

- Modify: `ferrosa-cluster/src/pair/handler.rs` (replace empty stub)

**Context:** The `PairWriteForwardHandler` is an `RpcHandler` (from ferrosa-net) that processes `PairWriteForward` messages. Its behavior depends on the node's role:

- **Primary receives PairWriteForward** (forwarded from secondary): write locally, replicate to secondary, return PairWriteAck.
- **Secondary receives PairWriteForward** (replicated from primary): write locally, return PairWriteAck.

The handler delegates to `PairCoordinator` methods.

**Key trait from ferrosa-net:**

```rust
#[async_trait]
pub trait RpcHandler: Send + Sync {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message>;
}
```

- [ ] **Step 1: Write the handler**

```rust
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::handler::RpcHandler;

use crate::pair::coordinator::{decode_mutation, PairCoordinator};
use crate::pair::PairRole;

/// Handles incoming PairWriteForward messages.
///
/// Primary: applies locally + replicates to secondary, then ACKs.
/// Secondary: applies locally, then ACKs (no further replication).
pub struct PairWriteForwardHandler {
    role: Arc<ArcSwap<PairRole>>,
    coordinator: Arc<PairCoordinator>,
}

impl PairWriteForwardHandler {
    pub fn new(role: Arc<ArcSwap<PairRole>>, coordinator: Arc<PairCoordinator>) -> Self {
        Self { role, coordinator }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairWriteForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairWriteForward(b) => b,
            _ => return None,
        };

        let mutation = match decode_mutation(&body) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("failed to decode PairWriteForward: {e}");
                return None;
            }
        };

        let result = match **self.role.load() {
            PairRole::Primary => {
                // Forwarded write from secondary: apply + replicate back
                if let Err(e) = self.coordinator.apply_locally(&mutation) {
                    tracing::error!("failed to apply forwarded write: {e}");
                    return None;
                }
                self.coordinator.replicate_to_peer(&mutation).await
            }
            PairRole::Secondary => {
                // Replicated write from primary: apply locally only
                self.coordinator.apply_locally(&mutation).map_err(Into::into)
            }
        };

        match result {
            Ok(()) => Some(Message::PairWriteAck(Bytes::new())),
            Err(e) => {
                tracing::error!("PairWriteForward handler failed: {e}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_rejects_wrong_message_type() {
        // Verify the handler returns None for non-PairWriteForward messages.
        // Full integration tests with real StorageEngine are in Task 10.
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p ferrosa-cluster`
Expected: Compiles.

- [ ] **Step 3: Commit**

```bash
git add ferrosa-cluster/src/pair/handler.rs
git commit -m "feat(cluster): add PairWriteForwardHandler RPC handler"
```

---

### Task 6: PairNode — integration struct with PeerEventListener

**Files:**

- Modify: `ferrosa-cluster/src/pair/node.rs` (replace empty stub)
- Modify: `ferrosa-cluster/src/pair/mod.rs` (add re-exports)
- Modify: `ferrosa-cluster/src/lib.rs` (add re-exports)

**Context:** `PairNode` is the integration struct that brings everything together. It:

1. Creates the `PairCoordinator`
2. Creates RPC handlers and registers them in a `HandlerRegistry`
3. Implements `PeerEventListener` to handle connect/disconnect/suspected events
4. Provides a `start()` method that starts the RPC server and connects to peer

**Key types:**

- `ferrosa_net::rpc::HandlerRegistry` — `new()`, `register(msg_type, handler)`
- `ferrosa_net::rpc::server::RpcServer` — `new(config, host_id, registry)` then `start_and_get_addr(&Arc<Self>)`
- `ferrosa_net::peer::PeerEventListener` — `on_peer_connected`, `on_peer_disconnected`, `on_peer_suspected`
- `ferrosa_net::peer::PeerManager` — `new(config, host_id, listener)`, `add_peer(peer_id, pool)`
- `ferrosa_net::pool::PriorityPool` — `connect(config, host_id, peer_addr)`
- `ferrosa_net::codec::MsgType::PairWriteForward`

- [ ] **Step 1: Write PairNode**

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::RwLock;
use uuid::Uuid;

use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::pool::PriorityPool;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_storage::engine::StorageEngine;

use crate::config::ClusterConfig;
use crate::error::Result;
use crate::pair::coordinator::PairCoordinator;
use crate::pair::handler::PairWriteForwardHandler;
use crate::pair::{PairRole, PairState};

/// Integration struct for pair mode.
///
/// Owns the coordinator, RPC handlers, peer manager, and RPC server.
/// Implements `PeerEventListener` for lifecycle events.
pub struct PairNode {
    pub(crate) config: Arc<ClusterConfig>,
    pub(crate) net_config: Arc<NetConfig>,
    pub(crate) local_host_id: Uuid,
    pub(crate) role: Arc<ArcSwap<PairRole>>,
    pub(crate) state: Arc<RwLock<PairState>>,
    pub(crate) coordinator: Arc<PairCoordinator>,
    pub(crate) peer_manager: Arc<PeerManager>,
    pub(crate) storage: Arc<StorageEngine>,
}

/// Listener that logs peer events. PairNode handles events through
/// a separate mechanism since PeerManager owns the listener.
pub struct PairEventListener {
    role: Arc<ArcSwap<PairRole>>,
    state: Arc<RwLock<PairState>>,
}

impl PairEventListener {
    pub fn new(role: Arc<ArcSwap<PairRole>>, state: Arc<RwLock<PairState>>) -> Self {
        Self { role, state }
    }
}

impl PeerEventListener for PairEventListener {
    fn on_peer_connected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::info!(
            role = %**self.role.load(),
            peer = %host_id,
            "peer connected"
        );
        // Mark peer as connected — catch-up will be triggered by PairNode
        let state = self.state.clone();
        tokio::spawn(async move {
            state.write().await.connected = true;
        });
    }

    fn on_peer_disconnected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(
            role = %**self.role.load(),
            peer = %host_id,
            "peer disconnected"
        );
        let state = self.state.clone();
        tokio::spawn(async move {
            state.write().await.connected = false;
        });
    }

    fn on_peer_suspected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(
            role = %**self.role.load(),
            peer = %host_id,
            "peer suspected dead"
        );
        let state = self.state.clone();
        tokio::spawn(async move {
            state.write().await.connected = false;
        });
    }
}

impl PairNode {
    /// Create a new PairNode. Does not start networking — call `start()`.
    pub fn new(
        config: Arc<ClusterConfig>,
        net_config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_host_id: Uuid,
        peer_addr: SocketAddr,
        storage: Arc<StorageEngine>,
    ) -> Self {
        let role = PairRole::elect(local_host_id, peer_host_id);
        let role_arc = Arc::new(ArcSwap::from_pointee(role));
        let state = Arc::new(RwLock::new(PairState::new(role, peer_host_id, peer_addr)));

        let listener = Arc::new(PairEventListener::new(role_arc.clone(), state.clone()));
        let peer_manager = Arc::new(PeerManager::new(
            net_config.clone(),
            local_host_id,
            listener,
        ));

        let coordinator = Arc::new(PairCoordinator::new(
            role_arc.clone(),
            peer_host_id,
            storage.clone(),
            peer_manager.clone(),
        ));

        Self {
            config,
            net_config,
            local_host_id,
            role: role_arc,
            state,
            coordinator,
            peer_manager,
            storage,
        }
    }

    /// Build the handler registry with pair mode RPC handlers.
    pub fn build_registry(&self) -> HandlerRegistry {
        let mut registry = HandlerRegistry::new();
        let handler = Arc::new(PairWriteForwardHandler::new(
            self.role.clone(),
            self.coordinator.clone(),
        ));
        registry.register(MsgType::PairWriteForward, handler);
        registry
    }

    /// Start the RPC server and connect to peer.
    /// Returns the bound address of this node's RPC server.
    pub async fn start(&self) -> Result<SocketAddr> {
        let registry = self.build_registry();
        let server = Arc::new(RpcServer::new(
            (*self.net_config).clone(),
            self.local_host_id,
            registry,
        ));
        let addr = server
            .start_and_get_addr()
            .await
            .map_err(crate::error::ClusterError::Net)?;

        // Try to connect to peer — log and continue if peer isn't up yet.
        // PairEventListener or manual reconnect will handle it later.
        let peer_addr = self.state.read().await.peer_addr;
        let peer_host_id = self.state.read().await.peer_host_id;
        match PriorityPool::connect(
            self.net_config.clone(),
            self.local_host_id,
            peer_addr,
        )
        .await
        {
            Ok(pool) => {
                self.peer_manager
                    .add_peer((peer_host_id, peer_addr), pool)
                    .await;
                self.state.write().await.connected = true;
                tracing::info!(
                    role = %self.role(),
                    peer = %peer_host_id,
                    addr = %addr,
                    "pair node started, peer connected"
                );
            }
            Err(e) => {
                tracing::warn!(
                    role = %self.role(),
                    peer = %peer_host_id,
                    error = %e,
                    addr = %addr,
                    "pair node started, peer connection deferred"
                );
            }
        }

        Ok(addr)
    }

    /// Connect (or reconnect) to the peer. Call after peer is known to be up.
    pub async fn connect_to_peer(&self, peer_addr: std::net::SocketAddr) -> Result<()> {
        let pool = PriorityPool::connect(
            self.net_config.clone(),
            self.local_host_id,
            peer_addr,
        )
        .await
        .map_err(crate::error::ClusterError::Net)?;

        let peer_host_id = self.state.read().await.peer_host_id;
        self.peer_manager
            .add_peer((peer_host_id, peer_addr), pool)
            .await;
        self.state.write().await.connected = true;
        Ok(())
    }

    /// Get current role.
    pub fn role(&self) -> PairRole {
        **self.role.load()
    }

    /// Get the coordinator for write coordination.
    pub fn coordinator(&self) -> &Arc<PairCoordinator> {
        &self.coordinator
    }

    /// Check if peer is connected.
    pub async fn is_peer_connected(&self) -> bool {
        self.state.read().await.connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_node_elects_role_on_creation() {
        let high = Uuid::from_bytes([0xFF; 16]);
        let low = Uuid::from_bytes([0x00; 16]);

        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_config = test_storage_config(storage_dir.path());
        let storage = Arc::new(StorageEngine::new(storage_config, None).unwrap());

        let node = PairNode::new(
            config,
            net_config,
            high,
            low,
            "127.0.0.1:7000".parse().unwrap(),
            storage,
        );
        assert_eq!(node.role(), PairRole::Primary);
    }
}
```

**Note:** `StorageEngineConfig` has no `Default` impl. Use this helper (also used in Task 10 integration tests):

```rust
fn test_storage_config(dir: &std::path::Path) -> ferrosa_storage::engine::StorageEngineConfig {
    use ferrosa_storage::commitlog::config::CommitLogConfig;
    use ferrosa_storage::compaction::strategy::CompactionConfig;
    ferrosa_storage::engine::StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        data_dir: dir.to_path_buf(),
    }
}
```

- [ ] **Step 2: Update `pair/mod.rs` with re-exports**

Add to `ferrosa-cluster/src/pair/mod.rs`:

```rust
pub use coordinator::PairCoordinator;
pub use handler::PairWriteForwardHandler;
pub use node::PairNode;
```

- [ ] **Step 3: Update `lib.rs` with re-exports**

Add to `ferrosa-cluster/src/lib.rs`:

```rust
pub use pair::{PairCoordinator, PairNode, PairRole};
```

- [ ] **Step 4: Verify it compiles and tests pass**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass including the new `pair_node_elects_role_on_creation`.

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cluster/src/pair/node.rs ferrosa-cluster/src/pair/mod.rs ferrosa-cluster/src/lib.rs
git commit -m "feat(cluster): add PairNode integration struct with PeerEventListener"
```

---

## Chunk 3: Recovery and Operations

### Task 7: ClusterState implementation for pair mode

**Files:**

- Modify: `ferrosa-cluster/src/state.rs` (create, was not stubbed)
- Modify: `ferrosa-cluster/src/lib.rs` (add module)

**Context:** The `ClusterState` trait (in ferrosa-schema) requires `fn peers(&self) -> Vec<PeerInfo>`. The pair mode implementation returns the single peer's information.

**Key types:**

- `ferrosa_schema::system::peers::ClusterState` trait — `fn peers(&self) -> Vec<PeerInfo>`
- `ferrosa_schema::system::peers::PeerInfo` — struct with data_center, rack, host_id, etc.

- [ ] **Step 1: Write state.rs**

```rust
use std::net::IpAddr;
use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;

use ferrosa_schema::system::peers::{ClusterState, PeerInfo};

use crate::config::ClusterConfig;
use crate::pair::PairState;

/// ClusterState implementation for pair mode.
///
/// Returns the single peer as the only entry in `peers()`.
pub struct PairClusterState {
    config: Arc<ClusterConfig>,
    state: Arc<RwLock<PairState>>,
}

impl PairClusterState {
    pub fn new(config: Arc<ClusterConfig>, state: Arc<RwLock<PairState>>) -> Self {
        Self { config, state }
    }
}

impl ClusterState for PairClusterState {
    fn peers(&self) -> Vec<PeerInfo> {
        // We need synchronous access but state is behind RwLock.
        // Use try_read to avoid blocking. If locked, return empty.
        let state = match self.state.try_read() {
            Ok(s) => s,
            Err(_) => return vec![],
        };

        let peer_addr = state.peer_addr;
        let ip = peer_addr.ip();
        let port = peer_addr.port();

        vec![PeerInfo {
            peer: ip,
            peer_port: port,
            data_center: self.config.data_center.clone(),
            rack: self.config.rack.clone(),
            host_id: state.peer_host_id,
            preferred_ip: None,
            preferred_port: None,
            native_address: ip,
            native_port: 9042,
            schema_version: Uuid::nil(),
            tokens: vec![],
            release_version: env!("CARGO_PKG_VERSION").to_string(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pair::PairRole;

    #[test]
    fn pair_cluster_state_returns_peer() {
        let config = Arc::new(ClusterConfig::default());
        let peer_id = Uuid::new_v4();
        let peer_addr = "10.0.1.5:7000".parse().unwrap();
        let state = Arc::new(RwLock::new(PairState::new(
            PairRole::Primary,
            peer_id,
            peer_addr,
        )));
        let cluster_state = PairClusterState::new(config, state);

        let peers = cluster_state.peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].host_id, peer_id);
        assert_eq!(peers[0].peer, "10.0.1.5".parse::<IpAddr>().unwrap());
        assert_eq!(peers[0].peer_port, 7000);
    }
}
```

- [ ] **Step 2: Update lib.rs**

Add `pub mod state;` and `pub use state::PairClusterState;` to `ferrosa-cluster/src/lib.rs`.

- [ ] **Step 3: Verify it compiles and tests pass**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add ferrosa-cluster/src/state.rs ferrosa-cluster/src/lib.rs
git commit -m "feat(cluster): add PairClusterState implementing ClusterState trait"
```

---

### Task 8: Catch-up protocol

**Files:**

- Modify: `ferrosa-cluster/src/pair/catchup.rs` (replace empty stub)
- Modify: `ferrosa-storage/src/engine.rs` (add public `replay_from()` accessor)
- Modify: `ferrosa-storage/src/commitlog/mod.rs` (add `replay_from()` on CommitLog)

**Context:** When the secondary reconnects after a disconnect, it needs to catch up on missed mutations. The protocol:

1. Secondary sends `PairCatchUp { last_segment_id, last_offset }` to primary
2. Primary locates that position in commit log and replays all mutations from that point
3. Primary sends mutations via `PairCatchUpResponse(Bytes)` (serialized `Vec<Mutation>`)
4. If the segment has been recycled, primary responds with empty body → secondary requires full bootstrap

**Spec reference:** Part 2 → "Secondary catch-up protocol"

**Prerequisite: Add `replay_from()` to CommitLog.** The existing `CommitLog` has `open_and_replay()` (replays everything) but no method to replay from a specific position. We need to add one. Read `ferrosa-storage/src/commitlog/mod.rs` to understand the segment storage.

- [ ] **Step 1: Add `replay_from()` to StorageEngine**

The `CommitLog` field is `pub(crate)` on `StorageEngine`, so we add a public method on `StorageEngine` that delegates. Add to `ferrosa-storage/src/engine.rs`:

```rust
/// Replay mutations from a given commit log position forward.
///
/// Returns mutations with positions after `position`. If the segment
/// has been recycled, returns an empty vec (caller should bootstrap).
pub fn replay_from(&self, position: CommitLogPosition) -> ferrosa_common::Result<Vec<Mutation>> {
    self.commit_log.replay_from(position)
}
```

Then add to `ferrosa-storage/src/commitlog/mod.rs` on `impl CommitLog`:

```rust
/// Replay mutations from a given position forward.
///
/// Walks closed_segments + active segment, filtering to entries
/// after the given position. Returns empty vec if the requested
/// segment has been recycled.
pub fn replay_from(&self, position: CommitLogPosition) -> ferrosa_common::Result<Vec<Mutation>> {
    // Implementation must use the actual internal fields:
    // - self.closed_segments: Mutex<Vec<Arc<Segment>>>
    // - self.active: Arc<ArcSwap<Segment>>
    //
    // Read closed_segments, iterate segments with id >= position.segment_id,
    // use SegmentReader to read mutations, filter by offset > position.offset.
    // Then check the active segment similarly.
    //
    // If the requested segment_id is older than the oldest closed segment,
    // return Ok(vec![]) to signal "segment recycled, need full bootstrap".
    todo!("implement using actual segment internals — see commitlog/segment.rs")
}
```

**Note:** The exact implementation depends on `Segment` and `SegmentReader` internals. The implementer should read `ferrosa-storage/src/commitlog/segment.rs` to find the right methods for reading mutations from a specific segment. A unit test for `replay_from()` should be added to `ferrosa-storage/src/commitlog/mod.rs` (see Step 2b below).

Also add a test for `replay_from()` in the storage crate's own tests:

```rust
// In ferrosa-storage/src/commitlog/mod.rs #[cfg(test)] mod tests:
#[test]
fn replay_from_returns_mutations_after_position() {
    let dir = tempfile::tempdir().unwrap();
    let config = CommitLogConfig::test_config(dir.path());
    let commit_log = CommitLog::new(config).unwrap();

    // Append a few mutations, capture position after first
    let m1 = test_mutation("ks", "tbl", 1);
    commit_log.append(&m1).unwrap();
    // ... append more, then replay_from the first position
    // Assert only later mutations are returned
}
```

- [ ] **Step 2: Write catch-up protocol**

```rust
use std::sync::Arc;

use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_storage::CommitLogPosition;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::Mutation;

use crate::error::{ClusterError, Result};
use crate::pair::coordinator::{decode_mutation, encode_mutation};

/// Initiate catch-up from the secondary side.
///
/// Sends `PairCatchUp` to primary with last known position.
/// Primary replays mutations from that point forward.
pub async fn request_catchup(
    peer_manager: &PeerManager,
    peer_host_id: Uuid,
    last_position: Option<(u64, u64)>,
) -> Result<Vec<Mutation>> {
    let (segment_id, offset) = last_position.unwrap_or((0, 0));

    // Wire protocol uses u32 for offset; truncation is safe because
    // CommitLog segments are small (default 32 MB) so offset fits in u32.
    let wire_offset = u32::try_from(offset)
        .map_err(|_| ClusterError::Internal("catch-up offset exceeds u32::MAX".into()))?;
    let resp = peer_manager
        .send(
            peer_host_id,
            Message::PairCatchUp {
                last_segment_id: segment_id,
                last_offset: wire_offset,
            },
            Lane::Bulk,
        )
        .await
        .map_err(ClusterError::Net)?;

    match resp {
        Message::PairCatchUpResponse(body) => {
            if body.is_empty() {
                return Err(ClusterError::CatchUpRequired);
            }
            decode_catchup_response(&body)
        }
        other => Err(ClusterError::ReplicationFailed(format!(
            "expected PairCatchUpResponse, got {:?}",
            other.msg_type()
        ))),
    }
}

/// RPC handler for PairCatchUp requests (runs on primary).
pub struct PairCatchUpHandler {
    storage: Arc<StorageEngine>,
}

impl PairCatchUpHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairCatchUpHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let (segment_id, offset) = match msg {
            Message::PairCatchUp {
                last_segment_id,
                last_offset,
            } => (last_segment_id, last_offset),
            _ => return None,
        };

        // Wire u32 offset widens to internal u64 — safe, no data loss
        let position = CommitLogPosition {
            segment_id,
            offset: u64::from(offset),
        };

        match self.storage.replay_from(position) {
            Ok(mutations) => {
                if mutations.is_empty() {
                    // Either caught up or segment recycled
                    Some(Message::PairCatchUpResponse(Bytes::new()))
                } else {
                    match encode_catchup_response(&mutations) {
                        Ok(body) => Some(Message::PairCatchUpResponse(body)),
                        Err(e) => {
                            tracing::error!("failed to encode catch-up response: {e}");
                            None
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("catch-up replay failed: {e}");
                Some(Message::PairCatchUpResponse(Bytes::new()))
            }
        }
    }
}

/// Encode a list of mutations for catch-up response.
/// Format: [count:u32] [size:u32 data:bytes]*
fn encode_catchup_response(mutations: &[Mutation]) -> Result<Bytes> {
    let mut buf = Vec::new();
    let count = u32::try_from(mutations.len())
        .map_err(|_| ClusterError::Internal("too many mutations".into()))?;
    buf.extend_from_slice(&count.to_be_bytes());

    for mutation in mutations {
        let size = mutation.serialized_size();
        let size_u32 = u32::try_from(size)
            .map_err(|_| ClusterError::Internal("mutation too large".into()))?;
        buf.extend_from_slice(&size_u32.to_be_bytes());
        let offset = buf.len();
        buf.resize(offset + size, 0);
        mutation.serialize_into(&mut buf[offset..]);
    }

    Ok(Bytes::from(buf))
}

/// Decode a catch-up response into a list of mutations.
fn decode_catchup_response(body: &[u8]) -> Result<Vec<Mutation>> {
    if body.len() < 4 {
        return Err(ClusterError::Internal("truncated catch-up response".into()));
    }
    let count = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let mut offset = 4;
    let mut mutations = Vec::with_capacity(count);

    for _ in 0..count {
        if offset + 4 > body.len() {
            return Err(ClusterError::Internal("truncated mutation size".into()));
        }
        let size = u32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + size > body.len() {
            return Err(ClusterError::Internal("truncated mutation body".into()));
        }
        let mutation = Mutation::deserialize_from(&body[offset..offset + size])
            .map_err(ClusterError::Storage)?;
        mutations.push(mutation);
        offset += size;
    }

    Ok(mutations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn test_mutation(ts: i64) -> Mutation {
        let key = DecoratedKey {
            token: Token(ts),
            key: PartitionKey(vec![1, 2, 3]),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(vec![ts as u8], ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        Mutation {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key,
            rows: vec![row],
            timestamp: ts,
        }
    }

    #[test]
    fn encode_decode_catchup_response_roundtrip() {
        let mutations = vec![test_mutation(1), test_mutation(2), test_mutation(3)];
        let encoded = encode_catchup_response(&mutations).unwrap();
        let decoded = decode_catchup_response(&encoded).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].timestamp, 1);
        assert_eq!(decoded[1].timestamp, 2);
        assert_eq!(decoded[2].timestamp, 3);
    }

    #[test]
    fn decode_empty_body_returns_error() {
        assert!(decode_catchup_response(&[]).is_err());
    }

    #[test]
    fn encode_empty_mutations_list() {
        let encoded = encode_catchup_response(&[]).unwrap();
        let decoded = decode_catchup_response(&encoded).unwrap();
        assert!(decoded.is_empty());
    }
}
```

**Note:** The `commit_log` field is `pub(crate)` on `StorageEngine`. Step 1 adds a public `StorageEngine::replay_from()` method that delegates to `CommitLog::replay_from()`, keeping the field encapsulated.

- [ ] **Step 3: Register PairCatchUpHandler in PairNode::build_registry()**

In `ferrosa-cluster/src/pair/node.rs`, add to `build_registry()`:

```rust
use crate::pair::catchup::PairCatchUpHandler;
use ferrosa_net::codec::MsgType;

// Inside build_registry():
let catchup_handler = Arc::new(PairCatchUpHandler::new(self.storage.clone()));
registry.register(MsgType::PairCatchUp, catchup_handler);
```

- [ ] **Step 4: Verify it compiles and tests pass**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass including catch-up encode/decode roundtrip.

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cluster/src/pair/catchup.rs ferrosa-cluster/src/pair/node.rs ferrosa-storage/src/engine.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "feat(cluster): add catch-up protocol with CommitLog replay_from"
```

---

### Task 9: Switchover — operator-initiated role swap

**Files:**

- Modify: `ferrosa-cluster/src/pair/switchover.rs` (replace empty stub)
- Modify: `ferrosa-cluster/src/pair/node.rs` (add switchover method)

**Context:** Switchover swaps primary and secondary roles. The process:

1. Operator sends switchover command (via ferrosa-ctl or admin RPC)
2. Primary stops accepting new writes (drains in-flight)
3. Primary confirms secondary is fully caught up (replication lag = 0)
4. Primary sends `RoleSwap { new_primary, new_secondary }` to secondary
5. Secondary promotes to primary, begins accepting writes
6. Old primary demotes to secondary

**Spec reference:** Part 2 → "Switchover (planned, operator-initiated)"
**Threat model:** T11 (split brain) — no auto-promotion, operator control only. T13 — requires authenticated host_id.

**Phase 1 limitation:** The spec calls for "draining in-flight writes" before swapping. Phase 1 does NOT implement write draining — the switchover happens immediately. This means writes that are in-flight during the swap may fail. This is acceptable for Phase 1 since switchover is operator-initiated and brief unavailability is expected.

- [ ] **Step 1: Write switchover protocol**

```rust
use std::sync::Arc;

use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// Initiate switchover from the primary side.
///
/// Sends `RoleSwap` to the secondary, then swaps local role.
pub async fn initiate_switchover(
    peer_manager: &PeerManager,
    local_host_id: Uuid,
    peer_host_id: Uuid,
    role: &arc_swap::ArcSwap<PairRole>,
) -> Result<()> {
    if **role.load() != PairRole::Primary {
        return Err(ClusterError::NotPrimary);
    }

    // Send RoleSwap to secondary
    let resp = peer_manager
        .send(
            peer_host_id,
            Message::RoleSwap {
                new_primary: peer_host_id,
                new_secondary: local_host_id,
            },
            Lane::Raft,
        )
        .await
        .map_err(ClusterError::Net)?;

    // Verify ACK (secondary responds with RoleSwap echoing the assignment)
    match resp {
        Message::RoleSwap {
            new_primary,
            new_secondary,
        } => {
            if new_primary != peer_host_id || new_secondary != local_host_id {
                return Err(ClusterError::ReplicationFailed(
                    "role swap response mismatch".into(),
                ));
            }
        }
        other => {
            return Err(ClusterError::ReplicationFailed(format!(
                "expected RoleSwap response, got {:?}",
                other.msg_type()
            )));
        }
    }

    // Swap local role
    role.store(Arc::new(PairRole::Secondary));
    tracing::info!("switchover complete: demoted to secondary");
    Ok(())
}

/// RPC handler for RoleSwap messages (runs on secondary).
pub struct RoleSwapHandler {
    local_host_id: Uuid,
    role: Arc<arc_swap::ArcSwap<PairRole>>,
}

impl RoleSwapHandler {
    pub fn new(local_host_id: Uuid, role: Arc<arc_swap::ArcSwap<PairRole>>) -> Self {
        Self {
            local_host_id,
            role,
        }
    }
}

#[async_trait::async_trait]
impl RpcHandler for RoleSwapHandler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let (new_primary, new_secondary) = match msg {
            Message::RoleSwap {
                new_primary,
                new_secondary,
            } => (new_primary, new_secondary),
            _ => return None,
        };

        // Verify this node is being promoted
        if new_primary != self.local_host_id {
            tracing::error!(
                "role swap: expected new_primary={}, got {}",
                self.local_host_id,
                new_primary
            );
            return None;
        }

        // Promote to primary
        self.role.store(Arc::new(PairRole::Primary));
        tracing::info!("switchover complete: promoted to primary");

        // Echo back the assignment as confirmation
        Some(Message::RoleSwap {
            new_primary,
            new_secondary,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_swap_handler_rejects_wrong_message() {
        // The handler returns None for non-RoleSwap messages.
        // Full integration test in Task 10.
    }
}
```

- [ ] **Step 2: Register RoleSwapHandler in PairNode::build_registry()**

In `ferrosa-cluster/src/pair/node.rs`, add to `build_registry()`:

```rust
use crate::pair::switchover::RoleSwapHandler;

// Inside build_registry():
let role_swap_handler = Arc::new(RoleSwapHandler::new(
    self.local_host_id,
    self.role.clone(),
));
registry.register(MsgType::RoleSwap, role_swap_handler);
```

- [ ] **Step 3: Add switchover method to PairNode**

In `ferrosa-cluster/src/pair/node.rs`:

```rust
use crate::pair::switchover::initiate_switchover;

impl PairNode {
    /// Initiate a switchover: swap primary and secondary roles.
    /// Must be called on the current primary.
    pub async fn switchover(&self) -> Result<()> {
        let peer_host_id = self.state.read().await.peer_host_id;
        initiate_switchover(
            &self.peer_manager,
            self.local_host_id,
            peer_host_id,
            &self.role,
        )
        .await
    }
}
```

- [ ] **Step 4: Verify it compiles and tests pass**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cluster/src/pair/switchover.rs ferrosa-cluster/src/pair/node.rs
git commit -m "feat(cluster): add switchover protocol with RoleSwapHandler"
```

---

## Chunk 4: Integration

### Task 10: Integration tests — two-node pair mode

**Files:**

- Create: `ferrosa-cluster/tests/integration.rs`

**Context:** End-to-end tests that start two `PairNode` instances on localhost, verify:

1. Primary election (higher host_id wins)
2. Write on primary is replicated to secondary
3. Write on secondary is forwarded to primary, then replicated back
4. Switchover swaps roles
5. After switchover, write flows reverse

**Setup pattern:**

- Create two `StorageEngine` instances (separate temp dirs)
- Create two `PairNode` instances with known host_ids
- Start both (each gets an RPC server on a random port)
- Wait briefly for connections to establish

**Dependencies:**

- `ferrosa-storage::StorageEngine` + `StorageEngineConfig`
- `ferrosa-storage::commitlog::TableId`
- `ferrosa-net::config::NetConfig`
- `ferrosa-cluster::*`
- `tempfile` for temp dirs
- `tokio::test` with `test-util` for time control

- [ ] **Step 1: Write integration test scaffolding**

```rust
use std::sync::Arc;
use uuid::Uuid;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::pair::node::PairNode;
use ferrosa_cluster::pair::PairRole;
use ferrosa_net::config::NetConfig;
use ferrosa_storage::commitlog::TableId;
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};

/// Create a StorageEngine with a temp directory.
fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
    use ferrosa_storage::commitlog::config::CommitLogConfig;
    use ferrosa_storage::compaction::strategy::CompactionConfig;
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        data_dir: dir.to_path_buf(),
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

/// Create a NetConfig for testing with a random port.
fn test_net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

/// Register a test table on the storage engine so writes succeed.
fn register_test_table(storage: &StorageEngine) {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
    };
    storage.register_table(schema).unwrap();
}
```

- [ ] **Step 2: Write primary election test**

```rust
#[tokio::test]
async fn pair_elects_primary_by_host_id() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let id_high = Uuid::from_bytes([0xFF; 16]);
    let id_low = Uuid::from_bytes([0x00; 16]);

    let config = Arc::new(ClusterConfig::default());
    let net1 = Arc::new(test_net_config());
    let net2 = Arc::new(test_net_config());

    let storage1 = test_storage(dir1.path());
    let storage2 = test_storage(dir2.path());

    // Node1 has higher ID → should be primary
    let node1 = PairNode::new(
        config.clone(),
        net1,
        id_high,
        id_low,
        "127.0.0.1:7000".parse().unwrap(), // placeholder, overwritten by start()
        storage1,
    );

    let node2 = PairNode::new(
        config,
        net2,
        id_low,
        id_high,
        "127.0.0.1:7001".parse().unwrap(),
        storage2,
    );

    assert_eq!(node1.role(), PairRole::Primary);
    assert_eq!(node2.role(), PairRole::Secondary);
}
```

- [ ] **Step 3: Write write-replication integration test**

```rust
#[tokio::test]
async fn primary_write_replicates_to_secondary() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let id_primary = Uuid::from_bytes([0xFF; 16]);
    let id_secondary = Uuid::from_bytes([0x00; 16]);

    let config = Arc::new(ClusterConfig::default());
    let storage1 = test_storage(dir1.path());
    let storage2 = test_storage(dir2.path());

    register_test_table(&storage1);
    register_test_table(&storage2);

    // Start node2 (secondary) first — peer connection will be deferred
    // since node1 isn't up yet.
    let net2 = Arc::new(test_net_config());
    let node2 = PairNode::new(
        config.clone(),
        net2,
        id_secondary,
        id_primary,
        "127.0.0.1:19999".parse().unwrap(), // placeholder, node1 not started yet
        storage2.clone(),
    );
    let addr2 = node2.start().await.unwrap();

    // Start node1 (primary) pointing to node2's real address
    let net1 = Arc::new(test_net_config());
    let node1 = PairNode::new(
        config,
        net1,
        id_primary,
        id_secondary,
        addr2,
        storage1.clone(),
    );
    let addr1 = node1.start().await.unwrap();

    // Now connect node2 to node1 (deferred from start)
    node2.connect_to_peer(addr1).await.unwrap();

    // Give connections time to establish
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Write on primary via coordinator
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::Mutation;

    let mutation = Mutation {
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
        key: DecoratedKey {
            token: Token(42),
            key: PartitionKey(vec![1, 2, 3]),
        },
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }],
        timestamp: 1000,
    };

    node1.coordinator().coordinate_write(&mutation).await.unwrap();

    // Verify data exists on secondary
    let table_id = TableId {
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
    };
    let result = storage2.read(&table_id, &mutation.key).unwrap();
    assert!(result.is_some(), "mutation not replicated to secondary");
}
```

**Note:** `StorageEngine::read()` returns `Result<Option<Partition>>`. The assertion checks `is_some()` to confirm data was replicated. For deeper verification, inspect `Partition.rows`.

- [ ] **Step 4: Write secondary-forwarding test**

```rust
#[tokio::test]
async fn secondary_write_forwarded_to_primary() {
    // Same setup as primary_write_replicates_to_secondary...
    // (create dirs, storages, register tables, start nodes, connect)

    // Write on SECONDARY via coordinator — should forward to primary
    let mutation = /* same test mutation */;
    node2.coordinator().coordinate_write(&mutation).await.unwrap();

    // Verify data exists on primary (storage1)
    let table_id = TableId {
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
    };
    let result = storage1.read(&table_id, &mutation.key).unwrap();
    assert!(result.is_some(), "mutation not forwarded to primary");
}
```

- [ ] **Step 5: Write switchover test**

```rust
#[tokio::test]
async fn switchover_swaps_roles() {
    // Same setup as primary_write_replicates_to_secondary...
    // (create dirs, storages, register tables, start nodes, connect)

    // Verify initial roles
    assert_eq!(node1.role(), PairRole::Primary);
    assert_eq!(node2.role(), PairRole::Secondary);

    // Initiate switchover from primary
    node1.switchover().await.unwrap();

    // Verify roles swapped
    assert_eq!(node1.role(), PairRole::Secondary);
    assert_eq!(node2.role(), PairRole::Primary);
}
```

- [ ] **Step 6: Verify all tests pass**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass — unit tests and integration tests.

- [ ] **Step 7: Run clippy and fmt**

Run: `cargo clippy -p ferrosa-cluster --all-targets && cargo fmt -p ferrosa-cluster --check`
Expected: Clean (fix any warnings).

- [ ] **Step 8: Commit**

```bash
git add ferrosa-cluster/tests/integration.rs
git commit -m "test(cluster): add integration tests for pair mode replication and switchover"
```

---

## Post-Implementation Notes

### What this plan builds

- `ferrosa-cluster` crate with pair mode: primary/secondary roles, write coordination, replication, catch-up, switchover
- 11 source files, ~1,200 LOC estimated
- ~25-30 tests (unit + property + integration)

### What it does NOT build (deferred)

- **CQL router integration** — Router needs a `coordinate()` path to call `PairCoordinator` instead of `StorageEngine` directly. This is a separate PR touching ferrosa-cql.
- **DDL forwarding** — Schema changes in pair mode (secondary forwards DDL to primary). Requires changes to ferrosa-schema.
- **Raft consensus** — Phase 2 (cluster mode with 3+ nodes)
- **Token ring** — Phase 2
- **Hinted handoff** — Phase 3
- **TLS/mTLS** — ferrosa-net Phase 2
- **Replication lag virtual table** — Observability work

### Integration path (next PRs)

1. **ferrosa-cql router integration** — Add `coordinate()` path that delegates to `PairCoordinator`
2. **ferrosa binary startup** — Read `--seed` args, create `PairNode`, inject into `SharedState`
3. **DDL forwarding** — Route DDL through pair mode
4. **Replication lag tracking** — Virtual table for observability
