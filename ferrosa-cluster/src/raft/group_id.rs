//! `RaftGroupId` — identifies a Raft consensus group.
//!
//! In single-DC deployments a node belongs to exactly one Raft group; in
//! multi-DC deployments (ADR-015) each DC runs its own per-DC Raft group
//! for metadata, ring state, and tokens. `RaftGroupId` is the value type
//! used to address those groups in a `Map<RaftGroupId, Arc<FerrosRaft>>`.
//!
//! ## Determinism
//!
//! Per-DC group IDs are derived deterministically from the DC name via a
//! UUID v5 hash with a fixed namespace. Two nodes that read the same DC
//! name from `FERROSA_DATA_CENTER` will compute identical
//! `RaftGroupId`s, even across restarts and binary upgrades — which is
//! how peers in the same DC find each other when forming a per-DC Raft
//! quorum.
//!
//! The "default" DC group used for backward-compat with single-DC
//! deployments is exposed as [`RaftGroupId::default_dc`].

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable namespace UUID for per-DC Raft groups.
///
/// Generated once and frozen here. Do not change — every existing
/// per-DC group ID is derived from this constant; rotating it would
/// silently fork every cluster.
const FERROSA_RAFT_GROUP_NAMESPACE: Uuid =
    Uuid::from_u128(0x6e_7d_24_0c_4f_3e_4b_9a_88_a1_71_b9_2a_ca_53_61);

/// The conventional DC name used for single-DC deployments where the
/// operator did not configure a `data_center` explicitly.
pub const DEFAULT_DC_NAME: &str = "default";

/// Identifier for a Raft consensus group.
///
/// Newtype around [`Uuid`] so the type system distinguishes Raft group
/// IDs from other UUIDs (host IDs, schema versions, txn IDs, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RaftGroupId(pub Uuid);

impl RaftGroupId {
    /// Derive a deterministic group ID from a DC name.
    ///
    /// Uses UUID v5 with the `FERROSA_RAFT_GROUP_NAMESPACE` namespace
    /// so every node that reads `data_center = "<name>"` agrees on the
    /// resulting `RaftGroupId`.
    pub fn for_dc(dc_name: &str) -> Self {
        Self(Uuid::new_v5(
            &FERROSA_RAFT_GROUP_NAMESPACE,
            dc_name.as_bytes(),
        ))
    }

    /// The group ID for the implicit "default" DC used by single-DC
    /// deployments (backward-compat — see ADR-015 § "Backward-compat").
    pub fn default_dc() -> Self {
        Self::for_dc(DEFAULT_DC_NAME)
    }

    /// Borrow the underlying [`Uuid`].
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Display for RaftGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RaftGroupId({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// W6.1 RED: `RaftGroupId` is a `Uuid` newtype with equality, hash,
    /// serde working. Per-DC IDs are deterministic UUID v5 values.
    #[test]
    fn raft_group_id_is_uuid_per_dc() {
        let dc1_a = RaftGroupId::for_dc("dc1");
        let dc1_b = RaftGroupId::for_dc("dc1");
        let dc2 = RaftGroupId::for_dc("dc2");

        // Equality: same DC name → same id.
        assert_eq!(dc1_a, dc1_b, "deterministic per-DC derivation");
        // Different DC name → different id.
        assert_ne!(dc1_a, dc2);

        // Newtype around Uuid.
        let u: &Uuid = dc1_a.as_uuid();
        assert_eq!(u.get_version_num(), 5, "must be UUID v5");

        // Hash works (insert into HashSet/HashMap).
        let mut set: HashSet<RaftGroupId> = HashSet::new();
        set.insert(dc1_a);
        set.insert(dc1_b);
        set.insert(dc2);
        assert_eq!(set.len(), 2, "duplicates collapse");

        let mut map: HashMap<RaftGroupId, &str> = HashMap::new();
        map.insert(dc1_a, "dc1");
        map.insert(dc2, "dc2");
        assert_eq!(map.get(&dc1_b), Some(&"dc1"));

        // Serde round-trip (JSON) — transparent over Uuid.
        let s = serde_json::to_string(&dc1_a).expect("ser");
        let back: RaftGroupId = serde_json::from_str(&s).expect("de");
        assert_eq!(back, dc1_a);

        // Serde round-trip (bincode) — wire compat for Raft messages.
        let bytes = bincode::serialize(&dc2).expect("bincode ser");
        let back2: RaftGroupId = bincode::deserialize(&bytes).expect("bincode de");
        assert_eq!(back2, dc2);
    }

    #[test]
    fn default_dc_is_stable() {
        let a = RaftGroupId::default_dc();
        let b = RaftGroupId::for_dc("default");
        assert_eq!(a, b, "default_dc() == for_dc(\"default\")");
    }

    #[test]
    fn display_includes_uuid() {
        let id = RaftGroupId::for_dc("dc1");
        let s = format!("{id}");
        assert!(s.contains("RaftGroupId"));
        assert!(s.contains(&id.0.to_string()));
    }
}
