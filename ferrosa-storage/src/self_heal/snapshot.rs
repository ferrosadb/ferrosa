//! Plain-data observable state the controller decides over.
//!
//! A [`HealthSnapshot`] is a pure read of engine + cluster state at one tick.
//! It carries **everything** [`decide`](super::decide::decide) needs:
//! detected issues, the per-(table,issue) attempt ledger, the logical tick
//! counter, and the replica-health facts required to make the FMEA #1
//! quarantine-safety decision. Because the snapshot is plain data, `decide`
//! never touches the engine, the cluster, the clock, or RNG — it is a pure
//! function and is unit-tested as such.
//!
//! ## Replica-health port (FMEA #1)
//!
//! The controller lives in `ferrosa-storage`, which sits *below* the cluster
//! layer in the dependency graph, so it cannot call cluster APIs directly.
//! Instead, replica health is modelled as plain data on the snapshot
//! ([`RangeReplicaHealth`]) that an upstream layer (cluster wiring, a
//! follow-up) populates. A single-node engine simply reports
//! [`ReplicaPosture::SingleNode`], which the quarantine rail treats as
//! "no healthy replica → never quarantine".

use std::collections::BTreeMap;

/// Identifies a table by keyspace + name. Ordered so initiator selection and
/// issue iteration are deterministic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableKey {
    pub keyspace: String,
    pub table: String,
}

impl TableKey {
    pub fn new(keyspace: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
        }
    }
}

impl std::fmt::Display for TableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.keyspace, self.table)
    }
}

/// The kind of data issue a detector found. Used as the second half of the
/// `(table, issue)` cooldown/attempt key, and to choose an action.
///
/// Only `CorruptSstables` is implemented in this slice. `Bloat` and
/// `Divergence` are reserved as **extension points** (drain / converge
/// follow-ups) so the enum and the decision fold already account for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IssueKind {
    /// One or more SSTable generations were excluded as corrupt.
    CorruptSstables,
    /// SSTable count over the bloat threshold. *Follow-up (drain).*
    Bloat,
    /// Replica Merkle divergence. *Follow-up (converge).*
    Divergence,
}

impl IssueKind {
    /// Fixed remediation priority — lower sorts first (acted on first).
    /// Corruption (potential data loss / unavailability) outranks bloat and
    /// divergence per the design's fixed priority order.
    pub fn priority(self) -> u8 {
        match self {
            IssueKind::CorruptSstables => 0,
            IssueKind::Divergence => 1,
            IssueKind::Bloat => 2,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            IssueKind::CorruptSstables => "corrupt_sstables",
            IssueKind::Bloat => "bloat",
            IssueKind::Divergence => "divergence",
        }
    }
}

/// Whether a healthy peer replica can refill the rows in a corrupt gen.
///
/// This is the FMEA #1 gate. The controller only quarantines a corrupt gen
/// when [`ReplicaPosture::HealthyReplicaAvailable`] is reported for the
/// table's affected range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaPosture {
    /// Single-node deployment (RF effectively 1). The corrupt gen is the only
    /// copy — quarantining it is permanent data loss. Never quarantine.
    SingleNode,
    /// RF > 1, but every replica for this range is corrupt or unreachable.
    /// Same outcome as single-node: never quarantine.
    NoHealthyReplica,
    /// At least one reachable peer replica holds a healthy copy of the range
    /// and can refill the rows after quarantine. Safe to quarantine.
    HealthyReplicaAvailable,
}

impl ReplicaPosture {
    /// True only when a healthy replica can refill — the sole condition under
    /// which quarantine is permitted (FMEA #1).
    pub fn can_refill(self) -> bool {
        matches!(self, ReplicaPosture::HealthyReplicaAvailable)
    }
}

/// One corrupt SSTable generation detected for a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptSstable {
    /// Generation number (parsed from the SSTable dir/file prefix).
    pub generation: u64,
    /// Human-readable reason the smoke test excluded it (for loud logging).
    pub reason: String,
}

/// A detected data issue for one table, plus the cluster facts needed to act
/// on it safely. All plain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIssue {
    pub table: TableKey,
    pub kind: IssueKind,
    /// Corrupt generations (only populated for `CorruptSstables`).
    pub corrupt_sstables: Vec<CorruptSstable>,
    /// Whether a healthy replica can refill this table's affected range.
    pub replica_posture: ReplicaPosture,
}

impl TableIssue {
    /// The `(table, issue)` ledger key.
    pub fn ledger_key(&self) -> (TableKey, IssueKind) {
        (self.table.clone(), self.kind)
    }
}

/// Per-(table, issue) remediation bookkeeping the controller carries across
/// ticks and folds into each snapshot. `decide` reads it; it never mutates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AttemptState {
    /// How many remediation attempts have been made for this issue.
    pub attempts: u32,
    /// Logical tick of the most recent attempt (for deterministic cooldown).
    /// `None` means "never attempted".
    pub last_attempt_tick: Option<u64>,
    /// Set once the issue has been escalated (max attempts hit) so the
    /// controller stops retrying and the health surface stays degraded.
    pub escalated: bool,
}

/// Deterministic ring membership for initiator selection (FMEA #4).
///
/// For a given (table, range) the initiator is the **lowest host-id among the
/// range's owners** — no random election. `this_host` identifies the local
/// node so `decide` can answer "am I the initiator?" purely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingView {
    /// This node's host-id.
    pub this_host: u64,
    /// Owners (host-ids) per table. Sorted vec → deterministic min.
    pub owners_by_table: BTreeMap<TableKey, Vec<u64>>,
}

impl RingView {
    /// A single-node ring where this host owns everything.
    pub fn single_node(this_host: u64) -> Self {
        Self {
            this_host,
            owners_by_table: BTreeMap::new(),
        }
    }

    /// Deterministic initiator for a table: the lowest host-id among its
    /// owners. If the table has no recorded owners (e.g. single-node or
    /// not-yet-populated), the local node is the initiator.
    pub fn initiator_for(&self, table: &TableKey) -> u64 {
        self.owners_by_table
            .get(table)
            .and_then(|owners| owners.iter().copied().min())
            .unwrap_or(self.this_host)
    }

    /// True iff this node is the deterministic initiator for the table.
    pub fn is_initiator(&self, table: &TableKey) -> bool {
        self.initiator_for(table) == self.this_host
    }
}

/// The full observable snapshot `decide` operates on. Pure data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    /// Monotonic logical tick counter (NOT wall-clock). Drives cooldown
    /// deterministically so `decide` never reads the clock.
    pub tick: u64,
    /// All detected issues this tick, in a stable order.
    pub issues: Vec<TableIssue>,
    /// Per-(table, issue) attempt ledger carried across ticks.
    pub ledger: BTreeMap<(TableKey, IssueKind), AttemptState>,
    /// Ring membership for deterministic initiator selection.
    pub ring: RingView,
}

impl HealthSnapshot {
    /// Look up the attempt state for an issue (defaulting to never-attempted).
    pub fn attempt_state(&self, issue: &TableIssue) -> AttemptState {
        self.ledger
            .get(&issue.ledger_key())
            .copied()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initiator_is_lowest_host_id_among_owners() {
        let table = TableKey::new("ks", "t");
        let mut owners = BTreeMap::new();
        owners.insert(table.clone(), vec![7, 2, 5]);
        let ring = RingView {
            this_host: 2,
            owners_by_table: owners.clone(),
        };
        assert_eq!(ring.initiator_for(&table), 2);
        assert!(ring.is_initiator(&table));

        // A different local host with the same ring is NOT the initiator.
        let ring_other = RingView {
            this_host: 5,
            owners_by_table: owners,
        };
        assert_eq!(ring_other.initiator_for(&table), 2);
        assert!(!ring_other.is_initiator(&table));
    }

    #[test]
    fn single_node_initiator_is_self() {
        let ring = RingView::single_node(42);
        let table = TableKey::new("ks", "t");
        assert_eq!(ring.initiator_for(&table), 42);
        assert!(ring.is_initiator(&table));
    }

    #[test]
    fn replica_posture_refill_semantics() {
        assert!(ReplicaPosture::HealthyReplicaAvailable.can_refill());
        assert!(!ReplicaPosture::SingleNode.can_refill());
        assert!(!ReplicaPosture::NoHealthyReplica.can_refill());
    }

    #[test]
    fn issue_priority_orders_corruption_first() {
        assert!(IssueKind::CorruptSstables.priority() < IssueKind::Divergence.priority());
        assert!(IssueKind::Divergence.priority() < IssueKind::Bloat.priority());
    }
}
