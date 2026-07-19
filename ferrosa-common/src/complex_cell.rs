//! Module: Cassandra-exact CRDT representation of collection columns and counters.
//! Correctness: Correct when [`ComplexColumn::merge`] and [`CounterCell::merge`] are
//! commutative, associative, and idempotent (so replicas converge regardless of
//! delivery order), when per-element reconciliation follows Cassandra's last-write-wins
//! rule (higher timestamp wins; a tombstone wins an equal-timestamp tie), and when a
//! complex (collection-level) deletion shadows exactly the element cells at or below its
//! timestamp. Verified by the unit + proptest suites below.
//! Last revised: 2026-07-19
//! Last changed: New module — increment 1 of the CRDT-collections design
//!   (specs/proposed/crdt-collections-and-counters.md): the pure in-memory core, not yet
//!   wired into `Row`/memtable/SSTable.
//!
//! # Why this exists
//!
//! ferrosa stored a `list`/`set`/`map` as a single whole-collection [`CellValue`], which
//! makes an append a non-commutative read-modify-write — so an Accord transactional
//! `UPDATE v = v + [x]` had nowhere to put the delta and silently dropped it. Cassandra's
//! own model avoids this: each collection element is its own cell keyed by a **cell path**
//! (a timeuuid for `list`, the element for `set`, the key for `map`), reconciled by
//! last-write-wins. That representation is a CRDT — appends/adds/puts are commutative,
//! idempotent, read-free inserts — so it composes with Accord (whose execution timestamp
//! supplies the ordering key) *and* converges on the tunable-consistency path.
//!
//! This module is the pure, dependency-free core of that model. Later increments wire
//! [`CellPath`] into `Row`, teach the memtable/SSTable to store per-element cells, and
//! encode them onto the commit-log/Accord wire.

use std::collections::BTreeMap;

use crate::cell::{CellValue, Timestamp, NO_DELETION_TIME, NO_TIMESTAMP};

/// Identifies one element of a complex (collection) column — Cassandra's *cell path*.
///
/// The bytes are opaque here; higher layers give them meaning per collection type:
/// a 16-byte timeuuid for `list` (so path order == append order), the serialized
/// element for `set`, or the serialized key for `map`. Ordering of paths defines the
/// materialized element order, so `Ord` is derived over the raw bytes.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct CellPath(pub Vec<u8>);

impl CellPath {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        CellPath(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Reconcile two candidate cells for the *same* cell path and return the winner.
///
/// Cassandra's rule (`Cells.reconcile`): the higher timestamp wins; on an equal
/// timestamp a tombstone (deletion) beats a live cell, and two cells of the same kind
/// are broken deterministically by their full ordering. This makes `reconcile` a join
/// over a total order — commutative, associative, and idempotent — which is what lets
/// element cells converge across replicas independent of delivery order.
pub fn reconcile(a: &CellValue, b: &CellValue) -> CellValue {
    use std::cmp::Ordering;
    match a.timestamp.cmp(&b.timestamp) {
        Ordering::Greater => a.clone(),
        Ordering::Less => b.clone(),
        Ordering::Equal => match (a.is_tombstone(), b.is_tombstone()) {
            // Equal timestamp: a deletion wins the tie (Cassandra semantics).
            (true, false) => a.clone(),
            (false, true) => b.clone(),
            // Same kind: deterministic total-order tiebreak (larger value / cell wins),
            // symmetric so `reconcile(a,b) == reconcile(b,a)`.
            _ => a.max(b).clone(),
        },
    }
}

/// A complex (collection) column as a set of per-element cells reconciled by
/// last-write-wins, plus a collection-level deletion — Cassandra-exact, and a CRDT.
///
/// - `cells`: `cell path -> element cell` (live or tombstone). Ordered, so iteration is
///   the materialized element order.
/// - `complex_deletion`: the collection-level tombstone timestamp (from `SET v = null`
///   or a `SET v = [..]` overwrite). An element is shadowed iff its timestamp is `<=`
///   this value; [`NO_TIMESTAMP`] means "no collection deletion".
///
/// Element cells and the deletion are *never* pruned during [`merge`](Self::merge) (that
/// would make merge depend on the merged deletion and break associativity); shadowed
/// cells are excluded only when [`materialize`](Self::materialize)d. Garbage-collecting
/// them is a compaction concern for a later increment.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ComplexColumn {
    cells: BTreeMap<CellPath, CellValue>,
    complex_deletion: Timestamp,
}

impl Default for ComplexColumn {
    fn default() -> Self {
        Self::new()
    }
}

impl ComplexColumn {
    /// An empty collection (no elements, no collection deletion).
    pub fn new() -> Self {
        ComplexColumn {
            cells: BTreeMap::new(),
            complex_deletion: NO_TIMESTAMP,
        }
    }

    /// Add/overwrite one element (`v = v + [e]`, `v = v + {e}`, `v[k] = e`) at `timestamp`.
    /// Read-free: reconciled against any existing cell at the same path (add-wins by LWW).
    pub fn add(&mut self, path: CellPath, value: Vec<u8>, timestamp: Timestamp) {
        self.upsert(path, CellValue::live(value, timestamp));
    }

    /// Remove one element (`v = v - {e}`, `DELETE v[k]`) at `timestamp` by writing a
    /// tombstone at its path. Read-free.
    pub fn remove(&mut self, path: CellPath, timestamp: Timestamp) {
        self.upsert(path, CellValue::tombstone(timestamp, NO_DELETION_TIME));
    }

    /// Delete the whole collection (`SET v = null`) at `timestamp`: shadow every element
    /// with timestamp `<= timestamp`. For a collection *overwrite* (`SET v = [..]`) the
    /// caller deletes at `timestamp - 1` and then [`add`](Self::add)s the new elements at
    /// `timestamp`, so the new elements (strictly greater) survive — Cassandra semantics.
    pub fn delete_collection(&mut self, timestamp: Timestamp) {
        self.complex_deletion = self.complex_deletion.max(timestamp);
    }

    fn upsert(&mut self, path: CellPath, cell: CellValue) {
        match self.cells.get(&path) {
            Some(existing) => {
                let winner = reconcile(existing, &cell);
                self.cells.insert(path, winner);
            }
            None => {
                self.cells.insert(path, cell);
            }
        }
    }

    /// The collection-level deletion timestamp ([`NO_TIMESTAMP`] if none).
    pub fn complex_deletion(&self) -> Timestamp {
        self.complex_deletion
    }

    /// Whether a path currently materializes as a live element (present, and not shadowed
    /// by a tombstone or the collection deletion).
    pub fn contains(&self, path: &CellPath) -> bool {
        self.cells.get(path).is_some_and(|c| self.is_present(c))
    }

    fn is_present(&self, cell: &CellValue) -> bool {
        cell.value.is_some() && cell.timestamp > self.complex_deletion
    }

    /// The materialized elements in path order: `(cell path, element value bytes)` for
    /// every live, non-shadowed element. This is what a read assembles into a
    /// `CqlValue::List/Set/Map`.
    pub fn materialize(&self) -> Vec<(&CellPath, &[u8])> {
        self.cells
            .iter()
            .filter(|(_, cell)| self.is_present(cell))
            .map(|(path, cell)| {
                (
                    path,
                    cell.value
                        .as_deref()
                        .expect("is_present implies Some value"),
                )
            })
            .collect()
    }

    /// Number of materialized (live, non-shadowed) elements.
    pub fn len(&self) -> usize {
        self.cells.values().filter(|c| self.is_present(c)).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Merge another replica's view into this one, in place. Commutative, associative,
    /// idempotent: the max collection-deletion, and the per-path [`reconcile`] winner.
    pub fn merge_from(&mut self, other: &ComplexColumn) {
        self.complex_deletion = self.complex_deletion.max(other.complex_deletion);
        for (path, cell) in &other.cells {
            self.upsert(path.clone(), cell.clone());
        }
    }

    /// Merge two replica views into a new one (non-mutating [`merge_from`](Self::merge_from)).
    #[must_use]
    pub fn merge(&self, other: &ComplexColumn) -> ComplexColumn {
        let mut out = self.clone();
        out.merge_from(other);
        out
    }
}

// ---------------------------------------------------------------------------
// Counters — a PN-Counter (Cassandra's counter model).
// ---------------------------------------------------------------------------

/// Identifies one shard of a counter — the replica (node) that owns it. Only the owning
/// node mutates its shard, so a shard's `count` is authoritative and its `clock` orders
/// that node's own successive writes. 16 bytes = a host UUID.
pub type CounterShardId = [u8; 16];

/// One node's contribution to a counter: the running `count` it has applied, tagged with
/// a monotonically increasing `clock` so a later write from the same node wins a merge.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CounterShard {
    pub clock: i64,
    pub count: i64,
}

/// A counter as a PN-Counter: a map of per-node shards. The value is the sum of shard
/// counts; a merge keeps, per node, the shard with the higher clock. Commutative,
/// associative, idempotent.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct CounterCell {
    shards: BTreeMap<CounterShardId, CounterShard>,
}

impl CounterCell {
    pub fn new() -> Self {
        CounterCell {
            shards: BTreeMap::new(),
        }
    }

    /// Apply a delta from node `id`: bump that node's running count and advance its clock.
    /// `clock` must be strictly greater than the node's previous clock for the write to
    /// take effect (a stale/replayed write with a `<=` clock is ignored — idempotent).
    ///
    /// This is the **owning node's local accumulation** — a node mutates only its own
    /// shard, in clock order, so the shard's `count` is the authoritative running total.
    /// Other replicas receive that whole shard via [`put_shard`](Self::put_shard) /
    /// [`merge`](Self::merge); they must NOT re-accumulate another node's deltas with
    /// `increment` (that would double-count or lose updates on a partial delivery).
    pub fn increment(&mut self, id: CounterShardId, delta: i64, clock: i64) {
        let entry = self.shards.entry(id).or_insert(CounterShard {
            clock: i64::MIN,
            count: 0,
        });
        if clock > entry.clock {
            entry.clock = clock;
            entry.count += delta;
        }
    }

    /// Directly set/observe a node's shard (used when decoding a stored counter). Keeps
    /// the higher-clock shard, so this is also a merge of a single shard.
    pub fn put_shard(&mut self, id: CounterShardId, shard: CounterShard) {
        match self.shards.get(&id) {
            Some(existing) if existing.clock >= shard.clock => {}
            _ => {
                self.shards.insert(id, shard);
            }
        }
    }

    /// The counter's value: the sum of all shard counts.
    pub fn value(&self) -> i64 {
        self.shards.values().map(|s| s.count).sum()
    }

    pub fn merge_from(&mut self, other: &CounterCell) {
        for (id, shard) in &other.shards {
            self.put_shard(*id, *shard);
        }
    }

    #[must_use]
    pub fn merge(&self, other: &CounterCell) -> CounterCell {
        let mut out = self.clone();
        out.merge_from(other);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn path(b: &[u8]) -> CellPath {
        CellPath::new(b.to_vec())
    }

    // ---- ComplexColumn: unit ------------------------------------------------

    #[test]
    fn empty_column_has_no_elements() {
        let c = ComplexColumn::new();
        assert!(c.is_empty());
        assert_eq!(c.materialize(), Vec::<(&CellPath, &[u8])>::new());
    }

    #[test]
    fn add_then_materialize() {
        let mut c = ComplexColumn::new();
        c.add(path(b"p1"), b"a".to_vec(), 10);
        assert_eq!(c.len(), 1);
        assert!(c.contains(&path(b"p1")));
        assert_eq!(c.materialize(), vec![(&path(b"p1"), b"a".as_slice())]);
    }

    #[test]
    fn materialized_order_is_cell_path_order() {
        let mut c = ComplexColumn::new();
        // Insert out of path order; materialize must come back sorted by path.
        c.add(path(b"c"), b"3".to_vec(), 1);
        c.add(path(b"a"), b"1".to_vec(), 1);
        c.add(path(b"b"), b"2".to_vec(), 1);
        let vals: Vec<&[u8]> = c.materialize().into_iter().map(|(_, v)| v).collect();
        assert_eq!(vals, vec![b"1".as_slice(), b"2", b"3"]);
    }

    #[test]
    fn lww_higher_timestamp_wins_per_path() {
        let mut c = ComplexColumn::new();
        c.add(path(b"p"), b"old".to_vec(), 10);
        c.add(path(b"p"), b"new".to_vec(), 20);
        assert_eq!(c.materialize(), vec![(&path(b"p"), b"new".as_slice())]);
        // A stale write (lower ts) does not win.
        c.add(path(b"p"), b"stale".to_vec(), 5);
        assert_eq!(c.materialize(), vec![(&path(b"p"), b"new".as_slice())]);
    }

    #[test]
    fn tombstone_wins_equal_timestamp_tie() {
        let mut live_first = ComplexColumn::new();
        live_first.add(path(b"p"), b"v".to_vec(), 10);
        live_first.remove(path(b"p"), 10);
        assert!(!live_first.contains(&path(b"p")), "remove ties and wins");

        let mut tomb_first = ComplexColumn::new();
        tomb_first.remove(path(b"p"), 10);
        tomb_first.add(path(b"p"), b"v".to_vec(), 10);
        assert!(!tomb_first.contains(&path(b"p")), "order-independent tie");
    }

    #[test]
    fn add_wins_over_earlier_remove() {
        // remove@10 then add@20 -> present (add-wins by LWW).
        let mut c = ComplexColumn::new();
        c.remove(path(b"p"), 10);
        c.add(path(b"p"), b"v".to_vec(), 20);
        assert_eq!(c.materialize(), vec![(&path(b"p"), b"v".as_slice())]);
    }

    #[test]
    fn collection_deletion_shadows_earlier_elements() {
        let mut c = ComplexColumn::new();
        c.add(path(b"a"), b"1".to_vec(), 10);
        c.add(path(b"b"), b"2".to_vec(), 15);
        c.delete_collection(15); // shadows ts <= 15
        assert!(
            c.is_empty(),
            "all elements at/below the deletion are shadowed"
        );
    }

    #[test]
    fn element_after_collection_deletion_survives() {
        // The `SET v = null; v = v + [x]` case: delete@15, append@20 -> [x].
        let mut c = ComplexColumn::new();
        c.add(path(b"a"), b"old".to_vec(), 10);
        c.delete_collection(15);
        c.add(path(b"b"), b"x".to_vec(), 20);
        assert_eq!(c.materialize(), vec![(&path(b"b"), b"x".as_slice())]);
    }

    #[test]
    fn overwrite_semantics_delete_at_ts_minus_one() {
        // `SET v = [x]` at ts=20: delete_collection(19) then add(x, 20).
        let mut c = ComplexColumn::new();
        c.add(path(b"a"), b"old".to_vec(), 10);
        c.delete_collection(19);
        c.add(path(b"new"), b"x".to_vec(), 20);
        assert_eq!(c.materialize(), vec![(&path(b"new"), b"x".as_slice())]);
    }

    #[test]
    fn merge_is_commutative_associative_idempotent_small() {
        let mut a = ComplexColumn::new();
        a.add(path(b"a"), b"1".to_vec(), 10);
        a.remove(path(b"b"), 12);
        let mut b = ComplexColumn::new();
        b.add(path(b"b"), b"2".to_vec(), 11); // loses to a's remove@12
        b.add(path(b"c"), b"3".to_vec(), 9);
        let mut c = ComplexColumn::new();
        c.delete_collection(9); // shadows c's element at 9
        c.add(path(b"d"), b"4".to_vec(), 20);

        assert_eq!(a.merge(&b), b.merge(&a), "commutative");
        assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)), "associative");
        assert_eq!(a.merge(&a), a, "idempotent (self)");
        assert_eq!(a.merge(&b).merge(&b), a.merge(&b), "idempotent (repeat)");
    }

    // ---- ComplexColumn: property --------------------------------------------

    /// One user-level operation on a collection column.
    #[derive(Clone, Debug)]
    enum Op {
        Add(Vec<u8>, Vec<u8>, Timestamp), // path, value, ts
        Remove(Vec<u8>, Timestamp),
        DeleteAll(Timestamp),
    }

    fn apply(col: &mut ComplexColumn, op: &Op) {
        match op {
            Op::Add(p, v, ts) => col.add(CellPath::new(p.clone()), v.clone(), *ts),
            Op::Remove(p, ts) => col.remove(CellPath::new(p.clone()), *ts),
            Op::DeleteAll(ts) => col.delete_collection(*ts),
        }
    }

    fn build(ops: &[Op]) -> ComplexColumn {
        let mut c = ComplexColumn::new();
        for op in ops {
            apply(&mut c, op);
        }
        c
    }

    // Small domains so paths and timestamps collide and exercise reconcile / shadowing.
    fn op_strategy() -> impl Strategy<Value = Op> {
        let pth = prop::collection::vec(0u8..3, 1..2); // 1-byte path in {0,1,2}
        let val = prop::collection::vec(0u8..5, 1..2);
        let ts = 1i64..6;
        prop_oneof![
            (pth.clone(), val, ts.clone()).prop_map(|(p, v, t)| Op::Add(p, v, t)),
            (pth, ts.clone()).prop_map(|(p, t)| Op::Remove(p, t)),
            ts.prop_map(Op::DeleteAll),
        ]
    }

    proptest! {
        /// Applying the same op-set in ANY order to one column converges (add/remove
        /// reconcile is commutative; delete-all is a max), so a shuffled replay must
        /// materialize identically.
        #[test]
        fn prop_single_replica_order_independent(
            mut ops in prop::collection::vec(op_strategy(), 0..12),
            seed in any::<u64>(),
        ) {
            let in_order = build(&ops);
            // Deterministic shuffle from the seed (no RNG in the build itself).
            let mut s = seed;
            for i in (1..ops.len()).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                let j = (s >> 33) as usize % (i + 1);
                ops.swap(i, j);
            }
            let shuffled = build(&ops);
            prop_assert_eq!(in_order.materialize(), shuffled.materialize());
        }

        /// Replication convergence: splitting the ops across two replicas and merging
        /// yields the same materialized state as applying them all to one replica.
        #[test]
        fn prop_merge_matches_single_replica(
            ops in prop::collection::vec(op_strategy(), 0..12),
            split in any::<prop::sample::Index>(),
        ) {
            let all = build(&ops);
            let cut = if ops.is_empty() { 0 } else { split.index(ops.len() + 1) };
            let left = build(&ops[..cut]);
            let right = build(&ops[cut..]);
            let merged = left.merge(&right);
            prop_assert_eq!(all.materialize(), merged.materialize());
        }

        /// Merge is commutative and idempotent for arbitrary columns.
        #[test]
        fn prop_merge_commutative_idempotent(
            ops_a in prop::collection::vec(op_strategy(), 0..8),
            ops_b in prop::collection::vec(op_strategy(), 0..8),
        ) {
            let a = build(&ops_a);
            let b = build(&ops_b);
            prop_assert_eq!(a.merge(&b), b.merge(&a));
            prop_assert_eq!(a.merge(&b).merge(&b), a.merge(&b));
        }

        /// Merge is associative for arbitrary columns.
        #[test]
        fn prop_merge_associative(
            ops_a in prop::collection::vec(op_strategy(), 0..6),
            ops_b in prop::collection::vec(op_strategy(), 0..6),
            ops_c in prop::collection::vec(op_strategy(), 0..6),
        ) {
            let a = build(&ops_a);
            let b = build(&ops_b);
            let c = build(&ops_c);
            prop_assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)));
        }
    }

    // ---- CounterCell --------------------------------------------------------

    fn nid(n: u8) -> CounterShardId {
        let mut id = [0u8; 16];
        id[0] = n;
        id
    }

    #[test]
    fn counter_empty_is_zero() {
        assert_eq!(CounterCell::new().value(), 0);
    }

    #[test]
    fn counter_sums_across_nodes() {
        let mut c = CounterCell::new();
        c.increment(nid(1), 5, 1);
        c.increment(nid(2), 3, 1);
        c.increment(nid(1), 2, 2); // node 1 now at 7
        assert_eq!(c.value(), 10);
    }

    #[test]
    fn counter_ignores_stale_clock_idempotent() {
        let mut c = CounterCell::new();
        c.increment(nid(1), 5, 2);
        c.increment(nid(1), 100, 1); // stale clock -> ignored
        assert_eq!(c.value(), 5);
    }

    #[test]
    fn counter_merge_keeps_higher_clock_per_node() {
        let mut a = CounterCell::new();
        a.increment(nid(1), 5, 1);
        a.increment(nid(2), 1, 1);
        let mut b = CounterCell::new();
        b.increment(nid(1), 9, 2); // higher clock for node 1
        assert_eq!(a.merge(&b).value(), 9 + 1);
        assert_eq!(a.merge(&b), b.merge(&a), "commutative");
        assert_eq!(a.merge(&b).merge(&b), a.merge(&b), "idempotent");
    }

    proptest! {
        /// PN-Counter replication converges. Each node OWNS its shard and accumulates its
        /// own increments locally (in clock order); the whole shard is then replicated.
        /// Distributing the per-node shards across two replicas and merging must
        /// reconstruct the global value (sum of every node's accumulated count), and the
        /// merge is commutative.
        #[test]
        fn prop_counter_replication_converges(
            incs in prop::collection::vec((0u8..4, -10i64..10), 0..20),
        ) {
            // Owning-node accumulation: each node builds its authoritative shard.
            let mut clocks = std::collections::BTreeMap::<u8, i64>::new();
            let mut owning = std::collections::BTreeMap::<u8, CounterCell>::new();
            for (node, delta) in &incs {
                let clk = clocks.entry(*node).or_insert(0);
                *clk += 1;
                owning
                    .entry(*node)
                    .or_default()
                    .increment(nid(*node), *delta, *clk);
            }
            let expected: i64 = owning.values().map(|c| c.value()).sum();

            // Replication: each node's FULL shard is delivered to one of two replicas.
            let mut left = CounterCell::new();
            let mut right = CounterCell::new();
            for (i, cell) in owning.values().enumerate() {
                if i % 2 == 0 { left.merge_from(cell); } else { right.merge_from(cell); }
            }
            let merged = left.merge(&right);
            prop_assert_eq!(merged.value(), expected);
            prop_assert_eq!(left.merge(&right), right.merge(&left));
        }
    }
}
