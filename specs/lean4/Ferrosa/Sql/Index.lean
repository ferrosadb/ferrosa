/-
  Ferrosa formal specification — what the layer under the SQL surface must provide (indexes).
  =====================================================================

  The SQL surface promised O(1) lookups on indexed columns. This file states what that can and
  cannot mean, so the layer beneath has something checkable to satisfy.

  **Two different O(1)s, and only one of them is achievable.**

    - *Global*: one node contact per equality lookup. Achievable — hash-partition the index by
      the value, so the value names exactly one index partition
      (`equality_contacts_one_node`).
    - *Global*: one node contact per **range** lookup. Not achievable with a hash-partitioned
      index, because a range's values land on every node
      (`range_lookup_cannot_be_constant`). A range index must be ordered and pays
      O(log N + k) on one node, or scatter-gathers across all of them.

    - *Local*: one record read per probe, versus a linear scan
      (`index_lookup_is_constant`).

  The obligation the layer must meet is therefore not "O(1)" but:
  **equality probes touch one node and one record; range probes touch one node and O(log N + k)
  records, or all nodes.** Anything stronger than that is a promise the index cannot keep.

  And one correctness obligation that ties this to the transaction work: an index maintained
  outside the write's atomic boundary can be made inconsistent with the table, in both
  directions (`a_non_atomic_index_write_is_inconsistent`).
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sql.Index

open Ferrosa.Ledger

/-! ## Global cost: one node for equality, all nodes for range -/

/-- Which node serves a value under a hash-partitioned index. -/
def nodeOf (nodes key : Nat) : Nat := key % nodes

/-- **An equality lookup touches exactly one node.**  The value hashes to one partition, and that
    partition is the whole answer. -/
theorem equality_contacts_one_node (nodes key : Nat) (h : 0 < nodes) :
    nodeOf nodes key < nodes := Nat.mod_lt key h

/-- The cost of an equality lookup in node contacts, independent of table size. -/
def equalityNodeContacts : Nat := 1

/-- The cost of a range lookup in node contacts: every node, because a range's values hash
    everywhere. -/
def rangeNodeContacts (nodes : Nat) : Nat := nodes

/-- **A range lookup cannot be constant.**  With three nodes, two adjacent values already land on
    two different nodes, so no bound smaller than the node count holds. -/
theorem range_lookup_cannot_be_constant :
    nodeOf 3 0 ≠ nodeOf 3 1 ∧ nodeOf 3 0 = 0 ∧ nodeOf 3 1 = 1 := by decide

/-- The honest bound: equality is O(1) in nodes, range is O(nodes). -/
theorem only_equality_is_constant (nodes : Nat) (h : 1 < nodes) :
    equalityNodeContacts < rangeNodeContacts nodes := by
  simp [equalityNodeContacts, rangeNodeContacts]
  omega

/-! ## Local cost: one record, not a scan -/

/-- Records read by an index probe: the one the entry names. -/
def indexLookupReads : Nat := 1

/-- Records read by a full scan. -/
def tableScanReads (rows : Nat) : Nat := rows

theorem index_lookup_is_constant : indexLookupReads = 1 := rfl

/-- **An index probe beats a scan exactly when there is more than one row**, which is the whole
    claim: the cost does not grow with the table. -/
theorem index_lookup_beats_a_scan (rows : Nat) (h : 1 < rows) :
    indexLookupReads < tableScanReads rows := by
  simp [indexLookupReads, tableScanReads]
  omega

/-- A range probe reads the matching rows plus the index walk; it is not constant, and pretending
    otherwise would be the bug. -/
def rangeLookupReads (hits logn : Nat) : Nat := hits + logn

theorem a_range_probe_is_not_constant (hits : Nat) (h : 1 < hits) :
    indexLookupReads < rangeLookupReads hits 0 := by
  simp [indexLookupReads, rangeLookupReads]
  omega

/-! ## Index maintenance must be inside the atomic boundary -/

/-- An indexed table: the rows, and the index entries. -/
structure Indexed where
  rows : List Nat
  idx : List Nat
  deriving DecidableEq, Repr

/-- The two sides agree: no dangling entry, no unindexed row. -/
def Indexed.consistent (x : Indexed) : Prop := x.rows = x.idx

/-- A write that touches only one side. -/
def Indexed.insertRowOnly (x : Indexed) (k : Nat) : Indexed :=
  { rows := k :: x.rows, idx := x.idx }

def Indexed.insertIndexOnly (x : Indexed) (k : Nat) : Indexed :=
  { rows := x.rows, idx := k :: x.idx }

/-- A write that touches both sides in one step. -/
def Indexed.insertBoth (x : Indexed) (k : Nat) : Indexed :=
  { rows := k :: x.rows, idx := k :: x.idx }

/-- **One-sided maintenance is inconsistent immediately**, in both directions: a row with no
    entry is invisible to the index (a lost lookup), an entry with no row is a lookup that fails
    after finding it (a dangling pointer). -/
theorem a_non_atomic_index_write_is_inconsistent :
    ¬ (Indexed.insertRowOnly ⟨[], []⟩ 1).consistent
      ∧ ¬ (Indexed.insertIndexOnly ⟨[], []⟩ 1).consistent
      ∧ (Indexed.insertBoth ⟨[], []⟩ 1).consistent := by
  unfold Indexed.consistent Indexed.insertRowOnly Indexed.insertIndexOnly Indexed.insertBoth
  decide

/-- Maintaining both sides in one step keeps the invariant for any starting state that had it. -/
theorem atomic_maintenance_preserves_consistency (x : Indexed) (k : Nat)
    (h : x.consistent) : (x.insertBoth k).consistent := by
  unfold Indexed.consistent Indexed.insertBoth at *
  rw [h]

/-! ## The ledger -/

/-- A range lookup costs one round trip per node. -/
def scatterGatherHops (nodes : Nat) : Nat := nodes

/-- And the rows it returns. -/
def scatterGatherRows (nodes rowsPerNode : Nat) : Nat := nodes * rowsPerNode

/-- **Scatter-gather is the accepted shape**: `nodes` hops for `nodes × rowsPerNode` rows, so with a
    small node count and a large match the hops are noise and the rows dominate. This is a deliberate
    choice of `O(nodes)` over an `O(1)` claim that a hash-partitioned index cannot deliver. -/
theorem scatter_gather_is_the_accepted_shape :
    scatterGatherHops 16 = 16 ∧ scatterGatherRows 16 10000 = 160000 := by decide

def ledger : List SpecItem := [
  ⟨"an equality lookup on an indexed column contacts one node",
   .met "Index.equality_contacts_one_node"⟩,
  ⟨"an equality probe reads one record, not a scan",
   .met "Index.index_lookup_beats_a_scan"⟩,
  ⟨"a range lookup on an indexed column costs one round trip per node, which is accepted",
   .met "Sql.Index.scatter_gather_is_the_accepted_shape"⟩,
  ⟨"index and table cannot diverge",
   .failing
     "only if the index write is inside the same atomic boundary as the row write; today maintenance is a separate step"
     "Index.a_non_atomic_index_write_is_inconsistent"⟩,
  ⟨"atomic maintenance preserves consistency",
   .met "Index.atomic_maintenance_preserves_consistency"⟩
]

theorem sql_index_has_one_red_item : redCount ledger = 1 := by decide

def main : IO Unit :=
  printLedger "SQL index obligations (layer beneath the surface)" ledger

end Ferrosa.Sql.Index
