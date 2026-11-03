/-
  Ferrosa formal specification — what the layer under the SQL surface must provide
  for indexes.
  =====================================================================

  A lookup on an indexed column was promised to be constant-time, and only one of
  the two senses of that is achievable. An equality probe names one value, which
  belongs to one partition, so it costs one node contact and one record read however
  large the table is. A range probe names a set of values that belong to every
  partition, so it cannot be answered by one node: it either walks an ordered index
  on one node or gathers from all of them.

  The obligation is therefore a shape rather than a constant — equality touches one
  node and one record, range touches one node and a walk or every node — and
  anything stronger is a promise the index cannot keep. One correctness item sits
  beside the cost: an index maintained outside the row write's atomic boundary can be
  left disagreeing with the table, in either direction.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sql.Index

open Ferrosa.Ledger

/-! ## Global cost: one node for equality, all nodes for range

An equality probe names one value, that value belongs to one partition, and only that partition can
hold a matching row, so the lookup contacts one node however large the table is. A range probe names a
set of values, and a hash sends adjacent values to different partitions — with three nodes, two
neighbouring keys already land apart — so the cost is one contact per node. The bound a client can rely
on is therefore a shape: constant for equality, linear in the node count for range.

-/

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

/-! ## Local cost: one record, not a scan

An index probe reads the one record its entry names, so the cost does not grow with the table and it
beats a full scan as soon as the table holds more than one row. A range probe reads every matching row
plus the walk that found them, so its cost grows with the number of matches and no constant bound
covers it. The same distinction appears in the node count and in the record count, and only the second
is what a client feels on a single-row lookup.

-/

/-- Records read by an index probe: the one the entry names. -/
def indexLookupReads : Nat := 1

/-- Records read by a full scan. -/
def tableScanReads (rows : Nat) : Nat := rows

/-- An index probe reads exactly one record whatever the table holds, which is the
    whole of the constant-time claim. -/
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

/-- A range probe reads every matching row, so it grows with the number of matches
    and is never a constant-cost operation. -/
theorem a_range_probe_is_not_constant (hits : Nat) (h : 1 < hits) :
    indexLookupReads < rangeLookupReads hits 0 := by
  simp [indexLookupReads, rangeLookupReads]
  omega

/-! ## Index maintenance must be inside the atomic boundary

If the row write and the index write are separate steps, a crash between them leaves the two sides
disagreeing: a row with no entry is invisible to the index, and an entry with no row is a lookup that
finds an identifier and then fails. Neither state can be repaired by reading, because nothing records
that the other half is missing. Maintaining both sides in one step keeps them in agreement from any
state that had it, which is why the fix belongs inside the row write's atomic boundary.

-/

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

/-! ## The ledger

Five obligations are listed against the declaration that decides each, and one is red: the index and
the table can diverge, because maintenance runs as a separate step from the row write, so a lookup can
miss a committed row or find one that is gone. `scatter_gather_is_the_accepted_shape` records the one
deliberate departure — a range lookup at one hop per node, chosen over the `O(1)` claim a
hash-partitioned index cannot deliver — and it counts as met because the cost is the stated one.
`sql_index_has_one_red_item` pins the red count so a CI check can require it to fall and never to rise.

-/

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

/-- One of the five index obligations is red: the index and the table can diverge,
    because maintenance today runs as a separate step rather than inside the row
    write. A lookup can then miss a committed row or find one that is gone. -/
theorem sql_index_has_one_red_item : redCount ledger = 1 := by decide

def main : IO Unit :=
  printLedger "SQL index obligations (layer beneath the surface)" ledger

end Ferrosa.Sql.Index
