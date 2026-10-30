/-
  Unimplemented: not there yet.
  =============================

  The ledgers say a property is not proved. That is one word for several situations, and only one
  of them is a red (see `Ferrosa/Broken.lean`). This file holds the rest:

    unimplemented   no code, no format field, no plan node. Build it.
    requirement     no implementation can meet it as written. Narrow the requirement.
    proof           the behaviour may well be right; the proof is missing.
    measurement     the claim needs evidence, not a proof. Build the harness.
    inherited       a third-party protocol decides it. Document, do not fix.
    undecided       blocked on a design choice nobody has made yet.
    closed          was listed as red, has since been implemented.

  ## How the accounting is checked

  A row does not describe the ledger item it classifies in prose. It *names* it: `ledger` is the
  module key of the area ledger and `item` is the exact `SpecItem.name` in that ledger. The area
  ledgers themselves are enumerated exactly once, in `areas` below; that one list is what the
  runner prints and what `ledgerTotal` sums, so a ledger cannot be printed without being counted or
  counted without being printed. Against that:

    every_row_names_a_red_item            every row resolves to an item that exists and is red;
    every_red_is_classified_exactly_once  every red item in every registered ledger is named by
                                          exactly one row — no red unclassified, none double-counted;
    per_area_counts                       each area's row count is that area's red count;
    rows_are_the_ledger_reds              the totals agree.

  Rows that were red when they were written and are discharged now are not deleted: they live in
  `discharged`, and `discharged_rows_name_met_items` proves each one still names a real ledger item
  whose status is `.met`.
-/
import Ferrosa.Ledger
import Ferrosa.Commons.Hll
import Ferrosa.Testing.Jepsen
import Ferrosa.Testing.JepsenModels
import Ferrosa.Testing.Histories
import Ferrosa.Testing.Guarantees
import Ferrosa.Unsafe.Invariants
import Ferrosa.Cdc.Subscribe
import Ferrosa.Flight.Subscribe
import Ferrosa.Broken
import Ferrosa.Cql.AggregateObligations
import Ferrosa.Cql.Subscribe
import Ferrosa.Storage.Recovery
import Ferrosa.Storage.Tombstone
import Ferrosa.Storage.Format
import Ferrosa.Sql.Semantics
import Ferrosa.Bolt.Session
import Ferrosa.Sparql.Semantics
import Ferrosa.Sql.Index
import Ferrosa.Sql.Txn
import Ferrosa.Storage.Mvcc
import Ferrosa.Storage.Streaming
import Ferrosa.Sql.Consistency

namespace Ferrosa.Unimplemented

open Ferrosa.Ledger

set_option maxRecDepth 8000

/-! ## The registry: every area ledger, named once -/

/-- One area ledger: its module key, the ledger itself, and how to print it. -/
structure Area where
  /-- Module key, matching the file path under `Ferrosa/` (`Ferrosa.Storage.Mvcc`). -/
  key : String
  ledger : List SpecItem
  print : IO Unit

/-- `Ferrosa/Cql/AggregateObligations.lean` predates the shared ledger types and carries its own
    copy of `Status`/`SpecItem`; this maps its ledger into the shared type so it is counted with
    everything else rather than summed as a literal. -/
def cqlAggregateLedger : List SpecItem :=
  Ferrosa.Cql.AggregateObligations.ledger.map fun i =>
    match i.status with
    | .met p => ⟨i.name, .met p⟩
    | .failing why wit => ⟨i.name, .failing why wit⟩

/-- The bridge does not change the count: the six the module pins for itself are the six counted
    here. -/
theorem cql_aggregate_bridge_preserves_reds :
    redCount cqlAggregateLedger = Ferrosa.Cql.AggregateObligations.redCount := by decide

/-- Every area ledger in the corpus. This is the only enumeration of them: `printAllLedgers` walks
    it and `ledgerTotal` sums it, so the printed set and the counted set cannot drift apart.
    `Ferrosa/Ledgers.lean` re-scans the source tree and fails the run if a file defining
    `def ledger : List SpecItem` is missing from this list. -/
def areas : List Area := [
  ⟨"Ferrosa.Cql.AggregateObligations", cqlAggregateLedger, Ferrosa.Cql.AggregateObligations.main⟩,
  ⟨"Ferrosa.Storage.Recovery", Ferrosa.Storage.Recovery.ledger, Ferrosa.Storage.Recovery.main⟩,
  ⟨"Ferrosa.Storage.Tombstone", Ferrosa.Storage.Tombstone.ledger, Ferrosa.Storage.Tombstone.main⟩,
  ⟨"Ferrosa.Storage.Format", Ferrosa.Storage.Format.ledger, Ferrosa.Storage.Format.main⟩,
  ⟨"Ferrosa.Sql.Semantics", Ferrosa.Sql.ledger, Ferrosa.Sql.main⟩,
  ⟨"Ferrosa.Bolt.Session", Ferrosa.Bolt.ledger, Ferrosa.Bolt.main⟩,
  ⟨"Ferrosa.Sparql.Semantics", Ferrosa.Sparql.ledger, Ferrosa.Sparql.main⟩,
  ⟨"Ferrosa.Sql.Index", Ferrosa.Sql.Index.ledger, Ferrosa.Sql.Index.main⟩,
  ⟨"Ferrosa.Sql.Txn", Ferrosa.Sql.Txn.ledger, Ferrosa.Sql.Txn.main⟩,
  ⟨"Ferrosa.Storage.Mvcc", Ferrosa.Storage.Mvcc.ledger, Ferrosa.Storage.Mvcc.main⟩,
  ⟨"Ferrosa.Storage.Streaming", Ferrosa.Storage.Streaming.ledger, Ferrosa.Storage.Streaming.main⟩,
  ⟨"Ferrosa.Sql.Consistency", Ferrosa.Sql.Consistency.ledger, Ferrosa.Sql.Consistency.main⟩,
  ⟨"Ferrosa.Cdc.Subscribe", Ferrosa.Cdc.ledger, Ferrosa.Cdc.main⟩,
  ⟨"Ferrosa.Cql.Subscribe", Ferrosa.Cql.Subscribe.ledger, Ferrosa.Cql.Subscribe.main⟩,
  ⟨"Ferrosa.Flight.Subscribe", Ferrosa.Flight.ledger, Ferrosa.Flight.main⟩,
  ⟨"Ferrosa.Commons.Hll", Ferrosa.Commons.Hll.ledger, Ferrosa.Commons.Hll.main⟩,
  ⟨"Ferrosa.Testing.Jepsen", Ferrosa.Testing.Jepsen.ledger, Ferrosa.Testing.Jepsen.main⟩,
  ⟨"Ferrosa.Testing.JepsenModels", Ferrosa.Testing.Models.ledger, Ferrosa.Testing.Models.main⟩,
  ⟨"Ferrosa.Testing.Histories", Ferrosa.Testing.Histories.ledger, Ferrosa.Testing.Histories.main⟩,
  ⟨"Ferrosa.Testing.Guarantees", Ferrosa.Testing.Guarantees.ledger, Ferrosa.Testing.Guarantees.main⟩,
  ⟨"Ferrosa.Unsafe.Invariants", Ferrosa.Unsafe.ledger, Ferrosa.Unsafe.main⟩
]

/-- Look up an area ledger by module key. -/
def findLedger : List Area → String → Option (List SpecItem)
  | [], _ => none
  | a :: rest, k => if a.key = k then some a.ledger else findLedger rest k

def ledgerOf (k : String) : Option (List SpecItem) := findLedger areas k

/-- A `find?` result is a list element: the registry cannot answer with a ledger it does not hold. -/
theorem findLedger_mem : ∀ (as : List Area) (k : String) (l : List SpecItem),
    findLedger as k = some l → ∃ a ∈ as, a.key = k ∧ a.ledger = l := by
  intro as
  induction as with
  | nil =>
    intro k l h
    simp [findLedger] at h
  | cons a rest ih =>
    intro k l h
    by_cases hk : a.key = k
    · simp [findLedger, hk, Option.some.injEq] at h
      exact ⟨a, List.mem_cons_self .., hk, h⟩
    · simp [findLedger, hk] at h
      obtain ⟨b, hb, hbk, hbl⟩ := ih k l h
      exact ⟨b, List.mem_cons_of_mem a hb, hbk, hbl⟩

/-- Registering one ledger twice would double-count it, so the registry is a set of keys. -/
theorem area_keys_are_distinct : (areas.map (·.key)).Nodup := by decide

/-- Pinned so that dropping a ledger with no reds — which the total cannot see — is still a build
    failure rather than a quietly smaller registry. -/
theorem registered_ledger_count : areas.length = 21 := by decide

/-! ## The total, derived -/

/-- The number of red items across the corpus: the sum of the registered ledgers' `redCount`.
    There is no literal term here. -/
def ledgerTotal : Nat := (areas.map fun a => redCount a.ledger).sum

theorem ledgerTotal_is_the_registry_sum :
    ledgerTotal = (areas.map fun a => redCount a.ledger).sum := rfl

/-- Pinned so that removing a ledger from the registry — the drift that has recurred here — is a
    build failure rather than a silently smaller total. -/
theorem ledgerTotal_pinned : ledgerTotal = 59 := by decide

/-- Print every registered ledger, with the header its area module prints. -/
def printAllLedgers : IO Unit := do
  for a in areas do
    a.print
    IO.println ""

/-! ## The classification rows -/

/-- Absent: no code path, no format field, no plan node. -/
structure Item where
  area : String
  what : String
  /-- Module key of the area ledger this row classifies. -/
  ledger : String
  /-- The exact `SpecItem.name` in that ledger. -/
  item : String

def unimplemented : List Item := [
  ⟨"CQL aggregate", "COUNT(DISTINCT) merges across shards",
   "Ferrosa.Cql.AggregateObligations", "COUNT(DISTINCT) merges across shards"⟩,
  ⟨"CQL aggregate", "COUNT(ESTIMATE_DISTINCT) has a relative error bound",
   "Ferrosa.Cql.AggregateObligations", "COUNT(ESTIMATE_DISTINCT) has a relative error bound"⟩,
  ⟨"CQL aggregate", "COUNT(ESTIMATE_DISTINCT) merges across shards",
   "Ferrosa.Cql.AggregateObligations", "COUNT(ESTIMATE_DISTINCT) merges across shards"⟩,
  ⟨"CQL aggregate", "the CQL surface accepts COUNT(DISTINCT c)",
   "Ferrosa.Cql.AggregateObligations", "CQL surface accepts COUNT(DISTINCT c)"⟩,
  ⟨"CQL aggregate", "the CQL surface accepts COUNT(ESTIMATE_DISTINCT c)",
   "Ferrosa.Cql.AggregateObligations", "CQL surface accepts COUNT(ESTIMATE_DISTINCT c)"⟩,
  ⟨"Recovery", "a corrupted payload is detected",
   "Ferrosa.Storage.Recovery", "a corrupted payload is detected"⟩,
  ⟨"Recovery", "corruption is quarantined, never deleted",
   "Ferrosa.Storage.Recovery", "corruption is quarantined, never deleted"⟩,
  ⟨"Tombstone", "gc_grace is enforced against that precondition",
   "Ferrosa.Storage.Tombstone", "the purge path checks the grace window and propagation before removing a tombstone"⟩,
  ⟨"fnf", "a flush can resume at the last verified record",
   "Ferrosa.Storage.Format", "a flush can resume at the last verified record"⟩,
  ⟨"fnf", "a table can be exported to BTI for Cassandra by compaction",
   "Ferrosa.Storage.Format", "a table can be exported to BTI for Cassandra by compaction"⟩,
  ⟨"SQL", "a multi-statement transaction is atomic",
   "Ferrosa.Sql.Semantics", "a multi-statement transaction is atomic"⟩,
  ⟨"Bolt", "queued result streams are bounded",
   "Ferrosa.Bolt.Session", "queued result streams are bounded"⟩,
  ⟨"Txn", "SERIALIZABLE isolation is available",
   "Ferrosa.Sql.Txn", "SERIALIZABLE isolation is available"⟩,
  ⟨"distinct", "the relative error of the HLL estimate is bounded by 1.04/sqrt(m)",
   "Ferrosa.Commons.Hll", "the relative error is bounded by 1.04/sqrt(m)"⟩,
  ⟨"distinct", "a HyperLogLog estimator backs COUNT(ESTIMATE_DISTINCT)",
   "Ferrosa.Commons.Hll", "a HyperLogLog estimator backs COUNT(ESTIMATE_DISTINCT)"⟩,
  ⟨"Jepsen", "every run's verdict preserves lost, unexpected and recovered as multisets",
   "Ferrosa.Testing.Histories", "every run records lost, unexpected and recovered as multisets, not as a single error count"⟩,
  ⟨"Unsafe", "every unsafe site is discharged by a Kani harness rather than by review",
   "Ferrosa.Unsafe.Invariants", "every `unsafe` site in `docs/unsafe-ledger.md` is discharged by a Kani harness, not by review"⟩,
  ⟨"Unsafe", "the truncation contract is enforced in the safe rename and GC paths",
   "Ferrosa.Unsafe.Invariants", "the truncation contract is enforced in the safe rename/GC paths, where it can be proved"⟩,
  ⟨"Unsafe", "there is a machine-checked route to proving unsafe code, rather than a documented one",
   "Ferrosa.Unsafe.Invariants", "the Aeneas/separation-logic route to proving `unsafe` is not available yet"⟩,
  ⟨"Jepsen", "the set model is ported, including its multi-value reads",
   "Ferrosa.Testing.JepsenModels", "the set model is ported, including its multi-value reads"⟩,
  ⟨"Jepsen", "the set-full and unique-ids models are ported",
   "Ferrosa.Testing.JepsenModels", "the set-full and unique-ids models are ported"⟩,
  ⟨"Jepsen", "the generators, nemeses and stats assertions are ported",
   "Ferrosa.Testing.JepsenModels", "Jepsen's generators, nemeses and stats assertions are ported"⟩,
  ⟨"Jepsen", "the suite asserts strict serializability for multi-key transactions",
   "Ferrosa.Testing.Jepsen", "the Jepsen suite asserts strict serializability for multi-key transactions"⟩,
  ⟨"Jepsen", "the checker runs Elle with read-only transactions in the graph",
   "Ferrosa.Testing.Jepsen", "the checker runs Elle with read-only transactions in the graph"⟩,
  ⟨"Jepsen", "a multi-DC run with local reads treats a semantics consequence as a failure",
   "Ferrosa.Testing.Jepsen", "a multi-DC run under LOCAL_QUORUM or DC_QUORUM must expect strict-serializability violations after a failover"⟩,
  ⟨"MVCC", "versions are stored in the fnf record",
   "Ferrosa.Storage.Mvcc", "versions are stored in the fnf record so the probe stays constant"⟩,
  ⟨"MVCC", "compaction carries the spill pointer forward",
   "Ferrosa.Storage.Mvcc", "compaction carries the spill pointer forward"⟩,
  ⟨"Streaming", "buffer memory is one shared pool sized by a shared_memory setting",
   "Ferrosa.Storage.Streaming", "buffer memory is one shared pool sized by a shared_memory setting"⟩,
  ⟨"Consistency", "the write path captures a transaction's read set and validates it before committing",
   "Ferrosa.Sql.Consistency", "the write path captures a transaction's read set and validates it before committing"⟩,
  ⟨"MVCC", "the read path records a predicate lock for every row it reads",
   "Ferrosa.Storage.Mvcc", "the write path records a predicate lock for every read it takes"⟩,
  ⟨"SUBSCRIBE", "the gap signal carries the position to replay from",
   "Ferrosa.Cdc.Subscribe", "the wire gap signal carries the position to replay from"⟩,
  ⟨"SUBSCRIBE", "a cancelled subscription reports its final cursor",
   "Ferrosa.Cdc.Subscribe", "a cancelled subscription reports its final cursor"⟩,
  ⟨"SUBSCRIBE", "the Flight surface can plan and serve a stream ticket",
   "Ferrosa.Flight.Subscribe", "plan.rs can produce a stream ticket"⟩,
  ⟨"SUBSCRIBE", "each consumer's unacknowledged messages are queued on disk and truncated by acknowledgement",
   "Ferrosa.Cdc.Subscribe", "a slow consumer's overflow spills to disk instead of being dropped"⟩,
  ⟨"SUBSCRIBE", "a proportional controller drives the fair share",
   "Ferrosa.Cdc.Subscribe", "the fair share is driven by a proportional controller"⟩,
  ⟨"MVCC", "an asynchronous vacuum decrements reference counts after each commit",
   "Ferrosa.Storage.Mvcc", "vacuum runs asynchronously after the committing transaction completes, off the write path"⟩,
  ⟨"Tombstone", "the purge path cannot resurrect a deleted row",
   "Ferrosa.Storage.Tombstone", "deleted data cannot be resurrected"⟩
]

/-- Everything the ledgers list that is neither broken nor absent. -/
structure Other where
  area : String
  what : String
  why : String
  /-- Module key of the area ledger this row classifies. -/
  ledger : String
  /-- The exact `SpecItem.name` in that ledger. -/
  item : String

def nonDefects : List Other := [
  ⟨"Tombstone", "the merge is order-independent",
   "proof: only the colliding cases are checked, not the general statement",
   "Ferrosa.Storage.Tombstone", "the merge is order-independent"⟩,
  ⟨"fnf", "record CRCs cost less than 5% of point-read p99",
   "measurement: needs the harness, not a theorem",
   "Ferrosa.Storage.Format", "record CRCs cost less than 5% of point-read p99"⟩,
  ⟨"Bolt", "replies can be paired to messages by index",
   "inherited: Bolt pairs them by arrival, and a FAILURE carries no statement identity",
   "Ferrosa.Bolt.Session", "replies can be paired to messages by index"⟩,
  ⟨"Bolt", "a failure is attributable to a statement",
   "inherited: the protocol has nowhere to put the identity",
   "Ferrosa.Bolt.Session", "a failure is attributable to a statement"⟩,
  ⟨"SPARQL", "OPTIONAL scoping is specified",
   "undecided: no representation in the plan types yet",
   "Ferrosa.Sparql.Semantics", "OPTIONAL scoping is specified"⟩,
  ⟨"CQL aggregate", "COUNT(DISTINCT) is O(1) in memory",
   "closed: count.rs implements DistinctExact with a disk-backed slot table, 19/19 tests",
   "Ferrosa.Cql.AggregateObligations", "COUNT(DISTINCT) is O(1) in memory"⟩,
  ⟨"MVCC", "a snapshot lookup stops at the first file holding the key",
   "requirement: refuted — the newest file's version may be invisible to the snapshot, so every file holding a version must be consulted; one probe per SSTable in the view is the real bound, and that is what is proved",
   "Ferrosa.Storage.Mvcc", "a snapshot lookup can stop at the first file holding the key"⟩
]

/-! ## Rows that are discharged now

Two rows below classify items that have since been proved: the ledger absorbed the claim. They are
kept, not deleted — deleting a row is how the count stayed balanced while the bookkeeping went
wrong — and each one is required to name a real ledger item whose status is `.met`. They are not
part of the classification: `nothing_discharged_is_still_classified` keeps the two lists apart.
-/

/-- A row that was red when it was written and is discharged now. -/
structure Discharged where
  area : String
  what : String
  why : String
  /-- Module key of the area ledger that now proves it. -/
  ledger : String
  /-- The exact `SpecItem.name` of the discharging entry. -/
  item : String

def discharged : List Discharged := [
  ⟨"Recovery", "an acknowledged write survives a crash",
   "discharged: the ledger states the narrower claim — every mode except the operator-opted-in periodic one — and proves it with `Recovery.the_ack_guarantee_is_a_configuration_not_a_law`, which is this row's requirement",
   "Ferrosa.Storage.Recovery", "an acknowledged write survives a crash under every mode except periodic, which the operator opts into"⟩,
  ⟨"Index", "a range lookup on an indexed column is constant in nodes",
   "discharged: the claim was narrowed to one round trip per node, which is what a BTree costs, and `Sql.Index.scatter_gather_is_the_accepted_shape` proves that narrower claim; the O(1) version was never implementable",
   "Ferrosa.Sql.Index", "a range lookup on an indexed column costs one round trip per node, which is accepted"⟩
]

/-! ## The checks -/

/-- The (ledger, item) pairs the classification claims to cover. -/
def rows : List (String × String) :=
  (Ferrosa.Broken.broken.map fun i => (i.ledger, i.item))
    ++ (unimplemented.map fun i => (i.ledger, i.item))
    ++ (nonDefects.map fun o => (o.ledger, o.item))

/-- `r` names an item of ledger `r.1` that satisfies `p`. -/
def resolves (p : SpecItem → Bool) (r : String × String) : Bool :=
  match ledgerOf r.1 with
  | none => false
  | some l => (l.filter p).any fun i => i.name == r.2

/-- A `resolves` hit is a real row of a real registered ledger — the witness form of the same
    fact, so a failure names the item instead of a boolean. -/
theorem resolves_spec (p : SpecItem → Bool) (r : String × String) (h : resolves p r = true) :
    ∃ a ∈ areas, a.key = r.1 ∧ ∃ i ∈ a.ledger, p i = true ∧ i.name = r.2 := by
  unfold resolves at h
  split at h
  · simp at h
  · rename_i l hl
    obtain ⟨a, ha, hk, hlk⟩ := findLedger_mem areas r.1 l hl
    rw [List.any_eq_true] at h
    obtain ⟨i, hi, hbeq⟩ := h
    obtain ⟨hi, hp⟩ := List.mem_filter.mp hi
    exact ⟨a, ha, hk, i, by rw [hlk]; exact hi, hp, beq_iff_eq.mp hbeq⟩

/-- **(a)** Every classification row names a ledger item, and that item is red. A row that names
    a discharged item, a renamed one, or a ledger that is not registered fails here. -/
theorem every_classified_row_is_red : rows.all (resolves isRed) = true := by decide

/-- **(a)**, in witness form. -/
theorem every_row_names_a_red_item (r : String × String) (h : r ∈ rows) :
    ∃ a ∈ areas, a.key = r.1 ∧ ∃ i ∈ a.ledger, isRed i = true ∧ i.name = r.2 :=
  resolves_spec isRed r (List.all_eq_true.mp every_classified_row_is_red r h)

/-- **(b)** Every red item of every registered ledger is named by exactly one row: no red is
    unclassified, and none is classified twice. Structural over both lists — the registry is
    discharged by case split, and within an area the obligation is a fold over that ledger's own
    red items, each named by its `SpecItem.name`. Adding, removing or renaming a red item changes
    the obligation at a named position instead of shrinking an opaque count. -/
theorem every_red_is_classified_exactly_once (a : Area) (ha : a ∈ areas) :
    (a.ledger.filter isRed).all (fun i => rows.count (a.key, i.name) == 1) = true := by
  obtain ⟨k, l, p⟩ := a
  simp only [areas, List.mem_cons, List.not_mem_nil, or_false] at ha
  rcases ha with ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩
  all_goals decide

/-- **(c)** Each area's rows number exactly that area's reds. -/
theorem per_area_counts (a : Area) (ha : a ∈ areas) :
    (rows.filter fun r => r.1 == a.key).length = redCount a.ledger := by
  obtain ⟨k, l, p⟩ := a
  simp only [areas, List.mem_cons, List.not_mem_nil, or_false] at ha
  rcases ha with ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩ | ⟨rfl, rfl, rfl⟩
  all_goals decide

/-- The pairs the discharged rows name: not part of the classification, but still checked. -/
def dischargedRows : List (String × String) := discharged.map fun d => (d.ledger, d.item)

/-- A discharged row still names a real ledger item, so a row cannot rot into a dangling string. -/
theorem every_discharged_row_names_an_item : dischargedRows.all (resolves isMet) = true := by decide

/-- …and the item it names is `.met`: a row that names a *red* item is in the wrong list, and one
    that names nothing at all fails above. -/
theorem discharged_rows_name_met_items (r : String × String) (h : r ∈ dischargedRows) :
    ∃ a ∈ areas, a.key = r.1 ∧ ∃ i ∈ a.ledger, isMet i = true ∧ i.name = r.2 :=
  resolves_spec isMet r (List.all_eq_true.mp every_discharged_row_names_an_item r h)

/-- A discharged row is not also a classification row, so the partition cannot hide a red here. -/
theorem nothing_discharged_is_still_classified :
    (dischargedRows.filter fun r => rows.contains r).length = 0 := by decide

/-- The totals agree, which is now a consequence of (a), (b) and (c) rather than a coincidence. -/
theorem rows_are_the_ledger_reds : rows.length = ledgerTotal := by decide

/-! ## The pinned counts -/

/-- **Every red item in every ledger appears here or in `Broken.broken`, exactly once.** -/
theorem every_ledger_item_is_classified :
    Ferrosa.Broken.broken.length + unimplemented.length + nonDefects.length = ledgerTotal := by
  native_decide

theorem unimplemented_has_the_right_length : unimplemented.length = 37 := by native_decide

/-- Seven, not eight: two rows were discharged into the ledger itself (see `discharged`), and one
    new row records the refuted constant-probe claim in the MVCC ledger. -/
theorem twelve_are_not_defects : nonDefects.length = 7 := by native_decide

/-- **Live defects now outnumber everything else.**  Thirteen reds against twelve requirements,
    proofs, measurements, inherited protocol behaviour and closures. This theorem had the opposite
    direction until the memory specs found three more: the closer the specs get to the code, the
    more of it turns out to be shipping something that is not true. -/
theorem live_defects_outnumber_the_rest : nonDefects.length < Ferrosa.Broken.broken.length := by
  native_decide

def main : IO Unit := do
  IO.println s!"unimplemented — no code path yet: {unimplemented.length} items"
  for i in unimplemented do
    IO.println s!"  [{i.area}] {i.what}"
  IO.println ""
  IO.println s!"not defects — requirement, proof, measurement, inherited, undecided, closed: {nonDefects.length}"
  for o in nonDefects do
    IO.println s!"  [{o.area}] {o.what}"
    IO.println s!"      {o.why}"
  IO.println ""
  IO.println s!"discharged — was red here, the ledger now proves it: {discharged.length}"
  for d in discharged do
    IO.println s!"  [{d.area}] {d.what}  ({d.ledger})"
    IO.println s!"      {d.why}"
  IO.println ""
  IO.println s!"red items across the registered ledgers: {ledgerTotal}"
  IO.println s!"classified: {Ferrosa.Broken.broken.length} broken + {unimplemented.length} unimplemented + {nonDefects.length} not defects = {Ferrosa.Broken.broken.length + unimplemented.length + nonDefects.length}"

end Ferrosa.Unimplemented
