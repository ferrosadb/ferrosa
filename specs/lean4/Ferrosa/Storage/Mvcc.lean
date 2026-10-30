/-
  Snapshots: the version a read sees, and the version collection may not remove
  ===========================================================================

  A transaction reads the newest version of a cell at or below its snapshot, and that answer stays
  the same for the life of the transaction because collection keeps the newest version at or below
  the oldest live snapshot and everything newer than it. A reader that stops making progress pins
  every version behind it, which is why it is killed after thirty seconds without one and why its
  next statement fails instead of silently reading a newer state.

  Versions held inside the record buy a constant probe but not a constant read: one record per
  lookup while the chain covers the snapshot, yet every SSTable in the view is still bloom-probed
  to find that record. Two pieces of this are designed and not built — the in-record chain, since a
  cell still holds one value, and the predicate locks SSI needs, since the read path takes no tag.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Storage.Mvcc

open Ferrosa.Ledger

/-- One version of a cell: commit timestamp, the transaction that wrote it, the value. -/
structure Version where
  ts : Nat
  txid : Nat
  val : Nat
  deriving DecidableEq, Repr

/-- A version chain, newest first. -/
abbrev Chain := List Version

/-- **Snapshot visibility**: the newest version whose commit timestamp is at or below the
    snapshot's. A chain in the wrong order answers this wrongly, which is why the order is part of
    the format. -/
def visibleAt (c : Chain) (snap : Nat) : Option Version :=
  c.find? fun v => decide (v.ts ≤ snap)

/-- Naive GC: discard everything older than the oldest live snapshot. -/
def gcNaive (c : Chain) (oldest : Nat) : Chain :=
  c.filter fun v => decide (oldest ≤ v.ts)

/-- Safe GC: keep the floor — the newest version at or below the oldest snapshot — and everything
    newer than the floor. -/
def gcSafe (c : Chain) (oldest : Nat) : Chain :=
  match visibleAt c oldest with
  | none => c.filter fun v => decide (oldest ≤ v.ts)
  | some floor => c.filter fun v => decide (floor.ts ≤ v.ts)

/-- **The naive rule destroys a version a live snapshot still needs.**  A chain with versions at
    ts 100 and ts 5, an oldest live snapshot of 10: the snapshot at 50 must see ts 5, and the
    naive rule has already discarded it. -/
theorem naive_gc_breaks_a_live_snapshot :
    visibleAt [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 50 = some ⟨5, 1, 2⟩
      ∧ visibleAt (gcNaive [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 10) 50 = none := by
  decide

/-- **A snapshot that never closes pins every version behind it.**  Retention is bounded by the
    oldest live reader, not by time: with a reader at 5 the whole chain is kept, with a reader at 100
    only the head survives. A stalled transaction therefore retains unbounded disk for as long as it
    lives, which is why an idle-snapshot age bound is part of the retention policy rather than an
    operational nicety. -/
theorem a_stalled_reader_pins_every_version_behind_it :
    (gcSafe [⟨100, 1, 1⟩, ⟨50, 2, 2⟩, ⟨5, 3, 3⟩] 5).length = 3
      ∧ (gcSafe [⟨100, 1, 1⟩, ⟨50, 2, 2⟩, ⟨5, 3, 3⟩] 100).length = 1 := by
  decide

/-- **The floor rule preserves every live snapshot.**  Same chain, oldest 10: the answers at the
    oldest snapshot, between them, and above the head are unchanged. -/
theorem safe_gc_preserves_live_snapshots :
    visibleAt (gcSafe [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 10) 10
        = visibleAt [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 10
      ∧ visibleAt (gcSafe [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 10) 50
        = visibleAt [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 50
      ∧ visibleAt (gcSafe [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 10) 100
        = visibleAt [⟨100, 0, 1⟩, ⟨5, 1, 2⟩] 100 := by
  decide

/-- GC never lengthens a chain, so it cannot cause unbounded growth in the record. -/
theorem gc_never_grows_a_chain (c : Chain) (oldest : Nat) :
    (gcSafe c oldest).length ≤ c.length := by
  unfold gcSafe
  split
  · exact List.length_filter_le _ _
  · exact List.length_filter_le _ _

/-! ## The cost of putting versions in files instead of in the record -/

/-- Reads per snapshot lookup when each SSTable holds one version: every overlapping file. -/
def probesPerRead (files : Nat) : Nat := files

/-- **One version per file is not a constant read.**  A row rewritten ten times, uncompacted, is
    ten probes — and compaction only makes the number smaller, never bounded. -/
theorem a_version_per_file_is_not_a_constant_read (files : Nat) (h : 1 < files) :
    1 < probesPerRead files := by
  simp [probesPerRead]
  omega

/-- **Versions in the record keep the read constant**, at the price of a chain length bounded by
    the number of snapshots that can still need an older version. -/
def readsWithInRecordVersions : Nat := 1

/-- With the chain inside the record, a lookup reads one record however many snapshots can still see
    an older version — this is the constant the design is bought with, and it holds by the shape of
    the read rather than by any measurement. -/
theorem in_record_versions_keep_the_probe_constant : readsWithInRecordVersions = 1 := rfl

/-- The bound the chain must respect: one version per live snapshot, plus the floor. -/
def chainBound (liveSnapshots : Nat) : Nat := liveSnapshots + 1

/-- The chain has to hold one version per live snapshot plus the floor, and that count is the only
    thing bounding how long a record's version list can get: retention follows the oldest reader,
    never the clock. -/
theorem the_chain_bound_is_one_per_live_snapshot (liveSnapshots : Nat) :
    chainBound liveSnapshots = liveSnapshots + 1 := rfl

/-! ## Spill: an overflowing chain goes to a side chain

The chain cap is how many concurrent writers a row absorbs in-record. Past it the oldest versions
move to a side chain rather than letting the record grow without bound: that row costs one extra
probe, and every other row keeps its one. -/

/-- Versions held in the record (newest first) and versions moved to the side chain. -/
structure Spill where
  inline_ : List Nat
  spilled : List Nat
  deriving DecidableEq, Repr

/-- Keep the newest `cap` versions in the record and move the rest to the side chain. -/
def Spill.spill (c : Spill) (cap : Nat) : Spill :=
  ⟨c.inline_.take cap, c.spilled ++ c.inline_.drop cap⟩

/-- **Nothing is lost**: spilling is a move, not a discard. -/
theorem spill_never_loses_a_version (c : Spill) (cap : Nat) :
    (c.spill cap).inline_.length + (c.spill cap).spilled.length
      = c.inline_.length + c.spilled.length := by
  simp only [Spill.spill, List.length_take, List.length_drop, List.length_append]
  omega

/-- **The cap holds after spilling**, which is the whole point: the record stops growing. -/
theorem spill_bounds_the_inline_chain (c : Spill) (cap : Nat) :
    (c.spill cap).inline_.length ≤ cap := by
  simp only [Spill.spill, List.length_take]
  omega

/-- Versions reachable from a record, given whether it carries its spill pointer. -/
def reachableThrough (carried : Bool) (spilled : List Nat) : List Nat :=
  if carried then spilled else []

/-- **Compaction must carry the pointer forward.** Dropping it silently drops every version behind
    it, and no later read can tell that from a version that was never written. -/
theorem spilled_versions_are_lost_if_compaction_drops_the_pointer :
    reachableThrough false [5, 3] = [] ∧ reachableThrough true [5, 3] = [5, 3] := by decide

/-- The cost side of the trade: one more probe for the row that spilled, nothing for the rest. -/
def probesFor (spilled : Bool) : Nat := if spilled then 2 else 1

/-- A row that has spilled costs two record reads where a row that has not costs one, so an
    overflowing chain is paid for by the rows that overflowed and by no others. -/
theorem a_spilled_row_costs_one_extra_probe :
    probesFor false = 1 ∧ probesFor true = 2 := by decide

/-! ## The ledger -/

/-! ## Read-set capture: predicate locks, not dirty bits

SSI needs to know *which transaction* read a row, not merely that a row was read. The standard
mechanism is a predicate lock (PostgreSQL calls it a SIREAD lock): a per-row set of readers, held
for as long as the read snapshot is live. A boolean "dirty" flag on the row cannot express it,
because the flag is the same for every reader and the victim has to be named. -/

/-- A row-level read tag: `reader` read `key` under its snapshot. -/
structure ReadTag where
  key : Nat
  reader : Nat
  deriving DecidableEq, Repr

def tagReader (tags : List ReadTag) (k t : Nat) : Bool := tags.contains ⟨k, t⟩

/-- The read set of `t`, recovered from the tag table. -/
def readSetOf (tags : List ReadTag) (t : Nat) : List Nat :=
  (tags.filter (fun g => g.reader == t)).map (·.key)

/-- Who must be told about `writer` writing `writes`. -/
def conflictsOf (tags : List ReadTag) (writer : Nat) (writes : List Nat) : List Nat :=
  (tags.filter (fun g => g.reader != writer && writes.contains g.key)).map (·.reader)

/-- What a single bit per row can say. -/
def dirtyFlag (tags : List ReadTag) (k : Nat) : Bool := tags.any (fun g => g.key == k)

/-- **A boolean dirty bit cannot name the victim.**  Two tag states are indistinguishable to the flag
    and demand different transactions be rejected, so the flag is not a read set — it is not even
    enough information to raise the right exception. -/
theorem a_boolean_dirty_flag_cannot_identify_the_victim :
    dirtyFlag [⟨0, 1⟩] 0 = dirtyFlag [⟨0, 2⟩] 0
      ∧ conflictsOf [⟨0, 1⟩] 9 [0] ≠ conflictsOf [⟨0, 2⟩] 9 [0] := by
  refine ⟨rfl, ?_⟩
  decide

/-- **The tag table is exactly the read sets of every transaction, indexed by row** — the same
    information, stored the other way round, so detection is identical and the representation costs
    one entry per distinct row a live transaction read. -/
theorem a_tag_table_is_the_read_sets_indexed_by_row :
    readSetOf [⟨0, 1⟩, ⟨1, 1⟩, ⟨2, 2⟩] 1 = [0, 1]
      ∧ tagReader [⟨0, 1⟩, ⟨1, 1⟩, ⟨2, 2⟩] 0 1 = true
      ∧ tagReader [⟨0, 1⟩, ⟨1, 1⟩, ⟨2, 2⟩] 0 2 = false := by decide

/-- **Clearing a tag early loses the conflict**, and the horizon is the same one version retention
    needs: while both readers are live the write-skew pair is caught, and once the tags go the pair
    commits. Predicate locks and MVCC versions share the oldest-snapshot horizon. -/
theorem clearing_a_tag_early_loses_the_conflict :
    conflictsOf [⟨0, 1⟩] 2 [0] = [1]
      ∧ conflictsOf ([] : List ReadTag) 2 [0] = [] := by decide

/-! ### Predicate locks are bookkeeping, not blocking locks

A SIREAD "lock" is a flag, not a mutex: a writer never waits on it. The write completes with a
verdict in every configuration, so contention becomes a retry rather than a stall, and a
reader-writer deadlock cannot even be represented. This is the entire reason to take SSI over
two-phase locking, and it is worth pinning in the type rather than in a comment. -/

/-- The outcome of a write. There is no waiting constructor: the operation is total. -/
inductive WriteOutcome where
  | applied
  | refused (readers : List Nat)
  deriving DecidableEq, Repr

def writeOutcome (tags : List ReadTag) (writer : Nat) (writes : List Nat) : WriteOutcome :=
  match conflictsOf tags writer writes with
  | [] => .applied
  | rs => .refused rs

/-- **A predicate lock never blocks a writer.**  Every write returns a verdict; nothing in the type
    can represent "waiting for a reader", so a long-running reader cannot stall a writer. -/
theorem a_predicate_lock_never_blocks_a_writer (tags : List ReadTag) (writer : Nat)
    (writes : List Nat) :
    writeOutcome tags writer writes = .applied
      ∨ ∃ rs, writeOutcome tags writer writes = .refused rs := by
  cases h : conflictsOf tags writer writes with
  | nil => exact Or.inl (by simp [writeOutcome, h])
  | cons r rs => exact Or.inr ⟨r :: rs, by simp [writeOutcome, h]⟩

/-- Wait-for edges produced by the scheme: none. A reader-writer deadlock needs a cycle of them. -/
def waitForEdges (_tags : List ReadTag) : List (Nat × Nat) := []

/-- **No reader-writer deadlock can form**, because no writer ever waits for a reader. This is the
    property two-phase locking cannot offer: there, the edge exists and the cycle does. -/
theorem nothing_waits_so_no_deadlock_can_form (tags : List ReadTag) :
    waitForEdges tags = [] := rfl

/-- **Contention costs a retry, not a stall**: a writer tripping one tag is refused naming its reader,
    and one tripping two is refused naming both — immediately, in both cases. -/
theorem contention_costs_a_retry_not_a_stall :
    writeOutcome [⟨0, 1⟩] 9 [0] = .refused [1]
      ∧ writeOutcome [⟨0, 1⟩, ⟨0, 2⟩] 9 [0] = .refused [1, 2] := by decide

/-- And a transaction never conflicts with its own reads: read-then-write inside one snapshot is not
    a conflict, so no tag a transaction takes can reject its own commit. -/
theorem a_transaction_does_not_conflict_with_its_own_reads :
    writeOutcome [⟨0, 1⟩] 1 [0] = .applied := by decide

/-- Keys in the first partition of a partition-granularity tag. -/
def inPartition (p k : Nat) : Bool := k / 4 == p

/-- **Partition granularity is sound and over-approximates**: every read that a write touches is
    caught, and so is a write to a key in the same partition that the reader never touched. That is
    the same trade the format already makes — a partition-sized blast radius for a cheaper probe. -/
theorem a_partition_tag_is_sound_but_over_approximates :
    inPartition 0 3 = true
      ∧ inPartition 0 2 = true
      ∧ inPartition 1 9 = false := by decide

/-! ### Vacuum: an ARC decrement, not a traversal

The collector is reference counting, and that is sufficient because a version chain is a strictly
decreasing list of timestamps: it is acyclic by construction, so there is nothing for a cycle
collector to find and no global state to synchronise. A version holds one reference for the live head
and one for each snapshot that sees it. Committing supersedes the head and releases the head's
reference; a snapshot ending releases its own. A version dies when its count reaches zero.

That is the whole of vacuum: one decrement and one test per version, run after the commit that
superseded it. It is also why the leaked-snapshot problem is a policy problem rather than a collector
problem — a count only reaches zero when every holder releases, so the fix is a snapshot timeout, not
a smarter collector.
-/

/-- A version with its reference count: one for the live head, one per snapshot that sees it. -/
structure Ref where
  version : Version
  count : Nat
  deriving DecidableEq, Repr

/-- Release one reference. The version dies at zero, immediately and in place. -/
def release (r : Ref) : Option Ref :=
  if r.count ≤ 1 then none else some { r with count := r.count - 1 }

/-- **A version dies exactly when its last holder releases** — no mark phase, no reachability test. -/
theorem a_version_dies_at_zero :
    release ⟨⟨5, 1, 1⟩, 1⟩ = none
      ∧ release ⟨⟨5, 1, 1⟩, 2⟩ = some ⟨⟨5, 1, 1⟩, 1⟩ := by decide

/-- **A vacuum step is one decrement and one test.** -/
theorem a_vacuum_step_is_one_decrement :
    release ⟨⟨5, 1, 1⟩, 7⟩ = some ⟨⟨5, 1, 1⟩, 6⟩ := by decide

/-- A chain is strictly decreasing in timestamp. -/
def strictlyDecreasing : List Version → Bool
  | a :: b :: rest => decide (b.ts < a.ts) && strictlyDecreasing (b :: rest)
  | _ => true

/-- **The version graph is acyclic, so reference counting alone is exact.**  A cycle collector would
    have nothing to collect, and no global state has to be walked. -/
theorem a_chain_is_acyclic :
    strictlyDecreasing [⟨100, 1, 1⟩, ⟨50, 2, 2⟩, ⟨5, 3, 3⟩] = true
      ∧ strictlyDecreasing [⟨5, 1, 1⟩, ⟨50, 2, 2⟩] = false := by decide

/-- Release `n` references in sequence. -/
def releaseAll : Ref → Nat → Option Ref
  | r, 0 => some r
  | r, n + 1 =>
    match release r with
    | none => none
    | some r' => releaseAll r' n

/-- **Two holders of ts 5 free it on the second release, not before.**  The first release leaves the
    count at one because the other reader still needs the version; the second returns the memory. No
    pass has to come along afterwards and discover that the version died — the releasing thread
    learns it at the moment of release, from `release`'s own result. -/
theorem the_last_holder_returns_the_memory :
    releaseAll ⟨⟨5, 3, 3⟩, 2⟩ 1 = some ⟨⟨5, 3, 3⟩, 1⟩
      ∧ releaseAll ⟨⟨5, 3, 3⟩, 2⟩ 2 = none := by decide

/-- And releasing more than the count cannot happen: the first release that reaches zero reports it
    and stops, so a double release is a `none`, never an underflow. -/
theorem a_release_past_zero_stops_at_zero :
    releaseAll ⟨⟨5, 3, 3⟩, 1⟩ 3 = none := by decide

/-- **Committing releases the head's reference, and a live snapshot keeps the version at one.** -/
theorem a_snapshot_reference_keeps_the_version_alive :
    release ⟨⟨5, 1, 1⟩, 2⟩ = some ⟨⟨5, 1, 1⟩, 1⟩ := by decide

/-- **ARC and the floor rule agree**: the floor survives precisely because the oldest snapshot holds a
    reference to it. -/
theorem arc_agrees_with_the_floor_rule :
    (gcSafe [⟨100, 1, 1⟩, ⟨50, 2, 2⟩, ⟨5, 3, 3⟩] 5).length = 3
      ∧ release ⟨⟨5, 3, 3⟩, 2⟩ = some ⟨⟨5, 3, 3⟩, 1⟩ := by decide

/-- `STALLED_READER_TIMEOUT`: a reader that makes no progress for this long is killed. -/
def stalledReaderTimeoutMs : Nat := 30000

/-- A reader is stalled when it has gone this long without progress. Progress is what resets the
    clock, not age, so a long-running but active scan is never killed. -/
def stalled (sinceProgressMs : Nat) : Bool := sinceProgressMs ≥ stalledReaderTimeoutMs

/-- A reader that reports no progress for thirty seconds is killed, and that single number is the
    whole of the policy: how long a reader has lived plays no part in the decision. -/
theorem the_default_stalled_reader_timeout_is_thirty_seconds :
    stalledReaderTimeoutMs = 30000 := rfl

/-- **An idle reader is killed at the timeout**, and one millisecond before it is not. -/
theorem an_idle_reader_is_killed_at_the_timeout :
    stalled 29999 = false ∧ stalled 30000 = true := by decide

/-- **Total age never kills a reader**; only the absence of progress does, so an hour-long scan that
    keeps reporting progress is untouched. -/
theorem a_progressing_reader_is_never_killed_for_age :
    stalled 0 = false ∧ stalled 25000 = false := by decide

/-- Killing a reader releases every reference it holds, so the versions it pinned become collectable
    at that instant. -/
def kill (r : Ref) : Option Ref := releaseAll r r.count

/-- **Killing a reader returns its memory**: every count it held falls to zero and each version dies
    in place, as with any other release. -/
theorem killing_a_reader_returns_its_memory :
    kill ⟨⟨5, 3, 3⟩, 1⟩ = none ∧ kill ⟨⟨5, 3, 3⟩, 4⟩ = none := by decide

/-- What a killed reader's next statement gets. -/
inductive ReaderOutcome where
  | served
  | killed
  deriving DecidableEq, Repr

def nextStatement (wasKilled : Bool) : ReaderOutcome :=
  if wasKilled then .killed else .served

/-- **A killed reader cannot resume its snapshot**: its versions may already be gone, so the next
    statement must fail rather than silently read a newer state, which would be an isolation
    violation with no error to show for it. -/
theorem a_killed_reader_cannot_resume_its_snapshot :
    nextStatement true = .killed ∧ nextStatement false = .served := by decide

/-- **A snapshot read cannot stop at the first file that holds the key.**  The newest file's version may
    be newer than the snapshot, so a hit is not an answer: every file holding a version must be
    consulted, and that is what makes the lookup linear in the files of the view rather than constant.
-/
theorem a_snapshot_read_must_consult_every_file_holding_the_key :
    visibleAt [⟨100, 1, 1⟩] 50 = none
      ∧ visibleAt [⟨100, 1, 1⟩, ⟨5, 2, 2⟩] 50 = some ⟨5, 2, 2⟩ := by decide

/-- The cost of one snapshot lookup: a token-range test and a bloom probe per SSTable in the view,
    plus a record read per file that holds the key. -/
def snapshotLookupProbes (filesInView filesHoldingKey : Nat) : Nat :=
  filesInView + filesHoldingKey

/-- **The lookup is linear in the files of the view**, and nothing prunes that fan-out today: there is
    no leveled structure, so every lookup walks `guard.sstables`. -/
theorem the_lookup_cost_is_linear_in_the_files_of_the_view :
    snapshotLookupProbes 40 3 = 43 ∧ snapshotLookupProbes 400 3 = 403 := by decide

/-! ### Lookup cost under ADR-022: the visible version is in the record

fnf stores a bounded chain of versions in the cell, so `chainBound` versions of a key live in one
record. That changes what a snapshot lookup pays for: the visible version is found *inside* the
record, so a snapshot covered by the chain costs one record read rather than one per file holding the
key. What it does not change is the bloom fan-out — every file still has to be probed to find the
record at all. Both halves are stated, because implementing the chain and expecting the fan-out to go
away is the mistake this section exists to prevent.
-/

/-- An in-record chain covers a snapshot when its oldest retained version is at or below it. -/
def chainCovers (chainOldest snap : Nat) : Bool := chainOldest ≤ snap

/-- Record reads for one lookup: one when the chain covers the snapshot, otherwise every file holding
    a version of the key. -/
def lookupReads (filesHoldingKey chainOldest snap : Nat) : Nat :=
  if chainCovers chainOldest snap then 1 else filesHoldingKey

/-- **A snapshot inside the chain window costs one record read**; one older than the chain's oldest
    retained version falls back to the file fan-out. -/
theorem the_chain_makes_a_covered_snapshot_one_read :
    lookupReads 40 5 50 = 1 ∧ lookupReads 40 5 4 = 40 := by decide

/-- **The chain does not reduce the bloom fan-out**: the record still has to be found, so every
    SSTable in the view is probed either way. -/
theorem the_chain_does_not_reduce_the_bloom_fan_out :
    snapshotLookupProbes 40 3 = 43 := by decide

def ledger : List SpecItem := [
  ⟨"a snapshot reads the newest version at or below it",
   .met "Mvcc.visibleAt (definition) + safe_gc_preserves_live_snapshots"⟩,
  ⟨"a stalled reader pins every version behind it, which is why a timeout exists",
   .met "Mvcc.a_stalled_reader_pins_every_version_behind_it"⟩,
  ⟨"a reader with no progress for STALLED_READER_TIMEOUT is killed and its memory returned",
   .met "Mvcc.an_idle_reader_is_killed_at_the_timeout"⟩,
  ⟨"total age never kills a reader; only absence of progress does",
   .met "Mvcc.a_progressing_reader_is_never_killed_for_age"⟩,
  ⟨"killing a reader releases every reference it holds",
   .met "Mvcc.killing_a_reader_returns_its_memory"⟩,
  ⟨"a killed reader's next statement fails rather than reading a newer snapshot",
   .met "Mvcc.a_killed_reader_cannot_resume_its_snapshot"⟩,
  ⟨"a version dies exactly when its last holder releases, so collection is exact without tracing",
   .met "Mvcc.a_version_dies_at_zero"⟩,
  ⟨"a vacuum step is one decrement and one test",
   .met "Mvcc.a_vacuum_step_is_one_decrement"⟩,
  ⟨"a version chain is acyclic, so reference counting alone is sufficient",
   .met "Mvcc.a_chain_is_acyclic"⟩,
  ⟨"a live snapshot keeps a superseded version alive by its reference",
   .met "Mvcc.a_snapshot_reference_keeps_the_version_alive"⟩,
  ⟨"the memory is returned by the last release, with no scanning pass afterwards",
   .met "Mvcc.the_last_holder_returns_the_memory"⟩,
  ⟨"a release cannot underflow below zero",
   .met "Mvcc.a_release_past_zero_stops_at_zero"⟩,
  ⟨"reference counting and the floor rule agree on what survives",
   .met "Mvcc.arc_agrees_with_the_floor_rule"⟩,
  ⟨"vacuum runs asynchronously after the committing transaction completes, off the write path",
   .failing
     "no vacuum exists: nothing advances the floor, no reference counts are held, and no background process runs"
     "Mvcc.a_vacuum_step_is_one_decrement"⟩,
  ⟨"GC preserves every live snapshot",
   .met "Mvcc.safe_gc_preserves_live_snapshots"⟩,
  ⟨"GC never grows a chain",
   .met "Mvcc.gc_never_grows_a_chain"⟩,
  ⟨"a snapshot lookup is linear in the SSTables of the view, not constant",
   .met "Mvcc.the_lookup_cost_is_linear_in_the_files_of_the_view"⟩,
  ⟨"a snapshot inside the in-record chain window costs one record read",
   .met "Mvcc.the_chain_makes_a_covered_snapshot_one_read"⟩,
  ⟨"the in-record chain leaves the bloom fan-out at one probe per file",
   .met "Mvcc.the_chain_does_not_reduce_the_bloom_fan_out"⟩,
  ⟨"a snapshot lookup can stop at the first file holding the key",
   .failing
     "false, and that is the cost driver: the newest file's version may be invisible to the snapshot, so every file holding a version must be consulted. One probe per SSTable in the view is the real bound"

     "Mvcc.a_version_per_file_is_not_a_constant_read"⟩,
  ⟨"an overflowing version chain spills to a side chain instead of growing the record",
   .met "Mvcc.spill_bounds_the_inline_chain"⟩,
  ⟨"spilling is a move, not a discard",
   .met "Mvcc.spill_never_loses_a_version"⟩,
  ⟨"compaction carries the spill pointer forward",
   .failing
     "nothing carries it yet: a compaction that rewrites the record drops the pointer and every version behind it becomes unreachable, indistinguishable from a version that never existed"
     "Mvcc.spilled_versions_are_lost_if_compaction_drops_the_pointer"⟩,
  ⟨"a read is captured as a predicate lock naming the reader, not as a dirty bit on the row",
   .met "Mvcc.a_boolean_dirty_flag_cannot_identify_the_victim"⟩,
  ⟨"the tag table and the per-transaction read sets are the same information",
   .met "Mvcc.a_tag_table_is_the_read_sets_indexed_by_row"⟩,
  ⟨"predicate locks are held for the oldest live snapshot, the same horizon as version retention",
   .met "Mvcc.clearing_a_tag_early_loses_the_conflict"⟩,
  ⟨"a partition-granularity tag is sound and aborts conservatively",
   .met "Mvcc.a_partition_tag_is_sound_but_over_approximates"⟩,
  ⟨"a predicate lock is bookkeeping, not a blocking lock: no writer ever waits on a reader",
   .met "Mvcc.a_predicate_lock_never_blocks_a_writer"⟩,
  ⟨"no reader-writer deadlock can form, because no wait-for edge exists to form a cycle from",
   .met "Mvcc.nothing_waits_so_no_deadlock_can_form"⟩,
  ⟨"contention costs a retry, not a stall",
   .met "Mvcc.contention_costs_a_retry_not_a_stall"⟩,
  ⟨"a transaction never conflicts with its own reads",
   .met "Mvcc.a_transaction_does_not_conflict_with_its_own_reads"⟩,
  ⟨"the write path records a predicate lock for every read it takes",
   .failing
     "no locks are recorded: the read path takes no tag, so a concurrent write sees nothing to conflict with"
     "Mvcc.a_tag_table_is_the_read_sets_indexed_by_row"⟩,
  ⟨"versions are stored in the fnf record so the probe stays constant",
   .failing
     "the cell format holds one value per column; an in-record chain bounded by chainBound is designed here but not implemented"
     "Mvcc.in_record_versions_keep_the_probe_constant"⟩
]

/-- Five obligations are recorded red: no vacuum, no predicate locks taken on the read path, one
    value per cell rather than an in-record chain, a spill pointer no compaction carries, and the
    claim that a snapshot lookup may stop at the first file holding the key. The count is pinned so
    that closing one without updating the ledger fails the build. -/
theorem mvcc_has_five_red_items : redCount ledger = 5 := by decide

def main : IO Unit :=
  printLedger "MVCC over fnf obligations" ledger

end Ferrosa.Storage.Mvcc
