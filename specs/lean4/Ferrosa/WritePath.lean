/-
  The write path: when a write is durable, when it is visible, and what a crash takes
  ================================================================================

  A client is never told a row exists that the engine is not holding: an ack follows the commit-log
  append, and under `batch` it also follows an fsync, so an acknowledged write survives the process
  dying. The default mode is not that one — under `periodic` an acked write can still be unsynced
  when the machine stops, and that loss is a reachable state rather than a hypothesis. Admission
  fails closed, so a rejected row is never partly written and nothing appends after the failure, and
  no later tier drops a row on its way through flush or quarantine.

  On the read side cells resolve to the newest timestamp, with one gap a user can meet: two replicas
  holding the same cells in a different physical order pick different winners at equal timestamps,
  so read repair is what converges them, not the merge alone.
-/
import Ferrosa.Core

namespace Ferrosa.WritePath

open Ferrosa

/-! ## The store as the write path sees it: one log, a synced prefix, two memtables

A write is durable exactly when its position in the commit log falls below `synced`, so the distance
between `synced` and the log's length is the number of rows a crash can still take. The two memtables
are staging, not a second copy of record: `active` fills, is sealed into `flushing`, and is published
as an SSTable a reader can see, and no step anywhere removes a row from the log. Quarantine is where a
row that fails validation at flush or replay goes — out of the memtable, still in the log, never
silently dropped.
-/

/-- The three sync strategies of `SyncStrategyConfig`. -/
inductive SyncMode where
  | batch      -- fsync every write: zero-loss window
  | periodic   -- background fsync on a timer: bounded loss window (default)
  | group      -- batched fsync bounded by `max_wait`
  deriving DecidableEq, Repr

/-- A single cell-level write.  `ts` is the write timestamp used by cell-level
    last-write-wins on the read path. -/
structure Entry where
  key : Node
  ts : Nat
  val : Nat
  deriving DecidableEq, Repr

/-- One storage engine, as the write path sees it.

    `log` is the commit log; `synced` is the length of its fsynced prefix, so
    entry `i` is durable iff `i < synced`.  `active`/`flushing` are the two
    memtables, `published` the SSTables, `quarantined` the rows rejected by
    cell/clustering validation. -/
structure Store where
  log : List Entry
  synced : Nat
  active : List Entry
  flushing : List Entry
  published : List (List Entry)
  quarantined : List Entry
  acked : Nat
  admitted : Bool
  maxLog : Nat
  deriving Repr

def init (mode : SyncMode) : Store where
  log := []
  synced := 0
  active := []
  flushing := []
  published := []
  quarantined := []
  acked := 0
  admitted := true
  maxLog := 1024

/-! ## The steps a row takes, and the guard on each

One constructor per arrow in `data-flow.md`.  The guards are the ones the spec
states: admission is checked *before* the commit-log append (fail closed); an
ack requires the row to already be in the commit log; under `Batch` an ack
additionally requires the entry to be fsynced. -/
inductive Step (mode : SyncMode) : Store → Store → Prop where
  /-- `CommitLog::append` + `active.put`.  No client-visible effect yet. -/
  | write {s : Store} {e : Entry}
      (hadm : s.admitted = true) (hcap : s.log.length < s.maxLog) :
      Step mode s { s with log := s.log ++ [e], active := s.active ++ [e] }
  /-- Admission failure: fail closed, before the commit-log append. -/
  | admitFail {s : Store} (h : s.admitted = true) :
      Step mode s { s with admitted := false }
  /-- Background/timer fsync: the whole current log becomes durable. -/
  | fsync {s : Store} :
      Step mode s { s with synced := s.log.length }
  /-- Client ack.  Requires the row to be logged; under `Batch` also durable. -/
  | ack {s : Store}
      (hbound : s.acked < s.log.length)
      (hsync : mode = .batch → s.synced = s.log.length) :
      Step mode s { s with acked := s.acked + 1 }
  /-- `flush_guard.lock` + swap in a fresh memtable. -/
  | sealMem {s : Store} :
      Step mode s { s with flushing := s.flushing ++ s.active, active := [] }
  /-- `ArcSwap: prepend SstableDescriptor` — the flushing memtable becomes an
      immutable, readable SSTable. -/
  | publish {s : Store} :
      Step mode s { s with published := s.flushing :: s.published, flushing := [] }
  /-- A row that fails validation at flush or replay is quarantined and
      skipped; the flush continues. -/
  | quarantine {s : Store} {e : Entry} (h : e ∈ s.active) :
      Step mode s { s with active := s.active.filter (fun x => x != e),
                           quarantined := s.quarantined ++ [e] }

/-! ## Which stores the engine can actually reach, given its sync mode

`SyncMode` is a configuration, so it is a parameter of the transition system and not a field of the
store: `Reachable .batch s` reads "`s` is reachable when the engine fsyncs every write", and the
same store may be reachable under one mode and not another. Every property `WritePath` proves is
stated over this predicate, which is why each of them is conditional on the mode an operator chose.
-/

inductive Reachable (mode : SyncMode) : Store → Prop where
  | init : Reachable mode (init mode)
  | step : Reachable mode s → Step mode s s' → Reachable mode s'

/-! ## What holds of every store the engine can reach, in any mode

Three things hold whatever sync mode the operator selected: the commit log only grows, the fsynced
prefix never runs past the log, and the ack counter never runs past the log either — a client is never
told a row exists that the engine is not holding. What the mode buys is the fourth: under `Batch` an
acknowledged write is also a synced one, so it survives the process dying, while under the default
`Periodic` there is a reachable store where the ack precedes the fsync and an unclean stop loses that
write. Admission failure is sticky as well: once a row is refused, no later step appends to the log, so
a full log stops taking writes instead of quietly degrading durability.
-/

/-- The commit log only ever grows. -/
theorem log_append_only {mode : SyncMode} {s s' : Store} (h : Step mode s s') :
    s.log <+: s'.log := by
  cases h with
  | write _ _ => simp [List.prefix_append]
  | admitFail _ => simp
  | fsync => simp
  | ack _ _ => simp
  | sealMem => simp
  | publish => simp
  | quarantine _ => simp

/-- Step-level: the durable prefix never runs past the log. -/
theorem step_synced_le {mode : SyncMode} {s s' : Store} (hs : Step mode s s')
    (h0 : s.synced ≤ s.log.length) : s'.synced ≤ s'.log.length := by
  cases hs with
  | write _ _ => simp; omega
  | admitFail _ => simpa using h0
  | fsync => simp
  | ack _ _ => simpa using h0
  | sealMem => simpa using h0
  | publish => simpa using h0
  | quarantine _ => simpa using h0

/-- The fsynced prefix never runs past the log, so the durable region is always a real prefix of
    what was written — the premise every durability statement about a reachable store is stated
    against. -/
theorem synced_le_logged {mode : SyncMode} {s : Store} (h : Reachable mode s) :
    s.synced ≤ s.log.length := by
  induction h with
  | init => simp [init]
  | step hr hs ih => exact step_synced_le hs ih

/-- Step-level: the ack counter never outruns the log. -/
theorem step_acked_le {mode : SyncMode} {s s' : Store} (hs : Step mode s s')
    (h0 : s.acked ≤ s.log.length) : s'.acked ≤ s'.log.length := by
  cases hs with
  | write _ _ => simp; omega
  | admitFail _ => simpa using h0
  | fsync => simpa using h0
  | ack hb _ => simp; omega
  | sealMem => simpa using h0
  | publish => simpa using h0
  | quarantine _ => simpa using h0

/-- **No ack without a log record.**  The client-visible ack counter can never
    outrun the commit log: a client is never told a row exists that the engine
    is not holding. -/
theorem acked_le_logged {mode : SyncMode} {s : Store} (h : Reachable mode s) :
    s.acked ≤ s.log.length := by
  induction h with
  | init => simp [init]
  | step hr hs ih => exact step_acked_le hs ih

/-- Step-level: under `Batch`, acks are always durable. -/
theorem step_batch_durable {s s' : Store} (hs : Step .batch s s')
    (h0 : s.acked ≤ s.synced) (h1 : s.synced ≤ s.log.length) :
    s'.acked ≤ s'.synced := by
  cases hs with
  | write _ _ => simpa using h0
  | admitFail _ => simpa using h0
  | fsync => simp; omega
  | ack _ hb => have := hb rfl; simp; omega
  | sealMem => simpa using h0
  | publish => simpa using h0
  | quarantine _ => simpa using h0

/-- **Batch mode closes the loss window**: every acked write is fsynced. -/
theorem batch_ack_is_durable {s : Store} (h : Reachable .batch s) : s.acked ≤ s.synced := by
  induction h with
  | init => simp [init]
  | step hr hs ih => exact step_batch_durable hs ih (synced_le_logged hr)

/-- **FMEA ST-1, proved reachable.**  Under the default `Periodic` strategy
    there is a state in which the engine has acked a write that is not yet
    fsynced: an unclean stop loses it.  The spec states this; here it is a
    theorem about the specified machine. -/
theorem periodic_ack_can_be_lost :
    ∃ s, Reachable .periodic s ∧ s.acked > s.synced := by
  let e0 : Entry := ⟨0, 1, 42⟩
  let s0 : Store := init .periodic
  let s1 : Store := { s0 with log := [e0], active := [e0] }
  let s2 : Store := { s1 with acked := 1 }
  have h1 : Reachable .periodic s1 :=
    Reachable.step (Reachable.init)
      (Step.write (s := s0) (e := e0) rfl (by simp [s0, init]))
  have h2 : Reachable .periodic s2 :=
    Reachable.step h1
      (Step.ack (s := s1) (by simp [s1, s0, init])
        (by intro hc; simp at hc))
  exact ⟨s2, h2, by simp [s2, s1, s0, init]⟩

/-- Admission failure cannot be undone: no transition flips `admitted` back. -/
theorem admitted_sticky {mode : SyncMode} {s s' : Store} (h : Step mode s s')
    (hT : s'.admitted = true) : s.admitted = true := by
  cases h <;> simp_all

/-- **Fail-closed, precisely**: once admission has failed, no subsequent
    transition appends to the commit log.  The engine stops accepting rows
    instead of silently degrading durability. -/
theorem no_append_after_admission_failure {mode : SyncMode} {s s' : Store}
    (h : Step mode s s') (hfalse : s.admitted = false) : s'.log = s.log := by
  cases h with
  | write hadm _ => simp_all
  | admitFail _ => simp_all
  | fsync => simp_all
  | ack _ _ => simp_all
  | sealMem => simp_all
  | publish => simp_all
  | quarantine _ => simp_all

/-! ### Flush and quarantine keep their rows

`data-flow.md`: the flush path seals the active memtable into `flushing`, writes
a BTI SSTable, and prepends its descriptor; quarantine moves a rejected row to a
durable sidecar.  Neither may silently drop a row. -/

/-- **Every memtable row is in the commit log.**  The commit-log append happens
    before the `put`, and every later tier only ever moves rows around. -/
theorem active_subset_log {mode : SyncMode} {s : Store} (h : Reachable mode s) :
    ∀ e, e ∈ s.active → e ∈ s.log := by
  induction h with
  | init => intro e he; simp [init] at he
  | step hr hs ih =>
      intro e he
      cases hs with
      | write _ _ =>
          simp only [List.mem_append] at he
          rcases he with h1 | h2
          · exact List.mem_append_left _ (ih e h1)
          · simp only [List.mem_append, List.mem_singleton] at h2 ⊢
            exact Or.inr h2
      | admitFail _ => exact ih e he
      | fsync => exact ih e he
      | ack _ _ => exact ih e he
      | sealMem => simp at he
      | publish => exact ih e he
      | quarantine _ =>
          simp only [List.mem_filter] at he
          exact ih e he.1

/-- **Flush loses no row**: the seal-then-publish sequence carries every row of
    the active memtable into the published SSTable. -/
theorem flush_keeps_rows (s : Store) :
    ∀ e, e ∈ s.active →
      e ∈ ({ { s with flushing := s.flushing ++ s.active, active := [] } with
               published := (s.flushing ++ s.active) :: s.published,
               flushing := [] } : Store).published.flatten := by
  intro e he
  simp only [List.mem_flatten]
  exact ⟨s.flushing ++ s.active, by simp, by simp [he]⟩

/-- Quarantine retains the rejected row in the durable sidecar. -/
theorem quarantine_retains_row (s : Store) (e : Entry) (h : e ∈ s.active) :
    e ∈ ({ s with active := s.active.filter (fun x => x != e),
                   quarantined := s.quarantined ++ [e] } : Store).quarantined := by
  simp

/-- Quarantine leaves every *other* row in the memtable. -/
theorem quarantine_keeps_others (s : Store) (e r : Entry)
    (hr : r ∈ s.active) (hne : (r != e) = true) :
    r ∈ ({ s with active := s.active.filter (fun x => x != e),
                   quarantined := s.quarantined ++ [e] } : Store).active := by
  simp only [List.mem_filter]
  exact ⟨hr, hne⟩

/-! ## Cell-level last-write-wins

The read path merges partitions "newest-first with cell-level LWW"
(`ferrosa-storage/specs/data-flow.md` § Read path).  Two things must hold for
that phrase to mean anything: resolution picks the maximum timestamp, and
merging the same write twice is a no-op. -/

/-- Merge one incoming cell into the current value.  Ties keep the incoming
    cell, which is what "newest-first, first wins" amounts to. -/
def mergeCell (inc : Nat × Nat) : Option (Nat × Nat) → Option (Nat × Nat)
  | none => some inc
  | some cur => some (if inc.1 ≥ cur.1 then inc else cur)

/-- Resolution over a newest-first partition list. -/
def resolveCell (l : List (Nat × Nat)) : Option (Nat × Nat) :=
  l.foldr mergeCell none

/-- Resolution is defined one cell at a time: the answer for a newest-first list is that list's head
    merged into the answer for its tail, which is the step form every read-path argument uses. -/
theorem resolveCell_cons (p : Nat × Nat) (ps : List (Nat × Nat)) :
    resolveCell (p :: ps) = mergeCell p (resolveCell ps) := by
  simp [resolveCell]

/-- A merge always yields a cell once it has an incoming one, whatever was there before, so a
    partition's resolution cannot be empty by accident. -/
theorem mergeCell_ne_none (inc : Nat × Nat) (st : Option (Nat × Nat)) :
    mergeCell inc st ≠ none := by
  cases st <;> simp [mergeCell]

/-- So resolution returns nothing only for the empty list: a partition with a cell always resolves
    to a value, and a missing answer means there was no cell there rather than a lost one. -/
theorem resolveCell_none {l : List (Nat × Nat)} (h : resolveCell l = none) : l = [] := by
  cases l with
  | nil => rfl
  | cons p ps => rw [resolveCell_cons] at h; exact absurd h (mergeCell_ne_none p _)

/-- LWW never rewinds a cell's timestamp. -/
theorem mergeCell_monotone {inc cur r : Nat × Nat}
    (h : mergeCell inc (some cur) = some r) : cur.1 ≤ r.1 := by
  simp only [mergeCell] at h
  split at h
  · simp only [Option.some.injEq] at h; subst h; omega
  · simp only [Option.some.injEq] at h; subst h; omega

/-- **Idempotent apply** — the property Accord's applied-txn ledger (I-28)
    relies on: replaying the same write twice changes nothing. -/
theorem mergeCell_idempotent (inc : Nat × Nat) (st : Option (Nat × Nat)) :
    mergeCell inc (mergeCell inc st) = mergeCell inc st := by
  cases st with
  | none => simp [mergeCell]
  | some cur =>
      simp only [mergeCell]
      by_cases h : inc.1 ≥ cur.1 <;> simp [h]

/-- Resolution returns an element of the list. -/
theorem resolveCell_mem : ∀ {l : List (Nat × Nat)} {r : Nat × Nat},
    resolveCell l = some r → r ∈ l := by
  intro l
  induction l with
  | nil => intro r h; simp [resolveCell] at h
  | cons p ps ih =>
      intro r h
      rw [resolveCell_cons] at h
      cases hst : resolveCell ps with
      | none =>
          simp [mergeCell, hst] at h
          subst h; simp
      | some cur =>
          simp only [mergeCell, hst] at h
          by_cases hc : p.1 ≥ cur.1
          · simp only [if_pos hc, Option.some.injEq] at h
            subst h; simp
          · simp only [if_neg hc, Option.some.injEq] at h
            subst h
            exact List.mem_cons_of_mem _ (ih hst)

/-- Resolution returns a maximum-timestamp cell. -/
theorem resolveCell_max : ∀ {l : List (Nat × Nat)} {r : Nat × Nat},
    resolveCell l = some r → ∀ p ∈ l, p.1 ≤ r.1 := by
  intro l
  induction l with
  | nil => intro r h; simp [resolveCell] at h
  | cons p ps ih =>
      intro r h q hq
      rw [resolveCell_cons] at h
      cases hst : resolveCell ps with
      | none =>
          have hps : ps = [] := resolveCell_none hst
          subst hps
          simp [mergeCell, hst] at h
          subst h
          simp only [List.mem_cons, List.not_mem_nil, or_false] at hq
          subst hq; omega
      | some cur =>
          simp only [mergeCell, hst] at h
          have hcur : ∀ x ∈ ps, x.1 ≤ cur.1 := ih hst
          by_cases hc : p.1 ≥ cur.1
          · simp only [if_pos hc, Option.some.injEq] at h
            subst h
            simp only [List.mem_cons] at hq
            rcases hq with rfl | hq
            · omega
            · have := hcur q hq; omega
          · simp only [if_neg hc, Option.some.injEq] at h
            subst h
            simp only [List.mem_cons] at hq
            rcases hq with rfl | hq
            · omega
            · exact hcur q hq

/-- A newer write is immediately visible to a later read. -/
theorem resolveCell_sees_newest {ts v : Nat} {l : List (Nat × Nat)}
    (h : ∀ p ∈ l, p.1 ≤ ts) : resolveCell ((ts, v) :: l) = some (ts, v) := by
  rw [resolveCell_cons]
  cases hres : resolveCell l with
  | none => simp [mergeCell]
  | some q =>
      have hq : q.1 ≤ ts := h q (resolveCell_mem hres)
      simp only [mergeCell]
      rw [if_pos (by omega)]

/-- **Equal timestamps are order-dependent.**  Two replicas holding the same two
    cells in different physical order resolve to different winners, so "cell-level
    LWW" alone does not converge them: repair has to surface the tie (cluster
    invariant 3), yet the storage read path silently resolves by position. -/
theorem lww_ties_are_order_dependent :
    resolveCell [(5, 1), (5, 2)] ≠ resolveCell [(5, 2), (5, 1)] := by
  native_decide

/-! ## Coordinated (tunable-CL) write

`data-flow.md` § 1: the coordinator resolves `rf` replicas, fans out, and
returns once `block_for(rf)` acks arrive.  The ack set is exactly
`replicas ∩ live`. -/

structure Coord where
  n : Nat
  rf : Nat
  replicas : SetU
  live : SetU

def Coord.ackSet (c : Coord) : SetU := SetU.inter c.replicas c.live

def Coord.acks (c : Coord) : Nat := c.ackSet.card c.n

/-- The write is acked iff the live replica count reaches the CL threshold. -/
def Coord.ok (c : Coord) (cl : ConsistencyLevel) : Prop := blockFor cl c.rf ≤ c.acks

/-- **Invariant 1, forward direction**: a returned `Ok` really does mean the
    threshold was met.  No path returns success below `block_for(rf)`. -/
theorem ok_means_threshold_met {c : Coord} {cl : ConsistencyLevel}
    (h : c.ok cl) : blockFor cl c.rf ≤ c.acks := h

/-- **Invariant 1, reverse direction**: when the live replica count is short the
    coordinator cannot return `Ok`, so the only alternative is `Unavailable` —
    never a stale success. -/
theorem short_ack_set_must_fail {c : Coord} {cl : ConsistencyLevel}
    (h : c.acks < blockFor cl c.rf) : ¬ c.ok cl := by
  intro hok
  simp only [Coord.ok] at hok
  omega

/-- Acks are always drawn from the replica set. -/
theorem acks_are_replicas {c : Coord} :
    ∀ i, i < c.n → c.ackSet i = true → c.replicas i = true := by
  intro i _ h
  simp only [Coord.ackSet, SetU.inter] at h
  rw [Bool.and_eq_true] at h
  exact h.1

/-- **The write/read intersection requirement.**  Two ack sets drawn from the
    same `rf`-sized replica universe intersect whenever `W + R > rf`; this is
    what "QUORUM at RF=3 is read-your-write" reduces to. -/
theorem quorum_read_sees_quorum_write {rf w r : Nat}
    {A B : SetU} (hA : A.card rf = w) (hB : B.card rf = r) (hwr : r + w > rf) :
    ∃ i, i < rf ∧ A i = true ∧ B i = true :=
  quorum_wr_intersects hwr hA hB

end Ferrosa.WritePath