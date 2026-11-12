/-
  Ferrosa formal specification — storage: evicting a cached SSTable, and healing one the
  object store never received.
  ======================================================================================

  Local disk is a cache; the object store is the durable copy. `sync_sstables_to_s3`
  (`ferrosa-storage/src/engine.rs`) uploads each generation and then lists it in the node's
  manifest. `enforce_uploaded_sstable_cache_limit` evicts uploaded generations under disk
  pressure: it deletes the local files and writes a `<gen>.evicted` marker, and startup
  restores a marked generation from the store, refusing to start when the store does not hold
  it.

  A generation's state is four facts: its files are on local disk, the manifest lists it, the
  store holds every component at its local size, and an eviction marker exists. The data
  survives while it is local or stored (`NoLoss`); a marker is honoured only if the store
  holds the generation (`Recoverable`).

  The bug this models has two halves, and both read "the manifest lists it" as "the store
  holds it". The manifest can list a generation the store does not hold: an upload that
  stored only a sidecar, a generation copied in by hand, a manifest written ahead of a
  failed upload. That is the environment step `drift`.

  * Eviction (`evictOld`) took a listed generation as uploaded and deleted it. After drift
    that destroys the only copy and leaves a marker startup cannot honour
    (`evict_old_loses_data`, `old_reaches_loss`). This half was fixed on `main` by
    HEAD-checking every component first (`components_missing_from_store`); here that is
    `evictNew`, which preserves both invariants (`evict_new_preserves_no_loss`,
    `evict_new_preserves_recoverable`).
  * Sync (`syncOld`) skipped any listed generation, so a drifted generation is a fixpoint:
    no number of sync passes ever uploads it (`sync_old_never_heals`). Eviction is then
    refused forever (safe, but the disk never drains) and the data has one copy. The fix,
    `syncNew`, verifies a listed local generation against the store and uploads it when the
    store lacks it (`sync_new_heals`), and is unchanged for unlisted generations
    (`sync_new_agrees_on_unlisted`).

  With the fixed rules, every reachable state satisfies both invariants
  (`new_rules_keep_the_invariant`), and a drifted generation becomes evictable after one sync
  (`drifted_generation_heals_then_evicts`). With the old rules a reachable state loses the
  data and carries a marker startup refuses (`old_reaches_loss`).

  Scope. One generation; the store does not lose an object it acknowledged (so a generation
  verified once stays stored, which is what lets the fix remember verified generations per
  process); "stored" means every component present at its local size, the check both
  `components_missing_from_store` and the fixed sync perform. The pending-upload ledger,
  compaction retirement and concurrent passes are not modelled: none of them can make a
  listed generation stored.
-/

namespace Ferrosa.Storage.Eviction

/-! ## A generation and its invariants -/

/-- What is true of one SSTable generation on one node. -/
structure Gen where
  /-- The generation's files are on local disk. -/
  isLocal : Bool
  /-- The node's manifest lists it. -/
  listed : Bool
  /-- The store holds every component at its local size. -/
  stored : Bool
  /-- A `<gen>.evicted` marker exists. -/
  marker : Bool
  deriving Repr, DecidableEq

/-- A freshly flushed generation: on disk, not yet uploaded or listed. -/
def fresh : Gen := ⟨true, false, false, false⟩

/-- Some copy of the data exists. -/
def NoLoss (g : Gen) : Prop := g.isLocal = true ∨ g.stored = true

/-- Startup can honour the marker: a marked generation is in the store. -/
def Recoverable (g : Gen) : Prop := g.marker = true → g.stored = true

/-- Both invariants. -/
def Inv (g : Gen) : Prop := NoLoss g ∧ Recoverable g

instance (g : Gen) : Decidable (NoLoss g) := by unfold NoLoss; infer_instance
instance (g : Gen) : Decidable (Recoverable g) := by unfold Recoverable; infer_instance
instance (g : Gen) : Decidable (Inv g) := by unfold Inv; infer_instance

/-! ## The rules -/

/-- The environment: the manifest comes to list a generation without the store gaining it
(a sidecar-only upload, a hand-copied generation, a manifest written ahead of an upload). -/
def drift (g : Gen) : Gen := { g with listed := true }

/-- Delete the local files and leave a marker. -/
def evict (g : Gen) : Gen := { g with isLocal := false, marker := true }

/-- The shipped evictor: a listed local generation counts as uploaded. -/
def evictOld (g : Gen) : Gen := if g.listed && g.isLocal then evict g else g

/-- The guarded evictor: it also requires every component in the store
(`components_missing_from_store` returns nothing). -/
def evictNew (g : Gen) : Gen := if g.listed && g.isLocal && g.stored then evict g else g

/-- Upload every component and list the generation once the upload is confirmed. -/
def upload (g : Gen) : Gen := { g with stored := true, listed := true }

/-- The shipped sync: a listed generation is skipped, whatever the store holds. -/
def syncOld (g : Gen) : Gen := if g.listed then g else if g.isLocal then upload g else g

/-- The fixed sync: a listed generation is skipped only once the store is verified to hold it;
a local generation that is unlisted, or listed but missing from the store, is uploaded. -/
def syncNew (g : Gen) : Gen := if g.isLocal && !(g.listed && g.stored) then upload g else g

/-- Startup restores a marked generation from the store. -/
def restore (g : Gen) : Gen := { g with isLocal := true, marker := false }

/-! ## The bug: listed was read as stored -/

/-- A generation the manifest lists and the store never received. -/
def drifted : Gen := drift fresh

/-- The shipped evictor deletes the only copy of a drifted generation. -/
theorem evict_old_loses_data : NoLoss drifted ∧ ¬ NoLoss (evictOld drifted) := by decide

/-- And leaves a marker startup cannot honour: the node refuses to start. -/
theorem evict_old_strands_the_marker : ¬ Recoverable (evictOld drifted) := by decide

/-- A drifted generation is a fixpoint of the shipped sync. -/
theorem sync_old_fixes_drifted : syncOld drifted = drifted := by decide

/-- Any number of shipped sync passes leaves a drifted generation exactly as it was. -/
theorem sync_old_repeat_fixes_drifted (n : Nat) : Nat.repeat syncOld n drifted = drifted := by
  induction n with
  | zero => rfl
  | succ n ih =>
    show syncOld (Nat.repeat syncOld n drifted) = drifted
    rw [ih]; exact sync_old_fixes_drifted

/-- So no number of sync passes ever uploads it. -/
theorem sync_old_never_heals (n : Nat) : (Nat.repeat syncOld n drifted).stored = false := by
  rw [sync_old_repeat_fixes_drifted]; rfl

/-! ## The fix -/

/-- The guarded evictor never deletes the last copy. -/
theorem evict_new_preserves_no_loss (g : Gen) : NoLoss g → NoLoss (evictNew g) := by
  obtain ⟨l, li, s, m⟩ := g
  cases l <;> cases li <;> cases s <;> cases m <;> decide

/-- And never leaves a marker the store cannot honour. -/
theorem evict_new_preserves_recoverable (g : Gen) : Recoverable g → Recoverable (evictNew g) := by
  obtain ⟨l, li, s, m⟩ := g
  cases l <;> cases li <;> cases s <;> cases m <;> decide

/-- The guarded evictor refuses a drifted generation. -/
theorem evict_new_keeps_drifted : evictNew drifted = drifted := by decide

/-- The fixed sync leaves every local generation in the store. -/
theorem sync_new_heals (g : Gen) (h : g.isLocal = true) : (syncNew g).stored = true := by
  obtain ⟨l, li, s, m⟩ := g
  cases h; cases li <;> cases s <;> rfl

/-- On an unlisted generation the fixed sync is the shipped one. -/
theorem sync_new_agrees_on_unlisted (g : Gen) (h : g.listed = false) : syncNew g = syncOld g := by
  obtain ⟨l, li, s, m⟩ := g
  cases h; cases l <;> cases s <;> cases m <;> decide

/-- On a listed generation the store already holds, the fixed sync does nothing (no
re-upload of healthy data). -/
theorem sync_new_skips_stored (g : Gen) (hl : g.listed = true) (h : g.stored = true) :
    syncNew g = g := by
  obtain ⟨l, li, s, m⟩ := g
  cases hl; cases h; cases l <;> rfl

/-- One fixed sync, and the drifted generation becomes safe to evict. -/
theorem drifted_generation_heals_then_evicts :
    let g := evictNew (syncNew drifted)
    g.isLocal = false ∧ g.marker = true ∧ Inv g := by decide

/-! ## Reachable states

A generation starts `fresh`, and any rule may fire at any time, including drift.
-/

/-- One step of the shipped system. -/
inductive OldStep : Gen → Gen → Prop
  | drift (g : Gen) : OldStep g (drift g)
  | sync (g : Gen) : OldStep g (syncOld g)
  | evict (g : Gen) : OldStep g (evictOld g)
  | restore (g : Gen) : g.marker = true → g.stored = true → OldStep g (restore g)

/-- One step of the fixed system. -/
inductive NewStep : Gen → Gen → Prop
  | drift (g : Gen) : NewStep g (drift g)
  | sync (g : Gen) : NewStep g (syncNew g)
  | evict (g : Gen) : NewStep g (evictNew g)
  | restore (g : Gen) : g.marker = true → g.stored = true → NewStep g (restore g)

/-- States reachable from `fresh` under a step relation. -/
inductive Reach (step : Gen → Gen → Prop) : Gen → Prop
  | init : Reach step fresh
  | step {g g' : Gen} : Reach step g → step g g' → Reach step g'

/-- The shipped rules reach a state with no copy of the data and a marker startup refuses. -/
theorem old_reaches_loss : ∃ g, Reach OldStep g ∧ ¬ NoLoss g ∧ ¬ Recoverable g :=
  ⟨evictOld drifted,
   .step (.step .init (.drift fresh)) (.evict drifted),
   by decide, by decide⟩

/-- Every fixed rule preserves the invariant. -/
theorem new_step_preserves (g g' : Gen) (hs : NewStep g g') (hi : Inv g) : Inv g' := by
  cases hs with
  | drift =>
    obtain ⟨l, li, s, m⟩ := g
    cases l <;> cases li <;> cases s <;> cases m <;> revert hi <;> decide
  | sync =>
    obtain ⟨l, li, s, m⟩ := g
    cases l <;> cases li <;> cases s <;> cases m <;> revert hi <;> decide
  | evict =>
    exact ⟨evict_new_preserves_no_loss g hi.1, evict_new_preserves_recoverable g hi.2⟩
  | restore hm hst =>
    obtain ⟨l, li, s, m⟩ := g
    cases hm; cases hst; cases l <;> cases li <;> decide

/-- With the fixed rules, no reachable state loses the data or strands a marker. -/
theorem new_rules_keep_the_invariant (g : Gen) (h : Reach NewStep g) : Inv g := by
  induction h with
  | init => decide
  | step _ hs ih => exact new_step_preserves _ _ hs ih

end Ferrosa.Storage.Eviction
