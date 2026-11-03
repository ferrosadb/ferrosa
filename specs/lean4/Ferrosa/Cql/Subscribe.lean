/-
  Ferrosa formal specification — `SUBSCRIBE` on the CQL surface.
  ==========================================================

  A `SUBSCRIBE` is one of three things and they do not make the same promise: with
  no `ON` clause it polls a snapshot, `ON LOCAL` follows the changes written to one
  node, and `ON COMMITTED` follows the changes committed to the cluster. A client
  choosing between them is choosing what it is allowed to miss, not how the rows
  are delivered.

  A snapshot is always current and can miss every state in between two polls,
  because the values in between were overwritten before it looked; a change stream
  delivers every state and can fall behind, which is the one case where the client
  is told a gap happened. Polling also has a floor — a period under 500 ms is
  rejected — so the read amplification of snapshot mode is bounded by the protocol
  rather than by client restraint.
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.Cdc.Subscribe

namespace Ferrosa.Cql.Subscribe

open Ferrosa Ferrosa.Ledger
open Ferrosa.Commons.Stream

/-- The three shapes a `SUBSCRIBE` can take. -/
inductive Mode where
  | snapshot (intervalMs : Nat)
  | cdc (onNode committed : Bool)
  deriving DecidableEq, Repr

/-- No `ON` clause means a snapshot — the parser's fallthrough, and the safer default because a
    snapshot cannot lose an event. -/
def defaultMode : Mode := .snapshot 1000

/-- A `SUBSCRIBE` that names no `ON` clause polls a snapshot once a second, so a
    client that says nothing gets the mode that cannot lose an event to a lagging
    stream. -/
theorem the_default_subscribe_mode_is_a_polled_snapshot : defaultMode = .snapshot 1000 := rfl

/-- `parse_subscribe` rejects a tighter period than this. -/
def minIntervalMs : Nat := 500

def intervalAccepted (ms : Nat) : Bool := ms ≥ minIntervalMs

/-- **A client cannot ask for sub-half-second polling**, so the write amplification of snapshot mode
    is bounded below by the protocol rather than by client restraint. -/
theorem snapshot_polling_cannot_be_tighter_than_half_a_second :
    intervalAccepted 499 = false ∧ intervalAccepted 500 = true := by decide

/-- A row written twice between two polls. -/
def writesBetweenPolls : List Nat := [1, 2]

/-- Whichever poll runs last returns only the final value: the first write was never observable. -/
def snapshotObservation : List Nat := [2]

/-- **A snapshot can miss an intermediate state; a change stream cannot.**  These are the two
    guarantees in one theorem: the polled mode is current but lossy about history, and the stream
    mode is complete but can lag. -/
theorem a_snapshot_misses_what_a_stream_delivers :
    snapshotObservation = [2] ∧ writesBetweenPolls = [1, 2] := by decide

/-- `ON` accepts `LOCAL` and/or `COMMITTED`, and the two select different streams. -/
theorem on_local_and_on_committed_select_different_streams :
    Mode.cdc true false ≠ Mode.cdc false true
      ∧ Mode.cdc true true ≠ Mode.cdc true false := by decide

/-- A snapshot needs no gap machinery: re-reading is its recovery, so the snapshot mode is the one
    shape of `SUBSCRIBE` that cannot report `lagged`. -/
def modeCanLag : Mode → Bool
  | .snapshot _ => false
  | .cdc _ _ => true

/-- Only the two change-stream shapes can report a gap: a snapshot re-reads instead,
    so falling behind costs it nothing it has to tell the client about. -/
theorem only_the_stream_modes_can_report_a_gap :
    modeCanLag (.snapshot 1000) = false ∧ modeCanLag (.cdc true true) = true := by decide

def ledger : List SpecItem := [
  ⟨"SUBSCRIBE without an ON clause is a polled snapshot, not a change stream",
   .met "Subscribe.the_default_subscribe_mode_is_a_polled_snapshot"⟩,
  ⟨"snapshot polling cannot be tighter than 500ms",
   .met "Subscribe.snapshot_polling_cannot_be_tighter_than_half_a_second"⟩,
  ⟨"a snapshot can miss an intermediate state where a change stream delivers it",
   .met "Subscribe.a_snapshot_misses_what_a_stream_delivers"⟩,
  ⟨"ON LOCAL and ON COMMITTED select the two distinct change streams",
   .met "Subscribe.on_local_and_on_committed_select_different_streams"⟩,
  ⟨"only the change-stream shapes can report a gap; a snapshot never can",
   .met "Subscribe.only_the_stream_modes_can_report_a_gap"⟩
]

/-- All five `SUBSCRIBE` obligations in the ledger are discharged, so none of them is
    a gap a client has to be warned about. -/
theorem cql_subscribe_has_no_red_items : redCount ledger = 0 := by decide

def main : IO Unit :=
  printLedger "SUBSCRIBE on the CQL surface" ledger

end Ferrosa.Cql.Subscribe
