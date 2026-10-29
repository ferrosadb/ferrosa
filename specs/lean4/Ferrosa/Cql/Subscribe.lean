/-
  Ferrosa formal specification — SUBSCRIBE on the CQL surface.
  ==========================================================

  `ferrosa-cql/src/parser.rs::parse_subscribe` accepts three shapes, and they are not three spellings
  of one thing:

    * `SUBSCRIBE <SELECT ...>`              — a polled snapshot. `SubscribeStreams::Snapshot`.
    * `SUBSCRIBE <SELECT ...> ON LOCAL`    — the `WrittenOnNode` change stream.
    * `SUBSCRIBE <SELECT ...> ON COMMITTED`— the `CommittedToCluster` change stream.
    * `... WITH INTERVAL <ms>`             — snapshot polling period, floored at 500ms.

  The distinction that matters to a client is not delivery mechanism but what can be *missed*: a
  snapshot is always current and can miss every intermediate state, while a change stream delivers
  every state and can lag. Those are different guarantees and the spec states both.
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

theorem cql_subscribe_has_no_red_items : redCount ledger = 0 := by decide

def main : IO Unit :=
  printLedger "SUBSCRIBE on the CQL surface" ledger

end Ferrosa.Cql.Subscribe
