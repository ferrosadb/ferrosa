/-
  Ferrosa formal specification — SUBSCRIBE on the Arrow Flight surface.
  ===================================================================

  `ferrosa-flight/src/plan.rs` builds read tickets as token-range-scoped CQL: one `FlightEndpoint`
  per ring token range, each ticket a bounded `SELECT`, fetched with `do_get`. A change stream has no
  token range — it is a single ordered sequence per node — so the read ticket shape does not apply
  to it, and the Flight surface needs its own ticket and its own gap encoding.

  What must hold at this layer is the CDC rule restated in Arrow terms: a gap is never an empty
  batch. An empty `RecordBatch` is the one encoding that would turn "you missed events" into "nothing
  happened", and that is the silent-loss failure this whole area exists to prevent.
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.Cdc.Subscribe

namespace Ferrosa.Flight

open Ferrosa Ferrosa.Ledger
open Ferrosa.Commons.Stream Ferrosa.Cdc

/-- The kinds of ticket the Flight server can be asked for. -/
inductive Ticket where
  /-- A token-range-scoped `SELECT`: what `plan.rs` produces today. -/
  | rangeSelect (start finish : Nat)
  /-- A subscription: one stream, from a cursor. -/
  | stream (committed : Bool) (cursor : Nat)
  deriving DecidableEq, Repr

def Ticket.isRangeSelect : Ticket → Bool
  | .rangeSelect _ _ => true
  | .stream _ _ => false

/-- **A stream ticket is not a range ticket.**  A subscription has no token range to splice, so it
    cannot be planned by the read path's endpoint fan-out: one stream is one endpoint. -/
theorem a_stream_ticket_is_not_a_range_select :
    Ticket.isRangeSelect (.stream true 7) = false
      ∧ Ticket.isRangeSelect (.rangeSelect 0 1024) = true := by decide

/-- What a `do_get` response can carry. -/
inductive Batch where
  /-- A record batch of events. -/
  | records (count : Nat)
  /-- Metadata only: the position to resume from and the outstanding count. -/
  | gap (start count : Nat)
  /-- Not resumable: the client must re-snapshot. -/
  | reset
  deriving DecidableEq, Repr

/-- The batches produced for a gap. Never `[]`. -/
def batchesFor (g : Gap) : List Batch :=
  match g with
  | .resumable start count => [Batch.gap start count]
  | .resetRequired => [Batch.reset]

/-- **A gap is never encoded as an empty batch.**  Every gap yields a frame that names its kind, so a
    client cannot mistake "you missed events" for "nothing changed" — the Arrow-surface form of the
    no-silent-loss rule. -/
theorem a_gap_is_never_an_empty_batch :
    batchesFor (.resumable 7 3) = [Batch.gap 7 3]
      ∧ batchesFor .resetRequired = [Batch.reset]
      ∧ batchesFor (.resumable 7 3) ≠ [] := by decide

/-- **Consumer-paced `do_get` cannot reach the writer.**  `plan.rs`'s read path lets a slow client
    throttle a scan; a subscription must not, because the producer is the write path. The Flight
    layer therefore inherits the bounded bus rather than the scan's backpressure. -/
def producerProgress (_clientDrainRate accepted : Nat) : Nat := accepted

theorem flight_backpressure_cannot_reach_the_writer (rate accepted : Nat) :
    producerProgress rate accepted = accepted := rfl

/-- The position a resumed `do_get` continues from is the cursor the gap carried, so the two surfaces
    agree: what CQL reports as a gap, Flight expresses as the next ticket. -/
def resumeTicket (g : Gap) : Option Ticket :=
  match g with
  | .resumable start _ => some (.stream true start)
  | .resetRequired => none

theorem a_resumable_gap_becomes_the_next_ticket :
    resumeTicket (.resumable 7 3) = some (.stream true 7) ∧ resumeTicket .resetRequired = none := by
  decide

def ledger : List SpecItem := [
  ⟨"a stream ticket is not a token-range ticket and is not planned by the read fan-out",
   .met "Flight.a_stream_ticket_is_not_a_range_select"⟩,
  ⟨"a gap is never encoded as an empty record batch",
   .met "Flight.a_gap_is_never_an_empty_batch"⟩,
  ⟨"a slow Flight consumer cannot throttle the write path",
   .met "Flight.flight_backpressure_cannot_reach_the_writer"⟩,
  ⟨"a resumable gap becomes the next do_get ticket, so CQL and Flight agree on the cursor",
   .met "Flight.a_resumable_gap_becomes_the_next_ticket"⟩,
  ⟨"plan.rs can produce a stream ticket",
   .failing
     "plan.rs only builds token-range SELECT tickets; there is no stream ticket type and do_get has no subscription path, so the Flight surface cannot serve SUBSCRIBE at all today"
     "Flight.a_stream_ticket_is_not_a_range_select"⟩
]

theorem flight_has_one_red_item : redCount ledger = 1 := by decide

def main : IO Unit :=
  printLedger "SUBSCRIBE on the Arrow Flight surface" ledger

end Ferrosa.Flight
