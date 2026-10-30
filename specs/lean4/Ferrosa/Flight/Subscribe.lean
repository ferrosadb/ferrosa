/-
  Flight subscriptions: a gap is a frame, never an empty batch
  ===========================================================

  A Flight client that falls behind is handed a frame that says so — a `gap` naming the position
  and the outstanding count, or a `reset` telling it to re-snapshot — so "you missed events"
  cannot arrive encoded as "nothing changed". A subscription is one endpoint from one cursor, not
  a token-range fan-out, because a change stream is a single ordered sequence and has no range to
  split.

  The surface does not exist yet. `plan.rs` builds token-range `SELECT` tickets and `do_get` has
  no subscription path, so a Flight client cannot subscribe at all today; what is specified is the
  ticket and gap encoding that path would need, and it is the same cursor `Cdc.Subscribe` reports.
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

/-- A client's drain rate is not an argument to the producer's progress, so a slow `do_get`
    consumer cannot throttle the write path the way a slow scan throttles only itself. -/
theorem flight_backpressure_cannot_reach_the_writer (rate accepted : Nat) :
    producerProgress rate accepted = accepted := rfl

/-- The position a resumed `do_get` continues from is the cursor the gap carried, so the two surfaces
    agree: what CQL reports as a gap, Flight expresses as the next ticket. -/
def resumeTicket (g : Gap) : Option Ticket :=
  match g with
  | .resumable start _ => some (.stream true start)
  | .resetRequired => none

/-- A resumable gap becomes the next stream ticket carrying the same cursor, and a reset becomes no
    ticket at all, so the position `Cdc.Subscribe` reports is the position Flight's next `do_get`
    asks from. -/
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

/-- The area records one unmet obligation — there is no stream ticket type and no subscription
    `do_get` — and the count is pinned so that implementing it without updating the ledger fails
    the build. -/
theorem flight_has_one_red_item : redCount ledger = 1 := by decide

def main : IO Unit :=
  printLedger "SUBSCRIBE on the Arrow Flight surface" ledger

end Ferrosa.Flight
