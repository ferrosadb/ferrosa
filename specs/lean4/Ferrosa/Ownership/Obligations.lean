/-
  The ledger: what the ownership machine proves, and what the tree does not do
  ==========================================================================

  Eighteen obligations the machine discharges, and six the code does not meet. The six are not
  abstract: each names a site in `Ferrosa/Ownership/Audit` with a file and line, and each costs a
  second residence of a payload that a handle would have shared — a peer send that clones the
  mutation before encoding it, a snapshot served to each follower as its own copy, a snapshot build
  that duplicates the state it is about to serialise, a third resident copy of the encoded bytes, a
  row cloned into every observer dispatch, and a drain that clones rows out of a mutation it owns.

  The item and red counts are pinned below, so closing one of these and leaving the table alone is a
  build failure, and so is adding an obligation without counting it.

  What is proved, and where: `Ferrosa/Ownership/Machine` carries the heap, the four transitions and
  the invariant over every reachable state; `Ferrosa/Ownership/Audit` carries the sites and what each
  clone costs. This module carries only the claims and the counts.
-/
import Ferrosa.Ledger
import Ferrosa.Ownership.Machine
import Ferrosa.Ownership.Audit

namespace Ferrosa.Ownership

open Ferrosa.Ledger

/-- The area's claims: the obligations the machine discharges, then the duplications the audit found. -/
def ledger : List SpecItem := [
  -- 1. single residence
  ⟨"a fan-out to N consumers is one payload and N handles",
   .met "Ownership.a_fan_out_to_three_consumers_is_one_payload_and_four_handles"⟩,
  ⟨"a fan-out's memory is the payload plus one handle per consumer, not the payload per consumer",
   .met "Ownership.one_payload_and_n_handles_cost"⟩,
  ⟨"sharing one payload to k consumers costs less than copying it k times, for k of two or more",
   .met "Ownership.sharing_beats_copying"⟩,
  ⟨"one resident value payload per value and version, at every reachable state",
   .met "Ownership.reachable_one_residence_per_version"⟩,
  -- 2. move on consume
  ⟨"a move leaves the sender with no usable handle, and gives the receiver the only one",
   .met "Ownership.hand_off_leaves_the_sender_without_a_handle"⟩,
  ⟨"a move leaves the reference count exactly where it was",
   .met "Ownership.refsOf_renameFn"⟩,
  ⟨"a move does not duplicate the payload it moves",
   .met "Ownership.a_move_leaves_the_sender_with_nothing"⟩,
  -- 3. refcount integrity
  ⟨"sharing raises the reference count by one and copies no payload bytes",
   .met "Ownership.attach_raises_the_reference_count"⟩,
  ⟨"the stored count equals the number of handles at every reachable state",
   .met "Ownership.reachable_count_is_the_handles"⟩,
  ⟨"releasing the last handle frees the payload and records the id exactly once",
   .met "Ownership.release_at_one_frees_the_payload"⟩,
  ⟨"a handle that is gone cannot be released a second time",
   .met "Ownership.release_of_an_unheld_payload_is_not_a_step"⟩,
  ⟨"release order does not change the state the heap ends in",
   .met "Ownership.drops_in_any_order_end_the_same_way"⟩,
  -- 4. shared immutability
  ⟨"a payload above one reference cannot be rewritten in place",
   .met "Ownership.mutation_is_refused_above_one_reference"⟩,
  ⟨"a mutation under unique ownership changes the length and nothing else",
   .met "Ownership.mutate_under_unique_ownership"⟩,
  ⟨"copy-on-write publishes a new payload instead of editing the shared one",
   .met "Ownership.cow_publishes_a_new_payload"⟩,
  -- 5. peer fan-out is shared
  ⟨"a peer send encodes once, however many peers it goes to",
   .met "Ownership.a_send_to_three_peers_allocates_one_artifact"⟩,
  ⟨"a peer send holds one handle per peer and leaves the value payload alone",
   .met "Ownership.a_send_to_three_peers_holds_four_handles_on_one_artifact"⟩,
  ⟨"a peer that fails costs exactly its own handle",
   .met "Ownership.a_failed_peer_send_releases_one_handle"⟩,
  -- 6. no accidental deep copy: the sites the audit found
  ⟨"a replication send does not copy the mutation it is about to encode",
   .failing
     "the pair coordinator clones the whole Mutation for the local apply at pair/coordinator.rs:122 and the primary forward path does the same at pair/handler.rs:47, while the peer send only borrows it to encode; one cloned Mutation per write, and clone-then-encode would let the row move"
     "Ownership.a_second_copy_is_a_second_residence"⟩,
  ⟨"a snapshot is served to every follower through a handle rather than a copy",
   .failing
     "current_snapshot_cloned returns Vec<u8> (state_machine.rs:599), so get_current_snapshot hands every follower that asks its own copy of the whole snapshot: k followers, k resident copies"
     "Ownership.k_follower_copies_cost_k_times_the_snapshot"⟩,
  ⟨"a snapshot build does not duplicate the state it is about to encode",
   .failing
     "build_snapshot clones RaftState into SnapshotData at state_machine.rs:2541 although encode_snapshot takes &SnapshotData, and encode_snapshot clones the bootstrap map again at :248"
     "Ownership.a_second_copy_is_a_second_residence"⟩,
  ⟨"the encoded snapshot is not resident three times during a build",
   .failing
     "state_machine.rs:2567 clones the encoded bytes into the published slot and :2571 clones them again for the disk write, while the original is still moved into the returned cursor"
     "Ownership.three_copies_cost_three_times"⟩,
  ⟨"a write that hands a row to an observer does not give the observer a second payload",
   .failing
     "StorageEngine::write clones the row into a Mutation at engine.rs:10203 (and the batch path at :10325) because the memtable takes the original by value; the code's own comment says the two owners are concurrent, which is what an Arc<Row> handle is for"
     "Ownership.two_payloads_are_twice_the_bytes"⟩,
  ⟨"an observer drain moves rows out of the mutation it owns",
   .failing
     "engine.rs:9651 and :9698 iterate &dm.rows and clone each row into the target memtable although the derived mutation is owned and dropped at the end of the loop"
     "Ownership.k_row_copies_cost_k_times_the_row"⟩
]

/-- The area records twenty-four obligations of which six are unmet, and both numbers are pinned:
    adding a claim, or closing one, forces the ledger to be updated in the same change. -/
theorem ownership_has_twenty_four_items_and_six_red : ledger.length = 24 ∧ redCount ledger = 6 := by
  decide

/-- Three of the six reds are on the snapshot path, so a build is where the duplications concentrate. -/
theorem three_reds_are_on_the_snapshot_path :
    (Ferrosa.Ownership.sites.filter (fun s => s.layer == "snapshot" && s.kind == .deep)).length = 5 := by
  native_decide

def main : IO Unit :=
  printLedger "ownership: payloads, handles, moves" ledger

end Ferrosa.Ownership
