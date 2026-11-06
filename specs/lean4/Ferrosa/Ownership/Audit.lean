/-
  The clone audit: where a payload is duplicated, and whether the duplication is allowed
  =======================================================================================

  The machine in `Ferrosa/Ownership/Machine` says what the rules are. This module says where the
  tree stands against them: every site on a fan-out or per-consumer path where a payload-typed value
  is cloned, and which of four things that clone is.

  * `handle` — an `Arc`, a `Bytes`, a borrow, or a single encode shared by every peer. No payload
    bytes are duplicated, so this is the discipline working. `handleMeta` is what it costs.
  * `deep` — the payload is duplicated where one payload and another handle would do. This is the
    violation: it costs a second residence for a value that already has one, which is what the
    ledger's reds name.
  * `cow` — a deep copy that is the sanctioned transform: a fork published as a new version
    (`Arc::make_mut` above reference count one), a read result materialised under a limit, or a
    metadata-to-row conversion. The copy is real and priced; it is not an accidental second
    resident copy of a value that others still refer to under the same version.
  * `bounded` — a slice, a header or a key: the copy is deliberately smaller than the payload and
    its bound is stated in the row.

  The distinction matters because counting the word `clone` would be wrong in both directions: an
  `Arc::clone` is the fix, not the defect, and `Arc::make_mut` contains no `clone` token at all.
  `Ferrosa/Storage/Streaming.lean` and `Ferrosa/Streaming/Audit.lean` cover what these sites do to
  *memory*; this table covers what they do to *residency*, and the two audits meet at the sites that
  are both — `store.rs:7606` copies every in-range partition per scan, which is a bound in the
  streaming audit and a priced copy-on-write here.
-/
import Ferrosa.Ledger
import Ferrosa.Ownership.Machine

namespace Ferrosa.Ownership

open Ferrosa.Ledger

/-! ## The classes

Four classes, and the boundary between the middle two is the whole audit. A `deep` copy and a `cow`
copy are both real duplications of bytes; what separates them is that a `cow` copy is published as a
new version (or materialised deliberately under a stated bound) while a `deep` copy leaves two
residences of the *same* version. The first is what `Arc::make_mut` does when a writer needs its own
copy; the second is what a `clone()` on a fan-out path does.
-/

/-- What a clone of a payload-typed value actually does. -/
inductive CloneKind where
  /-- A handle: `Arc::clone`, `Bytes::clone`, a borrow, or one encode shared by every consumer. -/
  | handle
  /-- A duplicated payload where a handle would do: the violation. -/
  | deep
  /-- The sanctioned transform: a fork published as a new version, or a deliberate materialisation. -/
  | cow
  /-- A slice, header or key whose copy is bounded well below the payload. -/
  | bounded
  deriving BEq, DecidableEq, Repr

/-- One audited site. -/
structure Site where
  /-- Which path it belongs to: `peer send`, `write pipeline`, `snapshot`, `projection`. -/
  layer : String
  /-- The module the row lives in, as the report groups them. -/
  module : String
  /-- What the site does, in one sentence a reviewer can check against the code. -/
  what : String
  /-- Which class the clone is in. -/
  kind : CloneKind
  /-- For a `cow` or `bounded` copy, what keeps it from being a second residence. Empty otherwise. -/
  bound : String
  /-- `file:line`, read at this revision. -/
  site : String

/-- The sites read for this audit. -/
def sites : List Site := [
  -- peer send and replication
  ⟨"peer send", "ferrosa-cluster/src/coordinator/write.rs",
   "one row encode per fan-out, before the replica loop; every remote replica clones the resulting `Bytes`",
   .handle, "", "ferrosa-cluster/src/coordinator/write.rs:344"⟩,
  ⟨"peer send", "ferrosa-cluster/src/coordinator/write.rs",
   "the per-replica work item clones `Option<Bytes>`, which is an `Arc` bump, not a re-encode",
   .handle, "", "ferrosa-cluster/src/coordinator/write.rs:402"⟩,
  ⟨"peer send", "ferrosa-cluster/src/coordinator/write.rs",
   "the NTS fan-out shares the same `Bytes` body with every remote replica",
   .handle, "", "ferrosa-cluster/src/coordinator/write.rs:920"⟩,
  ⟨"peer send", "ferrosa-cluster/src/pair/coordinator.rs",
   "coordinates a pair write by cloning the whole `Mutation` for the local apply while the peer send only borrows it to encode",
   .deep, "", "ferrosa-cluster/src/pair/coordinator.rs:122"⟩,
  ⟨"peer send", "ferrosa-cluster/src/pair/handler.rs",
   "the primary's forward path clones the decoded `Mutation` for the local apply, then encodes the original",
   .deep, "", "ferrosa-cluster/src/pair/handler.rs:47"⟩,
  ⟨"peer send", "ferrosa-cluster/src/pair/coordinator.rs",
   "`apply_locally` takes `&Mutation` and clones each row into `StorageEngine::write`, which takes the row by value",
   .deep, "", "ferrosa-cluster/src/pair/coordinator.rs:153"⟩,
  ⟨"peer send", "ferrosa-net/src/message.rs",
   "internode payload variants carry `Bytes`, so a message envelope shares the encoded payload",
   .handle, "", "ferrosa-net/src/message.rs:104"⟩,
  ⟨"peer send", "ferrosa-cluster/src/streaming/sender.rs",
   "`SstableSendRequest<'a>` borrows its directory, keyspace and table, so the request holds no copy of them",
   .handle, "", "ferrosa-cluster/src/streaming/sender.rs:36"⟩,
  -- write pipeline: ingest to memtable to flush
  ⟨"write pipeline", "ferrosa-storage/src/engine.rs",
   "`StorageEngine::write` appends the commit-log entry from `&row` (the encoder borrows), so the log costs no second row",
   .handle, "", "ferrosa-storage/src/engine.rs:10188"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/engine.rs",
   "with an observer registered, `write` clones the row into a `Mutation` because the memtable takes the original by value",
   .deep, "", "ferrosa-storage/src/engine.rs:10203"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/engine.rs",
   "the batch write clones every row into a `Mutation` for the observer dispatch and then moves the row into the memtable",
   .deep, "", "ferrosa-storage/src/engine.rs:10325"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/engine.rs",
   "the observer drain owns each derived `Mutation` and still clones every row into the target memtable",
   .deep, "", "ferrosa-storage/src/engine.rs:9651"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/engine.rs",
   "the synchronous observer dispatch clones each row of a derived mutation it already owns",
   .deep, "", "ferrosa-storage/src/engine.rs:9698"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/memtable/sharded.rs",
   "`put` copy-on-writes the whole partition when any handle on it is outstanding, which is what a scan holding it does",
   .cow, "one partition per write, while a scan holds that partition", "ferrosa-storage/src/memtable/sharded.rs:202"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/memtable/skiplist.rs",
   "a visit clones the partition deliberately so the memtable keeps reference count one, and a debug assertion pins that the clone does not bump it",
   .cow, "one partition per visit, per scan", "ferrosa-storage/src/memtable/skiplist.rs:293"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/store.rs",
   "a token-range read clones each matched partition into its result, bounded by the caller's limit",
   .bounded, "the caller's `limit`, one partition each", "ferrosa-storage/src/store.rs:7367"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/store.rs",
   "a full-range scan clones every in-range active-memtable partition into a fresh `Arc`, so the memtable stays uniquely owned",
   .cow, "the memtable, times concurrent scans (see `Ferrosa.Streaming.Audit`)", "ferrosa-storage/src/store.rs:7606"⟩,
  ⟨"write pipeline", "ferrosa-storage/src/range_merger.rs",
   "the merge hands the memtable's `Arc<Partition>` through instead of cloning it, one refcount bump",
   .handle, "", "ferrosa-storage/src/range_merger.rs:528"⟩,
  -- snapshot
  ⟨"snapshot", "ferrosa-cluster/src/raft/state_machine.rs",
   "`build_snapshot` clones the whole `RaftState` into `SnapshotData`, which `encode_snapshot` then borrows",
   .deep, "", "ferrosa-cluster/src/raft/state_machine.rs:2541"⟩,
  ⟨"snapshot", "ferrosa-cluster/src/raft/state_machine.rs",
   "`encode_snapshot` clones the bootstrap map a second time for the trailing extension after the state was already cloned",
   .deep, "", "ferrosa-cluster/src/raft/state_machine.rs:248"⟩,
  ⟨"snapshot", "ferrosa-cluster/src/raft/state_machine.rs",
   "the encoded snapshot is cloned into the published slot while the original is still live for the disk write and the returned cursor",
   .deep, "", "ferrosa-cluster/src/raft/state_machine.rs:2567"⟩,
  ⟨"snapshot", "ferrosa-cluster/src/raft/state_machine.rs",
   "a third resident copy of the same bytes is taken for the blocking disk write",
   .deep, "", "ferrosa-cluster/src/raft/state_machine.rs:2571"⟩,
  ⟨"snapshot", "ferrosa-cluster/src/raft/state_machine.rs",
   "`get_current_snapshot` clones the whole `Vec<u8>` out of the slot, so every follower that asks holds its own copy",
   .deep, "", "ferrosa-cluster/src/raft/state_machine.rs:599"⟩,
  ⟨"snapshot", "ferrosa-cluster/src/raft/state_machine.rs",
   "a snapshot builder owns a point-in-time copy of the state, which is what makes it independent of the apply that continues",
   .cow, "the catalogue state, one copy per build", "ferrosa-cluster/src/raft/state_machine.rs:2782"⟩,
  -- projection writes
  ⟨"projection", "ferrosa-cluster/src/system_table_writer.rs",
   "`write_ordered`/`write_at` take the row by value, restamp it under a unique owner and move it into the engine",
   .handle, "", "ferrosa-cluster/src/system_table_writer.rs:169"⟩,
  ⟨"projection", "ferrosa-cluster/src/system_table_writer.rs",
   "the timestamp scan copies the last clustering key of each page as its cursor",
   .bounded, "one clustering key per 32-row page", "ferrosa-cluster/src/system_table_writer.rs:122"⟩
]

/-! ## What the table says

Counts are pinned so a row cannot be relabelled quietly, and every row names a location: a clone
audit whose rows do not name file and line is an opinion.
-/

/-- The layers the table covers, so it can be printed by layer. -/
def layers : List String := ["peer send", "write pipeline", "snapshot", "projection"]

/-- Every row names a `file.rs:line`. -/
theorem every_site_names_a_location :
    sites.all (fun s => (s.site.splitOn ".rs:").length > 1) = true := by native_decide

/-- Every `cow` or `bounded` row says what makes the copy acceptable. -/
theorem every_sanctioned_copy_states_its_bound :
    sites.all (fun s => match s.kind with
      | .cow => !s.bound.isEmpty
      | .bounded => !s.bound.isEmpty
      | _ => true) = true := by native_decide

/-- The layers are the four the report groups by. -/
theorem layers_are_distinct : layers.Nodup := by decide

/-- Every row is in one of the covered layers. -/
theorem every_site_is_in_a_covered_layer :
    sites.all (fun s => layers.contains s.module || layers.contains s.layer) = true := by
  native_decide

/-- **The table is 26 sites.** Pinned so a row cannot be dropped silently. -/
theorem site_count : sites.length = 26 := by decide

/-- Eight rows are handles: the discipline working, and what a fix looks like. -/
theorem handle_sites :
    (sites.filter fun s => s.kind == .handle).length = 8 := by native_decide

/-- **Twelve rows duplicate a payload where a handle would do.** These are the ledger's reds, grouped
    into six obligations: a send, a snapshot served to followers, a snapshot build, a per-write row,
    a batch row, and a drain of an owned mutation. -/
theorem deep_sites :
    (sites.filter fun s => s.kind == .deep).length = 12 := by native_decide

/-- Four deep copies are the sanctioned transform: they publish a new version or materialise a read
    result under a limit. -/
theorem cow_sites :
    (sites.filter fun s => s.kind == .cow).length = 4 := by native_decide

/-- Two copies are bounded well below the payload: a partition per limit, a clustering key per page. -/
theorem bounded_sites :
    (sites.filter fun s => s.kind == .bounded).length = 2 := by native_decide

/-- The four classes partition the table. -/
theorem classes_partition_the_sites :
    (sites.filter fun s => s.kind == .handle).length
      + (sites.filter fun s => s.kind == .deep).length
      + (sites.filter fun s => s.kind == .cow).length
      + (sites.filter fun s => s.kind == .bounded).length = 26 := by native_decide

/-- The snapshot layer, counted per module: one builder copy that is a transform, five duplications. -/
theorem snapshot_layer_counts :
    (sites.filter (fun s => s.layer == "snapshot" && s.kind == .deep)).length = 5
      ∧ (sites.filter (fun s => s.layer == "snapshot" && s.kind == .cow)).length = 1
      ∧ (sites.filter fun s => s.layer == "snapshot").length = 6 := by native_decide

/-- The peer-send layer: three handles, three duplications, one borrow-carrying envelope and one
    borrowing request. -/
theorem peer_send_layer_counts :
    (sites.filter (fun s => s.layer == "peer send" && s.kind == .deep)).length = 3
      ∧ (sites.filter (fun s => s.layer == "peer send" && s.kind == .handle)).length = 5
      ∧ (sites.filter fun s => s.layer == "peer send").length = 8 := by native_decide

/-- The write-pipeline layer: two handles, four duplications, three sanctioned copies, one bounded. -/
theorem write_pipeline_layer_counts :
    (sites.filter (fun s => s.layer == "write pipeline" && s.kind == .deep)).length = 4
      ∧ (sites.filter (fun s => s.layer == "write pipeline" && s.kind == .cow)).length = 3
      ∧ (sites.filter (fun s => s.layer == "write pipeline" && s.kind == .handle)).length = 2
      ∧ (sites.filter fun s => s.layer == "write pipeline").length = 10 := by native_decide

/-- The projection layer: one move and one bounded key copy. Nothing on this path duplicates a
    payload, which is the shape the other paths are missing. -/
theorem projection_layer_counts :
    (sites.filter (fun s => s.layer == "projection" && s.kind == .deep)).length = 0
      ∧ (sites.filter fun s => s.layer == "projection").length = 2 := by native_decide

/-! ## What the duplications cost

Each red names one of these. They are statements about the machine, applied to the shape the code
has: a payload that exists twice is two residences and twice the bytes, and `k` copies are `k` times
the bytes.

These carry `native_decide` nowhere: the first three are arithmetic over the model, the last is an
induction.
-/

/-- Two payloads for one value are two residences of it. -/
theorem two_payloads_are_two_residences :
    keyCount 7 0 [⟨0, 1000, 7, 0, .value, 1, [0]⟩, ⟨1, 1000, 7, 0, .value, 1, [1]⟩] = 2 := by
  decide

/-- And twice the bytes, which is what a duplicated payload costs. -/
theorem two_payloads_are_twice_the_bytes :
    residentBytes [⟨0, 1000, 7, 0, .value, 1, [0]⟩, ⟨1, 1000, 7, 0, .value, 1, [1]⟩] = 2000 + 2 * handleMeta := by
  decide

/-- Three resident copies of one encoded snapshot cost three times its bytes. -/
theorem three_copies_cost_three_times (len : Nat) :
    residentBytes [⟨0, len, 7, 0, .artifact, 1, [0]⟩, ⟨1, len, 7, 0, .artifact, 1, [1]⟩,
      ⟨2, len, 7, 0, .artifact, 1, [2]⟩] = 3 * len + 3 * handleMeta := by
  simp [residentBytes, bytesOf, metaOf, handleMeta]
  omega

/-- `k` copies of a snapshot cost `k` times its bytes: the shape `get_current_snapshot` has when `k`
    followers ask at once. -/
theorem bytesOf_replicate (k : Nat) (x : Payload) : bytesOf (List.replicate k x) = k * x.len := by
  induction k with
  | zero => simp [bytesOf]
  | succ n ih =>
    rw [List.replicate_succ, bytesOf, ih, Nat.succ_mul]
    omega

/-- A per-follower copy of the snapshot is `k` times the snapshot. -/
theorem k_follower_copies_cost_k_times_the_snapshot (k len : Nat) :
    bytesOf (List.replicate k ⟨0, len, 7, 0, .artifact, 1, [0]⟩) = k * len := by
  rw [bytesOf_replicate]

/-- A per-row copy in a loop is one payload per row: `k` rows of `rowLen` bytes are `k * rowLen`. -/
theorem k_row_copies_cost_k_times_the_row (k rowLen : Nat) :
    bytesOf (List.replicate k ⟨0, rowLen, 7, 0, .value, 1, [0]⟩) = k * rowLen := by
  rw [bytesOf_replicate]

/-- **A second copy of one value is a second residence**, which the model refuses as a transition
    (`inv_alloc` needs the key to be free) and the code produces by cloning.
    `two_payloads_are_two_residences` is the count; this is the same fact stated as the cost the
    ledger's reds carry. -/
theorem a_second_copy_is_a_second_residence :
    keyCount 7 0 [⟨0, 1000, 7, 0, .value, 1, [0]⟩, ⟨1, 1000, 7, 0, .value, 1, [1]⟩] = 2
      ∧ residentBytes [⟨0, 1000, 7, 0, .value, 1, [0]⟩, ⟨1, 1000, 7, 0, .value, 1, [1]⟩]
          = 2000 + 2 * handleMeta := by
  exact ⟨two_payloads_are_two_residences, two_payloads_are_twice_the_bytes⟩

end Ferrosa.Ownership
