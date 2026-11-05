/-
  The audit: where the seam leaks, and what the code still changes on the way in
  =================================================================================

  Three of the six obligations are about the model and are discharged by theorems in
  `Canonical.lean` and `Seam.lean`. The other two — one canonical form every path produces, and
  layout knowledge confined to the codec — are about the *code*, so they are discharged here by
  measurement, and the measurement is recorded as data so a reviewer can check the arithmetic.

  The numbers below are a snapshot, taken at the revision this module was written at, by counting
  occurrences in shipping Rust sources (test modules, `src/tests/` and `*_tests.rs` excluded):

  ```text
  rg -c 'ferrosa_sstable::(types|statistics|marshal)' ferrosa*/src
  rg -c '"Data\.db"|"Partitions\.db"|"Rows\.db"|"Statistics\.db"|"Filter\.db"|"TOC\.txt"|"CompressionInfo\.db"' ferrosa*/src
  ```

  540 references to the codec crate's value and header types from 101 shipping files in 12 crates,
  and 437 literal component filenames in 21 shipping files in 4 crates, while the codec publishes
  those names as constants (`ferrosa-sstable/src/toc.rs:16-37`). That is the seam's honest state: not
  closed, and closed by two mechanical changes rather than by a redesign.

  The fidelity findings are narrower and worse. Three of the four are things the model says must be a
  function of the request and the code makes a function of the writer's clock or of a narrowing cast.
-/
import Ferrosa.Codec.Canonical
import Ferrosa.Codec.Seam
import Ferrosa.Ledger

namespace Ferrosa.Codec

open Ferrosa.Ledger

/-! ## The leak inventory

One row per crate that names the codec's value type or its component files. `values` counts
references to `ferrosa_sstable::types|statistics|marshal`; `layout` counts literal component
filenames. A crate with both is leaking twice: the pipeline's data type is the format's, and the
files the format writes are named in code that is not the format.
-/

/-- What one crate knows about the format that it should not. -/
structure LeakRow where
  /-- The crate. -/
  crate : String
  /-- References to the codec crate's value and header types. -/
  values : Nat
  /-- Literal on-disk component filenames (`"Data.db"` and friends). -/
  layout : Nat
  deriving DecidableEq, Repr

/-- The leak inventory, by crate, at the revision this module was written at. -/
def leaks : List LeakRow := [
  ⟨"ferrosa-storage", 236, 365⟩,
  ⟨"ferrosa-cluster", 161, 43⟩,
  ⟨"ferrosa-cql", 58, 0⟩,
  ⟨"ferrosa-postgres", 27, 0⟩,
  ⟨"ferrosa-graph", 23, 0⟩,
  ⟨"ferrosa", 8, 0⟩,
  ⟨"ferrosa-row-bridge", 7, 0⟩,
  ⟨"ferrosa-sparql", 6, 0⟩,
  ⟨"ferrosa-ctl", 6, 22⟩,
  ⟨"ferrosa-schema", 3, 0⟩,
  ⟨"ferrosa-cdc", 3, 0⟩,
  ⟨"ferrosa-loadgen", 2, 0⟩,
  ⟨"ferrosa-index-builder", 0, 7⟩
]

/-- References to the codec's value type from code that is not the codec. Pinned so the seam's
    state is a number in the document rather than a claim in the prose. -/
def valueLeaks : Nat := (leaks.map (·.values)).sum

/-- Literal component filenames outside the codec. -/
def layoutLeaks : Nat := (leaks.map (·.layout)).sum

/-- **The pipeline's value type is the format's.**  540 references in 101 shipping files, and the
    count is pinned: closing the seam has to move this number, and a reviewer can check it with the
    `rg` line in this module's header. -/
theorem value_leaks_are_five_hundred_and_forty : valueLeaks = 540 := by decide

/-- **The layout is named outside the codec 437 times.**  `ferrosa-sstable` exports `toc::DATA` and
    its siblings; the shipping code writes the literals instead. -/
theorem layout_leaks_are_four_hundred_and_thirty_seven : layoutLeaks = 437 := by decide

/-- Twelve crates hold the format's value type, so a change to it is not a change to one module. -/
theorem twelve_crates_hold_the_value_type :
    (leaks.filter (fun r => r.values != 0)).length = 12 := by decide

/-- Four crates name the format's files, so a format swap edits storage, the coordinator, the CLI
    and the index builder — not the codec alone. -/
theorem four_crates_name_the_files : (leaks.filter (fun r => r.layout != 0)).length = 4 := by decide

/-- The largest single holder, so a reader knows where the work is: `ferrosa-storage`. -/
theorem storage_holds_most_of_the_leak :
    (leaks.filter (fun r => r.values > 200)).map (·.crate) = ["ferrosa-storage"] := by decide

/-! ## The fidelity findings

Four places where the canonical form the code produces is not the canonical form the operator asked
for. Each names the file and line, because a finding without a site is a suspicion.
-/

/-- One fidelity finding: what the rule is, what the code does instead, and where. -/
structure Finding where
  /-- The rule the canonicalisation has to obey. -/
  rule : String
  /-- What the code does instead, with the site. -/
  code : String
  /-- `open` while it stands. -/
  status : String
  deriving Repr

/-- The findings, at the revision this module was written at. -/
def findings : List Finding := [
  ⟨"the deletion time is derived from the write's own timestamp, so two nodes and two retries agree",
   "ferrosa-row-bridge/src/row.rs:815-817 stamps a null's local deletion time from SystemTime::now(), and :826-830 computes a TTL's expiry as now + ttl. Two executions of one statement produce different cells, so a retried write and a replayed one disagree with the original, and cell_supersedes compares that field on the tie path (ferrosa-storage/src/merge.rs:177-190)",
   "open"⟩,
  ⟨"a deletion time is computed the same way on every path",
   "ferrosa-cluster/src/telemetry/writer.rs:131 writes (ts / 1_000_000) as i32 with no clamp, while ferrosa-graph/src/executor/expand.rs:6082 clamps the same quantity into u32 and ferrosa-graph/src/adjacency/reconcile.rs:866 clamps it into u32 via i64. Past 2038 seconds the first wraps negative and the cell reads as already purged",
   "open"⟩,
  ⟨"the writer's view of a cell's metadata field does not depend on its on-disk width",
   "ferrosa-storage/src/flush.rs:123-129 re-derives the Statistics header's representation: it tests DeletionTime.local_deletion_time (u32) against u32::MAX and then casts to i32 to compare with the header field, with the comment naming the width mismatch. That is a format decision made in the flush path",
   "open"⟩,
  ⟨"an artifact records which format wrote it, so a reader can refuse one from another version",
   "ferrosa-sstable/src/reader.rs opens a BTI table from component handles and checks no version; ferrosa-sstable/src/statistics.rs:147-172 carries no version field; ferrosa-sstable/src/toc.rs:16-37 lists filenames only. Dispatch is by which files exist, so a future format that reuses a component name is read as this one",
   "open"⟩,
  ⟨"the value type a stage receives is the canonical one, not the one the front end parsed",
   "two front-end value types reach the storage pipeline: ferrosa-common/src/cql_type/mod.rs:103 (CqlValue) and ferrosa-sql/src/types.rs:40 (Value), both converted at ferrosa-row-bridge/src/row.rs:804 (build_row). 199 direct CellValue constructions in 38 shipping files build cells without it, so the canonicalisation is a convention rather than the only door",
   "open"⟩
]

/-- Five findings, pinned so one cannot be quietly dropped. -/
theorem findings_are_five : findings.length = 5 := by decide

/-- **Every finding is open.**  A finding is closed by fixing the code and moving the row out; until
    then the document says so rather than implying the list is historical. -/
theorem every_finding_is_open : findings.all (fun f => f.status == "open") = true := by decide

/-! ## The obligations

Each of the six obligations appears here, split into the part the model proves and the part the code
owes. A model claim is `.met` and names its theorem; a code claim is red and says what is missing.
Nothing in this table is a projection: `redCount` is five and it is pinned.
-/

def ledger : List SpecItem := [
  ⟨"a tombstone and a zero-length value are different canonical values",
   .met "Canonical.tombstone_is_not_empty_string"⟩,
  ⟨"an expired cell keeps its value, its TTL and its expiry",
   .met "Canonical.expired_is_still_a_value"⟩,
  ⟨"a timestamp at either extreme survives canonicalisation",
   .met "Canonical.timestamp_extremes_are_representable"⟩,
  ⟨"canonicalising a collection drops no entry and invents none",
   .met "Canonical.sortEntries_mem"⟩,
  ⟨"two entry orders of one map canonicalise to the same cell",
   .met "Canonical.map_canon_ignores_entry_order"⟩,
  ⟨"observational equality on the canonical record is structural equality",
   .met "Canonical.obsEq_iff_eq"⟩,
  ⟨"every canonical value round-trips through a fixed-order, terminator-delimited codec",
   .met "Seam.codecA_roundtrip"⟩,
  ⟨"every canonical value round-trips through a length-prefixed, reverse-order codec",
   .met "Seam.codecB_roundtrip"⟩,
  ⟨"a decoder accepts only bytes its own encoder would have written",
   .met "Seam.codecA_accepts_only_encodings"⟩,
  ⟨"a published sequence reads back as what was published, for any codec with the two laws",
   .met "Seam.store_roundtrip"⟩,
  ⟨"a store obligation proved for one codec holds for another without re-derivation",
   .met "Seam.store_fidelity"⟩,
  ⟨"a truncated record is refused before any field is decoded",
   .met "Seam.short_record_is_refused"⟩,
  ⟨"a verified record consumes exactly its declared bytes and leaves the input after it untouched",
   .met "Seam.verified_record_consumes_exactly"⟩,
  ⟨"an artifact from another format is refused rather than misread",
   .met "Seam.another_formats_artifact_is_refused"⟩,
  ⟨"canonicalisation holds one record, not the dataset",
   .met "Seam.canonicalise_is_per_record"⟩,
  ⟨"every path hands the pipeline one canonical value type",
   .failing
     "the pipeline's value type is the codec crate's: 540 references from 101 shipping files in 12 crates, and the type's own module says it is format-specific (ferrosa-sstable/src/types.rs:1-14). Two front-end value types are converted into it at one door (ferrosa-row-bridge/src/row.rs:804) while 199 sites build cells without it"
     "none (implementation item)"⟩,
  ⟨"on-disk layout knowledge is confined to the codec module",
   .failing
     "437 literal component filenames in 21 shipping files across 4 crates, while the codec exports them as constants (ferrosa-sstable/src/toc.rs:16-37); the flush path re-derives the Statistics header's field width and sentinel (ferrosa-storage/src/flush.rs:123-129)"
     "none (implementation item)"⟩,
  ⟨"the canonical form is a function of the request, not of the writer's clock",
   .failing
     "a tombstone's local deletion time and a TTL's expiry are stamped from SystemTime::now() at ferrosa-row-bridge/src/row.rs:815-817 and :826-830, so the same statement executed twice produces different cells and the merged result can depend on which execution landed"
     "none (implementation item)"⟩,
  ⟨"a deletion time is computed the same, and width-safe, on every path",
   .failing
     "ferrosa-cluster/src/telemetry/writer.rs:131 casts a microsecond timestamp to i32 seconds with no clamp while ferrosa-graph/src/executor/expand.rs:6082 and ferrosa-graph/src/adjacency/reconcile.rs:866 clamp the same quantity; past 2038 the unclamped path wraps negative and the cell reads as already purged"
     "none (implementation item)"⟩,
  ⟨"an artifact records which format version wrote it",
   .failing
     "nothing in ferrosa-sstable records a format version: reader.rs opens a table from component handles without a version check, statistics.rs:147-172 has no version field, and toc.rs:16-37 lists filenames. ADR-022 puts a version in the fnf header; until then a reader cannot refuse an artifact from a different writer"
     "none (implementation item)"⟩
]

/-- Fourteen model claims discharged and five code obligations open. The count is pinned so a red
    cannot be dropped from the table, and so closing one has to move the number. -/
theorem audit_has_five_red_items : redCount ledger = 5 := by decide

/-- The model half of the obligations is not where the gaps are: every claim about the canonical form
    and the seam is discharged, and the five reds are all about the code. -/
theorem the_model_half_is_discharged : (ledger.filter isMet).length = 15 := by decide

def main : IO Unit :=
  printLedger "canonical form, codec seam and the audit (ADR-022)" ledger

end Ferrosa.Codec
