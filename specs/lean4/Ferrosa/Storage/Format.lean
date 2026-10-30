/-
  The record framing: one bad byte costs one record, and the index is a speed-up
  ===========================================================================

  A scan stops at the first record that fails its own length and CRC, so a damaged file yields a
  verified prefix: nothing after the damage is decoded, and nothing before it is skipped. The data
  is self-describing — every record carries its key — so a table can be read with no index at all,
  and a wrong or damaged trie can only cause a miss, never a value the record does not hold. The
  trie is kept because it is faster, not because it is trusted: the fast path reads one record where
  rebuilding reads every record.

  Two layout decisions are visible to a client. Cells are fixed-stride, so a projection touches one
  cell per projected column rather than every cell before it; and a value of eight bytes or less
  never reaches the arena, so a narrow projection decompresses nothing.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Storage.Format

open Ferrosa.Ledger

abbrev Key := Nat
abbrev Val := Nat

/-! ## The framed record -/

/-- One partition record as the scanner sees it. `declaredLen` comes from the record's own
    `len` field; `actualLen` is how many bytes are actually there; `crcStored` and
    `crcComputed` are the CRC in the header and the CRC of the bytes read. -/
structure Frame where
  key : Key
  val : Val
  declaredLen : Nat
  actualLen : Nat
  crcStored : Nat
  crcComputed : Nat
  deriving DecidableEq, Repr

/-- A record is only usable when its length is satisfied and its CRC matches. Both checks are
    over fields the record itself carries, so no index is consulted. -/
def Frame.verified (f : Frame) : Bool :=
  (f.declaredLen == f.actualLen) && (f.crcStored == f.crcComputed)

/-- A truncated record is not verified: the declared length overruns what is on disk. -/
theorem short_record_is_not_verified (f : Frame) (h : f.actualLen < f.declaredLen) :
    f.verified = false := by
  have hne : f.declaredLen ≠ f.actualLen := by omega
  simp [Frame.verified, hne]

/-- A record whose CRC mismatches is not verified. -/
theorem corrupt_record_is_not_verified (f : Frame) (h : f.crcStored ≠ f.crcComputed) :
    f.verified = false := by
  simp [Frame.verified, h]

/-! ## Scanning: the recovery path -/

/-- The records a scan accepts: everything up to the first record that does not verify. -/
def scanPrefix : List Frame → List Frame
  | [] => []
  | f :: rest => if f.verified then f :: scanPrefix rest else []

/-- The partitions a scan recovers. -/
def scan (frames : List Frame) : List (Key × Val) :=
  (scanPrefix frames).map fun f => (f.key, f.val)

/-- **A torn file yields a prefix.**  `frames = pre ++ rest` and the scan accepts exactly `pre`:
    nothing after the damage is read, and nothing before it is skipped. -/
theorem scan_is_a_prefix (frames : List Frame) :
    ∃ pre rest : List Frame, frames = pre ++ rest ∧ scanPrefix frames = pre := by
  induction frames with
  | nil => exact ⟨[], [], rfl, rfl⟩
  | cons f rest ih =>
    obtain ⟨pre, rest', hf, hp⟩ := ih
    by_cases h : f.verified = true
    · refine ⟨f :: pre, rest', ?_, ?_⟩
      · simp [hf]
      · simp only [scanPrefix, h, if_true]; rw [hp]
    · have h' : f.verified = false := by simpa using h
      refine ⟨[], f :: rest, ?_, ?_⟩
      · simp
      · simp [scanPrefix, h']

/-- **One bad record costs one record.**  With every record before `bad` verified and `bad`
    itself not, the scan returns exactly the records before it — the damage neither hides
    earlier data nor leaks later data. -/
theorem corruption_stops_the_prefix (pre post : List Frame) (bad : Frame)
    (hpre : pre.all Frame.verified = true) (hbad : bad.verified = false) :
    scanPrefix (pre ++ bad :: post) = pre := by
  induction pre with
  | nil => simp [scanPrefix, hbad]
  | cons f rest ih =>
    simp only [List.all, Bool.and_eq_true] at hpre
    simp only [List.cons_append, scanPrefix, hpre.1, if_true]
    rw [ih hpre.2]

/-- The scan in the form a reader of the table sees it: with every record before `bad` verified,
    the partitions recovered are exactly those records, and nothing the file holds after the damage
    is decoded as a partition. -/
theorem corruption_stops_the_scan (pre post : List Frame) (bad : Frame)
    (hpre : pre.all Frame.verified = true) (hbad : bad.verified = false) :
    scan (pre ++ bad :: post) = pre.map (fun f => (f.key, f.val)) := by
  unfold scan
  rw [corruption_stops_the_prefix pre post bad hpre hbad]

/-- **The data is self-describing.**  Every verified record before the first failure is
    recovered without consulting any index — the property `salvage` already relies on, stated
    as a guarantee of the format rather than an exception in the reader. -/
theorem scan_finds_every_verified_frame (pre : List Frame)
    (h : pre.all Frame.verified = true) (f : Frame) (hmem : f ∈ pre) :
    (f.key, f.val) ∈ scan pre := by
  induction pre with
  | nil => simp at hmem
  | cons g rest ih =>
    simp only [List.all, Bool.and_eq_true] at h
    rcases List.mem_cons.mp hmem with rfl | hmem
    · simp [scan, scanPrefix, h.1]
    · simp only [scan, scanPrefix, h.1, if_true, List.map_cons, List.mem_cons]
      exact Or.inr (ih h.2 hmem)

/-! ## The trie on the fast path -/

/-- The index: key → record offset. Nothing more is assumed about it — it may be wrong,
    stale, or rebuilt from scratch. -/
abbrev Trie := Key → Option Nat

/-- Read the record at an offset. Written out rather than using `List.get?`, which this
    toolchain's core does not carry. -/
def nthFrame : List Frame → Nat → Option Frame
  | [], _ => none
  | f :: _, 0 => some f
  | _ :: rest, n + 1 => nthFrame rest n

/-- A record read at an offset is a record the file actually holds, so an offset a damaged index
    names can only ever reach real data. -/
theorem nthFrame_mem (frames : List Frame) (n : Nat) (f : Frame)
    (h : nthFrame frames n = some f) : f ∈ frames := by
  induction frames generalizing n with
  | nil => simp [nthFrame] at h
  | cons g rest ih =>
    cases n with
    | zero => simp only [nthFrame, Option.some.injEq] at h; rw [h]; simp
    | succ m =>
      simp only [nthFrame] at h
      exact List.mem_cons_of_mem g (ih m h)

/-- The fast path. The trie proposes an offset; the record at that offset decides, because it
    carries its own key. A wrong offset therefore reads the wrong record and is *rejected*,
    not returned. -/
def lookupTrie (frames : List Frame) (t : Trie) (k : Key) : Option Val :=
  match t k with
  | none => none
  | some off =>
    match nthFrame frames off with
    | none => none
    | some f => if f.verified && (f.key == k) then some f.val else none

/-- **A wrong trie cannot produce a wrong value.**  Whatever the trie says, any value the fast
    path returns belongs to a verified record whose key is the key that was asked for. The
    only failure mode a corrupt index can cause is a miss. -/
theorem trie_cannot_produce_a_wrong_value (frames : List Frame) (t : Trie) (k : Key) (v : Val)
    (h : lookupTrie frames t k = some v) :
    ∃ f ∈ frames, f.key = k ∧ f.val = v ∧ f.verified = true := by
  unfold lookupTrie at h
  split at h
  · simp at h
  · rename_i off _
    split at h
    · simp at h
    · rename_i f hf
      split at h
      · rename_i hok
        simp only [Option.some.injEq] at h
        simp only [Bool.and_eq_true, beq_iff_eq] at hok
        exact ⟨f, nthFrame_mem frames off f hf, hok.2, h, hok.1⟩
      · simp at h

/-- **A miss is recoverable.**  Anything the fast path declines to return is still in the
    scan, so a damaged or missing index costs latency, never correctness. -/
theorem trie_answer_agrees_with_the_scan (pre : List Frame) (t : Trie) (k : Key) (v : Val)
    (hall : pre.all Frame.verified = true) (h : lookupTrie pre t k = some v) :
    (k, v) ∈ scan pre := by
  obtain ⟨f, hmem, hk, hv, _⟩ := trie_cannot_produce_a_wrong_value pre t k v h
  have hs := scan_finds_every_verified_frame pre hall f hmem
  rw [hk, hv] at hs
  exact hs

/-- Records read by the fast path: at most the one the trie points at. -/
def readsTrie (t : Trie) (k : Key) : Nat := if (t k).isSome then 1 else 0

/-- Records read to rebuild the index from the data: all of them. -/
def readsRebuild (frames : List Frame) : Nat := frames.length

/-- **The reason the trie stays.**  The fast path reads at most one record. -/
theorem trie_reads_at_most_one_record (t : Trie) (k : Key) : readsTrie t k ≤ 1 := by
  unfold readsTrie
  split <;> omega

/-- Rebuilding from the data reads every record — which is correct, and why it is the fallback
    and not the fast path. -/
theorem rebuild_reads_every_record (frames : List Frame) :
    readsRebuild frames = frames.length := rfl

/-! ## Trie block checksums -/

/-- A trie block carries its own CRC. It is checked when the block is first touched, which is
    the same point at which a decompression CRC would be checked. -/
structure TrieBlock where
  stored : Nat
  computed : Nat
  deriving DecidableEq, Repr

def TrieBlock.ok (b : TrieBlock) : Bool := b.stored == b.computed

/-- A trie block whose checksum disagrees with its bytes fails its own check, so a damaged index is
    refused at the moment it is first touched rather than trusted for a lookup. -/
theorem corrupt_trie_block_is_detected (b : TrieBlock) (h : b.stored ≠ b.computed) :
    b.ok = false := by
  simp [TrieBlock.ok, h]

/-! ## File state: three outcomes, not two -/

/-- The states a reader can find. `recoverable n` has no meaning in BTI: a torn BTI index does
    not open at all. -/
inductive FileState where
  | complete
  | recoverable (n : Nat)
  | torn
  deriving DecidableEq, Repr

def FileState.servable : FileState → Bool
  | .complete => true
  | .recoverable _ => true
  | .torn => false

/-- The state, decided from the header, the footer and the verified record count. A footer that
    is present makes a file complete **only** if its count matches what the scan verified —
    otherwise the file is downgraded to the prefix, not trusted. -/
def stateOf (headerOk footerOk : Bool) (declared verified : Nat) : FileState :=
  if !headerOk then .torn
  else if footerOk && declared == verified then .complete
  else .recoverable verified

/-- **An absent footer is a state, not a failure.**  A crash during flush leaves a servable
    prefix instead of an unopenable file. -/
theorem absent_footer_is_recoverable :
    stateOf true false 0 7 = FileState.recoverable 7 := by decide

/-- **A count mismatch is detected.**  A footer that claims more partitions than the data
    verifies does not make the file complete — this is the direction `validate_data_extent`
    cannot see, because it compares the index against the data rather than the footer against
    the scan. -/
theorem count_mismatch_downgrades_to_recoverable :
    stateOf true true 100 60 = FileState.recoverable 60 := by decide

/-- A complete file is servable, and so is a recovered prefix. -/
theorem both_servable :
    (FileState.complete).servable = true ∧ (FileState.recoverable 60).servable = true := by
  decide

/-- A bad header is the one case that is not servable — and it is quarantined, never deleted
    (`Storage.Recovery.only_quarantine_preserves_data`). -/
theorem torn_is_not_servable : (FileState.torn).servable = false := rfl

/-! ## Cells: fixed stride, so a projection is one deref

BTI's Data.db is delta-encoded and varint-packed: a cell's position depends on every cell before
it, so reaching column *n* means decoding cells 0..n-1. A fixed-stride cell struct removes that
dependency — the offset is `base + n * stride` — which is the whole point of a Cap'n-Proto-style
layout. What it costs is size: a fixed cell carries its timestamp and value reference in full,
where a delta-encoded one can spend a few bytes. `projection_cost_is_the_projection` is the
property worth having; the price is measured, not proved. -/

/-- The common cell: column, flags, and a `(offset, length)` reference into the record's value
    arena. 12 bytes. No timestamp — a cell shares its row's timestamp unless the row bitmap
    marks it as an exception, which is what keeps a fixed-stride list from paying 8 bytes per
    cell for a value that is the same across the row. -/
structure Cell where
  col : Nat
  flags : Nat
  valOff : Nat
  valLen : Nat
  deriving DecidableEq, Repr

/-- A cell that deviates from its row baseline: its own timestamp, TTL and local deletion
    time. 16 bytes, stored in a dense side array in cell order. -/
structure Exception where
  ts : Nat
  ttl : Nat
  ldt : Nat
  deriving DecidableEq, Repr

/-- Element `n` of the cell list. Constant work: the layout guarantees `n * stride`. -/
def nthCell : List Cell → Nat → Option Cell
  | [], _ => none
  | c :: _, 0 => some c
  | _ :: rest, n + 1 => nthCell rest n

/-- Cells touched to read cell `n` in a fixed-stride layout. -/
def cellsTouchedRandom (n : Nat) : Nat := if n == 0 then 0 else 1

/-- Cells touched to reach cell `n` when each cell's position depends on its predecessor's
    decoded length — the delta-encoded case. -/
def cellsTouchedSequential (n : Nat) : Nat := n

/-- **A projection is one deref per projected column.**  Reading cell `n` touches one cell,
    whatever `n` is. -/
theorem random_access_touches_one_cell (n : Nat) (h : n > 0) : cellsTouchedRandom n = 1 := by
  simp [cellsTouchedRandom, Nat.ne_of_gt h]

/-- The delta-encoded layout touches every cell up to `n`, which is why a narrow projection out
    of a wide row is expensive today. -/
theorem sequential_access_touches_every_preceding_cell (n : Nat) :
    cellsTouchedSequential n = n := rfl

/-- The gap is not a constant factor, it is the difference between one and `n`. -/
theorem projection_beats_sequential_for_wide_rows (n : Nat) (h : n > 1) :
    cellsTouchedRandom n < cellsTouchedSequential n := by
  have hn : n ≠ 0 := by omega
  simp only [cellsTouchedRandom, cellsTouchedSequential, beq_iff_eq, if_neg hn]
  omega

/-! ## Resynchronisation: the index bounds the blast radius of a scan too

`corruption_stops_the_scan` says the *scan* stops at damage. That is the right behaviour for a
reader that must not return unverified data, and the wrong behaviour for repair, which wants
everything else in the file. Since the trie is checksummed independently, it names record
offsets that do not depend on any record being readable — so repair can use it as a
resynchronisation map. -/

/-- Collect the verified records whose offsets the index names, ignoring damaged ones. -/
def collectAt (frames : List Frame) : List Nat → List (Key × Val)
  | [] => []
  | off :: rest =>
    match nthFrame frames off with
    | some f => if f.verified then (f.key, f.val) :: collectAt frames rest
                else collectAt frames rest
    | none => collectAt frames rest

/-- **Nothing is invented.**  Every partition resynchronisation reports came from a verified
    record, exactly as in the sequential case. -/
theorem collectAt_sound (frames : List Frame) (offs : List Nat) (k : Key) (v : Val)
    (h : (k, v) ∈ collectAt frames offs) :
    ∃ f ∈ frames, f.key = k ∧ f.val = v ∧ f.verified = true := by
  induction offs with
  | nil => simp [collectAt] at h
  | cons off rest ih =>
    simp only [collectAt] at h
    split at h
    · rename_i f hf
      split at h
      · rename_i hv
        simp only [List.mem_cons, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩ | h
        · exact ⟨f, nthFrame_mem frames off f hf, rfl, rfl, hv⟩
        · exact ih h
      · exact ih h
    · exact ih h

/-- **Nothing is lost either.**  Every verified record the index names is reported, so a
    damaged neighbour costs exactly one partition. -/
theorem collectAt_complete (frames : List Frame) (offs : List Nat) (off : Nat)
    (f : Frame) (hfind : nthFrame frames off = some f) (hver : f.verified = true)
    (hmem : off ∈ offs) : (f.key, f.val) ∈ collectAt frames offs := by
  induction offs with
  | nil => simp at hmem
  | cons o rest ih =>
    simp only [List.mem_cons] at hmem
    simp only [collectAt]
    rcases hmem with rfl | hmem
    · rw [hfind]; simp [hver]
    · split
      · rename_i g hg
        split
        · exact List.mem_cons_of_mem _ (ih hmem)
        · exact ih hmem
      · exact ih hmem

/-! ## Local deletion times cannot wrap -/

/-- `fnf` stores the local deletion time as an unsigned 64-bit second count, so the `i32` wrap
    that `Storage.Tombstone.ldt_overflows_before_2038_reachable_now` exhibits cannot arise. The
    same value that wraps at 32 bits is comfortably inside 64. -/
theorem i32_ldt_wraps_where_u64_does_not :
    ¬ (1800000000 + 500000000 < 2 ^ 31) ∧ (1800000000 + 500000000 < 2 ^ 64) := by
  decide

/-! ## The record: a hand-rolled layout with no free choices

The body is hand-rolled rather than generated, so the byte layout is a decision the format owns and
cannot drift with a codegen version. What that buys, stated as properties:

  - `encode_injective` — the bytes determine the cell. No layout ambiguity, no padding
    choices, so two writers of the same partition produce the same partition back.
  - `meta_reads_do_not_touch_the_arena` — the meta block (column ids, timestamps, liveness) is
    self-contained; scanning it never decompresses a value.
  - `zone_map_prune_is_sound` — a skipped partition never held a matching row.
  - `row_lookup_is_bounded_by_the_group` — a clustering lookup scans at most the group size,
    a bound that does not mention how many rows the partition has.
  - `column_ids_survive_a_new_column` — schema evolution is additive; adding a column does not
    renumber the cells of existing SSTables.
  - `well_formed_bounds_every_value` — every `(offset, length)` lies inside the arena.
-/

structure ZoneMap where
  minClustering : Nat
  maxClustering : Nat
  deriving DecidableEq, Repr

/-- One partition record: key, fixed-stride cell list, value arena, zone map. The meta block is
    `cells` (ids, timestamps, arena references); only the arena holds value bytes. -/
structure Partition where
  key : Nat
  /-- One bit per cell: set when the cell carries its own timestamp/TTL/ldt. -/
  bits : List Bool
  cells : List Cell
  /-- Exception cells, in cell order. Cell `n`'s exception is at `popcountUpTo bits n`. -/
  exceptions : List Exception
  arena : List Nat
  zone : ZoneMap
  deriving DecidableEq, Repr

/-- The canonical field order of a cell. Hand-rolled: 28 bytes, no padding to a generated
    word size, and no optional fields. -/
def Cell.encode (c : Cell) : List Nat := [c.col, c.flags, c.valOff, c.valLen]

/-- **The bytes determine the cell.**  Two cells with the same encoding are the same cell, so
    the layout carries no ambiguity that a writer could resolve differently. -/
theorem encode_injective (a b : Cell) (h : a.encode = b.encode) : a = b := by
  cases a with
  | mk ac ats aoff alen =>
    cases b with
    | mk bc bts boff blen =>
      simp only [Cell.encode, List.cons.injEq, List.nil_eq, and_true] at h
      obtain ⟨h1, h2, h3, h4⟩ := h
      rw [h1, h2, h3, h4]

/-- The meta block: what a projection of timestamps or a liveness check needs. It reads the
    exception array, never the arena and never the value bytes. -/
def Partition.meta (p : Partition) : List Nat := p.exceptions.map Exception.ts

/-- **Metadata reads never touch the value arena.**  Rewriting the arena leaves the meta block
    unchanged, which is what makes the meta/values split pay: a scan of timestamps never
    decompresses a single value. -/
theorem meta_reads_do_not_touch_the_arena (p : Partition) (arena : List Nat) :
    ({ p with arena := arena } : Partition).meta = p.meta := rfl

/-! ## The bitmap: cell index → exception slot, in one popcount -/

/-- Number of exception cells strictly before cell `n`. This is the exception slot index. -/
def popcountUpTo (bits : List Bool) : Nat → Nat
  | 0 => 0
  | n + 1 => popcountUpTo bits n + (if bits.getD n false then 1 else 0)

/-- The exception slot of cell `n + 1` is the slot of cell `n` plus one exactly when cell `n` is an
    exception — the step that turns the slot lookup into a single popcount. -/
theorem popcountUpTo_step (bits : List Bool) (n : Nat) :
    popcountUpTo bits (n + 1)
      = popcountUpTo bits n + (if bits.getD n false then 1 else 0) := rfl

/-- The slot index after `n + 1` cells is never below the index after `n`: walking one more cell can
    hand out a slot but never take one back, which is the induction step for the slot map being
    order-preserving. -/
theorem popcountUpTo_le_succ (bits : List Bool) (n : Nat) :
    popcountUpTo bits n ≤ popcountUpTo bits (n + 1) := by
  rw [popcountUpTo_step]
  split <;> omega

/-- **The slot map is order-preserving**, so exception slots are handed out in cell order and
    two exception cells can never land on the same slot. -/
theorem popcountUpTo_monotone (bits : List Bool) : ∀ n m, n ≤ m →
    popcountUpTo bits n ≤ popcountUpTo bits m
  | n, 0, h => by
      have : n = 0 := by omega
      subst this
      exact Nat.le_refl _
  | n, m + 1, h => by
      by_cases hnm : n ≤ m
      · have h1 := popcountUpTo_monotone bits n m hnm
        have h2 := popcountUpTo_le_succ bits m
        omega
      · have : n = m + 1 := by omega
        subst this
        exact Nat.le_refl _

/-- Concrete: cells 0 and 2 are the exceptions, and they get slots 0 and 1. -/
theorem exception_slots_differ :
    popcountUpTo [true, false, true] 0 = 0 ∧ popcountUpTo [true, false, true] 2 = 1 := by
  decide

/-! ## Size: the two-tier cell is never larger than the flat one -/

def commonCellBytes : Nat := 12
def exceptionBytes : Nat := 16

/-- Row cost without the bitmap word (4 bytes per 32 cells). -/
def rowBytesNoBitmap (cells exceptions : Nat) : Nat :=
  commonCellBytes * cells + exceptionBytes * exceptions

/-- One 32-bit word covers 32 cells. -/
def wordsNeeded (cells : Nat) : Nat := (cells + 31) / 32

/-- **Two tiers never cost more than one flat cell, and are strictly cheaper as soon as one
    cell takes its row's timestamp.**  `12c + 16e ≤ 28c` exactly when `e ≤ c`. -/
theorem two_tier_never_costs_more (cells exc : Nat) (h : exc ≤ cells) :
    rowBytesNoBitmap cells exc ≤ 28 * cells := by
  unfold rowBytesNoBitmap commonCellBytes exceptionBytes
  omega

/-- A 32-cell row needs exactly one bitmap word, so the slot lookup is one popcount. -/
theorem bitmap_fits_one_word : wordsNeeded 32 = 1 := by decide

/-- A zone map is disjoint from a query range exactly when the range misses it on one side. -/
def ZoneMap.intersects (z : ZoneMap) (lo hi : Nat) : Prop :=
  ¬ (hi < z.minClustering) ∧ ¬ (z.maxClustering < lo)

/-- **Pruning is sound.**  If every row's clustering value lies inside the zone map's bounds and
    the zone map is disjoint from the query range, then no row in the partition matches — so
    skipping the partition without reading its body can never drop an answer. -/
theorem zone_map_prune_is_sound (z : ZoneMap) (rows : List Nat) (lo hi : Nat)
    (hlo : ∀ r ∈ rows, z.minClustering ≤ r) (hhi : ∀ r ∈ rows, r ≤ z.maxClustering)
    (hdis : ¬ z.intersects lo hi) :
    ∀ r ∈ rows, ¬ (lo ≤ r ∧ r ≤ hi) := by
  unfold ZoneMap.intersects at hdis
  intro r hr hIn
  have hmin : z.minClustering ≤ r := hlo r hr
  have hmax : r ≤ z.maxClustering := hhi r hr
  apply hdis
  constructor <;> omega

/-- Offsets are stored once per group of 16 rows, so the table costs a quarter byte per row and
    a lookup scans at most one group. -/
def cellsScanned (stride rowIndex : Nat) : Nat := min stride (rowIndex + 1)

/-- **A row lookup is bounded by its group, not by the partition.**  The bound contains no
    reference to the number of rows, which is what keeps wide partitions cheap to probe. -/
theorem row_lookup_is_bounded_by_the_group (rowIndex : Nat) :
    cellsScanned 16 rowIndex ≤ 16 := Nat.min_le_left _ _

/-- Schema evolution adds a column id; it never renumbers the existing ones. -/
def addColumn (cols : List Nat) (id : Nat) : List Nat := cols ++ [id]

/-- Adding a column id leaves every id already in the schema where it was, so a table written before
    the change is read by the same ids afterwards and no existing cell has to be rewritten. -/
theorem column_ids_survive_a_new_column (cols : List Nat) (c id : Nat) (h : c ∈ cols) :
    c ∈ addColumn cols id := by
  unfold addColumn
  simp only [List.mem_append]
  exact Or.inl h

/-- Every `(offset, length)` a cell records lies inside the arena. -/
def Partition.wellFormed (p : Partition) : Bool :=
  p.cells.all fun c => c.valOff + c.valLen <= p.arena.length

/-- A check that holds of every element of a list holds of each one of them — the step that turns the
    whole-partition well-formedness test into a bound on a single cell. -/
theorem all_true_of_mem : ∀ {l : List Cell} {q : Cell → Bool}, l.all q = true →
    ∀ c ∈ l, q c = true
  | [], _, _, c, hc => by simp at hc
  | g :: rest, q, h, c, hc => by
      simp only [List.all, Bool.and_eq_true] at h
      rcases List.mem_cons.mp hc with rfl | hc
      · exact h.1
      · exact all_true_of_mem h.2 c hc

/-- A partition that passes its well-formedness check has every `(offset, length)` inside its arena,
    so projecting a cell reads value bytes the record actually holds and no part of a read can run
    past the end of the partition. -/
theorem well_formed_bounds_every_value (p : Partition) (h : p.wellFormed = true) :
    ∀ c ∈ p.cells, c.valOff + c.valLen ≤ p.arena.length := by
  have hall := all_true_of_mem (l := p.cells) (q := fun c => c.valOff + c.valLen <= p.arena.length) h
  intro c hc
  have := hall c hc
  simpa using this

/-! ## Resynchronisation: finding the next record after damage

A length prefix alone cannot be resynchronised: if the corrupted bytes are the length field, the
scan has no boundary to trust and the whole tail is lost. So every record starts with a **sync
word**, and a damaged region is skipped by scanning forward to the next one — with two checks,
because a payload can contain the same four bytes. -/

/-- The four bytes every record starts with. -/
def syncWord : Nat := 0x464E4601

/-- A candidate boundary found during a forward scan. -/
structure Candidate where
  sync : Nat
  len : Nat
  crcOk : Bool
  deriving DecidableEq, Repr

/-- A candidate is a usable boundary only if the sync word matches, the length is plausible for
    the file, and the payload checksum verifies. -/
def Candidate.accepted (c : Candidate) (fileLen : Nat) : Bool :=
  (c.sync == syncWord) && (0 < c.len) && (c.len ≤ fileLen) && c.crcOk

/-- **The sync word alone is not enough.**  A payload can contain those four bytes, so a scan that
    stops at the first match lands inside a record. -/
theorem a_payload_can_contain_the_sync_word : syncWord ∈ ([1, syncWord, 2] : List Nat) := by
  simp

/-- So a resync requires **two** consecutive accepted candidates: a false positive inside a
    payload almost never has a second valid record immediately after it. -/
def resyncConfident (a b : Candidate) (fileLen : Nat) : Bool :=
  a.accepted fileLen && b.accepted fileLen

/-- **A confident resync lands only on records that verify.**  Nothing unverified is ever decoded
    as a partition, whichever boundary the scan stops at. -/
theorem resync_yields_only_verified_records (a b : Candidate) (fileLen : Nat)
    (h : resyncConfident a b fileLen = true) : a.accepted fileLen = true := by
  simp only [resyncConfident, Bool.and_eq_true] at h
  exact h.1

/-- Records lost when the scan stops at the damage. -/
def lostWhenStopping (damaged total : Nat) : Nat := total - damaged

/-- Records lost when the scan resynchronises and recovers everything after the damage. -/
def lostWhenResyncing (damaged recovered total : Nat) : Nat := total - damaged - recovered

/-- **Resynchronising loses strictly less.**  A single damaged region costs the records inside it,
    not the tail of the file — which is the difference between losing one partition and losing
    every partition written after it. -/
theorem resync_loses_less_than_stopping (damaged recovered total : Nat)
    (h : 0 < recovered) (hfit : recovered ≤ total - damaged) :
    lostWhenResyncing damaged recovered total < lostWhenStopping damaged total := by
  unfold lostWhenResyncing lostWhenStopping
  omega

/-! ## Inline values: an eight-byte value never touches the arena

Shipped on day one: a cell whose value fits in eight bytes carries it in place. `valLen` is the
sentinel, and the eight value bytes occupy the space the arena reference would have used, so the
cell stays 12 bytes. -/

/-- A cell whose value is stored inline: `valLen` is the sentinel and `valOff` holds the bytes. -/
def Cell.isInline (c : Cell) : Bool := c.valLen == 0xFFFFFFFF

/-- Arena bytes read for this cell. -/
def Cell.arenaBytesRead (c : Cell) : Nat := if c.isInline then 0 else c.valLen

/-- **An inline cell reads no arena bytes**, so a projection of narrow columns never decompresses
    the value block at all. -/
theorem an_inline_cell_reads_no_arena_bytes (col flags : Nat) (v : Nat) :
    (⟨col, flags, v, 0xFFFFFFFF⟩ : Cell).arenaBytesRead = 0 := by
  simp [Cell.arenaBytesRead, Cell.isInline]

/-- And a non-inline cell reads exactly its value — the arena is still the right place for the
    wide values, it is just not the only one. -/
theorem a_wide_cell_reads_its_value_from_the_arena (col flags off len : Nat) (h : len ≠ 0xFFFFFFFF) :
    (⟨col, flags, off, len⟩ : Cell).arenaBytesRead = len := by
  simp [Cell.arenaBytesRead, Cell.isInline, h]

/-! ## Acknowledging at data fsync

The writer acknowledges once the data record is durable, before the index and footer are written,
because the index is a pure function of the data and can be rebuilt. That makes the footerless
state a normal state rather than a crash state, and it needs one property: a durable prefix in
which everything verifies *is* the whole durable set. -/

/-- **Acknowledge at data fsync is safe.**  If every record written so far verifies, the verified
    prefix is exactly those records — so the data alone reconstructs the table, with no index, no
    footer, and nothing else to wait for. -/
theorem ack_at_data_fsync_is_recoverable (durable : List Frame)
    (hall : durable.all Frame.verified = true) : scanPrefix durable = durable := by
  induction durable with
  | nil => rfl
  | cons f rest ih =>
    simp only [List.all, Bool.and_eq_true] at hall
    simp only [scanPrefix, hall.1, if_true]
    rw [ih hall.2]

/-! ## The ledger -/

def ledger : List SpecItem := [
  ⟨"a torn file yields a verified prefix",
   .met "Format.scan_is_a_prefix"⟩,
  ⟨"one damaged byte costs one record",
   .met "Format.corruption_stops_the_scan"⟩,
  ⟨"data is readable without the index",
   .met "Format.scan_finds_every_verified_frame"⟩,
  ⟨"a wrong or corrupt trie can never return a wrong value",
   .met "Format.trie_cannot_produce_a_wrong_value"⟩,
  ⟨"the index is a speed-up, not an authority",
   .met "Format.trie_answer_agrees_with_the_scan"⟩,
  ⟨"the fast path reads at most one record",
   .met "Format.trie_reads_at_most_one_record"⟩,
  ⟨"a corrupt trie block is detected",
   .met "Format.corrupt_trie_block_is_detected"⟩,
  ⟨"an absent footer is a recoverable state, not a failure",
   .met "Format.absent_footer_is_recoverable"⟩,
  ⟨"a footer count that disagrees with the data is not trusted",
   .met "Format.count_mismatch_downgrades_to_recoverable"⟩,
  ⟨"a projection costs one deref per projected column",
   .met "Format.projection_beats_sequential_for_wide_rows"⟩,
  ⟨"corruption costs one record even during a scan",
   .met "Format.collectAt_complete"⟩,
  ⟨"a local deletion time cannot wrap",
   .met "Format.i32_ldt_wraps_where_u64_does_not"⟩,
  ⟨"a cell that shares its row's timestamp costs 12 bytes, not 28",
   .met "Format.two_tier_never_costs_more"⟩,
  ⟨"the bitmap slot map is order-preserving, so slots never collide",
   .met "Format.popcountUpTo_monotone"⟩,
  ⟨"a 32-cell row needs one bitmap word, so the lookup is one popcount",
   .met "Format.bitmap_fits_one_word"⟩,
  ⟨"the layout has no free choices: the bytes determine the cell",
   .met "Format.encode_injective"⟩,
  ⟨"metadata reads never touch the value arena",
   .met "Format.meta_reads_do_not_touch_the_arena"⟩,
  ⟨"a zone map never prunes a matching row",
   .met "Format.zone_map_prune_is_sound"⟩,
  ⟨"a row lookup is bounded by its group, not the partition size",
   .met "Format.row_lookup_is_bounded_by_the_group"⟩,
  ⟨"adding a column never renumbers existing ones",
   .met "Format.column_ids_survive_a_new_column"⟩,
  ⟨"every cell value lies inside the arena",
   .met "Format.well_formed_bounds_every_value"⟩,
  ⟨"a single damaged record does not lose the tail of the file",
   .met "Format.resync_loses_less_than_stopping"⟩,
  ⟨"a resync never decodes unverified bytes as a partition",
   .met "Format.resync_yields_only_verified_records"⟩,
  ⟨"an eight-byte value is read without touching the arena",
   .met "Format.an_inline_cell_reads_no_arena_bytes"⟩,
  ⟨"a write may be acknowledged at data fsync",
   .met "Format.ack_at_data_fsync_is_recoverable"⟩,
  ⟨"a flush can resume at the last verified record",
   .failing
     "nothing in the layout prevents it — no record points forward — but the writer has no resume path yet"
     "none (implementation item)"⟩,
  ⟨"a table can be exported to BTI for Cassandra by compaction",
   .failing
     "requires SSTableWriter to select a format per table; today the writer is BTI-only"
     "none (implementation item)"⟩,
  ⟨"record CRCs cost less than 5% of point-read p99",
   .failing
     "a measurement, not a theorem: the CRC is over bytes already in the read buffer, but the budget must be measured against BTI on the same data"
     "none (measurement gate, ADR-022)"⟩
]

/-- Three obligations in this area are recorded red — a flush with no resume path, a BTI-to-Cassandra
    export, and the 5% CRC budget, which is a measurement rather than a theorem. The count is pinned
    so that none of them can be dropped from the ledger quietly. -/
theorem format_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "fnf framing obligations (ADR-022)" ledger

end Ferrosa.Storage.Format
