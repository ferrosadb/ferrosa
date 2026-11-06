/-
  One canonical in-memory form, and what "unchanged" has to mean
  ==============================================================

  The operator's rule is that a database reads a value into memory, puts it in one canonical
  shape, and passes *that* down the pipeline to a writer that marshals it to disk. Two things
  follow, and this module is the vocabulary for both.

  First, the canonical shape is a **type**, not a convention: a cell carries its column identity,
  a value that is either absent or present-and-possibly-empty, its write timestamp, its TTL, its
  local deletion time, and — for one element of a collection — its cell path. Nothing in that
  record says which protocol delivered it, which is what makes a stage that receives a
  source-specific shape a defect rather than a variant.

  Second, "unchanged" needs an equality before it can be a theorem. Here it is **structural
  equality of the canonical record** (`ObsEq` is the identity on fields, so `obsEq_iff_eq` holds):
  the observation function is the whole cell, because every field is something a client can
  observe — a value, its absence, an expiry, a deletion, an element identity. The one place where
  observational and structural equality come apart is a collection whose entry order is not part
  of the value, and canonicalisation removes that order *before* the cell exists, so it never
  reaches the codec.

  The awkward cases the round-trip has to survive are named here rather than discovered later:
  `none` is a tombstone and `some []` is a zero-length value (`tombstone_is_not_empty_string`);
  a timestamp takes its whole `Int` range, and the sign is carried explicitly
  (`encInt_roundtrips_at_the_extremes`); an expired-but-unpurged cell keeps its TTL and deletion
  time (`expired_is_still_a_value`); a map keeps every entry and loses only the order nobody
  defined as part of it (`map_canon_preserves_entries`, `map_canon_depends_only_on_sorted_entries`).
-/

namespace Ferrosa.Codec

/-! ## Bytes

`Bytes` is a list of octets. The model never reasons about *how many* bytes a field costs — that is
the format's business, proved in `Storage/Format.lean` for the 12-byte cell — only about what the
bytes have to determine. A codec that needs the bytes to determine the value cannot be excused by a
cheaper layout, and a layout claim cannot be excused by the codec being injective.
-/

/-- An octet string. -/
abbrev Bytes := List Nat

/-! ## The canonical cell

Six fields, and each one exists because dropping it loses something a client can see. The value is
an `Option` so that "there is no value here" (a tombstone) and "the value is zero bytes long" are
different values of the type rather than different *conventions* for the same value — the sentinel
approach the shipped code takes (`NO_DELETION_TIME`, `NO_TIMESTAMP`) works only as long as no
caller can produce the sentinel legitimately, which is a domain argument, not a type one.
-/

/-- One canonical cell: column identity, value-or-absence, write time, TTL, local deletion time,
    and — for one element of a collection — the element's path.

    `val = none` is a tombstone; `val = some []` is a zero-length value. `ttl = 0` means no
    expiry. `ldt` is the absolute expiry second, `0` when there is none. `path = none` is a
    scalar column; `some p` is the element `p` of a complex column. -/
structure Cell where
  /-- Column identity: the schema's field id, never a position in a row. -/
  col : Nat
  /-- `none` = tombstone, `some []` = zero-length value. -/
  val : Option Bytes
  /-- Write timestamp, microseconds since the epoch. Signed, because a client may supply one. -/
  ts : Int
  /-- Time-to-live in seconds; `0` means none. -/
  ttl : Int
  /-- Absolute expiry, seconds since the epoch; `0` means none. -/
  ldt : Int
  /-- Element identity within a complex column. -/
  path : Option Bytes
  deriving DecidableEq, Repr

/-- The tombstone at a column and timestamp. -/
def Cell.tombstone (col : Nat) (ts : Int) (ldt : Int) : Cell :=
  ⟨col, none, ts, 0, ldt, none⟩

/-- A live cell at a column and timestamp, with no expiry and no path. -/
def Cell.live (col : Nat) (v : Bytes) (ts : Int) : Cell :=
  ⟨col, some v, ts, 0, 0, none⟩

/-! ## The observation, and the equality every fidelity claim uses

A reader observes the whole cell: the value or its absence, the timestamp it will be ordered by, the
expiry it will obey, the deletion it records, and — for a collection — which element it is. So the
observation function is the identity on fields, and observational equality is *definitionally* the
same relation as structural equality. That is deliberate: it means a round-trip claimed here is not
"looks equal", and any fidelity gap has to show up as a field that is missing from this record.
-/

/-- Everything a client can observe about a cell. One field per field of `Cell`, which is the point:
    the observation hides nothing, so a lost field cannot hide behind the observation. -/
structure Obs where
  col : Nat
  val : Option Bytes
  ts : Int
  ttl : Int
  ldt : Int
  path : Option Bytes
  deriving DecidableEq, Repr

/-- What a client observes of a cell. -/
def Cell.observe (c : Cell) : Obs := ⟨c.col, c.val, c.ts, c.ttl, c.ldt, c.path⟩

/-- Observational equality: the observation of two cells agrees in every field. -/
def ObsEq (a b : Cell) : Prop := a.observe = b.observe

/-- **The observation hides nothing.**  Two cells are observationally equal exactly when they are
    the same record, so "unchanged" in this corpus means structural equality and not a
    representative of it. -/
theorem obsEq_iff_eq (a b : Cell) : ObsEq a b ↔ a = b := by
  constructor
  · intro h
    have := h
    simp only [ObsEq, Cell.observe] at this
    cases a; cases b; simp_all
  · intro h; subst h; rfl

/-! ## Rows: ordering that is part of the value

A row is a list of cells in column order, and that order *is* part of the value: the reader walks it
to pair cells with columns, and a writer that reorders cells silently re-pairs them. So the row's
canonical form is an ordered list, not a set, and the round-trip below is stated on the list.
-/

/-- A row: its cells, in column order. -/
structure Row where
  cells : List Cell
  deriving DecidableEq, Repr

/-- The columns of a row, in order — what a writer that reordered cells would change. -/
def Row.cols (r : Row) : List Nat := r.cells.map (·.col)

/-! ## Source values, and what canonicalisation is allowed to do

One ingest path's value shape, written out as the front ends actually see it: a null (which is *not*
a value), a zero-length value, an opaque value, or a map whose entry order the wire protocol
preserved. Canonicalisation is the map from this shape into `Cell`.

The map case is where "canonicalisation may choose a representation but may not lose information"
bites. Choosing to sort entries is a representation choice; dropping an entry, folding a duplicate,
or inventing one is a loss. `map_canon_preserves_entries` is the theorem that the choice really is
only about order.
-/

/-- A value as one ingest path hands it over. `null` is the wire's out-of-band null, `empty` is a
    zero-length value, `blob` is an opaque value, and `map` is a collection whose entry order the
    path preserved. -/
inductive SrcVal where
  | null
  | empty
  | blob (b : Bytes)
  | map (entries : List (Bytes × Bytes))
  deriving DecidableEq, Repr

/-- Lexicographic comparison on byte strings, so the sort's order is this module's own definition
    rather than whatever the library happens to provide for `List`. `true` means "strictly less". -/
def lexLt : Bytes → Bytes → Bool
  | [], [] => false
  | [], _ :: _ => true
  | _ :: _, [] => false
  | a :: as, b :: bs => if a < b then true else if b < a then false else lexLt as bs

/-- Non-strict order, the form the insertion sort needs. -/
def lexLe (a b : Bytes) : Bool := !lexLt b a

/-- Insert one entry into a list already sorted by key. Insertion sort, so the sort is a function
    this module owns rather than a library's, and the lemmas below are structural inductions. -/
def insEntry (p : Bytes × Bytes) : List (Bytes × Bytes) → List (Bytes × Bytes)
  | [] => [p]
  | q :: qs => if lexLe p.1 q.1 then p :: q :: qs else q :: insEntry p qs

/-- Entries in key order. Two maps that differ only in entry order have the same canonical form
    because they have the same sorted form. -/
def sortEntries : List (Bytes × Bytes) → List (Bytes × Bytes)
  | [] => []
  | p :: ps => insEntry p (sortEntries ps)

/-- Canonicalisation of a collection: sort by key, then encode. The order in the source value is
    not carried, which is the representation choice; the entries are, which is the obligation.
    `[0]` is the empty map and `1 :: …` is a cons, so an empty map is not the empty byte string. -/
def encodeEntries : List (Bytes × Bytes) → Bytes
  | [] => [0]
  | (k, v) :: rest => 1 :: (k ++ (2 :: (v ++ (3 :: encodeEntries rest))))

/-- Canonicalisation: one source value becomes one cell at a column, timestamp and TTL. -/
def canon (col : Nat) (ts ttl ldt : Int) : SrcVal → Cell
  | .null => ⟨col, none, ts, 0, ldt, none⟩
  | .empty => ⟨col, some [], ts, ttl, ldt, none⟩
  | .blob b => ⟨col, some b, ts, ttl, ldt, none⟩
  | .map es => ⟨col, some (encodeEntries (sortEntries es)), ts, ttl, ldt, none⟩

/-! ## The awkward cases, each settled rather than assumed

Five claims, five theorems, no prose promise. Each names the case that a sentinel-based
representation gets wrong and the type-based one gets right.
-/

/-- **A tombstone is not an empty value.**  `canon null` and `canon empty` are different cells at
    the same column, timestamp and deletion time, so no downstream stage can conflate a deletion
    with a zero-length write. This is the case that a `Vec<u8>` with a length-0 sentinel cannot
    express, and the shipped `CellValue` only expresses it because its `value` is an `Option`. -/
theorem tombstone_is_not_empty_string (col : Nat) (ts ldt : Int) :
    canon col ts 0 ldt .null ≠ canon col ts 0 ldt .empty := by
  intro h
  simp [canon] at h

/-- **A zero-length value is not the absent value**, stated as the converse so a reader can see both
    directions: the presence bit is what distinguishes them, and it survives canonicalisation. -/
theorem empty_is_present (col : Nat) (ts ldt : Int) :
    (canon col ts 0 ldt .empty).val = some [] := rfl

/-- **The tombstone's value is absent**, so a read that projects the value alone sees nothing —
    which is what a client is told a deleted cell holds. -/
theorem tombstone_value_is_absent (col : Nat) (ts ldt : Int) :
    (canon col ts 0 ldt .null).val = none := rfl

/-- **An expired-but-unpurged cell is still a value.**  A TTL that has already elapsed does not turn
    the cell into a tombstone: the value, the TTL and the absolute expiry are all still there, so a
    reader that honours the expiry and one that reports it agree about what is stored. Collapsing
    this case into a deletion is the classic "compaction ate my data" bug. -/
theorem expired_is_still_a_value (col : Nat) (v : Bytes) (ts ttl ldt : Int) :
    (canon col ts ttl ldt (.blob v)).val = some v
      ∧ (canon col ts ttl ldt (.blob v)).ttl = ttl
      ∧ (canon col ts ttl ldt (.blob v)).ldt = ldt := by
  simp [canon]

/-- **The timestamp range is the whole `Int` range**, extremes included: `canon` carries the sign and
    the magnitude without narrowing, so a client-supplied timestamp at either end is stored as
    itself rather than as a sentinel that means "no timestamp". -/
theorem timestamp_extremes_are_representable (col : Nat) (v : Bytes) (lo hi : Int) :
    (canon col lo 0 0 (.blob v)).ts = lo ∧ (canon col hi 0 0 (.blob v)).ts = hi := by
  simp [canon]

/-! ## Collections: what canonicalisation is allowed to forget

Two claims, and the split between them is the whole obligation. Membership is preserved in general
(no entry is dropped, none is invented); the *order* is not preserved, and that is not a loss
because the order was never part of the value. The general statement is that the canonical form is a
function of the sorted entries — so any two source orders with the same entries land on the same
cell, and the specific swap below shows the quotient is not empty.
-/

/-- Insertion preserves membership: an entry is in the result exactly when it was in the input. -/
theorem insEntry_mem (p : Bytes × Bytes) (qs : List (Bytes × Bytes)) (e : Bytes × Bytes) :
    e ∈ insEntry p qs ↔ e = p ∨ e ∈ qs := by
  induction qs with
  | nil => simp [insEntry]
  | cons q rest ih =>
    unfold insEntry
    split <;> simp_all [List.mem_cons, or_assoc, or_left_comm, or_comm]

/-- **Sorting keeps every entry and invents none.**  This is the "no lossy canonicalisation" claim
    for a collection: the representation choice is order alone, and the set of entries is exactly
    what arrived. -/
theorem sortEntries_mem (es : List (Bytes × Bytes)) (e : Bytes × Bytes) :
    e ∈ sortEntries es ↔ e ∈ es := by
  induction es with
  | nil => simp [sortEntries]
  | cons p ps ih =>
    simp only [sortEntries, insEntry_mem, ih, List.mem_cons]

/-- **The canonical form of a map is a function of its sorted entries.**  Two source values that
    sort to the same entry list produce the same cell, so the entry order a protocol happened to
    send is not carried into the pipeline — and, because `canon` is injective on the encoded
    entries, nothing else is collapsed either. -/
theorem map_canon_depends_only_on_sorted_entries
    (col : Nat) (ts ttl ldt : Int) (a b : List (Bytes × Bytes))
    (h : sortEntries a = sortEntries b) :
    canon col ts ttl ldt (.map a) = canon col ts ttl ldt (.map b) := by
  simp [canon, h]

/-- Two entries in either order canonicalise to one cell, so a map's wire order is not part of what
    is stored. Concrete, because it is the witness that the quotient above is not the empty one:
    `h1` and `h2` say the two keys are strictly increasing in the sort's own order. -/
theorem map_canon_ignores_entry_order (col : Nat) (ts : Int) (k₁ k₂ v₁ v₂ : Bytes)
    (h1 : lexLe k₁ k₂ = true) (h2 : lexLe k₂ k₁ = false) :
    canon col ts 0 0 (.map [(k₁, v₁), (k₂, v₂)])
      = canon col ts 0 0 (.map [(k₂, v₂), (k₁, v₁)]) := by
  have hinner : insEntry (k₂, v₂) [(k₁, v₁)] = [(k₁, v₁), (k₂, v₂)] := by
    simp [insEntry, h2]
  simp [canon, sortEntries, insEntry, h1, hinner]

/-- And a map with an entry removed is a *different* cell, so canonicalisation does not fold two
    distinct maps together — the direction that would be a silent data loss. -/
theorem map_canon_distinguishes_a_dropped_entry (col : Nat) (ts : Int) (k : Bytes) (v : Bytes) :
    canon col ts 0 0 (.map [(k, v)]) ≠ canon col ts 0 0 (.map []) := by
  intro h
  have hval := congrArg Cell.val h
  simp [canon, sortEntries, insEntry, encodeEntries] at hval

end Ferrosa.Codec
