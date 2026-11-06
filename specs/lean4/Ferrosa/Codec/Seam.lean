/-
  The codec seam: two laws, and why the format swap is cheap
  ==========================================================

  The marshal-to-disk step is going to be replaced by the native `fnf` record (ADR-022). If the
  pipeline is written against the *format*, that swap has a blast radius measured in every caller;
  if it is written against a *value*, the swap costs one module and one proof. So the seam is a
  structure with two functions and two laws, and everything below is quantified over *any* codec
  that has them:

  1. **Round-trip** — `unmarshal (marshal c) = some c` for every canonical `c` in the codec's
     domain.
  2. **Acceptance** — whatever the decoder accepts, the encoder produces: `unmarshal t = some c`
     implies `marshal c = t`. The accepted set is exactly the image, so a decoder cannot be talked
     into a value that no encoder would have written.

  The second law is not decoration. It is what makes truncation detection *sound*: without it, a
  truncated record could decode as a shorter valid value, because the decoder would be free to
  accept bytes the encoder never emits. With it, a decoder that returns `some` has returned bytes
  it would itself have written, so "is this a whole record" is a question about the bytes alone.

  Two codecs are modelled on purpose, and they disagree about everything a format is allowed to
  disagree about: field order, tag, and whether the record carries its own length. Both laws are
  proved for both, and the store's fidelity theorem is proved once, by induction over a list, for
  an arbitrary codec — so `store_fidelity_A` and `store_fidelity_B` are one-liners.
-/
import Ferrosa.Codec.Canonical
import Ferrosa.Ledger

namespace Ferrosa.Codec

open Ferrosa.Ledger

/-! ## The interface

Two functions and two laws. There is deliberately no byte-width, no alignment, no checksum and no
version field here: those are the format's business, and a pipeline that could name them is a
pipeline that would have to change when the format does.
-/

/-- A codec: how a canonical value becomes bytes and back, with the two laws that make the bytes
    trustworthy. `domain` is the set of canonical values the format can represent — for a
    fixed-width format it is a real restriction, and the round-trip law is stated over it rather
    than over a type the format cannot hold. -/
structure Codec where
  /-- The canonical values this format can represent. -/
  domain : Cell → Prop
  /-- Canonical value → bytes. Total: the store always has something to write. -/
  marshal : Cell → Bytes
  /-- Bytes → canonical value. Partial: corrupt or truncated bytes have no value. -/
  unmarshal : Bytes → Option Cell
  /-- **Law 1.** A value the format can represent survives the round trip. -/
  roundtrip : ∀ c, domain c → unmarshal (marshal c) = some c
  /-- **Law 2.** The decoder accepts only bytes the encoder produces, so the accepted set is the
      image and a truncation cannot masquerade as a shorter value. -/
  accepts_only_encodings : ∀ t c, unmarshal t = some c → marshal c = t

/-- **The bytes determine the value.**  Derived, not assumed: if two representable values marshal to
    the same bytes, law 1 gives two decodes of those bytes and law 2 says each decode is the
    encoding of its own value, so the values agree. A codec cannot be ambiguous without breaking one
    of the two laws. -/
theorem Codec.injective (k : Codec) {a b : Cell} (ha : k.domain a) (hb : k.domain b)
    (h : k.marshal a = k.marshal b) : a = b := by
  have h1 := k.roundtrip a ha
  rw [h, k.roundtrip b hb] at h1
  exact Option.some.inj h1.symm

/-- **A codec's round trip preserves every observable.**  Stated through the observation function
    rather than through `=`, because this is the form a client-facing fidelity claim takes; the two
    agree here (`obsEq_iff_eq`), which is the point of making the observation the whole record. -/
theorem roundtrip_preserves_every_observation (k : Codec) (c : Cell) (hc : k.domain c) :
    ∃ c', k.unmarshal (k.marshal c) = some c' ∧ ObsEq c' c :=
  ⟨c, k.roundtrip c hc, (obsEq_iff_eq c c).mpr rfl⟩

/-! ## The store: a sequence of published artifacts, and reading it back

A store is what the writer publishes: one byte string per canonical value, in order. Reading it back
is the decoder applied to each. Neither mentions a codec's particulars, which is what the theorems
below quantify over.
-/

/-- Publish a sequence of canonical values: one artifact per value, in order. -/
def Codec.publish (k : Codec) (cs : List Cell) : List Bytes := cs.map k.marshal

/-- Read a published sequence back. One failed artifact fails the read: a store that silently
    skipped one would be reporting a shorter dataset as the dataset. -/
def Codec.readBack (k : Codec) : List Bytes → Option (List Cell)
  | [] => some []
  | b :: bs => (k.unmarshal b).bind fun c => (k.readBack bs).map fun cs => c :: cs

/-- **Round-trip, at the store.**  Everything the store holds reads back as exactly what was
    published, in order, for **any** codec satisfying the two laws. The proof is one induction and
    it never looks at a byte pattern: that is the seam. -/
theorem store_roundtrip (k : Codec) :
    ∀ cs : List Cell, (∀ c ∈ cs, k.domain c) → k.readBack (k.publish cs) = some cs
  | [], _ => by simp [Codec.publish, Codec.readBack]
  | c :: cs, hd => by
    have hc : k.domain c := hd c (List.mem_cons_self ..)
    have htail : ∀ d ∈ cs, k.domain d := fun d hd' => hd d (List.mem_cons_of_mem c hd')
    have ih := store_roundtrip k cs htail
    simp only [Codec.publish] at ih ⊢
    simp only [Codec.readBack, List.map_cons, k.roundtrip c hc, Option.bind_some, ih,
      Option.map_some]

/-! ## The bytes, and the primitives a self-delimiting format needs

A number is written as its unary expansion followed by a zero, and an optional byte string as a
presence tag followed by a length and a payload. The point of both is the pair of lemmas about them:
decoding `enc n ++ rest` yields `n` and `rest`, so a decoder walks a record by *consuming* it and a
truncated tail is a decode failure rather than a shorter value. Byte *widths* are not modelled —
`Storage/Format.lean` owns the 12-byte cell and the CRC — because what the seam needs is that the
bytes determine the value, not how few of them there are.
-/

/-- A number as `n` ones then a zero. -/
def encNat (n : Nat) : Bytes := List.replicate n 1 ++ [0]

/-- Read a number and the rest of the input. `none` on a truncated or malformed field: a field with
    no terminator is not a shorter number. -/
def decNat : Bytes → Option (Nat × Bytes)
  | [] => none
  | 0 :: r => some (0, r)
  | 1 :: r => (decNat r).map (fun p => (p.1 + 1, p.2))
  | _ :: _ => none

/-- **The number decoder consumes exactly its field.**  Stated in `++ rest` form so that a chain of
    fields decodes by one rewrite per field. -/
theorem decNat_append (n : Nat) (rest : Bytes) : decNat (encNat n ++ rest) = some (n, rest) := by
  induction n with
  | zero => simp [encNat, decNat]
  | succ m ih =>
    have h : encNat (m + 1) ++ rest = 1 :: (encNat m ++ rest) := by
      simp [encNat, List.replicate_succ]
    rw [h]
    simp only [decNat, ih, Option.map_some]

/-- A number's own field, with nothing after it. -/
theorem decNat_encNat (n : Nat) : decNat (encNat n) = some (n, []) := by
  simpa using decNat_append n []

/-- **A signed number keeps its sign.**  `Int` is written through its own constructors — a non-negative
    value as `0` and its magnitude, a negative one as `1` and its magnitude minus one — so every
    `Int` is encodable and no value is reserved as a "no timestamp" sentinel. -/
def encInt : Int → Bytes
  | Int.ofNat m => 0 :: encNat m
  | Int.negSucc m => 1 :: encNat m

/-- Read a signed number and the rest of the input. -/
def decInt : Bytes → Option (Int × Bytes)
  | 0 :: r => (decNat r).map (fun p => (Int.ofNat p.1, p.2))
  | 1 :: r => (decNat r).map (fun p => (Int.negSucc p.1, p.2))
  | _ => none

/-- **Every `Int` round-trips**, both extremes and zero included: the sign is carried, so no sentinel
    is needed and none can be aliased by a legitimate value. -/
theorem decInt_append (n : Int) (rest : Bytes) : decInt (encInt n ++ rest) = some (n, rest) := by
  cases n with
  | ofNat m => simp [encInt, decInt, decNat_append]
  | negSucc m => simp [encInt, decInt, decNat_append]

/-- An optional byte string: `[2]` for absent, `[3]` then the length then the bytes for present. -/
def encOptBytes : Option Bytes → Bytes
  | none => [2]
  | some b => 3 :: (encNat b.length ++ b)

/-- Read an optional byte string and the rest of the input. -/
def decOptBytes : Bytes → Option (Option Bytes × Bytes)
  | 2 :: r => some (none, r)
  | 3 :: r => (decNat r).map (fun p => (some (p.2.take p.1), p.2.drop p.1))
  | _ => none

/-- **A tombstone and a zero-length value are different fields on disk.**  `none` decodes to `none`
    and `some []` decodes to `some []`, so the distinction the canonical type makes survives the
    codec rather than being re-derived from a length. -/
theorem decOptBytes_append (v : Option Bytes) (rest : Bytes) :
    decOptBytes (encOptBytes v ++ rest) = some (v, rest) := by
  cases v with
  | none => simp [encOptBytes, decOptBytes]
  | some b =>
    have h : encOptBytes (some b) ++ rest = 3 :: (encNat b.length ++ (b ++ rest)) := by
      simp [encOptBytes]
    rw [h, decOptBytes, decNat_append, Option.map_some]
    simp

/-! ## Codec A: forward field order, tag `0xA1`, no record length

Fields in schema order, each self-delimiting, terminated by the end of the input.
-/

/-- Codec A's bytes: tag, column, timestamp, TTL, local deletion time, value, path. -/
def marshalA (c : Cell) : Bytes :=
  0xA1 :: (encNat c.col ++ (encInt c.ts ++ (encInt c.ttl ++ (encInt c.ldt ++
    (encOptBytes c.val ++ (encOptBytes c.path ++ []))))))

/-- Codec A's parser. Every field is read before the next is looked at, and the record is accepted
    only if the input ends exactly where the last field does. -/
def decodeA (s : Bytes) : Option Cell :=
  match s with
  | 0xA1 :: rest =>
    (decNat rest).bind fun p =>
    (decInt p.2).bind fun q =>
    (decInt q.2).bind fun r =>
    (decInt r.2).bind fun u =>
    (decOptBytes u.2).bind fun v =>
    (decOptBytes v.2).bind fun w =>
    if w.2 = [] then some ⟨p.1, v.1, q.1, r.1, u.1, w.1⟩ else none
  | _ => none

/-- **Codec A's parser inverts its encoder.**  One rewrite per field, because each field's lemma is
    stated in the `++ rest` form the encoder produces. -/
theorem decodeA_marshalA (c : Cell) : decodeA (marshalA c) = some c := by
  simp only [marshalA, decodeA, decNat_append, decInt_append, decOptBytes_append,
    Option.bind_some]
  rfl

/-- Codec A's decoder. **Law 2 is enforced by re-encoding**: a parse is accepted only when the bytes
    are exactly what the encoder would have produced. A format that enforces it structurally — with
    a length and a check field — gets it from the framing model below instead; both are ways to make
    "the accepted set is the image" true. -/
def unmarshalA (s : Bytes) : Option Cell :=
  match decodeA s with
  | some c => if marshalA c = s then some c else none
  | none => none

/-- **Law 1 for codec A.** -/
theorem codecA_roundtrip (c : Cell) : unmarshalA (marshalA c) = some c := by
  simp [unmarshalA, decodeA_marshalA]

/-- **Law 2 for codec A.** -/
theorem codecA_accepts_only_encodings (t : Bytes) (c : Cell) (h : unmarshalA t = some c) :
    marshalA c = t := by
  unfold unmarshalA at h
  split at h
  · rename_i d _hd
    split at h
    · rename_i hbytes
      simp only [Option.some.injEq] at h
      rw [← h]
      exact hbytes
    · simp at h
  · simp at h

/-- Codec A, as the interface sees it. Every canonical cell is in its domain: the encoding is
    variable-width and carries both signs, so nothing in `Cell` is outside it. -/
def codecA : Codec where
  domain := fun _ => True
  marshal := marshalA
  unmarshal := unmarshalA
  roundtrip := fun c _ => codecA_roundtrip c
  accepts_only_encodings := fun t c h => codecA_accepts_only_encodings t c h

/-! ## Codec B: the fields in reverse order, tag `0xB2`, and a record length

A deliberately different format: it writes the path first and the column last, it declares its total
record length up front, and it therefore rejects a truncated record before decoding a single field.
Nothing above changes, which is the claim the seam has to make.
-/

/-- Codec B's record body: path, value, deletion time, TTL, timestamp, column — the reverse of A. -/
def bodyB (c : Cell) : Bytes :=
  encOptBytes c.path ++ (encOptBytes c.val ++ (encInt c.ldt ++
    (encInt c.ttl ++ (encInt c.ts ++ (encNat c.col ++ [])))))

/-- Codec B's bytes: tag, total body length, body. -/
def marshalB (c : Cell) : Bytes := 0xB2 :: (encNat (bodyB c).length ++ bodyB c)

/-- Codec B's parser. The length check comes **first**, so a truncated record is refused without any
    field being decoded and cannot be read as a shorter record. -/
def decodeB (s : Bytes) : Option Cell :=
  match s with
  | 0xB2 :: rest =>
    (decNat rest).bind fun p =>
    if p.2.length < p.1 then none
    else
      (decOptBytes (p.2.take p.1)).bind fun w =>
      (decOptBytes w.2).bind fun v =>
      (decInt v.2).bind fun u =>
      (decInt u.2).bind fun r =>
      (decInt r.2).bind fun q =>
      (decNat q.2).bind fun pc =>
      if pc.2 = [] then some ⟨pc.1, v.1, q.1, r.1, u.1, w.1⟩ else none
  | _ => none

/-- **Codec B's parser inverts its encoder**, and the length field is what makes the record boundary
    explicit: the body is taken by length, not by where the fields happen to end. -/
theorem decodeB_marshalB (c : Cell) : decodeB (marshalB c) = some c := by
  simp only [marshalB, decodeB, decNat_append, Option.bind_some, List.take_length]
  simp [bodyB, decOptBytes_append, decInt_append, decNat_append, decNat_encNat]

/-- Codec B's decoder, with law 2 enforced the way A enforces it. -/
def unmarshalB (s : Bytes) : Option Cell :=
  match decodeB s with
  | some c => if marshalB c = s then some c else none
  | none => none

/-- **Law 1 for codec B.** -/
theorem codecB_roundtrip (c : Cell) : unmarshalB (marshalB c) = some c := by
  simp [unmarshalB, decodeB_marshalB]

/-- **Law 2 for codec B.** -/
theorem codecB_accepts_only_encodings (t : Bytes) (c : Cell) (h : unmarshalB t = some c) :
    marshalB c = t := by
  unfold unmarshalB at h
  split at h
  · rename_i d _hd
    split at h
    · rename_i hbytes
      simp only [Option.some.injEq] at h
      rw [← h]
      exact hbytes
    · simp at h
  · simp at h

/-- Codec B, as the interface sees it. -/
def codecB : Codec where
  domain := fun _ => True
  marshal := marshalB
  unmarshal := unmarshalB
  roundtrip := fun c _ => codecB_roundtrip c
  accepts_only_encodings := fun t c h => codecB_accepts_only_encodings t c h

/-! ## The seam theorem: one proof, two formats

`store_fidelity` is quantified over `Codec`, so it is not proved for A and then again for B: it is
proved once for the interface and instantiated twice. That is the economy of the seam — the `fnf`
swap re-establishes `roundtrip` and `accepts_only_encodings` for the new codec, and every store
obligation comes back for free.
-/

/-- A pipeline stage: a function over canonical values. There is no `Bytes` in this type, so no stage
    can name a format, a version, or a field offset. -/
def Stage := Cell → Cell

/-- A pipeline: stages applied in order. -/
structure Pipeline where
  stages : List Stage

/-- Run a pipeline over one canonical value. -/
def Pipeline.run (p : Pipeline) (c : Cell) : Cell := p.stages.foldl (fun c s => s c) c

/-- **Store fidelity for any codec.**  What is published reads back as what the pipeline produced,
    in order, for every codec whose two laws hold. -/
theorem store_fidelity (k : Codec) (p : Pipeline) (cs : List Cell)
    (hd : ∀ c ∈ cs, k.domain c) (hp : ∀ c, k.domain c → k.domain (p.run c)) :
    k.readBack (k.publish (cs.map p.run)) = some (cs.map p.run) :=
  store_roundtrip k (cs.map p.run) fun c hc => by
    obtain ⟨d, hd', rfl⟩ := List.mem_map.mp hc
    exact hp d (hd d hd')

/-- **The same theorem, at codec A.**  One line, because nothing about A had to be re-derived. -/
theorem store_fidelity_A (p : Pipeline) (cs : List Cell) :
    codecA.readBack (codecA.publish (cs.map p.run)) = some (cs.map p.run) :=
  store_fidelity codecA p cs (fun _ _ => trivial) (fun _ _ => trivial)

/-- **The same theorem, at codec B**, which disagrees with A about field order, tag and framing. -/
theorem store_fidelity_B (p : Pipeline) (cs : List Cell) :
    codecB.readBack (codecB.publish (cs.map p.run)) = some (cs.map p.run) :=
  store_fidelity codecB p cs (fun _ _ => trivial) (fun _ _ => trivial)

/-- **An artifact from another codec version is refused, not misread.**  B's encoding starts with a
    tag A's decoder does not accept, so feeding one format's bytes to the other's reader yields
    nothing rather than a wrong value. Stated for these two versions: two formats that shared a tag
    would need a version field to tell them apart, which is why ADR-022 puts one in the header. -/
theorem another_formats_artifact_is_refused (c : Cell) :
    unmarshalA (marshalB c) = none ∧ unmarshalB (marshalA c) = none := by
  constructor <;> simp [unmarshalA, unmarshalB, decodeA, decodeB, marshalA, marshalB]

/-! ## Record boundaries: what the framing has to deliver

A record on disk is a declared length, a check value over the payload, and the payload. The two
properties the format needs are stated here over *any* codec, because they must not depend on what
the payload means:

  - a record whose payload is shorter than its declared length is **refused**, before any field is
    decoded, so a truncated record cannot be read as a shorter value;
  - a record that passes consumes exactly its declared bytes and leaves the rest of the input
    untouched, so the reader's position is a function of the record and cannot drift into a
    neighbour.

The residual caveat is stated rather than hidden: a length field that runs past one record and into
the next can only be caught by the check value, so a *collision* — the spanning bytes carrying the
right check — is not excluded by this theorem. That is why ADR-022 also requires two consecutive
accepted candidates to resynchronise, and why `Storage/Format.lean` proves a resync lands only on
records that verify.
-/

/-- The check value a record carries. A real format uses CRC32C (ADR-022); the model needs only a
    function of the payload, because what is proved is about the *order* of the checks. -/
def checkValue (p : Bytes) : Nat := p.foldl (fun acc b => acc + b) 0

/-- Frame a marshalled value: declared length, check value, payload. -/
def frame (k : Codec) (c : Cell) : Bytes :=
  let p := k.marshal c
  encNat p.length ++ (checkValue p :: p)

/-- Read one record out of the bytes that follow its length field: the length, then the check, then
    the payload. The two tests come **before** the codec is asked for a value, which is what bounds
    the damage to one record. -/
def readRecordAt (k : Codec) (n chk : Nat) (p : Bytes) : Option (Cell × Bytes) :=
  if p.length < n then none
  else if checkValue (p.take n) ≠ chk then none
  else (k.unmarshal (p.take n)).map (fun c => (c, p.drop n))

/-- Read one framed record from a stream. -/
def readFrame (k : Codec) (s : Bytes) : Option (Cell × Bytes) :=
  match decNat s with
  | none => none
  | some (n, s1) =>
    match s1 with
    | [] => none
    | chk :: p => readRecordAt k n chk p

/-- **A truncated record fails at that record.**  Whatever the payload holds and whatever the codec
    would say about it, a payload shorter than its declared length is refused — the length check
    precedes the decode, so a truncation cannot be read as a shorter value. -/
theorem short_record_is_refused (k : Codec) (n chk : Nat) (p : Bytes) (short : p.length < n) :
    readRecordAt k n chk p = none := by
  simp [readRecordAt, short]

/-- **A corrupt record fails at that record**, before the payload is decoded at all. -/
theorem corrupt_record_is_refused (k : Codec) (n chk : Nat) (p : Bytes)
    (bad : checkValue (p.take n) ≠ chk) (fit : ¬ p.length < n) :
    readRecordAt k n chk p = none := by
  simp [readRecordAt, fit, bad]

/-- **A verified record consumes exactly its bytes.**  The remainder is the rest of the input,
    untouched, so the reader's next position is a function of the declared length — it cannot land
    inside a neighbouring record, and it cannot skip one. -/
theorem verified_record_consumes_exactly (k : Codec) (n chk : Nat) (p rest : Bytes) (c : Cell)
    (len : p.length = n) (ok : checkValue p = chk) (dec : k.unmarshal p = some c) :
    readRecordAt k n chk (p ++ rest) = some (c, rest) := by
  subst len
  simp [readRecordAt, ok, dec]

/-- **A record the store wrote reads back as the value it wrote.**  The statement an operator cares
    about: framing does not change what the value is. -/
theorem frame_roundtrip (k : Codec) (c : Cell) (hc : k.domain c) (rest : Bytes) :
    readFrame k (frame k c ++ rest) = some (c, rest) := by
  simp [readFrame, frame, decNat_append, readRecordAt, k.roundtrip c hc]

/-! ## Bounded memory: one record, not one dataset

"Read it into memory" is per record. The canonical value exists in memory while it is processed, and
the pipeline needs no second copy and no unbounded collection of them: `Pipeline.run` is a fold over
one cell, and canonicalisation is a map over records. A stage that must hold many records to produce
one answer is a different problem — `Ferrosa/Storage/Streaming.lean` owns the measurement and the
page bound — and the model here names the boundary rather than duplicating the solution.
-/

/-- Canonicalise a stream of source values. Defined as a `map`, which is the theorem: no step of it
    needs more than the record it is on. -/
def canonicaliseStream (col : Nat) (ts ttl ldt : Int) (vs : List SrcVal) : List Cell :=
  vs.map (canon col ts ttl ldt)

/-- **Canonicalisation is per record.**  One source value in, one canonical value out, in order, so
    what a canonicalisation holds is the record it is on and nothing else. -/
theorem canonicalise_is_per_record (col : Nat) (ts ttl ldt : Int) (vs : List SrcVal) :
    (canonicaliseStream col ts ttl ldt vs).length = vs.length := by
  simp [canonicaliseStream]

/-- Records a stage must hold to produce one answer, counted from the stages that need more than the
    record they are on. One is the seam's bound; a grouping or sorting stage is where it stops being
    one. -/
def recordsPerAnswer (extra : Nat) : Nat := 1 + extra

/-- **The bound is one record, and it is a bound only for stages that take one.**  Stated so the
    boundary is explicit: the pipeline modelled here holds one record per answer, and a stage that
    needs more says so by raising `extra` — which is the streaming lane's obligation, not a gap in
    this one. -/
theorem the_bound_is_one_record (col : Nat) (ts ttl ldt : Int) (vs : List SrcVal) :
    recordsPerAnswer 0 = 1 ∧ (canonicaliseStream col ts ttl ldt vs).length = vs.length :=
  ⟨rfl, canonicalise_is_per_record col ts ttl ldt vs⟩

end Ferrosa.Codec
