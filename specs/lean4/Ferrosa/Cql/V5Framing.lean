/-
  Ferrosa formal specification — CQL front end: v5 segment reassembly and progress.
  ===============================================================================

  Protocol v5 carries each message (a 9-byte envelope header plus its body) in one
  or more *segments*. A message larger than 131 071 bytes is split into consecutive
  segments, every one marked `isSelfContained = 0`; the receiver knows the message
  is complete when the bytes it has accumulated reach the length the envelope
  header declares (`v5_reassembly_target` in `ferrosa-cql/src/frame.rs`).

  The decoder is driven by `tokio_util::codec::Framed`, which calls `decode` again
  only after it returned a frame, or after another read from the socket. So a
  `None` from `decode` is a promise: *the buffer holds nothing more I can use*.

  The shipped decoder consumed **one** segment per call and returned `None` when the
  message was still incomplete — even when the next segment was already buffered.
  A client that writes a split message in one go then never sends another byte; the
  server waits for input that will not come, the request never runs, and the client
  never gets a reply. In production this was a conditional `UPDATE` whose payload
  crossed 128 KiB and hung: `decode_old_strands_a_buffered_segment` and
  `framed_old_delivers_nothing` below are that trace.

  The fix keeps consuming buffered segments until the message completes or the
  buffer has no segment left. `decode_new_progress` is the invariant `Framed`
  relies on, `decode_new_reassembles` is correctness for any split of any message,
  and `framed_new_delivers_all` composes them: every message whose segments are all
  buffered is delivered, in order, without another read.

  Scope. Segments are modelled after their header and CRC checks and after
  per-segment decompression (compressed framing decompresses each segment before
  it joins the accumulation, so the same model covers it). Bytes of a segment that
  has not fully arrived are not in `segs`: the decoder cannot use them, and that is
  the honest `None`. Several envelopes inside one self-contained segment are queued
  by `emit_envelopes`, which this model does not need.
-/

namespace Ferrosa.Cql.V5Framing

/-! ## Segments, decoder state, results -/

/-- A fully received segment: its payload and the `isSelfContained` flag. -/
structure Seg (α : Type) where
  data : List α
  sc : Bool
  deriving Repr, DecidableEq

/-- What `decode` returns: a message and the state after it, a request for more
bytes, or a protocol error (a split message that overran its declared length). -/
inductive Res (α : Type) where
  | frame (env : List α) (acc : List α) (segs : List (Seg α))
  | needMore (acc : List α) (segs : List (Seg α))
  | error
  deriving Repr, DecidableEq

/-- One segment joining the accumulation. -/
inductive Absorb (α : Type) where
  | emit (env : List α)
  | wait (acc : List α)
  | overrun
  deriving Repr, DecidableEq

variable {α : Type}

/-- `target p` is the total length the envelope header in `p` declares, or `none`
while the 9-byte header is incomplete (`v5_reassembly_target`). -/
def absorb (target : List α → Option Nat) (acc : List α) (s : Seg α) : Absorb α :=
  if s.sc then .emit (acc ++ s.data)
  else
    match target (acc ++ s.data) with
    | none => .wait (acc ++ s.data)
    | some n =>
      if (acc ++ s.data).length < n then .wait (acc ++ s.data)
      else if (acc ++ s.data).length = n then .emit (acc ++ s.data)
      else .overrun

/-- The decoder as shipped: one segment per call. -/
def decodeOld (target : List α → Option Nat) (acc : List α) : List (Seg α) → Res α
  | [] => .needMore acc []
  | s :: rest =>
    match absorb target acc s with
    | .emit e => .frame e [] rest
    | .wait a => .needMore a rest
    | .overrun => .error

/-- The fix: keep consuming buffered segments until a message completes or none is left. -/
def decodeNew (target : List α → Option Nat) (acc : List α) : List (Seg α) → Res α
  | [] => .needMore acc []
  | s :: rest =>
    match absorb target acc s with
    | .emit e => .frame e [] rest
    | .wait a => decodeNew target a rest
    | .overrun => .error

/-- `Framed`: call `decode` while it yields frames; on `needMore`, wait for the
socket. `fuel` bounds the loop; returns the delivered messages and what is left. -/
def framed (dec : List α → List (Seg α) → Res α) :
    Nat → List α → List (Seg α) → List (List α) × List α × List (Seg α)
  | 0, acc, segs => ([], acc, segs)
  | fuel + 1, acc, segs =>
    match dec acc segs with
    | .frame e acc' segs' =>
      let (es, a, s) := framed dec fuel acc' segs'
      (e :: es, a, s)
    | .needMore acc' segs' => ([], acc', segs')
    | .error => ([], acc, segs)

/-! ## The violation

An envelope whose first byte declares its total length (3), split into a two-byte
and a one-byte segment, both buffered. -/

/-- The length a header declares: here, the first byte. -/
def toyTarget : List Nat → Option Nat
  | [] => none
  | n :: _ => some n

def toySegs : List (Seg Nat) := [⟨[3, 7], false⟩, ⟨[8], false⟩]

/-- The shipped decoder asks for more bytes while a complete segment is buffered. -/
theorem decode_old_strands_a_buffered_segment :
    decodeOld toyTarget [] toySegs = .needMore [3, 7] [⟨[8], false⟩] := by
  decide

/-- Under `Framed`, nothing is delivered and the last segment is stranded: the hang. -/
theorem framed_old_delivers_nothing :
    framed (decodeOld toyTarget) 10 [] toySegs = ([], [3, 7], [⟨[8], false⟩]) := by
  decide

/-- The fix delivers the message from the same buffer. -/
theorem framed_new_delivers_it :
    framed (decodeNew toyTarget) 10 [] toySegs = ([[3, 7, 8]], [], []) := by
  decide

/-! ## Progress: `needMore` only when no buffered segment is left -/

/-- The invariant `Framed` relies on, which the shipped decoder breaks. -/
theorem decode_new_progress (target : List α → Option Nat) :
    ∀ (segs : List (Seg α)) (acc acc' : List α) (segs' : List (Seg α)),
      decodeNew target acc segs = .needMore acc' segs' → segs' = [] := by
  intro segs
  induction segs with
  | nil =>
    intro acc acc' segs' h
    simp only [decodeNew, Res.needMore.injEq] at h
    exact h.2.symm
  | cons s rest ih =>
    intro acc acc' segs' h
    simp only [decodeNew] at h
    split at h
    · cases h
    · exact ih _ _ _ h
    · cases h

/-- The shipped decoder does not satisfy it. -/
theorem decode_old_breaks_progress :
    ¬ ∀ (segs : List (Seg Nat)) (acc acc' : List Nat) (segs' : List (Seg Nat)),
      decodeOld toyTarget acc segs = .needMore acc' segs' → segs' = [] := by
  intro h
  have := h toySegs [] [3, 7] [⟨[8], false⟩] decode_old_strands_a_buffered_segment
  cases this

/-! ## Reassembly: any split of any message -/

/-- `target` reads `e`'s header: on any proper prefix it is unknown or `e`'s length,
and on `e` itself it is `e`'s length. -/
def Declares (target : List α → Option Nat) (e : List α) : Prop :=
  target e = some e.length ∧
    ∀ p q, p ++ q = e → p.length < e.length → target p = none ∨ target p = some e.length

/-- A message split into non-empty, non-self-contained slices. -/
def slices (cs : List (List α)) : List (Seg α) := cs.map fun c => ⟨c, false⟩

theorem flatten_nil_of_nonempty :
    ∀ (cs : List (List α)), (∀ c ∈ cs, c ≠ []) → cs.flatten = [] → cs = []
  | [], _, _ => rfl
  | c :: cs, hne, h => by
    simp only [List.flatten_cons, List.append_eq_nil_iff] at h
    exact absurd h.1 (hne c (by simp))

/-- From any proper prefix `acc` of `e`, the remaining slices of `e`, followed by
anything, decode to exactly `e`, leaving exactly what followed. -/
theorem decode_new_reassembles_from (target : List α → Option Nat) (e : List α)
    (hd : Declares target e) :
    ∀ (cs : List (List α)) (acc : List α) (rest : List (Seg α)),
      (∀ c ∈ cs, c ≠ []) → acc ++ cs.flatten = e → acc.length < e.length →
      decodeNew target acc (slices cs ++ rest) = .frame e [] rest := by
  intro cs
  induction cs with
  | nil =>
    intro acc rest _ he hlt
    simp at he
    subst he
    exact absurd hlt (Nat.lt_irrefl _)
  | cons c cs ih =>
    intro acc rest hne he hlt
    have hlen : (acc ++ c).length + cs.flatten.length = e.length := by
      rw [← he]; simp [List.flatten_cons, List.length_append, Nat.add_assoc]
    have hpre : (acc ++ c) ++ cs.flatten = e := by
      rw [← he]; simp [List.flatten_cons, List.append_assoc]
    have hrest : ∀ c' ∈ cs, c' ≠ [] := fun c' h => hne c' (by simp [h])
    simp only [slices, List.map_cons, List.cons_append, decodeNew, absorb]
    show (match (match target (acc ++ c) with
      | none => Absorb.wait (acc ++ c)
      | some n =>
        if (acc ++ c).length < n then Absorb.wait (acc ++ c)
        else if (acc ++ c).length = n then Absorb.emit (acc ++ c)
        else Absorb.overrun) with
      | .emit e => Res.frame e [] (slices cs ++ rest)
      | .wait a => decodeNew target a (slices cs ++ rest)
      | .overrun => Res.error) = Res.frame e [] rest
    by_cases hfull : (acc ++ c).length = e.length
    · -- The last slice: the accumulation is the whole message.
      have hflat : cs.flatten = [] := List.eq_nil_of_length_eq_zero (by omega)
      have hcs : cs = [] := flatten_nil_of_nonempty cs hrest hflat
      subst hcs
      have hae : acc ++ c = e := by simpa using hpre
      rw [hae, hd.1]
      simp [slices]
    · -- More slices to come: wait, then the rest completes it.
      have hlt' : (acc ++ c).length < e.length := by omega
      have hwait : (match target (acc ++ c) with
          | none => Absorb.wait (acc ++ c)
          | some n =>
            if (acc ++ c).length < n then Absorb.wait (acc ++ c)
            else if (acc ++ c).length = n then Absorb.emit (acc ++ c)
            else Absorb.overrun) = Absorb.wait (acc ++ c) := by
        have hl : acc.length + c.length < e.length := by simpa using hlt'
        rcases hd.2 (acc ++ c) cs.flatten hpre hlt' with h | h
        · simp [h]
        · simp [h, hl]
      rw [hwait]
      exact ih (acc ++ c) rest hrest hpre hlt'

/-- Correctness for a fresh message: all of `e`'s slices buffered, followed by
anything, decode to `e` in one call. -/
theorem decode_new_reassembles (target : List α → Option Nat) (e : List α)
    (hd : Declares target e) (hne : e ≠ []) (cs : List (List α))
    (hcs : ∀ c ∈ cs, c ≠ []) (hjoin : cs.flatten = e) (rest : List (Seg α)) :
    decodeNew target [] (slices cs ++ rest) = .frame e [] rest :=
  decode_new_reassembles_from target e hd cs [] rest hcs (by simpa using hjoin)
    (by cases e with
        | nil => exact absurd rfl hne
        | cons _ _ => simp)

/-! ## Delivery under `Framed`: every buffered message, in order -/

/-- A buffered message: its bytes, the slices it was sent as, and a header that
declares its length. -/
structure Sent (α : Type) where
  env : List α
  parts : List (List α)

def Sent.ok (target : List α → Option Nat) (m : Sent α) : Prop :=
  Declares target m.env ∧ m.env ≠ [] ∧ (∀ c ∈ m.parts, c ≠ []) ∧ m.parts.flatten = m.env

/-- Whatever the client wrote in one go — any number of split messages — `Framed`
delivers every one of them, in order, from the buffer alone. -/
theorem framed_new_delivers_all (target : List α → Option Nat) :
    ∀ (ms : List (Sent α)) (fuel : Nat), (∀ m ∈ ms, m.ok target) → ms.length < fuel →
      (framed (decodeNew target) fuel [] ((ms.map fun m => slices m.parts).flatten)).1 =
        ms.map (·.env) := by
  intro ms
  induction ms with
  | nil =>
    intro fuel _ hfuel
    cases fuel with
    | zero => exact absurd hfuel (Nat.lt_irrefl 0)
    | succ f => simp [framed, decodeNew]
  | cons m ms ih =>
    intro fuel hok hfuel
    cases fuel with
    | zero => exact absurd hfuel (Nat.not_lt_zero _)
    | succ f =>
      have hm := hok m (by simp)
      have hstep := decode_new_reassembles target m.env hm.1 hm.2.1 m.parts hm.2.2.1 hm.2.2.2
        ((ms.map fun m => slices m.parts).flatten)
      simp only [List.map_cons, List.flatten_cons, framed, hstep]
      simp only [List.length_cons] at hfuel
      have := ih f (fun m' h => hok m' (by simp [h])) (by omega)
      simp [this]

end Ferrosa.Cql.V5Framing
