/-
  Ferrosa formal specification — `oid` columns on the wire (found while building W1).
  ===================================================================================

  The `pg_catalog` projections typed every OID column as `int4`. Synthetic OIDs
  are hashed into `[16384, 2^32)`, and an `int4` is signed: the binary encoder
  refused every OID at or above `2^31`, so a driver reading `pg_class.oid` in
  binary failed for about half of all relations. The fix gives OID columns the
  `oid` type (26): an unsigned 4-byte value.

  `int4_cannot_carry_high_oids` is the bug; `oid_encodes_every_oid` and
  `oid_round_trips` are the fix; `oid_refuses_outside_range` shows nothing is
  wrapped.
-/

namespace Ferrosa.Sql.Oid

def two31 : Int := 2147483648
def two32 : Int := 4294967296

/-- int4 binary: the value as a 32-bit two's-complement word, or a refusal. -/
def encodeInt4 (n : Int) : Option Nat :=
  if -two31 ≤ n ∧ n < two31 then some (n % two32).toNat else none

/-- oid binary: the value as an unsigned 32-bit word, or a refusal. -/
def encodeOid (n : Int) : Option Nat :=
  if 0 ≤ n ∧ n < two32 then some n.toNat else none

def decodeOid (w : Nat) : Int := w

/-- The bug: the largest synthetic OID does not fit an int4. -/
theorem int4_cannot_carry_high_oids : encodeInt4 (two32 - 1) = none := by
  decide

theorem oid_encodes_every_oid (n : Int) (h0 : 0 ≤ n) (h1 : n < two32) :
    (encodeOid n).isSome := by
  simp [encodeOid, h0, h1]

theorem oid_round_trips (n : Int) (h0 : 0 ≤ n) (h1 : n < two32) :
    (encodeOid n).map decodeOid = some n := by
  simp [encodeOid, h0, h1, decodeOid]
  omega

theorem oid_refuses_outside_range (n : Int) (h : n < 0 ∨ two32 ≤ n) : encodeOid n = none := by
  unfold encodeOid
  split
  · omega
  · rfl

/-- The encoded word fits four bytes. -/
theorem oid_word_fits (n : Int) (w : Nat) (h : encodeOid n = some w) : w < 2 ^ 32 := by
  unfold encodeOid at h
  split at h
  · simp at h
    subst h
    simp [two32] at *
    omega
  · simp at h

end Ferrosa.Sql.Oid
