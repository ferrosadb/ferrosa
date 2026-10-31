import FerrosaRust.Storage

open Aeneas Aeneas.Std Result
open ferrosa_storage

/-! ## The first refinement: `ferrosa-storage/src/count.rs` as Charon and Aeneas see it

`FerrosaRust.Storage` is the translated module. The constants below are not written down here — they
are read out of the extraction, so `count.MAGIC` is whatever `count.rs:126` says today.

That is the mechanism, and it is why these theorems live in a library of their own: change the Rust
and the extraction changes with it, so a proof that closes over the old values stops closing. The
corpus cannot do that, because its theorems are about a model written by hand beside the code rather
than about the code.

`count.MAGIC` and friends carry `@[irreducible]`, so each proof unfolds them first; the extraction is
the only place the numbers are stated.

The second theorem below is written as the machine now says it. It was first stated as "the header
holds a whole number of slots", which looks obvious and is false: 64 is not a multiple of 24. The
extraction refuted it in a build, which is the behaviour that was missing.
-/

namespace FerrosaRust.Count

/-- The count file's magic number is not its header size. A reader that confused the two would start
    parsing at byte 5063543869334442836 and read nothing. -/
theorem magic_is_not_the_header : count.MAGIC ≠ count.HEADER := by
  unfold count.MAGIC count.HEADER
  decide

/-- `MIN_CAPACITY` is a power of two, which is what makes the probe's mask a mask rather than a
    wrapping modulo. -/
theorem min_capacity_is_a_power_of_two :
    count.MIN_CAPACITY.val &&& (count.MIN_CAPACITY.val - 1) = 0 := by
  unfold count.MIN_CAPACITY
  decide

/-- The header does **not** divide into slots: 64 bytes of header over 24-byte slots leaves 16. A
    reader that assumed whole slots would misplace every record after the first. -/
theorem header_leaves_a_remainder : count.HEADER.val % count.SLOT.val = 16 := by
  unfold count.HEADER count.SLOT
  decide

end FerrosaRust.Count
