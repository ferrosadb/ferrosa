import FerrosaRust.Storage
import FerrosaRust.Link.Count

/-! ## Rust, translated, and the theorems about it

The code here is Charon's MIR extraction of this repository's Rust, translated to Lean by Aeneas.
The point of keeping it in its own library is that a theorem stated over an extracted item is a
theorem about the Rust: change the Rust and the extraction changes with it, so the proof has to be
redone or it breaks.

That is the difference between this and `specs/lean4`, whose theorems are about a model transcribed
by hand. Nothing in the corpus makes that transcription track the code, so a change to the
implementation cannot turn a corpus claim red — which is exactly what a specification needs to be
able to do.
-/
