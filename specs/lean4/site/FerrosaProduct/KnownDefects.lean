import Ferrosa

/-!
# Known defects

These are built, shipping, and wrong: each one tells a user something false today, which is a
different category from a feature that was never written. Every row names a theorem in its area's
ledger that proves the promise does not hold in the model, so the list is evidence, not opinion.

Rows that open with **Model only** are the exception. The Lean model admits the hazard, but the
shipping code was read and does the correct thing, with tests that pin it. They stay on the list
because the ledger item is still red, and the label stops them being read as a defect a customer
meets.

The list below is evaluated from `Ferrosa.Broken.broken`, the same data the corpus classifies, so it
cannot drift from it. The file and line evidence for each row is on the
[Broken module page](Ferrosa/Broken/).
-/

#eval do
  for i in Ferrosa.Broken.broken do
    IO.println s!"{i.area}: {i.what}"
    IO.println s!"  {i.plain}"
    IO.println ""
