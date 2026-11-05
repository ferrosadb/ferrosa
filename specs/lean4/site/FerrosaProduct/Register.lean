import FerrosaProduct.Register.Part1
import FerrosaProduct.Register.Part2
import FerrosaProduct.Register.Part3
import FerrosaProduct.Register.Part4
import FerrosaProduct.Register.Part5
import FerrosaProduct.Register.Part6
import FerrosaProduct.Register.Part7

/-!
# The theorem register

Every theorem in the specification, 772 in all, in the book's order. 52 are red: they prove that
a property the product needs does not hold yet, and each is tracked by a ledger in
`Ferrosa.Unimplemented` until the code is fixed. A theorem marked *counterexample* shows what goes
wrong in a design the code does not use, or a history the checker must reject; it is evidence for a
guarantee, not a defect. The rest are guarantees.

Each part lists its modules, and each theorem links to where it is stated and proved on its module
page. The register is generated from the compiled specification, so it cannot omit a theorem the
corpus contains.

- [I. The query surface](FerrosaProduct/Register/Part1/): 215 theorems, 16 red
- [II. Consistency, replication and topology](FerrosaProduct/Register/Part2/): 171 theorems, 2 red
- [III. What leaves the cluster](FerrosaProduct/Register/Part3/): 62 theorems, 7 red
- [IV. The write path](FerrosaProduct/Register/Part4/): 175 theorems, 19 red
- [V. Foundations](FerrosaProduct/Register/Part5/): 30 theorems, 0 red
- [VI. Evidence](FerrosaProduct/Register/Part6/): 75 theorems, 5 red
- [VII. Limits and accounting](FerrosaProduct/Register/Part7/): 44 theorems, 3 red
-/
