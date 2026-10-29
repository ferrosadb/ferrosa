/-
  Ferrosa formal specification — the SPARQL surface (`ferrosa-sparql`).
  =====================================================================

  `ferrosa-sparql` has an engine, a planner, an executor, `filter`, `property_path`, `rdf_star`
  and `update`. Three semantics questions decide whether it can answer correctly, and the third
  is the one that bites:

    1. **Bag semantics.** A graph is a *bag* of triples, and a basic graph pattern is matched as
       the multiset of homomorphisms. A duplicated triple must double every solution
       (`a_duplicated_triple_double_counts`), and `DISTINCT` is a query-level property that a
       plan may not silently push around. The planner carries `distinct : bool`, which is fine
       only if every plan for a query agrees on it.
    2. **Monotonicity.** Adding a triple never removes a solution
       (`adding_a_triple_never_removes_a_match`), so an ingested triple can be applied
       incrementally; a *deleted* triple cannot, which is the asymmetry incremental view
       maintenance has to respect.
    3. **Property paths are a set of pairs.**  `p+` is the transitive closure and the spec
       returns each `(subject, object)` pair once. An expansion that follows every path without a
       visited set returns one row per path — `closure_without_a_visited_set_duplicates_pairs`
       exhibits a two-node cycle producing three rows for two pairs.

  RDF-star adds a fourth: a quoted triple is a term, and any store that keys terms by a number
  must encode it injectively. `encoding_is_not_injective` shows the obvious packing aliasing a
  quoted triple with an IRI, which is a query returning rows it must not.

  Not modelled, and listed red rather than claimed: `OPTIONAL` scoping. The distinction between
  `{ A OPTIONAL { B FILTER(c) } }` and `{ A OPTIONAL { B } FILTER(c) }` is the subtlest rule in
  SPARQL 1.1 and there is no representation of it in the plan types today.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sparql

open Ferrosa.Ledger

abbrev Triple := Nat × Nat × Nat
abbrev Graph := List Triple

/-! ## Bag semantics -/

/-- How many times a triple occurs in the graph. -/
def multiplicity (g : Graph) (s p o : Nat) : Nat := g.countP fun t => t == (s, p, o)

/-- **A duplicated triple double-counts.**  The store is a bag, so a self-join over a triple that
    occurs twice yields four solutions, not one. -/
theorem a_duplicated_triple_double_counts :
    multiplicity [(1, 2, 3), (1, 2, 3)] 1 2 3 = 2
      ∧ multiplicity [(1, 2, 3), (1, 2, 3)] 1 2 3 * multiplicity [(1, 2, 3), (1, 2, 3)] 1 2 3
          = 4 := by
  decide

/-- **Adding a triple never removes a solution.**  This is what makes incremental ingestion
    sound; the same does not hold for deletion, which is why deletes need re-evaluation. -/
theorem adding_a_triple_never_removes_a_match (g : Graph) (t : Triple) (s p o : Nat) :
    multiplicity g s p o ≤ multiplicity (t :: g) s p o := by
  unfold multiplicity
  simp only [List.countP_cons]
  split <;> omega

/-- And the converse fails for deletion: removing a triple can remove a match that a duplicate
    elsewhere still justified is no longer justified. -/
theorem deleting_a_triple_can_remove_a_match :
    multiplicity [(1, 2, 3), (1, 2, 3)] 1 2 3 = 2
      ∧ multiplicity [(1, 2, 3)] 1 2 3 = 1 := by decide

/-! ## Property paths -/

/-- Successors of a node. -/
def succs (g : List (Nat × Nat)) (n : Nat) : List Nat :=
  (g.filter fun e => e.1 == n).map fun e => e.2

/-- Expand `p+` for `fuel` steps **without** a visited set: every path is followed. -/
def expandNoVisited (g : List (Nat × Nat)) : Nat → List Nat → List Nat
  | 0, _ => []
  | fuel + 1, frontier => frontier ++ expandNoVisited g fuel (frontier.flatMap (succs g))

def dedup : List Nat → List Nat
  | [] => []
  | x :: xs => x :: (dedup xs).filter fun y => y != x

/-- **`p+` must return each pair once.**  A two-node cycle expands to three rows for two distinct
    nodes, because the walk has no memory; the spec's closure is a set of pairs. -/
theorem closure_without_a_visited_set_duplicates_pairs :
    expandNoVisited [(0, 1), (1, 0)] 3 [0] = [0, 1, 0]
      ∧ (dedup (expandNoVisited [(0, 1), (1, 0)] 3 [0])).length = 2 := by
  decide

/-- The same for a self-loop, where an expansion without memory never terminates at all given
    unbounded fuel — the fuel bound is the only thing stopping it, which is not a semantics. -/
theorem a_self_loop_diverges_without_fuel :
    expandNoVisited [(0, 0)] 3 [0] = [0, 0, 0] := by decide

/-! ## RDF-star -/

/-- A term: an IRI, or a quoted triple (RDF-star). -/
inductive Term where
  | iri (n : Nat)
  | quoted (s p o : Nat)
  deriving DecidableEq, Repr

/-- The obvious way to key a term by a number in a storage layer. -/
def encode : Term → Nat
  | .iri n => n
  | .quoted s p o => s * 1000000 + p * 1000 + o

/-- **A quoted triple aliases with an IRI.**  The packing is not injective, so a store keyed this
    way cannot tell a quoted triple from the IRI with the same number — and a query for the IRI
    returns rows that must not match. -/
theorem encoding_is_not_injective :
    Term.quoted 1 2 3 ≠ Term.iri 1002003
      ∧ encode (Term.quoted 1 2 3) = encode (Term.iri 1002003) := by decide

/-- The consequence, stated on the store: one key, two distinct terms. -/
theorem two_terms_share_a_store_key :
    ∃ (a b : Term), a ≠ b ∧ encode a = encode b :=
  ⟨Term.quoted 1 2 3, Term.iri 1002003, by decide, by decide⟩

/-! ## The ledger -/

def ledger : List SpecItem := [
  ⟨"triple multiplicity is preserved (bag semantics)",
   .met "Sparql.a_duplicated_triple_double_counts"⟩,
  ⟨"adding a triple never removes a solution",
   .met "Sparql.adding_a_triple_never_removes_a_match"⟩,
  ⟨"deleting a triple requires re-evaluation, not incremental application",
   .met "Sparql.deleting_a_triple_can_remove_a_match"⟩,
  ⟨"`p+` returns each pair once",
   .failing
     "expansion follows every path; a cycle yields one row per path, not per pair, and a self-loop only terminates because of the fuel bound"
     "Sparql.closure_without_a_visited_set_duplicates_pairs"⟩,
  ⟨"RDF-star terms are distinct from IRIs in the store",
   .failing
     "the natural numeric encoding of a quoted triple aliases with an IRI, so a lookup returns rows it must not"
     "Sparql.encoding_is_not_injective"⟩,
  ⟨"OPTIONAL scoping is specified",
   .failing
     "`{A OPTIONAL {B FILTER(c)}}` and `{A OPTIONAL {B} FILTER(c)}` differ; the plan types carry no representation of the distinction"
     "none (not modelled)"⟩
]

theorem sparql_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "SPARQL surface obligations" ledger

end Ferrosa.Sparql
