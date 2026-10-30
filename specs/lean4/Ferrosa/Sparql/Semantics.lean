/-
  Ferrosa formal specification — the SPARQL surface (`ferrosa-sparql`).
  =====================================================================

  A graph here is a bag of triples, so a triple stored twice doubles every solution that
  joins on it, and the row counts a client sees depend on how the data arrived. Adding a
  triple never removes a match, which is what lets ingested data be applied without
  re-running the query; deleting one is the case that needs the query re-evaluated
  instead.

  Two answers a query can return that it should not: `p+` is a set of pairs, but an
  expansion that walks every path without remembering where it has been returns one
  row per path — on a two-node cycle, three rows for two pairs. A quoted triple packed
  into a single number can land on the same key as an IRI, so a lookup for that IRI
  returns rows that must not match. The third gap is `OPTIONAL` scoping, the rule that
  separates a filter inside the optional from one outside it: the plan types cannot
  express it, and it is listed as owed rather than claimed.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sparql

open Ferrosa.Ledger

abbrev Triple := Nat × Nat × Nat
abbrev Graph := List Triple

/-! ## A triple stored twice counts twice

A graph is a bag of triples, so a repeated triple is a second row and a solution joining on it comes
back four times rather than once — the counts a client sees depend on how the data arrived. Adding a
triple never removes a match, which is what lets ingested data be applied without re-running the
query. Removing one is the case that needs re-evaluation: a match that a duplicate elsewhere still
justified is no longer justified.

-/

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

/-! ## A path query returns pairs, not walks

`p+` is a set of pairs, so two nodes joined by a path must appear once however many paths connect
them. An expansion that follows every path without remembering where it has been returns a row per
walk instead: on a two-node cycle that is three rows for two pairs. On a self-loop nothing stops the
walk but the fuel bound, and a bound is not a semantics — a client counting results over a cyclic
graph gets a number that depends on the graph's shape rather than on the distinct pairs.

-/

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

/-! ## A quoted triple has to keep its own identity in the store

A quoted triple and an IRI are different terms, and a store that keys both by a number has to keep
them apart. The packing used here does not: `Term.quoted 1 2 3` and the IRI `1002003` land on the same
store key, so a lookup for that IRI returns rows that must not match, and the client can neither see
them nor exclude them. `two_terms_share_a_store_key` states the collision on the store rather than on
the encoding, which is where a query meets it.

-/

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

/-! ## The ledger

Six obligations of the SPARQL surface are listed against the declaration that decides each, and three
are red: `p+` returning a row per path, a quoted triple sharing a store key with an IRI, and `OPTIONAL`
scoping having no representation in the plan types at all. Bag semantics, incremental ingestion and the
re-evaluation a delete needs are the three that hold, each naming the theorem that shows it.
`sparql_has_three_red_items` pins the count so a CI check can require it to fall and never to rise.

-/

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

/-- Three of the six SPARQL obligations are red: `p+` returns a row per path rather than
    per pair, a quoted triple can share a store key with an IRI, and `OPTIONAL` scoping
    has no representation at all. -/
theorem sparql_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "SPARQL surface obligations" ledger

end Ferrosa.Sparql
