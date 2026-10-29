/-
  Ferrosa formal specification — the obligation ledger, shared.
  ============================================================

  Every spec area records what it has proved and what it still owes. An item is either
  discharged by a named theorem or it is red, and a red item names the theorem that
  exhibits the gap. Red items are asserted to exist in a fixed number
  (`<area>_hasN_red_items`), so implementing one without updating the ledger breaks the
  build: the red list can only shrink on purpose.
-/
namespace Ferrosa.Ledger

/-- An item is either discharged by a named theorem or red for a stated reason. -/
inductive Status where
  /-- Discharged; the string names the theorem that proves it. -/
  | met (proof : String)
  /-- Not discharged. `why` says what is missing; `witness` names the theorem exhibiting
      the gap, or is `none (implementation item)` when the gap is not expressible here. -/
  | failing (why : String) (witness : String)
  deriving Repr

structure SpecItem where
  name : String
  status : Status
  deriving Repr

def render (i : SpecItem) : String :=
  match i.status with
  | .met proof => s!"  PASS  {i.name}\n          proved by {proof}"
  | .failing why wit => s!"  RED   {i.name}\n          why: {why}\n          witness: {wit}"

def redCount (items : List SpecItem) : Nat :=
  items.countP (fun i => match i.status with | .failing _ _ => true | _ => false)

def printLedger (title : String) (items : List SpecItem) : IO Unit := do
  IO.println s!"{title} ({items.length} items, {redCount items} red)\n"
  IO.println (String.intercalate "\n\n" (items.map render))

end Ferrosa.Ledger
