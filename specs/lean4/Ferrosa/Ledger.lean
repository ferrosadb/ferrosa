/-
  The ledger as a data structure: discharge a claim with a theorem, or record it red
  ==================================================================================

  Every area of the corpus can be read as a list of claims, and each claim is either discharged by a
  named theorem or red — stated as a property the code does not have, with the theorem that exhibits
  the failure and a sentence saying what a user sees. Nothing here proves anything about Ferrosa; it
  supplies the vocabulary the other modules use to say what they owe.

  The count of red items in an area is itself pinned by a theorem, so a gap that is closed and a gap
  that is quietly dropped are different events: the first breaks the build until the ledger is
  updated, the second cannot happen without deleting a theorem the book cites.
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

/-- The same predicate as `redCount`, as an explicit function, so a ledger can be filtered. -/
def isRed : SpecItem → Bool
  | ⟨_, .failing _ _⟩ => true
  | _ => false

/-- The complementary predicate: discharged by a named theorem. -/
def isMet : SpecItem → Bool
  | ⟨_, .met _⟩ => true
  | _ => false

/-- `isRed` is the predicate `redCount` counts, spelled as a function. -/
theorem isRed_eq_status_test (i : SpecItem) :
    isRed i = (match i.status with | .failing _ _ => true | _ => false) := by
  obtain ⟨n, s⟩ := i
  cases s <;> rfl

/-- The red count is the length of the ledger filtered to its red items, so counting and filtering
    cannot disagree about how many obligations an area has left open. -/
theorem redCount_eq_filter_length (items : List SpecItem) :
    redCount items = (items.filter isRed).length := by
  have hfun : (fun i => match i.status with | .failing _ _ => true | _ => false) = isRed := by
    funext i
    exact (isRed_eq_status_test i).symm
  unfold redCount
  rw [List.countP_eq_length_filter, hfun]

/-- The names of the red items, in ledger order. This is what the classification bookkeeping in
    `Ferrosa/Unimplemented.lean` is keyed on: a row names one of these, or it names nothing. -/
def redNames (items : List SpecItem) : List String := (items.filter isRed).map (·.name)

/-- The names of the red items are as many as the red count, so the list a classification is keyed
    on covers every unmet obligation and no discharged one. -/
theorem redNames_length (items : List SpecItem) : (redNames items).length = redCount items := by
  simp [redNames, redCount_eq_filter_length]

def printLedger (title : String) (items : List SpecItem) : IO Unit := do
  IO.println s!"{title} ({items.length} items, {redCount items} red)\n"
  IO.println (String.intercalate "\n\n" (items.map render))

end Ferrosa.Ledger
