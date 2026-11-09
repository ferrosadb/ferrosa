/-
  Ferrosa formal specification — PostgreSQL enum types on the front end.
  ======================================================================

  Oban declares `CREATE TYPE oban_job_state AS ENUM ('available', …)` and later
  `ALTER TYPE oban_job_state ADD VALUE IF NOT EXISTS 'cancelled'`. Ferrosa stores an
  enum-typed column as text and validates every write against the type's labels: a
  value outside the enum is refused (`22P02`), never stored. The type's labels only
  ever grow — PostgreSQL has no `DROP VALUE` and neither does Ferrosa — so a value
  that was a label when it was written is still a label after any `ADD VALUE`.

  Source: `ferrosa-postgres/src/pg_enum.rs` (`EnumType`, `EnumType::add_value`,
  `EnumType::validate`).
-/

namespace Ferrosa.Sql.Enum

/-! ## The type and its labels

An enum is its ordered label list, with no duplicates. `CREATE TYPE … AS ENUM` with a repeated label
is refused (PostgreSQL does the same), so every enum the catalog holds satisfies `Nodup`.

-/

/-- An enum type: its labels, in declaration order. -/
structure EnumType where
  labels : List String
  deriving DecidableEq, Repr

/-- `CREATE TYPE … AS ENUM (labels)`: refused when a label repeats. -/
def create (labels : List String) : Option EnumType :=
  if labels.Nodup then some ⟨labels⟩ else none

/-- A created enum has no duplicate labels. -/
theorem create_nodup (labels : List String) (e : EnumType) (h : create labels = some e) :
    e.labels.Nodup := by
  unfold create at h
  split at h
  · cases h; assumption
  · cases h

/-! ## `ADD VALUE IF NOT EXISTS`

Adding a label that is already present changes nothing; adding a new one appends it. Doing it twice
is doing it once, which is what lets a migration that ran halfway be run again.

-/

/-- `ALTER TYPE … ADD VALUE IF NOT EXISTS l`. -/
def addValue (e : EnumType) (l : String) : EnumType :=
  if l ∈ e.labels then e else ⟨e.labels ++ [l]⟩

/-- **`ADD VALUE IF NOT EXISTS` is idempotent.** -/
theorem addValue_idempotent (e : EnumType) (l : String) :
    addValue (addValue e l) l = addValue e l := by
  unfold addValue
  by_cases h : l ∈ e.labels
  · simp [h]
  · simp [h]

/-- After `ADD VALUE`, the label is declared. -/
theorem addValue_declares (e : EnumType) (l : String) : l ∈ (addValue e l).labels := by
  unfold addValue
  by_cases h : l ∈ e.labels
  · simp [h]
  · simp [h]

/-- **Labels only grow.**  A label declared before `ADD VALUE` is still declared after it. -/
theorem addValue_keeps_labels (e : EnumType) (l v : String) (hv : v ∈ e.labels) :
    v ∈ (addValue e l).labels := by
  unfold addValue
  by_cases h : l ∈ e.labels
  · simp [h, hv]
  · simp [h, hv]

/-- `ADD VALUE` keeps the labels duplicate-free. -/
theorem addValue_nodup (e : EnumType) (l : String) (hn : e.labels.Nodup) :
    (addValue e l).labels.Nodup := by
  unfold addValue
  by_cases h : l ∈ e.labels
  · simp [h, hn]
  · simp only [h, ↓reduceIte]
    rw [List.nodup_append]
    refine ⟨hn, by simp, ?_⟩
    intro a ha b hb hab
    simp at hb
    subst hb hab
    exact h ha

/-! ## Writes are validated, so stored values are labels

A write to an enum-typed column goes through `validate`, which refuses a value that is not a label.
A column built only from validated writes therefore holds only labels, and keeps holding only labels
through any sequence of `ADD VALUE`s.

-/

/-- The refusal a write outside the enum gets (`22P02 invalid_text_representation`). -/
structure InvalidEnumValue where
  value : String
  deriving DecidableEq, Repr

/-- Validate one value against the type. -/
def validate (e : EnumType) (v : String) : Except InvalidEnumValue String :=
  if v ∈ e.labels then .ok v else .error ⟨v⟩

/-- A validated value is a declared label. -/
theorem validated_is_label (e : EnumType) (v w : String) (h : validate e v = .ok w) :
    w ∈ e.labels := by
  unfold validate at h
  split at h
  · cases h; assumption
  · cases h

/-- A value outside the enum is refused, naming the value. -/
theorem outside_value_is_refused (e : EnumType) (v : String) (h : v ∉ e.labels) :
    validate e v = .error ⟨v⟩ := by
  simp [validate, h]

/-- Store a value into a column only if it validates. -/
def write (e : EnumType) (column : List String) (v : String) : Option (List String) :=
  match validate e v with
  | .ok w => some (w :: column)
  | .error _ => none

/-- **A stored value is always a declared label.**  If every value already in the column is a
    label, so is every value after a write that succeeded. -/
theorem write_keeps_only_labels (e : EnumType) (column column' : List String) (v : String)
    (hall : ∀ x ∈ column, x ∈ e.labels) (hw : write e column v = some column') :
    ∀ x ∈ column', x ∈ e.labels := by
  unfold write at hw
  cases hv : validate e v with
  | error err => rw [hv] at hw; cases hw
  | ok w =>
    rw [hv] at hw
    cases hw
    intro x hx
    simp at hx
    cases hx with
    | inl hx => subst hx; exact validated_is_label e v x hv
    | inr hx => exact hall x hx

/-- And an `ADD VALUE` between writes cannot make a stored value stop being a label. -/
theorem stored_labels_survive_add_value (e : EnumType) (l : String) (column : List String)
    (hall : ∀ x ∈ column, x ∈ e.labels) : ∀ x ∈ column, x ∈ (addValue e l).labels :=
  fun x hx => addValue_keeps_labels e l x (hall x hx)

/-! ## Concurrent `ADD VALUE`s do not lose each other

The first design stored the whole label list in one schema entry, so two sessions adding different
labels at the same moment each wrote "the list plus mine" and the later write erased the other's label —
a lost update (`whole_list_rewrite_loses_a_label`). Each added label is now its own schema entry, keyed
by the label: a write inserts one key and never rewrites another, so the two adds commute and both
labels survive in either order (`per_label_adds_commute`, `concurrent_adds_both_survive`).

-/

/-- The schema entries holding added labels: a set of keys, written by insertion. -/
def insertKey (keys : List String) (l : String) : List String :=
  if l ∈ keys then keys else keys ++ [l]

/-- The old design: each session rewrites the whole list it read. -/
def rewriteList (_stored : List String) (readThenAdded : List String) : List String := readThenAdded

/-- **The old design loses a label.**  Both sessions read `[a]`; one writes `[a, b]`, the other
    `[a, c]`; whichever lands second erases the first's label. -/
theorem whole_list_rewrite_loses_a_label :
    let afterBoth := rewriteList (rewriteList ["a"] ["a", "b"]) ["a", "c"]
    "b" ∉ afterBoth := by decide

/-- A label is among the keys after an insertion exactly when it was there before or is the
    label just inserted. Inserting one key neither drops nor invents any other. -/
theorem mem_insertKey (keys : List String) (l x : String) :
    x ∈ insertKey keys l ↔ x ∈ keys ∨ x = l := by
  unfold insertKey
  by_cases h : l ∈ keys
  · simp only [h, ↓reduceIte]
    constructor
    · intro hx; exact Or.inl hx
    · intro hx; cases hx with
      | inl hx => exact hx
      | inr hx => subst hx; exact h
  · simp [h]

/-- **Per-label entries commute**: the same keys result whichever add lands first. -/
theorem per_label_adds_commute (keys : List String) (l1 l2 x : String) :
    x ∈ insertKey (insertKey keys l1) l2 ↔ x ∈ insertKey (insertKey keys l2) l1 := by
  simp only [mem_insertKey]
  constructor
  · intro h; rcases h with (h | h) | h <;> simp [h]
  · intro h; rcases h with (h | h) | h <;> simp [h]

/-- **Concurrent adds both survive**, in either order. -/
theorem concurrent_adds_both_survive (keys : List String) (l1 l2 : String) :
    l1 ∈ insertKey (insertKey keys l1) l2 ∧ l2 ∈ insertKey (insertKey keys l1) l2 := by
  simp [mem_insertKey]

/-! ## Oban's type, concretely

The general results above, applied to the enum Oban actually creates. Oban's v1 migration
declares `oban_job_state`, and a later migration runs `ALTER TYPE … ADD VALUE IF NOT EXISTS
'cancelled'`, possibly more than once. Running it twice must equal running it once, and the new
label must then validate while an unknown label is still refused.
-/

/-- Oban's `oban_job_state` as its v1 migration declares it. -/
def obanJobState : EnumType :=
  ⟨["available", "scheduled", "executing", "retryable", "completed", "discarded"]⟩

/-- **Oban's `ADD VALUE IF NOT EXISTS 'cancelled'` run twice equals run once, and 'cancelled' is then
    a label.** -/
theorem oban_cancelled_is_idempotent :
    addValue (addValue obanJobState "cancelled") "cancelled" = addValue obanJobState "cancelled"
      ∧ validate (addValue obanJobState "cancelled") "cancelled" = .ok "cancelled"
      ∧ validate obanJobState "bogus" = .error ⟨"bogus"⟩ := by
  refine ⟨addValue_idempotent _ _, ?_, ?_⟩ <;> simp [validate, addValue, obanJobState]

end Ferrosa.Sql.Enum
