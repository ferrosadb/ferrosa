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

/-! ## Oban's type, concretely -/

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
