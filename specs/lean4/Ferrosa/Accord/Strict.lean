/-
  Ferrosa formal specification — Accord, part 5: the reorder buffer and
  real-time order.
  ==========================================================================

  Strict serializability asks for one thing more than serializability: the serial order
  has to agree with the order clients saw, so a transaction that completed before another
  began cannot be placed after it.  Accord buys that agreement with a reorder buffer — a
  replica waits out a transaction's deadline before processing it, so any conflicting
  transaction stamped earlier has already arrived.  The deadline formula is an engineering
  premise rather than a theorem: it holds only while measured skew and latency stay inside
  the P99.9 and P99 estimates it is computed from, which is why the premise is named
  (`DeadlineSound`) and kept apart from its consequence — dependency completeness for
  transactions still in flight, which is the `DepComplete` hypothesis
  `Accord/Protocol.lean` needs.  Losing the premise is observable in a run: a
  lower-timestamped transaction that never reached the buffer means the deadline was set
  from stale numbers, not that the argument failed.
-/
import Ferrosa.Accord.Protocol

namespace Ferrosa.Accord

/-! ## The deadline formula §7.1 sets the buffer's clock from -/

/-- `Deadline(t0, C, P) = wall_clock(t0.time) + SkewMax + maxLat - Latency(C,P)`,
    transcribed.  All quantities are nanoseconds. -/
def deadline (t0Time skewMax maxLatency latencyCP : Nat) : Nat :=
  t0Time + skewMax + maxLatency - latencyCP

/-- The deadline never precedes the proposing timestamp itself, given a latency
    estimate no larger than the worst-case latency. -/
theorem deadline_ge_t0 (t0Time skewMax maxLatency latencyCP : Nat)
    (h : latencyCP ≤ skewMax + maxLatency) : t0Time ≤ deadline t0Time skewMax maxLatency latencyCP := by
  unfold deadline
  omega

/-- A later timestamp gets a later deadline: the buffer preserves order. -/
theorem deadline_monotone (t₁ t₂ skewMax maxLatency latencyCP : Nat) (h : t₁ ≤ t₂) :
    deadline t₁ skewMax maxLatency latencyCP ≤ deadline t₂ skewMax maxLatency latencyCP := by
  unfold deadline
  omega

/-! ## The guarantee the formula rests on, stated as a premise rather than proved -/

/-- "If skew and latency bounds are not exceeded, any conflicting transaction
    with `t0_γ < t0_τ` sent by any coordinator will arrive at P before
    `Deadline(t0_τ, C, P)`."

    This is the empirical claim, not a theorem: it holds while the measured
    skew and latency stay inside the P99.9 / P99 windows the formula is
    computed from.  Stating it separately is what keeps the proof below honest
    about which part is engineering and which part is logic. -/
def DeadlineSound (arrival : TxnId → Nat) (dl : TxnId → Nat) (t0 : TxnId → Nat) : Prop :=
  ∀ γ τ, t0 γ < t0 τ → arrival γ ≤ dl τ

/-! ## What the buffer buys: dependency completeness for in-flight conflicts -/

/-- A replica processes `τ`'s PreAccept only after `τ`'s deadline, and everything
    that arrived by then is in its buffer. -/
def ProcessedAfterDeadline (arrival dl : TxnId → Nat) (buffered : TxnId → List TxnId)
    (τ : TxnId) : Prop :=
  ∀ γ, arrival γ ≤ dl τ → γ ∈ buffered τ

/-- **The point of the buffer.**  Under §7.1's guarantee, every in-flight
    conflicting transaction with a lower timestamp is in the buffer when `τ` is
    processed — which is the missing dependency `Protocol.lean`'s
    `DepComplete` needs, for the in-flight case.  (The committed case is
    covered by recovery; `Recovery.lean` shows what happens when it is not.) -/
theorem deadline_yields_dependency (arrival dl t0 : TxnId → Nat)
    (buffered : TxnId → List TxnId)
    (hsound : DeadlineSound arrival dl t0)
    (hproc : ∀ τ, ProcessedAfterDeadline arrival dl buffered τ)
    (γ τ : TxnId) (h : t0 γ < t0 τ) : γ ∈ buffered τ :=
  hproc τ γ (hsound γ τ h)

/-- Contrapositive, in the form an operator cares about: a transaction that is
    *not* in the buffer despite a lower timestamp is evidence the deadline was
    set from stale skew or latency estimates — i.e. the premise, not the logic,
    failed. -/
theorem missing_dependency_means_premise_failed (arrival dl t0 : TxnId → Nat)
    (buffered : TxnId → List TxnId)
    (hsound : DeadlineSound arrival dl t0)
    (hproc : ∀ τ, ProcessedAfterDeadline arrival dl buffered τ)
    (γ τ : TxnId) (h : t0 γ < t0 τ) (hmiss : γ ∉ buffered τ) : False :=
  hmiss (deadline_yields_dependency arrival dl t0 buffered hsound hproc γ τ h)

/-! ## Real-time order, and why a timestamp needs the buffer behind it

Strict serializability additionally needs the serial (timestamp) order to agree
with real time.  That is a property of timestamp *assignment*, not of the
protocol's messaging, and it is what the buffer's `- Latency(C, P)` term is
protecting: a coordinator that stamps `t0` from its own clock without waiting
out the deadline can stamp a transaction that was invoked later with a smaller
`t0` than one that already completed. -/

/-- Timestamps respect real time: if `a` completed before `b` was invoked then
    `a` is stamped earlier.  The reorder buffer plus HLC gives this while the
    skew bound holds. -/
def TsRespectsRealTime (t0 : TxnId → Nat) (txns : List TxnRec) : Prop :=
  ∀ a ∈ txns, ∀ b ∈ txns, a.completeUs < b.invokeUs → t0 a.id < t0 b.id

/-- The checker-side counterpart, for the histories a run actually produces:
    `StrictlySerializable` is `Serializable` plus this. -/
theorem strict_iff (h : History) :
    StrictlySerializable h ↔
      ∃ order, serializableBy h.committed order = true ∧ realTimeOk h.committed order = true := by
  constructor
  · rintro ⟨order, horder⟩
    unfold strictlySerializableBy at horder
    rw [Bool.and_eq_true] at horder
    exact ⟨order, horder.1, horder.2⟩
  · rintro ⟨order, hser, hrt⟩
    refine ⟨order, ?_⟩
    unfold strictlySerializableBy
    rw [Bool.and_eq_true]
    exact ⟨hser, hrt⟩

end Ferrosa.Accord
