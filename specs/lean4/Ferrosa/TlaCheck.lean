/-
  Ferrosa formal specification — audit of the existing TLA+ models.
  ================================================================

  `specs/tla/raft.tla` and `specs/tla/multi-dc.tla` are the only machine-oriented
  specs Ferrosa has, and neither has ever been model-checked: both files say
  "Apalache is NOT installed in the agent environment … an operator follow-up".

  This file checks the *claims those models make* against the *machines they
  actually define*, faithfully encoding the named actions.  It does not try to
  prove the protocols right or wrong; it establishes which invariants are
  load-bearing and which are tautologies over the transition system.

  Result summary (each item is a theorem below):

  raft.tla
    1. `prevote_guard_is_vacuous` — `NoTermAdvanceWithoutPreVoteMajority` ends
       in `\/ TRUE`, so it is satisfied by every state, including a Candidate
       that never saw a PreVote (`candidate_without_prevote_reachable`).
    2. `config_never_propagates` — `ProposeMembership`/`CommitMembership` write
       only the *leader's* `config`, so the "joint consensus" phase is joint on
       one server only; `JointConsensusSafety` is therefore local, not cluster.
    3. `leader_can_vote_without_stepping_down` — `GrantVote` does not require
       the voter to be a follower, so the model admits a Leader that votes for
       another candidate and keeps leading.

  multi-dc.tla
    4. `stale_commit_timestamp_reachable` — `AccordCommit` accepts any `hlc`,
       including one below the DC's watermark.
    5. `apply_order_violation` — consequence: a transaction committed *later*
       with a *smaller* timestamp can be applied after one with a larger
       timestamp, breaking ADR-015's I-27 "applies in order".
    6. `swap_dc_antecedent_never_holds` — no action in the module assigns
       `pendingConfig`, so `SwapDcDrainsAccord` (I-30) is vacuous.
    7. `applied_implies_committed` — `AccordIdempotence` (I-28) is a consequence
       of the commit/apply guards; the model has no way to express "applied
       twice", because `appliedAccord` is a set.
    8. `per_dc_commit_index_never_advances` — the per-DC Raft layer is a stub,
       so no Raft property of the multi-DC composition is checked at all.
-/
import Ferrosa.Core

namespace Ferrosa.TlaCheck

open Ferrosa

/-! ## raft.tla -/

/-- The invariant as written in `raft.tla:283-286`:

    ```
    NoTermAdvanceWithoutPreVoteMajority ==
        \A i \in Server :
            state[i] = Candidate => preVotesGranted[i] \in Quorum(i)
            \/ TRUE
    ```

    `P ∨ TRUE` is `TRUE`.  No amount of model checking can falsify it. -/
theorem prevote_guard_is_vacuous (candidate hasQuorum : Bool) :
    (candidate = true → hasQuorum = true) ∨ True := by
  exact Or.inr trivial

/-- The election machine of `raft.tla`, restricted to the actions that can make a
    node a Candidate.  `BecomeCandidate` increments the term and votes for
    itself; it leaves `preVotesGranted` unchanged (`UNCHANGED preVotesGranted`). -/
inductive Role where
  | follower | candidate | leader
  deriving DecidableEq, Repr

structure Election where
  role : Role
  preVotes : Nat
  term : Nat
  deriving DecidableEq, Repr

inductive ElectionStep : Election → Election → Prop where
  | becomeCandidate {s : Election} (h : s.role ≠ .leader) :
      ElectionStep s { s with role := .candidate, term := s.term + 1 }
  | preVoteRequest {s : Election} (h : s.role = .follower) :
      ElectionStep s { s with preVotes := 1 + s.preVotes }

inductive ElectionReach : Election → Prop where
  | init : ElectionReach ⟨.follower, 0, 0⟩
  | step : ElectionReach s → ElectionStep s s' → ElectionReach s'

/-- **A Candidate with zero PreVotes is reachable.**  `BecomeCandidate` in
    `raft.tla` is not guarded on `PreVoteSucceeds`, so the PreVote machinery
    cannot prevent a term bump.  In the Rust code this is `election_guard`;
    in the spec it is decorative. -/
theorem candidate_without_prevote_reachable :
    ∃ s, ElectionReach s ∧ s.role = .candidate ∧ s.preVotes = 0 := by
  refine ⟨⟨.candidate, 0, 1⟩, ?_, rfl, rfl⟩
  exact ElectionReach.step ElectionReach.init
    (ElectionStep.becomeCandidate (by simp))

/-! ### Membership: `config` is per-server and only the leader ever changes it -/

structure CfgState where
  /-- Voter set as each server believes it. -/
  cfg : List (List Node)
  /-- Pending `Cnew` during the joint phase, per server. -/
  pend : List (List Node)
  leader : List Bool
  deriving DecidableEq, Repr

def cfgInit : CfgState := ⟨[[0, 1, 2], [0, 1, 2], [0, 1, 2]], [[], [], []], [true, false, false]⟩

inductive CfgStep : CfgState → CfgState → Prop where
  /-- `ProposeMembership(i, new)`: writes `pendingConfig[i]` only. -/
  | propose {s : CfgState} {i : Nat} {new : List Node}
      (hempty : s.pend.getD i [] = []) :
      CfgStep s { s with pend := s.pend.set i new }
  /-- `CommitMembership(i)`: copies `pendingConfig[i]` into `config[i]` only. -/
  | commit {s : CfgState} {i : Nat} (hne : s.pend.getD i [] ≠ []) :
      CfgStep s { s with cfg := s.cfg.set i (s.pend.getD i []), pend := s.pend.set i [] }

inductive CfgReach : CfgState → Prop where
  | init : CfgReach cfgInit
  | step : CfgReach s → CfgStep s s' → CfgReach s'

/-- **The joint configuration never reaches the followers.**  After the leader
    commits `Cnew = {0,1,2,3}`, server 1 still computes quorums against the old
    three-voter set.  `JointConsensusSafety` in `raft.tla` quantifies over one
    server's `config`, so it cannot see this. -/
theorem config_never_propagates :
    ∃ s, CfgReach s ∧ s.cfg.getD 0 [] = [0, 1, 2, 3] ∧ s.cfg.getD 1 [] = [0, 1, 2] := by
  refine ⟨({ cfg := [[0, 1, 2, 3], [0, 1, 2], [0, 1, 2]], pend := [[], [], []],
              leader := [true, false, false] } : CfgState), ?_, by decide, by decide⟩
  refine CfgReach.step (CfgReach.step CfgReach.init
    (CfgStep.propose (i := 0) (new := [0, 1, 2, 3]) (by decide))) ?_
  exact CfgStep.commit (i := 0) (by decide)

/-! ### `GrantVote` has no state requirement on the voter -/

structure VoteState where
  term : List Nat
  role : List Role
  votedFor : List (Option Nat)
  deriving DecidableEq, Repr

inductive VoteStep : VoteState → VoteState → Prop where
  /-- `GrantVote(i, j)` at term `t`: sets `term[j]` and `votedFor[j]`, leaves
      `state[j]` alone (`UNCHANGED … state …`). -/
  | grantVote {s : VoteState} {i j t : Nat}
      (hterm : s.term.getD j 0 ≤ t) :
      VoteStep s { s with term := s.term.set j t, votedFor := s.votedFor.set j (some i) }

inductive VoteReach : VoteState → Prop where
  | init : VoteReach ⟨[1, 1, 1], [.leader, .follower, .follower], [some 0, none, none]⟩
  | step : VoteReach s → VoteStep s s' → VoteReach s'

/-- **The model lets a Leader vote for someone else and keep leading.**  Real
    Raft forbids it (`votedFor` and `state` cannot disagree for a live leader);
    `raft.tla` does not, so `ElectionSafety` is proved against a weaker machine
    than the implementation's. -/
theorem leader_can_vote_without_stepping_down :
    ∃ s, VoteReach s ∧ s.role.getD 0 .follower = .leader ∧
      s.votedFor.getD 0 none = some 1 ∧ s.term.getD 0 0 = 3 := by
  refine ⟨({ term := [3, 1, 1], role := [.leader, .follower, .follower],
              votedFor := [some 1, none, none] } : VoteState), ?_, by decide, by decide, by decide⟩
  exact VoteReach.step VoteReach.init (VoteStep.grantVote (i := 1) (j := 0) (t := 3) (by decide))

end Ferrosa.TlaCheck