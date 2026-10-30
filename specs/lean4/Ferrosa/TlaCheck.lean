/-
  The TLA+ models: which of their invariants can fail at all
  ==========================================================

  `specs/tla/raft.tla` and `specs/tla/multi-dc.tla` are the only machine-oriented specs Ferrosa has,
  neither has ever been model-checked, and several of the invariants they claim to check cannot fail
  of any state. Encoding the modules' actual actions shows why: `NoTermAdvanceWithoutPreVoteMajority`
  ends in `\/ TRUE`, so it holds of a Candidate that never saw a PreVote;
  `ProposeMembership` and `CommitMembership` write only the leader's own `config`, so the
  joint-consensus phase is joint on one server and `JointConsensusSafety` is a local claim rather than
  a cluster one; and `GrantVote` never requires the voter to be a follower, so the model admits a
  Leader that votes for another candidate and keeps leading. Each of those weakens the machine an
  election-safety argument would be checked against, and no amount of model checking can expose it,
  because a tautology has no counterexample to find. What `raft.tla` actually checks is here; the
  `multi-dc.tla` half of the same audit is `Ferrosa/Accord/AccordMultiDcLean.lean`.
-/
import Ferrosa.Core

namespace Ferrosa.TlaCheck

open Ferrosa

/-! ## What `raft.tla` claims, and the guard that holds of every state

The first invariant covered here is `NoTermAdvanceWithoutPreVoteMajority`, which ends its disjunction with `\/ TRUE` in `raft.tla`. A disjunction with `TRUE` is `TRUE` in every state, so the invariant holds vacuously and model checking can never falsify it. `prevote_guard_is_vacuous` is that rewriting stated once, and the election machine below shows it is not a harmless typo around a property that holds for some other reason: `BecomeCandidate` is not guarded on `PreVoteSucceeds`, so a candidate with zero PreVotes is reachable even if the guard were repaired.
-/

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

/-! ### Membership: `config` is per-server and only the leader ever changes it

`raft.tla` gives every server its own `config` and `pendingConfig`, which is right, but `ProposeMembership` and `CommitMembership` write only the index of the server taking the step. A leader that commits the new configuration therefore leaves the others computing quorums against the old one. `JointConsensusSafety` quantifies over a single server's `config`, so the model cannot express the claim the joint phase is supposed to make, and `config_never_propagates` exhibits the state where the divergence sits.
-/

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

/-! ### `GrantVote` has no state requirement on the voter

`GrantVote(i, j)` sets the voter's term and its `votedFor` and leaves the voter's `state` untouched, which the action spells as `UNCHANGED … state …`. Nothing then stops a Leader from voting for another candidate and continuing to lead, which real Raft forbids because a live leader's `votedFor` and `state` cannot disagree. `leader_can_vote_without_stepping_down` exhibits that state, so `ElectionSafety` is proved against a machine weaker than the implementation's.
-/

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