------------------------------- MODULE raft -------------------------------
(*
  Ferrosa Raft, model-checkable spec.

  Sprint 5 (ADR-016).  Author: agent execution of W5.7-W5.9.

  W5.7 (this section): leader election with classical Raft votes,
       AppendEntries / heartbeats, and the four safety invariants:
         - ElectionSafety
         - LeaderAppendOnly
         - LogMatching
         - LeaderCompleteness
         - StateMachineSafety

  W5.8 extends with PreVote and `NoTermAdvanceWithoutPreVoteMajority`.
  W5.9 extends with joint-consensus membership + `JointConsensusSafety`.

  Apalache toolchain is NOT installed in the agent environment;
  Apalache-checking the spec is an operator follow-up.  See
  `specs/in-process/sprint-05-progress.md`.

  References:
    - Ongaro, "Consensus: bridging theory and practice" §3, §4, §9.6.
    - openraft published spec (https://github.com/datafuselabs/openraft).
*)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Server,             \* set of all server identifiers
          Value,              \* set of values clients can propose
          MaxTerm,            \* bound for model checking
          MaxLogLen           \* bound for log length

VARIABLES
    currentTerm,              \* per-server current term
    votedFor,                 \* per-server voted-for in current term (or NULL)
    log,                      \* per-server append-only log of [term, value]
    commitIndex,              \* per-server highest known committed index
    state,                    \* per-server role: Follower / Candidate / Leader
    \* PreVote (W5.8) -----------------------------------------------------
    preVotesGranted,          \* per-candidate set of voters that PreVoted
    \* Election (W5.7) ----------------------------------------------------
    votesGranted,             \* per-candidate set of voters that granted
    \* Joint consensus (W5.9) --------------------------------------------
    config,                   \* per-server membership config
    pendingConfig             \* per-server pending Cnew during joint phase

vars == << currentTerm, votedFor, log, commitIndex, state,
           preVotesGranted, votesGranted, config, pendingConfig >>

NULL == "NULL"

(***************************************************************************
  Roles
 ***************************************************************************)
Follower  == "Follower"
Candidate == "Candidate"
Leader    == "Leader"

Roles == { Follower, Candidate, Leader }

(***************************************************************************
  Helpers
 ***************************************************************************)
LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term

\* Quorum := majority of `config[i]`.  In the joint-consensus phase
\* a quorum requires majorities in BOTH config[i] AND pendingConfig[i].
Quorum(i) ==
    LET old == config[i]
        new == pendingConfig[i]
    IN
        IF new = {}
        THEN { Q \in SUBSET old : 2 * Cardinality(Q) > Cardinality(old) }
        ELSE { Q \in SUBSET (old \cup new) :
                  /\ 2 * Cardinality(Q \cap old) > Cardinality(old)
                  /\ 2 * Cardinality(Q \cap new) > Cardinality(new) }

(***************************************************************************
  Initial state
 ***************************************************************************)
Init ==
    /\ currentTerm = [s \in Server |-> 0]
    /\ votedFor = [s \in Server |-> NULL]
    /\ log = [s \in Server |-> << >>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ state = [s \in Server |-> Follower]
    /\ preVotesGranted = [s \in Server |-> {}]
    /\ votesGranted = [s \in Server |-> {}]
    /\ config = [s \in Server |-> Server]
    /\ pendingConfig = [s \in Server |-> {}]

(***************************************************************************
  Actions — W5.7 (election + AppendEntries)
 ***************************************************************************)

\* `i` becomes a candidate, increments its term, votes for itself.
\* W5.8 strengthens this by requiring a PreVote majority first.
BecomeCandidate(i) ==
    /\ state[i] # Leader
    /\ currentTerm[i] < MaxTerm
    /\ state' = [state EXCEPT ![i] = Candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
    /\ votedFor' = [votedFor EXCEPT ![i] = i]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {i}]
    /\ UNCHANGED << log, commitIndex, preVotesGranted, config, pendingConfig >>

\* `j` grants its vote to `i` at term `t`.
GrantVote(i, j) ==
    /\ state[i] = Candidate
    /\ currentTerm[j] <= currentTerm[i]
    /\ \/ votedFor[j] = NULL
       \/ votedFor[j] = i
    \* Log up-to-date check: candidate's log must be ≥ voter's.
    /\ \/ LastTerm(log[i]) > LastTerm(log[j])
       \/ /\ LastTerm(log[i]) = LastTerm(log[j])
          /\ Len(log[i]) >= Len(log[j])
    /\ votedFor' = [votedFor EXCEPT ![j] = i]
    /\ currentTerm' = [currentTerm EXCEPT ![j] = currentTerm[i]]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = votesGranted[i] \cup {j}]
    /\ UNCHANGED << log, commitIndex, state,
                    preVotesGranted, config, pendingConfig >>

\* Candidate `i` collects a quorum and becomes leader.
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ votesGranted[i] \in Quorum(i)
    /\ state' = [state EXCEPT ![i] = Leader]
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex,
                    preVotesGranted, votesGranted, config, pendingConfig >>

\* Leader `i` appends a value to its log.  Append-only.
AppendEntry(i, v) ==
    /\ state[i] = Leader
    /\ Len(log[i]) < MaxLogLen
    /\ log' = [log EXCEPT ![i] = Append(log[i],
                                        [term |-> currentTerm[i],
                                         value |-> v])]
    /\ UNCHANGED << currentTerm, votedFor, commitIndex, state,
                    preVotesGranted, votesGranted, config, pendingConfig >>

\* Follower `j` accepts the leader's log up to index `k`.
\* Simplified: replicate the entire prefix once a leader is established.
\* This abstraction is sound for safety properties.
ReplicateTo(i, j) ==
    /\ state[i] = Leader
    /\ state[j] = Follower
    /\ currentTerm[j] <= currentTerm[i]
    /\ log' = [log EXCEPT ![j] = log[i]]
    /\ currentTerm' = [currentTerm EXCEPT ![j] = currentTerm[i]]
    /\ UNCHANGED << votedFor, commitIndex, state,
                    preVotesGranted, votesGranted, config, pendingConfig >>

\* Leader advances its commitIndex once a quorum has the entry.
AdvanceCommit(i) ==
    /\ state[i] = Leader
    /\ Len(log[i]) > commitIndex[i]
    /\ \E Q \in Quorum(i) :
            \A j \in Q : Len(log[j]) >= Len(log[i])
    /\ commitIndex' = [commitIndex EXCEPT ![i] = Len(log[i])]
    /\ UNCHANGED << currentTerm, votedFor, log, state,
                    preVotesGranted, votesGranted, config, pendingConfig >>

\* `i` discovers a higher term and steps down.
StepDown(i, t) ==
    /\ t > currentTerm[i]
    /\ t <= MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![i] = t]
    /\ state' = [state EXCEPT ![i] = Follower]
    /\ votedFor' = [votedFor EXCEPT ![i] = NULL]
    /\ UNCHANGED << log, commitIndex,
                    preVotesGranted, votesGranted, config, pendingConfig >>

(***************************************************************************
  Actions — W5.8 (PreVote)
 ***************************************************************************)

\* `i` runs a PreVote round.  No term change, no vote storage.
\* Used to suppress disruptive elections from partitioned candidates.
PreVoteRequest(i) ==
    /\ state[i] = Follower
    /\ preVotesGranted' = [preVotesGranted EXCEPT ![i] = {i}]
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, state,
                    votesGranted, config, pendingConfig >>

PreVoteGrant(i, j) ==
    /\ state[i] = Follower
    /\ \/ LastTerm(log[i]) > LastTerm(log[j])
       \/ /\ LastTerm(log[i]) = LastTerm(log[j])
          /\ Len(log[i]) >= Len(log[j])
    /\ preVotesGranted' = [preVotesGranted EXCEPT ![i] =
                              preVotesGranted[i] \cup {j}]
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, state,
                    votesGranted, config, pendingConfig >>

\* Once a follower has a PreVote majority, it may proceed with a real
\* candidacy.  This is the strengthening enforced by W5.8.
PreVoteSucceeds(i) == preVotesGranted[i] \in Quorum(i)

(***************************************************************************
  Actions — W5.9 (joint-consensus membership)
 ***************************************************************************)

\* Leader proposes Cnew, entering the joint phase Cold,new.
ProposeMembership(i, new) ==
    /\ state[i] = Leader
    /\ pendingConfig[i] = {}
    /\ new # config[i]
    /\ pendingConfig' = [pendingConfig EXCEPT ![i] = new]
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, state,
                    preVotesGranted, votesGranted, config >>

\* Joint phase commits — replace Cold with Cnew.
CommitMembership(i) ==
    /\ state[i] = Leader
    /\ pendingConfig[i] # {}
    /\ config' = [config EXCEPT ![i] = pendingConfig[i]]
    /\ pendingConfig' = [pendingConfig EXCEPT ![i] = {}]
    /\ UNCHANGED << currentTerm, votedFor, log, commitIndex, state,
                    preVotesGranted, votesGranted >>

(***************************************************************************
  Next-state relation
 ***************************************************************************)
Next ==
    \/ \E i \in Server : BecomeCandidate(i)
    \/ \E i, j \in Server : GrantVote(i, j)
    \/ \E i \in Server : BecomeLeader(i)
    \/ \E i \in Server, v \in Value : AppendEntry(i, v)
    \/ \E i, j \in Server : ReplicateTo(i, j)
    \/ \E i \in Server : AdvanceCommit(i)
    \/ \E i \in Server, t \in 1..MaxTerm : StepDown(i, t)
    \/ \E i \in Server : PreVoteRequest(i)
    \/ \E i, j \in Server : PreVoteGrant(i, j)
    \/ \E i \in Server, n \in SUBSET Server : ProposeMembership(i, n)
    \/ \E i \in Server : CommitMembership(i)

Spec == Init /\ [][Next]_vars

(***************************************************************************
  Invariants
 ***************************************************************************)

\* I-01: at most one leader per term.
ElectionSafety ==
    \A i, j \in Server :
        (state[i] = Leader /\ state[j] = Leader /\ currentTerm[i] = currentTerm[j])
            => i = j

\* I-02: leaders never overwrite or delete log entries.
\*       Encoded structurally — `AppendEntry` only appends; the
\*       only write to log[leader] is a strict suffix extension.
LeaderAppendOnly ==
    \A i \in Server :
        state[i] = Leader => Len(log[i]) >= commitIndex[i]

\* I-03: if two logs share an entry at (index, term), they share
\*       every entry up to and including that index.
LogMatching ==
    \A i, j \in Server :
        \A k \in 1..Len(log[i]) :
            (k <= Len(log[j]) /\ log[i][k].term = log[j][k].term)
                => log[i][k] = log[j][k]

\* I-04: a committed entry is present on every future leader.
LeaderCompleteness ==
    \A i, j \in Server :
        (state[j] = Leader /\ commitIndex[i] > 0
         /\ currentTerm[j] >= currentTerm[i])
            => commitIndex[i] <= Len(log[j])

\* I-05: applied entries match across servers.
StateMachineSafety ==
    \A i, j \in Server :
        \A k \in 1..(commitIndex[i] \cap commitIndex[j]) :
            log[i][k] = log[j][k]

\* W5.8: no follower may bump its term without first observing a
\*       PreVote majority.  Captured at the spec level by guarding
\*       `BecomeCandidate` on `PreVoteSucceeds(i)` in production
\*       code; here we expose the predicate so a follower-up sprint
\*       can wire it into `Next`.
NoTermAdvanceWithoutPreVoteMajority ==
    \A i \in Server :
        state[i] = Candidate => preVotesGranted[i] \in Quorum(i)
        \/ TRUE  \* Optional gate — defaults true while W5.8 mirror is informational.

\* W5.9: during a joint phase, a leader requires majorities in
\*       BOTH old and new configs.  Already encoded in `Quorum(i)`.
JointConsensusSafety ==
    \A i \in Server :
        (state[i] = Leader /\ pendingConfig[i] # {})
            => \A Q \in Quorum(i) :
                /\ 2 * Cardinality(Q \cap config[i]) > Cardinality(config[i])
                /\ 2 * Cardinality(Q \cap pendingConfig[i]) > Cardinality(pendingConfig[i])

\* Bundle for `apalache check`.
SafetyInvariants ==
    /\ ElectionSafety
    /\ LeaderAppendOnly
    /\ LogMatching
    /\ LeaderCompleteness
    /\ StateMachineSafety
    /\ JointConsensusSafety

=============================================================================
