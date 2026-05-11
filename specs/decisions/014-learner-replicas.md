# ADR-014: Learner Replicas

> Date: 2026-05-09
> Status: Proposed
> Companion to: ADR-013 (Membership), ADR-015 (Multi-DC)

## Context

ADR-003 ("Raft for metadata, tunable CL for data") notes "3-5 Raft voter nodes; remaining nodes are learners" but no implementation plan exists. The learner concept is partially supported by openraft 0.9 (`raft.add_learner`) and is used internally during voter promotion (Sprint 1 `MembershipChanger`), but ferrosa exposes no operator API to keep nodes as long-lived learners — they are always promoted to voter immediately.

## Decision

Add a first-class **Learner** role with operator API, distinct from the transient learner-during-add-voter state. Learners:

- Receive `AppendEntries` from the leader.
- Apply log entries to their state machine.
- Serve `LOCAL_ONE` reads (and `LOCAL_QUORUM` reads if the configured RF allows reading from learners — see below).
- Do **not** participate in Raft quorum (vote, replication majority).
- Cannot become leader; cannot vote.
- Are not counted in `committed_cluster_size`; their failure does not affect quorum.

### Operator API

- `ferrosa-ctl cluster add-learner <host_id> <addr>` → `MembershipChanger::add_learner_only(host_id, addr)`.
  - Steps: peer_manager.ensure_peer; network_factory.register_node; raft.add_learner; RaftOp::JoinNode (with `state: NodeState::Learner` — new lifecycle variant).
  - Tokens may be assigned (RF coverage) or not, configurable.
- `ferrosa-ctl cluster promote-to-voter <host_id>` → `MembershipChanger::promote_learner_to_voter` (already in ADR-013).
- `ferrosa-ctl cluster demote-to-learner <host_id>` → `MembershipChanger::demote_voter_to_learner`.
  - Steps: if leader, `transfer_leader` first (Sprint 3); raft.change_membership(`ChangeMembers::RemoveVoters` + `AddLearners`); RaftOp::SetNodeState(Learner).

### Read routing

Coordinator queries the ring + role table:

| CL | Routing |
|---|---|
| `LOCAL_ONE` | any local-DC replica (voter or learner) |
| `LOCAL_QUORUM` | quorum of local-DC voters; learners not counted |
| `QUORUM` | global quorum of voters; learners not counted |
| `ONE` (deprecated) | any replica (voter or learner) |
| `ALL` | every voter and every learner that owns the token |

Learners' reads are slightly stale (bounded by AppendEntries lag, typically <1× heartbeat_interval). For strict-consistency reads, callers use `SERIAL`/`LOCAL_SERIAL` which forces a leader round-trip.

### Token ownership

Learners can own tokens or not, configurable per learner via `--owns-tokens=true|false`:

- **owns-tokens=true**: full read replica; participates in repair; takes ownership share of the ring. Useful for read-only DR replicas.
- **owns-tokens=false**: learner has full state-machine state but doesn't appear in `ring.replicas()` lookups. Useful for analytics nodes or future witnesses.

## Rationale

Learners are required for:
- **Multi-DC read scaling** (ADR-015): DC2 nodes are async learners following DC1's quorum.
- **Capacity expansion before voter promotion**: add a node, let it catch up under read traffic, then promote.
- **DR replicas**: a remote learner that's never promoted; consumed as DR target only.

## Consequences

### Positive

- Multi-DC and capacity-expansion patterns become first-class.
- Quorum sizing is decoupled from node count.

### Negative

- Read routing complexity grows; coordinator must consult role table.
- New `NodeState::Learner` variant; migration of `state.members` lifecycle states (existing: `Joining, Normal, Leaving, Decommissioned`).

## Acceptance criteria

- [ ] `MembershipChanger::add_learner_only`, `promote_learner_to_voter`, `demote_voter_to_learner` implemented.
- [ ] `RaftOp::SetNodeState(Learner)` apply correctness; `state.members[N].state == Learner`.
- [ ] `ring.replicas(token)` excludes learners with `owns_tokens=false`.
- [ ] CL routing per the table above; tested via Jepsen `learner-read` workload.
- [ ] Endurance run (Sprint 8) with 3 voters + 1 learner per DC: zero linearizability violations.

## Open questions

1. **Should learners participate in repair?** If yes, repair payload doubles for an `add_learner_only` node. Default: yes for owns-tokens=true, no for owns-tokens=false. Configurable via `--repair=true|false`.
2. **Should learners be tagged with a DC?** Yes — required for ADR-015. The `NodeInfo.data_center` field already exists.

## References

- ADR-013 (Membership), ADR-015 (Multi-DC).
- `specs/raft-correctness-plan.md` Sprint 8.
- openraft `raft/impl_raft_blocking_write.rs` (add_learner / change_membership semantics).
