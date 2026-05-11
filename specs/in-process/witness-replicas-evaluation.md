---
type: evaluation
status: complete
sprint: 8
work-item: W8.10
created: 2026-05-09
authors: [agent-sprint-08]
---

# W8.10 — Witness Replicas: Design Evaluation

> Companion to: ADR-014 (learner replicas), ADR-015 (multi-DC),
> ADR-018 (fork openraft into ferrosadb).

## Summary

**Recommendation: DEFER until Sprint 10+.**
**Effort estimate: 2_400 LOC ± 600 LOC, 4–6 engineer-weeks of focused work
on the openraft fork plus 2 weeks of integration on ferrosa-cluster.**

The current path — 3 voters + 1 long-lived learner per DC (ADR-014, W8.2) —
delivers most of the cost benefit witnesses promised at a fraction of the
implementation risk. Witnesses become attractive only when (a) durable
storage costs dominate compute costs and (b) we have hardened the
fork-and-patch openraft workflow far enough that 2_000+ LOC of
quorum-machinery surgery is routine. Today neither holds.

## Background

Spanner-style witness replicas are voting members of a Raft (or Paxos)
group that participate in elections and quorum but do not store the
replicated log or state. The leader replicates the log to peers and
data to non-witness peers; the witness sees only the AppendEntries
metadata it needs to vote. Witnesses are ~1/10 the cost of a full
voter — same network, no disk — and turn an N-DC topology into one
where N–1 DCs can survive a region failure without paying for a full
N-th replica.

## What ferrosa already has (ADR-014 / W8.2)

- `NodeState::Learner { owns_tokens: bool }` lifecycle.
- `MembershipChanger::add_learner_only / promote / demote`.
- `ring.replicas()` and `nts_replicas()` skip non-token-owning learners.
- `coordinator/cl_routing::eligible_replicas_for_cl` excludes learners
  from voter-quorum CLs.
- Per-DC Raft (ADR-015) means a learner with `owns_tokens=false` and
  `repair=false` is approximately the cost shape of a witness:
  AppendEntries-only follower, no token-owning data fan-out.

What ferrosa's learners are NOT:
- They don't vote — a witness is a voter that doesn't store.
- They don't influence quorum sizing — a witness reduces required
  voter count from 3 to 2 in a 2-voter-plus-1-witness DC.
- They don't lower compute cost on the leader — every learner still
  receives every log entry (the openraft replication loop fans out
  identical bytes; only the apply layer differs).

## Cost analysis

Production reference: Fly.io `performance-1x` machine in iad/cdg/nrt
~$25/month each. Storage adds another ~$10/month for the 5-GB
NVMe-backed volume the voter needs.

| Topology | Per-DC nodes | Per-DC monthly | 3-DC monthly |
|---|---|---|---|
| Today (3 voters + 1 learner) | 4 voters-equivalent | $140 | $420 |
| 3 voters / DC, no learner | 3 voters | $105 | $315 |
| 2 voters + 1 witness / DC | 2 voters + 1 witness | $80 | $240 |
| 3 voters + 1 witness / DC | 3 voters + 1 witness | $115 | $345 |

**Witness savings vs the W8.2 default**: ~$180/month on a 3-DC
deployment, ~43%. Significant but not transformative — and the
saving lives mostly in the voter→witness swap, which forces the
quorum from 2-of-3 to 2-of-3-with-witness-tiebreak, an availability
shape the ops team has to learn.

## Engineering effort

The openraft fork (per ADR-018) is the natural home for the change.
A witness role requires touching every place the engine treats
membership as "voters and learners":

| Module | Surface | Estimated LOC |
|---|---|---|
| `quorum/` | `Membership::voter_ids`, `quorum_set`, joint-consensus quorum predicates | 600 |
| `progress/` | replication progress per peer; witness gets a pruned copy | 400 |
| `replication/` | strip log payload + state-machine bytes from the AppendEntries the leader sends a witness | 500 |
| `engine/` | election restriction predicates, `vote_handler::handle_vote_req` (witness can vote but not become leader without log) | 300 |
| `raft/` | public surface: `add_witness`, `change_membership(AddWitness/RemoveWitness)` | 200 |
| `raft-types/` | `Membership` serialization round-trip + version pin | 100 |
| **Subtotal (openraft fork)** | | **2_100** |
| ferrosa-cluster integration (`MembershipChanger`, `state.members`, ring, repair) | | 300 |
| **Total** | | **~2_400** |

This is conservative. Witness semantics interact with leadership
transfer (Sprint 3 W3.13) — the witness CAN'T become leader because
it has no log to serve, so `transfer_to(witness_id)` must error out
or auto-redirect. CheckQuorum (ADR-012) needs to count witnesses
correctly (a witness vote DOES count toward quorum heartbeats but
DOESN'T count toward "does the leader still have a healthy log
target"). PreVote already requires log freshness as a precondition;
witnesses by definition don't have log freshness, so PreVote either
needs a witness-aware predicate or witnesses skip PreVote entirely.

The compounding integration with Sprints 3 and 4's openraft patches
is where the timeline stretches. Each patch we have today
(LeadershipTransfer, CheckQuorum, PreVote) is ~150 LOC of fork.
Witnesses bump that backlog by an order of magnitude.

## Risk surface

- **Fork divergence (ADR-018 R1)**: 2_400 LOC is ~5% of the
  openraft codebase. Upstream merges become significantly more
  painful; the rebase cost on every openraft 0.10 / 1.0 RC bumps
  proportionally.
- **Liveness regressions**: witness-aware quorum predicates are
  exactly the kind of place TigerBeetle's "viewstamped replication"
  rewrites lost time. Ferrosa's TLA+ spec (Sprint 5) needs a
  `Witness` variable and a refinement check; that's another
  4 engineer-days on top of the 2_400 LOC.
- **Operator ergonomics**: today an operator says "3 voters + 1
  learner", a familiar Cassandra shape. Witnesses introduce a third
  role that Jepsen, runbooks, and dashboards all have to model.

## Comparison to ADR-014 learners (status quo)

| Property | Learner (today) | Witness (proposed) |
|---|---|---|
| Counts toward quorum | No | Yes |
| Stores log | Yes | No |
| Stores state machine | Yes | No |
| Network bandwidth (steady state) | Same as voter | ~1% of voter (heartbeat metadata) |
| Disk cost | Same as voter | Zero |
| Compute cost on leader | Same as voter | ~1% (no payload formation) |
| Implementation cost | Done (~600 LOC) | ~2_400 LOC + ongoing fork tax |
| Failure-mode coverage | Per ADR-014 | Per ADR-014 + new modes (S-53 from ADR-015 §"Witness vote during DC1 failure") |
| Operator mental model | Cassandra-familiar | New role to teach |

The bandwidth and disk savings are real. But on Fly.io
`performance-1x` at $25/month the savings are ~$10/month/replica,
which is dwarfed by the implementation and ongoing-maintenance cost.

## Decision criteria — when to reopen

Reopen the witness evaluation when ANY of:

1. **Storage cost dominates compute**: the average ferrosa
   deployment's S3 bill exceeds 5× the EC2 bill. Today the ratio is
   ~1:3 the other way (S3 write-behind keeps disk bills low).
2. **A 5+-DC deployment becomes plausible**: at N=5 the 3-voter +
   1-learner shape has 20 nodes; a 3-voter + 1-witness shape has 15
   plus 5 cheap witnesses, a meaningful saving.
3. **openraft 1.0 lands witnesses upstream**: the openraft authors
   have discussed this in #221 but the work hasn't started. If
   upstream takes it on, our cost shrinks to ferrosa-cluster
   integration (~300 LOC) — a clear go.
4. **A regulated deployment requires "voting in 3 jurisdictions
   without storage outside 1"**: data-sovereignty constraints can
   make witnesses the only legal topology. We have no such
   customer today.

## Recommendation

**DEFER. Re-evaluate in Sprint 10 after openraft 1.0 lands (W8.11).**
If openraft 1.0 ships with witnesses, the cost collapses; if not,
the W8.2 long-lived-learner topology is the right place for the
foreseeable next year of ferrosa deployments.

If we DO undertake it later, the implementation order is fixed:

1. TLA+ spec extension (`Witness` variable, refinement check) — 1 week.
2. openraft fork PRs in dependency order: `raft-types` → `quorum/` →
   `progress/` → `replication/` → `engine/` → public `raft/` API. 4 weeks.
3. ferrosa-cluster integration: `NodeState::Witness` variant,
   `MembershipChanger::add_witness/promote_witness_to_voter`, ring +
   repair updates, CL routing (witnesses count toward quorum but
   never serve reads). 2 weeks.
4. Jepsen tier extension: `tier-witness` workload exercising
   2-voter+1-witness DCs under partition + DC death. 1 week.

**Total: 8 engineer-weeks calendar with one engineer; 4 weeks
calendar with two. Budget approval prerequisite: a customer or
internal use case that makes the $180/month/cluster saving
material.**

## References

- ADR-014 § "Open questions" — defers witness role.
- ADR-015 § "Witness replicas — deferred" — names the LOC budget
  (2_000–4_000) that this evaluation refines to ~2_400.
- ADR-018 — openraft fork rationale + ongoing maintenance discussion.
- openraft GitHub issue #221 — witness discussion (open, no
  scheduled work as of 2026-05-09).
- Spanner: "Spanner: Becoming a SQL System" (SIGMOD 2017) §3 —
  witness role in production at Google.
- CockroachDB: "Living without Atomic Clocks" — witness avoidance
  in CRDB (they use 5-replica quorums instead). Useful counterpoint:
  N=5 makes witnesses unnecessary at moderate cost.
