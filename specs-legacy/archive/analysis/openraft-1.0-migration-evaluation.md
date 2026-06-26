---
type: evaluation
status: complete
sprint: 8
work-item: W8.11
created: 2026-05-09
authors: [agent-sprint-08]
---

# W8.11 — openraft 1.0 Migration: Design Evaluation

> Companion to: ADR-018 (fork openraft into ferrosadb).

## Summary

**Recommendation: HOLD on the fork through Sprint 10. Plan a
migration sprint when openraft 1.0.0 ships a stable release.**
**Effort estimate, when triggered: 6–10 engineer-weeks calendar,
~3_000 LOC of fork-side patches to re-rebase or retire, plus
~800 LOC of API-shape changes in ferrosa-cluster.**

The single largest variable is whether upstream lands `CheckQuorum`
and `LeadershipTransfer` in 1.0 (Sprint 3 patches; high probability)
versus PreVote (Sprint 3 patch; low probability, conceptually
debated by upstream). Today our fork carries roughly 1_500 LOC of
patches; that number will shrink with each upstream merge and is
the primary success metric for the migration.

## Background

ferrosa was forked from openraft 0.9 in Sprint 3 (ADR-018) to land
three Raft correctness fixes that upstream had not shipped:
PreVote, CheckQuorum, and LeadershipTransfer. Each is well-known
literature (Ongaro §6, §3.10, §9), each is a hard liveness
prerequisite for the Sprint 6+ multi-DC topology, and each had been
discussed in openraft GitHub issues for 12+ months without merging.

openraft 1.0 was announced for "first half of 2026" by databend-labs
in their changelog notes; as of 2026-05-09 the latest published
release on crates.io is 0.9.21 (no 1.0 alpha). The 1.0 work tracks
in PR #1024 (databend-labs/openraft).

## Patch inventory in our fork

A `git log` against the fork base picks out the touched modules:

| Patch | Sprint | LOC | Likelihood of upstream merge in 1.0 |
|---|---|---|---|
| **CheckQuorum** | S3 W3.1 | ~250 | High — PR #903 open since 2025-02; upstream maintainers approving in principle. |
| **LeadershipTransfer** (`transfer_to`, `timeout_now`) | S3 W3.13 | ~400 | High — PR #1011, "core team committed", expected in 1.0. |
| **PreVote round** | S3 W3.5 | ~350 | **Low** — upstream prefers a different approach (Joint-NoVote) which doesn't ship for 1.0. |
| `replication_lag_timeout` config knob | S3 W3.4 | ~50 | Already merged upstream (0.9.18). Will collapse on rebase. |
| `transfer_to` from non-leader → ForwardToLeader | S3 W3.14 | ~80 | Likely merged with LeadershipTransfer. |
| Election restriction predicate (apply pre-vote freshness) | S3 W3.6 | ~150 | Bound to PreVote — does not merge in 1.0 if PreVote doesn't. |
| Snapshot pusher: leader-driven InstallSnapshot batch (S4 W4.x) | S4 | ~200 | Medium — discussed in #978 but no PR. |
| Misc: trace events, metric extensions | S2-S5 | ~100 | Low. We don't need these upstream; carry forward. |
| **Total carried** | | **~1_580** | |

### Best case (1.0 lands CheckQuorum + LeadershipTransfer + replication_lag_timeout already in)

We retire ~700 LOC of fork patches. Remaining: PreVote (350) +
election-restriction predicate (150) + snapshot pusher (200) +
misc (100) ≈ **800 LOC carried forward**.

### Worst case (1.0 lands only minor cleanup, no major patches)

We carry ~1_580 LOC and the migration is mostly a rebase against
the API-shape changes openraft 1.0 introduces (its `RaftTypeConfig`
trait may take a different form; `Membership::nodes()` may change
return type; `ChangeMembers::AddVoterIds` may rename).

## API-shape changes likely in 1.0

Tracking the openraft 0.9 → 1.0 PR series:

1. `RaftTypeConfig` becomes a more `const`-friendly bundle —
   `declare_raft_types!` is being rewritten. Our two declarations
   (`FerrosRaftConfig` in `ferrosa-cluster/src/raft/mod.rs` and the
   harness's mirror) need a one-line shim each.
2. `Raft::client_write` may shift to a builder pattern with explicit
   timeout. Our `MembershipChanger` calls (`add_voter`,
   `add_learner_only`, `accord_vote_commit`, `update_metadata`)
   need a 1-line wrapper update each.
3. `RaftError` variants split into `RaftError::APIError(NetworkError)`
   vs `RaftError::APIError(LogicError)`. Today we match
   `RaftError::APIError(ClientWriteError::ForwardToLeader(_))` — that
   variant should keep its name but its parent path may change.
4. `BasicNode` is on track to be replaced by user-provided `Node`
   trait with `addr() / id()` getters. We use `BasicNode` everywhere;
   this is the largest single API change facing us, ~200 LOC of
   touch-up.
5. `RaftMetrics` gains structured fields (`replication: Vec<...>`
   typed). Our metrics consumers (Prometheus exporter,
   `ferrosa-ctl status`) need to be updated.

## ferrosa-side migration cost

| Crate | Touch | LOC |
|---|---|---|
| `ferrosa-cluster::raft` (RaftTypeConfig, Raft handle, network factory) | API rewrite | 250 |
| `ferrosa-cluster::membership` | wrapper updates | 80 |
| `ferrosa-cluster::raft::log_store` | sled SledLogStore conformance to new `RaftLogStorage` trait | 200 |
| `ferrosa-cluster::raft::state_machine` | conformance to new `RaftStateMachine` trait | 120 |
| `ferrosa-cluster::raft::network` | ForwardToLeader handling | 50 |
| Tests (`raft_harness.rs`, integration tests) | conformance | 100 |
| `ferrosa-cluster::raft_forward` (W1.13 ForwardToLeader path) | API touch-up | 30 |
| **Total** | | **~830** |

Bulk: the storage / state machine traits. openraft 1.0 is tightening
those interfaces; our SledLogStore (~1_100 LOC) is the largest
conformance surface.

## Effort estimation

Three-point estimation with the patch inventory + API-shape
analysis:

| Outcome | Optimistic | Most likely | Pessimistic |
|---|---|---|---|
| openraft 1.0 lands by Q3 2026 | yes | yes | no |
| CheckQuorum merges upstream | yes | yes | yes |
| LeadershipTransfer merges upstream | yes | yes | partial |
| PreVote merges upstream | partial | no | no |
| Patch LOC carried | 800 | 1_200 | 1_580 |
| ferrosa-side LOC | 600 | 830 | 1_100 |
| Calendar weeks (1 engineer) | 4 | 7 | 12 |
| Calendar weeks (2 engineers) | 2 | 4 | 7 |

**Expected (most likely): 7 calendar weeks for one engineer; 4 weeks
for two engineers paired.** The pessimistic path is if PreVote
ends up requiring a full re-design rather than a rebase.

## Risk surface

- **Soak time**: openraft 1.0 will have an alpha→beta→RC cycle.
  Migrating during alpha is high-risk; waiting for the .0 release
  is the conservative call.
- **Regression on Sprint 1–7 invariants**: every patch we carry
  was added because a Jepsen-tier failure mode demanded it. If
  the migration drops or alters one of those patches, we lose
  coverage. Migration MUST run the full Jepsen Tier::Standard +
  Tier::MultiDc + Tier::Endurance (sim) suite before the merge.
- **Dependency churn**: openraft 1.0 brings new transitive deps
  (probably `tracing` 0.2, possibly `tokio` 2.0). Lockfile churn is
  proportional.
- **Cassandra-style upgrade testing**: a 0.9 → 1.0 migration on a
  running cluster requires both versions to interoperate during
  a rolling restart. openraft does NOT promise wire compat across
  major versions; we must run a green/blue swap, with the entire
  cluster bouncing inside one orchestration window. ferrosa's
  bootstrap snapshot path (Sprint 4) is the right entry point.

## Dependencies on other Sprints

- **Sprint 9 (W8.10)**: a witness-replica decision is independent;
  if witnesses go ahead, openraft 1.0 work blocks until 1.1 (where
  witnesses might land).
- **Sprint 5 sim/TLA+**: when openraft 1.0 lands, our refinement
  check (Sprint 5 W5.10) may flag transitions that 1.0 changed in
  shape but not semantics. Budget 1 engineer-day to re-tune.
- **Sprint 3 W3.x patches**: each patch's authors should be tagged
  as reviewers on the migration PR — they have the most context
  on what the patch was protecting against.

## Decision criteria — when to migrate

Pull the trigger when ALL of:

1. openraft 1.0.0 has shipped on crates.io (not 1.0.0-alpha).
2. CheckQuorum and LeadershipTransfer are merged upstream and our
   fork can drop those patches cleanly.
3. We have a 2-week window where no other major Raft/Accord work
   is in flight (rebase + Jepsen-tier soak takes the full two).
4. Either ADR-018's "fork tax" has accumulated past 2_500 LOC OR
   a 1.0-only feature blocks a customer commitment.

## Recommendation

**HOLD through Sprint 10.** The fork tax is ~1_580 LOC today and
trending flat — Sprint 7 added zero LOC, Sprint 8 added zero LOC.
The W8.10 witness evaluation already defers; combining the two
evaluations, ferrosa's openraft surface is in a known-good steady
state.

When openraft 1.0 ships:

1. Spike a one-week migration prototype off `feature/raft-gap-close`.
2. Run Tier::Standard + Tier::MultiDc against the prototype.
3. If green, schedule a full migration sprint with two engineers
   paired, 4 calendar weeks. If red, file the regression and revisit.

The migration WILL need to happen — the openraft 0.9 line will not
get security or performance fixes once 1.0 is out — but the cost of
"do it now during alpha" greatly exceeds the cost of "do it once
1.0 is stable".

## References

- ADR-018 — fork openraft into ferrosadb.
- `specs/in-process/sprint-03-openraft-fork.md` — original fork
  rationale + patch list.
- `specs/in-process/sprint-03-openraft-patches.md` — per-patch
  detail (CheckQuorum, LeadershipTransfer, PreVote).
- `specs/raft-correctness-plan.md` Sprint 8 § "openraft 1.0
  evaluation".
- openraft PR #1024 (1.0 tracking).
- openraft PR #903 (CheckQuorum).
- openraft PR #1011 (LeadershipTransfer).
- openraft issue #872 (PreVote upstream debate).
