# ADR-015: Multi-DC — Raft-per-DC + Accord Cross-DC

> Date: 2026-05-09
> Status: Proposed
> Companion to: ADR-013, ADR-014

## Context

Today Ferrosa runs one global Raft group for metadata. NetworkTopologyStrategy at the schema layer is independent of the Raft topology. Operators wanting multi-DC get one Raft group spanning all DCs; cross-DC partition wedges quorum, cross-DC vote latency dominates election time.

The user has chosen: **per-DC Raft + Accord cross-DC**. Accord scaffolding already exists in `ferrosa-cluster/src/accord/`.

## Decision

Each DC runs its own Raft group for that DC's metadata, ring, and tokens. Cross-DC writes use Accord (CEP-15-style leaderless consensus with timestamp ordering). Reads at `LOCAL_QUORUM` hit only the local DC; `QUORUM` fans out via Accord.

### Topology

```
DC1: 3 voters + 1 learner (read replica)
DC2: 3 voters + 1 learner
DC3: deferred (witness role evaluation in Sprint 8)
```

`ModeController` carries `Map<RaftGroupId, Arc<FerrosRaft>>`. `RaftGroupId = Uuid` per DC.

### Cross-DC consistency invariants

1. **Timestamp-ordered apply** (I-27). State machine buffers Accord-marked entries by Accord timestamp; applies in order with a watermark. Watermark advances based on HLC + bounded clock skew.
2. **Idempotent apply** by Accord txn ID (I-28). `state.applied_accord_txns: BTreeMap<TxnId, AppliedRecord>` dedupes recovery retries.
3. **Apply-durability barrier**: Accord vote-commit waits for `wait().applied_index_at_least(...)` post-Raft-commit.
4. **Drain in-flight Accord on DC swap** (I-30): joint-consensus DC swaps wait for all Accord txns referencing the leaving DC's voters to complete or abort.

### Failure modes (from `raft-failure-mode-matrix.md` §9)

| S-NN | Scenario | Handling |
|---|---|---|
| S-48 | DC partition steady state | Each DC retains LOCAL availability; cross-DC blocked. |
| S-49 | WAN flap | CheckQuorum (ADR-012) + Accord recovery; no data loss. |
| S-50 | DC1 dies mid-Accord pre-accept | Accord recovery on DC2; aborts or commits cleanly. |
| S-51 | Joint-consensus DC swap | Drain Accord; commit joint config. |
| S-52 | HLC skew exceeds watermark | Reorder buffer stalls; cross-DC writes pause until skew reduces. |
| S-53 | Witness vote during DC1 failure | Deferred (no witness role today). |
| S-54 | Multi-DC during DDL | Per-DC Raft applies locally; Accord propagates schema cross-DC. |

### Witness replicas — deferred

Spanner-style non-storing voters. openraft has no concept; adding requires touching `quorum/`, `progress/`, `replication/`, and the election-restriction predicate (~2000–4000 LOC). **Defer past Sprint 8.** Until then, run 3 voters + 1 learner per DC.

## Rationale

Per-DC Raft + Accord is leaderless across DCs (good for partition tolerance) and gives strict per-DC linearizability for metadata. Compared to alternatives:

- **One Raft spanning DCs**: every write is cross-DC; latency dominated by inter-DC RTT; partition wedges global quorum. Rejected.
- **Per-DC Raft + 2PC across DCs**: tighter coupling but 2PC has known availability issues (CockroachDB moved away from it). Accord's leaderless protocol is strictly better.
- **Accord-only (no Raft)**: Accord doesn't replace per-DC metadata consensus; it complements it. Schema, ring, token assignment are still Raft.

## Consequences

### Positive

- Per-DC failures stay per-DC; no global outage from DC partition.
- Cross-DC RTO dominated by Accord recovery, not Raft re-election (with ADR-012, both are sub-second).
- Symmetric design: each DC is a peer.

### Negative

- Two membership protocols to reason about per DC (Raft) plus one cross-DC (Accord). More invariants.
- Schema changes propagate cross-DC asynchronously via Accord; brief window of per-DC inconsistency.
- Reorder buffer adds latency to cross-DC writes (one HLC-skew-bounded delay).

### Neutral

- Per-DC operator commands (`add-voter` etc.) are scoped to a DC; operator must specify `--dc=<name>`.

## Open questions

1. **HLC max skew default.** Conservatively `200 ms` in production (per Spanner's TrueTime ε analogue). Configurable via `FERROSA_HLC_MAX_SKEW_MS`.
2. **Accord txn fan-out for cross-DC writes**: per-DC quorum + cross-DC ack? Or every voter in every DC? **Decision: per-DC quorum + 1 per-other-DC ack.** Lowest-latency safe choice; matches Accord's "fast path".
3. **DR semantics if a whole DC dies.** RPO=0 from caller's perspective for committed writes (Accord recovery on surviving DCs handles); RPO unbounded for in-flight pre-accepts at moment of DC death. Document.

## Acceptance criteria (Sprints 6–7)

- Sprint 6: 3+3 dual-DC topology brings up two healthy per-DC Raft groups; LOCAL writes succeed; cross-DC writes return `NotImplemented`.
- Sprint 7: cross-DC writes via Accord work; bank workload at QUORUM holds invariant for 1h under `dc-partition + dc-slow` nemeses.
- Sprint 7: TLA+ multi-DC model checks at N=2 DCs × 3 voters; no safety violations.
- DC swap (joint consensus) works: `MembershipChanger::swap_dc(DC1, DC3)` in one operation.

## References

- Cassandra CEP-15 (Accord).
- Spanner replication docs.
- CockroachDB SIGMOD 2022 multi-region paper.
- `specs/raft-correctness-plan.md` Sprints 6–7.
- `specs/raft-failure-mode-matrix.md` §9.
- ADR-013 (joint consensus is the basis for DC swap).
