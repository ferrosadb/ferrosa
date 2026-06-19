# FMEA — Self-healing controller

> Last updated: 2026-06-03
> Companion to [`self-healing-controller-design.md`](./self-healing-controller-design.md)
> Scoring: Severity / Occurrence / Detection each 1–10; RPN = S × O × D. Higher = act first.

A controller that *acts on* the database autonomously is dangerous if wrong. Correctness
and never-worse weigh above availability (fail-loud rule). Determinism is a hard
requirement, so non-determinism is itself scored as a top failure mode.

## Failure modes

| # | Failure mode | Effect | S | O | D | RPN | Mitigation (→ test) |
|---|---|---|---|---|---|---|---|
| 1 | **Quarantine a corrupt gen with no healthy replica to refill** | Permanent data loss — the rows existed only in the quarantined (corrupt) gen | 10 | 4 | 5 | 200 | Before quarantine, confirm a peer replica can serve the affected ranges (RF>1 + reachable). If not, do NOT quarantine — escalate `ERROR` + health=degraded and leave files in place. Single-node / all-replicas-corrupt → never quarantine. Test: no-healthy-replica → no quarantine, escalates. |
| 2 | **Auto-repair OOMs the node it is healing** | Self-healing makes outages worse; OOM-loop | 10 | 5 | 3 | 150 | Every remediation primitive memory-bounded (reader pool, bounded startup, streaming merge, bounded compaction, **bounded repair fan-in**). Controller depends on these; repair fan-in bound is a hard prerequisite. Test: repair under full overlap on N≫fanin holds peak readers ≤ fanin. |
| 3 | **Non-deterministic decision** (RNG/clock/ordering inside `decide`) | Unreproducible, untestable, divergent behavior across nodes | 8 | 5 | 5 | 200 | `decide(snapshot, config)` is pure — no RNG/clock/IO. Determinism test: same snapshot → identical action, run ×100. Cluster initiator from host-id sort, not election. |
| 4 | **Repair thundering herd** — all replicas repair the same range simultaneously | Quorum pressure / load spike / possible unavailability | 8 | 5 | 4 | 160 | Deterministic single initiator per (table, range) = lowest host-id among owners. Test: only one node initiates for a range given a fixed ring. |
| 5 | **Heal-loop thrash** — same action retried forever, never resolves | CPU/IO burn, churn, masks a real problem | 6 | 5 | 4 | 120 | Per-(table,issue) deterministic cooldown + `MAX_ATTEMPTS` → escalate-and-stop (health=degraded, loud ERROR). Test: unresolved condition stops after N, escalates. |
| 6 | **Silent healing** — repairs without logging | Operator blind to recurring corruption/divergence | 7 | 3 | 5 | 105 | Loud `WARN` on every detected issue + `INFO` on every action/outcome + metrics + health surface, independent of remediation. Test: detector trip emits log+metric even with remediation disabled. |
| 7 | **Concurrent remediation race** (two actions at once) | Double-compaction / conflicting swaps / corruption | 9 | 2 | 5 | 90 | Single-threaded control loop; one action per tick; coordinate with the compaction gate. Test: loop never issues a 2nd action before the 1st verifies. |
| 8 | **Fights an in-flight manual `ferrosa-ctl repair` / compaction** | Wasted work, contention | 5 | 4 | 4 | 80 | Check for in-flight repair/compaction before acting; defer (deterministic cooldown). |
| 9 | **Membership churn mid-heal** changes the deterministic initiator | Two initiators or zero; duplicate/again-missing repair | 6 | 3 | 5 | 90 | Recompute initiator from the *current* ring each tick; repair is idempotent (LWW) so a duplicate is harmless; a dropped one re-triggers next scan. |
| 10 | **Detector false positive** (transient state) | Unnecessary action / load | 4 | 4 | 4 | 64 | Deterministic thresholds + cooldown; all actions idempotent + safe + verified; a transient clears on the next snapshot. |
| 11 | **"Effective" verify passes but issue persists** | Controller believes it healed when it didn't | 7 | 3 | 5 | 105 | Verify by re-running the *same* detector post-action; if it recurs within a window, count an attempt and escalate. Don't mark resolved on a single optimistic check. |

## Top risks to design out first

1. **#1 quarantine-without-replica data loss (200)** and **#3 non-determinism (200)** — one
   is irreversible data loss, the other violates the core contract. Gate #1 on a verified
   healthy replica; make `decide` pure + determinism-tested.
2. **#4 thundering herd (160)** — deterministic single initiator.
3. **#2 auto-repair OOM (150)** — the bounded-memory foundation + the repair-fan-in bound
   (prerequisite engine fix).

## Decisions this FMEA forces (owner input)

- **Q1 — Quarantine safety:** require a verified healthy replica before quarantining a
  corrupt gen (recommended; prevents #1). Confirm single-node deployments simply
  escalate-degraded and never quarantine.
- **Q2 — Default posture:** fully automatic (owner already chose) — confirm the master
  switch default (`FERROSA_SELFHEAL_ENABLED=true`) and conservative default thresholds.
