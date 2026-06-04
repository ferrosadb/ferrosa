# FMEA — Automatic anti-entropy repair scheduler

> Last updated: 2026-06-04
> Companion to [`automatic-repair-scheduler-design.md`](./automatic-repair-scheduler-design.md)
> Scoring: Severity / Occurrence / Detection each 1–10; RPN = S × O × D. Higher = act first.

An actor that *initiates repair on a live cluster with no operator* can amplify
load, lose data, or thrash. Correctness and never-worse weigh above availability
(fail-loud). Determinism is a hard requirement.

## Failure modes

| # | Failure mode | Effect | S | O | D | RPN | Mitigation (→ test) |
|---|---|---|---|---|---|---|---|
| 1 | **Thundering herd** — every replica initiates the same range at once | RF× load spike, quorum pressure, CQL listener crash (observed: 1 536-session storm) | 9 | 7 | 4 | 252 | Deterministic single initiator = lowest live host-id among range owners; recomputed per tick. Test: given a fixed ring, exactly one node initiates each range. |
| 2 | **Auto-repair OOMs the node it heals** | self-healing makes the outage worse | 10 | 4 | 3 | 120 | Inherit PR #83 bounds (bounded readers/merge/fetch/compaction) + `RepairCoordinator` session semaphore (4) + round-robin one table/cycle. Test: full-overlap repair under N≫fanin holds peak readers ≤ fanin (existing). |
| 3 | **Quarantine without a verified healthy replica** then refill finds none | permanent data loss (the only copy was the quarantined corrupt gen) | 10 | 4 | 5 | 200 | Quarantine gated on `ClusterView` verified-non-corrupt peer (FMEA #1 of the controller). Single-node / unverified → never quarantine, escalate-degraded. Test: no-verified-replica → no quarantine + no refill request. |
| 4 | **Non-deterministic initiator** (RNG/clock/race) | unreproducible, two initiators or zero | 8 | 4 | 5 | 160 | `select_initiated_ranges` is a pure function of (ring, host_id, rf); no RNG/clock. Test: same ring → identical selection ×N. |
| 5 | **Membership churn mid-cycle** flips the initiator | duplicate or skipped repair for a range | 6 | 4 | 5 | 120 | Recompute initiator from the *live* ring each tick; repair is idempotent LWW so a duplicate is harmless; a skipped range re-triggers next tick. Test: initiator recomputation under a node add/drop. |
| 6 | **Fights an in-flight manual `POST /repair`** | double work, contention | 5 | 5 | 4 | 100 | Shared in-flight set keyed by (table, range); skip if already running (manual or auto) + per-table cooldown. Test: in-flight entry suppresses a second initiation. |
| 7 | **Silent repair** — converges without logging | operator blind to recurring divergence/corruption | 7 | 3 | 5 | 105 | WARN on divergence found, INFO per session start/outcome, metrics + health surface, independent of outcome. Test: a divergent run emits log+metric. |
| 8 | **Runaway cadence** — interval too short → continuous repair load | steady-state load, never idle | 6 | 4 | 4 | 96 | Conservative default interval + round-robin spread + cooldown; one table in flight at a time. Test: scheduler initiates ≤ max_concurrent_tables per tick. |
| 9 | **Repairs system/local-only tables** | wasted work, possible metadata contention | 4 | 5 | 4 | 80 | Skip-keyspaces list (system\*); only user keyspaces with RF>1. Test: system keyspaces excluded from selection. |
| 10 | **Refill never happens after quarantine** (port dropped/failed) | quarantined range stays empty until next full cycle | 7 | 3 | 4 | 84 | `RepairTrigger` failure is logged loud + the range is still covered by the periodic cycle as a backstop; health surface shows pending refill. Test: refill request enqueues a targeted range repair; failure logged. |
| 11 | **Wrong RF used** (hardcoded 3 vs keyspace replication) | repairs wrong peer set / misses replicas | 7 | 4 | 4 | 112 | Read RF from each keyspace's replication options, not a constant (the web endpoint's `rf=3` default is a shortcut not to be copied). Test: selection uses per-keyspace RF. |

## Top risks to design out first

1. **#1 thundering herd (252)** — deterministic single initiator (lowest live
   host-id among owners), recomputed per tick. This is the headline risk; the
   1 536-session storm already crashed a CQL listener historically.
2. **#3 quarantine-without-verified-replica data loss (200)** — gate on the
   real `ClusterView`; never quarantine unverified.
3. **#4 non-determinism (160)** — pure `select_initiated_ranges`, determinism-tested.

## Decisions this FMEA forces (owner input)

- **Q1 — enable default** on vs off (self-managing vs opt-in).
- **Q2 — cadence** default interval.
- **Q3 — quarantine→refill** immediate targeted repair vs next-cycle only.
- **Q4 — phase scope** scheduler only vs scheduler + ClusterView wiring.
