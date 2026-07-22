# T0.6 result — scheduler-B0 no-step-down regression (GREEN)

**Task:** `t_88223ad0` (B0) / epic `t_f9a0500a`. **Date:** 2026-07-22.
**Verdict:** **PASS** — the B0 exit gate is met.

## Setup

- 3-node ferrosa cluster on **fly `shared-cpu-1x`** (the ~6.5% vCPU throttle is
  the starvation race fuzzer), RF=3, region `lax`.
- Workload: seed `load_test.data` (write_heavy, 180 s), then a **64-worker
  full-table `SELECT … WHERE val = 0x… ALLOW FILTERING` scan storm** for 300 s
  (loadgen `--scan-storm`), driven from a separate `performance-2x` client.
- Two arms, same box + same workload, torn down between:
  - **post-fix** — nodes built from B0 `HEAD` (bounded scan-producer pool).
  - **pre-fix** — nodes built from `origin/main` before B0 (tokio's default
    unbounded 512-thread blocking pool).

## Result

| Arm | `readyz` during storm | `storm_jumps` | Verdict |
|-----|----------------------|---------------|---------|
| **post-fix** | node0 `ready` 103/103, node1 `ready` 103/103, node2 `ready` 104/104 | 0 | **PASS** — no step-down |
| **pre-fix**  | node0 `notready` during the storm (leader stepped down), node1/2 ready | n/a (metric predates B0) | **PASS** — step-down reproduced |

Same box, same workload: the **unbounded** build's leader is starved out
(`/readyz` drops → CheckQuorum step-down); the **bounded** build's leader stays
up throughout. That differential is the proof — the bounded pool caps concurrent
blocking scan threads (~1 = `cores − reserved`, vs tokio's default 512), leaving
the raft heartbeat thread schedulable.

## Notes / caveats (honest scope)

- `ferrosa_sched_consensus_headroom_cores` sits at 0 during a scan on these
  boxes because `available_parallelism()` clamps to 1 under the shared-cpu
  cgroup quota (< 1 full core). Headroom is therefore **reported but not gated**;
  the differential is the load-bearing signal. See `config.env`.
- The scan storm is timeout-dominated (a full-table scan of the seed exceeds the
  30 s per-scan cap under the throttle) — the scans still *run* and load the
  scan-producer path; a handful complete. Non-vacuity is established by the
  pre-fix arm reproducing, not by a scan-completion count.
- Follow-ups filed: trustworthy raft term/leadership gauges (`t_310ad227`) and
  verifying the P0-17 election-guard actually spawns in the deployed bootstrap
  path (`t_1575bb4a`).

Reproduce: `deploy/fly-sched-b0-regression/run-all.sh --i-will-pay` (dry-run
without the flag). Artifacts land in `target/fly-sched-b0-regression/`.
