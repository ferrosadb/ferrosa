# Scheduler-B0 no-step-down regression (T0.6)

Live Fly harness for the B0 exit gate of the query-scheduler epic
(`t_88223ad0` / `t_f9a0500a`). It proves the bounded scheduler pool keeps the
Raft leader from being starved out under a full-table `ALLOW FILTERING` scan
storm — the viz workload that stepped node3 down on 2026-07-17.

## What it does

Two arms on one Fly app, run **sequentially** (teardown between):

| Arm | Node image | Client image | Expectation |
|-----|-----------|--------------|-------------|
| post-fix | B0 `HEAD` (bounded pool) | post-fix | **green** — no step-down |
| pre-fix  | `origin/main` pre-B0 (unbounded) | post-fix | **repro** — step-down observed |

The pre-fix arm establishes non-vacuity: the workload must actually be able to
starve an unbounded build, or a green post-fix result proves nothing. The client
always runs the post-fix `ferrosa-loadgen` (only it carries `--scan-storm`).

Nodes run on **shared-cpu-1x** on purpose: the ~6.5% vCPU throttle is the race
fuzzer that lets the dedicated raft thread starve when hundreds of unbounded
blocking scan threads oversubscribe the core. (The O_DIRECT baseline *pins*
`performance` CPUs for the opposite reason — do not copy its VM sizing.)

## Signals (T0.6 T3 exit gate)

Scraped from each node's `/metrics` + `/readyz` every 2 s during the storm:

- `ferrosa_raft_election_storm_term_jumps_total` — must stay **0** (post-fix)
- `ferrosa_raft_current_term` — must stay **stable** (post-fix)
- `ferrosa_sched_consensus_headroom_cores` — must stay **> 0** (post-fix)
- `/readyz` leader presence — must stay **ready** (post-fix); a drop to
  `notready` is the pre-fix step-down signal (pre-fix images predate the
  `ferrosa_raft_*` metrics, so its detection is readiness + log based)
- log substrings (`election storm detected`, `quorum lost`, `raft leader
  elected`, `seen a greater log id`) corroborate.

## Run

```bash
# Dry-run (prints the full plan, bills nothing):
deploy/fly-sched-b0-regression/run-all.sh

# Execute (BILLS Fly):
deploy/fly-sched-b0-regression/run-all.sh --i-will-pay

# Green arm only (skip the repro arm):
RUN_PREFIX_ARM=0 deploy/fly-sched-b0-regression/run-all.sh --i-will-pay
```

Tunables live in `config.env`. Artifacts + verdicts land in
`target/fly-sched-b0-regression/`. Always finish with a teardown:

```bash
FLY_APP=<app> deploy/fly-sched-b0-regression/teardown.sh --i-will-pay --destroy-app
```

## Files

- `config.env` — tunables (VM sizing, refs, workload, gate thresholds)
- `lib.sh` — dry-run-guarded flyctl helpers (nothing bills without `--i-will-pay`)
- `ferrosa-regression.Dockerfile` — plain `COPY . && cargo build` (builds both refs)
- `regression-entrypoint.sh` — env mapping only (no cgroup cap; throttle = VM size)
- `scrape.sh` — per-node `/metrics` + `/readyz` sampler
- `build-image.sh` — build+push one ref (worktree overlay for the pre-fix ref)
- `provision.sh` — one arm's 3 nodes + client (deterministic formation recipe)
- `run.sh` — seed → sample → scan-storm → collect
- `assess.sh` — PASS/FAIL against green|repro (fail-loud on no data)
- `run-all.sh` — orchestrate both arms end to end
- `teardown.sh` — destroy machines (`--destroy-app` to remove the app)
