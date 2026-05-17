---
type: todo
priority: P3
status: draft
created: 2026-05-16
updated: 2026-05-16
affected-versions: ferrosa v0.10.0 (and likely earlier)
---

# Bug: Raft cold-start takes ~3 minutes to elect a leader after `docker compose down`/`up`

## Why this is a Ferrosa bug

A 3-node ferrosa cluster shut down cleanly via `docker compose down`
(no data loss, MinIO bind-mount preserved, per-node volumes preserved)
should re-converge to a Raft leader in seconds when brought back up
with `docker compose up -d`. Observed convergence on 2026-05-16 was
~3 minutes:

- 18:59:28 — all 3 ferrosa nodes bound their listeners and emitted
  `cluster: pair/cluster connections established`.
- 18:59:41 — `openraft startup begin` on node1 (server_state=Learner,
  is_voter=true, last_committed=7870, last_membership voter_ids set).
- 19:01:58 onward — node1 enters a repeating loop:
  `pre-vote round did not reach quorum; staying follower (no term advance)`,
  one round every ~450ms.
- 19:02:55 — leader elected (node3, `term: 1, node_id: 3689348814741910323`)
  and `raft leader elected, swapping DDL path to Cluster` fires on node1.

That's ~3 minutes of pre-vote churn after pair/cluster connectivity was
already established. During this window, every coordinator read fails
with `Bulk lane send timeout` (see
[[bug-bulk-lane-send-timeouts-on-coordinated-reads]]); recovery only
begins once a leader exists.

## Observed on

- Cluster: `ferrosa-memory-node:v0.10.0` (sha `badbc54253b0`), built
  from main HEAD `b7eb20c`.
- Bring-up via `docker compose down && docker compose up -d`.
- 5 containers reach `healthy` in seconds (docker health check is a
  TCP probe). The "healthy" signal arrives long before Raft converges.

## Suspected scope

- Pre-vote logic may be too conservative when followers come up before
  a leader; quorum check fails on transient lane-not-yet-ready, then
  backs off with a 450ms tick. Cumulative effect: many rounds before
  the system happens to land all three nodes in vote-ready state.
- `openraft 0.9.x` with `loosen-follower-log-revert` may need tuning
  for fast restart with non-empty logs (last_committed=7870).
- The `Bulk lane send timeout` bug above probably feeds back: vote
  RPCs are not on Bulk lane (Raft has its own lane), but bring-up of
  Bulk lane reconnect attempts may starve the Raft thread on send-side
  contention.
- Could also be that `docker compose down` does not deliver SIGTERM
  with enough grace for a clean leader handoff, so the next start has
  to re-elect from scratch with stale `vote` UTime entries.

## Repro

1. `cd ferrosa-suite/ferrosa-memory`
2. Cluster up and healthy.
3. `docker compose down` (waits ~35s for stop_grace_period).
4. `docker compose up -d`.
5. Tail logs: `docker logs -f ferrosa-memory-node1-1 | grep -E "pre-vote|leader"`.
6. Time-from-start until `raft leader elected` log line — ~3min in observed run.

## Why it's load-bearing

The docker health check returns `healthy` once TCP listeners bind,
which is before Raft converges. Smoke scripts and operators who wait
for "all containers healthy" then immediately probe the cluster will
see `not_ready` summaries / timed-out reads for several minutes.

## Fix shape (speculative)

- Add a separate readiness probe that gates on "Raft leader present"
  rather than TCP bind.
- Document the expected cold-start time so smoke scripts can wait
  appropriately (or add a `wait-for-leader` helper).
- Investigate whether a faster pre-vote schedule (smaller tick or
  immediate retry on quorum miss) is safe with the
  `loosen-follower-log-revert` feature.

Related: [[bug-bulk-lane-send-timeouts-on-coordinated-reads]].
