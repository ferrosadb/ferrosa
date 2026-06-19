---
type: bug
priority: P1
reported-by: ferrosa-memory phase-0 backfill dry-run (2026-04-17)
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# Read-only workload bloats coordinator memory + wedges CQL protocol

## Observed

On a freshly-started 3-node cluster running ferrosa commit `22d6e11` (and
the `fcad3fa fix(storage): trigger compaction after loading existing
SSTables on startup` on top of `030b74c fix: lane auto-recovery`), a
read-only scan of ~28k rows made node1 climb from **400 MB → 1.85 GB**
(86% of its 2 GiB cgroup cap) and eventually wedged the CQL protocol
listener on all 3 nodes.

Memory snapshots (per `podman stats`), same 3-node cluster, identical
image across all nodes:

| Phase | node1 | node2 | node3 |
|---|---|---|---|
| Post-startup (T+25 s after `compose up`) | 401 MB | 260 MB | 190 MB |
| After 25 min of read-only `SELECT … FROM entity_store WHERE tenant_id = ? ALLOW FILTERING` (27,875 rows iterated) | **1,845 MB** | 633 MB | 638 MB |
| Delta | **+1,444 MB** | +373 MB | +448 MB |

The workload was driven from outside the cluster (Python
cassandra-driver) as a single prepared statement iterated to completion.
No writes (backfill script was in `DRY_RUN=1` mode).

Post-workload, all three ports accept TCP but fail the CQL protocol
handshake with `unpack requires a buffer of 2 bytes` (i.e. the server
closes the connection partway through frame negotiation). Same symptom
we saw pre-lane-recovery.

## Reproducer

```bash
# On a cluster that has ~14k entities × 2 session partitions (≈28k rows)
cd ferrosa-memory
FMEM_CONFIG=$HOME/.config/ferrosa-memory.toml \
  FMEM_DRY_RUN=1 \
  FMEM_CQL_PORT=19043 \
  FMEM_PROGRESS_EVERY=2000 \
  python3 scripts/backfill-embeddings-v2.py
# ~25 min later, observe node1 memory + CQL wedge
```

The script only reads — each row it SELECTs gets handed to an external
Ollama HTTP call and then discarded. No state grows in the client.

## Why the coordinator?

The scan talks to node2 (port 19043) as coordinator; node2's memory grew
~370 MB. node1 and node3 are **only replicas** (not the coordinator) for
this workload, yet node1 grew **4× node2's delta**. This says the leak
isn't coordinator-side at all — it's **replica-side per-read retention**.
Possible mechanisms:

- Read-repair or hinted-handoff data accumulating per read and not
  aging out.
- Query-context objects (result chunks, pager state, prepared-statement
  bindings) piling up in a bounded collection with no eviction.
- SSTable-index decoding buffers not being released after the reader
  moves past them.

node1's dominance in the bloat probably correlates with it holding the
seed-node role + the token range that owns the largest fraction of the
partitions being scanned.

## Relationship to other recent fixes

- `fcad3fa` — fixes *startup* memory by compacting SSTables post-load.
  That's a different code path; it doesn't touch the steady-state read
  path exercised here.
- `030b74c` — lane auto-recovery. Also orthogonal; the lane errors we
  saw here are symptoms of the wedge, not its cause.

So this is a separate, currently-unfixed growth source.

## Why the CQL listener wedges

Once a node's RSS gets within a few hundred MB of its cgroup memory cap,
Linux slows allocations to the point where the CQL listener's read loop
can't buffer a full frame before the client times out. TCP stays up
(because accept() doesn't allocate much) but every new connection
observes a half-written response and logs `unpack requires a buffer of
2 bytes`.

## Desired

A sustained read-only scan of ~30k rows should produce bounded,
release-back memory growth — not a persistent +1.4 GB on any single
node. Acceptable budget: steady-state RSS should not grow by more than
~50 MB for a scan of this size after the query completes.

## Diagnostics to try

1. **Heap profile** (perf or jemalloc profiler) on node1, captured at
   T+0 and T+25 min. Compare retained-object histograms.
2. **Per-query allocation log** — enable any per-query context
   accounting and trace whether `drop` is called on query completion.
3. **Iterate the read with `ALLOW FILTERING` stripped** (requires a
   proper secondary index on `tenant_id`) — if the leak is specific to
   the filtering path, that narrows it.
4. **Disable the graph-layer adjacency index for this scan** — if
   entity_store reads update a graph-side index that has unbounded
   growth, that's the suspect.
5. **Run the same scan against a fresh keyspace with the same row
   count** to rule out the duplicate-session-partition scenario
   (each entity appears in 2 partitions in our data).

## Acceptance Criteria

- [ ] After a 30-minute read-only scan of `entity_store` (any number of
      rows up to the full dataset), no node's RSS grows by more than
      100 MB relative to the post-startup baseline.
- [ ] CQL protocol handshake continues to succeed on all nodes during
      and after the scan (no `unpack requires a buffer of 2 bytes`
      wedge).
- [ ] Follow-up scans (same shape, back-to-back) do not show cumulative
      growth — each scan returns RSS to roughly the same level.

## Related

- `specs/implemented/bug-raft-empty-membership-after-recovery.md` — same
  cluster environment.
- `specs/implemented/bug-bootstrap-streaming-no-handler-registered.md`
  — same cluster environment.
- `specs/implemented/bug-commitlog-segment-leak-oom.md` — earlier
  memory fix, may share machinery with whatever this leak is in.
