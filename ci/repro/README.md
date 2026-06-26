# Repro harnesses — post-DDL `count(*)` undercount investigation

Reproduction scripts + instrumentation from the investigation into a reported
`SELECT count(*)` undercount (forge `t_8c4e44e8`): a bare `count(*)` on a
freshly-created, freshly-loaded table was reported to return fewer rows than are
present (observed values {12, 15, 23} for 50 rows) while a full `SELECT` of the
same table returns all 50.

## OUTCOME (2026-06-18): could NOT reproduce under controlled conditions

Despite an extensive campaign across two servers, both CQL protocol versions,
raw + driver clients, and ~a dozen Fly machines, **the undercount does not
reproduce in any controlled setting.** Every measurement made with a *robust*
harness (one that separates connection-errors from genuine wrong-value
undercounts) shows `count(*)` is correct.

| Test | Server | Protocol | Result |
|------|--------|----------|--------|
| robust churn + `stress-ng` | Apache Cassandra 5.0 (Fly) | v5 (default) | 173 correct, **0 undercount**, 7 conn_err |
| robust churn + `stress-ng` | ferrosa (Fly, `main`) | v4 (forced) | 400 correct, **0 undercount** |
| robust churn + `stress-ng` | ferrosa (Fly, `main`) | v5 (default) | 400 correct, **0 undercount** |
| raw protocol, fresh conn | ferrosa (local) | v4 | 180/180 correct |
| raw protocol, **pipelined** + heavy load | ferrosa (local) | v4 | 5,760 counts, **0 mismatch** |
| 1 persistent conn (warm + cold ks) + load | ferrosa (local) | v5 | **0 undercount** |
| 6 pooled persistent conns + load | ferrosa (local) | v5 | **0/240** |
| `count_range` unit tests | — | — | pass (in `ferrosa-storage/src/store.rs`) |

The **only** time undercounts were observed was with the *non-robust* legacy
harness (`count_ddl_hammer_legacy.py`, fresh-`Cluster()` per query, no
exception isolation) in an early local window on a heavily-overloaded laptop
(57–79/240, values {12,15,23}), plus the original report against the shipped
nightly. None of the controlled follow-ups reproduced it.

### Hypotheses raised and DISPROVEN (so we don't repeat them)
- **empty `partition_key` post-DDL → empty PK lookup** — a guard making
  `extract_pk_values` reject empty `pk_names` was RED→GREEN at the unit level but
  had **zero** effect on the integration repro. Reverted.
- **write-visibility lag** — re-measuring at +0.5s/+2s, a low count did not
  converge; but the full scan was always immediately correct.
- **`count_range` / metadata-merger defect** — proven correct in isolation
  (the `count_range_counts_every_partition_*` unit tests).
- **CQL stream-id misdelivery on pipelined v4** — raw pipelined client is
  correct even under heavy load (5,760 counts, 0 mismatch).
- **ferrosa v5 framing** — default-negotiated v5 churn against ferrosa under
  stress is 400/400 correct.
- **"python-driver fresh-`Cluster()` artifact"** (an earlier interim
  conclusion) — also unsupported: the same driver/pattern is clean against
  Cassandra (173/173) *and* against ferrosa v5 (400/400) under stress.

### Honest conclusion
`count(*)` is correct in every controlled test. The early undercounts were
either a fragile race requiring a specific machine-overload state that could not
be recreated, or artifacts of the non-robust harness under thrash — and the two
cannot be distinguished without a live reproduction, which was never recaptured.
**No root cause was pinned and no fix was made.**

If this recurs in the wild, capture (1) the exact build/version, (2) a wire
trace (`tcpdump` on 9042) at the moment of the wrong value, and (3) whether a
*persistent/pooled* connection also sees it — since on-demand reproduction has
not been achievable. Untested variables: the exact shipped-nightly binary
(`v2026.06.16.2048`, release) vs `main`; a debug build under load on Fly.

## Files

| File | What it does |
|------|--------------|
| `count_ddl_hammer.py` | Robust DDL+count fuzzer (per-iter `CREATE KS/TABLE` + 50 inserts, then `count(*)` + full `SELECT` from a fresh connection; catches exceptions; re-measures at +0.5s/+2s to separate a real miscount from visibility lag). Args: `[workers] [iters]`. |
| `count_ddl_hammer_legacy.py` | Original **non-robust** version (threads die on the first exception). The only harness that ever showed "defects"; kept for reference. Args: `[workers] [iters] [fresh|reuse]`. |
| `count_converge.py` | Single-connection convergence probe over ~15s. |
| `raw_count.py` | Hand-rolled raw CQL v4 client: fresh socket per count, own STARTUP/AUTH handshake, known stream-id, inspects the raw response header+body. The server-correctness oracle (note: its result parser matches ferrosa's frames, not Cassandra's). |
| `schema_propagation_repro.py` | Cross-connection keyspace-visibility probe. |
| `run_starved.sh` | CPU-starvation race fuzzer: ferrosa under `nice -n 19` + `2×ncpu` busy hogs, runs a harness, prints durable counters. `FERROSA_BIN` / `FERROSA_CFG` / `FERROSA_CQL_PORT`. |
| `count-probe-instrumentation.patch` | Env-gated (`FERROSA_COUNT_PROBE=1`) durable atomic counters across `router.rs` / `connection.rs` / `store.rs` (dumped by an independent OS thread so they survive runtime starvation). Re-apply with `git apply` for future hunts; **not for mainline source**. |

> The Fly harnesses used during the campaign (single-machine Cassandra/ferrosa +
> `stress-ng` + the robust/discriminator python clients) are reconstructable from
> `run_starved.sh` + the python harnesses above; they intentionally are not
> checked in as Fly-specific scripts.

## How to run (local)

```bash
cd ferrosa
git apply ci/repro/count-probe-instrumentation.patch   # optional: durable counters
cargo build -p ferrosa --bin ferrosa
# config with CQL on 127.0.0.1:19042, then:
FERROSA_CFG=/path/to/ferrosa-19042.toml ci/repro/run_starved.sh 6 40
```

Reproduction (if it ever fires again) is most likely under *genuine* heavy
build load layered with `stress-ng`/hogs on a multi-core host, using the
non-robust `count_ddl_hammer_legacy.py`. A CPU-throttled cloud instance was
tried (see workspace memory on shared-CPU race fuzzing) and did **not**
reproduce it with the robust harness.
