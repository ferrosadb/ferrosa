---
title: Testing Methodology and Results — 2026-07
status: Internal evidence — not public release guarantees
last_revised: 2026-07-24
executive_summary: >
  Two evidence tracks for the July 2026 work. (1) Distributed correctness:
  a native Rust Jepsen-style harness (ferrosa-jepsen) plus a separate
  Accord/Elle strict-serializability pipeline. Bank conservation under
  DC partition+slow passes, native linearizability + membership checks run in
  CI, and the Elle pipeline found and drove fixes for real Accord ordering
  bugs. The Accord list-append path is strict-serializable — CERTIFIED with Elle
  on a current RF=3 build (2026-07-24, e860ec88): fault-free, valid?=true, 0
  anomalies over 2,400 committed transactions (0 indeterminate). Scope: single-DC
  fault-free; fault-injected and multi-DC Elle certification are the next runs
  (not yet committed). (2) Performance: a week of
  write-path work, validated by a Fly perf-4x A/B, improved steady write
  throughput +48% (7.5k -> 11.1k w/s) and cut write latency ~3x (p99 44 ->
  16.5 ms). The dominant driver is removing a per-request connection-map clone
  that was 33% of write CPU; lexer-allocation elimination, memchr, sharded
  flush, tunable write admission, and a default-off transparent parameter
  cache contribute the remainder.
---

# Testing Methodology and Results — 2026-07

> **Status:** Internal evidence index, not public release guarantees. Correctness
> claims here are scoped precisely to what is *recorded*; pending items are
> called out as pending. See also [`specs/README.md`](../README.md).

This document records the testing **methodology** and **results** for two tracks
of July 2026 work: distributed correctness (Jepsen/Elle/Accord) and the
write-path performance sprint.

---

## Part 1 — Distributed correctness

### 1.1 Methodology (what is implemented)

**Native harness — `ferrosa-jepsen`** (runs under `cargo test`; live tiers gated
behind the `live-infra-tests` feature):

- **Orchestrator** (`ferrosa-jepsen/src/orchestrator.rs`) iterates
  topology × concurrency × driver × nemesis × workload, records a history, runs
  the checkers, and emits a `RunReport` (JSON + HTML).
- **Workloads** (`src/workload/`): `register`, `bank` (value conservation), 16
  `lwt-*` compare-and-set patterns, `forward-probe`, `membership-churn`,
  `late-join-flood` (21 in the `phase1` registry).
- **Nemeses** (`src/chaos/`): network partition / slow / jitter / loss (iptables
  + tc over SSH), process kill / pause, clock skew / strobe, disk slow / fail,
  WAN / cross-DC, and composed faults.
- **Checkers actually wired** (`src/checker/`):
  - **Linearizability** — a native Rust WGL backtracking checker over a
    single-value register model. This is the linearizability check that runs in
    unit tests / CI.
  - **Membership invariants** (`checker/membership.rs`) — structural checks.
  - **Knossos** (`checker/knossos.rs`) — shells to Clojure/`lein`; only runs when
    a jepsen dir + history file are supplied (not exercised by default).
  - **Elle** (`checker/elle.rs`) — **types only. The `UnifiedChecker` returns
    `elle_result = None`; Elle is not wired into the automated checker path.**
- **Cluster backends**: Docker Compose and caller-provided live CQL contacts are
  the wired backends. A Firecracker backend exists in-tree but is **not wired**
  into the run path (the older "Firecracker-based" description is stale).
- **Endurance simulator** (`src/endurance_sim.rs`): drives a dual-DC bank
  simulation over a 24-simulated-hour horizon under default `cargo test`.

**Accord strict-serializability pipeline (Elle)** — a separate, script/manual
flow, *not* invoked by `cargo test`:

- **Generator** (`ferrosa-jepsen/examples/elle_list_append.rs`): `append(k,v)` =
  `BEGIN; UPDATE la SET v = v + [v] WHERE k = ?; COMMIT` (an Accord transaction);
  `read` = `SELECT ... ` at `SERIAL`; RF ≥ 3. Outcome classification is sound
  (a failed COMMIT is recorded `:info`, never `:fail`).
- **Checker** (`ferrosa-jepsen/elle-checker/src/ferrosa/elle_check.clj`): an
  isolated `lein` project running `elle.list-append/check` under
  `:consistency-models [:strict-serializable]`. Fail-loud (exit 0 only on a
  definitive `:valid? true`). The checker is self-validated: a known-good history
  → `valid? true`; an injected anomaly (`append 1,2` then read `[2 1]`) →
  `valid? false (:G0-realtime)`.
- **Fly cert harness** (`deploy/fly-accord-elle/`): `certify.sh` (3-node RF=3,
  Tigris-S3-backed, always tears down), `certify-nemesis.sh` (minority partition
  + 200 ms WAN via ip6tables/tc), `certify-dc.sh` (dual-DC **scaffold, gated on
  cross-DC replication — not yet functional**).

**CI** — `.github/workflows/jepsen-multi-dc-nightly.yml`: nightly, brings up a
dual-DC (T3) Docker Compose stack and runs
`--tier multi-dc --pattern bank --nemesis dc-partition+dc-slow` for 600 s. It
checks the **bank conservation invariant only** (no Elle, no Knossos in CI),
files a GitHub issue on failure, and uploads logs (30-day retention).

### 1.2 Results (what is recorded)

| Check | Result | Source / caveat |
|---|---|---|
| Bank conservation under `dc-partition+dc-slow` | **Passes** | Nightly CI workflow + dev-reproduced on a 6-node T3 podman cluster after harness-bug fix `3cb39a31`. |
| Native register linearizability + membership | **Run in CI** | `ferrosa-jepsen` unit + integration tests. |
| Elle checker self-validation | **Valid** | Catches injected anomalies; passes known-good histories (`d581b529`). Validates the *checker*, not the DB. |
| **Elle strict-serializability, single-DC RF=3, current build** | **`valid? true`, `anomaly-types: nil`** — **committed evidence artifact** | Certified 2026-07-24 on build `e860ec88`. 2,400 `:ok` / 0 `:info` / 0 `:fail`; 4,800 ops. See `deploy/fly-accord-elle/CERTIFICATION.md` + `elle-cert-e860ec88-20260724T181744Z.edn`. |
| Server-minted transaction id | Drove strict-serializable `:info` from ~24% → **0** | Confirmed in the 2026-07-24 cert (0 of 2,400 indeterminate); commits `d485dd85` / `587f7926`. |

**Real Accord bugs found and fixed through this pipeline** (the pipeline's main
demonstrated value to date):

- **FileSyncWriter directory bug** (`fb6a6ef9`): `transition_to_cluster` handed
  `FileSyncWriter` the Accord directory instead of `protocol.log`, so PreAccept
  persistence hit `EISDIR` and *every* Accord transaction failed "quorum
  unavailable" — the true cause of a vacuous nightly bank run (0/11526). Fixed +
  fail-loud assert; live-verified 403/403.
- **List-append realtime anomaly** (`t_68f226b5`): an Elle run went
  **`valid? false`** with `:strong-PL-1-cycle-exists` / `:G-nonadjacent-item-realtime`
  because per-element list cells were ordered by coordinator-local `now_micros`
  instead of the Accord execution timestamp. Root-caused and fixed
  (`9236d30b`, `960bd2f6`, `f378aaf0`, HLC witness ingestion `04ebfd04`/`af1f5d4e`).

### 1.3 Certification scope and remaining gaps

**Certified (2026-07-24, build `e860ec88`):** single-DC, RF=3, fault-free Accord
`list-append` is **strict-serializable** — `valid? true`, no anomalies, over a
2,400-op history with 0 indeterminate outcomes. This is a reproducible run with a
committed evidence artifact (`deploy/fly-accord-elle/CERTIFICATION.md`). It
supersedes the pre-fix `valid? false` run (`t_68f226b5`), retained as
`elle-fly-history-2026-07-20-PREFIX-valid-false.edn`.

Still **not** covered — do not overclaim these:

- **Fault-injected Elle certification is not yet committed.** `certify-nemesis.sh`
  (minority partition + 200 ms WAN) has a commit-message `valid? true` claim
  (`587f7926`) but no committed history artifact. The 2026-07-24 cert above is
  **fault-free**.
- **Dual-DC (T3/T4) Elle** is scaffold-only (`certify-dc.sh`, gated on cross-DC
  replication). Real 24 h multi-DC endurance runs on the *simulator*, not a live
  cluster.
- **Elle is not wired into CI or the automated `UnifiedChecker`.** CI checks bank
  conservation only (nightly). The strict-serializability certification is a
  **manual, reproducible** run. "All three checkers agree" (Rust + Knossos + Elle)
  remains a plan.
- **Conservation is necessary but not sufficient** for strict serializability
  (stated explicitly in `3cb39a31`).
- **Unwired / scaffold-only:** the Firecracker backend and the polyglot
  (Python/Go/Node/Java/C#) driver matrix.
- The large formal plan in `specs/todo/jepsen-e2e-test-plan.md` (18,432
  combinations, 6 drivers, Firecracker + geo, three-checker agreement) is
  **aspirational** (it lives in `todo/`); do not cite its numbers as achieved.

**Bottom line for a release:** it is accurate to say ferrosa's Accord transaction
path is **strict-serializable — certified with Elle on a current RF=3 build
(2026-07-24), fault-free, 0 anomalies over 2,400 committed transactions**, that
the pipeline **found and fixed real transaction-ordering bugs**, and that **bank
conservation holds under DC partition + slow**. Scope the claim to single-DC
fault-free; fault-injected and multi-DC Elle certification are the next runs.

---

## Part 2 — Write-path performance (July 2026)

### 2.1 Methodology

- **Fly A/B harness** (`deploy/fly-bench/`): builds a server+loadgen image,
  forms a perf-4x (4 performance vCPU, 8 GiB) 3-node RF=3 cluster on Fly.io
  (Tigris-S3-backed), drives `ferrosa-loadgen --profile write_heavy`, and
  captures steady per-second throughput, client-side latency percentiles, a
  `/proc/stat` CPU-busy% delta, and a `perf -F 199 -g` flamegraph of the
  coordinator process (rendered with inferno).
- **Micro-benchmarks** (`ferrosa-cql/benches/`): criterion harnesses for the
  lexer per-token-class costs and the transparent-cache resolve paths.
- **Attribution** by comparing flamegraph leaf/tower shares before vs after each
  change, and by isolating each optimization in the micro-bench.

### 2.2 The optimizations (each: method → result)

| Change | Commit | Method | Result |
|---|---|---|---|
| **Connection counter → shared atomic** | `e0c595ee` | Flamegraph showed the per-request `ConnectionTracker` did an rcu/`ArcSwap` whole-map (`hashbrown`) clone. | **~22–33% of write CPU eliminated** (33% under the A/B load). The single biggest win. |
| Lexer: eliminate per-identifier heap alloc | `e1e178ef`, `d2344ffa` | Uppercase keyword lookup into a stack buffer; bulk-copy string literals instead of byte-by-byte push. | INSERT lex −31%, strings −58% (criterion). |
| Lexer: `#[inline]` hot methods + memchr + classification table | `43702a1f`, `ec32227d` | Inline the per-token methods; `memchr` for the string-literal scan; branchless `[bool;256]` table for the identifier run. | class_strings −21%, class_idents −11% (criterion). |
| Configurable write admission | `75543a06` | The coordinator's in-flight write cap was a hardcoded 128 (a raft-starvation band-aid); made it env-tunable. | Baseline throttled at 128 (68% CPU); raising it lets the box reach 82% CPU under load. |
| Sharded parallel SSTable encode + parallel fsync | `13f6fd2c`, `b87537f6` | Encode (~98% of flush time) was single-threaded per SSTable; shard the token-sorted partitions across a bounded rayon pool. | ~3–4.7× faster encode **when flush-bound** (not the write floor here). |
| O_DIRECT DirectWriter (dark, env-gated off) | `0d0989ef` … | Page-cache-bypassing sequential writer, A/B'd. | **No benefit** in these runs — kept dark/off, documented. |
| CFS-inspired fair-share scheduler (B1–B3) | `5f4e51ec` … | vruntime weighted admission authority + two-level group queue + bounded bulk-I/O permits. | Isolates scan bursts from interactive/consensus work (correctness/QoS, not raw write throughput). |
| **Transparent parameter cache** (default OFF) | PR #291 | Normalize repeated inline-literal INSERTs to a skeleton, verify once against a real parse, then bind decoded values through the existing prepared-insert fast path. | **46% per-parse** (cache hit 229 ns vs full parse 427 ns, criterion). ~**+2%** cluster throughput — small, because a hit still re-lexes the literal values. |

### 2.3 Fly A/B — cumulative result

- **BEFORE** = `247c20dd` (pre-#290: none of the above).
- **AFTER** = all of the above merged + the transparent cache **ON**.
- perf-4x, 256 writers, 120 s `write_heavy`, single-coordinator loadgen.

| Metric | BEFORE | AFTER | Δ |
|---|---|---|---|
| Steady throughput | 7,520 w/s | 11,148 w/s | **+48% (1.48×)** |
| Write p50 | 16.5 ms | 5.5 ms | **3.0× lower** |
| Write p99 | 43.9 ms | 16.5 ms | **2.7× lower** |
| Write mean | 17.6 ms | 5.9 ms | **3.0× lower** |
| CPU-busy% (`/proc/stat`) | 68% (128-cap throttled) | 82% | — |

(Totals: 904,355 vs 1,337,714 writes over 120 s. Caveat: the AFTER run's readers
starved to 0 while BEFORE served ~23k reads @ 192 r/s — negligible against the
write delta.)

**Attribution (flamegraph):** the BEFORE profile is dominated by the 33.5%
`ConnectionTracker` clone tower; AFTER it is gone. `Lexer::advance`'s *share*
grows 12.6% → 25.7% only because the 33% tower was removed (same absolute lexer
work, smaller total). The **connection-tracker fix is the dominant driver**;
lexer + write-admission + the cache are the remainder.

**Honest framing for release copy:** lead with the cumulative *"write data path
is 1.48× throughput and ~3× lower tail latency than the previous release,"*
credited to the connection-tracker fix + lexer work + tunable write admission.
Introduce transparent parameter caching as a **new, opt-in (default-off)**
feature for inline-literal workloads, quoting its **per-parse** number, not a
cluster-throughput figure it does not support.

### 2.4 Evidence artifacts

Flamegraphs (BEFORE / AFTER / diff), folded stacks, and a results summary are
retained at `~/ferrosa-perf-evidence/2026-07-24-writepath-ab/` (kept out of the
repo — multi-MB SVGs). The full running log of the perf investigation and the
transparent-cache design lives in forge task `t_48d5eeaa`.
