# Local Elle list-append certification — CQL/Accord path

**Verdict: `valid? true`** — Elle `list-append` under
`{:consistency-models [:strict-serializable]}`, on a live 3-node RF=3 cluster,
both **fault-free** and **under a restart fault schedule**.
`anomaly-types: nil` in both. The raw histories are committed under
[`evidence/`](evidence/); the verdicts below are quoted verbatim from the
checker's own output.

This is the local, single-host leg of the plan's S4 gate
(`specs/plans/read-in-transaction.md`: "Elle `list-append`, `:valid? true`, live
RF=3 + nemesis, no assumptions"). It is **not** a substitute for the Fly
certification in [`../fly-accord-elle/CERTIFICATION.md`](../fly-accord-elle/CERTIFICATION.md),
and it makes **no claim at all about the Postgres wire path** (see "Scope" below).

## What was certified

| | fault-free | fault-injected |
|---|---|---|
| Source under test | `af9fecda` (PR #551 head: the read-set-ordering fix; main merged in post-#550) | same |
| Cluster | `ferrosa-jepsen/tests/docker/elle-cluster-rf3.yml`, 3 nodes, `SimpleStrategy` RF=3, one host, one DC | same |
| Workload | `elle_list_append` `8 300 8 3` — 8 keys, 300 ops/worker, 8 workers, RF 3 | `8 2500 8 3` — sized to outlast the fault schedule |
| Faults | none | `SIGKILL` node3 (replica, 20 s down), then `SIGKILL` node1 (coordinator) |
| History | 4,800 events — **2,400 `:ok`, 0 `:info`, 0 `:fail`** | 40,000 events — **16,547 `:ok`, 3 `:info`, 3,450 `:fail`** |
| **Verdict** | **`valid? true`, `anomaly-types: nil`** | **`valid? true`, `anomaly-types: nil`** |
| Checker exit | 0 | 0 |
| Evidence | [`evidence/20261013T153158Z/`](evidence/20261013T153158Z/) | [`evidence/20261013T1537Z-nemesis/`](evidence/20261013T1537Z-nemesis/) |

A green verdict is only worth the workload behind it, so the histories are
audited, not just counted (script in "Audit" below):

* fault-free — 1,600 appends with **1,600 distinct values** (Elle can recover
  the per-key version order), 800 reads of which **797 returned a non-empty
  list** (max list length 201 of ~200 appends/key). All 8 keys exercised.
* fault-injected — 11,032 appends, **11,032 distinct values**; 5,515 reads,
  **5,512 non-empty** (max list length 1,381 of ~1,379 appends/key). All 8
  workers contributed 5,000 events each.

The 3 empty reads in each run are legitimate (`init`-ordered reads of a key
before any append landed), not a defect: the checker found no realtime edge
violation from them, which is why the verdict is `true` rather than a
`G-single-item-realtime` cycle.

### The fault run really was under fault

The generator's own normalized failure tally (raw, from
`RUNLOG.txt`) proves the faults bit live traffic rather than an idle cluster:

```
   1409  :fail BEGIN: No connections in the pool: The pool is broken; ... Connection refused (os err
    887  :fail BEGIN: No connections in the pool: The pool is broken; ... Connection broken, reason:
    706  :fail READ:  No connections in the pool: ... Connection refused (os erro
    445  :fail READ:  No connections in the pool: ... Connection broken, reason:
      3  :info COMMIT: Connection broken, ... Failed to read the frame header: early eof
      2  :fail UPDATE: Connection broken, ... early eof
      1  :fail READ:  Connection broken, ... early eof
```

Every COMMIT lost to a broken connection is recorded `:info` (indeterminate),
never `:fail` — a write that may have landed can never masquerade as a failed
op, which is what would fabricate an Elle anomaly. The script's own liveness
check reports `generator still running after the replica restart`, i.e. the
replica `SIGKILL` landed while the workload was live.

## Reproduce

```sh
# 1. Build a node image for THIS worktree (native arch, from this source).
#    CI's own node-image artifact is amd64-only and qemu-user segfaults it on
#    an Apple-silicon podman VM ("qemu: uncaught target signal 11"), so the
#    image is built here with the repo's own thin-runtime recipe.
deploy/local-elle/build-node-image.sh

# 2. Prove the checker in THIS environment is still red-capable before
#    trusting it (known-invalid -> valid? false / :G0-realtime; known-valid ->
#    valid? true).
ferrosa-jepsen/elle-checker/test/checker-control.sh

# 3. Fault-free certification. Brings the repo's RF=3 cluster up, runs the
#    CQL list-append generator, checks the emitted .edn with the checker
#    UNCHANGED, records the verdict, and tears the cluster down on every exit.
deploy/local-elle/certify-cql.sh

# 4. Fault-injected leg (replica + coordinator restart). Sized so the workload
#    outlasts the fault window — at 700 ops/worker it does NOT, and the faults
#    land on an idle cluster (see "Inconclusive attempts").
OUT_EDN="$PWD/deploy/local-elle/evidence/$(date -u +%Y%m%dT%H%M%SZ)-nemesis/elle-cql-nemesis.edn" \
  deploy/local-elle/certify-local.sh 2500 8
podman compose -f ferrosa-jepsen/tests/docker/elle-cluster-rf3.yml down -v   # certify-local.sh leaves it up
```

### Audit

```sh
grep -c ':type :ok'   <history>.edn      # committed, not vacuous
grep -c ':type :info' <history>.edn
grep -c ':type :fail' <history>.edn
grep -c ':type :ok.*:append' <history>.edn
```

## Build and environment provenance

* **Source**: `af9fecda29c24aa1d02eceeb004d3d615cfd6467`, branch
  `fix/writeskew-read-set-ordering` (PR #551), checked out into a dedicated
  worktree — clean, no local modifications.
* **Node image**: built from that worktree by `build-node-image.sh`
  (`cargo build --release -p ferrosa -p ferrosa-ctl` in `rust:1.94`, packaged
  with the repo's `tests/Dockerfile.nightly-bin`). `arm64/linux`, ELF
  BuildID `433c95b6…`.
* **Node memory**: each node reports `mem_limit_bytes=1073741824` (1 GiB) from
  the compose limit in `elle-cluster-rf3.yml` (see `evidence/*/cluster.log`).
  Well inside the 4 GB ceiling; nothing was raised to make a run pass.
* **Checker**: `ferrosa-jepsen/elle-checker` **unchanged** — the committed
  `ferrosa.elle-check` / `elle.list-append/check` under
  `:strict-serializable`, on `jepsen 0.3.6` + Clojure 1.12.0, JDK 17.0.20.
* **Cluster logs**: 0 `panic`/`ERROR` lines across the fault-free run.

## Repeatability

Three independent fault-free runs, all `valid? true` with the same op
breakdown (2,400 `:ok` / 0 `:info` / 0 `:fail`):

| run (UTC) | gate | verdict | evidence |
|---|---|---|---|
| 2026-10-13T15:26:14Z | `/readyz` only | `valid? true` | `evidence/20261013T152614Z/` |
| 2026-10-13T15:27:33Z | `/readyz` only | `valid? true` | `evidence/20261013T152733Z/` |
| 2026-10-13T15:31:58Z | full cluster gate | `valid? true` | `evidence/20261013T153158Z/` (canonical, full history) |

The first two ran under a weaker bring-up gate and are kept for their
`VERDICT.txt`/`checker.log` only; their histories are reproducible with the
same command and were not committed. The canonical run is the one to cite.

## Inconclusive attempts (kept, not hidden)

* `evidence/20261013T1530Z-nemesis/` — fault schedule at the Fly cert's
  `700 ops/worker`. The generator **finished before the first restart**, so
  both `SIGKILL`s landed on an idle cluster: the run's own log says
  `that fault hit an idle cluster` and `raise ops before trusting a green
  verdict`. Its `valid? true` is a fault-free verdict in fault-schedule
  clothing. **Do not cite it as a nemesis certification.** No history was
  committed for it: the fault window was never exercised.
* An earlier attempt of the same schedule aborted with **no history at all**
  — the generator started against a 2-node view and died with
  `Failed to perform initial cluster metadata fetch: … Connection refused
  (os error 61)`. Root cause was a rig defect, now fixed: `/readyz` goes green
  as soon as a Raft leader exists, while a driver connecting in that window
  still sees fewer than three nodes. Both drivers now wait for the CI job's
  own cluster-formation condition (per-node `mode=cluster` + `ddl_path=cluster`
  + a three-member `Normal` ring) on **every** node.

## Findings this certification surfaced

1. **`tests/Dockerfile.nightly-bin` cannot build.** Its `RUN` step invokes
   `file`, which its base image does not install
   (`/bin/sh: 1: file: not found` → exit 127). Fixed here by adding `file` to
   the existing `apt-get install` list — the only change to that file.
   (Reported, not silently absorbed: it blocks any run of the binary-fallback
   image path, including `.github/workflows/nightly-fuzz.yml`.)
2. **`certify-local.sh` leaves the cluster running.** It has no teardown path;
   the cluster was brought down by hand after each of its runs here. A
   certification rig that leaves a live RF=3 cluster behind is a leak on a
   shared machine. Not fixed in this change (its fault schedule is forge
   t_68cf9a98's rig).
3. **Fault-schedule sizing is load-dependent.** The Fly cert's `8 700 8 3`
   outlasted a ~155 s nemesis on Fly; the same workload finishes in ~30 s
   locally, so the faults land on an idle cluster. The local leg needs
   `2500 ops/worker`. The script warns correctly, but the default does not.

## Scope and honest limits

* **Single host, single DC, 3 nodes, one fault schedule** (replica restart,
  then coordinator restart). Not dual-DC (`certify-dc.sh` remains gated on
  cross-DC replication), not the iptables/`tc` partition+latency schedule, not
  crash-stop under a partition.
* **The coordinator restart's rejoin was not exercised under load.** The
  `SIGKILL` landed while the generator was live, but the generator finished
  before node1's restart completed (the run's own
  `generator finished before the coordinator restart` warning). Closing that
  needs a still larger workload, e.g.
  `OUT_EDN=… deploy/local-elle/certify-local.sh 8000 8`.
* **The Postgres wire path is NOT covered.** No PG claim is made or implied;
  PG ordered reads are subsumed by unified-transaction-manager Phase D MVCC
  (forge t_b5390b4a), and its certification is a different cycle.
* **This is a certification of the CQL/Accord list-append transaction path**,
  not a general statement about every CQL workload.
* The checker is reused **unchanged**. A red or `:unknown` verdict would have
  been recorded as such; `valid? true` here is what the checker returned, not
  a target that was aimed at.
