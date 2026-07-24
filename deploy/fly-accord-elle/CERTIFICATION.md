# Accord strict-serializability certification

## Current certification — `valid? true`, no anomalies

- **Verdict:** `valid? true`, `anomaly-types: nil` (Elle `list-append` under
  `:consistency-models [:strict-serializable]`; checker exits 0 only on a
  definitive valid result).
- **Build:** `e860ec88` (main, post-#290). Includes every Accord fix the
  certification depends on: FileSyncWriter-directory quorum fix `fb6a6ef9`,
  list-append execution-timestamp rebind `960bd2f6`/`9236d30b`, HLC witness
  ingestion `af1f5d4e`/`9491e87e`, PreAccept-past-conflict `f378aaf0`.
- **Date:** 2026-07-24.
- **Cluster:** 3-node RF=3 ferrosa on Fly.io (`performance` CPU, Tigris-S3-backed
  durability), formed fresh for the run and torn down after.
- **Workload:** `elle_list_append`, args `8 300 8 3` — 8 keys, 300 ops/worker,
  8 workers, RF 3. `append(k,v)` = `BEGIN; UPDATE la SET v = v + [v] WHERE k = ?;
  COMMIT` (an Accord transaction); `read` = `SELECT ...` at `SERIAL`.
- **History outcome:** **2,400 `:ok`, 0 `:info`, 0 `:fail`** — every submitted op
  committed; zero indeterminate. (Server-minted transaction ids drove the
  previously-observed ~24% `:info` to 0.) 4,800 ops total after Elle expands
  reads.

### Evidence artifacts (this directory)

- `elle-cert-e860ec88-20260724T181744Z.edn` — the full recorded history.
- `elle-cert-e860ec88-20260724T181744Z.edn.anomalies.edn` — the checker's anomaly
  dump: `nil` (no anomalies).

### Reproduce

```sh
# 1. Provision a fresh RF=3 cluster on the current build, run the generator,
#    retrieve the history, tear down (always):
OUT_EDN="$PWD/deploy/fly-accord-elle/elle-cert-<hash>-<stamp>.edn" \
  GEN_ARGS="8 300 8 3" ./deploy/fly-accord-elle/certify.sh

# 2. Check the history for strict-serializability violations:
cd ferrosa-jepsen/elle-checker
lein run "$OUT_EDN"           # prints valid? + anomaly-types; exit 0 iff valid? true
```

## Scope and honest limits

- This certifies the **single-DC, RF=3, Accord list-append** transaction path
  under a **fault-free** run. Fault-injected certification
  (`certify-nemesis.sh`: minority partition + 200 ms WAN) and **dual-DC**
  (`certify-dc.sh`, still gated on cross-DC replication) are separate and not
  covered by this artifact.
- The Elle checker is **not** wired into CI; CI checks the bank conservation
  invariant nightly. This certification is a **manual, reproducible** run.

## History

- `elle-fly-history-2026-07-20-PREFIX-valid-false.edn` — a superseded run from
  **before** the execution-timestamp fix. It was `valid? false`
  (`:strong-PL-1-cycle-exists`, `:G-nonadjacent-item-realtime`) because
  per-element list cells were ordered by coordinator-local `now_micros` instead
  of the Accord execution timestamp (root cause `t_68f226b5`, fixed in
  `960bd2f6`/`9236d30b`). Retained as the record of the bug this pipeline caught.
  Do not cite it as a passing result.
