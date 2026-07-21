# ferrosa-elle-check

Standalone [Elle](https://github.com/jepsen-io/elle) strict-serializability
checker for Ferrosa's Accord transactions (t_d7ffb5b7).

Isolated from the main `../jepsen` harness so it depends **only** on `jepsen`
(which bundles Elle) — avoiding that project's CQL-driver deps (`alia`/`hayt`).

## Pipeline

1. **Generate** a history against a running cluster (RF ≥ 3 so reads route
   through Accord) with the Rust generator:

   ```bash
   cargo run -p ferrosa-jepsen --example elle_list_append -- \
     <host:port> <out.edn> [keys] [ops-per-worker] [workers] [rf]
   ```

   Each op is `append(k,v)` = `BEGIN; UPDATE la SET v = v + [v] WHERE k=?; COMMIT`
   (unique `v`) or `read(k)` = `SELECT v ... WHERE k=?` at **SERIAL**. Outcome
   classification is sound: a failed `BEGIN`/`UPDATE` is `:fail` (definitely not
   applied); a failed/timed-out `COMMIT` is `:info` (indeterminate — never
   `:fail`, so a write that did land can't masquerade as a failed op).

2. **Check** the history for strict-serializability violations:

   ```bash
   lein run <history.edn> [out-dir]
   ```

   Prints `valid?` and `anomaly-types`. Exit `0` **only** on a definitive
   `:valid? true`; `false` (real violation) and `:unknown` (indeterminate, e.g.
   too many `:info` ops) both exit non-zero (fail-loud). Pass an `out-dir` to
   render anomaly graphs — this requires `graphviz` (`dot`) and `gnuplot`;
   without a dir the verdict is reported with no external-tool dependency.

## Validated

- Known-valid history → `valid? true`.
- Known-invalid history (append 1,2 then read `[2 1]`) → `valid? false`,
  `anomaly-types (:G0-realtime)`.

## Certification note

A meaningful verdict requires a cluster running a build that includes the Accord
fixes (read-visibility PR #281; FileSyncWriter-dir quorum fix `fb6a6ef9`). A
stale build surfaces `Accord quorum unavailable` COMMITs (indeterminate) and Elle
reports cycles from the degraded run — that is a broken *cluster*, not a checker
result.
