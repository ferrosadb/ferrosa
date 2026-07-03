---
crate: ferrosa-storage
doc: roadmap
last_updated: 2026-07-03
---

# ferrosa-storage — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the suite-level active-work
notes, in-code module markers (`self_heal`/`index` extension points), and the
dependency/usage review. The crate has **no in-source `TODO`/`FIXME`** markers —
open work lives in specs and the items below.

## Now (highest value)

- **Operator-facing durability guidance (FMEA ST-1).** The default `Periodic`
  sync strategy trades a bounded loss window for throughput. Make the per-table /
  per-deployment choice explicit (when to pick `Batch`/`Group`), and surface the
  active strategy + observed sync interval in metrics/health so the window is
  never assumed away.
- **Strict reader fan-in bound under full token overlap (FMEA ST-8).** Resident
  readers are pooled and digest walks stream, but the strict open-reader bound
  for fully-overlapping repair digests is still a separate acceptance gate rather
  than enforced in-engine. Land the gate so anti-entropy repair cannot blow the
  fmem cgroup.

## Next

- **Proactive disk-pressure flush throttle (FMEA ST-4).** Beyond the
  `local_disk_free_reserve_bytes` fail-closed admission gate, add an earlier
  signal that throttles/accelerates flush+upload as local NVMe usage climbs, so
  writes degrade gracefully instead of hitting the hard reserve.
- **Self-heal controller: drain + converge actions.** The control loop +
  quarantine action (the first vertical slice) are implemented; compaction-driven
  drain, anti-entropy converge, and divergence detection are marked as explicit
  follow-ups in `self_heal/mod.rs`. Each remediation must stay bounded before the
  controller runs without an operator.
- **End-to-end PITR hardening (FMEA ST-10).** Snapshot/restore + commit-log
  archiving exist; broaden restore validation coverage and the fork/branch
  orchestration consumed by `ferrosa-dbaas`.

## Later

- **TWCS / TTL-aware compaction levels.** STCS and UCS are implemented; a
  time-window strategy (or UCS with TTL-aware levels) for time-series tables.
- **Local NVMe bit-rot detection.** S3 objects are SHA-256-verified on read and
  flush self-readback exists, but there is no background scrub of local SSTables
  between flush and first read (FMEA ST-3 residual).
- **HVQ S3 spill-tier vector artifacts.** Page/range-readable `.qvec` resolver so
  vector artifacts need not fit on the compute node (per the storage topic spec's
  HVQ contract); current vector sidecars are whole-blob.
- **Grace-period GC + orphan sweep.** Confirm superseded-SSTable deletion grace
  and a periodic sweep of unreferenced S3 objects are bounded and observable.
- **Index reload read-cap pagination (t_1ec2e3fc).** `reload_indexes_from_system_schema`
  and `read_persisted_indexes` cap the `system_schema.indexes` scan at 10k rows
  and warn on truncation. The DROP TABLE cascade (t_ae06e925, landed) stops the
  table growing with orphans, but a legitimately huge index population still
  needs pagination instead of a cap.

## Recently landed

- **DROP INDEX live-state cleanup + index build path fixes (2026-07-03).**
  `StorageEngine::drop_index` now unwires declared indexes from the live
  `TableStore` and `IndexStateTracker` in Direct, pair, and Raft DDL paths.
  Keyed partition-index reads trust an empty consult only when the local storage
  tracker is authoritative and reports the index current with no pending SSTable
  backfill; otherwise they keep the bounded partition fallback. The local index
  backend also resolves engine table-dir SSTables under
  `sstables/<keyspace>.<table>` and writes generated sidecars beside those table
  SSTables.
- **DROP TABLE index-tombstone cascade (t_ae06e925, 2026-07-01).**
  `unregister_table` — the choke point for every DDL route (Direct, pair,
  cluster/Raft, and DROP KEYSPACE per-table) — tombstones the dropped table's
  `system_schema.indexes` registrations (`write_index_tombstones_for_table`)
  and sweeps its `IndexStateTracker` entries.
  `reload_indexes_from_system_schema` now returns
  `IndexReloadOutcome { restored, skipped }` and reports unresolvable rows via
  one summary warn + the `ferrosa_storage_index_reload_skipped_rows_total`
  counter (per-orphan detail demoted to debug). Pre-existing orphans are not
  GC'd automatically (a table can be mid-registration at boot); clean up
  manually with `DROP INDEX IF EXISTS`.

## Non-goals

- Query parsing, planning, protocol framing, or transport — those belong to the
  front-ends (`ferrosa-cql`, `ferrosa-postgres`, `ferrosa-sparql`,
  `ferrosa-graph`).
- Cluster routing/consensus — owned by `ferrosa-cluster`; this crate exposes the
  `DataStore` seam it routes through.
