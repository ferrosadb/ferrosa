# Spec Audit — ferrosa/specs/ (2026-06-19)

Audit-only pass: every "live" spec (196 files: top-level + proposed/todo/in-process/implemented/verified-test-plan) classified against the **actual code on origin/main** by four read-only audit agents. `archive/` (11) and `decisions/` ADRs (23) were left untouched (keep as-is). This report drives the rebuild decision — no specs were modified.

## Executive summary

The directory's *folders no longer reflect reality*. Across 196 specs, **~30 are misfiled**: things in `implemented/` with no implementation, things in `todo/`/`proposed/` that already shipped, and `in-process/` work that's done. Separately, **the threat-model / FMEA docs are systematically optimistic** — multiple security items marked "APPROVED/mitigated" are actually deferred (Phase 2) and unconfirmed in code. A clean rebuild reflecting implemented reality is justified.

| Area | Files | Accurate | Misfiled / drifted | Headline |
|------|-------|----------|--------------------|----------|
| `implemented/` | 55 | 40 implemented & accurate | **5 aspirational/obsolete (no code)** + 2 duplicate pairs + 7 partial | 5 "done" specs have no impl |
| top-level `specs/` | 67 | 22 impl + 16 reference | **27 partial**, 1 aspirational | threat models/FMEAs over-optimistic |
| `todo/` | 32 | 11 still-open | **9–11 already DONE**, 7 partial | ~⅓ of the board is stale |
| `proposed/`+`in-process/`+`verified/` | ~24 | — | **9 proposals shipped, 7 in-process done** | capnp brief: only 1 of 4 boundaries landed |

## Cross-cutting risks (highest value)

1. **Optimistic security docs (8 files).** `threat-model.md` (T02/T08 TLS), `threat-model-cql-bc.md` (T5/T6/T10/T11), `threat-model-net-cluster.md` (mTLS), `threat-model-cluster-formation.md` (admin-API auth), `threat-model-graph.md` (T12–16), `threat-model-rrd-wasm-timeseries.md` (URL allowlist), `observability-threat-model.md` (flamechart auth), `threat-model-rrd…`. Items read as mitigated but are Phase-2/deferred. **Action: re-status all security docs against code before any are cited as evidence.** This is the single most important correction — it's a trust/correctness gap, not cosmetics.
2. **`implemented/` contains 5 specs with no implementation** — `bug-commitlog-segments-retained-for-cold-tables`, `bug-ghost-rows-all-null-columns` (aspirational), `bug-read-path-memory-growth-bloats-coordinator` (empty acceptance criteria), `bug-coordinator-block-on-panic` (obsolete — no `block_on` remains), `gap-S1-bootstrap-completion-counting` (wiring unclear). → archive/verify, don't claim done.
3. **High-impact work still genuinely open** (mis-buried in `todo/`): `todo-multi-dc-node-dc-assignment` (all peers get local DC → breaks NTS/LOCAL_QUORUM), `todo-s3-sync-manifest-metadata-placeholder` (MIN/MAX/0 placeholders → breaks compaction range/timestamp filtering), `todo-hints-topology-change-wrong-node`, `todo-pitr-branch-copy-cli-api` (blocks DBaaS fork).

## Reclassification actions

### → MOVE to `implemented/` (shipped but filed as open/proposed) — ~18
todo: `accord-lwt-real-data-path-plan`, `bug-sstable-writes-not-crash-atomic`, `bug-streaming-range-read-perf-50x-floor`, `bug-system-schema-views-column-shape…`, `feature-inline-language-to-wasm-udf`, `filtered-index-multi-column-predicate`, `todo-enable-cql-role-auth-for-graph-table-isolation`, `todo-pitr-mutation-replay`, `todo-remote-index-build-backend`.
proposed: `automatic-repair-scheduler-design`(+fmea), `p0-bounded-sstable-reader-design`(+fmea), `self-healing-controller-design`(+fmea), `repair-fuzz-harness-design`.
in-process: `compaction-validator`, both HVQ specs, `rrd-async-worker…`, `rrd-live-materialization-observability`, `rrd-ring-memory-bounds…`, `streaming-audit-bugfix-checklist`.

### → ARCHIVE (obsolete / superseded / aspirational-no-code) — ~7
`implemented/bug-coordinator-block-on-panic`, `implemented/bug-commitlog-segments-retained-for-cold-tables`, `implemented/bug-ghost-rows-all-null-columns`, `proposed/hierarchical-vector-quantization` (superseded by shipped blueprint), `todo/bug-range-stream-chunk-reorder-closes-route` (fixed), `todo/bug-smoke-18765-tls-cert-path-mismatch` (fixed).

### → DELETE (true duplicates) — 2
`implemented/todo-rebalance-data-streaming` (= `gap-S4-rebalance-data-streaming`), `implemented/todo-sstable-tools-unimplemented` (= `gap-S5-sstable-tools`). Also consolidate the `p0-bounded-sstable-reader-checklist` / `p0-unbounded-sstable-reader-memory-oom` pair (plan vs root-cause).

### → UPDATE in place (drifted status, code moved on) — ~30
All 8 security docs (above); `components.md` (12→~24 crates); the `capnp-serialization` brief (scope = 1 of 4 boundaries, claimed worktree gone); the Draft-but-shipped plans (`remote-index-build-backend`, `secondary-index-pipeline`, `runtime-isolation`, `rrd-wasm-*`, `cluster-formation-*`, `dsm-controller-refactor`); the 7 partial todo items.

### → VERIFY-RUN (code present, needs a live run to confirm) — ~22
14 in `implemented/` (cluster/streaming bugs needing live-infra), `verified-test-plan/` (all — cqlsh-node3 handshake + entity-update-visibility have **no locatable fix**, repro first), `secondary-index-pipeline`, `sparql-endpoint-architecture`, `bug-recovery-oom-nonstreaming-snapshot`.

## Clean roadmap (grounded in the audit)

**Now** — high-impact, genuinely open, correctness/availability:
- `todo-multi-dc-node-dc-assignment` — peers get local DC; breaks NTS / LOCAL_QUORUM.
- `todo-s3-sync-manifest-metadata-placeholder` — placeholder token/timestamp breaks compaction range+timestamp filtering.
- Re-status + close the security gaps the threat models claim done (TLS T02/T08, internode mTLS, flamechart/admin-API auth).
- `bug-idle-cpu-spin-3cores` (root-caused, no fix landed) + verify `bug-recovery-oom` fix.

**Next** — finish PARTIAL work that's most of the way there:
- Both SSTable-reader bound specs (Phase 7 live-verify + Phase 8 ship), `todo-add-node-post-formation` (wire Joining→stream), `todo-read-all-partitions-use-trie-index`, rrd-udf E2E WASM test, jepsen `provision_t1` cluster path, cql-corpus gaps (SAI/JSON/MV).
- `todo-hints-topology-change-wrong-node`, `todo-batchlog-remote-delete-replay-duplication`.

**Later** — design-stage / scaling / nice-to-have:
- `pitr-branch-copy` (CLI/API — blocks DBaaS fork; arch spec active), `todo-5plus-node-scaling`, `todo-bootstrap-partition-sstables`, SPARQL OPTIONAL/RDF*/Turtle, `changelog-automation`, `dogfood-system-schema` remaining tables, geospatial P2-c/d.

## Recommendation on the rebuild

The drift is large enough (≈30 misfiled + 8 optimistic security docs) that the **legacy-move + clean rebuild** you were tempted toward is warranted — but do it mechanically from this audit, not by hand:
1. `git mv specs specs-legacy` (preserve history).
2. Generate a fresh `specs/` with: `implemented/` = only the 40+18 verified-implemented; `reference/` = the 16 architecture/DSM/FMEA docs kept current; `todo/` = the 11 still-open + 7 partial; `decisions/` = ADRs carried over; a single `ROADMAP.md` (the above); and an honest `SECURITY-STATUS.md` replacing the optimistic threat-model claims.
3. Drop the obsolete/duplicate/aspirational (archive in `specs-legacy/`, not in fresh `specs/`).

Per the chosen "audit-only first" gate: this report is the input — confirm the rebuild and I'll execute the move + regeneration (a mechanical pass over the lists above).

---

*Full per-file classification tables (all 196 rows) from the four audit agents are preserved in the audit run; the reclassification lists above are the actionable distillation.*
