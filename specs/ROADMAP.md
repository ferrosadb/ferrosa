# Ferrosa Roadmap

> Rebuilt: 2026-06-19
> Source: [`../specs-audit/SPEC-AUDIT.md`](../specs-audit/SPEC-AUDIT.md) — "Clean roadmap (grounded in the audit)"

This roadmap is grounded in the spec audit's classification of specs against the
actual code on `origin/main`. It supersedes the scattered `project-plan-*.md`
files (now in [`../specs-legacy/`](../specs-legacy/)).

## Now — high-impact, genuinely open, correctness / availability

- **`todo/todo-multi-dc-node-dc-assignment`** — all peers get the local DC, which
  breaks NetworkTopologyStrategy / `LOCAL_QUORUM`.
- **`todo/todo-s3-sync-manifest-metadata-placeholder`** — placeholder token/timestamp
  (MIN/MAX/0) breaks compaction range + timestamp filtering.
- **Re-status and close the security gaps the threat models claim done** — TLS
  T02/T08, internode mTLS, flamechart auth, formation admin-API auth. See
  [`SECURITY-STATUS.md`](SECURITY-STATUS.md) and [`security/`](security/).
- **`todo/bug-idle-cpu-spin-3cores`** — root-caused, no fix landed yet.
- **Verify `implemented/bug-recovery-oom-nonstreaming-snapshot`** —
  filed under `todo/` pending a live verification run of the fix.

## Next — finish PARTIAL work that is most of the way there

- Both SSTable-reader bound specs — Phase 7 live-verify + Phase 8 ship
  (`implemented/p0-bounded-sstable-reader-design`, `todo/p0-bounded-sstable-reader-checklist`,
  `todo/p0-unbounded-sstable-reader-memory-oom`).
- **`todo/todo-add-node-post-formation`** — wire Joining → stream.
- **`todo/todo-read-all-partitions-use-trie-index`**.
- **`todo/gap-cql-loaded-wasm-udf-e2e`** — rrd-udf end-to-end WASM test.
- **`todo/bug-jepsen-provision-t1-honors-firecracker-not-cluster-nodes`** — jepsen
  `provision_t1` cluster path.
- **`todo/cql-corpus-foundation-gaps`** — CQL corpus gaps (SAI / JSON / MV).
- **`todo/todo-hints-topology-change-wrong-node`**.
- **`todo/todo-batchlog-remote-delete-replay-duplication`**.

## Later — design-stage / scaling / nice-to-have

- **`todo/todo-pitr-branch-copy-cli-api`** — PITR branch/copy CLI + API; blocks
  DBaaS fork. Architecture spec active in `reference/pitr-branch-copy-architecture.md`.
- **`todo/todo-5plus-node-scaling`**.
- **`todo/todo-bootstrap-partition-sstables`**.
- **`todo/todo-sparql-optional-rdfstar-turtle`** — SPARQL OPTIONAL / RDF* / Turtle.
- **`todo/changelog-automation`**.
- **`todo/dogfood-system-schema`** — remaining system-schema tables.
- **`todo/geospatial-index`** — geospatial P2-c / P2-d.
