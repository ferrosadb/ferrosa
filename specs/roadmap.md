# Ferrosa Suite Roadmap

> Last updated: 2026-06-04
> Release context: Ferrosa Suite 0.13.0 hardening and documentation scrub

This checklist consolidates work raised by the Ferrosa and Ferrosa Memory doc
scrub. Items are ordered by value first, then by lower implementation complexity
where the value is comparable.

## P0 / Release Gates

- [ ] **Ferrosa: strict repair reader fan-in under full token overlap**
  - Value: prevents the remaining known OOM class during full-range digest/repair
    over tables whose SSTables all span the ring.
  - Acceptance: full-overlap repair digest over `N >> fanin` SSTables holds open
    readers within the configured fan-in cap or spills through a bounded external
    merge; data output remains byte-identical to the streaming golden path.
  - References: `ferrosa/specs/todo/p0-bounded-sstable-reader-checklist.md`,
    `ferrosa/specs/proposed/p0-bounded-sstable-reader-fmea.md`.

- [ ] **Ferrosa: land repair fuzz harness in CI**
  - Value: locks in byte/partition-bounded repair fetches, deterministic cursors,
    convergence behavior, and corruption handling.
  - Acceptance: `ferrosa-storage` and `ferrosa-cluster` repair fuzz tests run in
    the normal Rust test pipeline with committed proptest regressions.
  - References: `ferrosa/specs/proposed/repair-fuzz-harness-design.md`,
    `ferrosa-storage/src/test_support.rs`, `ferrosa-storage/tests/repair_fuzz.rs`.

- [ ] **Ferrosa: complete bloated-node fmem repair verification**
  - Value: proves the 0.13 OOM hardening against the workload that motivated it.
  - Acceptance: rebuilt 0.13 node image repairs the bloated fmem node with
    `OOMKilled=false`, stable restart count, converged counts, and sane viz/quorum
    checks.
  - References: `ferrosa/specs/todo/p0-bounded-sstable-reader-checklist.md`.

- [ ] **Ferrosa Memory: resolve entity-store sequential bulk ingest loss**
  - Value: protects memory correctness before larger automated ingestion and
    evaluation runs.
  - Acceptance: repeated large `ingest_entities` batches preserve all prior
    entities, update visibility, and ANN/hybrid retrieval coverage.
  - References: `ferrosa-memory/specs/todo/bug-entity-store-session-partitioning.md`.

## High Value / Low Complexity

- [ ] **Ferrosa: add operator metrics playbook for 0.13 OOM hardening**
  - Acceptance: docs name default thresholds and dashboard/alert guidance for
    resident-reader peaks, soft-cap breaches, `ferrosa_storage_compaction_running_max`,
    and `ferrosa_storage_compaction_pool_input_opens_total`.
  - References: `ferrosa/specs/storage.md`.

- [ ] **Ferrosa Memory: generate or verify MCP tool inventory from dispatch**
  - Acceptance: README/specs tool counts are produced by a script or checked by a
    test so the 61-tool surface does not drift again.
  - References: `ferrosa-memory/crates/ferrosa-memory-core/src/dispatch.rs`,
    `ferrosa-memory/README.md`, `ferrosa-memory/specs/README.md`.

- [ ] **Ferrosa Memory: make consolidation timeout-safe and observable**
  - Acceptance: long-running consolidation reports bounded progress, timeout
    reason, and partial result state instead of opaque 30s request failures.
  - References:
    `ferrosa-memory/specs/todo/bug-run-consolidation-timeout-under-prepare-failures.md`,
    `ferrosa-memory/specs/todo/feat-consolidation-cron-job.md`.

- [ ] **Ferrosa Memory: document runtime degradation signals**
  - Acceptance: specs include a short operator note covering repeated
    `ALLOW FILTERING`, replication heartbeat timeouts, flush-order warnings, and
    how to distinguish real OOM from degraded Ferrosa query/index behavior.
  - References: `ferrosa-memory/diagnostics/`, `ferrosa-memory/specs/status.md`.

## Medium Value / Next

- [ ] **Ferrosa: enforce CQL role boundaries for graph table isolation**
  - Acceptance: app roles cannot mutate graph-owned backing tables even if a
    caller regresses; public graph interfaces remain the only graph mutation path.
  - References: `ferrosa/specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md`,
    `ferrosa-memory/specs/decisions/adr-005-endpoint-only-ferrosa-client.md`.

- [ ] **Ferrosa Memory: close public typed-edge materialization blocker**
  - Acceptance: canonical public `TYPED_EDGE` MERGE materializes a row visible to
    `create_edge`, `explore_connections`, and the workbench without backing-table
    mutation.
  - References:
    `ferrosa-memory/specs/in-process/feat-endpoint-only-ferrosa-client.md`,
    `ferrosa/specs/archive/bug-public-cypher-typed-edge-merge-does-not-materialize.md`.

- [ ] **Ferrosa: fix vector binding in Scylla Rust driver path**
  - Acceptance: prepared inserts for `VECTOR<float, N>` bind successfully through
    the Ferrosa-supported Rust driver path.
  - References: `ferrosa/specs/todo/bug-scylla-driver-vector-serialization-binding.md`.

- [ ] **Ferrosa: fix `system_schema.views` driver shape**
  - Acceptance: Scylla/Cassandra driver schema agreement flows read
    `system_schema.views` without row-shape errors.
  - References: `ferrosa/specs/todo/bug-system-schema-views-column-shape-breaks-scylla-driver.md`.

- [ ] **Ferrosa Memory: improve operator note capture under hard limits**
  - Acceptance: overlarge operator memory notes spill into Ferrosa Memory or fail
    with explicit recovery instructions; no silent rejection.
  - References: `ferrosa-memory/specs/todo/bug-memory-tool-silent-rejection.md`.

## Larger Work

- [ ] **Ferrosa: self-healing controller over bounded primitives**
  - Acceptance: deterministic one-action-at-a-time controller detects, logs, and
    remediates corruption/divergence only after repair fan-in and fuzz gates are
    green.
  - References: `ferrosa/specs/proposed/self-healing-controller-design.md`,
    `ferrosa/specs/proposed/self-healing-controller-fmea.md`.

- [ ] **Ferrosa: cluster scaling hardening**
  - Acceptance: add-node bootstrap, 5+ node scaling, multi-DC node/DC metadata,
    and formation RF/CL behavior have reproducible tests and operator docs.
  - References: `ferrosa/specs/todo/todo-add-node-post-formation.md`,
    `ferrosa/specs/todo/todo-5plus-node-scaling.md`,
    `ferrosa/specs/todo/todo-multi-dc-node-dc-assignment.md`,
    `ferrosa/specs/todo/todo-formation-hardcoded-rf1-cl-one.md`.
