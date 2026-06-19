# Gap Closure Sprint Plan

Addresses all deferred integration work discovered during the ferrosa-memory launch debugging session (April 2026). Every item below has code that exists but isn't wired into production, or is stubbed with a placeholder.

## Sprint 1: Handler Registration (1-2 days)

Low-risk, high-value. All handlers are already implemented — they just need `registry.register()` calls in `controller/cluster.rs`.

| # | Work Item | Files | Est |
|---|-----------|-------|-----|
| 1.1 | Register Batchlog handlers (BatchlogWrite/Delete/Replay) | controller/cluster.rs, coordinator/batch.rs | 1h |
| 1.2 | Register Truncate handler (TruncateForward/TruncateAck) | controller/cluster.rs | 1h |
| 1.3 | Register 11 Accord message handlers | controller/cluster.rs, accord/ | 4h |
| 1.4 | Bootstrap completion counting (replace `received = expected` stub) | controller/cluster.rs:433 | 2h |

**Verification:** `grep` for every `MsgType::` variant in `codec.rs` and confirm each has a `register()` call. Zero unregistered handlers.

## Sprint 2: Graph/SPARQL Cluster Read Routing (1 day)

8 locations call `storage.read()` directly instead of `write_path.pk_read()` or `write_path.range_read()`. In cluster mode, this returns stale/incomplete data.

| # | Work Item | Files | Est |
|---|-----------|-------|-----|
| 2.1 | Route graph adjacency reads through WritePath | ferrosa-graph/src/adjacency/reconcile.rs | 2h |
| 2.2 | Route graph executor reads through WritePath | ferrosa-graph/src/executor/{expand.rs, varpath.rs, leapfrog.rs} | 3h |
| 2.3 | Route SPARQL executor reads through WritePath | ferrosa-sparql/src/executor.rs | 2h |
| 2.4 | Add WritePath reference to graph/SPARQL engine constructors | ferrosa-graph/src/engine.rs, ferrosa-sparql/src/lib.rs | 1h |

**Verification:** `grep -rn 'storage\.read\|storage\.read_range' ferrosa-graph/ ferrosa-sparql/` returns zero hits outside tests.

## Sprint 3: Accord Integration (3-5 days)

Wire the existing Accord consensus protocol into the CQL → storage execution path for LWT and strict-serializable transactions.

| # | Work Item | Files | Est |
|---|-----------|-------|-----|
| 3.1 | Call `route_decision()` in CQL router for INSERT/UPDATE/DELETE/SELECT | ferrosa-cql/src/router.rs | 4h |
| 3.2 | Implement `WritePath::accord_write()` that submits through AccordCoordinator | ferrosa-cluster/src/write_path.rs | 4h |
| 3.3 | Wire CrossShardCoordinator into ClusterCoordinator | ferrosa-cluster/src/coordinator/mod.rs | 4h |
| 3.4 | Wire AccordStateMachine prune_applied() into maintenance loop | ferrosa-cluster/src/controller/cluster.rs | 1h |
| 3.5 | Add Accord handlers to controller (from Sprint 1.3, if not done) | controller/cluster.rs | 4h |
| 3.6 | Integration test: LWT INSERT IF NOT EXISTS through Accord path | ferrosa-cql tests or ferrosa-jepsen | 4h |
| 3.7 | Integration test: Concurrent LWT from multiple coordinators | ferrosa-jepsen | 4h |

**Verification:** `INSERT INTO t (id) VALUES (1) IF NOT EXISTS` goes through Accord consensus, returns `[applied]=true/false` correctly, and survives concurrent execution from different nodes.

## Sprint 4: Rebalance & Operational (3-5 days)

| # | Work Item | Files | Est |
|---|-----------|-------|-----|
| 4.1 | Implement rebalance data streaming (token range transfer) | ferrosa-cluster/src/rebalance.rs | 8h |
| 4.2 | Snapshot API endpoints (POST/GET/DELETE /api/snapshots) | ferrosa-ctl/src/commands.rs | 4h |
| 4.3 | Restore API endpoint (POST /api/restore) | ferrosa-ctl/src/commands.rs | 4h |
| 4.4 | Graph DISTINCT modifier (currently silently ignored) | ferrosa-graph/src/parser/parse_impl.rs, executor | 4h |
| 4.5 | Graph negative patterns (currently silently ignored) | ferrosa-graph/src/parser/parse_impl.rs, executor | 4h |

**Verification:** Add a node to a 3-node cluster → data rebalances within 60s. Remove a node → no data loss.

## Sprint 5: Tooling & Polish (1-2 days)

| # | Work Item | Files | Est |
|---|-----------|-------|-----|
| 5.1 | SSTable dump utility | ferrosa-sstable/src/bin/ferrosa-sstable-dump.rs | 4h |
| 5.2 | SSTable import utility | ferrosa-sstable/src/bin/ferrosa-sstable-import.rs | 4h |
| 5.3 | PITR mutation replay from downloaded segments | ferrosa-storage/src/engine.rs | 4h |

## Dependencies

```
Sprint 1 (handlers) ──→ Sprint 3 (Accord needs handlers)
Sprint 2 (reads)    ──→ independent
Sprint 3 (Accord)   ──→ Sprint 1
Sprint 4 (ops)      ──→ independent
Sprint 5 (tools)    ──→ independent
```

Sprints 1, 2, 4, 5 can run in parallel. Sprint 3 depends on Sprint 1 completing first.

## Risk Assessment

| Sprint | Risk | Mitigation |
|--------|------|------------|
| 1 | Low — mechanical wiring | Existing tests cover handler logic |
| 2 | Low — WritePath already works for CQL reads | Follow existing CQL router pattern |
| 3 | High — Accord consensus is complex | Extensive existing unit tests; add integration tests |
| 4 | Medium — rebalance is new code | Start with 2→3 node add, not shrink |
| 5 | Low — offline utilities | No production impact |
