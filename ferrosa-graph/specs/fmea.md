---
crate: ferrosa-graph
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-graph — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This crate is on the read+write critical path and exposes two
network listeners, so consistency and DoS modes dominate.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| G-1 | **Adjacency-index desync** — an edge write commits but its OUT/IN adjacency rows do not (observer mutation dropped under backpressure, or crash between edge + index apply) | Traversals silently miss neighbors: `MATCH (a)-[]->(b)` returns incomplete results — wrong answers, not an error | 9 | 4 | 7 | 252 | **Partially mitigated.** Observer runs `ObserverMode::Sync` so the index normally commits *with* the edge. The background `reconcile` loop is the fallback: it scans edge tables to repair missing entries and tombstones orphans, and is idempotent. **Gap:** reconciliation is only armed when `reconciliation_interval > 0`; with the default `GraphConfig` (`ZERO`, `enabled:false`) only the one-shot reconcile at keyspace registration runs — steady-state drift is undetected. There is no metric/alert on observed drift. |
| G-2 | **Missing graph extensions ⇒ silent no-op** — an edge table lacks `graph.source` / `graph.target` (or `graph.label`) | `derive_adjacency_mutations` returns `vec![]`: the edge persists but is never indexed, so it is invisible to every traversal, with no error | 8 | 3 | 8 | 192 | **Open gap.** The observer early-returns on missing extensions by design (Phase-1 tables), and the reconciler skips the same tables. No validation warns that an edge table is unindexable. Should fail loud (or warn) when a `graph.type=edge` table lacks source/target. |
| G-3 | **Reconciler writes at hardcoded RF=1** — `write_mutation` / `write_tombstone` use `ReplicationStrategy::Simple{replication_factor:1}` and `ConsistencyLevel::One` regardless of the keyspace's actual replication | On a multi-replica keyspace, reconciler-repaired adjacency rows land on one replica only; the index is repaired non-uniformly and may re-diverge per replica | 7 | 4 | 6 | 168 | **Open gap.** The expand executor derives the real strategy via `graph_replication_strategy(schema, ks)`, but the reconciler does not — it always uses RF=1. Wire the reconciler to the keyspace replication like the query path does. |
| G-4 | **Variable-length path cost** — `[*min..max]` BFS fans out per hop; cost is bounded only by `max_var_path_visited` (default 100k) + `query_timeout` (30s), with no cost-based planning or per-partition index | A dense graph or large `max` exhausts the vertex budget / times out; a single query can pin a worker for the full timeout | 6 | 5 | 4 | 120 | **Mitigated, coarse.** Visited-set cycle detection + vertex budget + `max_fan_out_per_hop` (10k) cap blow-up and DoS (threat T13). **Gap:** budget is global, not cardinality-aware; no planner estimate of path cost, no early `EXPLAIN`-time rejection of unbounded `[*]`. |
| G-5 | **Auth surface gaps** — authorization is per-statement in `validate` (Select/Modify), not per-label/edge-type; HTTP uses Basic auth, Bolt has an `auth_disabled` superuser mode | Coarser-than-Neo4j access control; a misconfigured `auth_disabled`/`auth-disabled` flag exposes the full graph; no property-level redaction | 8 | 2 | 5 | 80 | **Partially mitigated.** `check_permission` gates every statement against the `AuthContext` (T3); HTTP sanitizes internal errors (T8) and supports TLS (T11); body-size + panic-catch layers present. **Gap:** no per-relationship-type or property-level authz; `auth_disabled` must be operator-guarded. |
| G-6 | **Orphan tombstone vs. concurrent edge write** — reconciler Phase 2 tombstones an adjacency row whose edge it cannot find, racing a concurrent edge create | A just-written edge's adjacency row could be tombstoned if the reconciler reads between edge-row visibility and observer apply | 7 | 2 | 6 | 84 | **Mitigated by Sync observer** (edge + adjacency commit together, shrinking the window) and timestamp ordering, but the reconciler does not take a snapshot/lock; a narrow TOCTOU window remains. |
| G-7 | **Reconciler swallows read errors** — `range_read`/`read` failures are `continue`d or `unwrap_or_default()`ed | A transient storage error makes a pass silently under-repair (or skip orphan detection) while reporting success | 5 | 3 | 7 | 105 | **Open gap (fail-loud violation).** Errors are discarded without logging in several spots (`Err(_) => continue`, `unwrap_or_default()`). Should log + surface per-table failures and reflect them in `ReconcileMetrics`. |
| G-8 | **SUBSCRIBE resource growth** — each subscription spawns a polling task re-executing the query | Many subscriptions × expensive queries amplify load | 5 | 3 | 4 | 60 | **Mitigated.** `SubscriptionRegistry` enforces a per-connection cap (`FERROSA_GRAPH_MAX_SUBSCRIPTIONS`, default 8, FMEA F5); `cancel_all` on disconnect; tasks are cancellation-token driven. |

## Top risks to act on

1. **G-1 (RPN 252)** — adjacency-index desync. The synchronous observer is the
   real guarantee; the reconciler is the fallback but is **disarmed by default**
   (`reconciliation_interval = ZERO`) and emits no drift metric. Arm background
   reconciliation in production config and add an observable drift counter so the
   fallback is detectable, not silent.
2. **G-2 (RPN 192)** — an edge table missing `graph.source`/`graph.target`
   indexes nothing and is invisible to traversals with no error. Fail loud at
   schema-validation time when a `graph.type=edge` table is unindexable.
3. **G-3 (RPN 168)** — the reconciler repairs at hardcoded RF=1, diverging from
   the keyspace's real replication. Route reconciler writes through
   `graph_replication_strategy` like the query path.

## Detection assets

- `adjacency/reconcile.rs` tests: repair-missing, idempotency, orphan-removal,
  partial-direction repair, yield policy (`tests` module).
- `tests/adjacency_replication.rs` — adjacency DDL/replication integration.
- `tests/graph_http_integration.rs` — HTTP endpoint over a `ferrosa-net` harness.
- Observer wire-format pin tests in `adjacency/observer.rs`.
- `ReconcileMetrics { entries_checked, entries_repaired, orphans_removed }` logged
  by `spawn_reconciliation` when non-zero (the only current drift signal).
