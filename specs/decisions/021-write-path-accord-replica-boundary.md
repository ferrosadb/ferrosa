# ADR-021: `write_path` as the Accord transaction / replica-resolution boundary

> Date: 2026-06-22
> Status: Proposed
> Scope: `ferrosa-cluster::write_path::WritePath` (new Accord-commit entry),
> `ferrosa-cql::router::route_lwt_via_accord` (becomes a thin submit), and the
> multi-key `BEGIN/COMMIT` route (Phase 5). Touches replica resolution and
> `AccordCoordinatorDriver` construction.
> Non-goal: changing the per-shard quorum core (`ShardQuorum`, `ParticipantSet`,
> `AccordTransport` — already ring-free, see PR #182); the Accord wire protocol.

## Context

The per-shard quorum foundation (PR #182) is deliberately topology-free:
`ParticipantSet::build(keys, replicas_of)` takes a **closure**, and
`AccordTransport` abstracts the network. Neither references `Ring`, `Token`, or
the partitioner. Activating *multi-shard* execution needs one thing: each
write-set key resolved to its token-range replicas
(`ring.replicas(token(key), rf)`).

Today that resolution can't happen where the driver is built:

- `ferrosa-cql::router::route_lwt_via_accord` **constructs
  `AccordCoordinatorDriver` directly and hand-builds `replica_ids` from
  `peers.live_peer_ids()`** — every live peer, not the token's replicas. This is
  both a latent correctness bug ("uses all live peers") and a layering leak:
  replica placement is a *cluster* concern living in the *query* crate.
- `ferrosa-cql::SharedState` carries **no ring snapshot**. The naive fix —
  thread a concrete `Ring` into `ferrosa-cql` — would couple the query layer to
  cluster-topology internals (`Ring`, `NodeState`, `node_id`↔`host_id`, the
  Murmur3 partitioner), and duplicate knowledge that already lives one layer
  down.

The CQL layer **already** reaches the cluster through
`state.write_path` (`ferrosa_cluster::write_path::WritePath`), whose `Cluster`
variant is the coordinator and **already owns the ring** (it is what
`coordinate_read` / the FTS scatter-gather use). The boundary already exists; we
are currently reaching *around* it.

## Decision

Make `WritePath` the boundary for **replica resolution** (the topology concern),
while keeping `AccordCoordinatorDriver` construction in the CQL layer for the
single-key LWT path — because the LWT **condition gate** is irreducibly
CQL-specific: it decodes CQL schema (`decode_agreed_row_to_map`) and evaluates
the `IF` predicate (`eval_lwt_for_statement`) inside a closure the driver runs
during the read-vote phase. The cluster layer cannot build that closure, so
relocating the whole driver there would just push a CQL closure back across the
boundary — no real decoupling.

Two-part decision:

1. **Replica resolution moves behind `WritePath`** (the only topology leak today):
   ```text
   WritePath::replicas_for_key(table_id, key) -> Vec<Uuid>   // host ids
   ```
   - `Cluster(coordinator)` — `ring.replicas(token(key), rf)` mapped `node_id` →
     `host_id`, with `rf` from the keyspace replication.
   - `Direct`/`Pair` — the local node(s) (standalone / two-node degenerate case).

   `route_lwt_via_accord` sources `replica_ids` from this instead of
   `peers.live_peer_ids()`. CQL stops touching `Ring`/`Token`/the partitioner;
   the "all live peers" bug is fixed at the root. CQL still constructs the driver
   and injects the applier/reader/gate (its own + engine concerns).

2. **The gateless multi-key path (Phase 5 `BEGIN/COMMIT`) gets the fuller
   `WritePath::accord_commit(write_set, ...)`** — no condition gate there, so the
   cluster layer can own the whole drive + per-key `ParticipantSet` build. This
   is added with Phase 5, not now.

Both mirror the codebase's dependency-inversion idiom — `Arc<dyn DataStore>`,
`Arc<dyn StorageApplier>`, the new `AccordTransport`, the planned
`TransactionCommitter` (Phase 6) — so they are consistent, not novel.

## Relation to exposing Accord transactions over SQL (Postgres + CQL)

The goal these increments serve is **multi-key Accord transactions exposed over
SQL** — `BEGIN/COMMIT` on both the Postgres and CQL front-ends. That makes the
*front-end-facing* seam, not the cluster-internal one, the load-bearing
boundary, and it must be **front-end-agnostic** because `ferrosa-postgres` does
**not** (and should not) depend on `ferrosa-cluster`.

Layering:

```text
ferrosa-postgres ─┐
                  ├─→  Arc<dyn TransactionCommitter>      (trait in ferrosa-storage — the shared dep)
ferrosa-cql ──────┘              │   commit_write_set(writes, predicate) -> result
                                 ▼
        ferrosa-cluster: the Accord commit impl
          1. resolve replicas per key  ← THIS ADR (ring.replicas, write.rs:278)
          2. per-shard quorum          ← ShardQuorum / ParticipantSet (PR #182)
          3. drive AccordCoordinatorDriver
```

So:

- **`TransactionCommitter`** (defined in `ferrosa-storage`, the crate both
  front-ends already share — Phase 6) is the SQL-exposure boundary. Postgres and
  CQL `BEGIN/COMMIT` both buffer DML into a write-set and call it on COMMIT;
  ROLLBACK drops the buffer.
- This ADR's `WritePath` replica resolution + PR #182's per-shard quorum are the
  **implementation** behind that trait, in `ferrosa-cluster`, wired into the impl
  in the binary (mirroring how `Arc<dyn StorageApplier>` is injected).
- The single-key LWT path keeps its CQL-specific condition gate (above) and uses
  the same `WritePath` replica resolution; it does not go through
  `TransactionCommitter` (no buffered write-set).

Net: replica resolution moves behind `WritePath` now (this ADR, unblocks the LWT
bug fix); the `TransactionCommitter` front-end seam lands with the multi-key
`BEGIN/COMMIT` work (Phase 5 CQL + Phase 6 Postgres) and reuses both.

## Consequences

**Positive**

- Removes the CQL → cluster-topology coupling; the query crate stops knowing
  about rings, tokens, partitioners, or replica sets.
- **Fixes the "all live peers" bug at its root** — the cluster resolves the
  correct token replicas instead of every live peer.
- One place owns replica placement + per-shard participant construction (where
  the ring already lives), so the multi-key `BEGIN/COMMIT` route (Phase 5) and
  the single-key LWT route share it.
- The per-shard quorum core (PR #182) is unchanged — it was built on the right
  side of this line.

**Negative / cost**

- A new method on `WritePath` and its variants; the `Direct`/`Pair` arms must
  handle the local degenerate case explicitly (fail-loud, never silently
  single-node a multi-shard txn).
- Moves the `local_applier`/`local_reader`/`condition_gate` wiring that the CQL
  router does today into the cluster layer.

## Alternatives considered

1. **Thread a `Ring` into `ferrosa-cql::SharedState`.** Rejected: couples the
   query layer to cluster topology internals and duplicates placement logic that
   `WritePath` already encapsulates.
2. **A narrow `ReplicaResolver` trait injected into the CQL router.** Better than
   (1), but still leaves CQL constructing and driving the `AccordCoordinatorDriver`
   — the deeper smell. `WritePath` *is* already the seam; adding a second one is
   redundant.
3. **Status quo (`live_peer_ids()`).** Rejected: it is the active bug, and the
   coupling only grows once multi-key transactions need per-key replicas.
