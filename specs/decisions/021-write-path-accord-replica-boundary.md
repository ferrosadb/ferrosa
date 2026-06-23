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

Make `WritePath` the **single boundary** for Accord transaction execution and
replica resolution. Add an entry point (shape, not final name):

```text
WritePath::accord_commit(write_set, read_predicate, options) -> AccordResult
```

- `Cluster(coordinator)` — resolves `ring.replicas(token(key), rf)` per key
  (mapping `node_id` → `host_id`), builds the `ParticipantSet`, constructs and
  drives the `AccordCoordinatorDriver`, and returns the result.
- `Direct`/`Pair` — the standalone / two-node degenerate case (one shard = the
  local node(s)), preserving today's single-node behavior.

`ferrosa-cql` submits **intent only** — the write-set plus the IF-predicate — and
never touches `Ring`, `Token`, `replica_ids`, or the driver. `route_lwt_via_accord`
collapses to: build the write-set + predicate, call `write_path.accord_commit`,
map the result to a `RouteResult`.

This mirrors the codebase's established dependency-inversion idiom — `Arc<dyn
DataStore>`, `Arc<dyn StorageApplier>`, the new `AccordTransport`, and the planned
`TransactionCommitter` (Phase 6 / ADR pending) — so it is consistent, not novel.

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
