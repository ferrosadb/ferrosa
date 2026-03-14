# Schema Replication for Pair Mode

> Last updated: 2026-03-14
> Status: Draft

## Goal

Replicate DDL operations (CREATE/DROP/ALTER KEYSPACE/TABLE) between pair mode
nodes so that:

1. Schema catch-up works on rejoin (the failing smoke test Phase 4)
2. DDL executed on one node automatically appears on the other
3. The pattern maps cleanly to Raft-based cluster mode later

## Problem

Schema is entirely in-memory (`ArcSwap<SchemaSnapshot>`) and not persisted or
replicated. When a node restarts or joins pair mode via catch-up, it has no
keyspaces or tables. Mutations replayed during catch-up are silently dropped
because `StorageEngine` doesn't have the tables registered.

Currently the smoke test works around this by running CREATE KEYSPACE/TABLE on
both nodes independently. This is fragile and doesn't scale.

## Design

Two complementary mechanisms:

### 1. Snapshot-based Schema Sync (Catch-up)

On pair formation after a force-promote rejoin, the primary sends its full
`SchemaSnapshot` to the secondary before replaying mutations:

```text
Primary (force-promoted)              Secondary (rejoining)
────────────────────────              ─────────────────────
1. RoleSwap → peer becomes secondary
2. Send PairSchemaSync(snapshot)  ──→  3. Deserialize SchemaSnapshot
                                       4. For each keyspace: create_keyspace_internal()
                                       5. For each table: create_table_internal()
                                          + engine.register_table()
6. Replay mutations via              ← (ready to receive writes)
   PairWriteForward
```

- Schema sync happens **before** mutation replay
- Uses internal methods that bypass auth checks (replication is trusted)
- Filters system keyspaces (`system`, `system_schema`, etc.)
- `SchemaSnapshot` is already `Serialize`/`Deserialize` via serde
- **Both** schema registry and storage engine must be updated: the handler
  calls `schema.create_table_internal()` then
  `engine.register_table(table_meta.to_storage_schema()?)` for each table
- If the secondary has stale local schema from a previous session, log a
  warning but let the primary's snapshot take precedence (primary is
  authoritative)

### 2. Live DDL Forwarding (Normal Operation)

During normal pair operation, DDL operations are coordinated through the
primary, like writes:

```text
Client sends CREATE TABLE to Secondary
    ↓
CQL Router detects DDL
    ↓
DdlCoordinator::coordinate_ddl()
    ├─ Secondary: serialize DdlOperation, send PairDdlForward to primary
    │   Primary: applies schema + registers storage + responds with PairDdlAck
    │   Primary: sends PairDdlForward to secondary (replication)
    │   Secondary: applies schema + registers storage + responds with PairDdlAck
    │   Returns success to client
    │
    └─ Primary: applies schema + registers storage locally
        Sends PairDdlForward to secondary (replication)
        Secondary: applies + registers + responds with PairDdlAck
        Returns success to client
```

Message flow clarification:

- **Secondary → Primary**: sends `PairDdlForward`, receives `PairDdlAck`
- **Primary → Secondary** (replication): sends `PairDdlForward`, receives
  `PairDdlAck`
- The handler uses **role-based dispatch** (same pattern as
  `PairWriteForwardHandler`): primary receiving a forward applies + replicates;
  secondary receiving a forward applies locally

The primary is the single DDL authority. This maps to Raft leader in cluster
mode — the abstraction carries over.

### DDL in Degraded Mode

When the peer is lost (writes unavailable), DDL follows the same behavior as
writes: `DdlPath::Unavailable` rejects DDL operations. After `force_promote`,
DDL works locally via `DdlPath::Direct`. This prevents schema divergence
between a degraded secondary and a still-running primary.

### Idempotency

All `*_internal` methods must be idempotent:

- `create_keyspace_internal`: succeed silently if keyspace already exists
- `create_table_internal`: succeed silently if table already exists
- `drop_keyspace_internal`: succeed silently if keyspace doesn't exist
- `drop_table_internal`: succeed silently if table doesn't exist

This handles retries, partial failures, and the catch-up scenario where the
secondary might have stale state from a previous session.

### Scope: Roles and Grants

Role/auth DDL (`CREATE ROLE`, `GRANT`, `REVOKE`) is **included** in the
`SchemaSnapshot` (roles and grants are already fields) and will be replicated
via snapshot catch-up. Live forwarding of role/auth DDL is deferred — it can
be added as additional `DdlOperation` variants later. For now, role changes
require execution on both nodes or a rejoin to pick them up via snapshot.

## Wire Protocol

Three new message types in `ferrosa-net`:

| Message | MsgType | Lane | Description |
|---------|---------|------|-------------|
| `PairSchemaSync(Bytes)` | `0x45` | Bulk | JSON-serialized `SchemaSnapshot` |
| `PairDdlForward(Bytes)` | `0x46` | Data | JSON-serialized `DdlOperation` |
| `PairDdlAck(Bytes)` | `0x47` | Data | Success/error response |

JSON for now. Zero-copy serialization (Cap'n Proto / FlatBuffers / rkyv) is a
future optimization for the entire internode protocol. The 256 MiB
`max_frame_body_size` is more than sufficient for schema snapshots.

Update proptest message decode fuzzing range from `0x01..=0x44` to
`0x01..=0x47` to include the new types.

## DdlOperation Enum

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlOperation {
    CreateKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(TableMetadata),
    DropTable { keyspace: String, table: String },
    AlterKeyspace { name: String, updates: KeyspaceUpdates },
    AlterTable { keyspace: String, table: String, updates: TableUpdates },
}
```

Carries structured metadata, not raw CQL strings. Avoids re-parsing and is what
`Schema::create_table()` already consumes.

**Prerequisite:** `KeyspaceUpdates` and `TableUpdates` must have `Serialize`
and `Deserialize` derives added (currently only `Debug, Clone`).

## Code Changes

### ferrosa-net

- Add `PairSchemaSync`, `PairDdlForward`, `PairDdlAck` to `Message` enum
- Add `MsgType` constants `0x45`, `0x46`, `0x47` to codec
- Encode/decode implementations for each
- Update proptest range to `0x01..=0x47`

### ferrosa-schema

- Add `Serialize, Deserialize` derives to `KeyspaceUpdates` and `TableUpdates`
- `Schema::apply_snapshot(snapshot: SchemaSnapshot, engine: &StorageEngine)` —
  bulk-load keyspaces and tables from a snapshot, bypassing auth and audit.
  Calls `engine.register_table()` for each table. Used during catch-up.
- `Schema::create_keyspace_internal(ks: KeyspaceMetadata)` — create without
  auth checks, idempotent, for replication
- `Schema::create_table_internal(table: TableMetadata)` — create without auth
  checks, idempotent, for replication
- Similar `drop_*_internal` and `alter_*_internal` methods, all idempotent
- `DropTable` internal must also handle storage engine cleanup (remove the
  table's memtable and SSTable references). Add `StorageEngine::unregister_table()`
  if it doesn't exist.

### ferrosa-cluster

- New `pair/ddl.rs` — `DdlOperation` enum, `DdlCoordinator` struct
- `DdlCoordinator::coordinate_ddl(op: DdlOperation)` — routes DDL through
  primary like `PairCoordinator::coordinate_write()` routes mutations
- `PairDdlForwardHandler` — RPC handler for DDL messages. Uses role-based
  dispatch: primary applies + replicates to secondary; secondary applies
  locally. Both call `schema.*_internal()` + `engine.register_table()`.
- `PairSchemaSyncHandler` — RPC handler for schema snapshot (calls
  `schema.apply_snapshot()` on the secondary during catch-up)
- Update `controller.rs` catch-up task: send `PairSchemaSync` before mutation
  replay
- Add `ddl_coordinator` to `ModeController` pair context

### ferrosa-cql

- DDL routes in `router.rs` (`route_create_keyspace`, `route_create_table`,
  `route_drop_keyspace`, `route_drop_table`, etc.) call through
  `DdlCoordinator` instead of schema/storage directly
- Add `ddl_path` to `SharedState` (parallel to `write_path`)
- `DdlPath` enum: `Direct` (standalone), `Pair(DdlCoordinator)`, `Unavailable`

### docker-compose.yml / tests

- No changes to docker-compose
- Smoke test Phase 1: create schema on node1 only, verify it appears on node2
  (remove dual-create on lines 99-102 of `tests/docker-smoke.sh`)
- Smoke test Phase 4: catch-up should fully pass with schema sync (remove
  manual re-create workaround on lines 203-205)

## Testing

### Unit tests

- `DdlOperation` serde roundtrip (all variants)
- `Schema::apply_snapshot()` creates keyspaces + tables + registers storage
- `DdlCoordinator` routes DDL through primary
- Idempotency: `create_table_internal` twice doesn't error

### Integration tests

- DDL replication: create on primary, verify on secondary
- DDL forwarding: create on secondary, verify on primary
- Schema catch-up: primary has tables, secondary gets them via snapshot on
  rejoin

### Docker smoke test

- Phase 1: create schema on one node only, verify on both
- Phase 4: failover data replicates via catch-up (schema sync + mutation
  replay)

### CI

- Add Docker smoke test to nightly workflow (main branch only, not PRs)
- Runs after unit tests pass
- ~3 min total (Docker build + 5-phase test)

## Error Handling

- DDL forward timeout (10s Data lane): originating node returns error to
  client. Schema may be applied on primary but not secondary. Next catch-up
  or DDL retry will reconcile.
- Schema snapshot too large: extremely unlikely (256 MiB limit), but if hit,
  fail the catch-up and log an error. Operator must reduce schema size or
  increase `max_frame_body_size`.
- Double-failure scenario: both nodes go down, operator restarts one and
  promotes it. If the other node had uncommitted schema changes, those changes
  are lost. This is inherent to two-node architecture (no quorum). The spec
  acknowledges this as a known limitation.

## Future: Raft Migration Path

In pair mode, primary is the DDL authority. In cluster mode, Raft leader takes
that role:

- **Pair:** secondary → forward to primary → primary applies + replicates
- **Cluster:** any node → propose to Raft leader → leader commits via Raft log

The `DdlPath` enum gets a `Cluster` variant. The `DdlOperation` serialization
format is reused in Raft log entries. `Schema::apply_snapshot()` is reused for
Raft snapshots.
