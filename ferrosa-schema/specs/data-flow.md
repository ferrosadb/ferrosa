---
crate: ferrosa-schema
doc: data-flow
last_updated: 2026-06-19
---

# ferrosa-schema — Data Flow

This documents the two flows that define the crate: an **auth-checked DDL apply**
(`Schema::create_table`) and the **permission check** it depends on. Both run
against the lock-free `ArcSwap<SchemaSnapshot>`.

## DDL apply + auth check (CREATE TABLE)

A front-end submits a parsed `TableMetadata` plus the caller's `AuthContext`.
The registry authorises, validates, swaps a new snapshot atomically, and audits.

```mermaid
sequenceDiagram
    autonumber
    participant FE as Front-end (CQL / Postgres / Flight)
    participant S as Schema (registry.rs)
    participant P as auth::check_permission
    participant Snap as ArcSwap&lt;SchemaSnapshot&gt;
    participant V as validation + graph rules
    participant A as AuditSink

    FE->>S: create_table(table, auth)
    S->>P: check_permission(auth, Create, Keyspace(ks))
    alt superuser
        P-->>S: Ok (bypass)
    else non-superuser
        P->>Snap: load() grants + roles
        Note over P: walk grants, resource hierarchy,<br/>member_of chain (cycle-safe via visited set)
        alt has matching grant
            P-->>S: Ok
        else no grant
            P-->>S: Err(PermissionDenied)
            S-->>FE: Err(PermissionDenied)
        end
    end

    S->>S: reject if is_system_keyspace(ks)
    S->>S: acquire write_lock (serialise writers)
    S->>Snap: load() then deep-clone snapshot
    S->>V: validate_table + validate_graph_extensions
    alt invalid
        V-->>S: Err(InvalidSchema)
        S-->>FE: Err(InvalidSchema)
    else valid
        S->>S: insert (ks,table); version = Uuid::new_v4()
        S->>Snap: store(Arc::new(new_snapshot))
        Note over Snap: atomic swap — concurrent readers see<br/>old or new snapshot, never a partial state
        S->>A: emit_audit_with_actor(TableCreated, auth)
        S-->>FE: Ok
    end
```

## How the replication path differs

The Raft / pair-mode path does **not** re-run auth or audit. It calls
`create_table_internal(table)` (or `apply_snapshot`), which clones, inserts
idempotently, and stores — but skips steps 2–6 above and does **not** bump
`version` (the Raft state machine calls `set_schema_version` separately so all
nodes converge). It also does **not** register the table with the
`StorageEngine`; the caller must do that (see FMEA SC-3).

## Notes on concurrency

- **Reads are lock-free.** `snapshot()` / `schema_ref()` load from `ArcSwap`
  without taking `write_lock`; hot-path callers (routers, observers) never block
  on a concurrent DDL.
- **Writes are serialised.** `write_lock: Mutex&lt;()&gt;` guarantees only one
  clone-mutate-swap runs at a time, so two concurrent `create_table` calls cannot
  lose each other's change.
- **Auth reads the same snapshot atom** the mutation will clone, so a permission
  decision is consistent with the schema state it is gating.
</content>
