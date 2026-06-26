---
title: Postgres Front-End — Sprint F Implementation Follow-Ups
status: todo
executive_summary: >
  Known gaps and hardening items surfaced while implementing the auth + connection
  spine (codec, SCRAM, handshake, TCP listener, schema-backed verifier store).
  None block the spine; each should be closed before driver-compat/M1 sign-off.
---

# Sprint F Implementation Follow-Ups

Captured during the auth-spine implementation (commits `1b77761c`..`ef76352b`).

1. **Wire `PostgresServer` into `main.rs` (port 5432).** Spawn `server::serve` with a
   `SchemaVerifierStore` over the shared `Schema`, gated by config/env
   (`FERROSA_POSTGRES_BIND`), alongside the CQL/Bolt/SPARQL listeners. Seed the dev
   creds with a SCRAM verifier. (Needs live-infra integration test — H3.)

2. **BackendKeyData is `(0, 0)`.** Cancellation (`CancelRequest`) is not wired; the
   per-connection key is a placeholder. Generate unique keys and implement the cancel
   protocol path.

3. **User-enumeration oracle on `UnknownRole`.** The handshake returns `UnknownRole`
   before the SASL exchange, distinguishing unknown vs. wrong-password timing. Harden
   by running the exchange against a dummy/derived verifier so both paths look alike.
   (threat-model.)

4. **`HASHED PASSWORD` roles cannot SCRAM-login (D4 gap).** `create_role_hashed` /
   `alter_role_hashed` have no plaintext, so no SCRAM verifier is stored — those roles
   can authenticate over CQL but not Postgres until a plaintext password reset. Decide
   on a migration/backfill story (Q3).

5. **`system_auth.roles` column projection omits `scram`.** The verifier persists via
   the authoritative snapshot + Raft replication path (where roles are reconstructed),
   but the lossy, write-only `role_to_row` projection does not carry it (same as
   `member_of` today). Either confirm that projection is truly dead and remove it, or
   extend it to carry `scram` for defense in depth.

6. **SCRAM channel binding (`SCRAM-SHA-256-PLUS`).** Rejected fail-loud in v1; implement
   under TLS once the TLS path is wired (Q4).

7. **Derivation duplicated across crates.** `ferrosa_schema::auth::scram::derive` and
   `ferrosa_postgres::scram::ScramVerifier::from_password` implement the same standard
   algorithm. Both are RFC-checked independently; consider consolidating into one
   neutral location if a third consumer appears.

8. **`ferrosa-postgres` → `ferrosa-cql` coupling (regresses D10). — RESOLVED.** The
   storage-backed table loader (`storage_provider.rs`) had reused
   `ferrosa_cql::bridge::partition_to_rows_with_storage_mapping` (+ `decode_value`,
   `parse_cql_type_in_keyspace`) so its Partition→row ordering matched the canonical CQL
   SELECT path exactly — but that pulled the ~54k-LOC `ferrosa-cql` crate back in as a
   dependency of `ferrosa-postgres`, which D10 deliberately removed.

   **Resolution:** extracted the partition-decode / row-decomposition + value codec + CQL
   type-name parser into a new neutral crate **`ferrosa-row-bridge`** (depends only on
   `ferrosa-common`, `ferrosa-sstable`, `ferrosa-schema`, `num-bigint`, `uuid`, `tracing` —
   never `ferrosa-cql`). `ferrosa-cql` now **re-exports** those functions at their original
   public paths (`ferrosa_cql::bridge::partition_to_rows_with_storage_mapping` /
   `parse_cql_type_in_keyspace`, `ferrosa_cql::types::{encode_value, decode_value}`), mapping
   the new crate's `RowBridgeError` back to `CqlError::Invalid` so its hundreds of internal
   callers and its full test suite needed zero changes. `ferrosa-postgres` now depends on
   `ferrosa-row-bridge` and the `ferrosa-cql` dependency is removed from its `Cargo.toml`.
   The logic is shared (not duplicated), so there is no risk of silently-divergent row
   ordering (FMEA's top risk class).

   **Evidence:** `cargo tree -p ferrosa-postgres -e normal | grep -c ferrosa-cql` = 0; full
   `ferrosa-cql` suite green (935 lib tests + integration binaries, 0 failed); the live
   differential-vs-Postgres oracle reports 26/26 Match.

9. **`ferrosa_sql::Value` is lossy for non-scalar types.** `cql_to_value` maps Float/Double/
   Decimal/Varint/Timestamp/Date/Time/Duration/Uuid/Inet/Blob/List/Set/Map/Tuple/UDT/Vector
   to `Value::Null` (documented, never panics). Widen the engine `Value`/`ColumnType` to
   carry these before they reach the differential-vs-Postgres oracle (tracked on the board
   as `t_f16cbbff`).
