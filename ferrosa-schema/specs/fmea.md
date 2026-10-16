---
crate: ferrosa-schema
doc: fmea
last_updated: 2026-10-15
---

# ferrosa-schema — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This crate is the auth + DDL authority, so security
failure modes dominate the high-RPN band. Rows reflect the **actual** code, not
aspirations.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| SC-Tcd650102 | `system_schema.tables` had no `flags` column, and `system_schema.columns` reported `position = 0` for regular columns. | The python driver derives compact storage from `flags`; with none it treated every table as compact static and dropped the clustering key (all 90 clustered tables of ferrosa-memory's keyspace parsed with `clustering_key == []`), so driver-side routing, paging and model mapping saw the wrong primary key. | 7 | 9 | 7 | 441 → 7 | **Fixed:** `TableRow.flags` (`compound` always, `counter` for counter tables, stored `dense`/`super`) is served as `set<text>`; `query_columns` reports `-1` for regular/static. Tests: `query_tables_reports_cassandra_flags`, `query_columns_reports_minus_one_for_non_key_positions`, `system_schema_tables_reports_compound_flags` (ferrosa-cql). |
| SC-1 | Well-known default credentials shipped | Development bootstrap seeds `ferrosa_admin`/`ferrosa_user` with passwords equal to their names. An operator who never rotates them leaves a trivially-guessable admin. | 9 | 5 | 4 | 180 | Partial. The historical implicit `cassandra/cassandra` superuser is removed: `cassandra` is created only when a secrets provider supplies an explicit `superuser_password`. Production seed roles receive random passwords, and password rotations are durable across restart. Development seed credentials remain intentionally well-known and must not be exposed to untrusted networks. |
| SC-2 | (resolved) Production TLS checks were stubs | `validate_production_requirements` never pushed a TLS violation, so a `FERROSA_MODE=production` node with TLS off passed the gate. | 8 | 1 | 2 | 16 | **Resolved (t_d5d122ba).** `ProductionCheckConfig.listeners` carries every client listener's posture (`ListenerTls`); an enabled listener that does not require TLS is `ListenerTlsNotRequired` (names the listener and key), an enabled listener with no TLS implementation is `ListenerWithoutTlsSupport` (names the disable key; no shipped listener uses it since Arrow Flight gained TLS in t_58db6320), and `internode_require_tls = false` is `InternodeTlsNotConfigured`. All block startup. The `ListenerTlsNotRequired` refusal names `[section] require_tls`, `[section] tls_cert` and `[section] tls_key`. Mutual TLS is not required yet (t_b6c820f4). |
| SC-3 | `apply_snapshot` / `create_table_internal` skip StorageEngine registration | These mutators update the in-memory snapshot but the doc-comment notes the caller MUST separately call `engine.register_table()`. A caller that forgets leaves a table visible in schema but unservable by storage — silent divergence. | 7 | 3 | 6 | 126 | Documented in the method doc-comment; not enforced in code. Relies on caller discipline (cluster catch-up path). `apply_snapshot` is also insert-only: an existing table keeps its definition, so pair catch-up could not deliver a missed ALTER (ferrosa-cluster CL-33). Catch-up now replaces differing tables with `replace_table_internal` and updates the engine itself. |
| SC-4 | `HASHED PASSWORD` roles cannot use Postgres SCRAM | `create_role_hashed` / `alter_role` with `HASHED PASSWORD` store the hash verbatim and set `scram = None` (a bcrypt/argon2 hash can't derive a SCRAM verifier). Such roles silently fail Postgres SCRAM login until a plaintext reset. | 5 | 4 | 5 | 100 | Documented D4 gap in `registry.rs` / `scram.rs`. `alter_role` clears the stale verifier so it fails closed; CQL login still works. |
| SC-5 | Concurrent role-grant cycle race | `grant_role_internal` checks `would_create_cycle` against current state only. Two independently-committed grants (`GRANT a TO b`, `GRANT b TO a`) could in principle form a cycle. | 6 | 2 | 5 | 60 | Mitigated: Raft applies entries serially, so the second grant sees the first edge already present and is rejected. `check_permission` is additionally cycle-safe via a `visited` set, so even a leaked cycle cannot hang traversal. |
| SC-6 | Full-snapshot clone on every mutation | Each DDL/grant clones the entire `SchemaSnapshot` (all keyspaces, tables, roles, grants, indexes, types, functions, aggregates) under `write_lock`. On a very large schema this makes every write O(schema size). | 4 | 4 | 3 | 48 | Accepted trade-off for lock-free reads via `ArcSwap`. Reads (hot path) are unaffected. Revisit if schema cardinality grows large. |
| SC-7 | Auto-rehash on login swallows failures silently | In `authenticate`, the hash-upgrade block uses `if let Ok(new_hash) = ...` and on error simply leaves the old hash; there is no log line when the rehash fails. | 3 | 3 | 6 | 54 | Login still succeeds with the old hash (fail-safe), but a persistently-failing rehash is invisible. Add a `tracing::warn!` on the error arm. |
| SC-8 | Identifier validation is ASCII-only and length-capped at 48 | `validate_table`/`validate_keyspace` reject anything but `[A-Za-z0-9_]{1,48}`. Quoted/Unicode identifiers that Cassandra permits are rejected. | 3 | 3 | 2 | 18 | Intentional conservative subset; low risk, surfaces a clear `InvalidSchema` error (fail-loud). |

## Top risks to act on

1. **SC-1 (RPN 180)** — development seed credentials. The implicit Cassandra
   compatibility account is gone and production generates random seed passwords,
   but development `ferrosa_admin` / `ferrosa_user` credentials remain
   well-known. Development mode must stay off untrusted networks.

## Detection assets

- `auth/permission.rs` tests — superuser bypass, direct/keyspace/role-hierarchy
  grants, cycle-safety, deny-by-default.
- `startup.rs` tests — production violations for default password, weak policy,
  S3 HTTP, env secrets, every client listener without required TLS, an enabled
  listener with no TLS support, and internode TLS not required (SC-2).
- `auth/password.rs` + `scram.rs` tests — hash round-trip, rehash detection,
  hash-format validation, SCRAM derivation.
- 354 in-crate tests + 19 integration tests
  (`tests/{auth_integration,integration,property_tests}.rs`).
</content>

## T-022 type-name registry

| ID | Failure mode | Effect | Detection | Mitigation |
|----|--------------|--------|-----------|------------|
| SCH-T022-01 | `cql_to_marshal_type` / `aggregate_tables::cql_type_to_string` keep private name tables | Drift from the CQL parser tables (FM-20) | `type_names_consumers_agree` (aggregate_tables), `no_wildcard_default_for_new_variant` (convert) | Both delegate to `ferrosa_common::cql_type::names` (see COM-T022-01) |

## T-150 jsonb type threading

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|---|---|---|---|---|
| SCH-T150-01 | New CqlValue variant unhandled in initcond rendering | Compile error only | 2 | 1 | 1 | 2 | Named arm; the exhaustive match forced it. |

## T-154a jsonb placement rules

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|---|---|---|---|---|
| SCH-T154a-01 | jsonb (or a type holding jsonb) accepted in a partition or clustering key on CREATE TABLE | Key bytes with no stable jsonb order (D3, FM-77) | 9 | 3 | 2 | 54 | `jsonb_rules::check_table` on `create_table`, `create_table_internal`, and the propose-side `check_create_table_jsonb`; recursive through frozen collections, tuples and UDTs. `tests/jsonb_key_rules.rs`. |
| SCH-T154a-02 | ALTER TABLE ADD slips in set/map-key/vector of jsonb or a key-kind jsonb column | Same, after the table exists (FM-79) | 8 | 3 | 2 | 48 | `check_added_columns` in `alter_table`, `alter_table_internal`, `check_alter_table_jsonb`. A refused ALTER changes nothing. |
| SCH-T154a-03 | A replicated or reloaded schema (`apply_snapshot`, Raft apply, disk load) carries a jsonb key | A bad schema loaded silently, or its bad table skipped | 9 | 2 | 2 | 36 | `apply_snapshot` checks the whole snapshot before applying any of it and returns the typed error; the Raft apply already surfaces `create_table_internal` errors loudly. |
| SCH-T154a-04 | A column type string the checker cannot read is treated as clean | The rule silently not applied | 8 | 2 | 2 | 32 | An unparseable type is `SchemaError::InvalidSchema`, never skipped. |
| SCH-T154a-05 | ALTER TYPE ADD FIELD jsonb on a UDT a key already uses | jsonb enters a key through the UDT (D21) | 8 | 2 | 3 | 48 | `alter_type_add_field` re-checks every key column against the changed type map and refuses; a jsonb-nesting field type is refused too. |

## T-300 jsonb DDL capability gate — REMOVED (D24, D32, D15a, FM-23)

The interim jsonb DDL capability gate (T-300, D24, D32, D15a) was REMOVED from
the source tree, not disabled. jsonb is a first-class data type: it ships
standalone, paired and clustered with no flag, env var, config key or operator
action, and it cannot be turned off again. The engine already replicates graph
materialization over Raft, so a column type is not special; a gate that can
REFUSE a data type is itself the defect. The gate, its verdict type, its entry
and apply-time checks, its per-refusal counter and the jsonb-table scan it used
are gone. Every failure mode it carried is closed; the rows below are re-marked
REMOVED with the residual risk stated as closed. The only surviving jsonb
constraint is the placement rule (T-154a, D3, D21) above.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|---|---|---|---|---|
| SCH-T300-01 | A new deployment mode or a new gate verdict silently permits jsonb DDL | jsonb replicated "without the capability ledger" | — | — | — | — | **REMOVED (T-300/D24/D32).** The gate and its verdict type were deleted, so no mode/verdict match remains to decide anything and no default can silently permit or refuse jsonb. Residual closed: a column type is no longer gated on deployment shape. |
| SCH-T300-07 | A stale published member view permits jsonb DDL | jsonb replicated to a member that cannot understand it | — | — | — | — | **REMOVED (t_d853fa2a).** The member-view read that fed the gate was deleted with it. Residual closed: jsonb is first-class, so a member on an older build is no longer a reason to refuse, and graph materialization already replicates over Raft. (The general peer-capability freshness mechanism survives for other capability bits; it no longer serves jsonb.) |
| SCH-T300-02 | jsonb DDL reaches a non-standalone node by a path that skips the entry check | A node without the ledger holds jsonb | — | — | — | — | **REMOVED.** The entry checks and every apply-time gate call were deleted; each create/alter/type path and `apply_snapshot` now carry only the placement rule. Residual closed: jsonb needs no ledger to replicate. |
| SCH-T300-03 | An unparseable column type is treated as clean | The rule silently not applied | 8 | 2 | 2 | 32 | **Folded into the placement rule (see SCH-T154a-04).** The placement reader returns `SchemaError::InvalidSchema` for an unreadable type string; never skipped. No gate is involved. |
| SCH-T300-04 | A UDT field that is or nests jsonb, or a column typed by such a UDT, slips past | Nested jsonb outside standalone | — | — | — | — | **REMOVED.** The UDT-field allow-check and its type-map resolution were deleted. The surviving placement rule still refuses jsonb (or a jsonb-nesting field type) entering a key through a UDT (SCH-T154a-05). |
| SCH-T300-05 | A node leaves standalone while jsonb columns exist | Cluster with jsonb and no ledger | — | — | — | — | **REMOVED.** Leaving standalone is no longer refused on account of jsonb columns; the departure check and its table scan were deleted with the gate. Residual closed: jsonb replicates on every topology (see ferrosa-cluster). |
| SCH-T300-06 | A scan for jsonb tables fails and is read as "none" | Departure allowed with jsonb present | — | — | — | — | **REMOVED.** The jsonb-table scan was deleted; nothing reads its result and nothing turns its error into a refusal. Departure is not gated on jsonb, so there is no residual. |
