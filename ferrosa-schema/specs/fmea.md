---
crate: ferrosa-schema
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-schema — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This crate is the auth + DDL authority, so security
failure modes dominate the high-RPN band. Rows reflect the **actual** code, not
aspirations.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| SC-1 | Well-known default credentials shipped | Bootstrap superuser defaults to password `"cassandra"`; `auth::bootstrap` seeds `ferrosa_admin`/`ferrosa_user` with passwords equal to their names. An operator who never rotates them leaves a trivially-guessable superuser/admin. | 9 | 6 | 4 | 216 | Partial. Bootstrap flags the default superuser `must_change` and emits `SuperuserPasswordMustChange`; `validate_production_requirements` flags `DefaultSuperuserPassword`. But the seed *admin/app* roles are not covered by that check, and nothing *forces* rotation. |
| SC-2 | Production TLS checks are stubs | `validate_production_requirements` has `CqlTlsNotConfigured` / `InternodeTlsNotConfigured` variants but the comment at `startup.rs:138` states the CQL/internode TLS checks are "stubs (added when those crates land)". Production mode passes TLS validation even with TLS fully disabled — a silent gap that contradicts fail-loud. | 8 | 5 | 7 | 280 | **Open gap.** The variants exist and `Display` works, but no code path ever pushes them. Wire the checks to real CQL/internode TLS config. |
| SC-3 | `apply_snapshot` / `create_table_internal` skip StorageEngine registration | These mutators update the in-memory snapshot but the doc-comment notes the caller MUST separately call `engine.register_table()`. A caller that forgets leaves a table visible in schema but unservable by storage — silent divergence. | 7 | 3 | 6 | 126 | Documented in the method doc-comment; not enforced in code. Relies on caller discipline (cluster catch-up path). |
| SC-4 | `HASHED PASSWORD` roles cannot use Postgres SCRAM | `create_role_hashed` / `alter_role` with `HASHED PASSWORD` store the hash verbatim and set `scram = None` (a bcrypt/argon2 hash can't derive a SCRAM verifier). Such roles silently fail Postgres SCRAM login until a plaintext reset. | 5 | 4 | 5 | 100 | Documented D4 gap in `registry.rs` / `scram.rs`. `alter_role` clears the stale verifier so it fails closed; CQL login still works. |
| SC-5 | Concurrent role-grant cycle race | `grant_role_internal` checks `would_create_cycle` against current state only. Two independently-committed grants (`GRANT a TO b`, `GRANT b TO a`) could in principle form a cycle. | 6 | 2 | 5 | 60 | Mitigated: Raft applies entries serially, so the second grant sees the first edge already present and is rejected. `check_permission` is additionally cycle-safe via a `visited` set, so even a leaked cycle cannot hang traversal. |
| SC-6 | Full-snapshot clone on every mutation | Each DDL/grant clones the entire `SchemaSnapshot` (all keyspaces, tables, roles, grants, indexes, types, functions, aggregates) under `write_lock`. On a very large schema this makes every write O(schema size). | 4 | 4 | 3 | 48 | Accepted trade-off for lock-free reads via `ArcSwap`. Reads (hot path) are unaffected. Revisit if schema cardinality grows large. |
| SC-7 | Auto-rehash on login swallows failures silently | In `authenticate`, the hash-upgrade block uses `if let Ok(new_hash) = ...` and on error simply leaves the old hash; there is no log line when the rehash fails. | 3 | 3 | 6 | 54 | Login still succeeds with the old hash (fail-safe), but a persistently-failing rehash is invisible. Add a `tracing::warn!` on the error arm. |
| SC-8 | Identifier validation is ASCII-only and length-capped at 48 | `validate_table`/`validate_keyspace` reject anything but `[A-Za-z0-9_]{1,48}`. Quoted/Unicode identifiers that Cassandra permits are rejected. | 3 | 3 | 2 | 18 | Intentional conservative subset; low risk, surfaces a clear `InvalidSchema` error (fail-loud). |

## Top risks to act on

1. **SC-2 (RPN 280)** — production TLS validation is a stub. The security gate
   *looks* complete (the violation variants exist) but never fires, so a
   `FERROSA_MODE=production` deployment with TLS off passes. This is the most
   dangerous kind of gap: a safety check that silently no-ops. Wire it to real
   config.
2. **SC-1 (RPN 216)** — default/seed credentials. The superuser path is
   partially mitigated (must-change flag + production check), but the seeded
   `ferrosa_admin` / `ferrosa_user` roles ship with password == username and are
   not covered by `validate_production_requirements`. Extend the production check
   to flag unrotated seed roles, or generate random seed passwords in production.

## Detection assets

- `auth/permission.rs` tests — superuser bypass, direct/keyspace/role-hierarchy
  grants, cycle-safety, deny-by-default.
- `startup.rs` tests — production violations for default password, weak policy,
  S3 HTTP, env secrets (but **not** TLS — see SC-2).
- `auth/password.rs` + `scram.rs` tests — hash round-trip, rehash detection,
  hash-format validation, SCRAM derivation.
- 354 in-crate tests + 19 integration tests
  (`tests/{auth_integration,integration,property_tests}.rs`).
</content>
