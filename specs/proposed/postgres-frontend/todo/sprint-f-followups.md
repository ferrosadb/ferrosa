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
