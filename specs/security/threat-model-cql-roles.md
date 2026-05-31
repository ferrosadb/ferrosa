# Threat Model — CQL role/auth DDL (CREATE/ALTER ROLE/USER options)

STRIDE analysis for the #74 work that adds `HASHED PASSWORD`, `OPTIONS`, and
`ACCESS TO/FROM …` to the role/user DDL grammar and execution.

## Scope & assets

- **Data flow:** client → CQL parser → `route_create_role`/`route_alter_role` →
  `Schema::create_role`/`alter_role` → role store (`salted_hash`) → Raft/DDL log.
- **Assets:** role credentials (`salted_hash`), authentication integrity (who can
  log in), authorization state (`SUPERUSER`, `LOGIN`, network access).
- **Trust boundary:** the CQL wire (untrusted client) → server role store.

## Grounding (verified in code)

- `PasswordHasher::verify_password_any` is **fail-closed**: an unrecognized hash
  prefix returns `Err`, not `Ok(true)`. The auth path must treat `Err` as denial.
- `route_create_role` already hashes plaintext on the coordinator to keep
  cleartext "off the wire and out of the Raft log" (router.rs).

## STRIDE inventory

| # | Cat | Threat | L×I | Mitigation |
|---|-----|--------|-----|------------|
| T1 | Spoofing | Admin sets `HASHED PASSWORD` to a hash whose preimage they know → log in as that role | 1×2 | Requires `CREATE/ALTER ROLE` privilege (already an admin op); accept as designed. Still validate the hash format (T3). |
| T2 | Tampering / EoP | Auth treats a malformed/unknown-format `salted_hash` as a match (fail-open) → any password logs in | 1×3 | **Verified fail-closed**: `verify_password_any` returns `Err` on unknown prefix. Add a test that login with a garbage-hash role is denied. |
| T3 | Tampering | `HASHED PASSWORD` stores an unverifiable credential (wrong algo) → role can never authenticate, or downstream verify errors | 2×2 | **Validate** the supplied hash is a supported PHC/bcrypt string (`$2a$/$2b$/$2y$/$argon2id$`) at CREATE/ALTER time; reject otherwise (**fail loud**). Store verbatim; do NOT run password-strength policy on a hash. |
| T4 | Repudiation | Role create/alter not audited | 1×2 | `Schema::create_role/alter_role` already emit audit events; ensure the new fields don't bypass that path. **Never** put password/hash in the audit payload. |
| **T5** | **Info disclosure (side-channel)** | **Secret in logs.** `CREATE ROLE … WITH PASSWORD='s'` / `HASHED PASSWORD='$2a$…'` carries the secret in the **query text**, which is logged verbatim on error — confirmed at `connection.rs` `"PREPARE failed for '{query}'"`. | **2×3** | **Redact** password/hashed-password literals from any logged query string (PREPARE + QUERY error paths). Add a `redact_role_secrets(query)` helper and a test asserting the secret never appears in the logged text. |
| T6 | Info disclosure (timing) | Non-constant-time verify, or distinguishable "no such role" vs "bad password" timing/errors → role/credential enumeration | 1×2 | bcrypt/argon2 verify is constant-time; auth returns a uniform `Bad credentials`. Don't weaken: the new paths feed the same verify. |
| T7 | Info disclosure | `salted_hash` exposed via `SELECT … FROM system_auth.roles` to non-superusers | 1×3 | Existing: router masks `salted_hash` for non-superuser callers (router.rs ~1757). Unchanged by this work. |
| T8 | DoS | Huge `OPTIONS` map / `ACCESS` set exhausts memory while parsing | 1×2 | Bound `OPTIONS`/`ACCESS` element counts (reuse `MAX_COLLECTION_ELEMENTS`). |
| **T9** | **EoP / false security** | **Silently ignoring `ACCESS TO/FROM …`.** Operator restricts a role to DC1; ferrosa parses and ignores it → role has no restriction → broader access than intended. | **2×3** | ferrosa has **no network authorizer**, so **reject** `ACCESS` clauses with a clear error rather than silently accept a security control we don't enforce (**fail loud**; matches the project's failure philosophy). Tracked as a separate feature if/when a network authorizer lands. |
| T10 | EoP | Non-superuser grants itself `SUPERUSER = true` | 1×3 | Existing `Schema::create_role/alter_role` enforce that only superusers may set superuser; unchanged. Add a regression test. |

## Decisions (security-driven)

1. **`HASHED PASSWORD`** — parse; **validate** the value is a supported hash
   format; store verbatim as `salted_hash` (no re-hash, no strength policy). A
   role created with a non-bcrypt/argon2 hash is **rejected**, not silently
   stored unusable.
2. **`OPTIONS = {…}`** — parse (bounded). Not a deny-control; ferrosa's password
   authenticator ignores them. Accepted and logged at debug; documented as
   not-interpreted. (Lower risk than ACCESS — OPTIONS grant nothing.)
3. **`ACCESS TO/FROM …`** — **rejected** with a clear error. Silently accepting
   an unenforced *restriction* is worse than not supporting it.
4. **Secret redaction (T5)** — redact `PASSWORD`/`HASHED PASSWORD` literals from
   any logged query text.

## Residual risk / open items

- Network authorization (`ACCESS`) enforcement is out of scope; rejecting the
  syntax is the safe interim. A future feature can implement the authorizer.
- A full audit that role-DDL audit events never contain secrets should be part
  of the secure-review pass before merge.
