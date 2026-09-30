---
crate: ferrosa-schema
doc: roadmap
last_updated: 2026-09-17
---

# ferrosa-schema — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), in-code doc-comments
(D4 SCRAM, `apply_snapshot` registration caveat, the TLS-stub comment at
`startup.rs:138`), and the dependency/usage review. No `TODO`/`FIXME` markers
exist in the source — the gaps below come from code review, not grep.

## Now (highest value)

- **Production mutual TLS** (FMEA SC-2 follow-on). One-way TLS on every listener
  and internode is enforced (t_d5d122ba); `CqlMutualTlsNotConfigured`,
  `InternodeMutualTlsNotConfigured` and `UnencryptedLocalStorage` are still never
  pushed. Mutual TLS is t_b6c820f4.

- **Keep development seed credentials confined to development** (FMEA SC-1).
  Production now generates random passwords, and the legacy `cassandra` role
  requires an explicit secret. The remaining risk is operational: a development
  node still uses documented `ferrosa_admin` / `ferrosa_user` credentials and
  must not be exposed to untrusted networks.

## Next

- **Enforce StorageEngine registration after `apply_snapshot`** (FMEA SC-3).
  Today the in-memory snapshot and the storage table set can silently diverge if
  a caller forgets `engine.register_table()`. Either return the list of tables
  that need registration, or surface a check/assertion so the divergence is
  loud.

- **Populate SCRAM verifiers for `HASHED PASSWORD` roles** (FMEA SC-4, D4).
  Decide a policy: reject `HASHED PASSWORD` for roles that need Postgres login,
  or document the plaintext-reset requirement at the DDL boundary so operators
  aren't surprised by a silently-unauthenticatable Postgres role.

- **Log auto-rehash failures** (FMEA SC-7). Add `tracing::warn!` to the error
  arm of the login-time hash upgrade so a persistently-failing rehash is visible.

## Later

- **Bound the snapshot-clone cost** (FMEA SC-6). If schema cardinality grows
  large, the per-mutation full clone becomes O(schema size). Consider a
  copy-on-write structural-sharing layout (e.g. persistent maps) so writes only
  clone touched sub-trees while keeping lock-free reads.

- **Widen identifier validation** (FMEA SC-8) to match Cassandra's
  quoted/Unicode identifier rules if a front-end needs them.

## Non-goals

- **Durability / SSTable writes** — this crate publishes the column-index
  contract and `SystemTableMutation`; persisting rows belongs to
  `ferrosa-storage`.
- **Raft replication mechanics** — the `*_internal` mutators are the trusted
  apply surface; the consensus and convergence logic lives in `ferrosa-cluster`.
- **Wire-protocol framing / query planning** — those belong to the front-ends
  (`ferrosa-cql`, `ferrosa-postgres`, `ferrosa-flight`).
</content>

## jsonb (T-150)

Done: type threading. Remaining: None for T-150.

## T-300 follow-ups

- T-154b replaces `jsonb_ddl_permitted` with the `ferrosa.jsonb.v1` ledger check
  at the same call sites and lifts the standalone-only rule.
- `jsonb_ddl_refused_total{mode}` is an in-process counter
  (`jsonb_rules::jsonb_ddl_refused_total`); exporting it to Prometheus is not
  wired yet.

## T-154a follow-ups

- T-154b: the `ferrosa.jsonb.v1` capability gate at propose and apply.
- ALTER ... TYPE to or from jsonb is not applied by any path yet (the CQL router
  rejects ALTER TYPE outright), so no rule exists for it here.
