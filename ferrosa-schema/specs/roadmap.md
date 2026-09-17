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

- **Wire the production TLS checks** (FMEA SC-2). `validate_production_requirements`
  already owns `CqlTlsNotConfigured`, `CqlMutualTlsNotConfigured`,
  `InternodeTlsNotConfigured`, `InternodeMutualTlsNotConfigured` and
  `UnencryptedLocalStorage`, but none is ever pushed. Thread real CQL/internode
  TLS config into `ProductionCheckConfig` so a `FERROSA_MODE=production` node
  with TLS disabled actually fails the gate. This is a silent safety no-op today.

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
