---
type: todo
priority: P0
status: implemented
created: 2026-11-27
updated: 2026-11-27
board: t_95557f4c
---

# Bug: a failed `system_auth.roles` replay at startup fell through to seeding and overwrote `ferrosa_admin`

## Defect

`ferrosa/src/main.rs` (auth-enabled startup, introduced by `491fadc75`,
"fix(auth): persist role credentials across restart", 2026-09-16) replayed
persisted roles with:

```rust
match loader.replay_roles_into_schema(&schema) {
    Ok(count) if count > 0 => info!(..),
    Ok(_) => {}
    Err(e) => tracing::warn!(%e, "failed to replay persisted roles"),
}
seed_default_roles_with_config(&schema, &seed_config)?;
```

When the replay failed (one undecodable row makes `load_roles` fail for the
whole table; a storage read error does the same), the registry held none of
the persisted roles. Unless `schema.json` happened to carry `ferrosa_admin`,
`seed_role_if_absent` saw it as absent and created it with
`FERROSA_SEED_ADMIN_PASSWORD`, or the PUBLISHED default when that variable was
unset. The node then served that credential and refused the operator's
persisted one, and the next clean shutdown wrote the seeded hash into
`schema.json`.

Correction to the original report: `create_role_internal` is memory-only, so
the seed did not itself overwrite the `system_auth.roles` row. The measured
effect (red run) was: node ready, published default accepted, the operator's
rotated password refused.

Related defect found while reproducing: seeded roles were never written to
`system_auth.roles`. Their only durable copy was `schema.json`, written at a
clean shutdown or by an unrelated DDL barrier, so a crash before either left
nothing, and the next start seeded again from whatever seed env that process
had. A restart without `FERROSA_SEED_ADMIN_PASSWORD` then got the published
default (observed once with persistence disabled: the operator's password was
refused after crash + restart without the env).

A second, quieter route led to the same place: `StorageEngine::walk_token_range`
on an unregistered table returns `Ok` with nothing, and `main` only warns when
`register_system_tables` fails. An unregistered `system_auth.roles` therefore
read as "no roles persisted yet".

## Contract (after the fix)

- Startup role restoration is one call, `SystemTableLoader::bootstrap_roles`
  (`ferrosa-cluster/src/system_table_loader.rs`): replay, then seed, then repair.
- Seeding runs only when the replay SUCCEEDED. A successful replay of zero roles
  is the fresh-data-dir case; a failed replay is never treated as fresh.
- A replay failure returns `RoleBootstrapError::Unreadable` before anything is
  seeded or written. `main` logs it at FATAL, naming `system_auth.roles` and the
  cause, and exits with code `65` (`EXIT_AUTH_STORE_UNREADABLE`) before any
  listener binds.
- `load_roles` refuses an unregistered `system_auth.roles` instead of reading it
  as empty.
- Seed roles created at startup are written to `system_auth.roles` immediately,
  so a seeded credential is authoritative from its first start and survives a
  crash. A failure to write them fails startup loudly.
- The development-mode published default (`ferrosa_admin`/`ferrosa_admin` when
  `FERROSA_SEED_ADMIN_PASSWORD` is unset) is kept for compatibility. It can now
  only be written on a genuinely fresh store, and its creation logs a WARN that
  names the published default and the variable that would replace it.

## Upgrade behaviour

- Healthy auth store: unchanged. The replay succeeds, every persisted role and
  every role in `schema.json` is restored, seeding finds them present and
  writes nothing, exactly as before. Only a role that is genuinely absent from
  both is seeded (as before) and is now also persisted.
- Unreadable auth store: the node no longer starts. It exits 65 with a message
  naming the table and the decode or IO error, and the stored rows are left as
  they were. Before the fix the same node started and reset `ferrosa_admin`.

## Tests

- `ferrosa/tests/role_replay_failure_process.rs` (real binary, auth on):
  - `unreadable_role_store_refuses_to_start_and_keeps_the_stored_admin`:
    provision with an operator password, write an undecodable role row into the
    stopped node's store, restart without the seed env. RED before the fix (the
    node became ready and the published default logged in); now exit 65, the
    table and cause named, nothing seeded, the stored admin hash byte-identical.
  - `restarts_after_a_clean_shutdown_keep_the_seeded_admin` and
    `restarts_after_a_crash_keep_the_seeded_admin`: restart without and then
    with `FERROSA_SEED_ADMIN_PASSWORD`; the operator password logs in, the
    published default does not, nothing is seeded, and the seeded admin is
    persisted and unchanged. RED before the fix: no admin row persisted (and,
    schedule-dependent, a crash re-seed).
- `ferrosa-cluster/src/system_table_loader.rs` unit tests:
  `fresh_store_seeds_the_admin_once`,
  `intact_store_restart_keeps_the_admin_with_and_without_the_seed_env`,
  `undecodable_role_row_refuses_bootstrap_and_seeds_nothing`,
  `unregistered_roles_table_is_unreadable_not_fresh`.

## Not done

- Whether an auth-enabled node should seed the published default at all when
  `FERROSA_SEED_ADMIN_PASSWORD` is unset (development mode) is a product
  decision; tracked on the board as a follow-up to t_95557f4c.
