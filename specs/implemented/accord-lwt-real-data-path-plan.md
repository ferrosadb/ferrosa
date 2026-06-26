# Accord LWT — full production data path (TDD implementation plan)

**Status:** in-process. Branch `fix/accord-lwt-real-data-path`.
**Closes:** `specs/bug-accord-lwt-acks-phantom-write.md` (P0 fake-success / silent data loss).
**Decision:** the FULL fix (real linearizable LWT through Accord), not the interim fail-loud/flag-off.

## Where it stands (verified map)
The Accord PROTOCOL layer is real and tested over TCP: PreAccept / Accept / Commit /
ReadVote / Apply all fan out via `PeerManager`, F+1 quorums, persist-before-reply via
`SyncWriter`, idempotent state-machine transitions, dep-wait orchestration
(`DepWaitApplier`). The 7 protocol "Gaps" are closed.

What is STUBBED is the **semantic layer + production wiring**:
- `ferrosa-cql/src/router.rs:1016` — `key = b"lwt-placeholder-key"` (no real partition key).
- `route_lwt_via_accord` discards `_stmt` — no mutation cells, no IF predicate carried.
- `coordinator.rs:797` Apply payload `result_data = key.clone()` — no mutation.
- `state_machine.rs:362 handle_apply` — persists `"Applied:{t}"` to the protocol log but
  **never writes the row**; `AccordStateMachine` has a `sync_writer` but **no `StorageApplier`**.
- `apply.rs` — `StorageApplier` trait + `NoopStorageApplier` (test-only) + `DepWaitApplier`
  exist, but **no real applier writes to `StorageEngine`**, and none is constructed in prod.
- `state_machine.rs:451 read_condition_holds_at` — hardcoded `INSERT IF NOT EXISTS` (conflict
  index), not a generic `IF col=val` evaluated against the stored row; ReadVoteOk returns no row.
- `ferrosa/src/main.rs` — `peer_manager: None` in `SharedState`; no applier wired → real clients
  can't even reach the path.

## TDD increments (red → green → verify → commit, in order)

1. **RED: prove the phantom write.** State-machine/handler test: drive a txn to `handle_apply`
   with mutation bytes + a capturing `StorageApplier`; assert the applier receives the mutation.
   Fails today (no applier on the state machine). *(Anchors the bug at the apply seam.)*
2. **Real persist seam.** Add `applier: Arc<dyn StorageApplier>` to `AccordStateMachine`
   (constructors default to `NoopStorageApplier`; `with_applier` for prod). In `handle_apply`,
   after the protocol-log sync, build `ApplyMutation { data: result_data, t, deps }` (t/deps from
   `TxnState`) and call `applier.apply(...)` (idempotent, persist-before-Applied). GREEN on #1.
3. **Real `StorageApplier` impl.** `EngineStorageApplier` decoding `data` as
   `(TableId, DecoratedKey, Row, ts)` → `StorageEngine::write(...)`. Idempotent on (txn_id,t).
   Test: applying persists a row readable via the engine.
4. **Mutation flow router→Apply.** `route_lwt_via_accord` extracts the real `DecoratedKey` and
   serializes the mutation from the `Insert/Update/Delete` AST; thread it through the
   coordinator's Apply payload (`result_data` = encoded mutation, not key). Test: coordinator
   Apply carries + applies the real mutation.
5. **Generic IF read + evaluation.** Send the IF predicate(s) `(col, op, val)` in
   `ReadVotePayload`; replica evaluates against the stored row (reuse the local LWT condition
   logic), returns `condition_holds` + the current row bytes. Coordinator maps to
   `[applied]=false` + `current_values`. Test: `IF col=val` true/false paths + current-row return.
6. **Production wiring.** Thread `PeerManager` + an `EngineStorageApplier` into `SharedState` /
   `AccordHandler` in `main.rs` (replace `peer_manager: None`). Construct the applier from the
   live `StorageEngine`. Replica ids from the cluster ring.
7. **End-to-end cluster test.** Real CQL `INSERT … IF NOT EXISTS` / `UPDATE … IF col=val` in
   cluster mode: persists, returns correct `[applied]`/`current_values`, survives a node restart;
   concurrent contenders → exactly one applies. Extend `accord_lwt_concurrent.rs`.
8. **Multi-statement (optional follow-on).** `BEGIN/COMMIT TRANSACTION` currently no-op
   (`router.rs:1343`, `session.rs TransactionState` unused) — wire atomic multi-statement or keep
   rejecting honestly. Track separately.

## Verification each increment
`cargo fmt --check`, `cargo clippy --all-targets`, `cargo test -p ferrosa-cluster -p ferrosa-cql`;
full-cluster integration for #7. No `#[ignore]`, no fake success — a stubbed sub-path must return
`Unavailable`/`ServerError`, never `[applied]=true`.

## Key files
- `ferrosa-cluster/src/accord/{state_machine.rs, apply.rs, coordinator.rs, handlers.rs}`
- `ferrosa-cql/src/router.rs` (`route_lwt_via_accord`, the local LWT condition logic to reuse)
- `ferrosa/src/main.rs` (SharedState wiring), `ferrosa-net/src/accord_messages.rs` (payloads)
- Tests: `ferrosa-cluster/tests/accord_lwt_concurrent.rs`
