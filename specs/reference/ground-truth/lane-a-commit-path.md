# Ground truth — the Accord commit path, end to end (Lane A)

- **Repo / revision:** `/Users/bkearns/src/ferrosa-suite/.wt-int2`, branch `feat/read-in-transaction`, HEAD `32fc001aecb32532ee84cb69305597fe78594e0b`.
- **Method:** read-only. Every claim below carries a `path:line` citation, and the quoted source line(s) were read directly from the file at that line. Where a comment contradicts the code, BOTH are recorded and flagged `CONTRADICTION`. Anything not determinable is under **Could not determine**.
- **One transaction traced:** a plain PostgreSQL `BEGIN; <DML>…; COMMIT;` over a CQL/Postgres front end, committed as one multi-key Accord transaction through `AccordTransactionCommitter`.

---

## 0. Hop-by-hop trace (front-end submit → durable row)

| # | Hop | Owner of the pending changes | Type | Citation |
|---|-----|------------------------------|------|----------|
| 1 | `BEGIN` opens the block and takes a snapshot | `Session` | `Session` (no `session.rs`; it lives in `extended.rs`) | `ferrosa-postgres/src/server.rs:656-658`, `:677-705`; `Session::txn_writes: TxnWriteSet` at `ferrosa-postgres/src/extended.rs:103` |
| 2 | Each in-txn DML is **buffered**, not applied | the session write-set | `TxnWriteSet { stage: WriteSetStage, count: usize }` | dispatcher comment `ferrosa-postgres/src/server.rs:668-671`; `Session::begin_txn` sets `self.txn_writes = TxnWriteSet::default()` `ferrosa-postgres/src/extended.rs:329`; `TxnWriteSet` def `ferrosa-postgres/src/txn_write_set.rs:47-52` |
| 3 | The buffer call: one serialized `Mutation` frame per row | the `WriteSetStage` inside the session | `WriteSetStage` (resident until 8 MiB, then a private mmap file) | `ferrosa-postgres/src/query.rs:1561-1572`; `TxnWriteSet::push` `ferrosa-postgres/src/txn_write_set.rs:88-95`; `WriteSetStage` def `ferrosa-storage/src/write_set_stage.rs:99` |
| 4 | `COMMIT` drains the session buffer | `StagedWriteSet` (moved out of the session) | `StagedWriteSet` | `commit_txn` `ferrosa-postgres/src/server.rs:912-921`; `Session::take_txn_stage` (`std::mem::take`) `ferrosa-postgres/src/extended.rs:422-427`; `StagedWriteSet` def `ferrosa-storage/src/write_set_stage.rs:402` |
| 5 | The staged set is streamed, one `TransactionWrite` at a time, into the Accord write-set builder | `StagedAccordWriteSetBuilder` | `{ keys: Vec<(String,Vec<u8>)>, payloads: WriteSetSpillBuilder, tombstones, tombstone_key }` | build `ferrosa-postgres/src/server.rs:1055-1061`; builder def `ferrosa-storage/src/accord/transaction_committer.rs:205-212` |
| 6 | Handoff to the committer | `StagedAccordWriteSet` (by value) | `{ keys: Vec<(String,Vec<u8>)>, payloads: Arc<WriteSetSpill>, tombstones: Vec<TransactionWrite> }` | call `ferrosa-postgres/src/server.rs:1100-1101`; signature `ferrosa-storage/src/accord/transaction_committer.rs:358-364`; def `:100-112` |
| 7 | Committer rebuilds only the small routing index; payload bulk stays in the spill | driven write-set + `Arc<WriteSetSpill>` | `Vec<(Vec<u8>,Vec<u8>)>` (keys resident, mutation empty) + `Some(Arc<WriteSetSpill>)` | `AccordTransactionCommitter::commit_postgres` `ferrosa-cluster/src/accord/transaction_commit.rs:257-313` (`payloads_arc()` at `:303`) |
| 8 | Accord driver holds the write-set + the spill | `AccordCoordinatorDriver` | `write_set: Vec<WriteSetEntry>` + `write_blobs: Option<Arc<WriteSetSpill>>` + `staged_prefix: Option<usize>` | `ferrosa-cluster/src/accord/coordinator.rs:640`, `:713`, `:721`; struct at `:606` |
| 9 | PreAccept / (Accept) / Commit ordered by consensus | — (protocol state, not payload) | `AccordCoordinator` (`CoordinatorPhase`, `CoordinatorDecision`) | `coordinator.rs:226`, `:189-200`, `:208-217`, `:1530` |
| 10 | Apply fan-out to replicas + the coordinator's own shard | borrowed `(key, mutation)` slices from resident set / spill mmap | `BorrowedWriteSetEntries<'a> = Vec<(&'a [u8], &'a [u8])>` | `coordinator.rs:57`, `:2741-2753`, `:3070-3105`, `:3128-3155` |
| 11 | Replica applies: decode + one atomic storage batch | `AccordWriteSet<'s,'v>` (borrowed) | `{ mutations: &[MutationView], survivors: &[usize], op_count }` | `ferrosa-cluster/src/accord/apply.rs:1118-1126`, `:1206-1210`, `:1341-1350` |
| 12 | Durable write + memtable visibility | storage engine | commitlog append + `force_sync`, then memtable | `ferrosa-storage/src/engine.rs:9593`, `:9637-9669` |
| 13 | Apply bookkeeping (protocol-log marker) | `AccordStateMachine` | `txn_states`/`conflict_index`/`sync_writer` | `ferrosa-cluster/src/accord/state_machine.rs:96-109`, `:1227-1238`, `:1296-1314` |

### The `BEGIN` buffer → Accord write-set handoff (explicitly)

The task's `ferrosa-postgres/src/server.rs:669-671` is the **comment** on the dispatcher arm; verified verbatim:

```
669|         // re-parses; cheap, and keeps the executor self-contained.) In an open
670|         // transaction, DML is BUFFERED into the session write-set instead of
671|         // applied; autocommit (no open txn) applies immediately.
```

The concrete handoff chain, each step cited:

1. `BEGIN` → `begin_block` → `session.begin_txn(isolation, snapshot)` which resets `self.txn_writes = TxnWriteSet::default()` (`server.rs:701`, `extended.rs:324-336`).
2. In-txn DML → `run_data_statement` passes `Some(session.txn_writes_mut())` to the executor (`server.rs:855-865`, accessor `extended.rs:360-362`); the DML executor buffers with `buffer.push(PgWrite(mutation))` (`query.rs:1561-1572`; batch sibling `query.rs:1646-1657`); `TxnWriteSet::push` serializes the `Mutation` to `self.stage.append(&frame)` (`txn_write_set.rs:88-95`).
3. `COMMIT` → `commit_txn` (`server.rs:659`, `:901`) drains via `session.take_txn_stage()` (`server.rs:912`, `extended.rs:422-427`) producing the `StagedWriteSet`.
4. `prepare_accord_writes_streaming(..., &mut |write| staged_writes.accept(write))` streams one `TransactionWrite` at a time into `StagedAccordWriteSetBuilder` (`server.rs:1056-1061`); `accept` routes tombstones aside else `payloads.append(&write.mutation); keys.push(...)` (`transaction_committer.rs:280-301`).
5. `committer.commit_postgres(&ctx.default_schema, accord_writes, tables, cluster_snapshot)` (`server.rs:1100-1101`) → `commit_postgres` (`transaction_commit.rs:257`) → `drive_accord_core(..., Some(payloads), staged_count, ...)` (`transaction_commit.rs:303-312`) → the `AccordCoordinatorDriver` (`coordinator.rs:606`).

Net: the buffer crosses three concrete types — `TxnWriteSet`/`WriteSetStage` (session) → `StagedWriteSet` → `StagedAccordWriteSet`/`Arc<WriteSetSpill>` — and only the *encoded mutation bytes* move; keys/order are the only resident per-write index.

---

## 1. Protocol phases — sequence, and the function/file each is decided in

Phases are decided by **`AccordCoordinator`** (the pure per-txn state machine, `coordinator.rs:226`), driven over the network by **`AccordCoordinatorDriver`** (`coordinator.rs:606`). Phases 1–4 are run by `AccordCoordinatorDriver::order_and_gate` (`coordinator.rs:1530`, doc `:1527-1529`).

**Phase enum:** `CoordinatorPhase { PreAccepting, FastPathCommit, Accepting, SlowPathCommit, Committed }` (`coordinator.rs:189-200`).
**Decision enum:** `CoordinatorDecision { Pending, FastPathCommit{t,deps}, NeedAccept{t,deps}, SlowPathCommit{t,deps} }` (`coordinator.rs:208-217`).

| Phase | Decided in (function, file:line) | Decision |
|-------|----------------------------------|----------|
| **PreAccept** (send) | `order_and_gate` builds the frame: single-key `bincode::serialize(&pa_payload)` → `Message::AccordPreAccept` (`coordinator.rs:1585-1587`); multi-key → `Message::AccordPreAcceptV2` (`coordinator.rs:1598-1600`) | — |
| **PreAccept** (fast-path vs Accept decision) | `AccordCoordinator::handle_preaccept_ok` (`coordinator.rs:321`) | fast quorum + all agree ⇒ `FastPathCommit` `:372-379`; slow quorum + any disagreement ⇒ `NeedAccept` (phase `Accepting`) `:384-397`; else `Pending` `:400` |
| **PreAccept** (window-expiry fallback) | `AccordCoordinator::finalize_preaccept` (`coordinator.rs:420`) | if `< slow_quorum_size` ⇒ `Pending` `:425-427`; else phase `Accepting`, `NeedAccept` `:429-434` |
| **Accept** (slow path) | `AccordCoordinator::handle_accept_ok` (`coordinator.rs:441`) | when `accept_responses.len() >= slow_quorum_size` ⇒ phase `SlowPathCommit`, `SlowPathCommit` `:470-476`; else `Pending` `:479` |
| **Accept dispatch** (driver) | `order_and_gate` `coordinator.rs:1965-1967`: `FastPathCommit` ⇒ commit directly; `NeedAccept` ⇒ run Accept `bincode::serialize(&accept_payload)` → `Message::AccordAccept` (`coordinator.rs:1976-1978`) | — |
| **Commit** (decision) | `order_and_gate`: `let commit_txn = txn_id;` then `quorum_broadcast(commit_msg, …)`; failure ⇒ `Err(AccordDriverError::QuorumUnavailable)` (`coordinator.rs:2183-2195`) | coordinator counts itself as an implicit ack (`coordinator.rs:619-622` doc) |
| **Commit** frame build | `bincode::serialize(&commit_payload)` → `Message::AccordCommit` (`coordinator.rs:2137-2139`) | — |
| Quorum sizes | `slow_quorum_size(rf) = rf/2 + 1` (`coordinator.rs:127-130`); `fast_quorum_size(rf) = (3f+1)/2 + 1`, `f = rf - slow_quorum_size(rf)` (`coordinator.rs:142-151`) | — |
| **Apply** (fan-out) | `order_and_gate` → `apply_phase_within`: `write_set.len()==1` ⇒ `quorum_broadcast_within(Message::AccordApply …)`; else `apply_fanout_bounded_within` (`coordinator.rs:2741-2753`) | — |

The replica side receives these in `ferrosa-cluster/src/accord/handlers.rs`: PreAccept `:432`, Accept `:513`, Commit `:537`, ApplyV2 `:590`, all `bincode::deserialize`.

---

## 2. What owns the transaction's pending changes, at each hop (concrete type + lifetime)

| Structure | Concrete type | Owns | Lifetime | Citation |
|-----------|---------------|------|----------|----------|
| Session write-set | `TxnWriteSet { stage: WriteSetStage, count: usize }` | serialized `Mutation` frames in a spilling stage | one txn (`TxnWriteSet::default()` on `begin_txn`/`end_txn`) | `txn_write_set.rs:47-52`, `extended.rs:329`, `:351` |
| Session staging | `WriteSetStage { dir, threshold_bytes, resident: Vec<Vec<u8>>, offsets, lens, … }` | resident prefix + (post-spill) an mmap file | until `finish()` / drop | `write_set_stage.rs:99-105`, `:156-160` |
| Drained staged set | `StagedWriteSet { map: Option<Mmap>, … }` | the mmap spill region | moved into COMMIT | `write_set_stage.rs:402-406` |
| Committer input | `StagedAccordWriteSet { keys: Vec<(String,Vec<u8>)>, payloads: Arc<WriteSetSpill>, tombstones: Vec<TransactionWrite> }` | resident routing index + `Arc<WriteSetSpill>` payload bulk | `'static` behind the `Arc` | `transaction_committer.rs:100-112` |
| Committer builder | `StagedAccordWriteSetBuilder { keys, payloads: WriteSetSpillBuilder, tombstones, tombstone_key }` | one `TransactionWrite` at a time | during staging | `transaction_committer.rs:205-212` |
| Driver write-set | `write_set: Vec<crate::accord::wire::WriteSetEntry>` (key resident; `mutation` EMPTY for staged entries) | keys + order (and, for the resident path, the payloads) | the driver's whole lifetime | `coordinator.rs:640`; `WriteSetEntry { key, mutation }` `ferrosa-cluster/src/accord/wire.rs:81-86` |
| Driver spill | `write_blobs: Option<Arc<ferrosa_storage::write_set_spill::WriteSetSpill>>` + `staged_prefix: Option<usize>` | the mmap payload region + the staged boundary | driver lifetime | `coordinator.rs:713`, `:721` |
| Borrowed view on fan-out | `BorrowedWriteSetEntries<'a> = Vec<(&'a [u8], &'a [u8])>` | zero-copy `(key, mutation)` slices from resident set OR spill mmap | transient per frame (dropped before each RPC) | `coordinator.rs:57`, `:1232-1248`, `:3150-3152` |
| Applier write-set | `AccordWriteSet<'s,'v> { mutations: &'s [MutationView<'v>], survivors: &'s [usize], op_count }` | borrows partitions + survivor indices only | the single `write_atomic_batch` call | `apply.rs:1118-1126` |
| Parked (dep-wait) set | `ParkedWriteSet::Resident(Vec<ApplyMutation>)` \| `Staged { spill: Arc<WriteSetSpill>, entries: Vec<StagedMutation> }` | either owned payloads or mmap spill + small index | until dep resolves or `reclaim_stale` | `apply.rs:108-119`, `:123-127`, `:95-98` |
| Committed protocol record | `TxnState { txn_id, t0, t, t_max, deps: HashSet<TxnId>, phase: TxnPhase, max_ballot_seen, accepted_ballot, result: Option<Vec<u8>> }` | per-txn consensus state, in `txn_states: HashMap<TxnId, TxnState>` | txn (until GC/retention) | `state_machine.rs:96-109`; `TxnState` def `ferrosa-common/src/accord.rs:425-446` |

**Ownership summary.** Owned payloads exist only (a) on the small resident path inside `drive_accord` (`Vec<TransactionWrite>`, `transaction_commit.rs:330-335`), and (b) transiently in `build_parked_set`. The large path keeps only keys resident; every payload byte lives in an `Arc<WriteSetSpill>` (mmap) read back per entry, or as `(&[u8], &[u8])` borrows.

---

## 3. Where the write-set is materialized or spilled, and what bounds it

**Spill creation (committer → driver).** `drive_accord` computes `payload_bytes = Σ write.mutation.len()` and stages only if `WriteSetSpill::should_stage(payload_bytes)`, else it keeps owned bytes (`transaction_commit.rs:353-374`); staging is `reserve_write_set_stage()` + `WriteSetSpill::stage(reservation, &mut blobs)`, which drains each blob to a temp file and mmaps it read-only (`transaction_commit.rs:356-364`; `ferrosa-storage/src/write_set_spill.rs:152-163`, mmap at `:203-205`). **Spill type:** `WriteSetSpill { map: Option<memmap2::Mmap>, _reservation, path, resident: Vec<Vec<u8>>, resident_bytes, offsets: Vec<u64>, lens: Vec<u32>, bytes: u64 }` (`write_set_spill.rs:84-92`).

**Bounds.**
- `WRITE_SET_SPILL_FLOOR_BYTES = 8 * 1024 * 1024` (8 MiB) — the default resident buffer / staging floor (`write_set_spill.rs:77`).
- `WriteSetSpill::should_stage(total_bytes)` = `total_bytes >= threshold` (`write_set_spill.rs:135-141`), threshold from `FERROSA_WRITE_SET_SPILL_THRESHOLD_ENV` (`ferrosa-storage/src/write_set_stage.rs:71`) via `resolve_spill_threshold` (invalid/zero ⇒ floor, `write_set_stage.rs:80-96`).
- The front-end session buffer is bounded by the *same* floor through `WriteSetStage`: resident while `resident_bytes + len <= threshold_bytes`, else spill (`write_set_stage.rs:212-231`); `WriteSetStage::new` reads the env once (`:156-160`).
- **The threshold is explicitly a BUFFER SIZE, never a cap** — a larger write-set spills, never refused: `write_set_spill.rs:64-77` ("This is an absolute cap on one transaction's resident payload set"), `write_set_stage.rs:199-205` (only the resident index grows), and the buffering site states "NO capacity refusal … the old `FERROSA_POSTGRES_MAX_TXN_WRITES` / `53400` limit is gone" (`query.rs:1563-1565`).

**Materialization into wire frames.** The per-peer Apply frame is built by the `encode_frame` closure `coordinator.rs:3070-3105`: region-reference capnp (`encode_accord_apply_v2_region[_with_compression]`) preferred, else capnp-inline (`encode_accord_apply_v2`), else **bincode** of `ApplyV2Payload` as the mixed-cluster fallback (`:3092-3104`). The capnp encoders write each borrowed `(key, mutation)` slice straight into the arena / region so no owned copy of the write-set is made (`coordinator.rs:3070-3105`; doc of the inline encoder `ferrosa-cluster/src/accord/wire.rs:448-450`). On the replica, decoding the region materializes one owned `Vec<u8>` per write that the *applier* requires (`handlers.rs:640-664`).

---

## 4. What is durable, and at which instant — when can a reader observe a row?

**Protocol (consensus) durability — the protocol log.** The production writer is `FileSyncWriter::new(accord_dir.join("protocol.log"))`, wired exactly once at `ferrosa-cluster/src/controller/cluster.rs:1632`. Each phase persists **before** advancing in-memory state, via `SyncWriter::write_and_sync`, which appends `frame_record(data)` in one `write_all` then `file.sync_all()` (`ferrosa-storage/src/accord/sync_writer.rs:112-145`). The on-disk framing is `[8-byte magic "FACCLOG1"] then repeated [u32 LE length][length bytes]` (`ferrosa-storage/src/accord/framed_log.rs:23-24`, `:40`, `:43-48`).
- PreAccept writes `format!("PreAccepted:{}:{}", …)` and rolls back on fsync failure (`state_machine.rs:787-803`).
- Accept writes `format!("Accepted:{}:{}:{}", …)`, no advance on failure (`state_machine.rs:861-872`).
- Commit writes `format!("Committed:{}:{}", …)`, then (only after it is durable) `state.commit(...)`, `committed_txns.insert`, `conflict_index.set_commit_ts` (`state_machine.rs:927-947`).
- Apply writes `format!("Applied:{}", …)` as the marker; only after it, `state.apply()` + `conflict_index.mark_applied` (`state_machine.rs:1296-1314`).

**Row durability — the data write.** The applied row is written by ONE call: `EngineStorageApplier` decodes the survivors and calls `self.engine.write_atomic_batch(AccordWriteSet { mutations, survivors, op_count })` (`apply.rs:1341-1350`). `StorageEngine::write_atomic_batch` (`ferrosa-storage/src/engine.rs:9593`) does, in order: Phase 1 appends every mutation with `self.commit_log.append_for_explicit_sync(m)` (`engine.rs:9641-9646`); then the durability barrier `self.commit_log.force_sync()?` (`engine.rs:9648-9658`); then Phase 2 writes each row into the active memtable via `state.store.write(&m.key, row.clone())` (`engine.rs:9660-9669`). The memtable write is `guard.active.put(key, row, …)` (`ferrosa-storage/src/store.rs:3884-3888`), and reads merge "active memtable, flushing memtable … and SSTables" (`store.rs:3890-3898`).

**Answer to "when is a row observable": strictly AFTER the commit-log fsync.** The row becomes readable in Phase 2 (memtable), which the code orders after `force_sync` in Phase 1; the engine comment makes this explicit: the barrier runs "BEFORE it is made visible in the memtable (Phase 2)" and the hazard it removes is "the rows would become readable while still unsynced" (`engine.rs:9648-9652`). So: durable in the commit log at `force_sync` (`engine.rs:9658`); observable (memtable) immediately after Phase 2 (`engine.rs:9660-9669`) — with no read that can see the row before the fsync. `apply.rs:354` states the applier contract: "The implementation must persist the write before returning `Ok(())`."

**What is merely in memory:** the `StagedAccordWriteSet`/`WriteSetSpill` mmap, the driver's `write_set`, the coordinator's protocol state (`AccordCoordinator`), the `AccordStateMachine::txn_states`/`committed_txns`/`conflict_index` (the conflict index is in-memory only and built empty via `ConflictIndex::new(...)` `state_machine.rs:323`), and the `DurabilityService` sync point (see Could-not-determine).

---

## 5. The apply fan-out — distribution and what bounds residency

- Multi-key writes go through `apply_fanout_bounded_within` (reached when `write_set.len() != 1`, `coordinator.rs:2741-2753`).
- Each replica gets a frame scoped to ONLY the keys it owns (`self.borrowed_write_set_entries(|key| self.replica_owns_key(peer_id, key))`, `coordinator.rs:3142-3145`), unless the write-set is uniform across peers, in which case ONE frame is built and shared by refcount (`apply_payload_is_uniform` `coordinator.rs:1261-1264`; `Some(frame) => frame.clone()` `:3132-3134`).
- **Residency bound:** `const APPLY_FANOUT_WINDOW: usize = 4` (`coordinator.rs:52`); the loop keeps `inflight.len() < APPLY_FANOUT_WINDOW` (`coordinator.rs:3129-3155`), and the borrows are freed before each RPC (`drop(entries)` `:3150-3152`). The doc: "keeps AT MOST `APPLY_FANOUT_WINDOW` replica payloads resident at a time, and lets the sends OVERLAP" (`coordinator.rs:2914-2915`).
- On the replica the multi-key write-set is applied **all-or-nothing** as one `write_atomic_batch` (`apply.rs:1335-1350`), preflighted so "a failure on any key must persist none" (`apply.rs:357-369`; preflight loop `engine.rs:9598-9634`); the applier's default non-atomic per-mutation loop is overridden precisely for this (`apply.rs:366-379`).
- Dependency fan-out is Txn-level: the union of every entry's deps (`apply.rs:1691-1693`); a write-set parked behind an unapplied dep is bounded by size (stage ≥ floor) and by time (`DEFAULT_PARKED_APPLY_RECLAIM_SECS = 60`, `apply.rs:56`, `:59`; `reclaim_stale` `:1510-1548`).

---

## 6. Failure points after some part is already durable — and what is left behind

**Coordinator (`coordinator.rs`).** The wrapper `run_transaction` catches any `order_and_gate` error that is not `ConditionNotMet` and finalizes the txn as a no-write (local + spawned remote) (`coordinator.rs:1490-1511`).
- PreAccept decode fail / remote `snapshot_stale`: replicas may hold a `PreAccepted` conflict entry; no rows. Cleanup: no-write finalize + `finalize_late_preaccept_registrations` (`coordinator.rs:1810-1824`, `:1896-1911`). Late preaccept votes are ignored, not panicked (*contradiction #3 context*, `:333-336`).
- Accept decode fail / no Accept quorum: replicas may hold `Accepted`; no rows (`coordinator.rs:2061-2065`, `:2097-2114`).
- **Local Commit not durably recorded** (fsync attempted, remotes not yet broadcast): `Err(... "coordinator local Accord commit was not durably recorded")`; possibly a partial local commit record (`coordinator.rs:2169-2173`).
- **Commit quorum not reached** (after the coordinator's own local Commit is already durable): `Err(QuorumUnavailable)`; durable `Committed` on the coordinator (maybe some remotes), no rows — the mutation travels only in Apply (`coordinator.rs:2190-2195`).
- **Apply quorum not reached (abandon)** and **coordinator's own apply errors/didn't reach `Applied`**: the coordinator's own-shard apply is deliberately deferred until the remote quorum is secured, so an abandon is truthful and leaves nothing behind (`coordinator.rs:2755-2772`, `:2783-2799`); if the quorum IS met and then the local apply fails, the txn is committed+durably applied on a quorum and the error is non-retryable (`coordinator.rs:2807-2808`, `:2849-2853`).
- A replica that misses the no-write finalize keeps the failed txn as a pending conflict; later reads/barriers there dep-wait and abstain (`coordinator.rs:3348-3355`).

**Applier (`apply.rs` / storage).**
- Pre-commit failures (decode, no observer, mixed timestamps, observer prepare) return `Err` before the batch — nothing applied (`apply.rs:1263-1265`, `:1300-1318`).
- Batch failure: all-or-nothing, no key landed (`apply.rs:1321-1323`, `:1347-1350`).
- **Observer notify fails AFTER the batch is durable:** `on_postgres_apply(...)` error propagates via `?` BEFORE the idempotency triples are inserted; rows ARE durable, but no `(txn,key,t)` record is left, so a retried Apply re-executes the observer/row (`apply.rs:1363-1377`; ordering comment `:1371-1372`).
- **Phase-2 memtable write fails after the Phase-1 fsync:** `state.store.write(...)?` aborts Phase 2 partway; the commit log is fully durable (replays on restart) but the memtable may be partial; `write_atomic_batch` returns `Err` ⇒ txn stays `Committed` (`engine.rs:9662-9669`; refusal path `store.rs:3702-3708`).
- Parked/cascade failures are fail-loud with no half-state: staging failure clears waits and Err (`apply.rs:1699-1705`, `:1755-1758`); cascade read-back/applier failure leaves the waiter un-applied and does NOT cascade past it (`apply.rs:1826-1843`).

**State machine (`state_machine.rs`).**
- **Applied-marker fsync fails** (row already durable in commit log + memtable): `bookkeep_applied` returns `Err(())`; the txn stays `Committed` (not falsely `Applied`); its Apply is retried, idempotent (`state_machine.rs:1296-1309`).
- Lock-free apply failure: `storage_ok=false` ⇒ `finish_apply` returns `SmResponse::None` without advancing, releasing the reservation (`state_machine.rs:239-251`, `:1227-1238`).
- Commit-handler fsync fail ⇒ no advance, no dependents woken (`state_machine.rs:927-933`). Accept/PreAccept fsync fail ⇒ no reply/no advance; PreAccept additionally rolls back its conflict-index + `TxnState` registration (`state_machine.rs:861-872`, `:787-803`).

---

## Contradictions (comment vs code) — record BOTH

1. **Applier write path.** Doc says each row is written via `StorageEngine::write`: "Each row is written via [`StorageEngine::write`], which appends to the commit log and the memtable (persist-before-return)" (`apply.rs:880-882`). Code never calls a per-row `StorageEngine::write` on this path; it commits through the streaming `write_atomic_batch` (`apply.rs:1341-1342`, `engine.rs:9593`).
2. **Protocol-log format (writer vs reader).** `framed_log.rs` claims every `frame_record` is recovered by `read_framed_log` (`ferrosa-storage/src/accord/framed_log.rs:3-4`), and `crash_recovery.rs` replays serialized `AccordProtocolEntry` binary records (`ferrosa-storage/src/accord/crash_recovery.rs:99-102`). But production writes ASCII text via `format!` (`state_machine.rs:787`, `:927`). A text record fed to `AccordProtocolEntry::deserialize` fails length/CRC ⇒ skipped, never recovered.
3. **Protocol log is written but not replayed.** `read_framed_log` and `CrashRecoveryReplay` have NO non-test callers (matches only in `framed_log.rs` tests `:174,:191,…,:355` and `crash_recovery.rs` tests `:400,:465,:562,:649`), while the only production `FileSyncWriter::new` is `cluster.rs:1632`. So Accord protocol state is written durably but never read back at startup.
4. **`ProtocolLog` is in-memory but typed as durable.** `protocol_log.rs` documents PreAccepted/Accepted/Committed entries as durable local storage but its field is `entries: Vec<AccordProtocolEntry>` and the module doc says "In-memory `Vec` storage. Disk persistence with segment rotation is a later optimization" (`ferrosa-storage/src/accord/protocol_log.rs:7-10`, `:21-26`); `ProtocolLog::new` has no production caller (only a test at `:116`).
5. **Fan-out residency claim.** Comment: production replaced the old path with `apply_fanout_bounded` "(one replica's payload resident at a time)" (`coordinator.rs:1304-1305`), but the actual bound is `APPLY_FANOUT_WINDOW` (4) and sends OVERLAP (doc `coordinator.rs:2914-2915`; test asserts `peak > 1` `:4869-4873`).
6. **Frame-format claim.** Doc: "A single `bincode` frame is serialized" (`coordinator.rs:2933-2938`), but the code picks capnp region/inline when every dialed peer advertises the capability, with bincode only as the mixed-cluster fallback (`coordinator.rs:3070-3105`). `protocol.rs:321-322` and `envelope.capnp:359-361` likewise describe the payloads as "mirroring the bincode shapes" though the delivered frames are capnp (bincode is the un-upgraded-peer fallback).
7. **"Concurrent" own-apply label.** The tracing message says the coordinator's own apply runs "concurrent with the remote fan-out" (`coordinator.rs:2697-2702`), but the code awaits `remote_apply` first then polls `local_apply` (`coordinator.rs:2772`, `:2807`), and the comment says this sequencing is deliberate to keep abandon truthful (`coordinator.rs:2758-2765`).
8. **Session-buffer "cap" wording.** `copy_stdin.rs:393-395` calls the staging buffer a "cap", whereas `query.rs:1545-1553` and `write_set_stage.rs:102-103` state there is NO cap (the set spills).

---

## Defect flag against the capnp hot-path rule (per-message serialization)

The workspace rule is: per-row/per-message hot-path serialization MUST be capnp; bincode/JSON on the hot path is a defect. On this path:
- **capnp (compliant):** the multi-key Apply frame — region-reference or capnp-inline (`coordinator.rs:3070-3105`; `ferrosa-net/src/protocol.rs` `encode_accord_apply_v2` / `encode_accord_apply_v2_region`; capnp schema `ferrosa-net/schema/ferrosa/internode/envelope.capnp:376-387`; entry type `ferrosa-net/src/protocol.rs:291-298`). The write-set entry (`WriteSetEntry` `wire.rs:81-86`) is capnp-backed in the wire form.
- **bincode (defect by the rule):** PreAccept/PreAcceptOK, Accept/AcceptOK, Commit/CommitOK, ReadVote/ReadVoteOK, single-key Apply/ApplyOK, and the multi-key `AccordApplyV2` fallback are all `bincode::serialize`/`deserialize`. Call sites: coordinator send/decode `coordinator.rs:1585-1587`, `:1598-1600`, `:1810`, `:1976-1978`, `:2052-2059`, `:2137-2139`, `:2186`, `:2239-2241`, `:2369`, `:1343`, `:2737`, `:1419`, `:3305`; replica side `handlers.rs:432`, `:469`, `:513`, `:537`, `:586`, `:599`, `:705`; wire contract doc `wire.rs:1-2` ("serialized via bincode"). There is no `encode_accord_preaccept/accept/commit/readvote` — only `encode/decode_accord_envelope`, `encode/decode_accord_apply_v2`, and the region variants (`ferrosa-net/src/protocol.rs:2156,2165,2188,2228,2436,2454`).
- `bincode` has **0 matches** under `ferrosa-storage/src` (confirmed), so the storage/applier path itself never bincode/JSON-encodes; the front-end staging serialization is the hand-rolled `Mutation::serialize_into` layout (`ferrosa-storage/src/commitlog/mutation.rs:355-379`).

---

## Could not determine

- **Whether the bincode Commit/Accept/Apply frames are actually taken on a modern 3-node cluster**, vs falling to the capnp path: the encoder choice is runtime-gated on per-peer capability negotiation (`coordinator.rs:3017-3024`, `supports_accord_capnp` / `supports_accord_apply_region` at `coordinator.rs:3031`), which is not statically resolvable. Same for whether `AccordApplyV2Region` vs inline capnp is used.
- **Whether the protocol log is replayed at startup anywhere outside this workspace search.** I found no production caller of `read_framed_log` / `CrashRecoveryReplay::new` (only tests) and no `ProtocolLog::new` production caller — I report the absence, not a positive replay path. A replay could exist in a binary I did not trace.
- **Whether `DurabilityService`/`ExclusiveSyncPoint` is driven by the live apply path.** `ShardProgress` (`dur...durability.rs:37-41`), `ExclusiveSyncPoint` (`:52-57`), and `report_applied`/`report_shard_applied` (`:79-85`, `:210-212`) exist, but every `DurabilityService::new` / `report_shard_applied` call site found is a test (`durability.rs:346,350,374,…`); no production driver of the sync point was located. So I cannot assert the sync point equals the row-fsync instant.
- **Explicit byte ceiling on the coordinator's Apply frame.** No `MAX_FRAME`/budget constant exists in `apply_fanout_bounded_within`; the only per-entry cap is the `u32` length in the region encoder, and the frame is bounded only indirectly by the peer's owned shard + `APPLY_FANOUT_WINDOW` (`coordinator.rs:52`, `:3129-3155`).
- **`commit_postgres`'s `_tables` parameter** is accepted (`transaction_commit.rs:261`) but I did not exhaustively confirm its downstream use for conflict scoping past line 498.
- **The coordinator's direct use of `dep_wait.rs`:** `coordinator.rs` never references `DepWaitGraph`; it reaches dep-wait only via `crate::accord::handlers::{await_conflicting_deps_applied, await_txn_applied_within}` (`coordinator.rs:2274,2300,2722`). `DepWaitGraph` (`dep_wait.rs:86`) is used by `state_machine.rs`/`apply.rs`/`mod.rs`.
