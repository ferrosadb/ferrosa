# IMPLEMENTED · P1 · Multi-key Accord apply re-keying by `(txn_id, key)`

**Updated:** 2026-06-23 (steps 1–4 implemented in-process; awaiting CI cross-shard e2e verification)
**Branch:** `feat/accord-multikey-execution` (off main; foundations already committed)
**Phase:** Accord multi-key transactions — Phase 2-execution + Phase 3 (fused)
**Risk:** DATA-LOSS-CRITICAL. The failure mode is *silently dropped writes* in the
consensus apply engine — no failing build, no error. TDD every step; do not land
without the verification below.

## Why

A multi-key Accord transaction has one `txn_id` but **N writes** (one per
partition key). The apply engine is keyed by `txn_id` (or `(txn_id, t)`) only, so
writes 2..N of a transaction collide/dedup and vanish. The per-shard quorum core
is already done + proven (PR #182); this is the remaining execution path.

## Context already in place (do not redo)

- `accord/shard_quorum.rs`: `ShardQuorum` (per-shard quorum), `ParticipantSet`
  (+ `from_per_key`) — tested.
- `accord/transport.rs`: `AccordTransport` seam + `new_multi_with_transport`.
- `coordinator.rs`: `quorum_broadcast` (per-shard commit/apply) + `single_shard_participant`.
- `TokenRing::replica_host_ids_for_strategy`, `WritePath::replicas_for_key`.
- Apply already routes through `DepWaitApplier`/`EngineStorageApplier`; the
  coordinator's OWN-replica apply iterates the write-set (`apply_v2_payload`).

## Steps (TDD, in order)

### 1. `DepWaitApplier` parks ALL of a txn's writes — `accord/apply.rs`
- `pending: Mutex<HashMap<TxnId, ApplyMutation>>` (~:460) →
  `HashMap<TxnId, Vec<ApplyMutation>>`.
- Add `try_apply_writeset(txn_id, Vec<ApplyMutation>)`; keep `try_apply(txn_id,
  mutation)` delegating with `vec![mutation]` so the single-key path is
  byte-identical.
- Park the whole `Vec` (~:519). In `cascade` (~:557) `pending.remove(&waiter)`
  yields a `Vec` → apply **each** mutation.
- **Trap:** the dep-wait *graph* stays `TxnId`-keyed (deps are txn-level). Do NOT
  re-key the graph.

### 2. `EngineStorageApplier` idempotency includes the key — `accord/apply.rs`
- `applied: Mutex<HashSet<(TxnId, u64)>>` (~:364) →
  `HashSet<(TxnId, Vec<u8>, u64)>` = (txn, partition-key, `t.time`).
- Move the idempotency check (~:388) to **after** `Mutation::deserialize_from`
  (~:393) so the partition key is available (each `ApplyMutation.data` decodes to
  exactly one partition).
- **Trap:** without the key in the tuple, writes 2..N (same `txn_id`+`t`) are
  deduped → **lost**.

### 3. Cascade-return / `Applied`-marking semantics — `apply.rs` + `state_machine.rs`
- `try_apply`/`cascade` return `(TxnId, data)` **per applied write** → a multi-key
  txn yields N entries with the same `TxnId`.
- The consumer in `state_machine.rs::handle_apply` (~:615, the
  `for (woken_id, woken_data) … bookkeep_applied` loop) must advance the txn to
  `Applied` / fsync its protocol-log marker **once per txn**, not N times.
  Either **dedup the return by `TxnId`** before bookkeeping, or prove
  `bookkeep_applied` idempotent and document it.
- **Trap:** N markers / double-cascade corrupts the protocol log.

### 4. Wire it through (only after 1–3 are green)
- `accord/handlers.rs`: add a `Message::AccordApplyV2(b)` arm (model on the v1
  `AccordApply` arm ~:183) — decode `ApplyV2Payload`, apply each write the replica
  was sent, return `AccordApplyOK`.
- `coordinator.rs::run_transaction`: build the multi-shard participant via
  `ParticipantSet::from_per_key` (per-key replicas); per-shard fan-out so each
  replica gets only its owned keys (`AccordApplyV2`); drop the
  `MultiKeyNotYetExecutable` guard (~:864). Conflict ordering may stay
  representative-key (document the limitation) — full per-key `PreAcceptV2` deps
  is a follow-up.

## Verification

- **In-process (this branch):**
  - `NoopStorageApplier`: a 2-key txn parked behind a dep → on resolve **both**
    writes apply (today only 1 — the RED). Single-key tests unchanged.
  - `EngineStorageApplier` engine test: one txn, two keys, same `t` → **both**
    persist (today the 2nd dedups).
  - `cargo test -p ferrosa-cluster`, `clippy --all-targets`, `fmt` — all green.
- **Live cross-shard e2e — DEFERRED to CI** (`t_afa3ee86`): cross-shard bank
  transfer on the 5-node `quint` profile (RF=3 < 5 → real multiple shards) —
  both legs or neither; a minority failure in one shard aborts. Local podman
  cluster is blocked (`t_88f278c3`); CI's `FERROSA_TEST_CONTAINERS` path works.

## Definition of done

Steps 1–4 landed, in-process tests green, **and** the CI cross-shard e2e
(`t_afa3ee86`) green. That last gate is what makes multi-key Accord genuinely
done — not the in-process tests alone.

## Implementation notes (2026-06-23)

Implemented via TDD (RED→GREEN per step) with added BDD scenarios + property tests.

**Deliberate strengthening of Step 2 — atomic write-set apply.** The spec's literal
Step 1/2 describes a per-key apply *loop* with per-`(txn,key,t)` idempotency. As
written, if key A persists and key B then fails, the loop returns A's entry →
`handle_apply` marks the txn `Applied` → **B is silently lost** with no retry signal.
Per the requester's explicit choice ("all-or-nothing batch"), `apply_writeset` instead
decodes + preflights every partition and commits them in ONE atomic `apply_batch`
(`write_atomic_batch` preflights all target tables before appending any commit-log
record), so a failure on any key persists none. Idempotency still keyed by
`(txn_id, partition_key, t)`; already-applied keys are filtered before the batch.

**Fan-out shape — per-replica filtered payload.** The coordinator builds a distinct
`AccordApplyV2` per replica scoped to only that replica's owned keys
(`apply_v2_messages` + `quorum_broadcast_per_peer`), and the replica applies what it
was sent — matching the existing "coordinator scopes, replica trusts" invariant
(the v1 `AccordApply` handler does no ownership filtering). This avoids plumbing a
ring/strategy into `AccordHandler`. Per-key replica resolution is an injected seam
(`with_per_key_replicas`); `None` = single shard (every replica owns every key). The
driver is not yet constructed in the live CQL path, so production wiring of the ring
resolver into the driver is the broader Phase-2 follow-up the CI e2e covers.

**Conflict ordering — per-key dependency union (RESOLVED, t_276e12).** PreAccept now
fans `AccordPreAcceptV2` carrying every key; each replica registers the txn under all
its keys and returns the UNION of dependencies across them (`handle_preaccept_multi`),
bumping `t` past the max conflict on any key. This serializes transactions that overlap
on a non-representative key. Single-key LWT keeps the v1 `AccordPreAccept` wire path
byte-identical. The representative first key is retained only for the single-key
ReadVote (LWT IF-read).

**Removed:** `AccordDriverError::MultiKeyNotYetExecutable` (the fail-loud guard) and
its references in `accord_nemesis.rs`.

**Public API added:** `StorageApplier::apply_writeset`,
`DepWaitApplier::try_apply_writeset`, `AccordStateMachine::handle_apply_writeset`,
`AccordCoordinatorDriver::with_per_key_replicas`, the `Message::AccordApplyV2`
inbound handler (registered in `controller/cluster.rs`). `ApplyMutation` now derives
`Clone`.

**Tests:** unit (apply/state_machine/handlers/coordinator), 2 BDD scenarios, 3
property tests (`prop_multikey_dag_applies_every_write_exactly_once_in_dep_order`,
`prop_idempotent_replay_converges_to_highest_agreed_t`,
`prop_writeset_apply_is_atomic_all_or_nothing`). `cargo test -p ferrosa-cluster`,
`clippy --all-targets`, `fmt` green. Cross-shard e2e (`t_afa3ee86`) remains the
final CI gate.

**Follow-ups (deferred):** (1) wire `AccordCoordinatorDriver` + ring resolver into
the live CQL execution path; (2) full per-key `PreAcceptV2` dependency union for
multi-shard conflict ordering.
