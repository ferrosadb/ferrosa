# Cluster Recovery + Storage OOM Seam Refactor Plan

> Use strict TDD. Work card-by-card, commit after each verified GREEN/refactor slice, and keep behavior changes behind focused regression tests.

**Goal:** Make the fmem/Ferrosa LOCAL_QUORUM read-timeout/OOM recovery path debuggable and correct by extracting pure decision seams from cluster recovery, bootstrap, and storage replay/compaction hot paths.

**Architecture:** Do not add another direct fix into `transition_to_cluster` or peer callbacks. First extract pure planners/phase seams with RED tests, then move side-effect execution behind small typed actions. Storage work should test forbidden intermediate behavior such as unbounded row materialization, startup upload replay blocking, compaction fan-in, and descriptor/resource pressure.

**Tech Stack:** Rust workspace (`ferrosa-cluster`, `ferrosa-storage`) and Cargo tests.

**Scratch saved:** `specs/in-process/scratch-peer-recovery-invite-reservation.patch` contains the aborted invite-reservation attempt. It is not applied to source.

**Current status:** Slices 1–6 are implemented and committed. The bootstrap work established the named phase seam (`DeliverInvites`, `EstablishPools`, `CreateRaft`, `WaitLeader`, `ReplaySchema`, `BootstrapStream`, `Promote`, `DrainQueue`) and bounded `BootstrapStream` row materialization. Slices 7–9 now continue from that seam into storage startup replay, compaction finalization, and a non-destructive fmem smoke.

---

## Current Code-Smell Evidence

- `ferrosa-cluster/src/controller/cluster.rs`
  - 2,292 lines.
  - `transition_to_cluster` starts at line 440 and is ~1,666 lines.
  - Smell scan: `LongFunction`, `HighComplexity CC=133`, nesting depth 13.
- `ferrosa-cluster/src/controller/peer_events.rs`
  - `on_peer_connected`: 99 lines.
  - `on_inbound_peer`: 69 lines.
  - Mixes peer tracking, mode transition, join triggering, ClusterInvite policy, and hint delivery.
- `ferrosa-storage/src/engine.rs`
  - `replay_pending_uploads`: 90 lines, deep nesting.
  - `poll_compactions`: 322 lines, CC=34.
  - `sync_sstables_to_s3`: 125 lines, deep nesting.
- `specs/dsm-cluster-formation.md` now records the current `controller/cluster` line count and the named bootstrap phase modules; use it to track the remaining high-LOC orchestration shell.

## Acceptance Criteria

1. The scratch invite-reservation patch is saved and source restored before refactor starts.
2. Every production behavior change has a RED test observed failing first.
3. Pure decision/planner seams exist for peer recovery/invite decisions before changing recovery behavior.
4. `transition_to_cluster` is split into named, testable phase helpers; phase failures report the phase name.
5. Bootstrap streaming has explicit tests for bounded materialization and no unbounded fallback after SSTable-stream failure.
6. Storage pending-upload replay has explicit queue/backpressure tests and does not block startup on full upload queues.
7. Compaction finalization has focused tests around upload confirmation, pending-log retention/removal, manifest update, and best-effort deletion enqueue.
8. After each card: run focused tests, `cargo fmt --all -- --check`, and commit only the card’s files.
9. Final smoke is non-destructive: no cluster data wipe; verify fmem node/entity counts and save/retrieve after rebuild/roll.

---

## Card 1: Preserve scratch + commit this plan

**Objective:** Preserve abandoned patch work and create the durable plan/board inventory.

**Files:**
- Create: `specs/in-process/scratch-peer-recovery-invite-reservation.patch`
- Create: `specs/in-process/refactor-cluster-recovery-storage-oom-seams.md`

**Verification:**

```bash
git diff -- ferrosa-cluster/src/controller/peer_events.rs ferrosa-cluster/src/controller/tests.rs > specs/in-process/scratch-peer-recovery-invite-reservation.patch
git restore -- ferrosa-cluster/src/controller/peer_events.rs ferrosa-cluster/src/controller/tests.rs
git status --short
```

Expected: no dirty `peer_events.rs` or `tests.rs`; scratch patch is present.

**Commit:**

```bash
git add specs/in-process/scratch-peer-recovery-invite-reservation.patch specs/in-process/refactor-cluster-recovery-storage-oom-seams.md
git commit -m "docs: plan cluster recovery and storage OOM seam refactor"
```

---

## Card 2: Finish existing Raft reset lock-release fix

**Objective:** Classify and verify the pre-existing `log_store.rs` fix before mixing it with new refactor work.

**Files:**
- Modify: `ferrosa-cluster/src/raft/log_store.rs`

**Hypothesis:** `SledLogStore::reset` must drop sled tree/db handles before returning so immediate reopen validation cannot race with sled directory lock release.

**RED status:** The source edit already exists from prior work, so do not claim a fresh RED. Instead, verify existing regression coverage and commit only if focused tests pass.

**Verification:**

```bash
cargo test -p ferrosa-cluster raft::log_store::tests::reset_clears_log_and_meta_trees -- --nocapture
cargo test -p ferrosa-cluster raft::log_store::tests::reset_on_empty_store_returns_zero_counts -- --nocapture
cargo test -p ferrosa-cluster raft::log_store::tests::reset_refuses_live_store_lock -- --nocapture
cargo fmt --all -- --check
```

**Commit:**

```bash
git add ferrosa-cluster/src/raft/log_store.rs
git commit -m "fix: release sled handles before raft reset returns"
```

---

## Card 3: Extract peer-event recovery decision planner

**Objective:** Make peer connect/inbound/recovered behavior testable without side effects.

**Files:**
- Create: `ferrosa-cluster/src/controller/peer_plan.rs`
- Modify: `ferrosa-cluster/src/controller/mod.rs`
- Modify: `ferrosa-cluster/src/controller/peer_events.rs`
- Test: `ferrosa-cluster/src/controller/tests.rs` or module tests in `peer_plan.rs`

**RED tests:**

1. `cluster_connect_existing_member_plans_join_and_invite_when_not_recently_invited`
2. `cluster_connect_existing_member_suppresses_duplicate_invite_with_recent_reservation`
3. `degraded_cluster_peer_connect_plans_mode_restore_only_when_committed_quorum_reached`
4. `peer_recovered_in_cluster_plans_invite_and_hint_delivery_independently`

**Design:**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PeerEventAction {
    TrackPeer { host_id: Uuid, addr: SocketAddr },
    TransitionToPair { host_id: Uuid, addr: SocketAddr, inbound: bool },
    TransitionToForming,
    TriggerClusterJoin { host_id: Uuid, addr: SocketAddr, cql_broadcast: Option<String> },
    SendClusterInvite { host_id: Uuid, force: bool },
    RestoreClusterMode,
    DeliverHints { host_id: Uuid },
}
```

Keep this pure. Execution remains in `peer_events.rs`.

**Verification:**

```bash
cargo test -p ferrosa-cluster peer_plan -- --nocapture
cargo test -p ferrosa-cluster cluster_reconnect -- --nocapture
cargo fmt --all -- --check
```

**Commit:**

```bash
git add ferrosa-cluster/src/controller/{mod.rs,peer_events.rs,peer_plan.rs,tests.rs}
git commit -m "refactor: extract peer recovery event planner"
```

---

## Card 4: Extract ClusterInvite planning and reservation seam

**Objective:** Centralize reconnect invite payload building, cooldown/reservation, and retry policy.

**Files:**
- Create: `ferrosa-cluster/src/controller/invite.rs`
- Modify: `ferrosa-cluster/src/controller/cluster.rs`
- Modify: `ferrosa-cluster/src/controller/peer_events.rs`
- Test: `ferrosa-cluster/src/controller/tests.rs` or `invite.rs` module tests

**RED tests:**

1. `reconnect_invite_plan_includes_self_and_excludes_recipient`
2. `reconnect_invite_plan_returns_none_when_no_peer_addresses_available`
3. `invite_reservation_is_atomic_for_burst_callbacks`
4. `invite_cooldown_allows_retry_after_interval`

**Verification:**

```bash
cargo test -p ferrosa-cluster reconnect_invite -- --nocapture
cargo test -p ferrosa-cluster cluster_invite -- --nocapture
cargo fmt --all -- --check
```

**Commit:**

```bash
git add ferrosa-cluster/src/controller/{cluster.rs,invite.rs,peer_events.rs,tests.rs}
git commit -m "refactor: extract cluster invite planning"
```

---

## Slice 5: Split `transition_to_cluster` into phase runner skeleton

**Status:** Implemented and committed. The bootstrap phase runner exposes the canonical phase order and phase-name errors, giving the storage follow-through an explicit boundary after `bootstrap_stream`.

**Objective:** Introduce a typed `ClusterBootstrapPlan`/`ClusterBootstrapContext` and move phase boundaries out of the god-function without changing behavior.

**Files:**
- Create/modify: `ferrosa-cluster/src/controller/bootstrap/mod.rs`
- Create: `ferrosa-cluster/src/controller/bootstrap/runner.rs`
- Modify: `ferrosa-cluster/src/controller/cluster.rs`

**RED tests:**

1. `bootstrap_runner_reports_phase_name_on_precondition_failure`
2. `bootstrap_runner_preserves_phase_order`
3. `bootstrap_runner_stops_on_required_phase_failure`

**Verification:**

```bash
cargo test -p ferrosa-cluster bootstrap::runner -- --nocapture
cargo test -p ferrosa-cluster raft_initializes_on_third_peer -- --nocapture
cargo fmt --all -- --check
```

**Commit:**

```bash
git add ferrosa-cluster/src/controller/bootstrap ferrosa-cluster/src/controller/cluster.rs
git commit -m "refactor: add cluster bootstrap phase runner"
```

---

## Slice 6: Extract bounded bootstrap streaming planner

**Status:** Implemented and committed. Bootstrap streaming now prefers SSTable-backed streaming, rejects unbounded row fallback after SSTable-stream failure, and bounds small-table row fallback.

**Objective:** Make row materialization budget and SSTable-first behavior explicit and tested.

**Files:**
- Modify: `ferrosa-cluster/src/controller/bootstrap/bootstrap_stream.rs`
- Modify: `ferrosa-cluster/src/controller/cluster.rs`
- Test: `ferrosa-cluster/src/controller/tests.rs` or module tests

**RED tests:**

1. `sstable_backed_table_uses_sstable_stream_before_row_materialization`
2. `failed_sstable_stream_does_not_fall_back_to_unbounded_rows`
3. `small_table_row_fallback_is_partition_bounded`
4. `stream_failure_reports_retry_required`

**Verification:**

```bash
cargo test -p ferrosa-cluster bootstrap_stream -- --nocapture
cargo fmt --all -- --check
```

**Commit:**

```bash
git add ferrosa-cluster/src/controller/bootstrap/bootstrap_stream.rs ferrosa-cluster/src/controller/cluster.rs ferrosa-cluster/src/controller/tests.rs
git commit -m "refactor: bound bootstrap streaming materialization"
```

---

## Card 7: Extract pending upload replay backpressure seam

**Continuation from bootstrap:** After `bootstrap_stream` and promotion complete, startup recovery must still replay durable upload work without materializing or queueing more work than the upload lane can accept. This card moves the next OOM seam from cluster bootstrap into storage pending-upload replay.

**Objective:** Ensure startup upload replay never blocks startup or drops durable pending work when queue capacity is exhausted.

**Files:**
- Create: `ferrosa-storage/src/upload/replay.rs`
- Modify: `ferrosa-storage/src/upload/mod.rs`
- Modify: `ferrosa-storage/src/engine.rs`

**RED tests:**

1. `pending_upload_replay_stops_at_queue_capacity`
2. `pending_upload_replay_leaves_log_entries_when_queue_full`
3. `pending_upload_replay_reports_missing_sstable_files`
4. `pending_upload_replay_submits_compaction_output_when_flush_dir_missing`

**Verification:**

```bash
cargo test -p ferrosa-storage pending_upload_replay -- --nocapture
cargo fmt --all -- --check
```

**Commit:**

```bash
git add ferrosa-storage/src/upload/{mod.rs,replay.rs} ferrosa-storage/src/engine.rs
git commit -m "refactor: isolate pending upload replay backpressure"
```

---

## Card 8: Extract compaction finalization seam

**Continuation from replay:** Once pending uploads can be replayed with bounded queue pressure, compaction completion needs the same explicit seam for upload confirmation, pending-log retention/removal, manifest update, and best-effort deletion enqueue.

**Objective:** Split `poll_compactions` around output swap, upload confirmation, pending-log handling, manifest update, and deletion enqueue.

**Files:**
- Create: `ferrosa-storage/src/compaction/finalize.rs`
- Modify: `ferrosa-storage/src/compaction/mod.rs`
- Modify: `ferrosa-storage/src/engine.rs`

**RED tests:**

1. `compaction_finalize_keeps_pending_log_when_upload_fails`
2. `compaction_finalize_removes_pending_log_after_upload_confirmed`
3. `compaction_finalize_applies_manifest_removals_with_output_add`
4. `compaction_finalize_does_not_block_on_input_deletion_completion`

**Verification:**

```bash
cargo test -p ferrosa-storage compaction_finalize -- --nocapture
cargo test -p ferrosa-storage --lib compaction -- --nocapture
cargo fmt --all -- --check
```

**Commit:**

```bash
git add ferrosa-storage/src/compaction ferrosa-storage/src/engine.rs
git commit -m "refactor: isolate compaction finalization"
```

---

## Card 9: Non-destructive local roll + fmem smoke

**Continuation from storage seams:** After pending-upload replay and compaction finalization are isolated, run the original fmem read-timeout/OOM smoke without deleting volumes or data. This proves the bootstrap and storage seams compose under a rebuild/roll rather than only in focused unit tests.

**Objective:** Verify the refactor/fixes against the original symptom without wiping Ferrosa/fmem data.

**Rules:**
- Do not delete volumes or data directories.
- Do not run `down -v`, `rm -rf`, `git clean`, or any destructive cleanup.
- Rebuild/roll only the necessary images/services.

**Smoke evidence:**

1. Ferrosa nodes stay up; no OOMKilled/die loop.
2. fmem CQL `LOCAL_QUORUM` read succeeds.
3. fmem node/entity counts are stable/nonzero as expected.
4. fmem save/retrieve works through the normal API/MCP path.
5. Capture resource counters: restart count, RSS, fd count, pending upload count, compaction backlog.

**Commit:** only if smoke requires a code/docs adjustment. Otherwise report evidence in PR/comment.
