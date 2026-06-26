# Raft Invariants Catalog

> Status: Draft
> Created: 2026-05-09
> Companion to: `raft-correctness-plan.md`, `raft-failure-mode-matrix.md`

## Purpose

Every invariant Ferrosa wants to enforce on its Raft layer, with explicit traceability to:

- The **bug** in the genome (`raft-correctness-plan.md` F1–F6) that motivates it.
- The **enforcement layer(s)** where it is checked: `tla+` (design), `sim` (deterministic simulation), `loom` (concurrency), `jepsen` (integration), `runtime` (production assertion / metric).
- The **failure** that would be observed if it were violated.

The same invariant is checked at multiple layers deliberately. TLA+ proves it of the design; simulation tests it against the implementation; Jepsen tests it end-to-end; runtime metrics catch what the others missed.

## Convention

Each invariant has the form:

> **I-NN: short name**
> Statement: a one-sentence claim that must hold.
> Origin: which bug / paper section / agent finding implies it.
> Layers: `[tla+, sim, loom, jepsen, runtime]`.
> Violation observable as: what an operator or test would see.
> Implementation note: what code or test enforces it.

Invariants are grouped:

- **§A — Single-cluster Raft safety**: classical Raft properties.
- **§B — Membership / topology consistency**: the four-maps-must-agree property and its derivatives. Most of the recent bug genome lives here.
- **§C — Apply path determinism**: state-machine apply must converge.
- **§D — Persistence and recovery**: durability across restart, OOM, log purge.
- **§E — Network and lane discipline**: lanes don't share fate; mutexes don't cross awaits.
- **§F — Multi-DC**: cross-DC handoff and Accord interaction.
- **§G — Operator and observability**: bounds on detection times for each defect class.

---

## §A — Single-cluster Raft safety

### I-01: Election Safety

Statement: At most one leader is elected for a given term.
Origin: Raft paper §5 (Election Safety property).
Layers: `[tla+, sim]`.
Violation observable as: two nodes simultaneously logging "I am leader" with the same term.
Implementation note: enforced by openraft's vote handler; we model-check it in the TLA+ spec at `specs/tla/raft.tla` (Sprint 5).

### I-02: Leader Append-Only

Statement: A leader never overwrites or deletes entries in its log; it only appends.
Origin: Raft paper §5 (Leader Append-Only property).
Layers: `[tla+, sim]`.
Violation observable as: a leader's `last_log_index` decreasing in metrics.
Implementation note: openraft enforces. Add a runtime assertion in `SledLogStore::append` that the new range begins exactly at `last_log_id + 1` (today: implicit, no assertion).

### I-03: Log Matching

Statement: If two logs contain an entry with the same index and term, the logs are identical in all entries up to and including that index.
Origin: Raft paper §5 (Log Matching property).
Layers: `[tla+, sim, runtime]`.
Violation observable as: divergent state between replicas after a successful AppendEntries.
Implementation note: openraft enforces via term+index consistency check. Runtime: `RAFT_LOG_DIVERGENCE_TOTAL` metric, incremented if `RaftAppendResponse` reports `conflict` after the leader has already committed the entry.

### I-04: Leader Completeness

Statement: If a log entry is committed in a given term, then that entry will be present in the logs of leaders for all higher terms.
Origin: Raft paper §5 (Leader Completeness property).
Layers: `[tla+, sim]`.
Violation observable as: a committed write becoming invisible after a leader change.
Implementation note: enforced by openraft's election restriction; covered by Jepsen register linearizability + bank conservation workloads. **PreVote does not affect this**; `loosen-follower-log-revert` could; see I-29.

### I-05: State Machine Safety

Statement: If a server has applied a log entry at a given index, no other server will ever apply a different log entry for the same index.
Origin: Raft paper §5 (State Machine Safety property).
Layers: `[tla+, sim, jepsen, runtime]`.
Violation observable as: divergent state at the same `last_applied`.
Implementation note: covered by I-03 + I-04. Runtime: every node exposes `state_hash(last_applied)`; orchestrator post-run diffs them.

---

## §B — Membership / topology consistency

This section is the bulk of the recent bug genome. F1 (two-maps drift) is the dominant defect class; six fixes in 12 months.

### I-06: Four-maps agree

Statement: For every node `N` known to the cluster, the four membership maps agree:
`state.members.contains(N.host_id) ⟺ openraft.Membership.voters().contains(N.node_id) ⟺ network_factory.node_map.contains(N.node_id) ⟺ peer_manager.peers.contains_key(N.host_id)`.

Origin: Agent A finding §8 (sync gaps); P0-21 saga; today's outage.
Layers: `[runtime, jepsen, sim]`.
Violation observable as: a node visible in `system.peers` but unreachable via AppendEntries; or a `Uuid::nil()` reply from `network_factory.new_client`.
Implementation note: Sprint 1 introduces `MembershipChanger` as the only API for membership change; all four maps update through it. Runtime metric `MEMBERSHIP_DRIFT_NODES` (gauge) reports the count of nodes where the four-way `⟺` fails. Jepsen `forward-probe` workload + `membership-snapshot` post-run diff. Sim test: every transition asserts I-06 holds post-step.

### I-07: No empty addresses

Statement: For every `(host_id, NodeInfo)` in `state.members`, `NodeInfo.addr != ""`. Equivalent: every voter in openraft has a routable address known to `peer_manager`.
Origin: `handle_join_request` placeholder addr (`controller/membership.rs:86`); Agent C finding §5.
Layers: `[runtime, jepsen]`.
Violation observable as: AppendEntries to that node fails with "lane is reconnecting" forever.
Implementation note: `handle_join_request` is removed in Sprint 1 (replaced by `MembershipChanger::add_voter` which takes the addr); `RaftOp::JoinNode` apply rejects empty addr with `tracing::error!` and skips the insert.

### I-08: openraft Membership ⊆ state.members

Statement: Every voter in openraft's Membership is also in ferrosa `state.members`.
Origin: Agent A finding §8A.
Layers: `[runtime, sim]`.
Violation observable as: openraft sends AppendEntries to a node ferrosa doesn't know how to address.
Implementation note: weaker form of I-06; useful as a faster/cheaper runtime check. `MembershipChanger` writes to openraft *after* `state.members`, so a partial failure leaves state.members ahead, never behind.

### I-09: state.members ⊆ openraft Membership ∪ {pending learners}

Statement: A node in `state.members` is either an openraft voter or has been added as a learner (transient state during voter promotion).
Origin: Agent A finding §8A; ADR-014.
Layers: `[runtime, sim]`.
Violation observable as: a node in `state.members` that the leader cannot replicate to.
Implementation note: `MembershipChanger::add_voter` enforces order: `add_learner` → wait-caught-up → `change_membership(AddVoterIds)` → `RaftOp::JoinNode`. The "transient learner" window is bounded by Sprint 3's Leadership Transfer + ADR-014.

### I-10: Decommission removes from all four maps

Statement: After `MembershipChanger::remove_voter(N)` returns Ok, `N.host_id` appears in none of `state.members`, `openraft.Membership`, `node_map`, `peer_manager.peers`.
Origin: Agent A finding §9 (LeaveNode does not call change_membership); phantom voter risk.
Layers: `[sim, jepsen, runtime]`.
Violation observable as: quorum size grows monotonically; Jepsen `decommission` workload sees a failed write at QUORUM after decommission.
Implementation note: Sprint 1 `MembershipChanger::remove_voter` issues `change_membership(RemoveVoters)` then `RaftOp::LeaveNode`. Order matters: remove from openraft first, otherwise the leave proposal can't be replicated.

### I-11: Approval is replicated, not just controller-local

Statement: For every host_id `H`, `controller.approved_nodes.contains(H) ⟺ raft_state.approved_nodes.contains(H)` on every node.
Origin: Agent A finding §11 (`RaftOp::ApproveNode` apparently never proposed).
Layers: `[sim, jepsen]`.
Violation observable as: `auto_join=false` admits a node on the leader but rejects on followers (or vice versa).
Implementation note: Sprint 1 fixes `approve_node` to propose `RaftOp::ApproveNode` via `MembershipChanger`. The local `controller.approved_nodes` becomes a cache populated by apply, not the source of truth.

### I-12: Token ring is Raft-replicated, not local

Statement: Every node's token ring is derived solely from `state.token_map` and `state.members`, not from any local view of peers.
Origin: bug commit `7944e6b9` (April 2026 — produced 67/67/33 data scatter when violated).
Layers: `[sim, jepsen, runtime]`.
Violation observable as: divergent reads from coordinators on the same partition.
Implementation note: enforced by `sync_ring()` already; runtime metric `RING_HASH` per node, alarm on divergence.

### I-13: Quorum sizing is committed, not connected

Statement: Quorum = `floor(committed_cluster_size / 2) + 1` where `committed_cluster_size` is openraft's voter count, not `peer_manager.live_peers().count()`.
Origin: bug commit `e800890e` (April 2026).
Layers: `[sim, runtime]`.
Violation observable as: apparent quorum loss with majority of voters healthy; or apparent quorum present with minority of voters healthy.
Implementation note: already fixed; add a `cargo clippy::deny(custom = "no-connected-peer-count-in-quorum")` lint or a grep CI gate. Runtime: `RAFT_COMMITTED_CLUSTER_SIZE` metric.

---

## §C — Apply path determinism

### I-14: Apply is deterministic given the log

Statement: Two replicas with the same log produce the same `state` after applying through the same index.
Origin: Raft paper §5; Agent A finding §2 (no system clocks, no fresh UUIDs in apply).
Layers: `[tla+, sim]`.
Violation observable as: divergent `state_hash` at same `last_applied`.
Implementation note: every non-deterministic input (`schema_version: Uuid`, IndexStatus timestamps) is leader-stamped and replicated through the log. The simulator's apply-step assertion verifies this by re-running apply with the same log on a fresh state machine and comparing.

### I-15: Apply propagates all errors

Statement: A `RaftOp::*` apply that fails on schema, storage, or system table writes returns `RaftResponse::Error(msg)` to the caller. `RaftResponse::Ok` means every consumer succeeded.
Origin: Agent A finding §2; Agent B failure class "non-leader-silent-drop" (9 incidents). Today, `apply_command` always returns `Ok` and `tracing::error!`s on failure — `RaftResponse::Error` is dead code.
Layers: `[runtime, sim]`.
Violation observable as: schema commit visible in `state.schema_version` but `engine.register_table` never ran; subsequent reads fail with "table not found."
Implementation note: rewrite `apply_command` to bubble all sub-errors. `client_write` callers see typed errors. **CI gate**: `grep -rn "let _ = " ferrosa-cluster/src/raft/state_machine.rs` returns zero matches.

### I-16: No mutex held across `.await`

Statement: No `tokio::sync::Mutex`, `parking_lot::Mutex`, or `std::sync::Mutex` is held across an `.await` point in any Raft RPC client, peer manager, or lane actor.
Origin: bug commit `9fa74ed4` (April 2026 — caused total heartbeat deadlock); Agent B failure class "election-storm".
Layers: `[loom, runtime, ci-lint]`.
Violation observable as: zero AppendEntries delivered; election storm.
Implementation note: clippy lint `await_holding_lock` is enabled and **denied** in CI. Loom test `lane_actor_no_deadlock_under_concurrent_send` asserts no deadlock with bounded message ordering.

---

## §D — Persistence and recovery

### I-17: `last_applied` survives OOM

Statement: After a process kill while the log is purged past in-memory state, on restart `last_applied >= last_purged_log_id`.
Origin: bug `bug-raft-startup-fails-after-oom-purged-log.md` (April 2026).
Layers: `[sim, jepsen]`.
Violation observable as: openraft errors with "expected index [0,N), got [Some(M),N)".
Implementation note: `recover_from_purge_point()` already implemented. Sim test: kill the process between log-purge and snapshot-write, assert clean restart.

### I-18: `last_membership` survives restart

Statement: After clean restart with a populated log + snapshot, `last_membership` is non-empty if any membership change has ever been committed.
Origin: bug `bug-raft-empty-membership-after-recovery.md`.
Layers: `[sim, jepsen]`.
Violation observable as: cluster comes up as 3 Learners, no leader, "raft leader election timed out."
Implementation note: `find_last_membership` + `recover_membership_from_topology_state` already implemented. Sim test: full restart of 3-node cluster, assert membership recovered.

### I-19: Recovered membership is the actual last committed

Statement: If the actual last committed Membership log entry was a joint config, recovery does not silently downgrade it to a voter-only config.
Origin: Agent A finding §11 question 7.
Layers: `[sim, runtime]`.
Violation observable as: a half-completed `change_membership` appears as completed after restart.
Implementation note: `recover_membership_from_topology_state` synthesizes `Membership::new(vec![voters], None)` (no joint state). **Fix**: synthesis fails loudly if the synthesized config doesn't match a committed log entry. Sprint 1 deliverable.

### I-20: `save_committed` flushes

Statement: A successful return from `SledLogStore::save_committed` implies the commit pointer is durable.
Origin: Agent A finding §3.
Layers: `[sim, runtime]`.
Violation observable as: committed entries reverted after crash.
Implementation note: today `save_committed` does not flush. Sprint 1 fixes.

### I-21: Legacy log decode is unambiguous

Statement: `SledLogStore::deserialize_entry` returns either `Ok(current_format)` or `Err(parse_error)`; the legacy fallback fires only on the specific bincode error class that indicates schema migration.
Origin: Agent A finding §3.
Layers: `[runtime, ci-test]`.
Violation observable as: a corrupt entry is silently re-interpreted as a legacy entry that happens to bincode-decode.
Implementation note: Sprint 1 adds an explicit log-entry version byte at offset 0; legacy entries have `version = 0`, current have `version = 1`. The fallback fires only on `version == 0`. Backward-compatible because legacy bincode encoding starts with the variant index byte, which is `< 32` for all legacy variants.

### I-22: Purge does not block heartbeats

Statement: Sled purge of up to `max_in_snapshot_log_to_keep` entries completes within `heartbeat_interval / 2`.
Origin: Agent A finding §3 + §11 question 9.
Layers: `[runtime, sim]`.
Violation observable as: AppendEntries timeouts during purge windows; election storm correlation with sled compactions.
Implementation note: today `purge` runs inline. Sprint 1 moves to `spawn_blocking`. Runtime metric `SLED_PURGE_DURATION_MS` histogram.

---

## §E — Network and lane discipline

### I-23: Raft lane independent of data lane

Statement: A saturated `Lane::Data` (e.g. bulk SSTable streaming) does not delay any `Lane::Raft` message by more than the network propagation delay + ε.
Origin: bug commit `70b3bef5` (April 2026 — bulk INSERTs caused election storm).
Layers: `[runtime, jepsen, sim]`.
Violation observable as: election storm during bulk write workload.
Implementation note: lane-specific connection in `PriorityPool`. Runtime metric `RAFT_LANE_DELAY_P99` < `heartbeat_interval / 2`.

### I-24: Reconnect IO does not steal Raft runtime

Statement: Reconnect attempts run on a thread or runtime distinct from the Raft engine.
Origin: bug commit `afc4d3db` (April 2026); Agent B failure class "election-storm".
Layers: `[runtime]`.
Violation observable as: reconnect storm correlates with election storm.
Implementation note: dedicated reconnect thread. Runtime metric: thread-CPU usage of reconnect thread.

### I-25: No unknown peer becomes Uuid::nil()

Statement: `FerrosRaftNetworkFactory::new_client` for an unknown `node_id` returns an explicit error, not `Uuid::nil()`.
Origin: Agent A finding §4 + §11.
Layers: `[runtime, sim]`.
Violation observable as: replication errors logged with "peer 00000000-0000-... unreachable" — no such peer.
Implementation note: Sprint 1 changes the API to `Result<FerrosRaftNetwork, RegistrationMissing>`. Caller (openraft `RaftNetworkFactory` trait) gets `RPCError::Unreachable` with a clear cause string.

### I-26: Wire-decoded counts are bounded

Statement: Every `Vec::with_capacity(n)` where `n` is decoded from a wire message has `n <= MAX_PEER_COUNT` (or analogous bound).
Origin: bug commit `4bd1f856` (April 2026 — proptest fed random bytes → 188 GB allocation OOM).
Layers: `[ci-lint, runtime]`.
Violation observable as: OOM under fuzzed wire input.
Implementation note: already fixed for ClusterInvite. CI gate: a `proptest` fuzz harness for every `Message::*` variant.

---

## §F — Multi-DC and Accord (Sprint 6+)

### I-27: Cross-DC writes apply in timestamp order

Statement: For any two Accord transactions T1, T2 with timestamps t1 < t2 that touch the same partition, T1's effects are applied before T2's on every replica.
Origin: ADR-015.
Layers: `[tla+, sim, jepsen]`.
Violation observable as: divergent reads after partition heal between DCs.
Implementation note: reorder buffer with watermark on `FerrosStateMachine`; Sprint 7.

### I-28: Idempotent apply by Accord txn ID

Statement: Applying the same Accord transaction twice produces the same state as applying it once.
Origin: ADR-015.
Layers: `[sim, jepsen]`.
Violation observable as: post-recovery balance violations in bank workload.
Implementation note: `state.applied_accord_txns: BTreeMap<TxnId, AppliedRecord>` dedupes. Sprint 7.

### I-29: `loosen-follower-log-revert` only fires on deliberate wipe

Statement: `RAFT_FOLLOWER_LOG_REVERTED_TOTAL` increments only after an operator action that wiped a node's raft data dir.
Origin: Agent D finding §11 + Agent A finding §1 (cargo features).
Layers: `[runtime, audit]`.
Violation observable as: silent data loss; followers truncating logs the leader still considers committed.
Implementation note: Sprint 1 audit. Runtime metric. If non-zero in production over 30 days without correlated wipe-and-rejoin, downgrade flag in production builds.

### I-30: Joint-consensus DC swap drains in-flight Accord

Statement: A `change_membership` that removes any voter `V` from a DC waits for every in-flight Accord transaction with `V ∈ participants` to complete or abort before the joint config commits.
Origin: ADR-015.
Layers: `[tla+, sim, jepsen]`.
Violation observable as: an Accord txn deadlocks waiting for a removed voter's vote.
Implementation note: `MembershipChanger::remove_voter` queries Accord coordinator pool for in-flight txns; blocks on drain. Sprint 7.

---

## §G — Operator and observability

### I-31: Election storm detected within 60 s at production cadence

Statement: Any election storm where `term` advances ≥ 2 within a 30 s window without `last_log_index` advance produces a `ELECTION_STORM_TERM_JUMPS_TOTAL` increment within 60 s.
Origin: bug commits `54d986a4` and `ac384afc` (April 2026); the first detector fired at test cadence but not production cadence.
Layers: `[runtime, jepsen]`.
Violation observable as: a node at term T18,000+ versus a leader at term T8 (the open in-process bug).
Implementation note: already implemented. Tested at production cadence (`election_timeout >= 3000ms`).

### I-32: Bootstrap signals are observable

Statement: Every `raft.initialize`, `raft.add_learner`, `raft.change_membership`, and `BootstrapComplete` call increments a counter or logs at `error!` level on failure; no `let _ = raft_tx.send(...)` swallows a signal.
Origin: bug commit `44a7e6bb` (P0-08 — surface swallowed Raft bootstrap signals).
Layers: `[runtime, ci-lint]`.
Violation observable as: silent formation failures.
Implementation note: already implemented. CI: `grep -rn "let _ =.*raft" ferrosa-cluster/src/controller/` returns only documented intentional ignores.

### I-33: `ferrosa-ctl raft reset` recovers a wedged node

Statement: An operator can run `ferrosa-ctl raft reset --node N` on any wedged node; on next process start, the node rejoins the cluster as a fresh Learner.
Origin: `bug-raft-stale-candidate-runaway-term-no-prevote.md` workaround section.
Layers: `[jepsen, integration-test]`.
Violation observable as: an operator must `rm -rf data/raft` manually, losing more state than necessary.
Implementation note: Sprint 1 lands `SledLogStore::reset(path) -> ResetCounts` from worktree `ferrosa-raft-fix` plus the `ferrosa-ctl` command.

---

## Top 10 invariants by leverage

If we could only enforce ten, these are the ten — chosen to dominate the bug genome:

1. **I-06 — Four-maps agree.** Six bugs, dominant defect class.
2. **I-15 — Apply propagates all errors.** Nine bugs in the "non-leader-silent-drop" class.
3. **I-16 — No mutex across `.await`.** Catastrophic when violated; cheap to enforce via clippy.
4. **I-23 — Raft lane independent of data lane.** Six election-storm bugs converged on this.
5. **I-12 — Token ring is Raft-replicated.** Single fix `7944e6b9` produced visible data scatter.
6. **I-29 — `loosen-follower-log-revert` only on deliberate wipe.** Latent silent-data-loss risk.
7. **I-31 — Election storm detected within 60 s at production cadence.** Two bugs to get this right.
8. **I-13 — Quorum sizing is committed, not connected.** False quorum recovery is ugly.
9. **I-21 — Legacy log decode is unambiguous.** One latent recovery hazard.
10. **I-33 — Operator escape hatch exists.** Without this, every other invariant violation needs a deletion.

## Sprint mapping

| Sprint | Invariants newly enforced | Invariants newly tested at this layer |
|---|---|---|
| 1 | I-06, I-07, I-08, I-09, I-10, I-11, I-13, I-15, I-19, I-20, I-25, I-29, I-32, I-33 | I-06, I-15 (runtime); I-29 (audit) |
| 2 | — | I-06, I-07, I-08, I-09, I-10, I-12, I-13, I-15 (jepsen) |
| 3 | I-04 (PreVote refines), I-31 (CheckQuorum substitute) | I-04, I-31 (jepsen) |
| 4 | — | I-15, I-32 (sim — bootstrap phases) |
| 5 | I-01, I-02, I-03, I-04, I-05, I-14 (TLA+) | most §A, §B, §C invariants in sim |
| 6 | — | I-06, I-12 in multi-DC topology (jepsen T3) |
| 7 | I-27, I-28, I-30 | I-27, I-28, I-30 (sim, jepsen) |
| 8 | — | endurance run validates everything for 24h |
