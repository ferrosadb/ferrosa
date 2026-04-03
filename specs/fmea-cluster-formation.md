# FMEA — Cluster Formation State Machine

> Date: 2026-04-01
> Scope: All state transitions T1–T9, T6a–T6c from specs/cluster-formation-state-machine.md
> Reference: ferrosa-cluster/src/controller.rs, mode.rs, ddl_path.rs, write_path.rs

## FMEA Table (sorted by RPN descending)

| ID | Transition | Failure Mode | Effect | Sev | Occ | Det | RPN |
|----|-----------|-------------|--------|-----|-----|-----|-----|
| F3 | T3 Forming→Cluster | DDL race during Forming window — DDL via Direct path never reaches followers | Schema divergence: leader has tables followers lack. Writes to those tables fail silently on followers. | 9 | 6 | 7 | **378** |
| F5 | T2/T3 | Missing reverse outbound pools for late peers — Raft init proceeds without waiting | Raft AppendEntries fails to unconnected peers. Quorum may not form. Schema replay silently drops. | 8 | 7 | 5 | **280** |
| F1 | T2 Pair→Forming | ClusterInvite not received — non-seed nodes fail to discover each other | Nodes stuck in Pair. Raft can't form quorum. Cluster is non-functional. **Partially mitigated:** ClusterInvite now sent on Data lane with 10-attempt retry (808b72b), and ClusterInvite handler triggers cluster transition on receiving nodes (ba7599a). Remaining risk: all 10 retries fail under sustained network partition. | 9 | 4 | 3 | **108** |
| F2 | T2 Pair→Forming | No Forming state — jumps Pair→Cluster. If pool not ready, Raft fails. | Raft init hangs. DDL on Direct forever. Cluster appears formed but inoperable. | 8 | 8 | 4 | **256** |
| F6 | T2/T3 | No Forming→Pair fallback — 3rd node disconnects, seed stuck in Cluster with no quorum | Permanent Raft quorum loss. Writes fail indefinitely. Requires operator restart. | 8 | 4 | 8 | **256** |
| F4 | T5a Decommission | No data streaming before removal — LeaveNode via Raft but data not transferred | Token ranges unavailable. Unflushed data permanently lost. | 10 | 5 | 5 | **250** |
| F13 | T6c Quorum Lost | Surviving node serves stale reads with no client-visible warning | Clients read stale data believing current. Application consistency violations. | 7 | 4 | 7 | **196** |
| F20 | T1/T2/T3 | PairSchemaSync arrives before handler registered (race with sleep-based timing) | Schema sync dropped. Secondary starts with empty schema. Writes fail. **Mitigated:** LazyRaft pattern (7b057b0) registers Raft handlers before async init, eliminating the handler registration race for Raft messages. PairSchemaSync handler registration timing is also improved. | 7 | 2 | 4 | **56** |
| F7 | T4 Add Member | Approval check outside Raft proposal — TOCTOU race | Unapproved node's JoinNode committed to Raft. Security boundary violated. | 7 | 3 | 9 | **189** |
| F10 | T3 Forming→Cluster | Node crash during Raft init — between mode swap and Raft::new | Ghost member in Raft membership. Other nodes can't reach it. Prevents quorum. | 9 | 3 | 7 | **189** |
| F9 | T1 Standalone→Pair | Reverse outbound pool fails (500ms delay, single attempt, no retry) | Primary can't send PairSchemaSync. Silent schema divergence. | 7 | 4 | 6 | **168** |
| F19 | T7 Degraded→Cluster | Hint overflow during extended outage — no auto repair triggered | Recovered node missing mutations beyond hint capacity. Data silently incomplete. | 7 | 3 | 8 | **168** |
| F12 | T8b/T9 Primary Fails + Promote | Old primary returns with unreplicated writes — silently discarded | Data loss for partition-era mutations. No conflict detection or warning. | 9 | 2 | 9 | **162** |
| F15 | T4 Add Member | No ClusterInvite to new node — can't reach all cluster members | Node in Raft membership but unreachable by some peers. Raft messages fail. | 8 | 5 | 4 | **160** |
| F11 | T1 Standalone→Pair | Concurrent inbound connections both trigger transition_to_pair | Race: duplicate PairCoordinator, corrupted pair state, duplicate handlers. | 8 | 3 | 6 | **144** |
| F14 | T3 Forming→Cluster | Schema replay errors treated as warnings — followers permanently miss DDL | Tables on leader but not followers. Writes to those tables produce silent errors. | 9 | 2 | 8 | **144** |
| F21 | T3 Forming→Cluster | connected_peers mutex contention — peer added during transition | Raft initialized with wrong membership. | 7 | 3 | 6 | **126** |
| F8 | T3 Forming→Cluster | Raft leader election timeout (30s) — DDL on Direct | 30s of unreplicated DDL. If leader never elected, Direct path persists. | 8 | 3 | 5 | **120** |
| F18 | T5b Decommission Leader | transfer_leader not implemented — leader removes itself from Raft | Remaining nodes lose coordinator. Possible membership corruption. | 9 | 2 | 5 | **90** |
| F16 | T1 Standalone→Pair | Partition immediately after pair formation — writes hang on replication | Write timeout or hang. User-visible latency spike. | 7 | 3 | 4 | **84** |
| F24 | T6b Leader Fails | Clock skew → premature Raft elections | Unnecessary elections. Brief write unavailability. Under heavy skew, livelock. | 5 | 3 | 5 | **75** |
| F22 | T4 Add Member | FD exhaustion — each peer needs multiple TCP connections | PriorityPool::connect fails. New node can't join. Existing connections disrupted. | 7 | 3 | 3 | **63** |
| F17 | T3 Forming→Cluster | Sled log store creation fails (disk full, permissions) | Mode is Cluster but Raft never initializes. Zombie state. | 9 | 2 | 3 | **54** |
| F25 | T2/T3 | Duplicate ClusterInvite propagation — no dedup | Exponential message amplification. Network saturation. | 5 | 3 | 3 | **45** |
| F23 | T8a Secondary Fails | Hint store init failure — expect() panics | Total node crash instead of graceful degradation. | 10 | 1 | 2 | **20** |

## Critical Findings (RPN ≥ 200)

### CRITICAL-1: DDL Race During Formation Window (F3, RPN=378)

`controller.rs:919` sets DDL to `DdlPath::Direct` during Raft init. DDL in this 30s+ window is local-only. Schema replay (L1071-1093) errors are logged as warnings and swallowed.

**Fix:** Block DDL during Forming state OR queue and replay after leader election.

### CRITICAL-2: Missing Reverse Outbound Pools (F5, RPN=280)

Reverse pool creation at L760-778 is fire-and-forget. Raft init at L950 proceeds without waiting. AppendEntries to unconnected peers fails silently.

**Fix:** `PeerManager::wait_for_peer(host_id, timeout)` before Raft init.

### CRITICAL-3: ClusterInvite Delivery (F1, RPN=270 -> 108)

**Partially mitigated.** ClusterInvite is now implemented and sent on the Data lane
with a 10-attempt retry loop (808b72b). The ClusterInvite handler triggers cluster
transition on receiving nodes (ba7599a). Remaining risk: all 10 retries fail under
sustained network partition.

**Remaining fix:** Persistent retry or operator-triggered re-invite for partition scenarios.

### CRITICAL-4: No Forming State + No Fallback (F2+F6, RPN=256)

Only Standalone|Pair|Cluster. No Forming state. Once in Cluster, can't go back. 3rd node disconnect = permanent stuck state.

**Fix:** Add Forming variant. 60s timeout → fall back to Pair.

### CRITICAL-5: No Decommission Data Streaming (F4, RPN=250)

`initiate_decommission` (L451-483) proposes LeaveNode but skips data transfer. Relies on S3 write-behind which may be stale.

**Fix:** Full sequence: LeaveNode → ReassignTokens → stream data → RemoveNode.

## Cross-Cutting Observations

1. **No persistent formation state** — all in-memory. Crash during transition = orphaned Raft members.
2. **Fire-and-forget spawns** — 7 critical `tokio::spawn` calls with no JoinHandle tracking.
3. **Single-attempt operations** — reverse pools, schema sync, Raft proposals attempted once. Transient failures → permanent inconsistency.
4. **Mode transition not atomic with side effects** — mode stored as Cluster before Raft init completes (L933 vs L950).
5. **Hardcoded timing** — 500ms, 2s, 4s magic numbers. Should be condition-based waits.

## Recommended Test Cases

| # | Test | Failure Mode | RPN | Infra |
|---|------|-------------|-----|-------|
| 1 | DDL during Forming replicates after Raft init | F3 | 378 | Firecracker |
| 2 | Raft init waits for all peer pools | F5 | 280 | Firecracker |
| 3 | Hub-and-spoke ClusterInvite propagation | F1 | 270 | Firecracker |
| 4 | Forming falls back to Pair on timeout | F6 | 256 | Firecracker |
| 5 | Forming state gates Raft initialization | F2 | 256 | Firecracker |
| 6 | Decommission streams data before removal | F4 | 250 | Firecracker |
| 7 | Quorum loss: stale reads flagged | F13 | 196 | Firecracker |
| 8 | Schema sync arrives after handler registered | F20 | 196 | Unit (mock) |
| 9 | Approval check inside Raft (TOCTOU) | F7 | 189 | Unit |
| 10 | Crash during Raft init recovers cleanly | F10 | 189 | Firecracker |
| 11 | Reverse pool retries on failure | F9 | 168 | Unit (mock) |
| 12 | Hint overflow triggers repair flag | F19 | 168 | Firecracker |
| 13 | Promoted secondary wins on old primary return | F12 | 162 | Firecracker |
| 14 | New member receives ClusterInvite with full peer list | F15 | 160 | Firecracker |
| 15 | Concurrent peer connections don't corrupt state | F11 | 144 | Unit |
