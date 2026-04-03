# FMEA — Cluster Formation State Machine

> Date: 2026-04-02
> Scope: All state transitions T1–T9, T6a–T6c from specs/cluster-formation-state-machine.md
> Reference: ferrosa-cluster/src/controller.rs, mode.rs, ddl_path.rs, write_path.rs

## FMEA Table (sorted by RPN descending)

| ID | Transition | Failure Mode | Effect | Sev | Occ | Det | RPN |
|----|-----------|-------------|--------|-----|-----|-----|-----|
| F3 | T3 Forming→Cluster | DDL race during Forming window — DDL via Direct path never reaches followers | Schema divergence: leader has tables followers lack. Writes to those tables fail silently on followers. | 9 | 6 | 7 | **378** |
| F5 | T2/T3 | Missing reverse outbound pools for late peers — Raft init proceeds without waiting | Raft AppendEntries fails to unconnected peers. Quorum may not form. Schema replay silently drops. **Partially mitigated:** CQL broadcast propagation through handshake (ce72f32, 1456778) improves peer address discovery. Hostname resolution (8bd3149) reduces address mismatch. Core fire-and-forget pool creation still unresolved. | 8 | 6 | 4 | **192** |
| F7 | T4 Add Member | Approval check outside Raft proposal — TOCTOU race | Unapproved node's JoinNode committed to Raft. Security boundary violated. | 7 | 3 | 9 | **189** |
| F10 | T3 Forming→Cluster | Node crash during Raft init — between mode swap and Raft::new | Ghost member in Raft membership. Other nodes can't reach it. Prevents quorum. | 9 | 3 | 7 | **189** |
| F19 | T7 Degraded→Cluster | Hint overflow during extended outage — no auto repair triggered | Recovered node missing mutations beyond hint capacity. Data silently incomplete. | 7 | 3 | 8 | **168** |
| F12 | T8b/T9 Primary Fails + Promote | Old primary returns with unreplicated writes — silently discarded | Data loss for partition-era mutations. No conflict detection or warning. | 9 | 2 | 9 | **162** |
| F4 | T5a Decommission | No data streaming before removal — LeaveNode via Raft but data not transferred | Token ranges unavailable. Unflushed data permanently lost. **Partially mitigated:** Bootstrap streaming now implemented for all nodes (ae5ba57) — formation-time streaming works. Decommission-time streaming still missing. | 10 | 4 | 4 | **160** |
| F11 | T1 Standalone→Pair | Concurrent inbound connections both trigger transition_to_pair | Race: duplicate PairCoordinator, corrupted pair state, duplicate handlers. | 8 | 3 | 6 | **144** |
| F14 | T3 Forming→Cluster | Schema replay errors treated as warnings — followers permanently miss DDL | Tables on leader but not followers. Writes to those tables produce silent errors. | 9 | 2 | 8 | **144** |
| F26 | T2/T3 | Formation timeout path — DDL path now restored to Direct after formation timeout | Fallback implemented: DDL path restored to Direct on timeout. Remaining risk: full Forming→Pair transition integration testing. | 5 | 3 | 4 | **60** |
| F13 | T6c Quorum Lost | Surviving node serves stale reads with no client-visible warning | Clients read stale data believing current. Application consistency violations. **Partially mitigated:** Correct system.peers addresses (ce72f32, 1456778) and hostname resolution (8bd3149) improve client-side topology awareness. Clients can now detect unreachable peers. No explicit quorum-loss warning to clients yet. | 7 | 4 | 5 | **140** |
| F6 | T2/T3 | No Forming→Pair fallback — 3rd node disconnects, seed stuck in Cluster with no quorum | Permanent Raft quorum loss. Writes fail indefinitely. Requires operator restart. **Partially mitigated:** Forming state (0bf686d) defines Forming→Pair as valid transition. `formation_timeout_secs` config exists but fallback path is untested (see F26). | 8 | 2 | 8 | **128** |
| F21 | T3 Forming→Cluster | connected_peers mutex contention — peer added during transition | Raft initialized with wrong membership. | 7 | 3 | 6 | **126** |
| F8 | T3 Forming→Cluster | Raft leader election timeout (30s) — DDL on Direct | 30s of unreplicated DDL. If leader never elected, Direct path persists. | 8 | 3 | 5 | **120** |
| F1 | T2 Pair→Forming | ClusterInvite not received — non-seed nodes fail to discover each other | Nodes stuck in Pair. Raft can't form quorum. Cluster is non-functional. **Partially mitigated:** ClusterInvite now sent on Data lane with 10-attempt retry (808b72b), and ClusterInvite handler triggers cluster transition on receiving nodes (ba7599a). Remaining risk: all 10 retries fail under sustained network partition. | 9 | 4 | 3 | **108** |
| F2 | T2 Pair→Forming | No Forming state — jumps Pair→Cluster. If pool not ready, Raft fails. | Raft init hangs. DDL on Direct forever. Cluster appears formed but inoperable. **Substantially mitigated:** Forming state added (0bf686d) with progressive join path Standalone→Pair→Forming→Cluster. Forming→Pair fallback transition defined. Remaining risk: fallback timeout path untested (see F26). | 8 | 3 | 4 | **96** |
| F27 | T3 Forming→Cluster | Bootstrap Phase C delay now configurable (10s, derived from formation_timeout_secs) | Delay increased from 5s to 10s and made configurable. Full RPC barrier remains future work. Reduced occurrence — 10s covers most bootstrap scenarios. | 8 | 2 | 3 | **48** |
| F18 | T5b Decommission Leader | transfer_leader not implemented — leader removes itself from Raft | Remaining nodes lose coordinator. Possible membership corruption. | 9 | 2 | 5 | **90** |
| F28 | T4/T6a/T6b | PeerManager broadcast map cleaned on disconnect via remove_peer() | **Fixed:** `remove_peer()` now cleans broadcast map entries on disconnect. Stale entry accumulation eliminated. Remaining risk: race between disconnect and map read. | 3 | 2 | 3 | **18** |
| F29 | T3 Forming→Cluster | LazyRaft now retries 3x with 5s intervals instead of single 10s timeout | **Fixed:** LazyRaft retries 3 times with 5s intervals (total 15s window). Messages queued during init, not dropped. Remaining risk: init exceeding 15s total. | 6 | 2 | 3 | **36** |
| F9 | T1 Standalone→Pair | Reverse outbound pool fails (500ms delay, single attempt, no retry) | Primary can't send PairSchemaSync. Silent schema divergence. **Partially mitigated:** CQL broadcast and hostname resolution (ce72f32, 8bd3149) ensure correct peer addresses in system.peers. Pool creation itself still single-attempt, but address correctness reduces misrouting. | 7 | 4 | 3 | **84** |
| F16 | T1 Standalone→Pair | Partition immediately after pair formation — writes hang on replication | Write timeout or hang. User-visible latency spike. | 7 | 3 | 4 | **84** |
| F24 | T6b Leader Fails | Clock skew → premature Raft elections | Unnecessary elections. Brief write unavailability. Under heavy skew, livelock. | 5 | 3 | 5 | **75** |
| F15 | T4 Add Member | No ClusterInvite to new node — can't reach all cluster members | Node in Raft membership but unreachable by some peers. Raft messages fail. **Mitigated:** CQL broadcast exchanged in handshake (ce72f32, 1456778), hostname resolution in FERROSA_CQL_BROADCAST (8bd3149), system.peers now populated with correct addresses. | 8 | 3 | 3 | **72** |
| F20 | T1/T2/T3 | PairSchemaSync arrives before handler registered (race with sleep-based timing) | Schema sync dropped. Secondary starts with empty schema. Writes fail. **Mitigated:** LazyRaft pattern (7b057b0) registers Raft handlers before async init, eliminating the handler registration race for Raft messages. PairSchemaSync handler registration timing is also improved. | 7 | 2 | 4 | **56** |
| F17 | T3 Forming→Cluster | Sled log store creation fails (disk full, permissions) | Mode is Cluster but Raft never initializes. Zombie state. | 9 | 2 | 3 | **54** |
| F25 | T2/T3 | Duplicate ClusterInvite propagation — no dedup | Exponential message amplification. Network saturation. | 5 | 3 | 3 | **45** |
| F22 | T4 Add Member | FD exhaustion — each peer needs multiple TCP connections | PriorityPool::connect fails. New node can't join. Existing connections disrupted. **Mitigated:** RAII IpSlotGuard (5063ca6) prevents CQL connection slot leaks. TCP keepalive detects dead peers in ~60s. | 7 | 2 | 2 | **28** |
| F23 | T8a Secondary Fails | Hint store init failure — expect() panics | Total node crash instead of graceful degradation. | 10 | 1 | 2 | **20** |

## Critical Findings (RPN ≥ 150)

### CRITICAL-1: DDL Race During Formation Window (F3, RPN=378)

`controller.rs:919` sets DDL to `DdlPath::Direct` during Raft init. DDL in this 30s+ window is local-only. Schema replay (L1071-1093) errors are logged as warnings and swallowed. **Highest-RPN item in this FMEA, unchanged since initial assessment.**

**Fix:** Block DDL during Forming state OR queue and replay after leader election.

### CRITICAL-2: Missing Reverse Outbound Pools (F5, RPN=280 -> 192)

**Partially mitigated.** CQL broadcast propagation (ce72f32, 1456778) and hostname
resolution (8bd3149) improve peer address discovery, reducing occurrence from 7 to 6.
Core fire-and-forget pool creation at L760-778 still unresolved. Raft init at L950
proceeds without waiting.

**Remaining fix:** `PeerManager::wait_for_peer(host_id, timeout)` before Raft init.

### CRITICAL-3: ClusterInvite Delivery (F1, RPN=270 -> 108)

**Partially mitigated.** ClusterInvite is now implemented and sent on the Data lane
with a 10-attempt retry loop (808b72b). The ClusterInvite handler triggers cluster
transition on receiving nodes (ba7599a). Remaining risk: all 10 retries fail under
sustained network partition. Below critical threshold but still high.

**Remaining fix:** Persistent retry or operator-triggered re-invite for partition scenarios.

### CRITICAL-4: No Forming State + No Fallback (F2+F6, RPN=256 -> 96/128)

**Substantially mitigated.** Forming state added (0bf686d) with progressive join
path Standalone→Pair→Forming→Cluster. Forming→Pair defined as valid transition.
`formation_timeout_secs` config exists. **However**, the timeout fallback path has
no test coverage (see F26, RPN=140). If the fallback is silently broken, F6 risk
reverts to pre-mitigation severity.

**Remaining fix:** Integration test for Forming→Pair fallback under timeout.

### CRITICAL-5: No Decommission Data Streaming (F4, RPN=250 -> 160)

**Partially mitigated.** Bootstrap streaming now implemented for all nodes (ae5ba57),
covering formation-time data transfer. `initiate_decommission` (L451-483) still
proposes LeaveNode without data transfer. Relies on S3 write-behind which may be stale.

**Remaining fix:** Full sequence: LeaveNode → ReassignTokens → stream data → RemoveNode.

### CRITICAL-6: Hint Overflow Without Auto Repair (F19, RPN=168)

Recovered node missing mutations beyond hint capacity. No automatic repair triggered after extended outage. Data silently incomplete.

**Fix:** Trigger anti-entropy repair when hint replay exceeds configurable threshold.

### CRITICAL-7: Unreplicated Primary Returns (F12, RPN=162)

Old primary returns with unreplicated writes after partition. Writes silently discarded with no conflict detection or warning.

**Fix:** Conflict detection on primary rejoin. Log/alert for discarded mutations.

## Cross-Cutting Observations

1. **No persistent formation state** — all in-memory. Crash during transition = orphaned Raft members.
2. **Fire-and-forget spawns** — 7 critical `tokio::spawn` calls with no JoinHandle tracking.
3. **Single-attempt operations** — reverse pools, schema sync, Raft proposals attempted once. Transient failures → permanent inconsistency.
4. **Mode transition not atomic with side effects** — mode stored as Cluster before Raft init completes (L933 vs L950).
5. **Hardcoded timing** — 500ms, 2s, 4s magic numbers remain. Phase C bootstrap delay now configurable (F27). LazyRaft timeout replaced with 3x retry (F29). Raft heartbeat/election tunable via env vars (P2-7).
6. ~~**No cleanup on disconnect**~~ — **Fixed:** PeerManager `remove_peer()` now cleans broadcast map on disconnect (F28).

## Recommended Test Cases

| # | Test | Failure Mode | RPN | Infra |
|---|------|-------------|-----|-------|
| 1 | DDL during Forming replicates after Raft init | F3 | 378 | Firecracker |
| 2 | Raft init waits for all peer pools | F5 | 192 | Firecracker |
| 3 | Approval check inside Raft (TOCTOU) | F7 | 189 | Unit |
| 4 | Crash during Raft init recovers cleanly | F10 | 189 | Firecracker |
| 5 | Hint overflow triggers repair flag | F19 | 168 | Firecracker |
| 6 | Promoted secondary wins on old primary return | F12 | 162 | Firecracker |
| 7 | Decommission streams data before removal | F4 | 160 | Firecracker |
| 8 | Concurrent peer connections don't corrupt state | F11 | 144 | Unit |
| 9 | Schema replay errors fail hard, not warn | F14 | 144 | Unit (mock) |
| 10 | Forming→Pair fallback on formation_timeout_secs | F26 | 140 | Firecracker |
| 11 | Quorum loss: stale reads flagged to client | F13 | 140 | Firecracker |
| 12 | Forming→Pair fallback on 3rd node disconnect | F6 | 128 | Firecracker |
| 13 | Raft leader election within timeout window | F8 | 120 | Firecracker |
| 14 | Hub-and-spoke ClusterInvite propagation | F1 | 108 | Firecracker |
| 15 | Forming state gates Raft initialization | F2 | 96 | Firecracker |
| 16 | Formation timeout restores DDL path to Direct | F26 | 60 | Unit |
| 17 | Bootstrap Phase C configurable delay covers streaming | F27 | 48 | Firecracker |
| 18 | LazyRaft retries handle slow init without dropping messages | F29 | 36 | Unit (mock) |
| 19 | Reverse pool retries on failure | F9 | 84 | Unit (mock) |
