# Correctness Hazard Scan: Cluster Formation

> Date: 2026-04-02 (Phase 5 update)
> Scope: ferrosa-cluster/src/controller/, ferrosa-cluster/src/raft/handlers.rs,
>   ferrosa-cluster/src/state.rs, ferrosa-net/src/{handshake,message,peer}.rs,
>   ferrosa-cql/src/server.rs
> Language: Rust
> Reference: Power of 10, CERT Rust, Clippy pedantic

## Summary

| Priority | Count | Category |
|----------|------:|----------|
| P0 — silent data loss/corruption | 3 (2 fixed) | DDL gap, ~~bincode silent empty~~ (45eaa91), ~~RwLock unwrap in Raft RPC~~ (45eaa91) |
| P1 — correctness under concurrency | 8 | std::sync::RwLock poison, fire-and-forget spawns, quorum calc, unbounded reads |
| P2 — latent bugs needing trigger | 6 | Hardcoded sleeps, missing shutdown, unbounded collections, magic numbers, silent fallbacks |

## Findings

### P0-1: DDL Applied During Forming Window Is Not Replicated

**Location:** `controller.rs:908-931` (DdlPath stays Direct during Forming→Cluster)
**Hazard:** DDL (CREATE TABLE, ALTER TABLE) applied between entering Forming state and Raft leader election is only on the local node. If the leader election fails or this node is not elected leader, the DDL is silently lost on other nodes.
**Mitigation in spec:** Leader replays schema after election (L1074-1098). But if this node is NOT the leader, its local-only DDL is never replicated.
**Fix:** Queue DDL operations during Forming state, replay them through Raft after leader election regardless of which node is leader. Or block DDL during Forming (simpler, brief window).

### P1-1: Mutex Poison on `.lock().unwrap()` — 17 Instances in Prod Code

**Location:** `controller.rs` lines 307, 364, 383, 500, 501, 511, 619, 931, 1115, 1124, 1201, 1202, 1214, 1238, 1257, 1309, 1332
**Hazard:** If any thread panics while holding `pair_context`, `connected_peers`, `pending_joins`, or `approved_nodes`, all subsequent lock attempts panic (poison propagation). In a distributed system, one bad request could cascade into total node failure.
**Fix:** Use `parking_lot::Mutex` (no poison) or handle poison: `.lock().unwrap_or_else(|e| e.into_inner())`.

### P1-2: Fire-and-Forget tokio::spawn — 7 Untracked Tasks

**Location:** `controller.rs` lines 641, 662, 723, 766, 950, 1129, 1292
**Hazard:** No `JoinHandle` stored for any spawned task. If a spawned task panics:
- The panic is silently swallowed by the tokio runtime
- No error propagation to the controller
- Critical operations (Raft init at L950, cluster join at L1129) can fail invisibly

**Most critical:** L950 spawns the entire Raft initialization (~150 lines). If this panics, the node appears to be in Cluster mode but has no functioning Raft instance. Writes will silently fail or hang.

**Partial mitigation:** The LazyRaft pattern (commit 7b057b0) partially addresses
the Raft init spawn by synchronizing handler readiness — Raft RPC handlers are
registered before the async init task is spawned, so handler registration is no
longer dependent on the spawned task completing. The spawned task itself is still
fire-and-forget.

**Fix:** Store `JoinHandle`s, use `tokio::task::JoinSet`, or at minimum add `.instrument()` tracing and panic hooks.

### P1-3: Race Between on_peer_connected and transition_to_cluster

**Location:** `controller.rs:1208-1249` and `controller.rs:741-1105`
**Hazard:** `on_peer_connected` reads `connected_peers` count under a Mutex, then calls `transition_to_cluster` without holding the lock. A third peer connecting between the read and the transition call could cause `transition_to_cluster` to be called twice concurrently.
**Evidence:** The mode check (`DeploymentMode::Pair`) provides some protection via ArcSwap, but ArcSwap compare-and-swap is not used — it's a load-then-store pattern.
**Fix:** Use `ArcSwap::compare_and_swap` for mode transitions, or hold a transition lock across the check-and-transition.

### P1-4: No Forming→Pair Fallback Timeout

**Location:** Missing — identified in spec Gap #6
**Hazard:** If a node enters Forming state (2nd peer seen) but the 3rd peer never fully connects, the node hangs indefinitely in Forming. Writes continue on Pair semantics with original peer, but the node never advances to Cluster and never falls back.
**Fix:** Add `Forming.deadline: Instant` with configurable timeout (default 60s). On timeout, log warning and transition back to Pair.

### P1-5: Role Election Race (UUID vs Connection Direction)

**Location:** `pair/mod.rs:25` (`PairRole::elect`) vs spec design
**Hazard:** Current code uses UUID comparison for role election. The spec proposes connection-direction-based assignment. During the migration, if nodes are running mixed versions, one node could use UUID and the other connection direction, both electing themselves Primary → split brain.
**Fix:** Implement as a single atomic change. The `need_reverse` flag in `transition_to_pair` already distinguishes inbound (seed) from outbound (joiner) — use this as the sole role determinant.

### P2-1: Hardcoded Sleep Delays in Transition Logic

**Location:** `controller.rs` lines 643 (500ms), 664 (4s), 725 (2s), 1044-1045 (100-500ms backoff)
**Hazard:** Fixed delays assume specific timing:
- 500ms for reverse pool establishment — may be insufficient under load or high latency
- 4s wait after force-promote rejoin — too long for fast networks, too short for slow ones
- 2s wait before schema sync — arbitrary
**Impact:** Under load or network congestion, these delays may be too short, causing operations on not-yet-ready connections. In tests, they add unnecessary latency.
**Fix:** Replace with condition variables or `tokio::sync::Notify` — wait for the actual event, not a fixed duration.

### P2-2: No Graceful Shutdown for Spawned Tasks

**Location:** `controller.rs` — no `CancellationToken`, no shutdown method
**Hazard:** When the node shuts down, 7 spawned tasks have no cancellation mechanism. In-flight Raft proposals, cluster joins, and schema syncs will be interrupted mid-operation. Partial Raft membership changes could leave the cluster in an inconsistent state.
**Fix:** Add `CancellationToken` to ModeController, pass to all spawned tasks, add a `shutdown()` method that cancels and awaits all tasks.

### P2-3: Unbounded `connected_peers` and `pending_joins`

**Location:** `controller.rs` — `connected_peers: Mutex<Vec<...>>`, `pending_joins: Mutex<BTreeSet<...>>`
**Hazard:** No upper bound on peer count or pending joins. A malicious or misconfigured cluster could cause unbounded memory growth.
**Fix:** Cap `connected_peers` at `max_cluster_size` (config). Cap `pending_joins` at a reasonable limit (e.g., 10).

---

## Phase 5 Findings (2026-04-02)

New hazards identified by scanning recently-changed files. Cross-referenced
against existing findings above to avoid duplication.

### P0-2: `bincode::serialize().unwrap_or_default()` Silently Produces Empty Data — FIXED (45eaa91)

**Location:** `ferrosa-cluster/src/controller/cluster.rs:642-643`
**Hazard:** During bootstrap streaming, `bincode::serialize(&wire_rows).unwrap_or_default()` silently replaces serialization failures with an empty `Vec<u8>`. The streamed mutation arrives at the destination with zero-length row data. The receiving node writes a partition with no rows — **silent data loss**. The sending node logs nothing.
**Impact:** P0 — a serialization bug (e.g., from a bincode version mismatch or a row with an unexpected field) would corrupt data across the cluster during bootstrap.
**Fix applied:** Replaced `.unwrap_or_default()` with `match` — serialization failures now log an error with the partition key and `continue` to skip the partition. No silent data loss.

### P0-3: `std::sync::RwLock::read().unwrap()` in Raft Init Async Task — FIXED (45eaa91)

**Location:** `ferrosa-cluster/src/controller/cluster.rs:366, 551, 600`
**Hazard:** The `node_map` is a `std::sync::RwLock` (from `raft/network.rs`). Three `.read().unwrap()` calls are inside the `spawn_tracked` async block. If the RwLock is ever poisoned (e.g., a panic in `register_node` which calls `.write().expect()`), the Raft init task panics. Since this task is fire-and-forget (tracked but not awaited for error recovery), the node enters Cluster mode with no functioning Raft instance and no DDL path.
**Impact:** P0 — node appears healthy but cannot process writes or DDL; silent split-brain risk.
**Fix applied:** All 3 sites now use `.unwrap_or_else(|e| e.into_inner())` to read through poison. The underlying HashMap data is still valid even when the lock is poisoned.

### P1-6: `std::sync::RwLock` in `IpConnectionTracker` — Panic on Poison

**Location:** `ferrosa-cql/src/server.rs:76, 87`
**Hazard:** `IpConnectionTracker` uses `std::sync::RwLock` with `.write().unwrap()`. If a thread panics while holding this lock (e.g., inside `try_acquire` or `release`), the RwLock becomes poisoned. All subsequent CQL connection attempts will panic the accept loop, taking down the entire CQL server.
**Impact:** P1 — a single panic permanently kills CQL connectivity.
**Fix:** Switch to `parking_lot::RwLock` (consistent with ferrosa-cluster's approach) or handle poison explicitly.

### P1-7: Fire-and-Forget `tokio::spawn` in `ClusterInviteHandler` and `peer_events.rs`

**Location:** `ferrosa-cluster/src/controller/cluster.rs:811, 840` (ClusterInviteHandler); `ferrosa-cluster/src/controller/peer_events.rs:161` (on_inbound_peer CQL broadcast store)
**Hazard:** Three `tokio::spawn` calls are not tracked via `spawn_tracked`:
- L811: Connecting to discovered peers on invite receipt
- L840: Re-broadcasting invite to new peers
- L161: Storing peer CQL broadcast in PeerManager
These tasks cannot be cancelled during shutdown and panics are silently swallowed.
**Impact:** P1 — the connection tasks (L811) are critical for mesh formation. If they panic silently, peers never connect and the cluster cannot form. The CQL broadcast task (L161) failing silently means system.peers returns wrong addresses.
**Fix:** Use `spawn_tracked` or at minimum wrap each spawn body with `.instrument(tracing::info_span!(...))` and add a panic hook.

### P1-8: Quorum Calculation Uses Shrinking Total — False Quorum Restoration

**Location:** `ferrosa-cluster/src/controller/peer_events.rs:54-68`
**Hazard:** In `on_peer_disconnected` (line 91-109), `total = connected + 1` uses the *current* connected count (after removal), not the original cluster size. In a 5-node cluster where 3 nodes disconnect sequentially: after losing 2 peers, `connected=2, total=3, quorum=2` — the check `connected+1 >= quorum` (3 >= 2) passes, so the node stays in Cluster mode. This is correct. But the inverse in `on_peer_connected` for DegradedCluster recovery (line 54-68) also uses the shrinking total, meaning reconnecting just 1 node out of 3 lost could falsely restore quorum.
**Impact:** P1 — the node could transition from DegradedCluster back to Cluster without actually having a quorum, leading to writes that cannot be replicated.
**Fix:** Store the total cluster membership count at formation time and use that fixed value for quorum calculations, not the dynamic connected count.

### P1-9: Unbounded `read_range` with `usize::MAX` During Bootstrap

**Location:** `ferrosa-cluster/src/controller/cluster.rs:608-614`
**Hazard:** `storage.read_range(&table_id, None, None, usize::MAX)` loads ALL partitions for a table into memory at once. For a table with millions of partitions, this causes OOM. The same pattern exists in `membership.rs:157` and `repair/mod.rs:45`.
**Impact:** P1 — bootstrap of a large table crashes the node with OOM.
**Fix:** Use a bounded page size (e.g., 10,000) and iterate in batches. The `RangeReadHandler` already uses `1_000_000` as a limit — but even that may be too large for production.

### P1-10: `RangeReadHandler` Hard Limit of 1,000,000 Partitions

**Location:** `ferrosa-cluster/src/raft/handlers.rs:653`
**Hazard:** `read_range(&table_id, None, None, 1_000_000)` is a hard-coded limit with no pagination. For tables with more than 1M partitions, the range read silently returns a truncated result. The coordinator performing a `SELECT COUNT(*)` gets a wrong answer.
**Impact:** P1 — silent incorrect query results for large tables.
**Fix:** Implement server-side pagination or at minimum return a `truncated: bool` flag in the response payload so the coordinator knows to issue follow-up requests.

### P2-4: Bootstrap Promotion Uses Fixed 5-Second Delay

**Location:** `ferrosa-cluster/src/controller/cluster.rs:700`
**Hazard:** The leader waits exactly 5 seconds after local bootstrap streaming completes before promoting Joining nodes to Normal. The code has a TODO comment acknowledging this: `// TODO: Replace fixed delay with proper BootstrapComplete RPC barrier.` If non-leader streaming takes longer than 5 seconds (large dataset, slow network), nodes are promoted before they have data — reads to those nodes return empty results.
**Impact:** P2 — stale/missing reads on newly promoted nodes. The window is bounded (data arrives eventually via read repair) but violates consistency guarantees during the gap.
**Fix:** Implement a BootstrapComplete RPC barrier as the TODO says.

### P2-5: `PairClusterState::peers()` Returns Empty Vec on Lock Contention

**Location:** `ferrosa-cluster/src/state.rs:38-41`
**Hazard:** `self.state.try_read()` returns `Err(_) => return vec![]` when the tokio RwLock is contended. A CQL client executing `SELECT * FROM system.peers` during a write to the pair state will see zero peers. Cassandra drivers may interpret this as a topology change and reconnect to different nodes.
**Impact:** P2 — transient driver reconnection storms during pair state updates. Not data loss, but degrades availability.
**Fix:** Use `self.state.blocking_read()` (since `ClusterState::peers()` is sync) or cache the last-known peer list and return the cached version on contention.

### P2-6: ClusterInvite Re-broadcast Fixed 500ms Delay

**Location:** `ferrosa-cluster/src/controller/cluster.rs:841`
**Hazard:** The re-broadcast of ClusterInvite to newly discovered peers waits a fixed 500ms for connections to establish. If connections take longer (e.g., TLS handshake under load), the re-broadcast fails silently and those peers never learn about the full cluster membership.
**Impact:** P2 — mesh formation may be incomplete in slow networks, requiring a second invite round.
**Fix:** Wait for the connection tasks to complete (use JoinSet) rather than a fixed delay.

### P2-7: Magic Numbers in Raft Configuration

**Location:** `ferrosa-cluster/src/controller/cluster.rs:407-412`
**Hazard:** Raft configuration values are hard-coded: `heartbeat_interval: 300`, `election_timeout_min: 1000`, `election_timeout_max: 2000`, `max_payload_entries: 100`, snapshot policy `LogsSinceLast(1000)`. These are not configurable via `ClusterConfig`.
**Impact:** P2 — operators cannot tune Raft for their network characteristics without code changes. In high-latency environments, the 1-2 second election timeout may be too aggressive.
**Fix:** Expose these as fields in `ClusterConfig` with the current values as defaults.

### P2-8: `compute_partition_digest` Uses `unwrap_or_default()` for Empty Digest

**Location:** `ferrosa-cluster/src/raft/handlers.rs:263`
**Hazard:** `bincode::serialize(&wire).unwrap_or_default()` in digest computation. If serialization fails, the digest is computed over an empty byte slice (always the same hash). Two different partitions that both fail to serialize would produce identical digests, causing read repair to incorrectly conclude they match.
**Impact:** P2 — read repair skips a partition that actually diverged. Extremely unlikely in practice (bincode serialization of owned types rarely fails) but violates the safety contract.
**Fix:** Return `Result<u32, Error>` instead of silently degrading.

## CI Pipeline Requirements

| Check | Status | Notes |
|-------|--------|-------|
| `cargo clippy --all-targets` | **Active** | Good |
| `cargo fmt --check` | **Active** | Good |
| `cargo test` | **Active** | Good |
| Miri (`cargo +nightly miri test`) | **Missing** | No unsafe in formation code, but useful for detecting UB in dependencies |
| Loom (`loom` crate for concurrency) | **Missing** | Would catch P1-3 race condition |
| `#[deny(clippy::unwrap_used)]` | **Missing** | Would flag all 17 P1-1 instances |

## Recommended Actions

| Priority | Hazard | Effort | Sprint |
|----------|--------|--------|--------|
| P0-1 | Block or queue DDL during Forming | S | 1 |
| ~~P0-2~~ | ~~Fix `unwrap_or_default()` in bootstrap serialization~~ | **DONE** (45eaa91) | — |
| ~~P0-3~~ | ~~Harden `std::sync::RwLock` in Raft init (node_map)~~ | **DONE** (45eaa91) | — |
| P1-1 | Replace `std::sync::Mutex` with `parking_lot::Mutex` | S | 1 |
| P1-2 | Track spawned tasks with JoinSet | M | 1 |
| P1-3 | Add transition lock or CAS for mode changes | M | 1 |
| P1-4 | Add Forming timeout with Pair fallback | S | 1 |
| P1-5 | Switch to connection-direction role assignment | S | 1 |
| **P1-6** | **Switch IpConnectionTracker to parking_lot::RwLock** | **S** | **1** |
| **P1-7** | **Track ClusterInviteHandler spawns via spawn_tracked** | **S** | **1** |
| **P1-8** | **Fix quorum calc to use fixed membership size** | **M** | **1** |
| **P1-9** | **Paginate bootstrap read_range (usize::MAX OOM)** | **M** | **2** |
| **P1-10** | **Add pagination/truncation flag to RangeReadHandler** | **M** | **2** |
| P2-1 | Replace sleeps with condition-based waits | M | 2 |
| P2-2 | Add CancellationToken + shutdown() | M | 2 |
| P2-3 | Cap collection sizes | S | 2 |
| **P2-4** | **Replace 5s bootstrap promotion delay with RPC barrier** | **M** | **2** |
| **P2-5** | **Cache PairClusterState peers to avoid empty on contention** | **S** | **2** |
| **P2-6** | **Replace 500ms invite re-broadcast delay with JoinSet wait** | **S** | **2** |
| **P2-7** | **Make Raft config values configurable via ClusterConfig** | **S** | **3** |
| **P2-8** | **Return Result from compute_partition_digest** | **S** | **3** |
