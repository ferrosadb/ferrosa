# Correctness Hazard Scan: Cluster Formation

> Date: 2026-04-01
> Scope: ferrosa-cluster/src/controller.rs and related modules
> Language: Rust
> Reference: Power of 10, CERT Rust, Clippy pedantic

## Summary

| Priority | Count | Category |
|----------|------:|----------|
| P0 — silent data loss/corruption | 1 | DDL gap during Forming window |
| P1 — correctness under concurrency | 5 | Mutex poison, fire-and-forget spawns, race conditions |
| P2 — latent bugs needing trigger | 3 | Hardcoded sleeps, missing shutdown, unbounded collections |

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
| P1-1 | Replace `std::sync::Mutex` with `parking_lot::Mutex` | S | 1 |
| P1-2 | Track spawned tasks with JoinSet | M | 1 |
| P1-3 | Add transition lock or CAS for mode changes | M | 1 |
| P1-4 | Add Forming timeout with Pair fallback | S | 1 |
| P1-5 | Switch to connection-direction role assignment | S | 1 |
| P2-1 | Replace sleeps with condition-based waits | M | 2 |
| P2-2 | Add CancellationToken + shutdown() | M | 2 |
| P2-3 | Cap collection sizes | S | 2 |
