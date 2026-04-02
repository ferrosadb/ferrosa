# DSM Analysis: ModeController Refactoring Plan

> Date: 2026-04-01
> Source: ferrosa-cluster/src/controller.rs (1977 lines, fan-out 22)
> Goal: Decompose god module into testable sub-modules, reduce propagation cost

## Current Function Map

| Region | Lines | Functions | Purpose |
|--------|------:|-----------|---------|
| Struct + constructor | 59-210 | new() | 151 lines — struct def, 15 fields, constructor |
| Test helpers | 215-295 | 2 | 80 lines — test-only constructors |
| Accessors | 296-369 | 10 | 73 lines — trivial getters/setters |
| Cluster membership | 374-487 | 2 | 113 lines — handle_join_request, initiate_decommission |
| Operator commands | 489-534 | 2 | 45 lines — force_promote, switchover |
| **Pair transition** | 536-740 | 1 | **204 lines** — transition_to_pair |
| **Cluster transition** | 741-1111 | 1 | **370 lines** — transition_to_cluster |
| Cluster join | 1112-1194 | 1 | 82 lines — trigger_cluster_join |
| Degraded | 1195-1207 | 1 | 12 lines — transition_to_degraded |
| Peer events | 1208-1348 | 6 | 140 lines — PeerEventListener + InboundPeerCallback |
| Free functions | 1350-1383 | 2 | 33 lines — schema sync, token gen |
| Tests | 1385-1977 | 20 | 592 lines |

**Total production code:** ~1385 lines (excluding tests)
**God methods:** `transition_to_pair` (204 lines), `transition_to_cluster` (370 lines)

## Dependency Analysis of Controller Fields

Which methods access which fields:

| Field | new | pair | cluster | join | promote | peer_events | degraded |
|-------|-----|------|---------|------|---------|-------------|----------|
| mode | W | W | W | R | W | R | W |
| write_path | W | W | W | - | W | - | W |
| cluster_state | W | W | W | - | - | - | - |
| ddl_path | W | W | W | - | W | - | W |
| storage | W | R | R | - | - | - | - |
| schema | W | R | R | - | - | - | - |
| config | W | R | R | R | - | R | - |
| net_config | W | - | R | - | - | - | - |
| local_host_id | W | R | R | R | - | - | - |
| peer_manager | W | R | R | R | - | - | - |
| registry | W | R | R | - | - | - | - |
| pair_context | W | W | W | - | W | - | - |
| force_promoted | W | R | - | - | W | - | - |
| connected_peers | W | - | - | - | W | RW | - |
| raft_instance | W | - | W | R | - | - | - |
| hint_store | W | - | R | - | - | - | - |
| approved_nodes | W | - | - | R | - | - | - |
| ring | W | - | W | R | - | - | - |
| pending_joins | W | - | - | RW | - | - | - |

### Key Observations

1. **`transition_to_pair` and `transition_to_cluster` share many fields** — both need mode, write_path, ddl_path, cluster_state, storage, schema, peer_manager, registry, pair_context. They cannot be fully separated without a shared context.

2. **Peer events are thin dispatchers** — they only read mode + connected_peers, then delegate to transition methods.

3. **Cluster membership** (join, decommission) is a clean extraction target — only needs raft_instance, config, approved_nodes, ring, pending_joins, peer_manager.

4. **Operator commands** (force_promote, switchover) only need pair_context, mode, write_path, ddl_path, force_promoted.

## Proposed Module Structure

```
ferrosa-cluster/src/
  controller/
    mod.rs              — ModeController struct, new(), accessors, shared context
    pair.rs             — transition_to_pair, pair-specific helpers
    cluster.rs          — transition_to_cluster, Raft init, schema replay
    formation.rs        — transition_to_forming, ClusterInvite handler (NEW)
    membership.rs       — handle_join_request, trigger_cluster_join, initiate_decommission
    operator.rs         — force_promote, switchover, transition_to_degraded
    peer_events.rs      — PeerEventListener + InboundPeerCallback impls
    token.rs            — generate_deterministic_token, send_schema_sync_to_peer
    tests.rs            — All 20 test functions
```

### Estimated Line Counts After Split

| Module | Lines | Responsibility |
|--------|------:|----------------|
| `mod.rs` | ~250 | Struct, constructor, accessors, ClusterStateHolder, ModeControllerHandles |
| `pair.rs` | ~220 | transition_to_pair, PairCoordinator setup, handler registration, reverse pool |
| `cluster.rs` | ~380 | transition_to_cluster, Raft init, TokenRing build, schema replay |
| `formation.rs` | ~150 | NEW: transition_to_forming, ClusterInvite handler, mesh verification |
| `membership.rs` | ~200 | handle_join_request, trigger_cluster_join, initiate_decommission |
| `operator.rs` | ~70 | force_promote, switchover, transition_to_degraded |
| `peer_events.rs` | ~150 | on_peer_connected/disconnected/suspected/recovered/failed, on_inbound_peer |
| `token.rs` | ~40 | generate_deterministic_token, send_schema_sync_to_peer |
| `tests.rs` | ~600 | All test functions + test helpers |

**mod.rs target: ~250 lines** (down from 1977)

## Implementation Pattern

The sub-modules will use `impl ModeController` blocks in separate files — Rust allows this natively:

```rust
// controller/mod.rs
pub struct ModeController { /* all fields stay here */ }

impl ModeController {
    pub fn new(...) -> (Arc<Self>, ModeControllerHandles) { ... }
    // accessors...
}

// Re-export so external callers don't change
pub use self::token::generate_deterministic_token;
```

```rust
// controller/pair.rs
use super::ModeController;

impl ModeController {
    pub(crate) fn transition_to_pair(&self, ...) { ... }
}
```

```rust
// controller/peer_events.rs
use super::ModeController;

impl PeerEventListener for ModeController {
    fn on_peer_connected(&self, peer: PeerId) { ... }
    fn on_peer_disconnected(&self, peer: PeerId) { ... }
    // ...
}

impl InboundPeerCallback for ModeController {
    fn on_inbound_peer(&self, peer_id: PeerId) { ... }
}
```

### Advantages of This Pattern

- **Zero API change** — callers still import `ModeController` from `crate::controller`
- **No new types needed** — methods access `self` fields directly
- **Each file is independently reviewable** — transition_to_pair is one file, transition_to_cluster is another
- **Tests can move to a dedicated file** — reduces noise in production code
- **New formation.rs slots in cleanly** — Sprint 1 adds Forming state here without touching pair.rs or cluster.rs

### Risks

1. **Field visibility** — all fields must be `pub(super)` or `pub(crate)` since sub-modules need access. This is fine for `controller/` internal modules.
2. **Ordering of `impl` blocks across files** — Rust doesn't care about ordering, but reviewers might find it surprising. Add a comment in mod.rs listing which files contain which impls.
3. **Async borrows** — `transition_to_cluster` spawns `tokio::spawn` that captures `Arc<Self>`. This works fine across files since the method still takes `&self`.

## TDD Extraction Protocol

The refactor uses `/tdd` with the **characterization test** (golden master) pattern. Tests are the safety net — no code moves without them.

### Step 0: Write Characterization Tests (RED→GREEN)

Before extracting anything, write tests that pin every public behavior of ModeController. These tests describe what the code DOES, not what it SHOULD do. They must all pass on the current monolithic controller.

```rust
// ferrosa-cluster/src/controller/tests.rs (new characterization tests)

#[test]
fn char_standalone_initial_state() {
    // Assert: mode=Standalone, write_path=Direct, ddl_path=Direct
}

#[test]
fn char_pair_transition_sets_coordinator_and_handlers() {
    // Assert: mode=Pair, PairWriteForwardHandler registered,
    //         PairDdlForwardHandler registered, pair_context set
}

#[test]
fn char_cluster_transition_inits_raft_and_ring() {
    // Assert: mode=Cluster, raft_instance set, ring set,
    //         ClusterCoordinator in write_path, RaftAppendHandler registered
}

#[test]
fn char_degraded_clears_write_path() {
    // Assert: mode=Standalone, write_path=Unavailable, ddl_path=Unavailable
}

#[test]
fn char_force_promote_overrides_role_on_next_pair() {
    // Assert: force_promoted=true, next transition_to_pair → Primary
}

#[test]
fn char_concurrent_connects_no_double_transition() {
    // Assert: 2 simultaneous on_peer_connected → exactly 1 pair transition
}

#[test]
fn char_trigger_join_dedup() {
    // Assert: same host_id twice → only 1 pending join
}

#[test]
fn char_decommission_requires_raft() {
    // Assert: decommission without Raft → error
}

#[test]
fn char_deterministic_tokens_stable() {
    // Assert: same (node_id, index) → same token across runs
}
```

### Extraction Steps

Each step: extract → `cargo test` → commit. If tests fail, revert and investigate.

| Step | Extract | Risk | TDD Gate |
|------|---------|------|----------|
| 0 | Write characterization tests | None | All tests GREEN on current code |
| 1 | `tests.rs` — move existing + new characterization tests | None | `cargo test` green |
| 2 | `token.rs` — 2 free functions | None | `cargo test` green |
| 3 | `peer_events.rs` — PeerEventListener + InboundPeerCallback | Low | `cargo test` green |
| 4 | `operator.rs` — force_promote, switchover, degraded | Low | `cargo test` green |
| 5 | `membership.rs` — join, decommission | Low | `cargo test` green |
| 6 | `pair.rs` — transition_to_pair (204 lines) | Medium | `cargo test` green |
| 7 | `cluster.rs` — transition_to_cluster (370 lines) | Medium | `cargo test` green |
| 8 | `formation.rs` — NEW code via /tdd red-green-refactor | None | New tests written RED first |

**Total refactoring effort: ~4-6 hours for steps 0-7** (step 0 adds ~1h for characterization tests, mechanical moves no logic changes)

## Validation Criteria

- [ ] Characterization tests written and GREEN before any extraction (Step 0)
- [ ] `cargo test -p ferrosa-cluster` passes after EACH step (not just at the end)
- [ ] `cargo clippy --all-targets` clean
- [ ] `controller/mod.rs` < 300 lines
- [ ] No function > 60 lines (Rule 4)
- [ ] Each sub-module has a single responsibility
- [ ] Fan-out of mod.rs < 10 (down from 22 — most imports move to sub-modules)
- [ ] New `formation.rs` code written entirely via /tdd red-green-refactor
- [ ] One commit per extraction step (not one giant commit)

## DSM Metrics After Refactoring (Projected)

| Metric | Before | After | Target |
|--------|--------|-------|--------|
| controller lines | 1977 | ~250 (mod.rs) | < 300 |
| Fan-out (mod.rs) | 22 | ~8 | < 10 |
| Largest method | 370 (transition_to_cluster) | 370 (in cluster.rs) | < 60 (future) |
| Propagation cost | 34% | ~24% | < 22% |
| Independently testable modules | 1 | 8 | 8 |

Note: The 370-line `transition_to_cluster` should be further decomposed within `cluster.rs` into helper functions (init_raft_components, build_token_ring, spawn_raft_init, replay_schema). This is Sprint 3, task 3.3 follow-up work.
