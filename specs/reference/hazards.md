# Correctness Hazard Scan — ferrosa (Rust-specific)

> Date: 2026-04-18
> Scope: ferrosa workspace (all 17 crates). Complements
>        `specs/hazards-cluster-formation.md` (cluster-formation specific)
>        and the system-level `specs/fmea.md`.
> Language: Rust (edition 2021)
> Lens: Power of 10, CERT Rust, clippy pedantic, tokio async idioms.
>
> This document is intentionally short. Each entry is one paragraph:
> what the hazard is, where it lives, and the smallest fix. Scoped
> deep-dives live in `hazards-cluster-formation.md`; do not duplicate
> them here.

## Priority summary

| Priority | Count | Category                                              |
|----------|------:|-------------------------------------------------------|
| P0       | 2     | Unwrap/expect on prod hot paths; wrong test attribute |
| P1       | 4     | Unbounded channels; Arc<Mutex> hotspots; poison       |
| P2       | 3     | Over-broad `rescue`/`?`; fire-and-forget spawns;
                     ignored `must_use` results                            |

## P0

### P0-1: `.unwrap()` / `.expect()` on prod request paths

**Where:** Widespread, but concentrated in `ferrosa-storage/src/engine.rs`
(flush/compaction callbacks), `ferrosa-cql/src/server.rs` (request
dispatch), `ferrosa-cluster/src/controller/*` (17 known sites tracked
in `hazards-cluster-formation.md` P1-1). **Why it's P0 here:** a panic
inside a tokio task is silently dropped by default, turning a hard crash
into a soft silent failure — the exact anti-pattern the safety rules
forbid. **Fix:** enable `#![deny(clippy::unwrap_used, clippy::expect_used)]`
per-crate and allow exceptions only with a `// SAFETY:` comment. Pair
with a `tokio::spawn` wrapper that logs + re-raises panics.

### P0-2: `#[tokio::test]` vs `#[test]` misuse in async code

**Where:** any test that `await`s inside `#[test]`, or any sync test
marked `#[tokio::test]` (the latter silently constructs an extra
runtime per test, inflating test time and occasionally deadlocking on
blocking I/O). **Why it's P0:** a misuse hides race conditions because
the test never exercises the async scheduler correctly. **Fix:** a
clippy/lint pass across the workspace; ensure every async test uses
`#[tokio::test(flavor = "multi_thread", worker_threads = N)]` with an
explicit thread count when it launches tasks, and every sync test is
`#[test]`. Add a CI check via `grep`-based custom rule or
`cargo-nextest`'s filter to flag mixed usage.

## P1

### P1-1: Unbounded `tokio::sync::mpsc::unbounded_channel()` / `crossbeam::channel::unbounded()`

**Where:** audit with `Grep` for `unbounded_channel\(\)` and
`channel::unbounded\(\)`. Historically present in `ferrosa-net`
lane-actor paths (partially mitigated by the Raft starvation fix that
introduced `Semaphore(128)` backpressure) and in the telemetry layer.
**Hazard:** producer faster than consumer → memory grows without
bound → OOM under the 2 GB cgroup. **Fix:** default to
`mpsc::channel(cap)` with an explicit capacity derived from the
subsystem's latency budget; add `try_send` + drop-with-metric fallback
paths for non-critical channels (telemetry) and `send().await` with
backpressure for critical ones (writes, Raft).

### P1-2: `Arc<Mutex<T>>` / `Arc<RwLock<T>>` hotspots in hot paths

**Where:**
`ferrosa-cluster/src/controller/*` (`connected_peers`,
`approved_nodes`, `pending_joins`),
`ferrosa-storage/src/memtable` (per-table lock),
`ferrosa-cql/src/server.rs` (`IpConnectionTracker`, already migrated
to `parking_lot::RwLock` per commit e462a8e).
**Hazard:** writer starvation under read load; poison propagation if
`std::sync::Mutex`; contention that doesn't show up in single-node
tests but dominates in a 3+ node cluster under write fan-out.
**Fix:** (a) migrate remaining `std::sync::Mutex` and
`std::sync::RwLock` to `parking_lot` (no poison, faster); (b) for
long-held critical sections, refactor to lock-then-copy-out-then-drop,
operate on the copy; (c) where the value is append-only, use
`arc_swap::ArcSwap<Vec<T>>` instead of a mutex.

### P1-3: `std::sync::*` poison propagation

**Where:** any remaining `std::sync::Mutex` / `std::sync::RwLock` in
`ferrosa-*` crates. Two prod sites already fixed (45eaa91, e462a8e);
more likely remain. **Hazard:** one panic → permanent node degradation,
as every subsequent `.lock().unwrap()` re-panics. Cross-ref
`hazards-cluster-formation.md` P1-1 (17 `.unwrap()` sites). **Fix:**
use `parking_lot` (preferred) or the
`.lock().unwrap_or_else(|e| e.into_inner())` pattern with a comment
justifying safety.

### P1-4: Fire-and-forget `tokio::spawn` with no `JoinHandle` tracking

**Where:** 7+ sites in `ferrosa-cluster/src/controller`; several in
`ferrosa-storage` compaction dispatch; at least one in
`ferrosa-graph/src/subscribe`. Partial mitigation via `spawn_tracked`
for 3 cluster sites (76e307b) and the Raft-lane dedicated-thread
pattern. **Hazard:** panics silently swallowed; no cancellation on
shutdown → partial state writes; JoinSet leaks. **Fix:** adopt
`spawn_tracked` (or a workspace-local `ferrosa_common::spawn`) that
installs a panic hook, records metrics, and registers into a
per-subsystem `JoinSet` for graceful shutdown.

## P2

### P2-1: Over-broad `?` that converts typed errors to a generic boxed error

**Where:** any function returning `anyhow::Result` that previously had
a specific error type; audit with `Grep` for
`-> anyhow::Result` in library crates (`ferrosa-storage`,
`ferrosa-schema`, `ferrosa-cluster`). **Hazard:** callers cannot match
on failure modes; `rescue`-style handlers default to "treat as
transient" and retry, masking real bugs. **Fix:** library crates expose
typed errors (`thiserror`); `anyhow` only at binary boundaries.

### P2-2: `#[must_use]` ignored on `Result` / `JoinHandle` / `Builder`

**Where:** any site returning a `Result` that is followed by a bare
expression statement; `JoinHandle`s dropped after `spawn()`. **Hazard:**
ignored errors; dropped handles silently detach tasks that should have
been awaited. **Fix:** enable `#![warn(unused_must_use)]` and
`#![warn(clippy::must_use_candidate)]` workspace-wide; CI treats
warnings as errors.

### P2-3: `select!` without `biased` when one branch is a shutdown signal

**Where:** long-running loops in `ferrosa-net` and
`ferrosa-cluster/src/controller`. **Hazard:** under load, the
shutdown branch is starved; `CancellationToken::cancelled()` may not
fire for many seconds. **Fix:** add `biased;` at the top of any
`select!` where one branch is a shutdown/cancellation; place the
cancellation branch first.

## CI gate recommendations

| Check                                               | Status  | Notes                                   |
|-----------------------------------------------------|---------|-----------------------------------------|
| `cargo fmt --check`                                 | active  | keep                                    |
| `cargo clippy --all-targets -- -D warnings`         | active  | keep                                    |
| `cargo test --all`                                  | active  | keep                                    |
| `#![deny(clippy::unwrap_used, clippy::expect_used)]`| missing | add per-crate, allow with comment       |
| `#![warn(unused_must_use)]` workspace               | missing | trivial to enable                       |
| Custom check: no `#[test]` with `.await` in body    | missing | grep or proc-macro gate                 |
| Loom (`cargo test --features loom`)                 | missing | targeted on lock hotspots (P1-2)        |
| `cargo miri test -p ferrosa-common`                 | missing | useful for UB in dependencies           |
| Channel-cap audit: no `unbounded_channel()` in core | missing | grep-based denylist with allow comments |

## Related documents

- `specs/fmea.md` — system-level FMEA (paired with this document)
- `specs/hazards-cluster-formation.md` — scoped cluster-formation hazards
- `specs/observability-fmea.md` — telemetry-specific modes
