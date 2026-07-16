# Bug: idle 3-node cluster busy-spins ~3 CPU cores (CQL keepalive/RequestTimeout under 2 vCPU)

> Docs triage note (2026-07-15): moved from `specs/todo/` to `specs/implemented/`.
> Implementation evidence: `ferrosa-storage/src/self_heal/` now avoids clean-tick
> row scans and replica-posture probes that caused the idle self-heal busy-spin;
> the source comments explicitly reference this bug.
> Verification runs: `cargo test -p ferrosa-storage --lib tick_with_no_corruption_is_idle`
> and `cargo test -p ferrosa-storage --lib clean_tick_does_not_probe_replica_posture`.

**Status**: Implemented and locally verified. Root-cause localized to a userspace busy-spin in the net/runtime layer; exact
loop not yet pinned (needs a CPU flamegraph). Filed 2026-06-14.
**Severity**: High — starves a 2-vCPU deployment before any work; surfaces as `KeepaliveTimeout` /
`RequestTimeout` flakes (ferrosa-memory `cql_live` CI on `ubuntu-latest` 2 vCPU). Also wastes CPU
everywhere — "creeps up" as cost/latency/scaling.

## Symptom

ferrosa-memory's "Cluster integration tests" (`cql_live`) fail on the 2-vCPU/7GB GitHub runner with
`KeepaliveTimeout` / "pool is broken" / `RequestTimeout(30000ms)` on connect, `CREATE TABLE`, and
the 100k-row `derived_cache` **seed** (write). Nodes go globally unresponsive under load. **Not**
OOM (no `OOMKilled`, no exit 137). Persists after merging #131 (read offload) + #132 (sstable OOM).

## Proof (measured, local repro: 3 nodes capped to ~0.66 CPU each ≈ 2 total, 2 G mem each)

**Memory is NOT the constraint.** During the failing 100k-seed, per-node `memory.current` stayed
~1.0–1.26 GB / 2 GB; `memory.events` = `oom 0 oom_kill 0 max 0` on all nodes (never hit the 2 GB
ceiling). Proven: fits in 2 G.

**CPU is the constraint, and it's a spin — not legitimate demand:**
- Under the 2-CPU cap during the seed: `cpu.stat nr_throttled` climbed continuously (node3
  `throttled_usec` ≈ 559 s over ~2 min) → nodes pegged at quota → 30 s timeouts.
- **Uncapped**, the same single-client seed drew **~9.2 cores total** (node1 3.15, node2 3.34,
  node3 2.70) — absurd for one serial insert client.
- **Idle, after a 60 s quiesce with ZERO client load: ~3.1 cores total** (≈1 core/node). An idle
  cluster has no reason to burn 3 cores.
- Thread attribution (`/proc/1/task/*/{stat,comm,syscall}`): the burners are **`data-rt`** (the
  per-lane `current_thread` runtime, `ferrosa-net/src/lane_actor.rs:460-468`) and the main
  **`tokio-rt-worker`** threads, in state **`R` with `syscall=running`** → **busy-spinning in
  userspace**, not blocked in epoll/futex.

Conclusion: a userspace busy-loop (likely a poll/tick that never parks) runs continuously on both
the lane runtime and the main runtime. On 2 vCPU it consumes the whole box at idle, so any real work
misses its deadlines → keepalive/RequestTimeout. Raft is NOT storming (election churn ~0).

## Ruled out (do not re-chase)
- OOM / 2 G memory limit (measured above).
- #131 (range-read offload) / #132 (sstable seek-index OOM) — both merged to main, spin persists.
- `spawn_alive_watcher` (`reconnect.rs:172`) — correctly parks on `watch::changed().await`.
- `run_heartbeat_loop` (`peer.rs:308`) — correctly uses `interval.tick().await`.
- The `yield_now().await` hits in `lane_actor.rs`/`peer.rs`/`pool.rs` — all in `#[tokio::test]` code,
  not production.

## ROOT CAUSE CONFIRMED via CPU flamegraph (pprof, 2026-06-14)

Built ferrosa with a gated `pprof` CPU sampler (`main.rs` `maybe_start_cpu_profiler`,
`FERROSA_CPU_PROFILE_SECS`/`_DELAY_SECS`), sampled an **idle** node for 30 s. **95% of samples** are:

```
repair::build_tree_for_range → repair::walk_token_range_for_digest
  → reader::next_clustered_row → read_row → read_at
    → io::get_or_open  (lru::LruCache<PathBuf, Arc<File>>) → hash<std::path::Path>
```

**An idle cluster runs anti-entropy repair Merkle builds continuously** (~3 cores total, persistent —
measured flat 2.0–3.1 cores across 80 s on a cluster idle ~5 min; does NOT decay, so it is steady
state, not post-restart catch-up). It is **not** the periodic scheduler: that defaults to a 24h
interval / `sub_tick = interval / ceil(tables/max_concurrent)` and had not ticked at the 34 s sample
mark. The continuous driver is the self-heal/quarantine-refill (`repair_wiring.rs request_refill` /
`cluster_view.replica_posture` via `probe_digest`) or peer Merkle-RPC (`repair/rpc.rs:161
RepairMerkleHandler`) firing on a tight cadence — to be pinned.

Two compounding defects:
1. **Repair re-reads every row body** to compute the digest (`walk_token_range_for_digest` streams
   full clustered rows) AND runs continuously → O(all data) CPU, forever.
2. **`get_or_open` hashes the full `PathBuf` per `read_at`** (`ferrosa-sstable/src/io.rs`,
   `LruCache<PathBuf, Arc<File>>`) — a path hash per row read, multiplied by millions of reads.

On 2 vCPU this idle spin owns the box → CQL ops miss their 30 s deadline → the `cql_live`
`KeepaliveTimeout`/`RequestTimeout` failures. Confirmed NOT OOM/RAM.

### Fix directions (pick per design)
- **(primary) stop the continuous trigger**: find why repair/posture re-fires on a tight loop with
  an idle, converged cluster and gate it (only repair on real divergence / respect a min interval /
  cache posture). The 24h scheduler is fine; the event/posture path is not.
- **(throttle, defense-in-depth)**: bound background-repair CPU (yield/sleep between partitions or a
  global repair CPU budget) so anti-entropy can never starve foreground CQL/keepalive.
- **(cheaper digest)**: compare precomputed per-partition/per-SSTable hashes instead of re-reading
  every row body each cycle.
- **(efficiency)**: cache the `Arc<File>` handle on the SSTable reader instead of an
  `LruCache<PathBuf,_>` lookup (path hash) per `read_at`.
- **regression test**: assert an idle node's `cpu.stat usage_usec` growth stays near zero over a
  quiet interval.

## (superseded) Next step: CPU flamegraph (mirror the heap-profiling that nailed the OOM)
gdb `thread apply all bt` detaches after one frame in the slim runtime image (broken unwinder), and
the image has no `perf`. Get the exact spinning stack by either:
- building ferrosa with a `pprof`-crate CPU profiler exposed on the web port (flamegraph of an idle
  node), or
- `perf record -g` against the podman VM / a privileged sidecar.
Then fix the loop to park on a waker/notify/interval instead of spinning. Add a regression test/assert
that an idle node's CPU stays near zero (e.g. bounded `usage_usec` growth over a quiet interval).
