# Bug: a half-open lane never reconnects — a healthy peer is treated as reachable forever

**Severity:** High — a single wedged lane silently drops every RPC to that peer,
with no lane-loss event, until the *local* node restarts.
**Discovered:** 2026-10-09
**Component:** `ferrosa-net` — `lane_actor.rs` (actor loop + alive watcher),
`reconnect.rs` (`spawn_alive_watcher`), `pool.rs` / `peer.rs` (pool ownership)
**Status:** in-process — probe-first detection implemented and unit-tested;
**the accumulation window that hid sparse failures has been removed**; live
verification (2026-10-27) **found that a condemned lane never redials and leaks
its socket** — fixed with actor-level tests (see Implementation Notes
2026-10-27); live re-verification of the fix pending

## Symptom

On a live 3-node cluster, after a rolling restart, the coordinator held a lane
to one peer that reported **every** RPC as timed out while the lane actor still
believed it was `Connected`:

```
WARN raft::network: AppendEntries send failed target=0b8e557e-… e=timeout: Raft lane timeout
WARN openraft::replication: heartbeat error: Unreachable node: … timeout: Raft lane timeout
WARN accord::coordinator: quorum broadcast RPC failed … error=timeout: Data lane timeout peer=0b8e557e-…
```

Counts over the same window on the coordinator:

| event                    | count |
|--------------------------|-------|
| `lane connection lost`   | **0** |
| `lane reconnected`       | **0** |
| `Data lane timeout`      | 5 (all the same peer) |

The peer never received the dropped RPCs. Consequently it never registered the
transactions it was ordered to wait for, so its apply-side dependencies were
permanently `absent`, it refused `ApplyOK`, the coordinator's apply quorum
failed, and every PostgreSQL transaction stalled (they share one barrier key).

## Why the existing reconnect machinery does not cover this

`lane_actor_loop` only leaves `Connected` on `LaneCommand::ConnectionLost`,
which is sent by the alive watcher (`spawn_alive_watcher` → `mark_reconnecting`).
That watcher watches `RpcClient::alive_rx`, a `watch` channel the client sets to
`false` when **its own read path observes a failure**. A half-open connection —
peer gone, no FIN/RST, no further traffic — produces no such observation: the
socket is writable, so writes succeed locally and the request simply dies at the
timeout. No death signal ⇒ no `ConnectionLost` ⇒ no `spawn_reconnect` ⇒ the lane
stays `Connected` forever while every RPC times out.

Per-RPC timeouts are counted in metrics (`record_rpc_timeout`) but are **not fed
back into lane health**, so the actor never acts on them.

The backoff/dormant/slow-retry ladder in `reconnect.rs` is correct; it is simply
never entered.

## Desired behaviour

An RPC timeout is evidence the connection is dead. After a bounded run of
consecutive send failures on a lane, the actor must treat the connection as
lost — the same transition `ConnectionLost` performs — so the existing backoff /
dormant ladder takes over. Recovery from a *healthy* peer must never be
triggered by a single slow request.

## Test list

Pure-logic first, following the `seeds_to_connect` precedent: extract the
decision into a function that takes state and returns a decision, so the actor
loop's policy is testable without I/O.

Done (10 tests, pure logic, no sockets):

- [x] below the threshold takes no action
- [x] reaching the threshold asks for a **probe**, not a disconnect
- [x] a failed probe returns `Reconnect` and condemns the lane
- [x] a probe that answers keeps a slow peer connected and re-arms the counter
- [x] never re-probes or re-reconnects while one is already in flight (a stale probe result is ignored)
- [x] only *consecutive* failures count — a success clears the run
- [x] interleaved successes never probe
- [x] ~~failures spread beyond the window do not accumulate~~ **REVERSED** — the
      window silently disarmed the guard for exactly this failure mode; the test now
      asserts `lane_health_sparse_failures_still_accumulate` (see Implementation Notes)
- [x] failures inside the window do accumulate
- [x] a zero window means no time limit (the guard is not permanently disarmed)
- [x] control: `mark_failed_transitions_through_exhaustion_to_dormant` stays green

Done (env + independence):

- [x] each knob honours its env var, falls back on `""` / `abc` / `0` / `-1` / `1.5` /
      `NaN` rather than panicking, and recovers once a valid value returns
- [x] the three knobs are independent — a malformed value for one does not move the others
      (test holds a process-wide `Mutex` and takes the `serial` key: a mutex alone cannot
      stop another env-reading test observing a half-set environment)

Still to do:

- [ ] **live verification**: wedge a lane on a real cluster and watch it probe,
      condemn, reconnect, and re-drive the dropped Applies

## Config surface (user directive: sizing values are tunables, not constants)

Follow the existing `parse_positive` / `env_positive` pattern in `reconnect.rs`
so a malformed value warns once and falls back rather than panicking.

All three follow the existing `parse_positive` / `env_positive` pattern, so an
unusable value warns once and falls back rather than panicking.

- `FERROSA_NET_LANE_FAILURE_THRESHOLD` — consecutive send failures within the
  window before the lane is probed (default 3; `1` is the footgun setting that
  probes on a single slow request).
- `FERROSA_NET_LANE_FAILURE_WINDOW_MS` — **NO LONGER CONSULTED.** A time window is
  the wrong predicate: a half-open lane fails on every use while its traffic may be
  sparse, so an idle gap is not evidence of recovery. Kept wired but inert; removing
  it (with the ignored second parameter of `LaneHealth::new`) is a follow-up.
- `FERROSA_NET_LANE_PROBE_TIMEOUT_MS` — how long a probe waits for a reply before
  calling the peer unreachable (default 2000).

## Decision: probe first

A wedged lane is **probed before it is disconnected**. A peer that is merely slow
— a GC pause, host contention — answers the probe and keeps its connection; only
a probe that fails hands the lane to the ladder. Detection is therefore
two-stage, and the pure value exposes it as `LaneAction::{None, Probe,
Reconnect}` rather than a bool.

Lane RPC and per-host pooling stay **separate**: they carry different priorities,
so routing lane RPCs through `PriorityPool` is explicitly not part of this fix.

## Not the cause (ruled out with evidence)

- **Identity refusal.** `SwapClient` refuses a peer whose `peer_host_id` differs
  from `expected_peer` and retries forever — a plausible permanent wedge. Live
  logs show **0** `identity mismatch` lines, and the peer *did* reconnect
  (`lane reconnected lane=Data …`), so this is not it.
- **Backoff/dormant ladder.** `slow-retry` count is 0; the ladder was never entered.

## Implementation Notes (2026-10-11) — the window is WHY the ladder was never entered

The detection above was correct but **could not fire for this failure mode**, and
this item's own evidence already showed it: *5 timeouts to the same peer* against a
threshold of *3*, yet **`slow-retry` count 0 — the ladder was never entered**.

`LaneHealth::record_failure` cleared the whole failure run whenever a failure
arrived more than `window` (default 30 s) after the run's *first* failure. Elapsed
time is not evidence that a connection recovered — only a successful round trip is,
which is what `record_success` records. A half-open lane fails on EVERY use, but a
Raft heartbeat to that peer only leaves every few minutes, so each failure reset the
run to 1 and the threshold of 3 was never reached: never probed, never condemned,
never re-dialled — the live ~20-minute `timeout: Raft lane timeout` loop.

**Fix:** a run is cleared ONLY by `record_success`. The slow peer the window was
meant to protect is protected by the **probe** instead, which is what the two-stage
design always intended — `lane_health_probe_success_keeps_a_slow_peer` and
`lane_health_interleaved_successes_never_probe` both still pin that.

**Tests:** red first —
`reconnect::lane_health_sparse_traffic_tests::a_sparse_but_unbroken_run_of_failures_still_reaches_the_probe`
failed `left: None, right: Probe` against the unmodified policy: a behavioural red at
the real decision point, and simultaneously the control on the diagnosis.
`lane_health_failures_outside_the_window_do_not_accumulate` **pinned the defect**, so
it was rewritten to the corrected contract with its assertion strengthened (never
relaxed). `ferrosa-net`: 237 passed / 0 failed; `clippy --all-targets --all-features
-D warnings` clean; `cargo fmt --check` clean.

Still outstanding:

- [ ] **live verification**: wedge a lane on a real cluster and watch it probe,
      condemn, reconnect, and re-drive the dropped Applies. Unchanged — the fly
      `machine exec` channel was too unreliable to drive it in this session.
- [ ] **remove the inert knob**: the now-ignored second parameter of
      `LaneHealth::new`, `lane_failure_window()`, `DEFAULT_LANE_FAILURE_WINDOW_MS`,
      `ENV_LANE_FAILURE_WINDOW_MS`, and the ~10 test call sites that pass it.

## Implementation Notes (2026-10-27) — a condemned lane never dialled again (t_dbcde486)

**Live verification found the "Desired behaviour" was never wired.** On the local
3-node dev cluster (v0.29.0) every Raft lane on all three nodes logged `lane probe
failed; the peer is unreachable, reconnecting` → `entering reconnecting state`
within 3 s of each other (18:01:31–34Z), ~5 min after a rolling restart, and then
**no `lane reconnected` for hours**: every RPC answered `lane is reconnecting;
retry later`, the metadata Raft group ran an election storm (node2 elected and
stepping down on CheckQuorum, nodes 1/3 triggering elections every few seconds),
~3k ERROR lines per 20 min, while Data/Bulk and ordinary reads kept working.
node1 also held **15** sockets to node3 where its 3 lanes need 3.

**Cause — the condemn path copied `ConnectionLost`'s transition but not its dial.**
`begin_reconnect` only sets `state = Reconnecting`. On the alive-watcher path that
is fine because the watcher closure has already called `spawn_reconnect(0)`; on the
condemn paths (`SendComplete`/`FireComplete` → `LaneAction::Reconnect`, and
`ProbeFailed`) nothing ever dialled, so no `SwapClient`/`MarkFailed` could arrive
and the lane was stranded in `Reconnecting` for good. Second defect: the condemned
`RpcClient` was merely dropped, but the client is cloned per request and its read
loop owns the socket's read half, so against a peer that is alive-but-stalled the
socket and its task leaked forever.

**Fix:** `condemn_lane` closes the condemned client (`RpcClient::close`, a
`CancellationToken` both I/O loops select on), enters `Reconnecting`, and calls
`spawn_reconnect(0)`. `watch_connection` (now shared by both watcher sites) ignores
a connection the actor closed itself, so one condemnation is one redial and a stale
`ConnectionLost` cannot condemn the replacement. `LaneState::Connected` now boxes
the client (clippy `large_enum_variant` after the new field).

**Tests (actor level, real loopback peer that handshakes then never answers):**
red first against v0.29.0 —
- `a_condemned_lane_redials_the_peer_and_reconnects`: failed `left: 1, right: 2`
  handshakes, status `Reconnecting`.
- `a_condemned_lane_closes_the_connection_it_gave_up_on`: failed `left: 0, right: 1`
  (the peer never read EOF).
- `a_condemned_lane_redials_exactly_once`: guard for the watcher interplay; negative
  control — disabling the `closed_locally` check made it fail `left: 3, right: 2`.
`ferrosa-net`: 286 passed / 0 failed; clippy `--all-targets --all-features -D
warnings` clean; fmt clean.

Still outstanding:

- [ ] live re-verification on the dev cluster with the fixed binary: a probe-failed
      lane logs `lane reconnected` and Raft keeps a stable leader past the ~5 min mark
- [ ] why every Raft probe failed at once ~5 min after the roll (the trigger, as
      opposed to the missing recovery) — not yet explained
