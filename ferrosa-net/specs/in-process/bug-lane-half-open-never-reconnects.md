# Bug: a half-open lane never reconnects — a healthy peer is treated as reachable forever

**Severity:** High — a single wedged lane silently drops every RPC to that peer,
with no lane-loss event, until the *local* node restarts.
**Discovered:** 2026-10-09
**Component:** `ferrosa-net` — `lane_actor.rs` (actor loop + alive watcher),
`reconnect.rs` (`spawn_alive_watcher`), `pool.rs` / `peer.rs` (pool ownership)
**Status:** in-process — TDD not started

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

- [ ] `LaneHealth::record_success` clears the consecutive-failure run
- [ ] `LaneHealth::record_timeout` below the threshold does not condemn the lane
- [ ] reaching the threshold condemns the lane exactly once (not on every later failure)
- [ ] a success after a condemned-but-unswapped lane re-arms it
- [ ] interleaved successes and timeouts never condemn (only *consecutive* failures do)
- [ ] threshold is read from `FERROSA_NET_LANE_FAILURE_THRESHOLD`; unset ⇒ default
- [ ] malformed / zero / negative threshold falls back to the default and warns once
- [ ] the new knobs are independent: setting one malformed must not move the other
- [ ] control: the existing clean-close path still transitions via `ConnectionLost`
- [ ] control: `mark_failed_transitions_through_exhaustion_to_dormant` stays green (ordering unchanged)

## Config surface (user directive: sizing values are tunables, not constants)

Follow the existing `parse_positive` / `env_positive` pattern in `reconnect.rs`
so a malformed value warns once and falls back rather than panicking.

- `FERROSA_NET_LANE_FAILURE_THRESHOLD` — consecutive send failures before the
  lane is condemned as dead (default 3; `1` is the footgun setting that makes a
  single slow request tear the lane down).
- `FERROSA_NET_LANE_FAILURE_WINDOW_MS` — optional: only count failures within
  this window, so a slow trickle of timeouts hours apart never accumulates into
  a false positive.

## Open question for the operator

Should the recovery action be a full re-dial (current `ConnectionLost`
semantics) or a **health probe** first, so a peer that is merely slow (rather
than gone) is not disconnected? Re-dial is simpler and matches the existing
ladder; probing is kinder under GC pauses or host contention. This changes the
shape of the fix and should be decided before implementation.

## Not the cause (ruled out with evidence)

- **Identity refusal.** `SwapClient` refuses a peer whose `peer_host_id` differs
  from `expected_peer` and retries forever — a plausible permanent wedge. Live
  logs show **0** `identity mismatch` lines, and the peer *did* reconnect
  (`lane reconnected lane=Data …`), so this is not it.
- **Backoff/dormant ladder.** `slow-retry` count is 0; the ladder was never entered.
