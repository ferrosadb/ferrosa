---
type: todo
priority: P1
status: fixed
created: 2026-06-09
updated: 2026-06-09
affected-versions: ferrosa main @ 029544d0 (and earlier — present since the invite re-broadcast loop)
fixed-by: fix/cluster-invite-connect-storm
---

# Bug: ClusterInvite connect path storms unreachable peers → host-wide ephemeral-port exhaustion

## Symptom

A long-running node accumulated **~15,400 sockets stuck in `SYN_SENT`**, all
targeting its own internode port `127.0.0.1:17000`, owned by the `ferrosa`
process. This consumed the **entire** ephemeral port range
(49152–65535 ≈ 16k ports), so *every* new outbound connection on the **host**
— including unrelated tools (`gh`, `git`, curl to any site) — failed with
`Can't assign requested address` (`EADDRNOTAVAIL`). The node's log grew to
**2.28 GB**, dominated by:

```
WARN ferrosa_cluster::controller::cluster: cluster invite: failed to connect
  to discovered peer uuid=<...> e=I/O error: Can't assign requested address
```

with dozens of distinct phantom peer UUIDs. Observed on a single-node
`ferrosa-interlock` deployment after a laptop hibernation; only a `kill` of the
node cleared it (the sockets then drained in ~60–90 s — **no reboot needed**).

## Why this is a Ferrosa bug

`ClusterInviteHandler::handle` (`controller/cluster.rs`) does, per inbound
`ClusterInvite`:

1. Plan a connection for every offered peer that isn't currently *live*
   (`plan_invite_peer_connection` → `Connect`).
2. Spawn a fresh `PriorityPool::connect` (multiple lanes = multiple TCP
   connects) for each.
3. **Re-broadcast** the same invite to those peers.

For a peer that can *never* go live — a stale `host_id` whose address resolves
to a dead listener (here, many dead host_ids all reverse-mapping to this node's
own `127.0.0.1:17000`) — `has_live_peer` stays false, so it is re-planned as
`Connect` on **every** invite round. The connect path had **no per-peer
failure backoff**, and the re-broadcast keeps the rounds coming, so the connect
attempts compound into a self-amplifying storm. Once ephemeral ports run out,
the `EADDRNOTAVAIL` errors become *both* cause and symptom and the node cannot
recover on its own.

The bounded lane-reconnect machinery (`ferrosa-net/reconnect.rs`:
exponential backoff + dormant state) does **not** apply here — it governs
re-dialing of *already-established* `PriorityPool` lanes, whereas these peers
never establish a pool at all (`PriorityPool::connect` fails first).

Notably, invite *delivery* already has a per-peer cooldown
(`recent_reconnect_invites` + `reserve_reconnect_invite`,
`CLUSTER_RECONNECT_INVITE_COOLDOWN = 30s`); the invite *connect* path was simply
missing the equivalent guard.

## Fix

Apply the same per-peer cooldown to the connect path, with a dedicated map so
delivery and connect cooldowns don't interfere:

- `controller/mod.rs` — new field `recent_invite_connects: Mutex<BTreeMap<Uuid,
  Instant>>` (+ initialized at all three constructor sites).
- `controller/cluster.rs` — before spawning `PriorityPool::connect` for a
  discovered peer, `reserve_reconnect_invite(...)` against `recent_invite_connects`;
  if within `CLUSTER_RECONNECT_INVITE_COOLDOWN`, skip the dial this round. The
  controller is reached via the handler's existing `Weak<ModeController>`.
- `controller/invite.rs` — regression tests: 50 unreachable peers re-offered
  across 100 rounds yield 4 dial attempts each (cooldown-bounded), not 100;
  and a peer is re-allowed exactly once the window elapses.

Result: an unreachable discovered peer is dialed at most once per 30 s instead
of once per invite round, capping the connect rate well below ephemeral-port
exhaustion while still permitting recovery when the peer returns.

## Follow-ups (not in this fix)

- **Phantom-peer accumulation.** Why does a single node accumulate dozens of
  dead host_ids all pointing at its own internode address? Likely host_id churn
  / stale membership not being pruned. The cooldown contains the *symptom*; the
  membership hygiene is a separate investigation.
- **Unbounded WARN logging.** The 2.28 GB log shows the failed-connect warning
  is not rate-limited. Consider a sampled/rate-limited log for this line.
