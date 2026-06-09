---
type: bug
priority: P3
reported-by: ferrosa-memory launch debugging
implemented-by: "codex"
verified-by: "unit (pool::tests::reconnect_*); live-cluster verification pending"
created: 2026-04-18
updated: 2026-06-08
---

# Lane reconnect may use stale IP when peer container restarts with new IP

## Current State

The reconnect code (`ferrosa-net/src/reconnect.rs:122`) correctly re-resolves
DNS via `tokio::net::lookup_host(peer_host)` on every retry attempt. The
`peer_host` field in `ActorReconnectContext` is stored as a string (not a
resolved `SocketAddr`) specifically for this purpose.

## Gap

When the cluster controller passes `peer_host` as an IP address string
(e.g., `"10.89.0.4:7000"` from a `SocketAddr::to_string()`), DNS
re-resolution is a no-op — the IP is already resolved. If the peer
container restarts with a different IP (common in podman/docker networks),
the lane retries the old IP indefinitely.

## Fix Direction

The cluster controller should pass the container hostname (from the
compose service name or Raft node configuration) instead of the resolved
IP. Alternatively, the PeerManager could maintain a mapping from host_id
to current hostname and update it when peers reconnect from new IPs.

## Resolution (2026-06-08)

Fixed at the `ferrosa-net` pool layer rather than chasing every controller call
site (`cluster.rs`, `pair.rs`, `membership.rs`, `peer_events.rs` all pass
`SocketAddr::to_string()`). `PriorityPool::connect` already learns the peer's
advertised, re-resolvable `internode_broadcast` hostname from the handshake
(`raft_client.peer_internode_broadcast()`). It now wires that hostname — not the
connect-time address — into each lane's `ActorReconnectContext.peer_host` via
`pick_reconnect_host()`, falling back to the connect-time host when the peer
advertised no usable broadcast. So a reconnect re-resolves the hostname and
follows the peer to its new IP, regardless of which caller initiated the pool.

This depends on peers advertising a hostname (not an IP) as
`FERROSA_INTERNODE_BROADCAST` — which the ferrosa-memory compose already does
(`node1:7000`). A peer that advertises an IP (or nothing) keeps the prior
behavior.

- Code: `ferrosa-net/src/pool.rs` (`pick_reconnect_host` + `connect` wiring).
- Tests: `pool::tests::reconnect_prefers_advertised_broadcast_hostname_over_connect_ip`,
  `pool::tests::reconnect_falls_back_to_connect_host_without_usable_broadcast`.
- Live verification still pending: restart one node so its container IP changes
  and confirm peers reconnect without a stale `:7000` `Bulk lane timeout`.

## Impact

P3 — only affects container restarts with IP reassignment. The workaround
is to restart the connecting node too, or use static IPs in the compose
network.

## Implementation Notes

- Updated cluster peer tracking so reconnects replace an existing peer's
  address instead of preserving the first observed container IP forever.
- Added `ClusterInvite` connection planning that refuses to downgrade an
  already-live peer to a conflicting stale address advertised by another node.
- Added regression tests for stale `connected_peers` reuse and stale invite
  address downgrades.
