---
type: bug
priority: P3
reported-by: ferrosa-memory launch debugging
implemented-by: "codex"
verified-by: ""
created: 2026-04-18
updated: 2026-05-22
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
