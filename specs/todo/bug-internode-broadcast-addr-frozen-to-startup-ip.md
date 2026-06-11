# BUG: Internode broadcast address frozen to startup IP — stale Raft membership after container IP churn

**Status**: Broadcast-hostname fix **merged** (PR #86, advertise + re-resolve the broadcast hostname — option 1). Follow-up for the lane-reconnect path — lanes reconnecting to a stale resolved IP instead of the advertised hostname — fixed in **PR #94** (`ferrosa-net` `pick_reconnect_host`, unit-tested). **Live-cluster verification still pending** on a real podman cluster; should move to `specs/verified-test-plan/` once verified, then to `archive/`. (2026-06-09)
**Component**: `ferrosa-net` (config), `ferrosa-cluster` (Raft membership / internode routing)
**Severity**: High — silent read-path degradation in any environment where node IPs change across restarts (podman/docker default networking, k8s pods, DHCP).
**Found**: 2026-06-05, debugging a live `ferrosa-memory` 3-node dev cluster.

## Symptom

A 3-node cluster reports **0 entities** but a partial edge count (~13k of an expected ~68k) through the
ferrosa-memory MCP. Data is fully intact on disk (`agent_memory.entity_store` = 920 MB). Reads are silently
degraded, not failing loudly.

MCP-side log (node addressed by host_id, via a dead IP):

```
smart_ingest: cross-session exact dedup lookup failed ... error=server error: cluster error: internal:
  index read from node 1229782938247303441 (11111111-1111-1111-1111-111111111111) via 10.89.1.176:7000:
  net: timeout: Bulk lane timeout
viz: failed to stream entities for snapshot: ... streaming range read:
  ChannelClosedBeforeDone { delivered_done: 0, expected_done: 1 }
```

- `11111111-1111-1111-1111-111111111111` is node1 (pinned `FERROSA_HOST_ID`).
- `10.89.1.176` is a **stale** address. node1 is currently at `10.89.1.58`. `10.89.1.176` matches no
  running container.
- Distributed **index reads** and **streaming range reads** route to the stale committed IP and time out;
  most other internode RPC works because it resolves the broadcast DNS name live. Entity range scans depend
  on the failing paths, so the entity count bottoms out at 0.

### Evidence: the Raft DB is full of dead addresses

Scanning each node's committed Raft state for `N.N.N.N:7000` literals:

```
$ podman exec ferrosa-memory_node1_1 grep -ao -E '10\.89\.1\.[0-9]+:7000' /var/lib/ferrosa/raft/datacenter1/db | sort | uniq -c
```

returns **~50 distinct IPs** (`10.89.1.4` … `10.89.1.202`) across all three nodes' DBs, and **none of the
current IPs** (`.58/.60/.62`) appears anywhere. The committed membership has been chasing container IP churn
and never converges on the live addresses.

## Root cause

`ferrosa-net/src/config.rs`:

```rust
pub struct NetConfig {
    ...
    /// Address advertised to peers (defaults to bind_addr).
    pub broadcast_addr: SocketAddr,   // line 13 — a RESOLVED IP:port, not a hostname
    ...
}

fn parse_socket_addr(raw: &str) -> Option<SocketAddr> {     // lines 84-94
    let trimmed = raw.trim();
    if let Ok(addr) = trimmed.parse() { return Some(addr); } // already an IP
    let mut resolved = trimmed.to_socket_addrs().ok()?;      // hostname -> IP, ONCE, at startup
    resolved.next()
}

// lines 125-128
if let Ok(v) = std::env::var("FERROSA_INTERNODE_BROADCAST") {
    if let Some(addr) = Self::parse_socket_addr(&v) {
        cfg.broadcast_addr = addr;   // frozen resolved IP
    }
}
```

`FERROSA_INTERNODE_BROADCAST=node1:7000` is resolved to an IP exactly once at boot and stored as a
`SocketAddr`. That IP is advertised to peers and committed into the openraft membership. When the container
restarts with a new IP, the hostname is never re-resolved against the committed entry, so the membership
keeps pointing at the previous generation's address. The internode index/range-read routing path uses the
committed IP literal rather than re-resolving the broadcast hostname.

This contrasts with `FERROSA_CQL_BROADCAST`, which `specs/components.md:383` documents as supporting
hostname resolution for `system.peers`. The internode broadcast has no equivalent re-resolution.

## Impact

Any deployment where a node's IP can change while its `host_id` is stable:
- podman/docker default bridge networking (IPs assigned from a pool per `up`)
- Kubernetes pods (new IP per reschedule)
- DHCP-leased hosts

The failure is **silent**: containers stay "healthy" (TCP probe passes), most RPC works, but distributed
index/range reads time out and counts/scans under-report. This violates the project's fail-loud principle —
a stale-membership read should surface as an error, not a 0 count.

## Suggested fix

1. **Store the broadcast as a resolvable target, re-resolve at connect time.** Keep the configured host:port
   string and resolve it when establishing/refreshing an internode connection (and when committing membership),
   rather than freezing an IP at startup. Mirror the CQL broadcast hostname-resolution behavior.
2. **Or: re-announce on startup.** On boot, if the node's resolved broadcast address differs from its committed
   membership address, commit a membership update so peers learn the current address.
3. **Fail loud on stale membership.** An index/range read that times out against a membership address should
   distinguish "peer unreachable at recorded address" from "empty result" so callers don't silently see 0 rows.
4. **Periodic membership address reconciliation** for long-lived clusters with DHCP/pod churn.

## Reproduction

1. Bring up a multi-node cluster using `FERROSA_INTERNODE_BROADCAST=<hostname>:7000` with pinned `FERROSA_HOST_ID`s
   on a network that assigns dynamic IPs (podman/docker default bridge).
2. `podman compose down && up` (or otherwise recreate containers) several times so each node gets a new IP.
3. Run a distributed index/range read (e.g. an ANN search or full entity range scan).
4. Observe `Bulk lane timeout` / `ChannelClosedBeforeDone` against a stale `N.N.N.N:7000` address, and
   under-reported counts, while `nodetool`-style health stays green.

## Operational mitigation (not a code workaround)

Pin static container IPs so the once-resolved broadcast address stays valid across restarts. Applied in
`ferrosa-memory/docker-compose.yml` (static `ipv4_address` per node on the `10.89.1.0/24` subnet). This is an
infra-level mitigation; the bug itself must be fixed here in `ferrosa-net`.

## Related

- `ferrosa-net/src/config.rs:13,84-94,125-128`
- `ferrosa-cluster/src/state.rs` (`BroadcastResolver`, `peer_broadcast`)
- `specs/components.md:383` (CQL broadcast hostname resolution — the behavior internode lacks)
- Possibly interacts with the bulk-lane starvation work (`bug-bulk-write-raft-starvation.md`): a stale
  address makes the Bulk lane timeout immediately instead of under load.
