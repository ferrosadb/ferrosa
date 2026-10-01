# BUG: Internode broadcast address frozen to startup IP — stale Raft membership after container IP churn

**Status**: **Verified — automated regression coverage landed (2026-09-30).** PR #86 fixed environment-variable hostname advertisement, PR #94 fixed lane reconnects, and the `fix/internode-verify` work (stacked on PR #479) landed the remaining two paths: the re-resolvable `FERROSA_INTERNODE_BROADCAST` hostname is now advertised in the handshake and committed as the node's own `NodeInfo.addr` (not the startup-resolved IP), and inbound reverse dialing uses the peer's advertised endpoint. The regression test `ferrosa-cluster::controller::tests::internode_broadcast_hostname_is_advertised_and_committed_not_a_frozen_ip` pins both halves: it drives a real `ferrosa-net` handshake to assert the advertised address is the hostname, feeds that advertisement through `membership::node_info_addr` to assert the committed membership entry is the hostname, and asserts dial-time resolution reaches the peer's CURRENT address. It was observed **RED** against the old advertisement (`Some("10.89.1.176:17000")`) and **GREEN** against the current code (`localhost:17000`), at commit `8897f3f3`. **The originally-requested live podman/docker IP-churn run was NOT performed** — this is its automated substitute (see Verification below).
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

### 2026-08-07 launchd recurrence: all identities routed to the seed

A three-node same-host cluster used distinct internode ports (`17000`, `17001`,
`17002`) and valid per-node `[internode].broadcast` values. After restart, node1
and node2 remained `pair/primary` while node3 recovered the three-member Raft
snapshot and became the sole leader. Node3's ring stored `127.0.0.1:17002` for
all three host IDs, so its successful-looking `ClusterInvite delivered` RPCs
looped back to node3 instead of reaching node1/node2.

The startup logs tied that topology collapse to two implementation gaps:

1. inbound handshakes logged `internode_broadcast=None` even though TOML set it;
2. the fallback reverse address combined the inbound IP with node3's own
   internode port, which is valid only when every peer uses the same port.

The launchd plist correctly selected a distinct TOML file per node. This was not
a launchd configuration or startup-order error.

## Operational mitigation (not a code workaround)

Pin static container IPs so the once-resolved broadcast address stays valid across restarts. Applied in
`ferrosa-memory/docker-compose.yml` (static `ipv4_address` per node on the `10.89.1.0/24` subnet). This is an
infra-level mitigation; the bug itself must be fixed here in `ferrosa-net`.

## Verification

### Code half (confirmed present at `8897f3f3`)

| Step | File:line | What it does |
| --- | --- | --- |
| Preserve the raw hostname at config load | `ferrosa-net/src/config.rs:56-61` (field), `:218-234` (set from `FERROSA_INTERNODE_BROADCAST`) | stores the unresolved `host:port` in `internode_broadcast` |
| Advertise it in the handshake | `ferrosa-net/src/handshake.rs:26` (field), `:81` (initiator), `:109,243` (acceptor `HandshakeAck`) | peers decode the hostname, not a resolved IP |
| Commit it as this node's own address | `ferrosa-cluster/src/controller/cluster.rs:1079,1101-1103,2241-2248` via `NetConfig::advertised_internode_addr` (`config.rs:130`) | seed/self `NodeInfo.addr` is the hostname |
| Store a peer's hostname on connect | `ferrosa-cluster/src/controller/peer_events.rs:524-531` | the peer-manager keeps the advertised hostname |
| Commit a peer's hostname, not the observed IP | `ferrosa-cluster/src/controller/membership.rs:29-37,65` (`node_info_addr`) | membership/token-ring entry re-resolves |
| Reverse-dial at the peer's advertised port | `ferrosa-cluster/src/controller/peer_events.rs:44-52` | inbound dialing no longer assumes a uniform port |
| Re-resolve on reconnect | `ferrosa-net/src/pool.rs:165,360-366` (`pick_reconnect_host`) | lane reconnect uses the hostname, not the connect-time IP |

### Automated regression test

`ferrosa-cluster::controller::tests::internode_broadcast_hostname_is_advertised_and_committed_not_a_frozen_ip`
(`ferrosa-cluster/src/controller/tests.rs`).

It drives a **real** `ferrosa-net` handshake over an in-memory duplex:

1. **Advertised half** — the node under test is configured with
   `internode_broadcast = Some("localhost:17000")` and a *different*
   `broadcast_addr = 10.89.1.176:17000` (the frozen startup IP). The peer decodes
   the `HandshakeAck` and asserts `internode_broadcast == Some("localhost:17000")`,
   i.e. the hostname — and explicitly **not** the startup IP.
2. **Committed half** — that advertisement is fed through
   `membership::node_info_addr(observed_current_addr, advertised)`; the committed
   `NodeInfo.addr` must equal the hostname, not the observed IP and not the
   startup IP, and must not be a parseable `SocketAddr` literal.
3. **Dial-time re-resolution** — `committed.to_socket_addrs()` must yield the
   peer's CURRENT address (`127.0.0.1:17000`), a different generation from the
   startup IP. The test ends with a non-vacuity guard proving the frozen
   `10.89.1.176:17000` literal resolves only to itself and therefore could never
   reach the current address.

### RED / GREEN evidence

- **RED** — with the advertisement temporarily reverted to the old behaviour
  (`send_handshake_ack(..., &Some(config.broadcast_addr.to_string()))`):
  `the handshake must carry the re-resolvable hostname, not a frozen IP;
  left: Some("10.89.1.176:17000") right: Some("localhost:17000")` → `1 failed`.
- **GREEN** — with the advertisement restored (`&config.internode_broadcast`):
  `test result: ok. 1 passed; 0 failed`.

### Scope of this verification

This is an **automated substitute** for the live-cluster run the item originally
requested. The repository's in-process Raft harness
(`ferrosa-cluster/tests/common/raft_harness.rs`) routes Raft RPCs over
`tokio::mpsc` channels and deliberately bypasses ferrosa-net/TCP, so it cannot
restart a node under a changed address; there is no automated harness that churns
container IPs. The test therefore pins the invariant the live run would exercise —
that the advertised address and the committed membership address are the
re-resolvable hostname, and that resolving it at dial time reaches the peer's
current address — at the two boundaries that gate the failure mode.

## Related

- `ferrosa-net/src/config.rs:13,84-94,125-128`
- `ferrosa-cluster/src/state.rs` (`BroadcastResolver`, `peer_broadcast`)
- `specs/components.md:383` (CQL broadcast hostname resolution — the behavior internode lacks)
- Possibly interacts with the bulk-lane starvation work (`bug-bulk-write-raft-starvation.md`): a stale
  address makes the Bulk lane timeout immediately instead of under load.
