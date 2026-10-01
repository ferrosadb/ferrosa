---
crate: ferrosa-net
doc: fmea
last_updated: 2026-09-09
---

# ferrosa-net — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This crate sits on the critical internode path: a transport
fault can stall consensus or silently drop cluster traffic, so severities run
high. Several entries are regression-hardened by existing tests; those that are
not are flagged as open gaps.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| NET-1 | A `MsgType` byte tag is added to the enum but not to the `TryFrom<u8>` decoder | Peers serialise the frame fine but reject it on receipt with `UnknownMessageType` — e.g. repair sessions converge **zero** partitions, silently | 9 | 4 | 7 | 252 | **Partially mitigated.** A regression test pins the repair tags `0x29..=0x2E` and the streaming tags `0x36..=0x3A`, but there is **no exhaustive test** asserting every declared `MsgType` round-trips. New tags can still slip through. Add an enumerate-all round-trip test. |
| NET-2 | Lane actor holds work behind a network round-trip and the caller future is cancelled | Half-sent state / leaked in-flight slot / wrong correlation | 9 | 2 | 6 | 108 | **Structural.** Lane state is owned by one actor; callers use `reserve().await` + `permit.send()`, so cancellation drops the permit cleanly. Central design invariant; covered by lane_actor tests. |
| NET-3 | Frame format mismatch (legacy frame on a Cap'n Proto connection or vice versa) | Body misparse → corrupt `Message` or hung stream | 8 | 2 | 4 | 64 | **Mitigated.** `InternodeCodec::decode` checks `header.version` against the negotiated `WireFrameFormat` and returns a descriptive `Protocol` error rather than misparsing. Covered by `capnp_envelope_framing` tests. |
| NET-4 | Lane reconnect pinned to a frozen IP; peer restarts with a new container IP | Lane retries a dead address forever; peer never rejoins (P3) | 8 | 3 | 5 | 120 | **Mitigated.** `pick_reconnect_host` prefers the peer's advertised re-resolvable internode-broadcast hostname; DNS re-resolved on every attempt. Unit-tested. Residual risk: peers that advertise no broadcast still pin the connect-time host. |
| NET-5 | Handler not yet registered when a Raft/vote frame arrives during mode transition | `dispatch` returns `None`; sender times out; election stalls (BUG-RAFT-HANDLER-RACE) | 8 | 3 | 5 | 120 | **Open / cross-crate.** `HandlerRegistry` supports dynamic registration, but the registration-vs-arrival race is owned by `ferrosa-cluster`. A test in `rpc/handler.rs` documents the bug (asserts the drop) but the fix is upstream. Track closure. |
| NET-6 | All lanes to a peer go `Dormant` after exhausting the fast reconnect cycles | No traffic to that peer until a slow-retry probe succeeds | 7 | 3 | 4 | 84 → 28 | **By design, observable (revised t_48d168ee).** `Dormant` is the indefinite slow-retry phase: one single-attempt probe per `slow_retry_interval()` (default 30 s + up to 25% jitter, `FERROSA_NET_RECONNECT_SLOW_INTERVAL_MS`), so recovery lags a returning peer by at most ~40 s instead of the former 5-minute probe + 3.5-minute probe cycle. `inc/dec_dormant_peer_count` metrics expose it. See NET-14. |
| NET-7 | Inbound flood: many half-open connections or oversized frames | Resource exhaustion / OOM | 7 | 2 | 4 | 56 | **Mitigated.** `max_connections` (512), `handshake_timeout` (5 s), and `max_frame_body_size` (256 MiB, enforced as `FrameTooLarge`) bound the surface. |
| NET-8 | `require_tls=false` (default) → internode traffic is plaintext | Eavesdrop / MITM on the internode network if operator forgets to enable TLS | 8 | 4 | 6 | 192 | **Fail-loud only when opted in.** `require_tls` errors at startup when set with no cert, but TLS is **off by default** and there is no mutual-TLS client-auth (`with_no_client_auth`). Operators must explicitly enable + provide a CA. Document as a deployment gap. |
| NET-9 | PSK unset (default `psk: None`) → handshake authenticates cluster-name only | Any host knowing the cluster name can join the internode mesh | 8 | 3 | 6 | 144 | **Optional auth.** HMAC-SHA256 token verification is constant-time and correct *when a PSK is set*, but PSK is `None` by default. Pair with NET-8: secure internode requires both PSK and TLS configured. |
| NET-10 | Streaming chunk frames dispatched out of wire order | Coordinator's contiguous-`seq` check trips → `ChannelClosedBeforeDone` mid-stream | 7 | 2 | 5 | 70 | **Mitigated.** `is_ordered_stream_response` keeps chunk/heartbeat/done on the ordered lane path; documented at length in `codec.rs`. Surfaced only for multi-chunk responses (wide partitions). |
| NET-11 | A lane remained `Connected` after its TCP client died while background reconnect was already running. | New Raft traffic was dispatched to the closed writer, producing repeated `connection closed` errors and `Raft lane timeout` backoff during Fly/OpenRaft peer disruptions. | 8 | 4 | 4 | 128 → 16 | **Fixed (2026-09-09):** the alive watcher transitions the lane to `Reconnecting` before reconnect backoff; closed-writer sends remove pending slots and signal the watcher. Regression covers drop, transient rejection, and recovery of all three lanes. |
| NET-12 | An RPC response frame the codec rejects (`FrameTooLarge`) or a body that fails to decode | The read loop ended (or dropped the response) without a log line, and the caller waiting on it failed only at its lane timeout — indistinguishable from a slow peer | 6 | 2 | 8 | 96 → 48 | **Partly mitigated:** the client read loop logs the stream error or decode failure at ERROR with the peer (and stream id / message type). The waiting request still fails at its lane timeout rather than when its connection dies — open. Exercised by `ferrosa-cluster` `tests/fulltext_replica_bulk_lane_budget.rs::replica_response_near_the_frame_limit_is_delivered_and_over_it_fails_the_query`. |
| NET-13 | `PeerManager` resolved a peer's pool, then `add_peer` replaced it and shut its lane actors down before the request was sent | The request failed with a closed-channel error reported as "lane permanently failed after max reconnection attempts" — wrong text (exhaustion goes `Dormant`, never fails), and the caller treated a healthy peer as dead. Bursts of 128 DDL-forward errors within 16 s of a staggered 3-node start | 7 | 5 | 3 | 105 → 21 | **Fixed (2026-09-29):** the error is now `NetError::LaneShutdown` ("lane actor shut down (connection replaced or closed)"); `PeerManager::on_current_pool` re-issues the request once on the current pool when the pool was replaced, and logs ERROR and surfaces the error when the registered pool is itself dead. Covered by `peer::tests::request_in_flight_across_pool_replacement_lands_on_new_pool` and `dead_current_pool_surfaces_lane_shutdown_without_retry_loop`. `remove_peer` now shuts the pool's actors down (t_b4d09b65; `peer::tests::remove_peer_shuts_down_the_pool_lane_actors`). **Fixed (t_a3df19a5):** a dead registered pool is deregistered and replaced by one identity-checked re-dial (serialised by `replace_lock`, single attempt per request, failure and recovery logged once); a failed re-dial errors the request and leaves a pool-less placeholder. Covered by `dead_registered_pool_is_replaced_and_request_succeeds` and `failed_replacement_errors_and_deregisters_the_dead_pool`. **Residual closed by NET-14:** the pool-less placeholder is now re-dialed by the heartbeat loop. |
| NET-14 | A peer is unreachable longer than the fast-retry budget (~10 min) and its node then returns | Observed live twice (2026-09-30): after ~30 min of node2 restoring from R2, node2 had 3 lanes to node1 and ZERO to node3 and reported `cluster member without quorum ... waiting_for: raft_quorum`; only a process restart recovered it. Three gaps: (1) a lane's death was lost when nothing was subscribed to the client's `alive` watch (`watch::Sender::send` drops the value without receivers; the lane actor subscribes after the client is built, and a reconnected client waits in the actor mailbox), leaving the lane `Connected` on a dead client forever; (2) a peer whose pool replacement failed kept a pool-less placeholder that nothing re-dialed; (3) the dormant probe was a full 10-attempt cycle every 5 minutes | 8 | 5 | 4 | 160 → 24 | **Fixed (t_48d168ee).** (1) `RpcClient` records death with `send_replace` and `spawn_alive_watcher` checks the current value before waiting (`slow_retry_tests::client_death_with_no_subscriber_is_visible_to_a_late_subscriber`, `reconnect::tests::alive_watcher_fires_when_connection_died_before_it_ran`). (2) `PeerManager`'s heartbeat loop re-dials pool-less placeholders (`PeerState::awaiting_redial`): identity-checked via `dial_verified`, backoff `redial_delay` (1 s doubling to the slow interval, jittered), one task per peer, stops when the peer is removed, and `install_pool` refuses a dial that completes after removal or replacement (`peer::tests::pool_less_peer_is_redialed_when_its_node_returns`, `removed_pool_less_peer_is_not_redialed_or_resurrected`, `install_after_removal_is_refused_and_leaves_no_trace`, `redial_refuses_an_address_owned_by_another_node`). (3) Slow phase is a single attempt per interval, forever (`slow_retry_tests::lane_reconnects_after_outage_longer_than_fast_budget`). Lanes also refuse a reconnect answered by a different host id (`reconnect_to_address_now_owned_by_another_node_is_refused`). Logging is edge-only (`slow_retry_logs_the_drop_and_the_recovery_once_each`); a shut-down lane stops dialing and leaves no task (`shutdown_during_slow_retry_stops_attempts_and_leaks_no_task`). **Residual:** the lane-level `MarkFailed`/probe signals use `try_send`, which logs ERROR but would strand a lane whose 256-slot mailbox is full at that instant; not observed. Fast-phase per-cycle WARN lines (two per outage) remain. |

## Top risks to act on

1. **NET-1 (RPN 252)** — the highest risk is a *non-exhaustive* `MsgType`
   round-trip test. The failure mode (peers silently dropping a whole class of
   frames) has already bitten repair. Add a test that iterates every declared
   variant through `TryFrom<u8>` so a new tag cannot ship undecodable.
2. **NET-8 (RPN 192) + NET-9 (RPN 144)** — secure-by-default gap: internode TLS
   and PSK auth are both **off by default**, and there is no mutual TLS. For any
   untrusted-network deployment this is a real exposure; capture as a hardening
   item and document the required `FERROSA_INTERNODE_TLS_*` + `FERROSA_INTERNODE_PSK`
   configuration.
3. **NET-5 (RPN 120)** — the handler-registration race is documented but the fix
   lives in `ferrosa-cluster`; track to closure so the documented bug-asserting
   test can be flipped to assert success.

## Detection assets

- `codec.rs` unit tests — frame round-trip, oversize rejection, repair- and
  streaming-tag round-trips, trace-context propagation.
- `tests/capnp_*` — Cap'n Proto envelope encode/decode, conformance, framing,
  version/feature negotiation.
- `handshake.rs` tests — PSK accept/reject, cluster/version mismatch, broadcast
  exchange, backward compat.
- `lane_actor.rs` / `pool.rs` tests — reconnect-host selection, exhaustion →
  dormant transition, stale `MarkFailed` rejection, cancel-safe send.
- `tests/reconnect_backoff.rs`, `tests/integration.rs` — end-to-end reconnect and
  send/receive.
- `metrics` — lane queue depth, in-flight RPCs, RPC timeouts, dormant peer count.
