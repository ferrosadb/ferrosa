---
crate: ferrosa-net
doc: fmea
last_updated: 2026-06-19
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
| NET-6 | All lanes to a peer go `Dormant` after exhausting reconnects | No traffic to that peer until a 5-minute dormant probe succeeds; slow recovery | 7 | 3 | 4 | 84 | **By design, observable.** Dormancy bounds reconnect cost; `inc/dec_dormant_peer_count` metrics expose it. Risk is the 5-minute `DORMANT_PROBE_INTERVAL` latency on a genuine transient outage. |
| NET-7 | Inbound flood: many half-open connections or oversized frames | Resource exhaustion / OOM | 7 | 2 | 4 | 56 | **Mitigated.** `max_connections` (512), `handshake_timeout` (5 s), and `max_frame_body_size` (256 MiB, enforced as `FrameTooLarge`) bound the surface. |
| NET-8 | `require_tls=false` (default) → internode traffic is plaintext | Eavesdrop / MITM on the internode network if operator forgets to enable TLS | 8 | 4 | 6 | 192 | **Fail-loud only when opted in.** `require_tls` errors at startup when set with no cert, but TLS is **off by default** and there is no mutual-TLS client-auth (`with_no_client_auth`). Operators must explicitly enable + provide a CA. Document as a deployment gap. |
| NET-9 | PSK unset (default `psk: None`) → handshake authenticates cluster-name only | Any host knowing the cluster name can join the internode mesh | 8 | 3 | 6 | 144 | **Optional auth.** HMAC-SHA256 token verification is constant-time and correct *when a PSK is set*, but PSK is `None` by default. Pair with NET-8: secure internode requires both PSK and TLS configured. |
| NET-10 | Streaming chunk frames dispatched out of wire order | Coordinator's contiguous-`seq` check trips → `ChannelClosedBeforeDone` mid-stream | 7 | 2 | 5 | 70 | **Mitigated.** `is_ordered_stream_response` keeps chunk/heartbeat/done on the ordered lane path; documented at length in `codec.rs`. Surfaced only for multi-chunk responses (wide partitions). |

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
