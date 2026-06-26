---
crate: ferrosa-net
doc: data-flow
last_updated: 2026-06-19
---

# ferrosa-net — Data Flow

This crate has two hot paths: **connection establishment** (TCP → handshake →
lane actors) and **per-message send/receive** (`LaneHandle` → actor → wire →
`HandlerRegistry`). The sequence below traces both for a single peer.

> Diagram note: generic types are written with escaped brackets
> (e.g. `mpsc::Sender&lt;LaneCommand&gt;`) so the Mermaid renderer does not treat
> them as markup.

```mermaid
sequenceDiagram
    autonumber
    participant Caller as Caller (ferrosa-cluster)
    participant Pool as PriorityPool
    participant Client as RpcClient (per lane)
    participant Codec as InternodeCodec
    participant Net as TCP / TLS
    participant Server as RpcServer (peer)
    participant Reg as HandlerRegistry (peer)
    participant Actor as lane_actor (local)

    Note over Pool,Net: 1. Connect (one of 3 lanes: Raft / Data / Bulk)
    Caller->>Pool: connect(config, host)
    Pool->>Net: lookup_host + TCP connect
    Pool->>Client: connect_with_tls_on_pool
    Client->>Codec: Frame{ Handshake, lane=Raft }
    Codec->>Net: 44-byte header + body
    Net->>Server: bytes
    Server->>Server: accept_handshake (verify cluster, version, HMAC-SHA256 token)
    Server-->>Client: HandshakeAck{ accepted, host_id, broadcasts }
    Client-->>Pool: HandshakePeer
    Pool->>Actor: spawn lane_actor (owns LaneState, mpsc::Receiver&lt;LaneCommand&gt;)
    Note over Pool,Actor: Raft lane may spawn on a dedicated OS thread + runtime

    Note over Caller,Reg: 2. Send (request/response on a lane)
    Caller->>Pool: send(Message, lane)
    Pool->>Actor: LaneHandle.reserve().await + permit.send(LaneCommand::Send)
    Actor->>Actor: enforce stream window + Data-lane in-flight cap
    alt window has capacity
        Actor->>Client: client.send_with_timeout(msg, lane, timeout)
        Client->>Codec: encode Frame (Legacy or CapnpEnvelope)
        Codec->>Net: header + body
        Net->>Server: bytes
        Server->>Codec: decode Frame (version must match negotiated format)
        Server->>Reg: dispatch(peer_id, msg_type, Message)
        Reg-->>Server: Option&lt;Message&gt; (None = fire-and-forget)
        Server-->>Client: response Frame (stream_id correlated)
        Client-->>Actor: LaneCommand::SendComplete(Result&lt;Message&gt;)
        Actor-->>Caller: Result&lt;Message&gt;
    else window full / pending cap exceeded
        Actor-->>Caller: Err(NetError::Overloaded)
    end

    Note over Actor,Net: 3. Disconnect -> reconnect lifecycle
    Net--xClient: TCP drop
    Client->>Actor: alive watcher fires
    Actor->>Actor: spawn_reconnect (connect_with_retry_cancelable, backoff)
    alt reconnect succeeds
        Actor->>Actor: LaneState = Connected(new client); re-attach alive watcher
    else MAX_RECONNECT_ATTEMPTS exhausted
        Actor->>Actor: exhaustion_count += 1
        alt exhaustion_count &lt; DORMANT_AFTER_EXHAUSTIONS
            Actor->>Actor: schedule retry cycle (sleep 5s)
        else
            Actor->>Actor: LaneState = Dormant; probe every DORMANT_PROBE_INTERVAL
        end
    end
    Note over Caller,Actor: while reconnecting -> Err(Reconnecting); dormant probe can wake to Connected
```

## Notes on the path

- **Three lanes, three connections.** `Raft`, `Data`, and `Bulk` each get their
  own TCP connection and actor, with distinct default timeouts (1 s / 10 s /
  60 s) so a slow bulk transfer cannot delay a Raft heartbeat.
- **Cancel safety.** The caller never holds a lock across the network round-trip;
  it only owns a `oneshot` receiver. If the caller future is dropped mid-send,
  the reserved mpsc permit is released and no half-sent state remains.
- **Backpressure is explicit.** The actor enforces a per-lane stream window
  (`max_streams_per_lane`) and a process-wide `data_lane_max_in_flight` cap;
  overflow returns `NetError::Overloaded` rather than queuing unboundedly.
- **Frame format negotiation.** Legacy (v1) and Cap'n Proto envelope (v2) frames
  coexist; the codec is pinned to the negotiated `WireFrameFormat` and rejects a
  mismatched version instead of misparsing it.
