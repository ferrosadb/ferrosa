---
crate: ferrosa-net
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-net — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the in-code design notes, and
the dependency/usage review. There are no `TODO`/`FIXME` markers in `src/`, so
this roadmap is gap- and risk-driven rather than scraped from the code.

## Now (highest value)

- **Exhaustive `MsgType` round-trip test** (FMEA NET-1). Today only the repair
  (`0x29..=0x2E`) and streaming (`0x36..=0x3A`) tags are pinned. Add a test that
  iterates *every* declared `MsgType` variant through `TryFrom<u8>` and back, so a
  newly-added discriminant cannot ship without a matching decoder arm. This is the
  cheapest fix for the crate's top failure mode (peers silently dropping frames).
- **Document the secure-internode deployment contract** (FMEA NET-8, NET-9).
  TLS and PSK are both off by default and there is no mutual TLS. Write the
  required `FERROSA_INTERNODE_TLS_CERT/KEY/CA` + `FERROSA_INTERNODE_REQUIRE_TLS` +
  `FERROSA_INTERNODE_PSK` configuration and surface it in the operator runbook so
  untrusted-network deployments don't silently run plaintext, anonymous mesh.

## Next

- **Mutual TLS (client auth)** (FMEA NET-8). Both `build_tls_acceptor` and
  `build_tls_connector` use `with_no_client_auth`. Add an opt-in mTLS mode that
  verifies the peer certificate against the configured CA, so TLS authenticates
  *both* directions instead of only encrypting.
- **Track the handler-registration race to closure** (FMEA NET-5). The
  `rpc/handler.rs` test currently asserts the *bug* (a `RaftVote` arriving before
  registration is dropped). Once the `ferrosa-cluster` fix lands, flip the
  assertion to expect successful dispatch and remove the documented-bug comment.
- **Tunable dormant probe interval.** `DORMANT_PROBE_INTERVAL` is a hard-coded
  5 minutes; a genuine transient outage incurs up to that latency before a lane
  re-probes. Make it env-tunable like the lane channel/stream capacities already are.

## Later

- **Property-test frame and `Message` codec round-trips.** `proptest` is already a
  dev-dependency; add `decode(encode(m)) == m` coverage across the `Message` and
  `FrameHeader`/`TraceContext` space as a format-stability net independent of the
  integration tests.
- **Complete the Cap'n Proto v2 cutover.** Legacy bodies still ride inside a
  `LegacyPayload` envelope until peers negotiate the Cap'n Proto frame format.
  Once all peers negotiate v2, retire the legacy frame path behind a deprecation
  window.
- **Connection-pool observability pass.** Surface per-peer lane state
  (Connected/Reconnecting/Dormant) and stream-window saturation as first-class
  metrics/health-check fields, building on the existing `metrics` counters.

## Non-goals

- Raft/Accord/repair *semantics*, query planning, storage, and membership policy
  — those belong to `ferrosa-cluster` and above; this crate only frames,
  authenticates, prioritises, and delivers their messages.
- Cassandra internode wire compatibility — ferrosa intentionally runs its own
  internode protocol (CQL client compatibility is a separate concern).
