# CapnProto internode conformance fixtures

These binary fixtures are golden full internode frames, not only CapnProto envelope bodies. They cover the negotiated frame header plus the CapnProto body so rolling-upgrade tests catch both header-version drift and schema/body drift.

- `invite_frame_v1.bin` is a three-node cluster invite from node 1 to node 2 that names node 2 and node 3 as peers.
- `bootstrap_plan_frame_v1.bin` is a bootstrap-control plan from node 2 to node 3 for `ks.tbl` with a bounded SSTable bulk plan.

`ferrosa-net/tests/capnp_conformance.rs` decodes these fixtures through `InternodeCodec` in CapnProto-envelope mode, rejects malformed/truncated mutations without falling back to legacy framing, checks the version/feature negotiation matrix, and runs a small in-memory three-node invite/rejoin/bootstrap wire smoke.

Interaction with the non-destructive local roll / fmem smoke card: these are protocol-level compatibility fixtures and do not start local nodes, rewrite cluster state, or wipe data. The local roll/fmem smoke should consume the same negotiated CapnProto frame mode as an end-to-end deployment check; these tests stay fast and deterministic so the destructive/non-destructive data-safety boundary remains in the live smoke card rather than hidden in unit tests.
