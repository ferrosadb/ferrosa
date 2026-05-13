---
title: Cap'n Proto serialization implementation brief
status: in-process
created: 2026-05-13
branch: feature/capnp-serialization
worktree: /Users/bkearns/src/ferrosa-suite/ferrosa-capnp
executive_summary: >
  Implement Cap'n Proto for Ferrosa-private high-priority serialization boundaries without changing Cassandra CQL wire compatibility or Cassandra Big/BTI SSTable disk compatibility.
---

# Cap'n Proto Serialization Implementation Brief

## Scope

Implement and test Cap'n Proto serialization for high-priority Ferrosa-private boundaries:

1. Remote index-builder coordination payloads.
2. Raft metadata protocol / persisted log / snapshot payloads with dual-read migration from bincode.
3. Accord internode protocol payloads carried inside existing opaque `ferrosa-net::Message` byte variants.
4. Vector index sidecar persistence for HNSW and IVFFlat using a versioned binary envelope.

## Non-goals / compatibility guardrails

- Do not change the Cassandra CQL native protocol.
- Do not change Cassandra Big/BTI SSTable core component formats.
- Do not change public Bolt, Cypher HTTP, SPARQL HTTP, or JSON operator APIs except if adding optional internal helpers.
- Do not replace operator-readable JSON manifests unless measured evidence justifies it.
- Keep old bincode/JSON readers where existing persisted data or mixed-version nodes require migration.

## Evidence from current code

- `ferrosa-net/src/message.rs` already has opaque `Bytes` variants for Raft, Accord, data, streaming, and index build messages.
- `ferrosa-cluster/src/raft/network.rs` uses `bincode::serialize` / `deserialize` for Raft RPCs.
- `ferrosa-cluster/src/raft/log_store.rs` stores Raft entries and metadata with bincode and already contains legacy decode glue for enum evolution.
- `ferrosa-cluster/src/raft/state_machine.rs` serializes snapshots with bincode.
- `ferrosa-cluster/src/index_coordination.rs` uses bincode for `IndexBuildRequestPayload` / `IndexBuildCompletePayload` and embeds JSON strings for metadata/schema.
- `ferrosa-index/src/vector/hnsw.rs` and `ferrosa-index/src/vector/ivfflat.rs` persist JSON bytes.

## Required engineering posture

- TDD only: write failing tests first for each boundary.
- Use explicit magic/version/codec envelopes at durable boundaries.
- Add golden decode fixtures for old and new formats.
- Preserve fail-loud decode errors; no `unwrap_or_default()` on protocol encode paths.
- Verify with crate-level cargo tests and final `cargo build --all-targets`.

## Suggested task order

1. Cap'n Proto schema/build infrastructure and codec helpers.
2. Remote index-builder payload migration.
3. Vector index sidecar envelope migration.
4. Accord payload codec migration.
5. Raft RPC/log/snapshot dual-codec migration.
6. Integration verification and binary build.
