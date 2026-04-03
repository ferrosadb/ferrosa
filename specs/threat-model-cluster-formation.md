# Threat Model — Cluster Formation State Machine

> **Date:** 2026-04-01
> **Scope:** Incremental attack surface from cluster formation state machine
> **Methodology:** STRIDE per element
> **Companion:** `specs/threat-model-net-cluster.md` covers base net/cluster layer (not duplicated here)
> **Design spec:** `specs/cluster-formation-state-machine.md`

## Scope Boundaries

Already covered in `specs/threat-model-net-cluster.md` (NOT duplicated):
- Rogue node joins (mTLS/PSK), internode eavesdropping (TLS), cluster name auth, Raft SM divergence, pair split brain, unauthorized role swap

This document covers:
- ClusterInvite protocol, Forming state transitions, connection-direction role assignment
- Degraded mode operator promotion, decommission with leader transfer + data streaming
- Raft initialization during the Forming window

## Assets

| # | Asset | Type | Impact if Compromised |
|---|-------|------|----------------------|
| AF1 | ClusterInvite peer list | Integrity | Rogue addresses injected into mesh |
| AF2 | Formation state transitions | Integrity | Cluster stuck in wrong mode |
| AF3 | Role assignment (Primary/Secondary) | Integrity | Split brain, data divergence |
| AF4 | Degraded mode promotion gate | Availability, Integrity | Unauthorized split brain |
| AF5 | Decommission control flow | Integrity, Confidentiality | Unauthorized removal, data exfil |
| AF6 | DDL during Forming window | Integrity | Schema divergence |
| AF7 | Raft membership at initialization | Integrity | Attacker node in initial Raft group |

## Threat Inventory

### 1. ClusterInvite Protocol

#### CF-T1: Peer List Poisoning via Forged ClusterInvite (Risk 8 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering |
| **Threat** | Compromised peer sends ClusterInvite with attacker-controlled (Uuid, SocketAddr) entries. Receiving nodes connect to rogue address, which is included in Raft group. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | (1) Validate every peer in invite via independent mTLS handshake — invite is a hint, not trust assertion. (2) Rate-limit: max 1 invite per peer per 10s. (3) Reject invites with local UUID + different address. |
| **Status** | **Gap** |

#### CF-T2: ClusterInvite Amplification Storm (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Threat** | Spec requires re-broadcast on receipt. Compromised node sends rapid invites → O(N²) amplification, saturating Raft lane. |
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Risk** | **6** |
| **Mitigation** | (1) Dedup by initiator UUID per formation epoch. (2) Suppress if local peer list is superset. (3) Cap 5 invites/min. (4) Monotonic invite_epoch counter. |
| **Status** | **Gap** |

#### CF-T3: ClusterInvite Replay (Risk 2 — Medium)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Spoofing |
| **Threat** | Captured invite replayed after formation complete, causing confusion or unnecessary connections. |
| **Likelihood** | 1 |
| **Impact** | 2 |
| **Risk** | **2** |
| **Mitigation** | Formation epoch/nonce in invite. Cluster-mode nodes ignore invites. TLS prevents external capture. |
| **Status** | **Gap** |

### 2. State Transitions

#### CF-T4: Forced Mode Transition via Fake Peer (Risk 8 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Elevation of Privilege |
| **Threat** | Authenticated peer connects → immediate mode transition. 2 connections → attacker in initial Raft membership. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | (1) Progressive formation state machine gates transitions (Standalone->Pair->Forming->Cluster). `FERROSA_CLUSTER_MODE` was removed (commit 83943a5) — the state machine itself enforces correct progression. (2) Initial formation should respect `auto_join=false`. (3) Log all mode transitions. |
| **Status** | **Partial** — progressive state machine gates transitions; formation approval is a gap |

#### CF-T5: Formation Stall — No Forming State (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Threat** | 3rd peer connects then drops. Node in Cluster mode but Raft has no leader. DDL on Direct forever. |
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Risk** | **6** |
| **Mitigation** | Implement Forming state. Forming→Pair fallback at 60s timeout. Mode revert on election failure. |
| **Status** | **Gap** |

#### CF-T6: Race in Concurrent Peer Connections (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering |
| **Threat** | Two peers connect simultaneously. Both `on_peer_connected` calls see Standalone, both call `transition_to_pair`. Second overwrites first, orphaning handlers. |
| **Likelihood** | 3 |
| **Impact** | 2 |
| **Risk** | **6** |
| **Mitigation** | Transition serialization mutex. Re-check mode under lock. Forming state provides natural serialization. |
| **Status** | **Gap** |

### 3. Role Assignment

#### CF-T7: UUID Election vs Connection Direction Confusion (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Spoofing, Integrity |
| **Threat** | Attacker chooses UUID (Uuid::max()) to always win election and become Primary. UUID is self-asserted during handshake. |
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Risk** | **6** |
| **Mitigation** | Complete switch to connection-direction roles. Remove PairRole::elect for initial formation. Bind UUID to mTLS cert if kept. |
| **Status** | **Partial** — controller.rs:550 forces Primary in Standalone mode; UUID fallback remains |

#### CF-T8: Dual-Primary from Concurrent Promotion (Risk 8 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Integrity |
| **Threat** | Both sides of partition promoted by operators. On heal, both initiate connections → both claim Primary. force_promoted=true on both. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | Promotion epoch/Lamport counter. Higher counter wins on reconnect. ferrosa-ctl warns about dual-promote risk. |
| **Status** | **Gap** |

### 4. Degraded Mode

#### CF-T9: Unauthenticated Admin API — Promote (Risk 12 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Elevation of Privilege |
| **Threat** | `POST /api/cluster/promote` has no authentication. Any VPC-internal entity can promote a secondary → split brain. |
| **Likelihood** | 3 |
| **Impact** | 4 |
| **Risk** | **12** |
| **Mitigation** | Bearer token via FERROSA_ADMIN_TOKEN. Bind admin API to localhost by default. Rate-limit 1/min. Audit logging. |
| **Status** | **Gap** |

#### CF-T10: Stale Read Exposure in Degraded Mode (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | Degraded secondary serves stale reads. Current code transitions to Standalone, hiding degradation from clients. |
| **Likelihood** | 3 |
| **Impact** | 2 |
| **Risk** | **6** |
| **Mitigation** | Dedicated DegradedPair state visible to CQL clients. Expose last_replicated_position via metrics. |
| **Status** | **Gap** |

### 5. Decommission

#### CF-T11: Unauthenticated Decommission via Admin API (Risk 12 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | DoS, Tampering |
| **Threat** | No auth on decommission endpoint. Attacker decommissions enough nodes to break quorum. |
| **Likelihood** | 3 |
| **Impact** | 4 |
| **Risk** | **12** |
| **Mitigation** | Same auth as CF-T9. Quorum safety check in initiate_decommission. Confirm for local node. |
| **Status** | **Gap** |

#### CF-T13: Partial Decommission — Crash Mid-Stream (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Integrity, Availability |
| **Threat** | Node crashes after LeaveNode committed but before streaming completes. Token ranges unavailable, unflushed data lost. |
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Risk** | **6** |
| **Mitigation** | Two-phase: LeaveNode (intent) → streaming → RemoveNode (completion). Track streaming progress in Raft state. |
| **Status** | **Gap** |

### 6. Raft Initialization

#### CF-T14: DDL Gap During Forming Window (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Integrity |
| **Threat** | DDL during Forming applied locally but never replicated. Leader replay only covers leader's schema, not followers'. |
| **Likelihood** | 2 |
| **Impact** | 3 |
| **Risk** | **6** |
| **Mitigation** | Reject DDL during Forming state. Schema checksum comparison post-formation. |
| **Status** | **Gap** |

#### CF-T15: Raft Membership Manipulation During Init (Risk 8 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Elevation of Privilege |
| **Threat** | Attacker connects between peer-count check and Raft initialization → permanent Raft member with vote, can block quorum. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | Freeze peer list on entering Forming. Late arrivals join via JoinNode after formation. Verify against approved_nodes. |
| **Status** | **Gap** |

#### CF-T17: All Nodes Call Raft::initialize() Independently (Risk 9 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Integrity |
| **Threat** | Each node calls initialize() with its local connected_peers — may differ. Inconsistent membership → split votes, no leader. |
| **Likelihood** | 3 |
| **Impact** | 3 |
| **Risk** | **9** |
| **Mitigation** | Only seed calls initialize(). Others wait for AppendEntries. Or: canonical sorted membership from ClusterInvite. |
| **Status** | **Gap** |

## Risk Summary

### Critical (Risk 8-12)

| ID | Threat | Risk | Status |
|----|--------|------|--------|
| CF-T9 | Unauth admin API (promote) | 12 | **Gap** |
| CF-T11 | Unauth admin API (decommission) | 12 | **Gap** |
| CF-T17 | Raft initialize() membership race | 9 | **Gap** |
| CF-T1 | Peer list poisoning via ClusterInvite | 8 | **Gap** |
| CF-T4 | Forced mode transition | 8 | **Partial** |
| CF-T8 | Dual-Primary from concurrent promotion | 8 | **Gap** |
| CF-T15 | Raft membership manipulation | 8 | **Gap** |

### High (Risk 6)

| ID | Threat | Risk | Status |
|----|--------|------|--------|
| CF-T2 | ClusterInvite amplification | 6 | **Gap** |
| CF-T5 | Formation stall | 6 | **Gap** |
| CF-T6 | Concurrent peer connection race | 6 | **Gap** |
| CF-T7 | Role confusion | 6 | **Partial** |
| CF-T10 | Stale read exposure | 6 | **Gap** |
| CF-T13 | Partial decommission | 6 | **Gap** |
| CF-T14 | DDL gap during Forming | 6 | **Gap** |

## Mitigation Priority

### P0: Before any formation code

| Mitigation | Threats | Effort |
|-----------|---------|--------|
| Admin API authentication (bearer token + localhost) | CF-T9, CF-T11 | M |
| Quorum safety in initiate_decommission | CF-T11 | S |
| Transition serialization mutex | CF-T6 | S |
| Freeze peer list on Forming entry | CF-T15 | S |
| Only seed calls raft.initialize() | CF-T17 | M |

### P1: During ClusterInvite implementation

| Mitigation | Threats | Effort |
|-----------|---------|--------|
| Validate invite peers via independent handshake | CF-T1 | M |
| Invite dedup + rate limit + epoch | CF-T2, CF-T3 | M |
| Forming state with fallback timeout | CF-T5 | M |
| Reject DDL during Forming | CF-T14 | S |
| Connection-direction role assignment | CF-T7 | S |
| Pair mode enforcement + formation approval | CF-T4, CF-T15 | S |

### P2: Decommission + degraded hardening

| Mitigation | Threats | Effort |
|-----------|---------|--------|
| Two-phase decommission | CF-T13 | L |
| Promotion epoch + conflict resolution | CF-T8 | M |
| DegradedPair state visible to clients | CF-T10 | M |
