---
title: Unified Transaction Manager — FMEA
status: proposed
component: transaction-manager
executive_summary: >
  Failure-mode analysis for the txn-id-keyed, MVCC-backed transaction manager. Highest
  risks: cross-client txn-id hijack (security), registry/intent unbounded growth
  (availability), coordinator failure losing interactive-transaction state (durability),
  MVCC version-GC reclaiming versions a live snapshot still needs (correctness), and
  multi-node forwarding sending a statement to the wrong owner (correctness). Each is
  paired with a design control and an acceptance gate. RPN = Severity x Occurrence x
  Detection (1-3 each; higher = worse).
last_revised: 2026-07-20
---

# Unified Transaction Manager — FMEA

Severity/Occurrence/Detection scored 1 (best) to 3 (worst). RPN = S x O x D.

| # | Failure mode | Effect | S | O | D | RPN | Control / acceptance gate |
|---|---|---|---|---|---|---|---|
| F1 | **Txn-id hijack** — a client references another client's txn-id | Cross-tenant read/write; isolation + auth breach | 3 | 2 | 3 | 18 | Unguessable 128-bit id; entry records auth principal; statement on a non-owned id fails loud. Gate: test a second principal referencing an id is rejected. |
| F2 | **Registry unbounded growth** — abandoned/leaked transactions accumulate | OOM / node crash (cf. compose-node OOM history) | 3 | 2 | 2 | 12 | Data lives in NVMe temp tables (constraint 7); RAM holds only per-txn metadata. Bounded registry *count* + per-txn deadline + TTL eviction (drops the temp table); fail-loud on overflow. Gate: load test with abandoned txns holds bounded RAM; eviction reclaims NVMe. |
| F2b | **NVMe exhaustion** — one huge transaction (or many) fills local NVMe | Node write-path stall / crash | 3 | 2 | 2 | 12 | Per-transaction + global NVMe budget; a transaction that would exceed it fails loud and aborts (drops its temp table) — never silently truncates or evicts a live transaction. Gate: oversized-transaction test aborts cleanly with NVMe reclaimed. |
| F3 | **Coordinator failure loses txn state** — owner node dies mid interactive transaction | Long SQL transaction aborts / hangs | 3 | 2 | 1 | 6 | Temp tables are on-disk NVMe, so intents survive a *process* restart and are recoverable/GC-able on the same node; on *node* loss, Phase A/B cleanly aborts (definite failure, never phantom commit); future durable-registry replicates the metadata for cross-node resume. Gate: process-restart recovers/aborts orphan temp tables; kill-node yields a clean abort, no partial commit. |
| F4 | **MVCC version GC too aggressive** — collects a version a live snapshot still needs | Snapshot read returns wrong/older data — silent corruption | 3 | 2 | 3 | 18 | GC only below the cluster read low-water-mark (oldest live `read_ts`); retain until then. The open-timeout (constraint 8) bounds that low-water-mark to ~one window (default 10s), so retention is small and a runaway reader cannot pin history. Gate: property test — no snapshot ever misses a version ≤ its read_ts under concurrent GC; retention stays within the timeout window. |
| F5 | **Intent not cleaned up on abort/crash** — orphan write intent | Later reads blocked/confused by a stale intent; space leak | 3 | 2 | 2 | 12 | Intents are txn-scoped and swept on abort/expire; recovery scans for intents of dead txns. Gate: abort/crash test leaves no live intents. |
| F6 | **Multi-node mis-forward** — statement for a txn-id routed to a non-owner node | `unknown transaction` errors, or worse, split state | 3 | 2 | 2 | 12 | Owner encoded in the txn-id; forward by id; unknown/foreign id fails loud (never create a second entry). Gate: statements for a remote id are forwarded and applied exactly once. |
| F7 | **Concurrent statements on one txn-id** — client pipelines within a transaction | Interleaved staging corrupts the transaction | 2 | 2 | 2 | 8 | Per-txn-id serialization (submission order); a transaction is logically serial. Gate: pipelined-within-txn test preserves order. |
| F8 | **Read-ts skew across nodes** — snapshot read_ts not coherent with commit order | Non-repeatable / inconsistent interactive reads | 3 | 2 | 3 | 18 | read_ts from the shared HLC (see t_813caf39 witness work); reads honor Accord order; document SI vs Serializable. Gate: Elle rw-register / SI checker passes for interactive read-write. |
| F9 | **Txn-id collision** | Two transactions share state | 3 | 1 | 3 | 9 | 128-bit random / TimeUUID with random low bits; collision negligible. Gate: id-uniqueness property test. |
| F10 | **Compat-shim ambiguity** — bare `BEGIN` vs `IN TRANSACTION <id>` on one connection interleave | Desync returns for shim users | 2 | 2 | 2 | 8 | Shim binds an internal id to the connection; disallow mixing shim + explicit-id on one connection; deprecate shim. Gate: mixed-mode test fails loud. |
| F11 | **Commit half-applies intents** (partial across keys) | Torn transaction — data loss | 3 | 1 | 1 | 3 | Reuse Accord all-or-nothing `apply_writeset` (already fail-loud, tested). Gate: existing multi-key atomicity proptests extended to intents. |
| F12 | **Deadlock / lock-wait** under Serializable conflicts | Stalls | 2 | 2 | 2 | 8 | Accord is deterministic-order (no lock-wait deadlock); bounded commit deadline aborts stragglers. Gate: contention load test bounds latency, no permanent stall. |

## Priority summary

- **RPN 18 (critical):** F1 hijack, F2 registry OOM, F4 MVCC GC, F8 read-ts skew — all
  gated before the corresponding phase ships.
- **RPN 9–12 (high):** F5 intent cleanup, F6 mis-forward, F9 collision.
- **RPN ≤ 8:** serialization, shim, atomicity, deadlock — controlled by existing Accord
  guarantees + small additions.

MVCC introduces the most novel correctness surface (F4, F5, F8); it must ship behind a
property-tested version-visibility invariant and an Elle/SI checker, not by inspection.
