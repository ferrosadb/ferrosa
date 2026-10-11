---
title: ADR-023 — Read-in-transaction: snapshot reads and a read-set committed through Accord
status: accepted
date: 2026-10-11
component: transaction-manager
executive_summary: >
  Transactional reads must be first-class: a SELECT inside BEGIN...COMMIT reads a snapshot
  and its read-set participates in the commit decision through Accord, so a transaction both
  reads and writes and returns its reads. The MVCC machinery required for this already exists
  on the PostgreSQL front end (MvccManager snapshots, an apply observer installed on the
  Accord apply path, a commit gate); the missing half is the engine-side multi-key read-at-t
  on the Accord commit path, plus bounded, non-materializing read-set handling. This ADR fixes
  the approach, the rejected alternatives, and the controls that keep the result honest.
---

# ADR-023 — Read-in-transaction: snapshot reads and a read-set committed through Accord

## Status

Accepted (2026-10-11). The requester chose **option C**: PostgreSQL MVCC supplies snapshot
visibility (as already built) and the Accord commit path validates the read-set at the agreed
commit-`t`. Option A (Accord validation with no MVCC snapshots) and option B (MVCC-only, where a
read never constrains the Accord commit) were rejected; B is precisely the shape that permits a
non-serializable result to be reported as success (failure mode RT-6).

## Context

Requirements, in the requester's words:

> "lets focus on read inside a transaction we are going to need that. accord needs to be
> able to contain the whole transaction semantics perhaps using mvcc"

Current behaviour, as measured:

- Engine-side transactions are **write-only**. `TransactionCommitter::commit(writes)` carries
  no read-set. The proposed design already added an additive `commit_with_reads(writes, reads)`
  whose default **refuses a non-empty read-set** rather than returning empty rows (returning
  defaults would fabricate row ABSENCE, turning a missing capability into wrong query results).
- CQL-side staging for reads **is built**: `session.rs` buffers reads (`stage_read`) and calls
  `commit_with_reads` with a read-set positionally aligned to the returned rows; the mock
  committer echoes canned rows.
- PostgreSQL MVCC **is built**: `MvccManager`/`MvccSnapshot`, `MvccCommitError::SerializationFailure`
  (the `40001` seen on the wire), a commit gate, resident-history accounting, and a
  `PostgresMvccApplyObserver` installed on the Accord apply path so Accord writes are applied
  through PostgreSQL MVCC.
- Unified Transaction Manager Phase A is **implemented**: transaction state is off the connection
  and in a server-wide registry keyed by an opaque txn id (`BEGIN` returns it, statements carry
  `IN TRANSACTION <id>`). Its own as-built note records that Phase A reads committed state and
  that **MVCC isolation is a later phase**.
- **Measured today (live 3-node cluster, two simultaneous transactions):**

  | Invariant | Result |
  |---|---|
  | one transaction: commit takes effect, read back by value | PASS |
  | rollback absent + committed write visible (positive control) | PASS |
  | same key: one committed value wins, loser refused `40001` | PASS |
  | disjoint keys: BOTH writes survive | **FAIL** |

  Disjoint-key detail: peer A was refused `40001`, peer B reported success with no error, and
  B's updated column read back **NULL** (row present, cell lost). Every simultaneous round
  refused exactly one peer, including the disjoint-key pair. So concurrent transactions collide
  even on different keys, and a successful commit can lose a cell.

## Decision

Make transactional reads first-class, with MVCC snapshots supplying visibility and Accord
supplying the commit decision:

1. **Reads are staged, not executed, inside an open transaction.** A `SELECT` inside
   `BEGIN...COMMIT` records `TransactionRead { keyspace, table, key }` in the transaction's
   read-set; the front end returns rows read at the transaction's snapshot.
   **Surface: Cassandra's `LET` block.** The standard spelling of a transactional read is
   `BEGIN TRANSACTION` ... `LET <var> = SELECT <cols> FROM <t> WHERE <pk> = ?` ...
   `COMMIT TRANSACTION`, where each `LET` binds one read of the transaction and later
   statements in the same transaction may reference the variable. Implementing `LET` rather
   than an ad-hoc select-in-transaction form keeps the CQL surface wire-compatible with
   Cassandra 5 and makes the read-set explicit at PARSE time instead of inferred from an
   open transaction, which is what the read-set validation needs.
2. **The read-set is validated through Accord at the agreed commit timestamp.** The engine
   executes each read key at the transaction's agreed `t` (multi-key read-at-`t`), so reads
   observe the transaction's own writes and the commit is strict-serializable rather than
   write-only-serializable.
3. **Snapshot visibility comes from PostgreSQL MVCC, not from a second store.** The MVCC
   manager's snapshots and the apply observer are the single source of visibility truth; Accord
   remains the single source of the commit order.
4. **The seam stays additive.** `commit(writes)` is unchanged for write-only callers; the
   read path is `commit_with_reads`. A committer that has not implemented the read path
   **fails loud** on a non-empty read-set.
5. **Both sets stay bounded and non-materializing.** The read-set and write-set must have a
   spilling/streaming representation before any cap is lifted; `commit_with_reads` must not
   require the whole write-set resident.

## Alternatives rejected

- **Connection-keyed transaction state** (the pre-Phase-A model). Rejected: it produced ~24%
  indeterminate operations in the Elle list-append certification — a client/server affinity
  defect, not a database one. Phase A removed it; this ADR does not reintroduce it.
- **Read-your-own-writes only, no read-set validation.** Rejected: it is not serializable, and
  it cannot pass Elle `list-append`, which needs a transaction to read a value and return it.
  A read must constrain the commit, not merely be visible to its own transaction.
- **A separate MVCC version store beside PostgreSQL MVCC.** Rejected: two visibility authorities
  means two ways for them to disagree, and the existing MVCC manager already owns version
  retention and the commit gate.
- **Materializing the read-set (and lifting the statement cap).** Rejected: removal of
  `FERROSA_CQL_TRANSACTION_MAX_STATEMENTS` is already documented as REFUSED because the buffer
  is resident with no spill path and the committer consumes the whole `Vec`. Read-in-transaction
  must not add a second unbounded resident structure.
- **Surfacing transactional reads as an in-transaction `SELECT` that bypasses Accord.** Rejected:
  it would return a value the commit never validated, which is exactly the silent-wrong-answer
  class this ADR exists to prevent.

## Consequences and required controls

- **Never fabricate absence.** A read the committer cannot serve must fail loud; `None` in
  `CommitReads` means "row genuinely absent at commit-`t`" and must not be a substitute for
  "not implemented".
- **Bounded by construction.** Read-set and write-set handling must be measured, not asserted:
  a flat peak-memory curve as row/statement counts grow, with a stated N.
- **No new unbounded wait.** A commit that cannot complete must fail loud inside its bound; the
  bound is a detector, never a knob.
- **Definition of done is certification, not a green unit test.** Elle `list-append` with
  `:consistency-models [:strict-serializable]` returning `:valid? true` against a live RF=3
  cluster under a partition/slow nemesis, with **no** `:linearizable-keys?` assumption.
- **The disjoint-key conflict and the lost cell found by the two-transaction probe are in
  scope.** A read-set validated through Accord must not make concurrency worse, and a commit
  that reports success must not lose a cell. Both are acceptance gates for this feature.
