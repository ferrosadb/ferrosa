---
title: FMEA — Read-in-transaction (snapshot reads + read-set commit through Accord)
status: proposed
date: 2026-10-11
component: transaction-manager
executive_summary: >
  Failure modes for transactional reads, scored by RPN, each item of RPN >= 50 mapped to an
  explicit test. The two highest-scoring modes are already observed on the live cluster - a
  successful commit losing a cell, and concurrent transactions colliding even on disjoint keys -
  so those entries carry measured evidence rather than speculation. Every mode has a stated
  detection and a control that fails loud rather than returning a plausible wrong answer.
---

# FMEA — Read-in-transaction

Scoring: Severity x Occurrence x Detection, each 1-10. Items with RPN >= 50 must map to an
explicit test; items with RPN >= 200 produce a work item.

| ID | Failure mode | Effect | S | O | D | RPN | Detection | Control | Test |
|---|---|---|---|---|---|---|---|---|---|
| RT-1 | Committer returns empty `CommitReads` for a read-set it cannot serve | Fabricated row ABSENCE; caller believes a key was empty | 9 | 3 | 3 | 81 | Returned rows not positionally aligned with the staged read-set | Default `commit_with_reads` REFUSES a non-empty read-set | positive control: empty read-set succeeds; non-empty on an unimplemented committer is refused by name |
| RT-2 | Concurrent transactions collide on DISJOINT keys | Throughput collapse; every concurrent txn refused `40001` | 8 | 8 | 2 | 128 | live two-transaction probe, disjoint pair | read-set validation must be key-scoped, not table-scoped | two simultaneous txns on different inline keys both survive, values asserted |
| RT-3 | A commit that reports success loses a cell (row present, value NULL) | Silent data loss; no error returned | 10 | 5 | 4 | 200 | read back the VALUE through a different session | apply path must carry the write-set's cells; assert cell content at the far end | live probe pair, BOTH COMMITs in flight, assert the survivor's VALUE. Measured GREEN in-process (3/3), so the reproducer must be CLUSTERED - an in-repo two-process reproducer is OWED |
| RT-4 | Read-set grows unbounded in RAM | OOM on a large transaction | 8 | 5 | 3 | 120 | measured peak memory vs read-set size at stated N | spilling/streaming read-set; no cap lift before it exists | measured flat curve; cap removal refused until it passes |
| RT-5 | `commit_with_reads` requires the whole write-set resident | OOM on commit of a large txn | 8 | 5 | 3 | 120 | measured commit peak vs write-set size | streaming committer; the existing 10 000 cap stays | measured curve at stated N; refusal if resident |
| RT-6 | Read inside a txn observes a value the commit never validated | Non-serializable result reported as success | 9 | 3 | 5 | 135 | Elle `list-append` / `rw-register` checker on a live cluster | reads execute at the agreed commit-`t` and constrain the commit | list-append strict-serializable, `:valid? true`, no inference assumptions |
| RT-7 | MVCC version retention grows without bound under long transactions | Resident-history growth; OOM | 7 | 4 | 4 | 112 | resident-history stats; bounded by txn deadline | transaction deadline bounds version retention | history accounting stays bounded under a long-running txn |
| RT-8 | Reaper/timeout races a commit and drops a staged read | Read-set silently truncated | 7 | 3 | 4 | 84 | commit after deadline must refuse, not commit partial | `ensure_active`/`take_for_commit` authorize before use; poison on partial | expired txn commit is refused loudly; no partial apply |
| RT-9 | In-transaction read served from the connection's own cache, not the snapshot | Stale read reported as transactional | 6 | 4 | 5 | 120 | read after another node's commit; compare to snapshot rule | snapshot read via MVCC, not a per-connection cache | read inside a txn does not observe a post-snapshot commit |
| RT-10 | `40001` refusal has no actionable retry contract | Client treats a refusal as a crash; retries hammer the cluster | 4 | 6 | 3 | 72 | client behaviour under `40001` | refusal is loud and named; retry policy stated | `40001` is returned with a stable SQLSTATE and documented retry guidance |

## Highest-priority items (RPN >= 200)

- **RT-3** — a successful commit losing a cell. Observed live: disjoint-key pair, peer A refused
  `40001`, peer B reported success with no error, and B's cell read back NULL (row present).
  Work item required; the probe pair with the refused peer FIRST is the reproducing test.
