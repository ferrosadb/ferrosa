# Commit Path — Ground Truth (code-inferred)

> Last updated: 2026-10-12
> Status: Ground truth (verified); supersedes the mechanisms in the review of the same date
> Raw evidence: `ground-truth/lane-a-commit-path.md`, `lane-b-abandon-depwait.md`,
> `lane-c-pg-transaction-buffer.md` (parallel recon; every claim cited `file:line`)

## What this is

Code-inferred ground truth for the Accord commit path and the PostgreSQL front-end transaction
boundary, produced so the path can be simplified and refactored for testability and provable
correctness. It records what the code DOES, not what it should do. Contradictions between comments and
code are recorded as contradictions. Where the truth could not be established it is listed under
Unresolved rather than inferred.

## Retractions (my earlier review of this date)

Three mechanisms in `commit-path-and-transaction-boundary-review.md` were WRONG and are retracted here.
Each was a plausible reading that a cited source read refutes.

- **F1 retracted** ("the statement path bypasses the buffer"). The opposite is true: DML in an open
  transaction is buffered — `copy_stdin.rs:276-289` ("Buffer into the SAME write-set INSERT/TRUNCATE
  use, so COMMIT applies it and ROLLBACK discards it"; "an open transaction must never be committed
  mid-statement"), `query.rs:1561-1573`, `server.rs:854-862`. A plain `BEGIN` opens a TRACKED
  transaction with the buffer engaged (`extended.rs:331-335`, `server.rs:701`); isolation `None` means
  session default, not untracked.
- **F1's COPY lead retracted.** In-transaction COPY buffers; the `FLUSH_EVERY=1000` autocommit batching
  is the `else` branch only. `pgbench -i`'s accounts COPY is inside one `begin`…`commit`, so no path
  applies that load early.
- **F2 retracted** ("one constant governs three waits, all deciding outcomes"). Three 10 s constants
  exist (`dep_wait.rs:25`, `state_machine.rs:1537`, `pool.rs:707`) plus a 5 s barrier bound, but only
  ONE is client-visible. The graph's own timer (`check_timeouts`/`abort_timeout`) and `break_cycle`
  have NO production caller — dead code.
- **F3 retracted** ("live age-keyed prune located at apply.rs:1468"). That call is
  `#[cfg(test)] prune_graph_for_test`, whose own doc says "Test-only: production prunes the graph from
  its maintenance loop." Production uses identity-keyed `forget_graph_applied` (`apply.rs:1606`,
  `state_machine.rs:1419`) in lockstep with the `TxnState` prune.

## Confirmed findings

- **G1 — An abandon can be a lie about durability.** `bookkeep_applied` returns `Err(())` when the
  protocol-log `Applied` marker's fsync fails (`state_machine.rs:1300-1310`, logging "txn stays
  Committed until its Apply is retried"), and `apply_writeset_and_ack` returns `None` — withholding
  `ApplyOK` — when `await_txn_applied` is false (`handlers.rs:157-161`). A replica that durably wrote
  its rows but failed that marker therefore holds a READABLE row while its ack is withheld, so the
  coordinator can report `TxnAbandoned` with data already durable. This is the "abort that leaves
  data" mechanism; the client contract "NOT committed; safe to retry" does not hold on that path.

  > **Update (R0 slice 1, committed on this branch):** the replica-side gate is closed. The marker
  > write and the ack decision are now one entry point, `applied_journal::record_applied_marker`,
  > called from `AccordStateMachine::bookkeep_applied`; it returns `AppliedAck::{Ack,Withhold}` and
  > keeps "the rows are on disk" apart from "the journal recorded the decision". A marker fsync
  > failure with durable rows now advances the txn to `Applied` — the phase the ack gate waits for —
  > so the replica acks a row it durably wrote instead of denying it. Red-first, at the smallest
  > reachable level (one inbound Apply, in process):
  > `handlers::tests::apply_acks_when_the_rows_are_durable_even_if_the_marker_fsync_fails` (observed
  > RED: `got None` after the 10 s dep-wait bound, with the applier already holding the row).
  > Negative controls: `a_failed_marker_withholds_when_nothing_durable_was_written` (a no-write
  > finalize has no durable row, so its marker failure still withholds) and
  > `sm_storage_apply_failure_does_not_advance`. **Still open:** whether G1 was the mechanism behind
  > the live 2/3 census (U2) — this slice removes the lie at the replica, it does not connect it to
  > that measurement; the coordinator side (R6) still turns a missing ack into the legacy
  > `abandoned:` message; and the metadata/fragment mismatch the experiment measured is not
  > addressed here.
- **G2 — `mark_applied` is asymmetric (`dep_wait.rs:214-224`).** It removes the reverse edges of the
  deps it just applied, then does `waiting_on.remove(txn_id)` + `wait_start.remove(txn_id)` — without
  clearing `waited_by` entries pointing AT `txn_id`. Production-reachable through the cascade. This is
  the concrete "tracking its data correctly" defect: one of two maps maintained by hand is not updated.
- **G3 — The intended concurrency buffer is not where the code buffers.** The owner's stated design is
  that MVCC IS the concurrency buffer (tracking a transaction's changes until COMMIT, empty otherwise).
  In the code the in-flight changes live in the FRONT END's spilling `TxnWriteSet` (`extended.rs:103`,
  `txn_write_set.rs:47-52`, `query.rs:1561-1572`); MVCC holds COMMITTED version history
  (`mvcc.rs:259-260`). The intent is therefore a design gap, not a description of the system.
- **G4 — One 10 s constant is client-visible and misnamed.** `TxnAbandoned` has exactly one production
  construction site, `coordinator.rs:2799` inside `if !apply_ok` (`:2783`); the message text at
  `:577-582` says "dependency-wait bound". The governing wait is the Apply fan-out RPC bound
  (`configured_txn_timeout()` → `:2745/:2751`). The message cannot distinguish RPC-timeout,
  uncounted-ack and RPC-error (a bare `bool` at `:3205/:3269`).
- **G5 — Ownership hop-by-hop** (lane A): `TxnWriteSet`/`WriteSetStage` (session) → `StagedWriteSet` →
  `StagedAccordWriteSet{p: Arc<WriteSetSpill>}` → driver `write_set: Vec<WriteSetEntry>` +
  `write_blobs: Option<Arc<WriteSetSpill>>` → borrowed `(&[u8],&[u8])` → applier
  `AccordWriteSet<'s,'v>`. Spill floor 8 MiB (`WRITE_SET_SPILL_FLOOR_BYTES`, `write_set_spill.rs:77`),
  explicitly a buffer size and never a cap.
- **G6 — The durability instant.** Consensus: `FileSyncWriter` append + `sync_all`
  (`sync_writer.rs:112-145`). The row: `write_atomic_batch` forces the commit-log sync (`engine.rs:9658`)
  BEFORE the memtable's Phase 2 (`engine.rs:9660-9669`). A row becomes observable strictly after the
  fsync; readers see it only at Phase 2.
- **G7 — Apply fan-out** is per-peer key-scoped capnp frames, residency bounded by
  `APPLY_FANOUT_WINDOW = 4` (`coordinator.rs:52`), all-or-nothing per replica.
- **G8 — 18 failure-after-durable points** enumerated in lane A, each with what it leaves behind.
- **G9 — Hot-path serialization rule violation (flag).** Multi-key Apply is capnp, but
  PreAccept/Accept/Commit/ReadVote, the single-key Apply and the `AccordApplyV2` fallback are bincode
  (`accord/wire.rs`, `ferrosa-net/src/protocol.rs`). By the repo's rule — per-message hot-path
  serialization must be capnp — this is a defect, not a preference.
- **G10 — Comment-versus-code contradictions: 8 (lane A) + 4 (lane B) + 8 (lane C)** recorded
  unresolved, including applier "StorageEngine::write" vs `write_atomic_batch`, a protocol.log whose
  ASCII-text writer has a binary reader, and "one replica at a time" / "bincode frame" docs that no
  longer match the fan-out.

## EXPERIMENT RESULT — measured 2026-10-12 (supersedes U1's "unexplained")

Two clean, consecutive measurements on the live 3-node cluster, after which the client was killed and
confirmed gone (`pgrep` = 0) before each read. Trigger: `pgbench -i --scale 10 --foreign-keys`.

1. **The abandon is reproducible on demand.** Run: COPY reached `1000000 of 1000000 tuples
   (elapsed 4.74 s)`, then `pgbench: error: query failed: ERROR: transaction was abandoned and NOT
   committed: ... exceeded the 10s dependency-wait bound`. So the rows existed when the abort was
   reported.
2. **The table is then unreadable — reproducibly.** A point lookup (`select abalance from
   pgbench_accounts where aid = 1`) is *served as a scan*, and fails after 30 s, twice, cleanly:

   ```
   ERROR: 58000: scan of public.pgbench_accounts failed: internal: streaming range read:
     a fragment-merge source produced no fragment within 30000ms; failing loudly rather than waiting
     forever (one silent source must not stall the scan) (query aborted; result discarded)
   ```

**What this establishes.** The defect is not "an abandon leaks rows". It is that **an abandoned
commit leaves table state that references data whose fragments never arrived** — a dangling
reference — and the read path correctly fails loud rather than returning a wrong answer. The earlier
669,315-row census was a different moment in the same broken state (metadata pointing at a partial
set). Two distinct harms, one cause: the transaction's *metadata* and its *fragments* do not share a
fate.

**Consequences for the design.** (a) The abandoned transaction's effect on storage metadata is not
rolled back by `finalize_no_write`, which addresses the row half and not the metadata half. (b) The
read path's fail-loud detector is doing its job and must be kept. (c) The decisive next probe is a
**forced-failure small case**: `BEGIN; COPY (3 rows); <fail the commit>;`, then a point lookup — to
reproduce the dangling reference at a size where the storage state is fully enumerable, rather than
inferring it from a million-row table.

**U1 is now answered at the level of observable behaviour, and U2 remains open:** G1's
fsync-failed-marker path is the leading candidate for *why* the fragment never arrived, but this
experiment does not yet connect them; that needs the small forced-failure case plus an instrumented
node.

## U1 REVISED — the 2/3 census is a partial READ, and it is the same defect as the failed scan

The owner observed that 669,315 of 1,000,000 is 2/3, i.e. "one server is not returning". Arithmetic
confirms it: 669315/1000000 = 0.669315 against 2/3 = 0.666667, a ratio of 1.00397 — two of three nodes
answered. This is the failure my own runbook already names: *a census reading roughly 1/N of the
expected rows is not lost data, it is the read path serving local data; read the keys per node and take
the union before believing a short census.*

Per-node measurement (runner env exposes F1_CONN and F3_CONN):

```
node1 (F1_CONN)  ERROR: scan of public.pgbench_accounts failed: read timeout:
                 CL=ONE, received=0, required=1, data_present=false
```

and the scan-level failure from the same table:

```
ERROR: 58000: scan of public.pgbench_accounts failed: internal: streaming range read:
  a fragment-merge source produced no fragment within 30000ms; failing loudly rather than waiting
  forever (one silent source must not stall the scan)
```

**One defect, stated precisely:** after the abandoned commit, ONE node does not return its share of
this table. The engine's own message names it — *one silent source* — and `required=1` with
`data_present=false` says the node expected one replica of that key and found no data. The census's
2/3 and the scan's 30 s abort are the same condition observed two ways.

**What is NOT established:** whether that node's share is *absent* (loss) or *unreachable* (a read-path
or locality defect with the data still present). The message is ambiguous — "read timeout" is a
reachability statement, `data_present=false` is an absence statement, and both appear in one line. The
union test that would separate them cannot complete while that node's own read fails, which is itself
the observation. Distinguishing them needs a direct per-node key read *and* a storage-level check on
the node that is silent — or the small forced-failure case below, where the state is enumerable.

**So the corrected statement of the defect:** an abandoned commit leaves one replica unable to serve
its share of the affected table, which shortens every scan by that share and can abort a scan
outright. "An abort that leaves data" is the right intuition about the *harm*; the mechanism may be a
missing fragment or an unreachable one, and the code as written cannot yet tell you which.

## REPRODUCER RESULT — the abandon is a lie whenever ANY replica applied (measured)

`scripts/abandon-damage-repro.sh` (committed `c23618d2`, note `specs/reference/abandon-damage-repro.md`)
reduces the defect to an enumerable size. Two consecutive runs, identical verdict:

```
the client was told: transaction was abandoned and NOT committed ... safe to retry
key 1: absent on node1, node2, node3      <- the coordinator's own shard (CL-60's ordering fix working)
key 2: READABLE on node2                  <- a REMOTE shard
key 3: READABLE on node3                  <- a REMOTE shard
VERDICT: FAIL -- DAMAGE REPRODUCED (per-node union non-empty; a retry would double-apply)
```

**The mechanism, and it is not G1.** The apply fan-out sends Apply to the remotes, and each remote
applies durably BEFORE it acks — that ordering is inherent, a replica cannot ack a write it has not
made. When those acks do not arrive inside the bound, the coordinator takes the `!apply_ok` branch and
reports the retryable abandon, while the remotes that did apply hold durable, readable rows. CL-60
deferred the coordinator's OWN apply until the remote quorum is secured; that is why key 1 is absent
and exactly why keys 2 and 3 survive. The defect was displaced onto the remote shards, not removed.

**Size law (measured, not inferred).** 3 keys x 8 MB (24 MB) commits clean; the SAME 3 keys x 40 MB
(120 MB) abandons. Separately 300 000 rows (4.3 MB) and 1 000 000 rows (14.8 MB) both commit clean. So
the bound responds to BYTE VOLUME, not key count, and the million-row `pgbench -i` abandon is
consistent with that (its accounts COPY is ~100 MB), not with its row count. Bracket `(24 MB, 120 MB]`,
not bisected. The 10 s bound was never touched.

**Named mechanisms refuted by measurement:** SIGSTOP of both remote engines, and of either single
remote engine, both fail at the CONSENSUS stage (`Accord quorum unavailable`, ~2.7 s) without reaching
Apply, leaving no rows — and SIGSTOP wedges the node's inter-node lanes, requiring a machine recycle.
Mechanism (ii) (stopping machines) is a strictly stronger form of the same freeze and cannot do better.

**Consequences.**
1. R6's design is now forced by evidence: once ANY replica has applied, the transaction is committed
   and the outcome must NOT be the retryable abandon. Only a zero-apply abandon is truthful. A
   partial-apply outcome must be committed-and-converging and non-retryable.
2. **R0 (`c085d5ad`, the marker/ack fix) does NOT close this reproducer.** The surviving rows belong to
   the apply fan-out's write-before-decision ordering, not to the withheld ack. The gate expectation
   recorded by the reproducing agent ("FAIL -> PASS across deploying c085d5ad") is WRONG and is
   corrected here: deploying it should still FAIL, and that is the prediction to test.
3. The script is the standing gate for any change to the apply path.

## Unresolved (needs a probe or an owner decision, not more reading)

- **U1 — The 669,315 surviving rows are unexplained by any path found.** In-txn COPY commits
  atomically, the autocommit branch is not taken, and 669,315 is not a multiple of 1000. Decisive
  experiment: `BEGIN; COPY (3 rows); <force the COMMIT to fail>;` then a POINT LOOKUP of a known key
  asserted absent — never a count. (G1 is a candidate mechanism but is not yet connected to this
  observation.)
- **U2 — Whether the fsync-failed-marker path (G1) is the actual cause of U1**, and how often it
  occurs in practice. Requires the experiment plus an instrumented node.
- **U3 — Which sub-cause produced the million-row abandon.** The code cannot distinguish; the message
  is a legacy misnomer (G4).
- **U4 — Whether the capnp path is taken at runtime** (capability negotiation is dynamic); no explicit
  Apply-frame byte ceiling was found.
- **U5 — RT-3's mechanism** remains unknown; the discriminator holds (read-modify-write loses the cell,
  blind write keeps it). Read-set recording is gated on `in_txn && SERIALIZABLE` (`server.rs:715-740`)
  and the row check is inert on the Accord commit path (only two `record_point_read` call sites), so
  default-isolation clients record nothing.
- **U6 — `mark_applied`'s asymmetry (G2): is it reachable in a live schedule, and what does it break?**
  Needs a test, per the review's own rule that a claim without a probe stays a hypothesis.

## Why this matters for the refactor

Every confirmed finding above is a case of the same shape: **correctness carried by state that is
duplicated, hand-synchronised, or keyed on the wrong thing** — two edge maps updated by hand (G2), a
durability guarantee that depends on a marker write succeeding on a peer (G1), two serialization
regimes on one wire (G9), one clock for several waits with one message for all of them (G4). The
simplification target is a single identity-keyed transaction record with one owner per hop, so the
tests can assert invariants rather than reconstruct behaviour from distributed bookkeeping.
