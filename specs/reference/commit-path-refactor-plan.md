# Commit Path — Refactor Plan

> Last updated: 2026-10-12
> Status: Draft for approval
> Grounding: `commit-path-ground-truth.md` (findings G1-G10, experiment result), `ground-truth/duplication-inventory.md`,
> `ground-truth/dsm-commit-path.md`, `ground-truth/lane-{a,b,c}-*.md`

## Overview

The commit path is the crate's highest-churn and highest-bug-fix area, its structure is held acyclic
by one flat facade plus oversized modules rather than by clean boundaries, and its correctness is
carried by state that is duplicated, hand-synchronised, or keyed on the wrong thing. This plan orders
the work by *harm removed per unit of change*, gates each phase on an invariant test, and names the
experiment that proves each phase.

Two claims in the original review briefs were refuted during recon and must not be implemented:
the applier does NOT carry two production idempotency sets (one set, `apply.rs:923`; the pair is a test
double), and there are NO dependency cycles to break. The acyclicity is real and is bought by a facade
and by very large functions.

## Phases

### R0 — Split the durability journal out of `state_machine.rs` (harm first)

`state_machine.rs` hosts five responsibilities in one module: consensus core, the durability journal
(the `Applied` marker of G1), read-path/MVCC, recovery, and observability. G1 — an abandon that can be
a lie about durability — lives in the journal. Split the journal into its own module with one entry
point, so the marker write and the ack decision are one function under one test.

**Acceptance:** a test that fails the marker fsync for one replica and asserts the coordinator cannot
report `TxnAbandoned` while that replica holds a readable row. This test does not exist today.

> **Status (slice 1 landed):** the replica-side half is done —
> `handlers::tests::apply_acks_when_the_rows_are_durable_even_if_the_marker_fsync_fails` pins it (red
> first), with the marker write and the ack decision behind the one journal entry point
> (`applied_journal::record_applied_marker`). The remaining R0 work is the module split proper: the
> consensus core, the read-path/MVCC surface, recovery and the observability hook are all still in
> `state_machine.rs`. The coordinator-side assertion (that no timeout path can *report* the abandon)
> is R6, not this slice.

### R1 — One transaction record, in `ferrosa-common`

Today "a transaction" is spread across `waiting_on`, `waited_by`, `wait_start`, `applied`, `aborted`,
the state machine's `TxnState`, `FinalizedTxns`, the applier's `applied`, plus MVCC dedup, crash-replay,
the Raft ledger and the conflict index. Introduce ONE identity-keyed record — id, dependencies, phase,
owner-per-hop — and make every one of those a view or a cache of it.

In particular collapse `waiting_on`/`waited_by` into a single map (the DSM's recommendation), which
removes the asymmetry at `dep_wait.rs:219` by construction rather than by patching it.

**Acceptance:** the number of structures answering "did this transaction apply?" drops from eight to
one; the lockstep test at `state_machine.rs:1990` is extended to cover the MVCC and `committed_txns`
agreement it does not assert today.

### R2 — One write-set representation, owned by MVCC

One logical write-set is currently carried by 17 structures across five hops, a `Mutation` is
serialized twice (stage frame and MVCC envelope) and decoded at least three times, and there are two
independent spill implementations (`WriteSetStage`, `WriteSetSpill`).

Both halves already live in `ferrosa-postgres`: the uncommitted `TxnWriteSet` and the committed
`mvcc::State`. Implement the owner's stated intent — MVCC IS the concurrency buffer, holding a
transaction's changes until COMMIT and empty otherwise, spilling for large write-sets — as ONE
buffered representation between the front end and the Accord write-set.

**Acceptance:** the experiment below still reproduces the abandon, and after the fix a commit that is
reported abandoned leaves the table fully readable (per-node union equals the expected row count).

### R3 — Decompose `order_and_gate`

One function of roughly a thousand lines with cognitive complexity 342 drives the
PreAccept/Accept/Commit/Apply sequence. Decompose it by phase and by wire family so each phase is a
testable unit. (Measure precisely before and after; two measurements of its length disagree, so the
first task in this phase is the measurement.)

**Acceptance:** each phase separately unit-testable; the module-size and cognitive-complexity numbers
recorded before and after; the DSM re-run shows the same acyclicity.

### R4 — One wire format on the hot path

Multi-key Apply is capnp; PreAccept, Accept, Commit, ReadVote, single-key Apply and the fallback all
use bincode, and the multi-key Apply itself has three formats chosen by a runtime capability gate.
Per the repo rule, per-message hot-path serialization must be capnp. Migrate the control messages and
delete the capability-gated fallback.

**Acceptance:** a golden round-trip fixture per message type, and a test that the capability gate can
no longer select a non-capnp path.

### R5 — Three named facades instead of one flat one

`mod.rs` re-exports 88 items across 21 modules with fan-out 0 — a star rather than a structure.
Replace it with three named facades (protocol, driver, storage seam) so the dependency graph shows
real boundaries and `frg dsm` becomes usable evidence rather than a lower bound.

**Acceptance:** `frg dsm` no longer reports the facade as fan-out 0; the matrix shows clusters.

### R6 — One place decides a transaction's fate

No wall-clock constant may convert into a client-visible transactional outcome. The 10 s bound
becomes a liveness detector that alarms; the decision is an outcome of the transaction record's own
state (R1). The client message must name the actual condition rather than the legacy "dependency-wait
bound" misnomer, and the bare `bool` at `coordinator.rs:3205/3269` must carry which of timeout /
uncounted-ack / RPC-error occurred.

**Acceptance:** a test that the same failure condition always produces the same client-visible message,
and that no timeout path can return `TxnAbandoned`.


### R6a — the abandon decision must be evidence-based (now forced by the reproducer)

The reproducer proves a partial apply: after an abandoned COMMIT, two remote shards were readable
while the coordinator's own shard was not. So the rule "an abandon is truthful because no replica has
applied" is false in practice — the remotes apply before they ack, and a lost/slow ack is
indistinguishable from no ack inside the bound.

Required outcome split: zero apply acks -> the retryable abandon is truthful and stays; ANY evidence
that a replica applied -> the transaction is committed, the outcome is a non-retryable
committed-and-converging error, and recovery completes it. The client contract "safe to retry" must
never be emitted for a partially applied transaction.

**Acceptance:** `scripts/abandon-damage-repro.sh` returns PASS (every key absent, or every key present
and the client told it committed — never a mix), across two consecutive runs.

**Note:** R0 (`c085d5ad`) does not close this gate; the reproduce agent's expectation that it would
was wrong and is corrected in the ground-truth note.

> **Status (slice 1 landed, 2026-10-12):** the decision is now evidence-based, at the
> smallest reachable level. The apply fan-out returns `ApplyFanout { quorum_reached,
> peer_acks }` (distinct remote verified `ApplyOK`s; the coordinator's deferred self-ack is
> NOT counted), and `apply_phase_within` splits `!apply_ok` into **zero acks** (the truthful
> retryable abandon, unchanged) and **any ack** (a non-retryable committed-and-converging
> error that names the commit and does NOT call `finalize_no_write`). Red first, in process:
> `coordinator::tests::a_partial_apply_below_quorum_is_not_reported_as_a_retryable_abandon`
> (observed RED `Err(TxnAbandoned { timeout: 200ms })` on one verified ack of the two
> required), with `the_multi_key_apply_fan_out_reports_the_partial_apply_evidence` and the
> negative control `zero_apply_acks_still_abandon_truthfully`. **Not closed:** the live
> reproducer gate itself (`scripts/abandon-damage-repro.sh` must PASS on the cluster, two
> runs), and the zero-ack lie when a replica applies and ALL acks are lost — that needs a
> durable abandon decision the apply path consults, i.e. the rest of R6.

## The canonical experiment (must be run before and after R0-R2)

```
BEGIN; COPY 3 rows; <force the commit to fail>;   →   point-lookup a known key, assert ABSENT
```

Current behaviour: after an abandoned bulk commit, one node does not return its share of the table
(census 2/3 of expected; per-node read fails `received=0, data_present=false`; scans abort at 30 s with
`58000 ... one silent source must not stall the scan`). Never assert on a count — a partial apply
preserves a count and drops the tail. Always take the per-node union.

## Not to be cut

`wire.rs` and `transport.rs` are already good seams (recorded so a later refactor does not "tidy"
them); `apply.rs`'s size is body length, not a boundary problem; epoch/electorate have zero in-degree.

## Open questions carried forward

- Whether the silent node's share is absent (loss) or unreachable (locality/read defect).
- Whether G1's fsync-failed marker is the mechanism behind the observed 2/3.
- `mark_applied`'s asymmetry is structurally real but currently a defensive no-op on production paths;
  R1 removes it by construction, so no separate fix is owed.
- The `Mutation` header's five hand-written sites and their absent agreement test (unverified).
