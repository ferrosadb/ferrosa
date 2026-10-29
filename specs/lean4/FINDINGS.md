# Findings: inconsistencies between the Ferrosa specs and their own machines

Every item below is a theorem in `specs/lean4/`, not an opinion. The "evidence"
column names the theorem; `lake build` type-checks it.

Severity is about *consequence*, not about how hard the fix is:

- **P0** — a claim relied on for safety is false or unprovable as stated.
- **P1** — a documented behaviour differs from the specified/computed one.
- **P2** — vacuous evidence: an invariant that cannot fail, cited as if it could.

---

## A. Write path

### A1 (P1) — the default sync mode loses acked writes, and that is reachable

`ferrosa-storage/specs/data-flow.md` says it plainly:

> A write is durable only after its commit-log entry is fsynced. Under the
> default `Periodic` strategy that fsync is deferred up to `sync_interval`, so an
> acked write can be lost on an unclean stop (FMEA ST-1).

The spec is honest; the model confirms it is not a corner case.
**Evidence:** `WritePath.periodic_ack_can_be_lost` — a three-step run reaches
`acked = 1, synced = 0`. `WritePath.batch_ack_is_durable` is the other half:
under `Batch` the invariant `acked ≤ synced` holds for all reachable states.
**Consequence:** the ack path is a durability claim only under `Batch`. Any
front-end that documents "write acknowledged ⇒ durable" is wrong at the default.

### A2 (P1) — cell-level LWW is order-dependent on equal timestamps

`ferrosa-storage/specs/data-flow.md` § Read path: "merge_partitions — cell-level
LWW, newest-first". `ferrosa-cluster/specs/overview.md` invariant 3: "Repair
never silently picks a winner. Equal-timestamp content divergence is recorded as
a timestamp tie and surfaced."

These are consistent only if *reads* also surface ties. Per-replica resolution
cannot: with `[(5,a),(5,b)]` versus `[(5,b),(5,a)]` the winner differs by
position.
**Evidence:** `WritePath.lww_ties_are_order_dependent`.
**Consequence:** two replicas with identical cells in different physical order
disagree about the value, and nothing in the read path reports a tie. Either
resolution needs a deterministic breaker (value hash, node id), or the read path
must adopt the same "surface the tie" rule the repair path has.

### A3 (**retracted** — see the correction below) `LOCAL_QUORUM` and the global RF

An earlier version of this document claimed that `LOCAL_QUORUM` "can never
succeed" on a `{dc1: 3, dc2: 3}` keyspace because the generic path computes
`block_for(LOCAL_QUORUM, 6) = 4`.  That is wrong, and the error is worth
recording because it is a mistake the code invites:

- `write_path.rs:938` dispatches on the strategy: `Simple` →
  `coordinate_write_with`, `NetworkTopology` → `coordinate_write_nts`.  **NTS
  keyspaces never reach the generic path**, so the claimed `Unavailable` does
  not happen.
- `LOCAL_QUORUM` is a quorum of the *local* DC — `local_rf / 2 + 1`, i.e. 2 for
  a three-replica DC.  The global RF is not an input to a local level at all;
  there is no such thing as "global quorum" for `LOCAL_*`.
- `QUORUM` on an NTS keyspace is a quorum in *each* DC: `quorum_local_1 +
  quorum_local_2`.  For `{3,3}` that is 2 + 2 = 4, which is exactly what
  `block_for(QUORUM, 6) = 6/2+1 = 4` produces — the two agree *by arithmetic
  coincidence* when the per-DC RF is odd.

So the real findings are narrower, and they are about the *set* of acks rather
than the number:

**Evidence:** `Topology.ConsistencyLevels.localQuorumThreshold_eq` (the threshold
is `localRf/2+1`, with no global RF in the signature),
`Topology.ConsistencyLevels.global_rf_would_over_ask_2_2` and `_3_3` (feeding the
global RF to a local level asks for more than the local DC's quorum),
`Topology.ConsistencyLevels.quorum_2x3_matches_per_dc_sum` (the coincidence above),
`Topology.ConsistencyLevels.quorum_2x2_undershoots_per_dc_sum` (the coincidence
fails for even per-DC RF), `Topology.ConsistencyLevels.quorum_threshold_parity`,
and `Topology.TwoDc.global_rf_local_quorum_would_over_ask`.

**Revised consequences, in order of severity:**

1. *`{2,2}` under-asks.* `block_for(QUORUM, 4) = 3`, while `2 + 2 = 4`.  Three
   acks satisfy a level that is documented as a quorum in each DC — and the only
   way to reach three is both replicas of dc1 plus one of dc2, i.e. a quorum in
   dc1 and **not** in dc2.  This bites exactly at the two-DC `{2,2}` rung of the
   replication ladder.
   **Evidence:** `quorum_2x2_undershoots_per_dc_sum`,
   `quorum_threshold_allows_2_1_split`.
2. *The threshold is a count, not a set condition.*  `write.rs:753` compares
   `total_acks >= required` for every level except `EACH_QUORUM`, whose per-DC
   counters are the only ones consulted.  So even where the number is right
   (`{3,3}`, 4 acks), 3-from-dc1 + 1-from-dc2 is accepted without a quorum in
   dc2.
   **Evidence:** `quorum_threshold_allows_uneven_split`.
3. *`LOCAL_QUORUM`'s count is right and its set is not.*  `required` is computed
   from `local_rf` (2), but the comparison is again `total_acks >= required`, so
   1 local + 1 remote satisfies a level whose whole purpose is to be answerable
   during a WAN cut.  It behaves correctly in the common case (a healthy local
   DC supplies 3 acks anyway), but the guarantee is weaker than the name.
   **Evidence:** the counting rule above, plus
   `Topology.TwoDc.local_quorum_acks_can_be_disjoint` for what an unconstrained
   ack set means for consistency.

The fix in one sentence: compute a *per-DC required count* for every level that
names a DC (`LOCAL_*`, `EACH_QUORUM`, and `QUORUM` on NTS), and compare it
against `dc_acks[dc]` rather than `total_acks`.

### A4 (P2) — "CL is an ack threshold" is not a consistency guarantee

`ferrosa-cluster/specs/overview.md` invariant 1 reads as though meeting the
threshold were sufficient for correctness. The threshold only guarantees
read-your-write when `W + R > RF`, and nothing in the code enforces that: `block_for`
is computed per operation, so a caller may write at `ONE` and read at `ONE`.
**Evidence:** `Core.quorum_wr_intersects` (the positive theorem),
`Core.rf2_one_one_no_intersection` and `Topology.ThreeNode.one_one_is_not_read_your_write`
(the counterexample at RF=2), `Topology.ThreeNode.all_equals_quorum_at_rf2`.
**Consequence:** the invariant should either say `W + R > RF` explicitly or the
coordinator should reject combinations that violate it.

---

## B. `specs/tla/raft.tla`

### B1 (P2) — `NoTermAdvanceWithoutPreVoteMajority` is `True`

```tla
NoTermAdvanceWithoutPreVoteMajority ==
    \A i \in Server :
        state[i] = Candidate => preVotesGranted[i] \in Quorum(i)
        \/ TRUE  \* Optional gate — defaults true while W5.8 mirror is informational.
```

`P ∨ TRUE` is `TRUE` for every state. The comment admits it, but the invariant is
still listed in `raft.cfg` under INVARIANTS, so a green run would report it as
checked.
**Evidence:** `TlaCheck.prevote_guard_is_vacuous`.

### B2 (P0) — PreVote does not gate candidacy in the model

`BecomeCandidate` in `raft.tla` requires only `state[i] # Leader` and bumps the
term. `preVotesGranted` is `UNCHANGED`. So a node becomes a Candidate with an
empty PreVote set, which is precisely the disruption PreVote exists to prevent.
Production does gate this (`election_guard`, ADR-012); the spec does not.
**Evidence:** `TlaCheck.candidate_without_prevote_reachable`.
**Fix:** guard `BecomeCandidate` on `PreVoteSucceeds(i)`, then delete the `\/ TRUE`.

### B3 (P0) — "joint consensus" is joint on one server only

`ProposeMembership` writes `pendingConfig[i]`; `CommitMembership` writes
`config[i]`. No action ever replicates either to another server. So
`JointConsensusSafety`, which quantifies over a single server's `config` and
`pendingConfig`, describes the leader's local bookkeeping.
**Evidence:** `TlaCheck.config_never_propagates` — after committing
`Cnew = {0,1,2,3}` on server 0, server 1 still has `{0,1,2}`.
**Consequence:** the model cannot check the property ADR-013 relies on
(joint quorums in *both* configurations cluster-wide). `multi-dc.tla` repeats the
mistake at DC granularity (`config : DC -> SUBSET ServersPerDC`).

### B4 (P1) — a Leader may grant a vote and keep leading

`GrantVote(i, j)` requires only `state[i] = Candidate`, `currentTerm[j] <=
currentTerm[i]`, and the log up-to-date check. It sets `currentTerm[j]` and
`votedFor[j]` while leaving `state[j]` untouched (`UNCHANGED ... state ...`).
**Evidence:** `TlaCheck.leader_can_vote_without_stepping_down` — a state with
`role[0] = Leader`, `term[0] = 3`, `votedFor[0] = some 1`.
**Consequence:** `votedFor` and `state` can disagree, which real Raft forbids. Any
safety argument carried from this model to the implementation is carried across a
weaker machine.

---

## C. `specs/tla/multi-dc.tla`

### C1 (P0) — a transaction can be committed below the DC watermark

```tla
AccordCommit(t, hlc) ==
    ...
    /\ hlc \in 1..MaxHlc          \* no reference to hlcWatermark[d]
```

`AdvanceWatermark` then applies everything with `hlc <= w`, so a transaction
committed at a timestamp the DC has already applied *past* is applied immediately
and out of order.
**Evidence:** `AccordMultiDc.stale_commit_timestamp_reachable`.

### C2 (P0) — therefore I-27 "timestamp-ordered apply" fails in the module

Concrete run: commit `t1` at HLC 5, advance the watermark to 5 (so `t1` applies),
then commit `t2` at HLC 3, advance again → apply order `[t1, t2]` while the commit
timestamps are `5, 3`.
**Evidence:** `AccordMultiDc.apply_order_violation` (`applied = [1, 2]`,
`appliedOutOfOrder = true`).
**Consequence:** ADR-015 §I-27 and the cluster overview's "Apply phase … buffered
by HLC in a reorder buffer" are not what the model does. A correct version needs
`AccordCommit` to reject `hlc <= hlcWatermark[d]` (or to stall the watermark
until in-flight transactions land).

### C3 (P2) — `SwapDcDrainsAccord` (I-30) is vacuous

No action in the module assigns `pendingConfig` — there is not even a membership
action — so the antecedent `pendingConfig[d] /= {}` never holds.
**Evidence:** `AccordMultiDc.swap_dc_antecedent_never_holds`
(`∀ s, Reach s → s.pending = [[], []]`).
**Consequence:** checking I-30 proves nothing about DC swaps.

### C4 (P2) — `AccordIdempotence` (I-28) is a guard, not a property

`appliedAccord` is a `SUBSET`, so "applied twice" is not expressible. The
invariant only asserts `applied ⊆ committed`, which the `AccordCommit` /
`AccordAbort` guards already imply.
**Evidence:** `AccordMultiDc.applied_implies_committed`;
`AccordMultiDc.watermark_monotone_by_guard` makes the same point for I-27's
monotonicity, which is enforced by `AdvanceWatermark`'s own guard.
**Consequence:** none of the four "checked" invariants constrain the protocol.

### C5 (P0) — the per-DC Raft layer is a stub

The header claims the module "composes N copies" of `raft.tla`. It does not: there
is one `DcTermTick` that bumps a term, and nothing touches `commitIndex`, `log`,
or membership.
**Evidence:** `AccordMultiDc.per_dc_commit_index_never_advances`
(`∀ s, Reach s → s.commitIdx = [0, 0]`).
**Consequence:** ADR-015's Sprint-7 acceptance criterion — "TLA+ multi-DC model
checks at N=2 DCs × 3 voters; no safety violations" — cannot be satisfied by
this file. It would pass trivially, which is worse than failing.

---

## D. Cluster formation and the replication ladder

### D1 (P0) — "writes continue at CL=QUORUM with 1 of 3 nodes down" is false at RF=2

`specs/reference/cluster-formation-state-machine.md` T6a:

> Quorum still holds (e.g., 1 of 3 nodes down — 2 remain, quorum = 2).
> 3. Writes continue at CL=QUORUM (2 of 3 replicas available)

That assumes RF = 3. At RF = 2 — the rung the escalation actually climbs first —
`QUORUM` is both replicas, so a single node loss blocks the keys whose replicas
include it (roughly half the ring).
**Evidence:** `Topology.ThreeNode.rf2_has_no_fault_tolerance`
(*every* single-node failure blocks QUORUM for some key),
`Topology.ThreeNode.one_replica_down_blocks_quorum_write`,
`Topology.Ladder.fault_tolerance_table = [0,0,0,1,1,2]`.
**Consequence:** the formation doc's availability story holds from RF=3 upward
only. RF=2 with `QUORUM` is a no-redundancy configuration sold as redundant.

### D2 (P0) — `LOCAL_QUORUM` is an availability level, not a consistency level

During a WAN cut, DC0 can ack on `{0,1}` and DC1 on `{3,4}` for the same key: two
successful writes, no shared replica.
**Evidence:** `Topology.TwoDc.local_quorum_acks_can_be_disjoint`
(`mayMiss [0,1] [3,4] = true`), together with
`Topology.TwoDc.local_quorum_survives_wan_cut` and
`Topology.TwoDc.global_quorum_blocked_by_wan_cut`.
**Consequence:** ADR-015 S-48 ("each DC retains LOCAL availability") is correct
about availability and silent about divergence. Reconciliation falls to LWW —
which A2 shows is order-dependent on ties. A `LOCAL_QUORUM` write should either
be documented as last-writer-wins-with-undefined-order, or routed through Accord.

### D3 (P1) — "RF = quorum" is quorum over replicas, not over the cluster

At 5 nodes and RF=3, `block_for(QUORUM, rf)` is 2 — two of three replicas, not
three of five nodes. A read that happens to consult a cluster majority
`{2,3,4}` can hold only replica 2, which may not have acked.
**Evidence:** `Topology.FiveNode.quorum_is_over_replicas_not_cluster`
(`blockFor .quorum 3 = 2 ∧ 2*2 < 5`),
`Topology.FiveNode.cluster_majority_read_can_miss_replica_write`,
`Topology.FiveNode.quorum_and_cluster_majority_differ`.
**Consequence:** any doc or operator runbook that equates "quorum" with "majority
of nodes" will mis-size availability and mis-read staleness.

### D4 (P1) — Pair mode's no-split-brain claim depends entirely on the operator gate

`cluster-formation-state-machine.md` T9: "Split brain is impossible: only one
node can be promoted, and connection direction determines roles on reconnect."

That is true of the *gated* system and false without the gate.
**Evidence:** `Topology.Pair.no_two_writable` (gated: at most one writable
primary in every reachable state),
`Topology.Pair.promotion_requires_operator_and_dead_peer` (promotion needs
`op = true` *and* the peer confirmed down),
`Topology.Pair.unguarded_promotion_splits_brain` (drop the gate → both nodes
writable, reachable),
`Topology.Pair.returning_primary_becomes_secondary` (the connection-direction
rule, verified).
**Consequence:** the prose attributes safety to connection direction; the model
shows the load-bearing part is the "primary confirmed dead" assertion. If
`ferrosa-ctl promote` can be run against a live-but-partitioned primary, the two
writable primaries are one operator mistake away — which is exactly what T9's
own "If both nodes were partitioned and both had writes" paragraph concedes.

### D5 (P2) — the ladder's fault tolerance is not monotone in the way it reads

`fault_tolerance_table = [0, 0, 0, 1, 1, 2]`: the 3-node RF=2 rung buys nothing
over pair mode in availability terms, and the 5-node RF=3 rung buys exactly what
the 2-DC rung buys. Only the 3-DC RF=6 ring tolerates two.
**Evidence:** `Topology.Ladder.fault_tolerance_table`,
`Topology.Ladder.three_nodes_is_the_first_autonomous_rung`,
`Topology.Ladder.pair_has_no_quorum`.
**Consequence:** the natural reading of "escalate to more nodes for more safety"
is right about *autonomy* (3 nodes is the first rung where Raft can elect without
an operator) and wrong about *durability per write*.

---

## E. What checks out

Worth recording, so the findings above are not read as "nothing works":

- Quorum intersection is a single counting argument and it holds for every rung:
  `Core.quorum_intersects`, `Topology.Ladder.no_two_disjoint_quorums_at_any_rung`.
- `W + R > RF` holds for QUORUM/QUORUM at every rung in the table:
  `Topology.Ladder.read_your_write_holds_at_every_rung`.
- Escalation never widens a quorum's share of the replica set:
  `Topology.Ladder.quorum_share_never_grows`.
- A single DC loss keeps QUORUM for every key at NTS `{2,2,2}`:
  `Topology.ThreeDcRing.single_dc_loss_keeps_quorum_for_every_key` — this is the
  property ADR-015 is buying, and it does hold.
- The ring tolerates one link loss while staying fully connected, and any two
  losses leave at least one cross-DC path:
  `Topology.ThreeDcRing.ring_survives_one_loss_fully`,
  `Topology.ThreeDcRing.two_link_loss_isolates_at_most_one_dc`.
- Storage: the commit log only grows (`log_append_only`), no ack outruns the log
  (`acked_le_logged`), every memtable row is in the log (`active_subset_log`),
  flush carries every row into the published SSTable (`flush_keeps_rows`),
  quarantine keeps both the rejected row and its neighbours
  (`quarantine_retains_row`, `quarantine_keeps_others`), and LWW apply is
  idempotent (`mergeCell_idempotent`).

## G7 — LWT is a CAS, and an unprotected one is G2 again

`INSERT … IF NOT EXISTS`, `UPDATE … IF`, `DELETE … IF EXISTS` are compare-and-set:
read a value, decide, write conditionally.  Three claims, all machine-checked in
`Cql/Lwt.lean`:

**The guarantee.** `at_most_one_applies` — starting from a cell holding `v0`, a
CAS `IF k = v0 SET k = va` applies and a second CAS with the same expectation then
fails, because the value it tests is now `va`.  `at_most_one_applies_either_order`
says the same for both orderings, so the outcome does not depend on which client
got there first.

**The failure contract.** `failed_cas_reports_the_state_it_tested` — a failed CAS
reports the value it tested, so the `CurrentValues` a client receives is the value
the next transaction in the serial order sees.  `failed_cas_observes_the_committed_value`
checks that on a concrete history: the `:info` result carries `[[1]]`, exactly what
the committed CAS wrote.

**The anomaly.** `unprotected_cas_is_not_serializable` — a CAS whose read set omits
its condition key (condition on `readKey`, write `writeKey`) produces, for two
concurrent clients, the write-skew history of G2: A reads k1 and writes k0, B reads
k0 and writes k1, both observe `[]`, and no serial order explains both.  The
protected version of the same pair, where B's read set includes A's write key,
serializes in timestamp order (`protected_cas_is_serializable`), and a two-CAS run
where the second fails is serializable with only the first in the serial order
(`cas_with_failure_is_serializable`).

**Consequence.** There is no separate LWT consistency argument.  A protected CAS
has `readKey = writeKey` (`protected_cas_reads_its_write_key`, `Lwt.ops`), which is
what puts it inside G1's `DepComplete` hypothesis; an unprotected one sits outside
it and is refuted.  The front-end obligation is therefore precise and checkable:
**every column appearing in an `IF` clause must be in the transaction's read set**
(`Cql.Classify.conditional_reads_its_keys` is the shape; the read set is built from
those columns).  A planner that drops the condition column from the read set turns
a CAS into a lost update.

## G8 — collections: the classifier is blind to the assignment terms

`UPDATE t SET tags = tags + {'x'} WHERE k = 1` has no `IF` clause, so `Stmt.blind`
calls it blind (`Collections.collection_append_classified_blind`).  It also cannot be
executed without reading `tags` (`Collections.collection_append_must_read`).  Both are
machine-checked, and together they are the finding
(`blind_classification_is_term_blind`): **`blind` is a property of the `IF` clause
alone, and a collection update is a read-modify-write with no `IF` in sight.**

What that costs depends on the type, and the distinction is the actionable part:

* **`SET<v>` / `MAP` are safe.** A set add is commutative and idempotent
  (`setAdd_comm`, `setAdd_idem`), so a last-write-wins merge of two concurrent adds
  is a union and loses nothing (`set_lww_is_order_free`, `set_lww_loses_nothing`).
* **`LIST<v>` is not.** Two concurrent appends merged by last-write-wins keep one
  element where serial execution keeps two (`list_lww_loses_an_element`), and the
  appends do not commute at all (`listSnoc_not_comm`).

The scalar case is genuinely blind (`scalar_assignment_is_genuinely_blind`), which is
why the fix belongs at the term level rather than by disabling the fast path:
`Stmt.blindSem := blind && !readsOwnColumn` rejects the collection append
(`blindSem_rejects_collection_append`), accepts the scalar assignment
(`blindSem_accepts_scalar`), and is a restriction of the implemented classifier
(`blindSem_is_stricter`), so tightening it cannot open a path that was closed.

## G9 — secondary indexes: readiness is a preference, not a guarantee

A secondary index is maintained by a background build over SSTables
(`ferrosa-storage/src/index/{scheduler,tracker}`), not inside the row write.  The
coordinator's answer is `select_index_ready_replicas`
(`ferrosa-cluster/src/coordinator/read.rs:51`), whose doc comment already says
"Returns all replicas (never filters)".

Formalised, that is:

* `Index.reorder_never_filters` — a replica is selected exactly when it is in the
  replica set, so a replica still building the index remains eligible;
* `Index.first_segment_is_ready` — the preference is real, just not a filter;
* `Index.all_building_returns_all` — the all-building case returns the input
  unchanged, which is what the Rust test of that name pins down;
* `Index.preference_is_not_completeness` — there are replica sets for which the
  selection returns exactly its input and nothing is ready, so the caller gets no
  signal that the answer may be incomplete;
* `Index.indexed_read_can_be_stale` — a strictly-serializable violation: a read that
  follows a committed write and does not observe it (`hIndexedStale`).  The fresh
  version of the same history passes (`indexed_read_when_fresh_is_serializable`), so
  the checker is discriminating rather than rejecting everything.

Index *entries* are not the hazard: they are keyed by `(value, primary key)` and
maintained by set operations, so concurrent inserts commute
(`Index.index_entries_commute`).  **Removals** are: they need the old row's value to
know what to remove (`setRemove_not_comm_with_add`), so the update path is a
read-modify-write again — the same shape as G8.

## G10 — the Cell/State bridge is now a refinement, not an analogy

`Lwt.lean` proved the CAS algebra on a scalar register and exhibited the anomaly on
the append-list keyspace.  `Refine.lean` connects them:
`encode`/`decode` are a section and a retraction (`decode_encode`) and the
abstraction is well defined (`decode_unique`) — so "the cell's value" is not one
choice among several; `simulates_append` shows the keyspace transition refines the
cell transition; and `lwt_refines` shows a whole LWT refines its cell-level
execution in both branches.  The CAS guarantee and the history check are therefore
statements about one system.

One asymmetry worth stating: `encode` is *not* preserved by appending (the list keeps
history a scalar cell forgets), which is exactly why the relation is
`Simulates`-after-`decode` rather than state equality.

## G11 — aggregates: the red list

`Cql/AggregateObligations.lean` is a machine-checked ledger of what the aggregate spec
owes and does not yet have. Six of ten items are red, and the four that are not
implementation gaps are the ones worth reading:

* **`COUNT(DISTINCT)` does not decompose across shards.** Summing per-shard counts
  over-counts: one value on two shards is one distinct value and two counted ones
  (`sum_of_shard_counts_overcounts`, `shard_sum_overcounts_by_the_overlap`). A
  distributed version must merge per-shard *sets*, not counts.
* **A Bloom filter cannot be merged into a count.** `[0]` and `[1, 2]` leave identical
  bit arrays — `{0, 1}` — while counting one and two (`bloom_bits_do_not_determine_the_count`).
  So OR-ing replica filters cannot yield a merged estimate: it needs a mergeable sketch
  (HLL) or a re-scan. Summing per-shard estimates over-counts the overlap
  (`sum_of_estimates_overcounts`), and merging bits loses the count outright.
* **The estimate's relative error bound is unproved.** `estimate_le_exact` gives
  monotonicity; the quantitative `estimate ≥ (1 - fpr)·exact` depends on the filter's
  fill, which the runtime reports and the model does not constrain. `exactness_does_not_hold`
  refutes the `ε = 0` version, so no exactness claim can stand in for it.
* **The model of exact distinct is not O(1) in memory.** `model_visited_set_length_is_the_count`
  shows the model holds one entry per distinct value. The Rust `DistinctExact` is O(1);
  the refinement from model to implementation is unproved. That is a modelling debt, not
  a bug — but it must not be reported as discharged.

The two surface items — no AST node and no planner arm for either function, `COUNT`
recognised by name-matching in the router — are implementation gaps with no Lean
witness, and are marked `failing` with `witness: none`.

`redCount` is asserted to be 6 (`ledger_has_six_red_items`), so implementing an item
without updating the ledger breaks the build — the red list can only shrink on purpose.

## F. Suggested order of attack

1. **C1/C2** — put the watermark guard on `AccordCommit`. This is a real
   protocol gap, not just a spec gap, and it is a two-line change in the model.
2. **A3** — for every level that names a DC, compare `dc_acks[dc]` against a
   per-DC required count instead of comparing `total_acks` against a global one.
   That single change closes the `{2,2}` under-ask and makes "2 local + 2 remote"
   true by construction rather than by arithmetic coincidence.
3. **G2/G4** — treat dependency completeness as a load-bearing invariant: any
   code path that can commit without recording a conflicting lower-timestamp
   transaction is a serializability bug, not an optimisation gap.
4. **B2/B3** — make `raft.tla` model PreVote and membership propagation, or
   retire it in favour of the Lean model; as it stands it cannot be used as
   evidence for ADR-012 or ADR-013.
5. **D1** — correct T6a in the formation doc to state the RF assumption, and
   say what RF=2 actually tolerates (nothing).
6. **A2/D2** — decide whether LOCAL_QUORUM divergence is acceptable, and if so
   give LWW a deterministic tie-breaker.

## G. Accord and Jepsen serializability

First, a separation that matters for where the proofs live.  There are two
different acknowledgements:

* the **consistency-level ack** — `acks ≥ blockFor(CL, rf)` over the replica set.
  This is topology, not Accord, and it is specified in
  `Topology/ConsistencyLevels.lean` (`RF=1` acks on the first ack at every level:
  `one_ack_suffices_at_rf_one`; a multi-statement transaction acks when every key
  has met its own CL: `txnAckOk_iff`).  It holds for a plain fan-out write with no
  consensus protocol anywhere.
* the **transaction ack** — Accord's commit decision plus the dep-wait before the
  read phase, which is what this section is about.

The correctness statement is the one Elle checks: a history is **strictly
serializable** when some total order of the committed transactions replays to
exactly the observed read results *and* respects real time.  Both halves are
defined in `Accord.Serializable` and both are decidable, so a history can be run
through them.

### G1 (P0, positive) — dependency completeness plus dep-wait is sufficient

**Statement.** If every conflicting transaction with a lower timestamp is in
`deps`, and no replica applies a transaction before its lower-timestamped
dependencies, then every reachable apply interleaving produces a serializable
history, with timestamp order as the serial order.

**Evidence.** `Accord.Protocol.LogOk.dep_applied_before` (the guard's
consequence, proved by induction on the log), `cfgScenarios_length` (3 of the 6
permutations survive the guard), `cfg_dep_ordered` (the conflicting pair is
ordered in every surviving log), `all_scenarios_serializable` and
`all_scenarios_strict` (both checkers accept every one, including the
interleavings where a non-conflicting transaction applies first),
`no_deps_all_orders_admissible` (6 of 6 survive once the edge is removed),
`foldl_apply_store` (a replica's state is the fold of its log).

**What is *not* proved:** the general statement "conflicting transactions appear
in timestamp order in any dep-respecting log" is recorded as the exhaustive
`cfg_dep_ordered` over the configuration in play rather than as a universally
quantified corollary. `dep_applied_before` is the general lemma it would follow
from; closing that last step is routine list-index arithmetic, not a question
about the protocol.

### G2 (P0) — a dropped dependency produces a non-serializable history

**Evidence.** `Accord.Recovery.write_skew_not_serializable` — two transactions
that read each other's key before the other wrote it: A reads k1 (sees `[]`) and
appends to k0; B reads k0 (sees `[]`) and appends to k1.  No order explains both
observations.  `dep_guard_restores_serializability` — the same pair with B's
dependency on A intact, where B observes A's append, is strictly serializable.
`write_skew_not_strict` — the unguarded history fails the strict checker as well,
so this is not a real-time subtlety.

**Consequence.** `DepComplete` is not a bookkeeping detail of the optimisation;
it is the whole of the serializability argument.  Anything that drops a
dependency (a recovery phase that misses an in-flight transaction, a reorder
buffer that fires early) lands exactly here.

### G3 (P0) — one ballot number is unsound, as §6.1 says

**Evidence.** `Accord.Recovery.recovery_correct` — with separate
`promBallot`/`accBallot`, recovery proposes the value chosen at ballot 3
(`some (3, 2)`).  `recovery_diverges` — with the conflated acceptor, a Prepare
at ballot 5 relabels a stale value as accepted at 5, and recovery proposes
`some (5, 1)`, a value no quorum chose.  `recovery_wrong_value` — the two choices
differ.  `conflation_relabels_without_accept` — the precise defect: the accepted
ballot moved from 1 to 5 while the value did not, so `(5, some 1)` is a claim
about a ballot at which nothing was accepted.  `correct_ballot_invariant` — the
relation the correct acceptor preserves (an accepted value is always recorded at
a ballot it had already promised).

**Reduction note.** §11.2's 24-step test interleaves client requests around this
acceptor triple.  The divergence is already present at the acceptor level, so the
model exhibits it there; the extra steps test *reachability* in a fuller system,
not the safety argument.

### G4 (P1) — the reorder buffer is what makes `DepComplete` true, and it is an empirical premise

**Evidence.** `Accord.Strict.deadline` transcribes §7.1 exactly;
`deadline_ge_t0`, `deadline_monotone` are its arithmetic.  `DeadlineSound` is
§7.1's guarantee *as a hypothesis* — it holds only while observed skew and
latency stay inside the P99.9/P99 percentile windows the formula is computed
from.  `deadline_yields_dependency` is the consequence: under that premise, every
in-flight conflicting transaction with a lower timestamp is in the buffer when a
transaction is processed, which is precisely the in-flight half of
`DepComplete`.  `missing_dependency_means_premise_failed` is the contrapositive
an operator needs: if a dependency is missing, the deadline was computed from
stale estimates.

**Consequence.** The buffer is not an optimisation for latency; it is a
correctness dependency of the serializability argument.  Disabling it (or
sizing it from NTP bounds rather than measured skew) invalidates G1's hypothesis
rather than merely slowing the fast path.

### G5 (P1) — strictness is a property of timestamp assignment, and the checker sees the difference

**Evidence.** `Accord.Serializable.hStaleRead_serial_but_not_strict` — a write
that completes at 10 µs and a read invoked at 20 µs that observes the pre-write
state is serializable (order the read first) but not strictly serializable.
`zero_times_fail_strict` — the same scenario family with all timestamps zero is
rejected by the strict checker; `all_scenarios_strict` — the same family with
realistic completion times is accepted.  `strict_iff` — the two predicates relate
as `Serializable ∧ realTimeOk`.

**Consequence.** "Jepsen says serializable" and "Jepsen says strictly
serializable" are different claims about Accord, and only the second one is what
`elle_check.clj` asks for (`:consistency-models [:strict-serializable]`).  The gap
between them is exactly the `- Latency(C, P)` term in the deadline.

### G6 — the checker on a real history

`.wt-pr457-linux/deploy/fly-accord-elle/elle-cert-e860ec88-20260724T181744Z.edn`:

```
parsed: events=4800 committed=2400 failed=0
serializable(first 4 committed, search over 4! orders): true
serializable(first 5 committed, search over 5! orders): true
```

That is `Accord.Edn`'s parser and `Accord.Serializable`'s search, run through
`lake env lean --run Ferrosa/Accord/RunCheck.lean`.

**Limits, stated plainly.** The windows are four and five transactions because
the search is over permutations; the whole-history form of the check (supply the
completion order and replay all 2400 transactions) is implemented as
`orderOk`/`serializableBy` but does not finish inside the interpreter's time
budget at that size, so it is not part of the default run.  Elle answers the same
question with a graph-based heuristic precisely because the exhaustive form does
not scale.  A `true` from the window check is a certificate; a `false` would not
be evidence of a bug.



## G12 — crash recovery: what the commitlog format can and cannot detect

The record layout in `ferrosa-storage/src/commitlog/mutation.rs` is self-describing —
length prefixes everywhere — and carries **no checksum**. That splits the integrity
question in two, and only one half is answered:

- `Recovery.truncation_is_detected` — a declared length that runs past the file is caught.
- `Recovery.no_checksum_detects_only_truncation` — two different mutations of the same
  serialized size are indistinguishable to the reader. A payload bit flip is accepted as
  a valid record and the state diverges silently. Quarantine cannot substitute for a CRC:
  by the time the bytes are read, nothing says they are wrong.

Replay policy is the second half, and it is not a style choice. A torn write means the
file ends mid-record, so the bytes after the tear are that record's tail, not the next
record's header. `Recovery.stop_replays_a_prefix` proves the stop policy replays a prefix
of what was written; `Recovery.skip_does_not_replay_a_prefix` shows the skip policy
applying a mutation decoded from bytes that were never a record boundary. Skip-and-continue
is not a recovery strategy.

Durability is a configuration, not a law. `Recovery.acked_survives_crash` holds with the
`SyncMode.onAck` premise (`Recovery.only_on_ack_implies_durability` says only that mode
discharges it), and `Recovery.periodic_sync_loses_acked_writes` is the same arithmetic
under periodic sync: three records written, one synced, the second acknowledged, the
process dies — the client was told the write succeeded and it is gone.

Replay is idempotent only for mutations that are. `put` and `del` are
(`put_is_idempotent`, `del_is_idempotent`); `inc` is not (`inc_is_not_idempotent`). Since
`mutation_id` is the only dedup key and the code states that a zero id is *never*
deduplicated, a legacy segment — or any producer that leaves the field zero — double-applies
counters (`zero_id_replay_duplicates_a_nonidempotent_mutation`).

Corruption handling is specified as a policy table rather than an implementation:
`Recovery.only_quarantine_preserves_data`. `quarantine` keeps the only copy; `delete`
destroys it; `ignore` is grouped with `delete` because a file that is never read again is
equally gone from the application's point of view, and nothing can ever repair it. The tree
has the pieces (`quarantine.rs`, `self_heal/`) but no theorem connects them to this
obligation yet, so the item is red.

## G13 — tombstones and TTL: the tie-breaks are requirements

Three facts about the stored shapes decide this section. TTL is materialised as a stored
local-deletion-time, not recomputed at read, which is the right design and is why
`Tombstone.expiry_depends_only_on_the_stored_ldt` holds: a replica with a wrong clock still
expires the cell at the same instant, because it compares against a number the coordinator
wrote. The corollary is `past_timestamp_expires_on_arrival` — the number is derived from the
*write* timestamp, which a client may supply with `USING TIMESTAMP`, so an old timestamp plus
a TTL produces a cell that is already expired when it lands.

The second fact is `i32`. A cell's local deletion time is a signed 32-bit second count, so it
crosses `2^31` in 2038 and any TTL that reaches past that wraps negative — and a negative
local deletion time reads as *already expired*. `Tombstone.ldt_overflows_before_2038_reachable_now`
is concrete: a cell written at epoch 1.8e9 with a 5e8-second TTL (~16 years) stores a negative
value and is invisible to every reader, while the writer was told it would live for years.
Twelve years is enough to hit this; it is not a 2038 problem in the sense of "later".

The third is the resolution rules. Two tie-breaks are needed and neither is stylistic, so both
are exhibited rather than asserted:

- `Tombstone.ts_only_merge_does_not_converge` — resolving on the timestamp alone leaves two
  replicas holding different winners forever, because `USING TIMESTAMP` lets two clients pick
  the same timestamp.
- `Tombstone.value_wins_ignores_a_delete` — breaking that tie on the value alone converges, and
  both replicas agree, and both are wrong: the delete silently loses. `tombstone_tiebreak_resolves_a_delete`
  is the same pair resolved correctly. Convergence without the tombstone rule buys nothing.

Purge is the last item and the one with a deadline on it. There is no purge path in the tree
and `gc_grace` exists only as a display default (ten days), so absence is monotone
(`no_purge_preserves_absence`) and resurrection is impossible — at the cost of unbounded space.
The condition for adding one is specified so that adding it cannot go wrong:
`purge_safe_after_all_replicas_applied`, and `purge_requires_all_replicas_applied` is the
premise restated as a refutation — a replica still holding an older value is exactly the case
`purge_before_propagation_resurrects` turns into a permanently undone delete, because after the
tombstone is gone, read repair copies the old value back and no tombstone remains to beat it.

## G14 — BTI cannot promise recoverability, and the framing is why

Three properties the BTI format cannot provide, each grounded in `ferrosa-sstable` and each
inherited from Cassandra rather than introduced here:

1. **No checksum over any index structure.** The only CRCs are per compressed chunk and
   `CRC.db` over the data extent; `Partitions.db`, `Rows.db` and the trie blocks have none. A
   flipped byte in an internal trie node parses, walks, and redirects every key under that
   subtree. A flat BIG index would damage one partition; a prefix-compressed trie has no such
   bound. `Format.trie_cannot_produce_a_wrong_value` is the reader-side answer — the record
   carries its own key, so a mis-resolved offset is rejected — but the format still cannot
   *detect* the corruption, only refuse to act on it.
2. **The index is an authority, not an accelerator.** `validate_data_extent` compares the
   footer's count against a linear walk, one-directionally (`reachable < expected`). A trie
   that lost entries while the data stayed intact passes and then silently misses partitions.
   Cassandra's answer is `sstablescrub`: offline, O(N), and CASSANDRA-9591 explicitly made it
   work with `Index.db` *missing* — that is an admission that the data is the authority, made
   in a tool rather than in the format.
3. **A torn file yields nothing.** The trie root and footer are written at close, so a partial
   `Partitions.db` does not open. Data survives only because `salvage` enumerates by walking
   `Data.db` (`next_partition_count`) instead of consulting the index — an exception in the
   reader, not a property of the file.

`Format.lean` specifies the framing that makes all three properties hold at the file level
rather than at the tool level: per-record `len | crc32c`, per-trie-block CRC, a checksummed
footer carrying the count and token bounds, and three file states instead of two. The trie
stays, because the record-key comparison already bounds what a corrupt trie can do, and the
fast path is the reason to have an index at all
(`Format.trie_reads_at_most_one_record` vs `rebuild_reads_every_record`). See ADR-022 for the
layout, the per-table `sstable_format` switch, and the BTI export path by compaction.

Two obligations remain red and are implementation, not specification: flush resumption, and
the per-table format dimension in `SSTableWriter`/`SSTableReader`. A third red item is the
performance gate — the CRC budget is a measurement against BTI on the same data, not a theorem.

## G15 — SQL: the surface promises a transaction the store cannot commit

`ferrosa-postgres` answers `BEGIN`/`COMMIT` with `ReadyForQuery(TransactionStatus::InTransaction)`,
so a driver concludes a transaction is in progress. Statements execute independently, so a crash
mid-transaction leaves a strict non-empty prefix of the statements applied —
`Sql.commit_is_not_atomic_across_statements` — an outcome SQL does not permit. The status message
is a fact about the session, not about what is durable (`status_is_not_a_durability_claim`).

Two semantic differences survive even without a crash, and both are hidden by tests that never use
NULL:

- **Three-valued logic.** Translating `NOT (b = v)` to `b <> v` admits rows where `b` is NULL;
  SQL's predicate is UNKNOWN and the row must not be selected (`null_passes_a_naive_not_equal`).
  The two translations agree whenever no NULL is involved, which is why the bug is easy to ship
  and hard to see.
- **Empty-set aggregates.** `SUM` over no rows is NULL, not 0 (`sum_of_no_rows_is_null_not_zero`).
  A dashboard that folds the result into a running total silently reads "no data" as zero.

Two things that are *not* bugs, checked so they are not "fixed" into bugs: `COUNT(*)` and
`COUNT(col)` differ exactly on NULL by design (`count_star_and_count_col_disagree`), and a
sequence need only be monotone, so the commitlog's non-idempotent counter replay
(`Storage.Recovery.zero_id_replay_duplicates_a_nonidempotent_mutation`) is harmless for `SERIAL`
and harmful only for `COUNTER` (`sequence_need_only_be_monotone`).

## G16 — Bolt: replies cannot be paired to messages

The session state machine is right where it is specified: only RESET (or GOODBYE) leaves FAILED
(`Bolt.failure_is_sticky`), and RESET restores the initial state exactly, queued streams included
(`reset_clears_streams_and_failure`). What the protocol cannot provide is the property every naive
driver assumes — that the *n*-th reply belongs to the *n*-th message. A message sent while FAILED
produces no reply at all, so the stream shortens and shifts underneath the client:
`replies_are_not_one_per_message` shows the same four messages producing four replies from a ready
session and two from a failed one, and `ignored_messages_shift_every_later_reply` is the cumulative
version. This is a property of Bolt, not of this implementation, and it cannot be fixed at the
server — only documented, or avoided by not pipelining.

Two consequences worth naming. `FAILURE` carries no statement identity, so in a pipelined session
the failing statement is exactly what the client cannot recover. And RUN without PULL accumulates
result streams with no protocol backpressure (`unconsumed_runs_grow_the_queue`), so the only bound
on server-side memory is a server-side limit. DISCARD releases a stream without reading it, which
is why it must exist.

## G17 — SPARQL: paths are paths, not pairs

The graph is a bag, so a duplicated triple doubles every solution
(`Sparql.a_duplicated_triple_double_counts`), and adding a triple is monotone — it never removes a
solution (`adding_a_triple_never_removes_a_match`) — while deleting one is not
(`deleting_a_triple_can_remove_a_match`). That asymmetry is the whole reason incremental ingestion
is sound and incremental deletion is not.

Property paths are where the semantics diverge from the implementation. `p+` is the transitive
closure and the specification returns each `(subject, object)` pair **once**; an expansion that
follows every path without a visited set returns one row per *path*
(`closure_without_a_visited_set_duplicates_pairs`: a two-node cycle yields three rows for two
pairs, and `a_self_loop_diverges_without_fuel` shows the self-loop case, where the fuel bound is
the only thing that stops the walk — a bound, not a semantics).

RDF-star adds a storage-level hazard rather than a semantic one: a quoted triple is a term, and the
natural numeric packing of a term aliases a quoted triple with an IRI —
`encoding_is_not_injective`, `Term.quoted 1 2 3` and `Term.iri 1002003` share a key. A store keyed
that way returns rows for a lookup that must not match. The fix is a tagged key (a discriminant
plus a payload), and it has to be in the key encoding, not in the query layer.

Listed red rather than claimed: `OPTIONAL` scoping. The difference between
`{ A OPTIONAL { B FILTER(c) } }` and `{ A OPTIONAL { B } FILTER(c) }` is the subtlest rule in
SPARQL 1.1 and the plan types carry no representation of it, so there is nothing to prove yet.

## G18 — the layer beneath: what the SQL surface would have to be given

Three obligations, each stated so the layer has something checkable to satisfy rather than an
adjective to honour.

**O(1) is achievable for equality and impossible for ranges.** A hash-partitioned index names
exactly one index partition for a value, so an equality probe is one node contact
(`Sql.Index.equality_contacts_one_node`) and one record (`index_lookup_beats_a_scan`). A range
cannot be: two adjacent values already hash to two nodes (`range_lookup_cannot_be_constant`), so
the honest bound is `O(nodes)` scatter-gather or `O(log N + k)` on a single ordered node
(`only_equality_is_constant`). Any specification that promises O(1) for range predicates on an
indexed column is promising something the index cannot deliver, and the correct response is to
choose which of the two bounds the workload needs.

**ACID is four obligations with four different owners.** Atomicity is *one durable decision obeyed
by every participant* (`Txn.one_decision_is_atomic`) — not ordered writes, and not a status message;
independent execution leaves a strict prefix (`independent_execution_is_not_atomic`), which is what
the surface does today. Durability is the commitlog premise `Storage.Recovery.acked_survives_crash`
moves from the data record to the decision record. Isolation is a *price*, not a property:
snapshot isolation is cheap and admits write skew — two transactions that each read the other's
column, both commit, and the invariant "at least one on call" is broken where a serial order would
have kept it (`snapshot_isolation_permits_write_skew`) — and serializable needs the read set, via
SSI or Accord's dependency tracking (`serializable_requires_the_read_set`). Consistency is the
application's, and is preserved only because I and A hold.

**MVCC and O(1) are one decision, not two.** The `fnf` cell holds one value. So either older
versions live in older SSTables — and a snapshot read probes every overlapping file
(`Mvcc.a_version_per_file_is_not_a_constant_read`), so the O(1) of the previous paragraph is gone —
or the versions live inside the record as a chain bounded by `chainBound = liveSnapshots + 1`
(`in_record_versions_keep_the_probe_constant`). Compaction lowers the number of files but never
bounds it, so it cannot rescue the first option.

The GC rule that makes the second option work is not the obvious one. "Discard everything older
than the oldest live snapshot" destroys the version a snapshot still needs, because the version a
snapshot needs is the *newest version at or below it*, which may itself be older than the oldest
snapshot: with versions at ts 100 and ts 5 and an oldest snapshot of 10, a reader at snapshot 50
needs ts 5, and the naive rule has already dropped it (`naive_gc_breaks_a_live_snapshot`). The safe
rule keeps a floor — the newest version at or below the oldest snapshot — and everything newer
(`safe_gc_preserves_live_snapshots`), which is the same shape as
`Storage.Tombstone.purge_requires_all_replicas_applied`: a collection rule that is sound only under
a precondition about what readers still need.

## G19 — consistency as configuration, and what each level actually buys

The SQL surface exposes four levels with QUORUM as the default
(`Consistency.default_is_quorum`). The arithmetic is not new: it is `blockFor` / `blockForDc` from
the topology layer with a narrower surface (`sql_levels_agree_with_the_topology_rule`), which
matters most for LOCAL_QUORUM — it takes the *local* replication factor, so a 6-replica keyspace
with 3 replicas per datacenter needs 2 acknowledgements, not 4. That row is in the acceptance
table as the regression test.

The table is the specification the tests read:

| level | global RF | local RF | acks | failures tolerated |
|---|---|---|---|---|
| ONE | 3 | 3 | 1 | 2 |
| QUORUM | 3 | 3 | 2 | 1 |
| LOCAL_QUORUM | 3 | 3 | 2 | 1 |
| ALL | 3 | 3 | 3 | 0 |
| ONE | 6 | 3 | 1 | 5 |
| QUORUM | 6 | 3 | 4 | 2 |
| LOCAL_QUORUM | 6 | 3 | 2 | 4 |
| ALL | 6 | 3 | 6 | 0 |

Three results bound what the levels can promise, and each is a red item because the surface
currently offers the level without the consequence being stated:

- **QUORUM reads see QUORUM writes on RF=3** (`quorum_reads_see_quorum_writes_rf3`, exhaustive over
  all eight subsets). This is what the default buys.
- **ONE does not** (`one_read_can_miss_a_one_write`): one replica acknowledged the write, another
  answers the read, and they never overlap. ALL writes are visible to every read level
  (`all_writes_are_seen_by_every_read`), which is the only configuration where reading at ONE is
  safe.
- **LOCAL_QUORUM is per-datacenter** (`local_quorum_does_not_cross_dcs`): a write acknowledged in
  dc1 and a read in dc2 do not overlap at all, so there is no cross-DC read-your-writes.

Two consequences that belong with the transaction work rather than the index work: the commit
decision's level is a *lower bound* on the data level, because a verdict acknowledged at ONE with
data at QUORUM can lose the verdict and keep the data
(`a_weak_decision_level_can_lose_the_verdict`); and **no consistency level prevents write skew**
(`all_does_not_prevent_write_skew`) — with ALL every replica agrees, on the state where the
invariant is broken. A level chooses which replicas answer; only read-set validation chooses which
versions are visible.
