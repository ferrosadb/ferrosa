---
title: Red ledger audit — all 107 reds and 56 Broken rows verified against code
status: complete
date: 2026-11-19
baseline: ferrosa v0.36.9 (9c2a076a4)
supersedes_estimate_in: specs/reds-burndown.md
---

# Red ledger audit

Every one of the 107 `red-ledger` rows and all 56 `Ferrosa/Broken.lean` rows were read against the
code at `9c2a076a4`, not read off the ledger. Four parallel audits; each row got a verdict, the
current `file:line`, the impact class, and a judgement on whether the row's stated **mechanism** is
right.

## Headline

| | Count | Meaning |
|---|---|---|
| **REFUTED** | **20** | The code does the right thing. The row should not be red. |
| **CONFIRMED** | **51** | Real. |
| **PARTIAL** | **33** | A narrowed residual survives; the row's text overstates or misdescribes it. |
| **UNVERIFIABLE** | 2 | Needs measurement or a probability model, not a read. |

Of the 20 REFUTED, only **16** are a simple flip. **6 were never real defects** — the Lean model is
stale about code that was always correct (`Recovery.lean` still models a record with no CRC). **1**
is a narrowing, not a fix: the claim holds for every mutation with an id, and the legacy zero-id
path is unreachable rather than absent.

Two structural findings outrank the count:

1. **The corpus cannot distinguish a design claim from a code claim.** That is how a stale model
   survives for a year, and how I misread three modules in an hour. One field fixes it.
2. **`specs/jepsen-evidence/project-plan.md` does not exist in this repository**
   (`git log --all -- '*jepsen-evidence*'` is empty), yet eleven ledger rows cite it as their spec.

## Correction: the `.met` rows are design proofs, and the corpus has no way to say so

My first read of `Cdc/Subscribe.lean:179,182` was that they were false *code* guarantees. That was
wrong, and the correction is the most actionable finding in this document.

`Cdc/Subscribe.lean`'s own header (lines 1-18) describes the module as a **design and a gap
analysis**, and every one of its `.met` rows cites a `Commons.Flow.*` theorem — a property of the
abstract controller model, e.g. `the_default_evaluation_period_is_five_seconds`. The red rows
beside them say explicitly that the bus "does not speak" that protocol yet. So the module is
internally coherent and honest **once you read the prose**.

The problem is that nothing in the *data* says which kind of claim a `.met` row is. `SpecItem` has
two fields, `name` and `status`, and no field distinguishes:

- **design** — proved of a model, with no claim that the code implements it; and
- **code** — proved of the shipping implementation.

The corpus already has conventions for both, used inconsistently:

- **`design:`/`code:` name prefixes** — used in exactly two files, `Raft/Quarantine.lean` (6 design
  + 6 code, correctly paired) and `Broken.lean` (6 `code:` rows). That is the right idea, applied to
  12 rows out of 465.
- **`none (… )` witnesses** — several reds carry `none (ordering obligation)`,
  `none (implementation item)`, correctly marking "no theorem, this is a code claim".
- **`unverified:` site prefixes** in `Broken.lean`, which the file itself counts and pins.

A reader, a CI job, or a count cannot tell a design `.met` from a code `.met`, which is exactly how
I misread three modules in an hour. The fix is one field, not a rewrite:

```lean
inductive Claim where
  | design   -- proved of a model; says nothing about the implementation
  | code     -- proved of the shipping implementation
```

`SpecItem` gains `kind : Claim`, `red_ledger.py` exports it, and the generated Rust table carries it
so a consumer can tell "the model says X" from "the code does X". `Cdc/Subscribe.lean`'s `.met` rows
become `design`, and the whole question dissolves instead of turning into a red.

**Consequence for the plan:** the rows below are *not* false guarantees and should not be moved to
red. The `.met` side still needs a full audit — I only read the red side, and the ~465 `.met` rows
have never been checked against the code — but that audit's output is a `kind` tag per row, not a
list of corrections.

## The `.met` side, restated correctly

The rows I originally flagged are **design proofs, not false claims** — see the correction above.
What remains true and worth acting on:

- `Cdc/Subscribe.lean` marks `.met` a 5-second evaluation period, a 90% early-reval rule and a
  `max_subscriber_file` cap. All three are theorems of `Commons.Flow`, a controller model. **The
  shipping CDC bus implements none of it** — it is a `tokio::broadcast` with a fixed capacity, and
  the red rows immediately below say so. Correct, but indistinguishable from a code claim at the
  data level.
- `specs/jepsen-evidence/project-plan.md` **does not exist in this repository**
  (`git log --all -- '*jepsen-evidence*'` is empty), yet eleven ledger rows cite it as their spec.

## REFUTED — 20 rows, in **three** classes (this split matters more than the count)

"Refuted" turned out to mean three different things, and only one of them is a simple flip. Every
one was checked against the Lean model, not just the Rust, because a red row is a statement about
the *model*, and a model can be stale about code that was never wrong.

### Class 1 — the MODEL is stale about code that is (and was) correct

These need the Lean model replaced with a model of the shipping code. No Rust work is involved, and
none of them was ever a real defect. They are the "no Rust change can ever close it" set.

| Row | What the model says | What the code does |
|---|---|---|
| `storage_recovery__a_corrupted_payload_is_detected` | `Rec` carries `checksum : Option Nat`; the theorem assumes `none` (`Recovery.lean:108-120`) | `commitlog/reader.rs:272-276` reads a 4-byte `payload_crc` and verifies `crc32fast::hash(&payload)` |
| `sql_semantics__aggregates_follow_sql_s_empty_set_rules` | `sumNaive` folds from 0 (`Sql/Semantics.lean:102`) | `exec.rs:379-381` returns `Value::Null` |
| `sql_semantics__three_valued_logic_is_preserved` | `notEqNaive` (`Sql/Semantics.lean:39`) | `plan.rs:218` Kleene `NOT` over `Option` |
| `sparql_semantics__rdf_star_terms_are_distinct_from_iris_in_the_store` | numeric packing (`Sparql/Semantics.lean:117-119`) | tagged identity header (`triple_store.rs:269-275`) |
| `bolt_session__replies_can_be_paired_to_messages_by_index` | `Session.lean` counts 0 replies per swallowed message | `server.rs:420` replies `IGNORED` per message |
| `bolt_session__queued_result_streams_are_bounded` | model assumes an unbounded queue | `pending_result: Option<PendingRows>` — bounded at **1** (`server.rs:531`) |

### Class 2 — the model is right; the CODE changed recently. Add a witness, then flip.

| Row | Fixed by / evidence |
|---|---|
| `raft…code_a_node_whose_restore_failed_answers_reads` | `1de80f29d`: `apply_snapshot_data_from` save-and-restores state/last_applied (`state_machine.rs:1105-1120`); boot refuses (`controller/cluster.rs:1500-1509`) |
| `raft…code_the_node_exits_an_inconsistent_state_on_an_index_it_was_told` | same rollback; decode failure refuses before the classifier runs |
| `raft…the_durable_snapshot_on_disk_never_moves_backwards` | `e73088c4b`: `persist_snapshot_to_disk` re-checks `snapshot_precedes` under `SnapshotWriterLock` (`:1418-1443`) |
| `storage_streaming__a_decoded_chunk_is_written_into_a_caller_supplied_buffer` | `ed1c7ffca`: `decompress_into`; read path uses it (`reader.rs:256`) |
| `streaming_audit__no_durable_artifact_changes_under_a_reader` | `4fece71f8`: reserve-ahead markers; bytes below `last_flushed` never rewritten (`segment.rs:855-1125`) |
| `streaming_audit__an_operator_that_spills_does_not_re_collect_its_result` | `b7752ca79`: `finish_rows` deleted; `finish_stream` only (`spill.rs:491`) |
| `streaming_audit__a_spilling_sort_s_input_is_not_already_a_vector_of_every_row` | `varpath.rs:451,506` streams into `RowAcc`/`SpillSortSink` |
| `storage_streaming__acks_are_coalesced` | premise false: `set_nodelay(true)` on every socket (`server.rs:309`); one frame per statement (`frame.rs:646`) |
| `ownership…a_write_that_hands_a_row_to_an_observer_does_not_give_the_observer_a_second_payload` | `b802c3a49`: `Arc::clone(&row)` is a handle (`engine/write.rs:163-170`) |
| `ownership…an_observer_drain_moves_rows_out_of_the_mutation_it_owns` | same: `row.clone()` on `&Arc<Row>` (`engine.rs:9708-9709`) |
| `systemtables…the_write_path_and_the_read_path_agree_on_an_equal_timestamp_tie` | one comparator: `reconcile_ref` and `cell_supersedes` both delegate to `reconcile_cmp` (`complex_cell.rs:63`, `merge.rs:193`) |
| `sql_txn__serializable_isolation_is_available` | read set captured (`server.rs:1044-1092`) and validated (`mvcc.rs:837`, `:1156-1166`); tests `serializable_read_write_skew_is_rejected_at_commit`, `serializable_predicate_read_detects_a_phantom_insert` |
| `sql_consistency__the_write_path_captures_a_transaction_s_read_set` | same capability as the row above |
| `systemtables…a_cell_re_granted_after_a_full_revoke_is_visible_again` | loader is timestamp-ordered, not absorbing (`system_table_loader.rs:401-410`) |
| `systemtables…a_revoked_permission_is_not_resurrected_by_an_equal_timestamp_write` | per-cell successor `wall.max(previous + 1)` (`system_table_writer.rs:87-127`) |
| `systemtables…convergence_does_not_rest_on_a_caller_convention` | same successor guard; `permission_regrant_survives_backward_clock_and_restart` |

### Class 3 — REFUTED as a *narrowing*, not a fix. The claim still holds; narrow it and say why.

| Row | Why it is neither a fix nor a model bug |
|---|---|
| `storage_recovery__replay_is_idempotent` | The model is correct and the Rust **now** matches it (`engine.rs:4238`, `:5114` dedup by UUID). But the model's own theorem `inc_is_not_idempotent` (`Recovery.lean:76`) is true of the abstract machine, and the legacy zero-id path is *unreachable* rather than absent. The honest row is the narrow one: "replay is idempotent for every mutation that carries a mutation id; the zero-id legacy path is re-applied and no current writer produces one." |

And one genuine **code** defect hiding under a "Model only" label:

- `sparql_semantics__p_returns_each_pair_once` — pair-once **holds** (`property_path.rs:286-288,328`,
  pinned by `tests/sparql_no_row_cap.rs:116-131`), but `(a,a)` is never emitted for `p+` because the
  start is seeded into `visited`. `ZeroOrMore` does emit it. Fix the code **and** add the self-pair
  case to the model.

---

## CONFIRMED — 51 real reds, ranked by damage

### Correctness, tier 1 (the four in PR2, plus what the audit re-ranked around them)

| Rank | Row | Why |
|---|---|---|
| **1** | `raft…code_a_latched_node_cannot_install_the_snapshot_that_would_repair_it` | Sticky latch with **no reset anywhere**; `install_snapshot` refuses before decoding (`:3061`, `:655-662`); `ConsensusHealth` has no clear, `RecoveryAdmission` has no fault removal. Sharper than the row: with the durable journal a latched node never starts openraft, so nothing ever *offers* it a snapshot. |
| **2** | `raft…a_restart_or_a_resync_repairs_an_entry_whose_destructive_effect_already_landed` | `unregister_table_classified` deletes SSTables and clears the pending-sweep flag before the ordered `TableDropped` write. Silent data loss. |
| **3** | `raft…the_staged_copy_leaves_no_trace_until_it_publishes` | `Arc`-shared schema/engine/system_writer; effects visible before `last_applied` publishes. Wrong answers on every apply group. **Deferred by owner.** |
| **4** | `sql_txn__the_commit_decision_is_durable_before_the_acknowledgement` | Durability holds; atomicity does not. The single CRC-protected record exists (`engine/write.rs:270-274`) and PG does not use it (`query.rs:2460` → `None`). Also `server.rs:1368-1375` commits DDL before rows, documented as non-rollback. |

### Correctness, tier 2 (real, narrower than stated)

| Row | Verdict detail |
|---|---|
| `sql_semantics__a_multi_statement_transaction_is_atomic` | CONFIRMED, **mechanism wrong**: DML is buffered to COMMIT (`query.rs:2249-2261`), not executed independently. The prefix comes from DDL-before-rows and per-mutation log entries. |
| `storage_tombstone__a_local_deletion_time_is_representable_for_every_valid_ttl` | CONFIRMED, **mechanism inverted**: `i32::try_from(..).unwrap_or(i32::MAX)` saturates to `NO_DELETION_TIME` = **never expires**. A 12-year TTL silently means permanent data. No TTL cap anywhere. |
| `codec_audit__the_canonical_form_is_a_function_of_the_request_not_of_the_writer_s_clock` | CONFIRMED. One `build_row` can stamp **three** different `ldt` values. But `ldt` is the *fifth* tiebreaker (after timestamp, tombstone, payload, ttl), so it shifts expiry/purge timing, not visibility. |
| `storage_tombstone__the_purge_path_checks_the_grace_window_and_propagation_before_removing_a_tombstone` | CONFIRMED — no propagation term in `purge.rs:46`. Resurrection risk. |
| `storage_tombstone__the_merge_is_order_independent` | CONFIRMED — cell level *is* a total order; row/partition metadata ties keep the first source (`merge.rs:67,171`). |
| `systemtables…a_timestamp_with_a_per_cell_sequence_converges_for_every_clock` | PARTIAL — `timestampSeq` is timestamp-first **in the model**, but the code uses a per-cell successor that survives a backward clock. Real residual: the successor is local, not log-ordered. |
| `raft…code_a_repair_replaces_local_state_wholesale_and_records_nothing_it_discarded` | CONFIRMED. `tables_dropped_by_snapshot` is a **stub returning `Vec::new()`** (`:3153-3163`). Local rows above the installed index survive while their metadata is replaced. |
| `raft…code_the_node_exits_an_inconsistent_state_…` / `…a_startup_inconsistency…` | PARTIAL, same defect: `NeedsPurgePointBaseline` adopts the purge point as `last_applied` on **empty** state (`:854-865`, `cluster.rs:1630-1634`). Fixing one discharges both. |
| `raft…code_a_node_whose_projection_diverged_still_votes` | CONFIRMED — `handlers.rs:1212-1230` has no admission term. Population narrowed by the journal. |
| `codec_audit__a_deletion_time_is_computed_the_same_and_width_safe` | CONFIRMED and **wider than stated**: 17 production sites, not 3. i32 sites go negative after 2038; u32 sites wrap in 2106. |
| `codec_audit__every_path_hands_the_pipeline_one_canonical_value_type` | CONFIRMED (numbers stale: 560 refs / 106 files; container-only 390/52; 875 raw `CellValue::*` sites). Note `CellValue` is *already* shared. |
| `codec_audit__on_disk_layout_knowledge_is_confined_to_the_codec_module` | CONFIRMED (429 literals / 23 files / 4 crates) |
| `codec_audit__an_artifact_records_which_format_version_wrote_it` | CONFIRMED. **No `fnf` code exists in Rust**; ADR-022 is aspirational. Only cheap slot is the `Statistics.db` trailer. |
| `sql_semantics__a_multi_statement…` (see above) / `sql_index__index_and_table_cannot_diverge` | PARTIAL — but the audit found a **new** defect: a declared index missing from the live map logs `error!` and **continues** (`store/write.rs:126-131`, `:173`), so the write is silently unindexed and a short read is presented as complete. |
| `sql_consistency__the_coordinator_enforces_the_per_datacenter_requirement` | PARTIAL, **cited line is a comment**. The real defects: `write_batch` drops `cl`/`rf` (`coordinator/write.rs:607-610`) and `coordinate_truncate` ignores remote failures (`truncate.rs:70-73`). |
| `sparql_semantics__optional_scoping_is_specified` | CONFIRMED and **understated**: every `OPTIONAL` right-hand side is silently dropped (`planner.rs:544-551`). |

### Memory / performance (all CONFIRMED)

`#70 scan materialises a page` · `#71 page bound is rows not bytes` · `#67 slow client grows the
coordinator` · `#69 no shared response pool` · `#76 no single admit permit` · `#25/#27/#78 snapshots
copied 3×` · `#29 system-table load unbounded` · `#26 snapshot restore holds 3 payloads` · `#34
orphaned spill dirs` · `#35 spill may land on tmpfs` · `#27 spill reserves no disk` · `#80` · `#82` ·
`#17 every SSTable probed` · `#20 version reclaim inside the commit closure` · `#24 no rewrap
operator` · `#21/#62/#59/#60/#61 MVCC absent (vacuous)` · `#10 flush resume` · `#11 BTI export action
missing (format already BTI)`

## PARTIAL — 33 rows whose text must be rewritten before anyone burns them down

Representative corrections, because each one would mislead whoever picks it up:

- `#2 restart cannot repeat a persistent failure` — there is **no boot/replay loop** any more; the
  durable journal keeps the node out of Raft entirely. What survives is that the quarantine is
  sticky.
- `#8 apply latch survives a restart` — the latch **is** durable now (journal); the *repair cursor*
  is not.
- `#22/#23 Jepsen book` — the four multisets exist and are correct (`checker/book.rs:106-160`), but
  only **1 of 22** workloads implements `final_read`, so "every run" is false.
- `#31 ORDER BY without a backend` — the stated mechanism is dead (hidden sort columns); the
  conclusion survives only in an embedded `spill: None` config that a server cannot reach.
- `#32` — both halves: named pools bounded, five raw `tokio::task::spawn_blocking` sites unbounded.
- `#36` — pool exists; residual is `SpillingDedup` (bounded by **row count** 65 536, not bytes) and
  SPARQL `SpillBag` charging nothing.
- `#1 pair write clones the mutation` — true, but post-`Arc` it copies no row payload. Cost is ~3
  orders of magnitude smaller than the row implies.
- `#14 HLL` / `#19/#20/#21 aggregates` — `hll.rs` **exists** now with a mergeable sketch, and the CQL
  AST **has** `AggregateCall{distinct, estimate_distinct}`; the reds are "unwired" or "execution
  refused", not "doesn't exist".

## UNVERIFIABLE — 2

- `storage_format__record_crcs_cost_less_than_5_of_point_read_p99` — **no benches exist** in
  `ferrosa-sstable` or `ferrosa-storage`. Needs the measurement harness.
- `commons_hll__the_relative_error_is_bounded_by_1_04_sqrt_m` — as worded it is a *standard error*,
  not a hard bound; `hll.rs:240` says so itself. Honest dischargeable form: relative standard error
  = 1.04/√m, with a fixed-δ confidence interval and a hash-model axiom.

---

## New defects found by the audit, in no ledger row

These were not being tracked. Each is a real defect with a named site.

| Defect | Site | Impact |
|---|---|---|
| A pipelined `RUN a; RUN b; PULL` **silently discards** `a`'s stream | `server.rs:531` | data loss on Bolt |
| A write whose declared index is missing from the live map logs and continues | `store/write.rs:126-131`, `:173` | wrong answer (short read presented as complete) |
| `write_batch` drops `cl`/`rf`; every row written at the coordinator default | `coordinator/write.rs:607-610` | wrong answer (consistency level ignored) |
| `coordinate_truncate` ignores remote failures and always returns `Ok` | `truncate.rs:19-22,70-73` | data loss |
| CQL read-set code is dead; LWT relies on a process-local mutex stripe | `transaction_keys.rs:32` (no caller), `local_lwt.rs:51` | wrong answer on a cluster |
| Version reclaim (`range(..).collect()`) runs **inside** the commit closure | `mvcc.rs:984-990` | per-commit allocation on the write path |
| Five raw `tokio::task::spawn_blocking` sites bypass the bounded `TaskPool` | `coordinator/read.rs:2740`, `fulltext_stream.rs:129,577`, `engine.rs:1262,12598` | unbounded memory |
| A spill may land on tmpfs; `staged_pages.rs:104` has the detector, unused here | `ferrosa-sql/src/spill.rs:524-529` | unbounded memory |
| `tables_dropped_by_snapshot` is a stub returning `Vec::new()` | `state_machine.rs:3153-3163` | silent divergence |

## Documentation drift found while auditing

- `docs/unsafe-ledger.md:23` and `Unsafe/Invariants.lean:233` say **seven** mmap sites; there are
  **six** (`Mmap::map` constructions). The seventh unsafe site in the set is `posix_fadvise`.
- `unsafe-ledger.md:102-105` counts four `engine.rs` `set_len` sites as production; they sit after
  `#[cfg(test)]` at `:17232`, and it misses six real production ones.
- `docs/backlog.md:32` says 65 unsafe sites; `guard-unsafe.py` reports **61**.
- All `Ownership/Audit.lean` anchors are pre-`Arc` (10 line numbers moved).
- All four Broken "Model only" site strings are stale.

---

## What this means for the plan

1. **PR1 got much bigger than 8 flips** — 20 REFUTED rows, of which 16 need only a witness (A) and 4
   need a model replaced first (B). Plus ≥3 `.met` rows must move the *other* way, back to red, and
   that requires writing the guarantee they claim or restating them honestly.
2. **PR2 is unchanged in scope but re-ranked.** The audit independently confirmed #33 and #30 as the
   top two by damage, and #54 as a real atomicity violation. #43 stays deferred by owner decision —
   and the audit makes deferral cheaper, because C1's fix (accept a verified install past the latch)
   and C3's fix (publish the staged copy only on adoption) touch the same `install_snapshot` /
   `finish_pending_apply` region, so doing them in adjacent PRs would be two rewrites.
3. **A third PR is now required** for the new defects, because "a pipelined Bolt RUN silently
   discards a stream" and "a write with a missing index silently continues" are not in any ledger row
   and would otherwise be lost.

---

# Part 2 — the `.met` side (358 rows)

The red side was audited first and is done. The `.met` side had never been read against the code —
only the red table had a Rust export, so the side a reader is meant to *trust* was unexamined.

| | Rows | Meaning |
|---|---|---|
| audited | **358** | every `.met` row in the corpus |
| `code-verified` | **160** | the shipping code implements it; tagged `code: ` and now in `red-ledger/src/met.rs` |
| `design` | **138** | a theorem about a model; says nothing about the implementation |
| **`code-untrue`** | **52** | the shipping code **contradicts** a row that reads as proof |
| `code-no-site` | **3** | no implementation, and the prose presents it as a feature rather than a model |

353 of 358 carry an explicit verdict; the other 5 are rows the auditors classed `design` while noting
they are `no-site` in substance.

**The red count would rise from 107 to ~159** if every `code-untrue` row were honestly restated as a
red. The corpus has been overstating its safety by roughly a third.

## The worst findings

These are not bookkeeping. Each is a shipping defect behind a `.met` row.

| # | Row said | Code does |
|---|---|---|
| 1 | "a wrong or corrupt trie can never return a wrong value" | `partition_index.rs:116-122` verifies **one byte** (`h2 & 0xFF`) and `reader.rs:993-1017` decodes whatever partition sits at that offset **with no key comparison**; `merge.rs:60` adopts `first.key`. A corrupt `Partitions.db` returns **another key's rows**. |
| 2 | "a corrupt trie block is detected" | checksums cover `Data.db` only (`checksum.rs:1-6`); nothing in `trie/`, `partition_index.rs` or `row_index.rs` has a CRC. Trie damage is silent. |
| 3 | "the index is a speed-up, not an authority" | same defect as #1 — index damage changes the **answer**, not the latency. |
| 4 | "purging requires the grace window **and propagation** to every replica" | `purge.rs:46-48` checks only grace + local overlap. `rg -i propagat` over storage+cluster finds nothing. Its own `.failing` twin says this. |
| 5 | "a tombstone shadows every value **at or below** its timestamp" | `merge.rs:211,232-236` and `types.rs:165-171` are strictly exclusive. A value at **exactly** the deletion timestamp survives. Cell level is correct; row/partition is not. |
| 6 | "a node that cannot reconstruct its applied state refuses to start" | `NeedsPurgePointBaseline` adopts the purge index on an **empty** `RaftState` and starts openraft (`cluster.rs:1631-1636`, `state_machine.rs:854-864`). The Lean theorem is safe only because it is conditional on `disk ≠ 0`; the ledger text is unconditional. |
| 7 | "re-applying an entry whose effects already landed produces the same state" | a replayed `CreateType` returns `TypeExists` → `ApplyError::Other` → `fences_replica()` → **the machine latches**. Same for `Create*`/`Drop*` of type/function/aggregate. The Lean "proof" is a `decide` over six hand-picked literals that omits every non-idempotent effect. |
| 8 | "`STALLED_READER_TIMEOUT` kills a reader with no progress" | the identifier exists **nowhere** in Rust. The twin row "total age never kills a reader; only absence of progress does" is **exactly inverted**: `mvcc.rs:601-611` kills on total age, so an actively progressing 600 s snapshot is killed. |
| 9 | "an empty run establishes nothing" | an empty history yields `all_passed() == true` and `profile.classify(true) == Confirmed`. The suite has a test asserting a zero-operation run **passed**. |
| 10 | "only a qualifying run shape licenses the claim" | no witness/read-only-transaction/non-local predicate exists in `ferrosa-jepsen`; no workload overrides `requires_transactional_checker()`. |
| 11 | "the socket-to-disk path costs two copies and no more" | three on the CQL write path: `bytes.to_vec()` → `CellValue` encode → `serialize_into` into the segment buffer. |
| 12 | "a full queue refuses the push rather than dropping the item" | `ws.rs:179-186` logs "dropping message" and **discards**; `notify.rs:280-288` logs "notification lost". True only of the CQL surfaces. |
| 13 | "the default consistency level is QUORUM" | the production coordinator is built with `ConsistencyLevel::One` (`cluster.rs:1922`). `ClusterConfig.default_cl = Quorum` is **dead code** — read nowhere. |
| 14 | "SNAPSHOT isolation is available and cheap" | `IsolationLevel` is `{ReadCommitted, RepeatableRead, Serializable}`; every level but Serializable/ReadCommitted is refused with `0A000`. |
| 15 | "triple multiplicity is preserved (bag semantics)" | the SPARQL store is a **set** — the row key *is* the triple, so a duplicate insert is an LWW overwrite. The theorem is `a_duplicated_triple_double_counts`. |
| 16 | "an equality lookup contacts one node" / "reads one record" / "a range costs one round trip per node" | all three false: every node is enumerated (`range_read_stream.rs:2425-2443`); one record **per match** (~103 000 rows under one key, per the code's own comment); and a range lookup **does not exist** — the planner matches `Eq` only, with its own test asserting `WHERE indexed_col > v` plans `FullScan`. |

## Three structural defects in the corpus itself

1. **A witness is cited by both a `.met` and a `.failing` row in the same ledger — five times.**
   The same theorem cannot prove a property and exhibit its failure. `cdc_has_four_red_items` pins
   red = 4 while four `.met` rows claim the same properties hold.
2. **`code-untrue` rows sit beside `.failing` twins that contradict them** — `Sql.Index` has
   "atomic maintenance preserves consistency" `.met` next to "index and table cannot diverge"
   `.failing`; `Tombstone` has the purge-propagation pair.
3. **A theorem proving `rfl`-vacuous or literal-vs-literal statements** passes as proof. Examples:
   `commitLevel requested := requested`, `producerProgress (_x) accepted := accepted`,
   `monotone [1,3,4] = true`, `batchesFor` singleton `≠ []`, `obsEq` defined as the six fields.
   These cannot be falsified by any code change.

Rows the audit found were **stale as reds** (the red text describes a fix that has landed): the
six `Streaming.Audit` marker/graph/varpath rows, the six `Ownership` Arc rows, the three
`Histories` "no workload keeps a book" rows, `an_unreadable_snapshot_skips_the_purged_log`, and
the `Sql.Index`/`Sql.Consistency` read-set pair.
