# Write-path performance audit

Companion to [PATTERNS.md](PATTERNS.md). This is the worklist: what to fix, in what order,
how to prove each fix, and how to hand a crate to a sub-agent mechanically.

## Order of work — correctness first, then performance

### Programme order — owner decision (final)

1. **Class F backpressure (C1) — load-bearing, not optional.** Index maintenance IS the
   observer path, so every index we add is another derived structure whose updates can vanish
   through a dropped queue entry. Fixing allocation or adding indexes before this multiplies the
   ways committed data can become unreachable.
2. **Allocation model (T1 -> T6).** The write path first: `Mutation::new` per write (T1) is the
   head of the chain, `9338` (T2) becomes a refcount bump once T1 carries `TableRef`/`Bytes`.
   Design: `docs/design/graph-identity-and-attributes.md` — identity is the declared key
   tuple (any arity), attributes are a typed map, access paths are derived index tables, and
   proximity is a cell-prefix scan rather than a key ordering. Read it before implementing.

3. **Graph secondary indexes.** `CREATE INDEX ... FOR (n:Label) ON (n.pk)` currently fails with
   `CypherSyntaxError`, so every graph "point read" is a label scan — measured 136 ms versus
   Neo4j's 2 ms at 20k nodes. This is a BUG, and it is the next performance lever after
   allocation: it dominates any per-row allocation win. It lands after (1) because an index is a
   derived structure maintained by the observer path.
4. **R-series refactors** (R1 tuning structs, R2/R3 the two hot functions, R4 engine.rs split).

Sequencing rationale: correctness before speed, and *cheap* correctness before *speculative*
speed — but the index bug outranks micro-optimisation because it is order-of-magnitude, not
percentage.

A performance win that leaves data unreachable is worthless, so the correctness items come
first and gate the rest.

### Phase 1 — correctness

| id | item | why it is correctness |
|---|---|---|
| **C1 ✅** | Async observer drops lose derived index entries — `engine.rs:9333-9343` | A committed edge can have no adjacency entry: durable data with no path to it, unreachable forever. `observer_drop_count` has no production reader. Detail: class F below. Policy is settled — backpressure, never drop. |
| **C2** | `&mut` on decoded input — 17 sites | The API invites rewriting the caller's row. Any transform that takes `&mut Row` can silently alter data on a path the caller believes is read-only. Sites: `memtable/mod.rs:284-285`, `:387` (`expand_legacy_collection_blobs`), `memtable/sharded.rs:539`, flush `engine.rs:4838`. |
| **C3** | Relationship `MERGE` key derivation on an implicit label (#610) | Write path derives 32 bytes, read path derives 32, uuid endpoint columns accept 16 — so truncating on one side only makes written edges untraversable. Both paths must change together. Acceptance test `#[ignore]`d pending this. |
| **C4** | Bolt diagnostic loss | The driver synthesises `An unknown error occurred`, and anonymous `CREATE (:Label {…})` is rejected as `no resolved binding`. A client cannot see why a write failed. |
| **C5** | Unjustified hardcoded constants that gate behaviour | A magic number encodes an assumption nobody stated, so nobody tests it. When the number is a capacity, a retry count, or a threshold, it decides when work is dropped, when a request fails, and when data spills — i.e. it is a correctness parameter, not tuning. |

Proof for C2 is a value assertion that the input is byte-identical after the call. C1's red
test is a capacity-1 observer with no drain.

### C5 — every behavioural constant is named, justified, externalised

Rule: a numeric literal that changes behaviour must (a) carry a comment naming the
assumption it encodes (memory bound? expected write rate? tolerated drop probability?
retry budget?), and (b) live in a tunable with a documented default — not inline at the call
site. `runtime_tuning.rs` already exists as the home for these, so use the existing pattern
rather than inventing a mechanism.

The observer queue is the worked example: `ObserverConfig::default().queue_capacity`
(10,000) is documented on the struct, but hardcoded at three construction sites
(`engine.rs:3589`, `3851`, `4144`) instead of read from deployment config — so the drop
threshold is fixed by whoever wrote the literal, and an operator cannot raise it.

Inventory of constants that gate behaviour (ferrosa-storage, non-test, 69 total; the ones
that most clearly need justification and externalisation):

| site | constant | what it decides |
|---|---|---|
| `store.rs:1448` | `MAX_SEALED_MEMTABLE_RETRIES = 10_000` | how long a write spins before failing — 10k is a load assumption, not a bound |
| `spill_budget.rs:38` | `DEFAULT_SPILL_THRESHOLD_PCT = 50` | when memory pressure spills — directly an OOM parameter |
| `store.rs:3042`, `:3046` | unselective-index share `10`, min matches `50` | whether the planner uses an index; changes result path and latency |
| `manifest.rs:14` | `MAX_CAS_RETRIES = 3` | when manifest publication gives up |
| `evicted_read.rs:48-52` | `MIN_PAGE_BYTES 4096`, `MAX_RUN_PAGES 8`, `MAX_OBJECT_INFOS 4096` | evicted-read batching and memory ceiling |
| `eviction_audit.rs:71-86` | max bytes 4/16/32, `MAX_RECORD_BYTES 1024` | audit ring retention |
| `store.rs:32,1433,3962,4070` | `64`, `8`, `8`, `8` | compaction candidate and retry bounds |
| `runtime_tuning.rs:11-18` | `DEFAULT_*` (8/64/8/8/16/8/64) | flush/compaction scheduling parallelism |

Method for each: state the assumption, decide whether it is a tunable (operator-facing) or a
named constant with a proof obligation (invariant-derived, e.g. `MIN_MAX_BYTES = 16` if a
record header cannot be smaller), and add a test that fails if the value is changed to
something the assumption forbids.

#### C5b — every externalised value is also observable

A tunable you cannot see is untunable. Each externalised value must publish, on the metrics
surface the crate already uses (no new endpoint):

1. the **configured** value (the limit),
2. the **current usage** gauge against that limit,
3. a **saturation or rejection counter** if the limit can be hit.

Without (2) an operator cannot tell whether the setting is too tight until something fails;
without (3) they cannot tell it was hit at all. The ratio usage/limit is then derivable
without a second metric.

Observer queue as the worked example — `Sender` has no length accessor, so:

```rust
// depth = bound - remaining; max_capacity() is the bound, capacity() is what is left.
// Snapshot, not an invariant: a concurrent send can make it briefly stale.
let depth = sender.max_capacity() - sender.capacity();
```

Expose depth, capacity, and the backpressure-event counter, so an undersized queue is
visible before it is diagnosed from latency. The same treatment applies to the other
behavioural constants in the table above: `DEFAULT_SPILL_THRESHOLD_PCT` needs current
spill volume against the budget, and `MAX_SEALED_MEMTABLE_RETRIES` needs a retry-exhausted
counter — a retry bound with no counter fails silently by definition.

### Phase 2 — performance, ranked by impact

Ranked by *frequency on the write path*, not by raw occurrence count. Evidence is
`file:line`; these are counts and shapes, not yet measurements.

| tier | site | cost per write | fix |
|---|---|---|---|
| **T1** · [scoped](scopes/T1-mutation-payload.md) | `engine.rs:9837` (and `:9720` when observers exist) | Up to 5 allocations and two full payload copies, **unconditional**, before any observer is consulted: `Mutation::new(keyspace.clone(), table.clone(), key.clone(), vec![row.clone()], ts)` | `TableRef { Arc<str>, Arc<str> }` + `Bytes` keys/cells; reuse one mutation buffer |
| **T2** | `engine.rs:9338` | Deep clone per observer per write, on top of T1 | Becomes a refcount bump for free once T1 lands |
| **T3** | `engine.rs:9261`, `:9308`, `:9981` | Whole-row clone per derived row to satisfy a `&Row` parameter while iterating a reference | `for row in dm.rows` — move, do not clone |
| **T4** | `engine.rs:4838` | Row clone per row during flush (background path, not ingest latency) | Take ownership into the normalisation instead of cloning |
| **T5** | `vec![row]` one-element allocation, same sites | One allocation per call, avoidable | Pass the row or a `SmallVec` |
| **T6** | `format!` with an allocation **per iteration** | One `String` per element (see below) | Compare/format without allocating; `write!` into a reused buffer |

### T6 — `format!` is hot only where it runs per iteration

`format!` allocates a `String`; `format_args!` builds a stack `fmt::Arguments` and allocates
nothing. The measured context across 2643 src sites is what decides whether that matters:

| context | count | verdict |
|---|---|---|
| error construction (`Err(`, `Error::*`, `anyhow!`, `expect`) | 1234 | cold — allocates only when an error is built |
| other (path building, metrics render, query-plan debug) | 1366 | mostly cold: per file-op, per scrape, or debug-only |
| inside a closure (`.map`/`.any(`,`.filter`) | 27 | **candidate — runs per element** |
| inside a `tracing::` macro | 16 | small; a `tracing` event with inline args avoids the `String` entirely |

So there is no logging hot path here — only 16 sites format inside `tracing`, and the
codebase mostly builds eager error/path strings instead. The real per-iteration offenders:

- `memtable/mod.rs:582`, `:596` — `format!("<index {idx}>")` inside column-name decode → per row;
- `engine.rs:12004` — `format!("{}.{}", id.keyspace(), id.table()) == *key` inside `.any(…)`:
  a `String` allocated per element purely to compare. Compare fields directly;
- `engine.rs:4758` — `.entry(format!("{}.{}", mutation.keyspace, mutation.table))` per mutation
  on the set-aside path;
- `commitlog/mod.rs:147-404` — ~50 `push_str(&format!(…))` in one metrics renderer: one
  `String` per line per scrape. `write!` into the existing `out` instead;
- `cql/router.rs:25259`, `31918`, `cql/bridge.rs:1342`, `raft/log_store.rs:1410` — hex built
  one two-char `String` per byte.

Caveat that decides the rewrite shape: `format_args!` cannot be stored or returned (its
temporaries die at the end of the statement), so it only works where the callee consumes it
immediately — logging. For owned output the fix is `write!(&mut buf, …)` into a reused buffer.

Tails, after triage: `&mut` input (17, correctness — folded into C2), identity `.to_string()`
(185, but mostly cold — metrics, maintenance, `sort_by` keys at `engine.rs:7300`;
roughly 10 real hot-path sites), `new_v4()` (210, verify which are per-write; cluster txn
ids dominate), `.to_vec()` on key/cell (27), `Vec::new()` (752, overwhelmingly cold — only
those inside row loops matter), `format!()` (2643, triaged in T6 — per-iteration sites only).

**T1 outranks T2**, which inverts the earlier focus: `9338` only costs on top of what `9837`
already paid, and only per registered observer.

## Measured inventory (working tree, `src/` only, tests excluded)

Counts are **smell indicators, not defects** — each must be triaged before it is fixed.

| crate | `&mut` on decoded input | payload clones | identity `.to_string()` | `new_v4()` | `to_vec()` | `format!` | `Vec::new()` |
|---|---|---|---|---|---|---|---|
| ferrosa-storage | **18** | **36** | **62** | 23 | 831 | 1539 | 478 |
| ferrosa-graph | 1 | **13** | **44** | 14 | 120 | 261 | 131 |
| ferrosa-cluster | 0 | **41** | 42 | **478** | 610 | 567 | 305 |
| ferrosa-cql | 0 | 33 | 17 | 31 | 185 | 671 | 177 |
| ferrosa-postgres | 1 | 8 | 29 | 25 | 131 | 296 | 93 |
| ferrosa (bin) | 0 | 2 | 3 | 35 | 33 | 131 | 19 |

The `.clone()` / `to_vec()` / `format!` totals are dominated by legitimate cold-path uses
(error messages, config, tests-adjacent helpers). **Only the bold cells are triaged as
hot-path candidates.**

## Classes, with predicate, fix, and proof

### A. `&mut` on decoded input — 18 sites (storage)

Predicate: `&mut (Mutation|Row|CellValue|ApplyMutation|DecoratedKey|PartitionKey)` in a
signature that is not `fn(&mut self, …)`.

Known sites: `memtable/mod.rs:284-285` (`existing`, `incoming`), `memtable/mod.rs:387`
(`expand_legacy_collection_blobs`, a transform), `memtable/sharded.rs:539`.

Fix: read the input; mutate only `existing`. For transforms, take ownership
(`Row -> Result<Row>`) or return a value.

Proof: value assertion that the incoming row is byte-identical after the call.

### B. Per-mutation identity strings — 62 / 44 / 42 sites

Predicate: `keyspace.to_string()` / `table.to_string()` / `label.to_string()` inside a
per-row or per-mutation constructor.

Fix: `TableRef { keyspace: Arc<str>, table: Arc<str> }` resolved once per statement;
`Mutation` holds it. Clone becomes two refcount bumps.

Proof: pointer identity of the shared name across two mutations from one statement.

### C. Payload clones — 36 / 41 / 33 sites

Predicate: `.clone()` on `Mutation` / `Row` / `CellValue`, or on a collection of them.

Known archetype: `ferrosa-cluster/src/coordinator/batch.rs:64` — `mutations: mutations.clone()`
for fan-out.

Fix: move when the caller is done; `Bytes`/`Arc` handles for fan-out; encode once and share
the encoded buffer per replica.

Proof: two replicas' cell pointers alias (identity, not count).

### D. `new_v4()` on the hot path — 478 in cluster

Predicate: `Uuid::new_v4()` inside a per-mutation/per-txn constructor.

Fix: mint once per statement/transaction where dedup semantics allow, or use
`(node_id, monotonic counter)`. Keep v4 where true randomness is required (keys).

Proof: allocations/entropy calls flat in row count.

### E. Growth without capacity — `Vec::new()` inside loops

Predicate: `Vec::new()` followed by `push` in a loop over rows.

Fix: `Vec::with_capacity` computed from the row count (e.g. `rows*2` for adjacency).

Proof: capacity after the write equals the predicted size (no reallocation happened).

## Class F — silent loss of derived index entries (CORRECTNESS, fix before any perf work)

**RESOLVED** — `fix/observer-noloss`, commit `517a498db`. A full queue now applies the
derivation inline on the writer thread (backpressure) instead of dropping it; the counter is
re-meant as `observer_backpressure_count` and read in production via
`ferrosa_storage_observer_backpressure_total` / `_observer_queue_depth` / `_observer_queue_capacity`;
capacity is externalised to `FERROSA_ASYNC_OBSERVER_QUEUE_CAPACITY`.

**Correction to this document's earlier claim.** Adjacency was *not* exposed: `AdjacencyIndexObserver`
is `Sync` mode and `GraphEngine` already derives inline. The live exposure was
`register_async_observer*` (which ignored `mode()`) plus the timeseries and subscription observers.
So the "durable edge with no path to it" scenario was reachable for those, not for adjacency —
worth stating plainly, because this document used adjacency to argue urgency.

`engine.rs:9333-9343` enqueues each committed mutation to every async observer with
`try_send(...).is_err()` → `drop_count += 1`. The write is durable; the **derived entry is
discarded**. For adjacency that means a written edge exists on disk and can never be
traversed — silent data inaccessibility, not a lost sample.

Nothing repairs it:

- `observer_drop_count` has **no production reader** — the only call sites are tests
  (`engine.rs:22355, 22367`);
- no drop path sets a dirty marker or triggers `reconcile_once_with(…, Repair)`.

Both halves of the fix already have homes:

1. **Make the hand-off cheap — "streaming with Arc".** With the payload refcounted
   (`TableRef`/`Bytes`, class B+C), `try_send` carries a pointer instead of deep-cloning
   two Strings, the key, and every row and cell (`engine.rs:9338`). This is what makes a
   lossless or low-drop policy affordable.
2. **Never lose the entry — backpressure is required.** (Owner decision, final.)

   **DROP IS NOT AN OPTION HERE.** Discarding a derived index entry is silent data loss: the
   committed record stays on disk while the path to it disappears. Drop-plus-repair was
   considered and rejected — a repair window is still a window in which committed data is
   unreachable, and the rule is that committed data stays reachable.

   The write path is synchronous, so backpressure has to be expressed without an `.await`:

   - **preferred — inline fallback:** when `try_send` returns Full, run the same derivation
     the drain task runs, synchronously on the writer. Nothing is lost, memory stays bounded,
     and the writer absorbs the cost, which is what backpressure means.
   - **blocking bounded send** only where the caller is provably not a tokio runtime worker
     thread (`blocking_send` panics inside a runtime context).
   - **an unbounded channel is also wrong** — "never drop" must not become "never bound"; an
     unbounded queue on a database is an OOM waiting for load.

   The existing counter is re-meant: it counts **backpressure events** (the writer paid the
   inline cost), not lost data. It must have a production reader — it is the signal that the
   queue is undersized, so an operator can raise the capacity.

**Red test:** register an async observer with capacity 1, do not drain, write N edges, then
drain and assert every edge is traversable, and that the backpressure counter shows the
writer absorbed the cost. Today: N-1 entries dropped silently, no marker, missing adjacency
entries — and nothing repairs them.

## R-series — refactor worklist (from churn + smell analysis)

Evidence: 90-day churn from `git log`, `frg smell-detect ferrosa-graph/src`, and file sizes.
Ordering rule: **churn x complexity**, not complexity alone — complexity in a file nobody
touches is stable even when it is ugly.

| file | commits / 90d | lines |
|---|---|---|
| `ferrosa-storage/src/engine.rs` | **161** | **39,323** |
| `ferrosa-graph/src/executor/expand.rs` | 17 | 10,037 |
| `ferrosa-graph/src/adjacency/reconcile.rs` | 10 | 1,641 |
| `ferrosa-graph/src/engine.rs` | 8 | 4,047 |

Highest cognitive complexity in the graph crate:

| function | cognitive | nesting | site |
|---|---|---|---|
| `execute_var_length` | 215 | 10 | `varpath.rs:66` |
| `expand_to_states` | 139 | 7 (CC 42, 371 lines) | `expand.rs:1666` |
| `pattern_predicate_exists` | 138 | **11** | `expand.rs:912` |
| `reconcile_once_with` | 122 | 6 | `reconcile.rs:204` |
| `collect_merge_ops_from_path` | 109 | 5 | `physical.rs:921` |
| `plan_match` | 104 | 7 | `physical.rs:1420` |
| `eval_expr` | 90 | 4 (237 lines) | `eval.rs:18` |
| `execute_optional_hops` | 86 | 6 | `expand.rs:2454` |
| `infer_schema_aware_merge_shape` | 73 | 5 | `expand.rs:4809` |

**The correlation that justifies the order:** the two tests that went red during the C4 work
(`negative_multi_hop_pattern_predicate_filters_existing_paths`,
`optional_match_preserves_rows_and_nulls_missing_pattern`) are the tests **of**
`pattern_predicate_exists` and `execute_optional_hops` — the #3 and #8 most complex functions
in the crate. At cognitive 138 with 11 levels of nesting, behaviour is not recoverable by
reading, so every edit there is a guess. `infer_schema_aware_merge_shape` (73) is the same
function class that broke three separate fix attempts during the merge-key work (#610).

### R1 — one tuning struct per crate (do first; queued behind C1)

Not merely a refactor: the smell scan shows **the same tunable name carrying 100x different
values in different files**, and the same five-field config block copy-pasted in five places.

| tunable | `expand.rs` | `varpath.rs` |
|---|---|---|
| `max_result_rows` | 10 | **1000** |
| `max_fan_out_per_hop` | 10 | **1000** |
| `max_collect_size` | 10 | **1000** |
| `max_groups` | 100 | **1000** |

Duplicated block (`segment_size`, `local_cache_max_bytes`, `flush_threshold_bytes`,
`flush_max_age_secs`, `max_pending_replay_mutations_without_schema`):
`expand.rs:8412`, `varpath.rs:513`, `reconcile.rs:1179`, `http.rs:1481`, `http.rs:1556`,
`engine.rs:3288`.

A row cap of 10 in one executor and 1000 in another is silent behavioural divergence that no
test covers, because each site is exercised with its own literal. Fold them into one config
struct per crate, externalise it (C5), and give each field a usage gauge (C5b).

### R2 — extract the two functions whose tests break

`pattern_predicate_exists` (138, nesting 11) and `execute_optional_hops` (86). Target nesting
<= 4 by lifting each nesting level into a named predicate or a small step function, so failure
modes become observable in the code rather than only in a red test.

### R3 — `expand_to_states`

371 lines, CC 42, cognitive 139, and it calls both R2 functions. Do it after R2, otherwise
the extraction boundaries will be wrong.

### R4 — split `ferrosa-storage/src/engine.rs`

39,323 lines at 161 commits per 90 days. Last, and deliberately after the payload work (T1):
splitting the file while its hottest type is being reshaped means doing both twice.

### Deliberately not in the series

`execute_var_length` has the highest cognitive score in the crate (215, nesting 10) but only
5 commits in 90 days. Ugly and stable beats broken and churning for prioritisation; revisit
after R3 if it becomes a churn hotspot.


## Crate briefs — execute in this order

Each brief is self-contained: scope, invariants, gates, and what is out of scope.

### 1. ferrosa-storage — the data model (blocking; do first)

**Goal:** `Mutation` carries shared identity and refcounted payload; rows are views over
one buffer on the persist-now path.

- Introduce `TableRef { keyspace: Arc<str>, table: Arc<str> }`; `Mutation.table: TableRef`.
- `CellValue` holds `Bytes` instead of `Vec<u8>`; `PartitionKey` holds `Bytes`.
- `rows: SmallVec<[Row; 1]>` for the single-row case.
- Add a `RowCursor` (P5) and `RowView<'a>`; keep owned `Row` only where bytes must outlive
  the call (parked write set).
- Triage all 18 class-A `&mut` sites.

**Invariants:** I1 data preserved (same bytes persisted); I2 wire format unchanged;
I3 no deep clone inside a request lifetime; I4 write outlives-paths move, never clone.

**Gates:** `cargo test -p ferrosa-storage`, `cargo clippy -p ferrosa-storage --all-targets -- -D warnings`, `cargo fmt`.

**Out of scope:** the cluster fan-out (brief 3) and the index derivation (brief 2).

### 2. ferrosa-graph — the index path

**Goal:** zero per-write allocation in adjacency derivation; already partially done
(`derive_into` + reused buffer + exact `clustering` sizing, commit `f79f4d63e`).

- Remaining per-mutation allocations: `vertex_id.to_vec()`, `edge_table.to_vec()`,
  `adj_keyspace.to_string()`, `"adjacency".to_string()`, `vec![row]`.
- Take `edge_table`/`adj_keyspace` as shared handles threaded from the caller, not rebuilt.
- Triage the 44 identity-`to_string` sites; most are outside the write path.

**Gates:** `cargo test -p ferrosa-graph`, clippy, fmt.

### 3. ferrosa-cluster — fan-out and txn ids

**Goal:** one encoded payload shared per replica; no 478-per-call UUID minting.

- Replace `mutations.clone()` fan-out with a shared handle or a single encode + `Bytes` per replica.
- Mint txn/mutation ids once per transaction, not per mutation, where replay dedup allows.

**Gates:** `cargo test -p ferrosa-cluster`, clippy, fmt. Clustered tests are required for
behaviour; unit tests alone are not proof for lease/fan-out changes.

### 4. ferrosa-cql / ferrosa-postgres — front ends

**Goal:** decode into views over the request buffer; reuse the response buffer per session
(the CQL `Session` already does this — extend the pattern, do not invent one).

**Gates:** `cargo test -p ferrosa-cql`, `-p ferrosa-postgres`, clippy, fmt.

### 5. ferrosa (bin) — wiring

**Goal:** one reusable buffer per connection/statement at the top level; no per-statement
`Vec` in the request path.

## Sub-agent brief template

Hand one crate per agent. The brief must carry:

1. **Root cause sites** — exact `file:line` from the audit, so the child does not re-derive.
2. **The pattern** — link PATTERNS.md and name the specific P-codes in scope.
3. **Invariants + tests** — one test per invariant, named in the brief.
4. **The mirror control** — the negative direction that must keep failing (e.g. the input
   row is unmodified; a copy would break the identity assertion).
5. **Gate commands verbatim**, including any env (`CARGO_TARGET_DIR`, `PATH`).
6. **Parking instruction** — an unfinished change is parked as a patch and the tree
   recompiled, never left red.
7. **No commits, no pushes**; report real pass/fail counts with every claim.

Mechanical rule for the child: change the type, let the compiler enumerate the call sites,
fix them all, then rebuild. Do not hand-grep for call sites after a type change.

## Definition of done for the programme

Allocations per write are **flat in row count**, proven by:

- identity tests (cell/name pointers alias rather than copy) for P2/P3;
- buffer-capacity tests for P4;
- an input-immutability test for P1;
- `cargo test --workspace` green and clippy `-D warnings` clean per crate.

No throughput claim is made until a profile of the write path shows allocator/clone
self-cost moving.
