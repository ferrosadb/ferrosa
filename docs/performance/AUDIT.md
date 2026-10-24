# Write-path performance audit

Companion to [PATTERNS.md](PATTERNS.md). This is the worklist: what to fix, in what order,
how to prove each fix, and how to hand a crate to a sub-agent mechanically.

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
2. **Never lose the entry** — one of:
   - **backpressure**: block/await until the queue has room, so index maintenance is
     lossless and the writer pays the latency honestly; or
   - **drop + repair**: on a drop, set a per-keyspace *adjacency dirty* marker and have the
     existing `ensure_adjacency_ready` / `ReconcileMode::Repair` heal it before serving
     traversals (the heal path already refuses queries with
     `record_query_refused_during_heal`, so the degraded window is explicit rather than a
     wrong answer).

   Either is correct. What is not correct is counting a drop that nothing reads.

**Red test:** register an async observer with capacity 1, do not drain, write N edges, then
drain and assert every edge is traversable — or that the keyspace is marked dirty and the
reconcile repairs it. Today: N-1 drops, no marker, missing adjacency entries.

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
