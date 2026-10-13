# Commit-path duplication inventory

- **Repo:** `/Users/bkearns/src/ferrosa-suite/.wt-int2`
- **Branch / HEAD:** `feat/read-in-transaction` @ `43ac79cb`
- **Method:** read-only, code-inferred. Every entry carries a `file:line` citation. Citations marked **✓** were re-verified by the author against the live file in this revision; unmarked citations come from parallel sub-recon and were not independently re-read. **No source file was modified.**
- **Ground truth consumed first:** `specs/reference/commit-path-ground-truth.md`, `specs/reference/commit-path-and-transaction-boundary-review.md`, `specs/reference/ground-truth/lane-a-commit-path.md`, `-lane-b-abandon-depwait.md`, `-lane-c-pg-transaction-buffer.md`.

**Scope:** the Accord commit path and the PostgreSQL front-end transaction boundary. One logical concept is the *pending changes of one transaction*; the inventory maps every structure, buffer, map, set and format that reifies it.

## Risk model

`risk = copies × consequences-of-divergence × absence-of-test`, each 1–5.
- **copies** = number of distinct representations that must agree.
- **consequences** = 5 when divergence produces a wrong durable answer (silent corruption, double-apply, dangling reference, client told "aborted" after rows landed); 3 when it produces a liveness failure (a txn abandoned/parked that should have committed); 1 when only memory growth or a log line.
- **absence-of-test** = 5 when no test asserts the copies agree; 3 when agreement is asserted only within one module; 1 when a test asserts agreement across the boundary in question.

---

## 1. Verdicts against the brief (a)–(f)

| # | Claim | Verdict | One-line basis |
|---|-------|---------|----------------|
| **a** | a txn's changes exist in ≥5 representations | **CONFIRMED (understated — ≥15 named + wire bytes)** | The 5 named types are a subset; builders, borrowed views, the wire frame, the parked set and an MVCC envelope add more. |
| **b** | two hand-maintained edge maps, one asymmetric mutator (`mark_applied`) | **CONFIRMED (structural); PARTIAL (live exploitability)** | `dep_wait.rs:88/90` two maps; `:219` asymmetric. On today's paths the asymmetric line is a defensive no-op — see §3.2. |
| **c** | "did this txn apply?" in ≥4 structures | **CONFIRMED (understated — ≥8 structures)** | Four claimed exist; plus MVCC dedup set, crash-replay set, RAFT ledger, conflict index. |
| **d** | two idempotency sets in the applier (`applied` + `payloads`) | **REFUTED (production)** | Production `EngineStorageApplier` has **one** set; the `applied`+`payloads` pair is the **test double**. |
| **e** | two serialization regimes on one wire (capnp multi-key Apply vs bincode elsewhere) | **CONFIRMED** | capnp for multi-key Apply; bincode for every control message and single-key Apply; the multi-key Apply itself has **3** formats. |
| **f** | two prune keys (identity `forget` live, age `prune` test-only but present) | **CONFIRMED** | `forget` (`dep_wait.rs:336`) live via `state_machine.rs:1419`; `prune` (`:309`) only `#[cfg(test)]` `apply.rs:1466`. |

---

## 2. Ranked duplication register

| Rank | ID | Concept | Copies | Cons. | Test gap | Risk |
|------|----|---------|:---:|:---:|:---:|:---:|
| 1 | **D1** | one txn's pending change across the pipeline (a) | 15+ | 5 | 3 | **225+** |
| 2 | **D2** | "did txn X apply?" (c) | 8 | 5 | 3 | **120** |
| 3 | **D3** | codec of one Accord message (e) | 3 | 5 | 3 | **45** |
| 4 | **D4** | on-disk `Mutation` header layout | 5 | 5 | 3 | **75** |
| 5 | **D5** | dep-wait forward/reverse edges (b) | 2 | 4 | 5 | **40** |
| 6 | **D6** | apply/no-write entry points (extra) | 2+2 | 3 | 5 | **30** |
| 7 | **D7** | write-set spill implementation (extra) | 2 | 3 | 5 | **30** |
| 8 | **D8** | dep-wait prune key (f) | 2 | 4 | 3 | **24** |
| 9 | **D9** | the txn-timeout bound + the `abandoned:` literal (extra) | 2+2 | 3 | 3 | **18** |

Ranking is `copies×consequences×test-gap`. D1/D2 dominate because many copies each carry a *different* lifetime and are pruned on *different* keys, so agreement is not local.

---

## 3. Detail — brief items

### D1 — one transaction's pending changes (item a) — CONFIRMED

**What is duplicated.** The same logical write (one `Mutation`, `ferrosa-storage/src/commitlog/mutation.rs:61 ✓`) is reified as at least the following, each a distinct type with its own lifetime and storage medium:

| # | Representation | Definition | Role in the pipeline |
|---|----------------|-----------|---------------------|
| 1 | `PgWrite(pub(crate) Mutation)` | `ferrosa-postgres/src/mvcc.rs:230 ✓` | one buffered write at the executor seam |
| 2 | `TxnWriteSet { stage, count }` | `ferrosa-postgres/src/txn_write_set.rs:47 ✓` | the session buffer |
| 3 | `WriteSetStage` | `ferrosa-storage/src/write_set_stage.rs:99 ✓` | resident Vec + spilling temp-file in progress |
| 4 | `StagedWriteSet { map: Option<Mmap>, … }` | `write_set_stage.rs:402 ✓` | finished, index-readable (resident prefix/mmap) |
| 5 | `TransactionWrite { keyspace, key, mutation }` | `ferrosa-storage/src/accord/transaction_committer.rs:23 ✓` | streamed hand-off unit |
| 6 | `AccordWriteSetBuilder` | `ferrosa-postgres/src/query.rs:2059` | PG-side streaming build (owns the reused serialize buffer) |
| 7 | `StagedAccordWriteSetBuilder` | `transaction_committer.rs:205 ✓` | builds keys + payload spill one write at a time |
| 8 | `WriteSetSpillBuilder` | `ferrosa-storage/src/write_set_spill.rs:347 ✓` | streams payloads to mmap'd temp file |
| 9 | `WriteSetSpill { map, resident, offsets, lens, bytes }` | `write_set_spill.rs:84 ✓` | mmap'd payload region, indexed |
| 10 | `StagedAccordWriteSet { keys, payloads: Arc<WriteSetSpill>, tombstones }` | `transaction_committer.rs:100 ✓` | committer output |
| 11 | driver `write_set: Vec<WriteSetEntry>` + `write_blobs: Option<Arc<WriteSetSpill>>` + `staged_prefix: Option<usize>` | `ferrosa-cluster/src/accord/coordinator.rs:640 ✓`, `:713 ✓`, `:721 ✓` | routing index + spill + the boundary between them |
| 12 | `BorrowedWriteSetEntries<'a> = Vec<(&[u8],&[u8])>` | `coordinator.rs:57 ✓` | zero-copy view for the frame encoder |
| 13 | wire frame (`ApplyV2Payload` / capnp region) | `ferrosa-cluster/src/accord/wire.rs:94 ✓`; encoder `coordinator.rs:3070-3105 ✓` | serialized Apply body |
| 14 | `ApplyMutation { data, t, deps }` | `ferrosa-cluster/src/accord/apply.rs:303` | owned applier payload |
| 15 | `MutationView<'a> { data: &[u8], t, deps }` | `apply.rs:326 ✓` | borrowed applier view |
| 16 | `AccordWriteSet<'s,'v>` | `apply.rs:1118 ✓` | streaming `WriteSetSource` over the views |
| 17 | `ParkedWriteSet::{Resident,Staged}` / `StagedMutation` | `apply.rs:108`, `:123` | a write-set parked behind an unapplied dep |

**Two copies of one write at the *byte* level.** The `Mutation` is serialized **twice** and decoded **≥3 times**:
- Serialize #1 into the session stage frame — `txn_write_set.rs:90-92`.
- Decode #1 to learn the written tables — `server.rs:998` (`staged.for_each_mutation(...)`) and again in the source `write_set_stage.rs:594/609`.
- Serialize #2 **inside the MVCC envelope** `MAGIC=b"FRPGMVC1"` — `ferrosa-storage/src/accord/postgres_mvcc.rs:7 ✓`, encode `query.rs:2258 ✓`, decode `apply.rs:1143 ✓ / :1264 ✓ / :1920 ✓`.

**Authoritative owner.** The encoded `Mutation` bytes are the single source of truth; the raw bytes are authoritative, every Rust type is a view. But **keys and append order** are a second, independent truth owned by the driver's `write_set`, and the staged-vs-resident boundary is a *third* fact split between `staged_prefix` and the spill's own `resident.len()` (`coordinator.rs:716-719, 2623-2639`). No single structure owns "this transaction's changes".

**Invariant.** (i) every representation carries the same mutation bytes, in the same order, with the same count; (ii) for `i < staged_prefix`, `write_set[i].mutation` is **empty** and the payload is `write_blobs.entry(i)`; (iii) `keys[i]` aligns with payload `i` (documented `transaction_committer.rs:91-99 ✓`).

**What breaks if copies disagree.**
- A count/key-order mismatch between `StagedWriteSet` and the Accord builder ⇒ the wrong key gets the wrong row image (silent corruption).
- A `staged_prefix` boundary mismatch ⇒ a staged entry's empty `mutation` field is sent instead of the spill payload ⇒ replica applies nothing, or a resident entry is read from the spill at an out-of-range index ⇒ fail-loud (`write_set_spill.rs:699 an_out_of_range_index_fails_loud`), or a stale row.
- Divergence between the stage frame and the envelope's inner mutation ⇒ a reader that decodes one and a writer that encoded the other disagree on the row image — this is the shape of the observed "committed cell reads back empty" (`review.md` F5 / RT-3) and the dangling-fragment read failure recorded in `commit-path-ground-truth.md:101-106`.

**Test coverage (agreement).** Partial, and only *within* a single module:
- `ferrosa-postgres/src/server.rs:3295 ✓ streaming_accord_writes_match_prepare_row_changes_per_partition` — count + key order + envelope decodes to the same per-partition metadata as the old whole-table path.
- `write_set_stage.rs:667 ✓ every_entry_round_trips_across_the_spill_boundary_and_the_last_entry`.
- `write_set_spill.rs:925 ✓ builder_matches_stage_byte_for_byte` (within `WriteSetSpill` only).
- `coordinator.rs:4892 ✓ a_spilled_write_set_fans_out_the_staged_payloads`; `:7732 ✓ coordinator_local_apply_borrows_the_staged_write_set_not_a_copy`.
- **None found** asserting byte-equality of the `Mutation` frame across the *stage → envelope* hop, or asserting the driver's `staged_prefix` boundary agrees with the spill's own residency. This is the sharpest untested seam in the path.

### D2 — "did transaction X apply?" (item c) — CONFIRMED (understated)

**What is duplicated.** ≥8 structures answer the same question, across three legitimate lifetimes plus scoped replicas:

| # | Structure | Definition | Written | Pruned on |
|---|-----------|-----------|---------|-----------|
| 1 | `AccordStateMachine.txn_states: HashMap<TxnId, TxnState>` (phase `TxnPhase::Applied`) | `state_machine.rs:100 ✓`; `TxnState` `ferrosa-common/src/accord.rs:426 ✓`, `TxnPhase::Applied` `:398 ✓` | `state_machine.rs:1312` | identity (`:1377-1382`) |
| 2 | `FinalizedTxns { tombstones: BTreeMap<TxnId,TombstoneKind> }` (`was_applied`) | `ferrosa-cluster/src/accord/finalized.rs:107 ✓`, `:108 ✓`, `:171 ✓` | `:196` ← `state_machine.rs:1392 ✓` | capacity + age (`finalized.rs:206/229`) |
| 3 | `EngineStorageApplier.applied: Mutex<HashSet<(TxnId,Vec<u8>,u64)>>` | `apply.rs:923 ✓` | `apply.rs:1375 ✓` | watermark (`:953-966`) |
| 4 | `DepWaitGraph.applied: HashMap<TxnId,Instant>` | `dep_wait.rs:95 ✓` | `:196-197` | identity `forget` (`:336 ✓`) |
| 5 | `MvccManager.applied_accord_txns: HashSet<(TxnId,Timestamp)>` | `ferrosa-postgres/src/mvcc.rs:265 ✓` | `:603 ✓` | **nothing** — absent from `prune_versions` (`mvcc.rs:867 ✓`) |
| 6 | `CrashRecoveryReplay.applied_txn_ids` | `ferrosa-storage/src/accord/crash_recovery.rs:72` | replay ingest | rebuilt on boot |
| 7 | RAFT `AppliedTxnLedger` | `ferrosa-cluster/src/raft/multi_dc_apply.rs:78` | `:115-119` | age/skew (`:126`) |
| 8 | `ConflictIndex` per-write `TxnStatus::Applied` | `ferrosa-storage/src/accord/conflict_index.rs:416` | `:417-423` ← `state_machine.rs:1314 ✓` | identity/status (`:446`) |

Neighbours that answer a *different* question and must not be conflated: `state_machine.rs:109 committed_txns` (committed ≠ applied), `:141 applying` (in-flight reservation), `:1279 bookkept` (per-batch dedup).

**Authoritative owner.** Ambiguous by design: three lifetimes are legitimate (live `TxnState`; durable-after-prune `FinalizedTxns`; storage dedup `applied`), and (4)/(5)/(7) are narrower-scope replicas. The two that can genuinely disagree about the *same* txn on the *same* replica are (1) and (3): the applier records `applied` after the durable batch, and the state machine advances the phase after the `Applied`-marker fsync — a failure between them leaves `applied` set but `phase == Committed` (`state_machine.rs:1300-1310`, the G1 abandon-durability path).

**Invariant.** `applier.applied ∋ txn ⇒ txn_states.phase == Applied ⇒ finalized.was_applied(txn)` after prune; and `graph.is_applied(dep) ⇒ dep applied on this replica`.

**What breaks if copies disagree.** `finalized.was_applied` false for a durably-applied, pruned txn ⇒ the replica **refuses `ApplyOK` for work it already did**, so the coordinator reports `TxnAbandoned` while rows are durable (`handlers.rs:368` gate; abandon emit `coordinator.rs:2799`) — the G1 defect. `graph.is_applied` false for a live dep ⇒ a waiter parks forever until its bound expires (`state_machine.rs:1838-1848`) — the D5 liveness failure. A missing applier gate ⇒ double-apply; the regression it prevents is documented at `apply.rs:3342-3345`.

**Test coverage.** A real cross-structure lockstep test exists — `state_machine.rs:1990 ✓ prune_applied_drops_the_idempotency_record_on_the_same_watermark` asserts applier count, graph count, and the shared floor. Plus `state_machine.rs:1850 ✓`, `handlers.rs:901 ✓`, `apply.rs:3175 ✓ / :3348 ✓`. **None found** asserting `MvccManager.applied_accord_txns` agrees with any cluster-side structure, or that `committed_txns` agrees with `txn_states`.

### D3 — codec of one Accord message (item e) — CONFIRMED

**What is duplicated.** One logical message family has two codecs, and the multi-key Apply has three wire formats:

| Message | Encode | Codec | Decode |
|---------|--------|-------|--------|
| PreAccept / V2 / OK | `coordinator.rs:1585`, `:1598`; `handlers.rs:453` | bincode | `handlers.rs:432 ✓ / :469 ✓`; `coordinator.rs:1810` |
| Accept / OK | `coordinator.rs:1976`; `handlers.rs:532` | bincode | `handlers.rs:513 ✓`; `coordinator.rs:2052` |
| Commit / OK | `coordinator.rs:2137`; `handlers.rs:555` | bincode | `handlers.rs:537 ✓`; `coordinator.rs:2186` |
| ReadVote / OK | `coordinator.rs:2239`; `handlers.rs:741/798` | bincode | `handlers.rs:705 ✓`; `coordinator.rs:2369` |
| `AccordApply` (single-key) | `coordinator.rs:1343` → `:2742` | bincode | `handlers.rs:560 ✓` |
| `AccordApply` (no-write finalize) | `coordinator.rs:1419`, `:3305 ✓` | bincode | `handlers.rs:560 ✓` |
| `AccordApplyOK` | `handlers.rs:167`, `:586` | bincode | `coordinator.rs:2737` |
| `AccordApplyV2` (fallback) | `coordinator.rs:3101 ✓` | **bincode** | `handlers.rs:599 ✓` |
| `AccordApplyV2Capnp` | `coordinator.rs:3086 ✓` | **capnp** | `handlers.rs:611 ✓` |
| `AccordApplyV2Region` | `coordinator.rs:3074/3080 ✓` | **capnp** | `handlers.rs:648 ✓` |

**Runtime-gated.** The codec is chosen per fan-out by capability negotiation — `coordinator.rs:3017-3055 ✓` (`supports_accord_capnp` `:3031 ✓`, `supports_accord_apply_region` `:3036 ✓`, `..._compressed` `:3041 ✓`; `let capnp = capnp && any_remote` `:3047 ✓`). So in a mixed-version cluster the **same logical Apply travels as bincode**; in a modern cluster as capnp.

**Authoritative owner.** The wire is the contract; there is no single encoder. The mutation is carried **opaque** (`wire.rs:85 ✓ mutation: Vec<u8>`; `envelope.capnp:379 mutation @1 :Data`), so only the *envelope* is duplicated, not the row layout.

**Invariant / what breaks.** Every peer must decode whatever the coordinator emits; a peer that lacks the capability must never receive a type byte it rejects. Divergence = a mixed cluster that silently drops Apply frames (connection drop) or a fallback that changes the on-wire shape of one transaction.

**Classification.** Two things hide here: (i) a **defect** — the repo rule is "per-message hot-path serialization MUST be capnp"; all control messages and the single-key Apply are bincode, recorded as **G9** in `commit-path-ground-truth.md:74-77`; (ii) a **deliberate** trade — the bincode fallback for un-upgraded peers is documented (`coordinator.rs:3017-3020`). Tests exist for capability negotiation and for each arm (`coordinator.rs:4892`, `:5401/5491` doubles), but **none found** asserts the bincode and capnp encodings of one Apply produce the same decoded write-set.

### D4 — one `Mutation`'s on-disk header layout, hand-written 5× — CONFIRMED

**What is duplicated.** `ferrosa-storage/src/commitlog/mutation.rs`: `serialized_size` (`:359 ✓`), `serialized_size_for_single_row` (`:384 ✓`), `serialize_into` (`:404 ✓`), `serialize_single_row_into` (`:437 ✓`), `serialize_borrowed_rows` (`:471 ✓`). Each restates the header field order by hand; the shared row/cell encoders (`serialize_row:504`, `serialize_cell:524`) and the decoder (`decode_fields:617`) are correctly single-sourced.

**Authoritative owner.** `mutation.rs` — the commit-log format. **Invariant:** the five entry points emit byte-identical output for the same fields; the borrowed twin must equal the owned encoding. **Divergence:** a field added to one encoder but not another ⇒ different bytes for the same `Mutation` ⇒ cross-node / restart mis-decode. **Tests:** `mutation.rs:930 borrowed_rows_serialize_like_an_owned_mutation` pins the borrowed twin; `serialize_single_row_into` has **no** agreement test (only used at `segment.rs:603`) — **none found**. This "owned + hand-written zero-copy twin that must stay byte-identical" idiom recurs across the tree (each pinned by its own test); it is systematic, not a one-off.

### D5 — dep-wait forward/reverse edge maps (item b) — CONFIRMED (structural)

**What is duplicated.** `DepWaitGraph` keeps `waiting_on: HashMap<TxnId,HashSet<TxnId>>` (`dep_wait.rs:88 ✓`) and `waited_by: HashMap<TxnId,HashSet<TxnId>>` (`:90 ✓`) as two hand-maintained mirrors. Six mutators touch them:

| Mutator | forward | reverse | consistent? |
|---------|---------|---------|-------------|
| `register_wait` `:135` | `:142` | `:143` | yes (both rolled back on cycle `:152-163`) |
| `mark_applied` `:196` | removes `txn_id` from each waiter `:204`; removes own forward edges `:219 ✓` | removes `waited_by[txn_id]` `:202`; **never** strips `txn_id` from `waited_by[dep]` of its own deps | **NO** |
| `clear_waits` `:230` | `:230` | `:232` | yes |
| `waive` `:257` | `:260` | `:265` | yes |
| `remove_txn` `:450` | `:452`, `:466` | `:454`, `:464` | yes for edges (leaves empty `waiting_on` entry `:468`) |
| `forget` `:336` / `prune` `:309` | — | — | N/A (touches only `applied`/`aborted`) |

**The asymmetric line: `dep_wait.rs:219 ✓`** — `mark_applied` drops the txn's own forward edges but leaves a stale reverse edge in each dep's `waited_by`.

**Authoritative owner.** Neither map — both are derived from the same edge set; the invariant (`∀a,b: b ∈ waiting_on[a] ⇔ a ∈ waited_by[b]`) has no single owner.

**What breaks.** Missing reverse edge ⇒ `mark_applied(dep)` never wakes the waiter ⇒ it parks until its bound expires and is abandoned — the liveness failure D2/D5 share. Phantom reverse edge (what `:219` leaves) ⇒ `waiters_of(dep)` reports a waiter that is not waiting; today benign (`waive` on a stale waiter is a no-op, `dep_wait.rs:262-264`), but a latent trap for any consumer of `waiters_of`.

**Live exploitability — PARTIAL.** Parallel recon measured that on all production paths `mark_applied` is reached only for a txn with no live forward edges: `register_waits_or_refuse` (`apply.rs:1648-1669`) registers edges only for unapplied deps and a waiting txn returns before cascade; both `notify_applied` sites (`state_machine.rs:1104`, `1119`) are no-write finalizes of an applying txn. So `:219` is today a defensive no-op. The **code-level asymmetry is real and completely untested**.

**Test coverage.** `dep_wait.rs:877 ✓`, `:908 ✓`, `:791 ✓` cross-read forward vs reverse, but only after `waive`/`clear_waits`. **None found** asserting the two maps agree for all mutators, and none exercising `mark_applied` on a txn that still has forward edges.

### D6 — apply / no-write finalize entry points (extra) — CONFIRMED

**What is duplicated.** (i) The single-key `AccordApply` arm inlines the body of `apply_writeset_and_ack` — compare `handlers.rs:156-169 ✓` with the inlined `apply_writeset_durably` + `await_txn_applied` + `ApplyOkPayload` + `bincode::serialize` at `handlers.rs:572-587 ✓`. (ii) Two construction sites emit the same empty no-write Apply frame: `no_write_fanout` (`coordinator.rs:1403-1426 ✓`, `ApplyPayload { result_data: Vec::new() }` + bincode + `Message::AccordApply`) and `finalize_late_preaccept_registrations` (`:3295-3306 ✓`), both then calling the shared `deliver_no_write_finalize` (`:3359 ✓`).

**Invariant / divergence.** Both sites must emit a byte-identical empty-Apply; the single-key arm must replicate the ack-withholding rule exactly. Divergence ⇒ one path acks where the other withholds, or a no-write finalize that is not recognised as such. **Tests:** behaviour is covered (`coordinator.rs:5965/6064/6094`, `abandon_durability.rs:234`), but **none found** asserts the two construction sites agree, and none pins the inline arm against the helper.

### D7 — two write-set spill implementations (extra) — CONFIRMED (deliberate, documented)

`WriteSetStage` (`write_set_stage.rs:99 ✓`) and `WriteSetSpill` (`write_set_spill.rs:84 ✓`) are two independent, `pub` implementations of "write-set that spills to a mmap'd temp file". Field-for-field: `map` (stage `:405` / spill `:92`), `resident` (`:107/ :410` / `:105`), `resident_bytes` (`:109` / `:107`), `offsets` (`:116` / `:109`), `lens` (`:118` / `:111`), `bytes`/`spill_bytes` (`:120, :416` / `:115`) — and the same index rule in `entry()` (`stage.rs:460 ✓` / `spill.rs:273 ✓`). Both carry their own staging-dir helper: `reserve_stage_dir` (`stage.rs:623 ✓`) vs `reserve_write_set_stage` (`spill.rs:571 ✓`).

**Authoritative owner.** None — split by consumer: `WriteSetStage` by ferrosa-postgres (`txn_write_set.rs:48 ✓`); `WriteSetSpill` by Accord (`coordinator.rs:713 ✓`, `transaction_commit.rs:355 ✓`). **Invariant:** same spill semantics. **Divergence:** an offset/drain-order bug fixed in one survives in the other. **Tests:** nothing compares the two implementations (`WriteSetSpill`'s own `builder_matches_stage_byte_for_byte` is intra-module). **Classification:** declared a deliberate trade — `write_set_stage.rs:620-621` says the two are kept separate so their staging dirs never collide — but it is still one algorithm in two copies.

### D8 — dep-wait prune key (item f) — CONFIRMED

**What is duplicated.** Two pruning keys for one concept ("which txns are applied/aborted"):
- Identity: `forget(&[TxnId])` (`dep_wait.rs:336 ✓`), live via `apply.rs:1606/1607 ✓ forget_graph_applied` ← `state_machine.rs:1419 ✓`, in lockstep with the `TxnState` drop.
- Age: `prune(max_age)` (`dep_wait.rs:309 ✓`), whose only callers are `#[cfg(test)]` (`apply.rs:1464-1468 prune_graph_for_test`), plus four tests (`dep_wait.rs:732/746/768/782`).

**Authoritative owner.** Identity (`forget`) — the code's own `forget` doc says "the state is the right key, not the clock" (`dep_wait.rs:320-335`). **Divergence:** rewiring `prune(max_age)` back into the maintenance loop would let the graph forget a dep whose `TxnState` is still live, so a waiter registers, is never woken, and parks until abandoned — the hazard the comment warns about. **Classification:** the presence of `prune` is a **deliberate recorded state** (its doc explicitly marks it test-only, `apply.rs:1464-1465`); the residual defect is that a hazard-carrying age-keyed function sits one call-site away from production with no test forbidding its re-wiring other than the doc. **Tests:** `state_machine.rs:1990 ✓` pins the identity path; **none found** pinning that `prune(max_age)` stays test-only.

### D9 — the txn-wait bound and the `abandoned:` literal (extra) — CONFIRMED

**What is duplicated.**
- Two constants for one conceptual bound "how long a txn waits for its dependencies": `DEP_WAIT_TIMEOUT` (`dep_wait.rs:25 ✓`, 10 s, drives `DepWaitGraph`) and the operator knob `FERROSA_ACCORD_TXN_TIMEOUT_SECS` → `DEFAULT_TXN_TIMEOUT` (`state_machine.rs:1537/1556-1557`). They are **not** aliased. Divergence: an operator raising the env var raises the coordinator's abandon bound but not the graph's hardcoded 10 s. **Test:** none found.
- The client contract is a **string literal split across crates**: produced at `coordinator.rs:579 ✓` ("abandoned: …"), consumed by `msg.contains("abandoned:")` at `ferrosa-postgres/src/server.rs:1208 ✓`. Divergence: changing the producer silently reclassifies a retryable `40001` as an opaque `58000`. Only the producer is pinned (`coordinator.rs:3425 ✓` asserts `starts_with("abandoned:")`); **none found** pins the consumer. The parallel sentinel `overloaded:` is worse — produced at `ferrosa-common/src/error.rs:109 ✓` and `ferrosa-storage/src/store.rs:2354 ✓`, matched at `error.rs:70-71 ✓`.

---

## 4. Refuted

### D-ref — item (d) two applier idempotency sets — **REFUTED (production)**

`EngineStorageApplier` (`apply.rs:915 ✓`) has exactly **one** set: `applied: Mutex<HashSet<(TxnId, Vec<u8>, u64)>>` (`:923 ✓`); retried Apply is suppressed at `:1282 ✓` (`already.contains(&idem)`) and inserted at `:1375 ✓`. No `payloads` field exists on it.

The `applied` + `payloads` **pair is the test double**: `NoopStorageApplier` (`apply.rs:720 ✓`) has `applied` (`:722 ✓`) and `payloads` (`:726 ✓`), but `payloads` is a **payload log** (documented `:723-725`, read only by `applied_data` `:755-758`), not a dedup gate — `was_applied` consults only `applied` (`:743-744`). Both are pruned together at `:775-783`.

Also **not** applier idempotency sets: `state_machine.rs:141 applying` (in-flight reservation), `:1279 bookkept` (per-batch dedup), `apply.rs:1407 pending` (parked write-sets), and `mvcc.rs:603 applied_accord_txns` (a single insert-gate, not two). Verdict: the claim is a **misreading of the test double**.

---

## 5. Duplication the brief did NOT name

Beyond D6–D9 above (all previously unnamed), two more:

- **D10 — `Mutation` serialized twice around the MVCC envelope (already folded into D1).** The stage frame and the envelope wrap (`postgres_mvcc.rs:7`) are two independent serializations of the same write; the envelope's inner `storage_mutation` is a re-`serialize_into` of the `Mutation` decoded from the first frame. No test asserts the two agree end-to-end.
- **D11 — the write is decoded a second time just to read its table name.** `server.rs:998 ✓` streams the stage a second time (`staged.for_each_mutation`) purely to build `write_tables`, decoding every frame — a second decode of the same bytes with a different purpose, adjacent to the source's own decode (`write_set_stage.rs:594/609`).

---

## 6. Classification summary

### (i) Real defects (a wrong durable answer or a liveness failure is reachable)
- **D1** — the sharpest seam: the `Mutation` frame is not byte-asserted across the stage→envelope hop, and the driver's `staged_prefix` boundary is not asserted against the spill's own residency.
- **D2** — `applier.applied` vs `TxnState.phase` can disagree across the fsync-failed-marker path (G1), which is the abandon-after-durable mechanism.
- **D3** — bincode on the hot path for control messages and single-key Apply is a rule violation (**G9**); no cross-codec decode test.
- **D4** — five hand-written encoders of one header; `serialize_single_row_into` has no agreement test.
- **D5** — the `mark_applied` asymmetry at `dep_wait.rs:219` is real and untested (live exploitability currently nil — defensive).
- **D6** — the single-key arm inlines its helper; two no-write frame construction sites, untested for agreement.
- **D9** — the split `abandoned:` literal has only the producer pinned.
- **D11** — the double decode for table discovery.

### (ii) Deliberate trades, recorded somewhere
- **D3 (bincode fallback arm only)** — documented `coordinator.rs:3017-3020`.
- **D7** — the two spill implementations are explicitly kept separate (`write_set_stage.rs:620-621`), sharing the one env knob `WRITE_SET_SPILL_THRESHOLD_ENV`.
- **D8** — `prune(max_age)` is explicitly declared test-only in its own doc (`apply.rs:1464-1465`); the trade is "keep the hazard-bearing function for tests".
- **D2** — the three distinct `"did X apply"` lifetimes (live / durable-after-prune / storage dedup) are intentional layering, not accidental duplication; the defect is only the *unasserted* agreement between them.

### (iii) Not adjudicated
- **Whether the `staged_prefix` boundary is redundant with `WriteSetSpill`'s own `resident.len()`.** The code keeps both; whether the second is derivable from the first (and therefore a pure duplicate) needs a targeted read of `coordinator.rs:1150-1167, 2623-2639` that this pass did not settle.
- **Whether the capnp path is actually taken at runtime** (capability negotiation is dynamic) — **U4** in the ground truth; not resolvable statically.
- **Whether `mark_applied`'s forward-edge cleanup is ever reached with a non-empty forward set** in a live schedule — the ground truth's **U6**; parallel recon found no production path that does, but did not prove no ordering can.
- **`MvccManager.applied_accord_txns` unbounded growth** — proven absent from `prune_versions` (`mvcc.rs:867`), but whether that is a leak or by design is not stated.
