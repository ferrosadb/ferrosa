# Backlog

Ranked on three axes, in strict order: **correctness, then performance, then size**. Nothing in the
performance or size sections may land while a correctness item it touches is open, and no size change
is accepted that regresses the performance suite — the trade we do allow is *space for time*
(ADR-022's 12/16-byte cell layout, per-partition framing, per-record CRC), because disk is cheap and a
fast path is not.

## How each axis is gated

| Axis | Gate | Where |
| --- | --- | --- |
| Correctness (spec) | `lake build` + `lake exe harness` (13 checks, 750 cases) + ledger reds | `.github/workflows/lean-specs.yml` |
| Correctness (the unsafe surface) | `tests/ci/test_unsafe_ledger.py` — 65 sites pinned by file | `ci.yml` |
| Correctness (behaviour) | `jepsen-smoke-chaos`, `jepsen-multi-dc-nightly`, `elle-strict-serializable-nightly`, `cluster-data-loss.yml` | workflows |
| Performance | `ferrosa-loadgen`: p50/p99 statement latency, rows/s on `SELECT *`, IO per point lookup | nightly |
| Size | release binary size, cold start, crate graph via `frg dsm analyze` | release |

Current state: 175 specification items across the 11 printed ledgers, 54 red; classification ledger
`15 broken / 36 unimplemented / 8 non-defects` (59 slots) — the five-slot gap is C10.

---

## 1. Correctness

| # | Item | Evidence | Effort |
| --- | --- | --- | --- |
| C1 | **The truncate path counts a bare ack total, not a per-DC requirement.** On `{5,3}`, five acks in the primary and none in the secondary satisfies it. | Ledger red *"the coordinator enforces the per-datacenter requirement for every write, not a total"*, witness `Consistency.a_total_count_can_hide_a_missing_datacenter`; `coordinator/write.rs:748-760` | S |
| C2 | **`block_for(EachQuorum)` panics, and `coordinator/write.rs:288` calls it bare.** `0x0007` decodes to `EachQuorum` from the wire, so a client can send it. Today the guard lives in the router, not the call site. | `consistency.rs:50`, call sites in `read.rs:1822` and `pair/coordinator.rs` match it explicitly; `write.rs:288` does not | S |
| C3 | **Delete the fabricated `&'static StoreView`** (`Arc::as_ptr` + three sibling raw casts). Highest-risk site in the codebase and the easiest to remove: nothing there needs `'static`. | `store.rs:2122`; `docs/unsafe-proof-plan.md` §1 | M |
| C4 | **Enforce the mmap truncation contract.** Seven mappings are sound only if the rename/GC paths never truncate beneath them — an obligation in safe code, so it is provable. | `unsafe-ledger.md` mmap rows; `Ferrosa.Unsafe.a_truncation_below_a_mapping_makes_the_tail_unreadable` | M |
| C5 | **Kani harnesses for the 65 production `unsafe` sites**, one per ledger row, each naming its bound. Until then the ledger's `Status` column is the only defence. | `docs/unsafe-proof-plan.md` §CI shape; three reds in `Ferrosa/Unsafe/Invariants.lean` | L |
| C6 | **Tombstone merge order-independence is only checked on colliding cases, not proved.** Same class: *"deleted data cannot be resurrected"* and *"the purge path checks grace and propagation"* are red. | `Ferrosa/Storage/Tombstone.lean`, 4 reds | M |
| C7 | **Commitlog: corrupted-payload detection, idempotent replay, corruption quarantined and never deleted.** Data-loss class, three reds. | `Ferrosa/Storage/Recovery.lean`, 3 reds | M |
| C8 | **The CQL surface rejects `COUNT(DISTINCT c)` and `COUNT(ESTIMATE_DISTINCT c)`** — the AST has no aggregate node, so aggregates are name-matched in the router. | `Ferrosa/Cql/AggregateObligations.lean`, 6 reds, two of them parser-level | M |
| C9 | **`local_dc` can resolve to an empty string**, so a `DcQuorum` read on a `Simple` strategy returns `Unavailable { required: 1, alive: 0 }`. The test was reverted to `Quorum`, which hides it. | `keyed_index_read_at_one_does_not_wait_for_slow_replica` | S |
| C10 | **The classification bookkeeping is concept-text-keyed, not ledger-linked** — 59 slots for 54 reds, and the `15 + 36 + 8 = ledgerTotal` invariant passes by construction. Give `SpecItem` a status field tied to the ledger entry. | `Ferrosa/Unimplemented.lean:139-143`; this drift has now recurred three times | M |
| C11 | **Nothing enforces "no axiom".** `Witnesses.lean` pins theorem shape; a `#print axioms` sweep would pin the axiom set, and has never run. | proposed, never executed | S |
| C12 | **The Jepsen verdict's profile comes from the environment, not the driver's per-query CL.** A statement-level override scopes the verdict to the wrong profile, which is both an over- and an under-claim. | `ferrosa-jepsen/src/checker/profile.rs::from_env` | M |
| C13 | **The profile wiring in `ferrosa-jepsen` has never compiled.** Eight files edited, `rustfmt`-clean, no `cargo check`. | uncommitted at the time of writing | S |
| C14 | **TOC sync and directory-entry ordering before the rename are unverified** in the `fnf` writer — the durability claim rests on them. | `ferrosa-sstable/src/writer.rs` | M |
| C15 | **Whole-history `orderOk` replay does not finish at 2400 committed transactions**, so the strict-serializability claim is bounded to the first-4/first-5 exhaustive windows. A verification limit, not a product bug — but it bounds what we may claim. | 400 s / 500 s timeouts, `EXIT=124` | M |

## 2. Performance

Time is the currency; disk and memory headroom may be spent to buy it, except where the memory bound
*is* the invariant (SUBSCRIBE spill, `ORDER BY` spill, the shared pool's fair share).

| # | Item | Evidence | Effort |
| --- | --- | --- | --- |
| P1 | **MVCC point lookup is `O(F)` probes** — one bloom probe per file in the view. The `fnf` chain fixed record reads, not fan-out; a per-view bloom plus leveled pruning is the fix. | `the_lookup_cost_is_linear_in_the_files_of_the_view` | L |
| P2 | **`COUNT(ESTIMATE_DISTINCT)` has no error bound and cannot merge across shards** — the HLL spec exists (`Commons/Hll.lean`), the implementation does not. Also unblocks C8. | `Cql/AggregateObligations.lean` reds | M |
| P3 | **TTL-heavy rows degenerate to 28 B/cell** under the 12/16 layout without a second bitmap. A disk cost that also costs IO on the fast path. | deferred at ADR-022; user chose "bitmap should be used" | M |
| P4 | **The shared pool fairness controller is not implemented** — one node-wide pool, `pool × rate / Σrate` fair share, 5 s eval on 2.5 s samples, banded steps at 80/90/100%, BDP as the target under contention. | `Ferrosa/Commons/Stream.lean`, `Storage/Streaming.lean` | L |
| P5 | **Ack coalescing on the pgwire and Flight paths.** Nagle is already off on every server socket (`c38add89`); the coalescer that replaces it per the wire-protocol spec is not in. | 46 ms/statement on Fly 6PN vs 66–100 µs loopback | M |
| P6 | **Commitlog group commit** — amortise the data-fsync ack across concurrent writers rather than one fsync per write. | ack-at-data-fsync is the ADR-022 decision | M |
| P7 | **`fnf` is not yet the default for new tables** — `bti` is still the path for new table creation, so the fast path is opt-in. | ADR-022: "fnf would be native day1" | M |

## 3. Size

Dedupe only. No size work may add a copy on a hot path, and none of it is worth a throughput
regression — which is why most of these are *splits* rather than inlining decisions.

| # | Item | Evidence | Effort |
| --- | --- | --- | --- |
| S1 | **`ferrosa-common` is a god element** — fan-in 22, instability 0.04, inside a 27-crate / 121-edge graph with 29.9% propagation cost. Split into narrow crates. | `frg dsm analyze`, ADR-023 | L |
| S2 | **Extract `ferrosa-value`** — one value type re-exported by `common`, `schema` and `row-bridge` instead of three near-copies; 27 call sites to update. | ADR-023; user decision "breaking changes allowed, fix all 27" | M |
| S3 | **Commons targets must be shared by ≥2 front ends** — already machine-checked, so this is a standing constraint on S1, not a separate task. | `Commons/FrontEnd.lean::every_commons_target_is_shared_by_at_least_two_front_ends` | — |
| S4 | **Collapse duplicated row/value/error types across the storage and SQL layers**, now that the Lean corpus says which invariants they actually carry. | `Ferrosa/Sql/Semantics.lean`, `Row` types in 4 crates | M |
| S5 | **Move the WASM example crates out of the workspace** — 58 of the 188 `src/` unsafe sites and a large share of build time are demo code. | `unsafe-ledger.md` inventory | S |
| S6 | **Monomorphisation in the router** — generic dispatch over store types; `dyn` where the fast path allows. Explicitly last: it is the one size item that can cost throughput. | `ferrosa-cql/src/router.rs`, 37.5k lines | L |

## Closed, and deliberately not on this list

- **`COUNT(*)` no longer counts by materialising rows** — `router.rs:531` counts from SSTable metadata
  in a single pass. An earlier note in this repo recorded this as open; it is not.
- **Nagle on server sockets** — disabled platform-wide.
- **Scatter-gather for a partitioned `IN`** — accepted as a design consequence, not a defect.

## The order to work in

C3, C2, C9 (each an afternoon, each a real defect), then C1/C4 together (same subsystem), then C10
because every later ledger number depends on the bookkeeping being honest, then P1/P2. C5 is the long
pole and should start as soon as the workspace compiles again, because it is the only item that
converts "reviewed by hand" into "checked by a machine".
