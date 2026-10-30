# Ferrosa Lean 4 specifications

Formal, machine-checked models of the two traces requested:

1. **write-path correctness** — storage admission → commit log → sync mode →
   memtable → ack → flush → SSTable, plus cell-level LWW resolution, plus the
   coordinated (tunable-CL) write;
2. **cluster replication and upgrade** — Single → Pair (operator-only promotion,
   no split brain) → 3 nodes at RF=2 → 5 nodes at RF=quorum → 2 DCs over one WAN
   link → 3 DCs in a ring.

Plus an audit of the two existing TLA+ models in [`specs/tla/`](../tla), which have never
been model-checked.

Everything is Lean core only — **no Mathlib**, no network dependency at build
time, no `decide`-by-magic over opaque objects.

```bash
cd specs/lean4
lake build                      # 59 modules, seconds after the toolchain is present
lake env lean --run Ferrosa/Accord/RunCheck.lean   # check a real Elle history
```

Toolchain: `leanprover/lean4:v4.35.0` (pinned in `lean-toolchain`). Everything is
Lean core — no Mathlib, no network. `omega`, `decide` and `native_decide` do the
work; `native_decide` is only used where the statement is a finite computation
over a concrete configuration, and each use says so above it.

## Review order

| # | File | What to read it for |
|---|------|--------------------|
| 1 | [`Ferrosa/Core.lean`](Ferrosa/Core.lean) | The one counting lemma every quorum claim rests on: `filter_and_length_le`, then `quorum_intersects`, `no_two_disjoint_quorums`, `quorum_wr_intersects`. Then `blockFor`/`blockForDc`, transcribed from [`consistency.rs`](../../ferrosa-cluster/src/consistency.rs). |
| 2 | [`Ferrosa/WritePath.lean`](Ferrosa/WritePath.lean) | Trace 1. `Step` has one constructor per arrow in [`ferrosa-storage/specs/data-flow.md`](../../ferrosa-storage/specs/data-flow.md); `Reachable` + step lemmas carry the invariants. |
| 3 | [`Ferrosa/TlaCheck.lean`](Ferrosa/TlaCheck.lean) | `raft.tla`'s named actions, encoded; the invariants that are tautologies. |
| 4 | [`Ferrosa/Accord/AccordMultiDcLean.lean`](Ferrosa/Accord/AccordMultiDcLean.lean) | `multi-dc.tla`'s Accord layer; the apply-order violation and the vacuous I-30. |
| 5 | [`Ferrosa/Topology/Basic.lean`](Ferrosa/Topology/Basic.lean) | Shared vocabulary: `Topo`, `okAt`, `localOkAt`, `mayMiss`, failure-pattern enumerators. |
| 6 | [`Ferrosa/Topology/Pair.lean`](Ferrosa/Topology/Pair.lean) | The no-split-brain machine: `Step` is parameterised by the operator policy, so "no automatic role swap" is a theorem, not a comment. |
| 7 | [`Ferrosa/Topology/Single.lean`](Ferrosa/Topology/Single.lean) → [`ThreeNode`](Ferrosa/Topology/ThreeNode.lean) → [`FiveNode`](Ferrosa/Topology/FiveNode.lean) → [`TwoDc`](Ferrosa/Topology/TwoDc.lean) → [`ThreeDcRing`](Ferrosa/Topology/ThreeDcRing.lean) | One rung each, in ladder order. |
| 8 | [`Ferrosa/Topology/Ladder.lean`](Ferrosa/Topology/Ladder.lean) | The threshold table, fault tolerance per rung, and the properties escalation must preserve. |
| 9 | [`Ferrosa/Accord/History.lean`](Ferrosa/Accord/History.lean) | Transactions, ops, histories, the keyspace. Read `State.runOps` / `State.readsOf` and the composition lemma `runOps_append` — everything downstream rests on it. |
| 10 | [`Ferrosa/Accord/Serializable.lean`](Ferrosa/Accord/Serializable.lean) | The Jepsen statement: `Serializable`, `StrictlySerializable`, and their executable checkers. Then the five demo histories. |
| 11 | [`Ferrosa/Accord/Protocol.lean`](Ferrosa/Accord/Protocol.lean) | Accord's apply layer: `Replica.apply`, the dep-wait guard, `LogOk` as a certificate, `DepComplete` as the hypothesis, and the exhaustive check over apply interleavings. |
| 12 | [`Ferrosa/Accord/Recovery.lean`](Ferrosa/Accord/Recovery.lean) | §6.1's two ballots. `recovery_correct` vs `recovery_diverges`, and the write-skew history that a dropped dependency produces. |
| 13 | [`Ferrosa/Accord/Strict.lean`](Ferrosa/Accord/Strict.lean) | §7's deadline formula, and `deadline_yields_dependency` — why the buffer is what makes `DepComplete` true. |
| 14 | [`Ferrosa/Accord/Edn.lean`](Ferrosa/Accord/Edn.lean) | A dependency-free EDN reader for the dialect `elle_check.clj` consumes. |
| 15 | [`Ferrosa/Topology/ConsistencyLevels.lean`](Ferrosa/Topology/ConsistencyLevels.lean) | The ack rule as a *topology* guarantee: `RF=1` acks on one ack, `LOCAL_QUORUM` on the local quorum, a transaction's ack as a conjunction over its keys. Plus the NTS `QUORUM` analysis and the retraction recorded in FINDINGS A3. |
| 16 | [`Ferrosa/Cql/Lwt.lean`](Ferrosa/Cql/Lwt.lean) | LWT as compare-and-set: `at_most_one_applies`, the failure contract, and the unprotected-CAS history that the checker rejects. |
| 17 | [`Ferrosa/Cql/Refine.lean`](Ferrosa/Cql/Refine.lean) | The `Cell`/`State` refinement: `Simulates`, `decode_encode`, `simulates_append`, `lwt_refines`. |
| 18 | [`Ferrosa/Cql/Collections.lean`](Ferrosa/Cql/Collections.lean) | Collections: set adds commute, list appends do not, and `blind` never looks at the assignment terms. |
| 19 | [`Ferrosa/Cql/Index.lean`](Ferrosa/Cql/Index.lean) | Secondary indexes: readiness is a preference (`reorder_never_filters`), and the stale indexed read is not serializable. |
| 20 | [`Ferrosa/Cql/Aggregate.lean`](Ferrosa/Cql/Aggregate.lean) | Aggregates: `COUNT(*)` is an accumulator that forgets which rows; `COUNT(DISTINCT)` is exact; `COUNT(ESTIMATE_DISTINCT)` is a proved lower bound, and `estimate_can_be_strict` is why the two names cannot be swapped. |
| 21 | [`Ferrosa/Cql/AggregateObligations.lean`](Ferrosa/Cql/AggregateObligations.lean) | The red list: ten aggregate obligations, six unmet, each red item carrying the theorem that shows why. `lake env lean --run` prints it. |
| 22 | [`Ferrosa/Storage/Recovery.lean`](Ferrosa/Storage/Recovery.lean) | Crash recovery: the record format has no checksum, so truncation is caught and corruption is not; the stop-policy replays a *prefix* and skipping does not; durability is a configuration (`periodic_sync_loses_acked_writes`); a zero `mutation_id` double-applies counters. |
| 23 | [`Ferrosa/Storage/Tombstone.lean`](Ferrosa/Storage/Tombstone.lean) | Tombstones, TTL, purge. Both tie-breaks are required, proved by counterexample; a cell's `i32` local deletion time wraps past 2038; purge without propagation resurrects deleted data. |
| 24 | [`Ferrosa/Storage/Format.lean`](Ferrosa/Storage/Format.lean) | The `fnf` record framing (ADR-022): a torn file yields a prefix, one bad byte costs one record, the trie can only miss and never lie, and an absent footer is a named state rather than a failure. |
| 25 | [`Ferrosa/Sql/Semantics.lean`](Ferrosa/Sql/Semantics.lean) | The Postgres surface: `COMMIT` is a prefix, not atomic; a naive `<>` admits NULL rows; `SUM` over no rows returns 0 where SQL says NULL. |
| 26 | [`Ferrosa/Bolt/Session.lean`](Ferrosa/Bolt/Session.lean) | The Bolt session: only RESET leaves FAILED, RESET is total, and replies **cannot** be paired to messages by index — a failed session silently shifts the whole stream. |
| 27 | [`Ferrosa/Sparql/Semantics.lean`](Ferrosa/Sparql/Semantics.lean) | SPARQL: bag multiplicity; `p+` without a visited set returns one row per path; the natural RDF-star term encoding aliases a quoted triple with an IRI. |
| 28 | [`Ferrosa/Sql/Index.lean`](Ferrosa/Sql/Index.lean) | What the layer under Postgres must supply for indexes: equality is O(1) in nodes and records, a range **cannot** be, and one-sided index maintenance is inconsistent. |
| 29 | [`Ferrosa/Sql/Txn.lean`](Ferrosa/Sql/Txn.lean) | ACID split by letter: atomicity is one durable decision, durability is the commitlog premise moved to the decision, isolation is a priced choice (SNAPSHOT admits write skew). |
| 30 | [`Ferrosa/Storage/Mvcc.lean`](Ferrosa/Storage/Mvcc.lean) | Versions over `fnf`: the naive GC rule destroys a version a live snapshot needs; one version per SSTable costs a probe per file, which is exactly the O(1) guarantee the index work needs. |
| 30a | [`Ferrosa/Topology/DcQuorum.lean`](Ferrosa/Topology/DcQuorum.lean) | DC_QUORUM for **every** topology, transcribed from [`dc_quorum.rs`](../../ferrosa-cluster/src/coordinator/dc_quorum.rs): any two writes share a replica (`dc_quorum_writes_share_a_replica`, `no_partition_admits_two_dc_quorum_writes`), the node gate alone intersects Cassandra's QUORUM, a same-origin read sees the write and a remote one can miss it, verdicts are monotone in acks, a dark majority of DCs blocks writes, and an `rf = 0` DC is inert. [`Ferrosa/Sql/Consistency.lean`](Ferrosa/Sql/Consistency.lean)'s DC rules delegate here. |
| 31 | [`Ferrosa/Sql/Consistency.lean`](Ferrosa/Sql/Consistency.lean) | Consistency as configuration: ALL/QUORUM/LOCAL_QUORUM/ONE, default QUORUM, an acceptance table the tests check against, and the read/write overlap results. |
| 32 | [`Ferrosa/Broken.lean`](Ferrosa/Broken.lean) | The red inventory, classified: 38 red items as code / requirement / proof / measurement / inherited / undecided, with the table pinned to the ledgers by count. |
| 33 | [`Ferrosa/Unimplemented.lean`](Ferrosa/Unimplemented.lean) | What is absent rather than wrong, plus the ledger items that are requirements, missing proofs, measurements, inherited protocol behaviour, or closed. |
| 34 | [`Ferrosa/Witnesses.lean`](Ferrosa/Witnesses.lean) | One `#check` per witness any ledger names. Ledger witnesses are strings, so without this a deleted theorem leaves the ledger citing a name that no longer exists. |
| 35 | [`Ferrosa/Storage/Streaming.lean`](Ferrosa/Storage/Streaming.lean) | Memory and the pipeline: zero-copy transforms, `inbound` bytes never written, peak memory independent of input size, and the page bound that is in rows rather than bytes. |
| 36 | [`FINDINGS.md`](FINDINGS.md) | Every inconsistency, with the theorem that exhibits it. |

## How to read a spec file

Each file opens with the doc comment naming the source document and quoting the
claim being checked. Theorems are stated in the spec's own vocabulary —
`t.okAt down key .quorum`, `mayMiss ack probe`, `Writable s 0` — so a reviewer can
compare them against the prose without reading Lean tactics.

A theorem name that reads like a sentence is deliberate: `rf2_has_no_fault_tolerance`,
`local_quorum_acks_can_be_disjoint`, `config_never_propagates`. The tactic block
is the evidence; the name is the claim.

## What is *not* modelled

Being explicit about the boundaries keeps the models honest:

- **Token placement.** `Topo.replicasOf` is given data, not `TokenRing::replicas`.
  The models reason about quorum *sizes* and *overlap*, which is what the
  consistency claims depend on; they say nothing about Murmur3 token skew.
- **The Accord protocol's message layer.** `Accord.Protocol` models the apply
  layer and the dependency sets it obeys; the PreAccept/Accept/Commit exchange is
  represented by the `LogOk` certificate, i.e. by its *effect* on the log rather
  than by its messages. `AccordMultiDcLean` encodes the watermark / apply-buffer
  layer of `multi-dc.tla`, because that is all that file defines.
- **Full-history serializability search.** `Accord.Serializable` decides the
  property by enumerating orders, which is factorial. On a real run
  (2400 committed transactions) the replay form — supply the order the run
  actually produced and check every read — is the usable one.
- **Storage format.** `Store` treats the commit log, memtables and SSTables as
  ordered row lists. SSTable encoding, S3 upload and manifest CAS are out of
  scope; the durability boundary they sit behind is modelled by `synced`.
- **Wall-clock and HLC skew.** `ts` is an abstract timestamp.

## Status

`lake build` completes over all 59 modules; no `sorry`, no `axiom`, no
dependency outside Lean core. Remaining hygiene step: a `#print axioms` sweep to
record the same fact as an artifact rather than as an absence of warnings.

The Accord checker has been run against a real Elle history
(`.wt-pr457-linux/deploy/fly-accord-elle/elle-cert-e860ec88-*.edn`):
4800 events, 2400 committed transactions, 0 failures, and the first 4- and
5-transaction windows verify as serializable by exhaustive search over orders.
Reproducing it is the `lake env lean --run` line at the top of this file.

## The book

`make book` renders this corpus as a PDF: seven parts, one chapter per module, high-level claims
first and the formal statements underneath them.

There is no second document. The prose the PDF prints is the prose in this directory — the `/-!`
sections are its headings and paragraphs, the `/-` header of each module is its chapter opening, the
`/--` docstrings are the sentences under each claim, and the ledgers under `Unimplemented.areas` are
its list of obligations. Adding a paragraph next to a theorem puts it in the book; the renderer
never invents text.

Statements are not scraped from the sources. `lake exe docs` reads them out of the compiled
environment, so what the PDF shows is what the elaborator holds — and it prints them the way the
editor does, with record fields named:

```text
Readable { r0 := Role.primary, r1 := Role.secondary, up0 := false, up1 := true } 1 ∧
¬Writable { r0 := Role.primary, r1 := Role.secondary, up0 := false, up1 := true } 1
```

The source for that theorem says `⟨.primary, .secondary, false, true⟩`. The positional form costs a
reader four field positions to decode; the printed form says the surviving node is the secondary and
therefore not writable.

What the build checks, beyond compiling:

- every ledger claim's `.met` string names a declaration that exists (a typo was silent before this);
- every registered area ledger has a chapter in `Ferrosa/Docs/Book.lean`;
- the PDF's red count is `Unimplemented.ledgerTotal` — 59, pinned by `decide` — not a number the
  renderer computed;
- the book places exactly the modules the corpus contains (`placed_module_count`).

`make part P=II` renders one part for review. Engine: `tectonic` if installed, otherwise `latexmk`
with XeLaTeX; the `.tex` is identical either way. CI builds it in `lean-specs.yml` and uploads the
artifact.

