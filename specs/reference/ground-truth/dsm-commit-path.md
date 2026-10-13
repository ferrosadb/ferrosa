# DSM Analysis — Accord Commit Path

> **Revision analysed:** `43ac79cb3023679c00cee4b4306e52fa2d5c52da` (branch `feat/read-in-transaction`,
> 2026-10-12 11:36:10 -0700, "docs(recon): experiment result — an abandoned commit leaves the table
> UNREADABLE"). Read-only: no source file was modified.
> **Workspace:** `/Users/bkearns/src/ferrosa-suite/.wt-int2`
> **Scope:** the commit path — `ferrosa-cluster/src/accord/**` plus the front-end / storage boundary
> files it hands off to (`ferrosa-postgres/src/{extended,txn_write_set,copy_stdin,server}.rs`,
> `ferrosa-storage/src/{write_set_spill,engine}.rs`, `ferrosa-storage/src/accord/{sync_writer,conflict_index,transaction_committer}.rs`).
> **Ground truth of record:** `specs/reference/commit-path-ground-truth.md` (G1–G10, U1–U6).

## 0. Tools and methods used — and what each one could NOT do

| # | Method | Command / path | Status |
|---|---|---|---|
| A | **`frg dsm`** (Forge DSM), `frg 0.18.2-nightly` at `/Users/bkearns/.ferrosa/bin/frg` | `frg dsm analyze ferrosa-cluster --level full --prefix 'crate::accord' --format json`; also `--with-cognitive`, `frg dsm extract --level full` | **Ran.** Primary tool. |
| B | **Hand-built import/use matrix** (Python, regex over `use`-paths **and** inline `crate::accord::…` path references, comments stripped, `mod tests` excluded), with Tarjan SCC + articulation-point analysis | ad-hoc script; edge dump at `/Users/bkearns/.hermes/cache/scratch/dsm/accord-prod-matrix.json` | **Ran.** Used to falsify/confirm A. |
| C | **`mcp__forge__module_outline`** + **`mcp__forge__cognitive_complexity`** | `module_outline` on the six commit-path files; `cognitive_complexity --churn --exclude-tests --max-cognitive 30` | **Ran.** Responsibility + complexity evidence. |

### Why two methods, and A's measured limits (this is the headline methodological finding)

`frg dsm analyze --level full` **did run** and did produce a real matrix — but measuring its
extraction against the source shows it is a **lower bound**, and three of its headline metrics are
artefacts of *what it counts*, not of the design:

1. **It counts only `use` lines, not inline paths.** `apply.rs` has 1 accord `use` line (`apply.rs:34`)
   but `coordinator.rs` carries **142** inline `crate::accord::` references against only 37 `use`
   lines. Over the whole subtree there are **266** `crate::accord::` occurrences (**116** in production
   regions, **87** of which resolve to a sibling production module — the Method-B matrix above). A's
   edge count for the accord subtree is **97 edges / 71 elements**, of which only **100** edges touch
   accord at all. A under-reports coupling by roughly an order of magnitude on the highest-churn file.
2. **It drops `pub use` re-exports.** `mod.rs:57-95` re-exports **88 names** from **21** modules. A reports
   `crate::accord:::` with **fan-in 26, fan-out 0** — a "god element" (`suggestions[0]`:
   `"crate::accord::: has fan-in of 26 (god element)"`). That fan-out of 0 is wrong: the facade *does*
   depend on every module it re-exports. A models the facade as a pure sink, which is exactly what
   makes a real graph look like a star.
3. **Consequently A's acyclicity is under-determined.** A reports `max_cycle_size: 0`,
   `num_cycles: 0`, `propagation_cost: 0.0333`. Those numbers are *not* evidence the design is acyclic,
   because A's graph is the star, and a star is trivially acyclic. **B therefore re-derived the graph
   independently** — see §3, where the acyclicity claim is re-established on better evidence rather
   than inherited. (B also caught its own false positive: an apparent `wire → apply` cycle turned out
   to be a rustdoc link at `wire.rs:208`, `/// [StorageReader]: crate::accord::apply::StorageReader` —
   not code. Comment-stripping removed it.)

**So: every structural claim below is stated with the method that produced it, and claims resting only
on A are marked `[A only]`.**

---

## 1. `frg dsm` output — element-level structure matrix (Method A)

`frg dsm analyze ferrosa-cluster --level full --prefix 'crate::accord'` → 71 elements, 97 edges.

| Metric | Value | Skill threshold | Verdict |
|---|---|---|---|
| Propagation cost | **0.0333** | `< 10%` good | "good" — **but computed on the facade-star graph; see §0.2** |
| Max cycle size | **0** | `0–2` good | good — **under-determined; see §3** |
| Num cycles | **0** | — | see §3 |
| Cluster quality | **0.7216** | `50–80%` warning | warning |
| Elements / edges | 71 / 97 | — | — |

**A's clusters, largest first** (cohesion = internal deps ÷ possible pairs):

| Cluster | Size | Cohesion | Members (A's names) |
|---|---|---|---|
| 5 | **17** | **0.0588** | `crate::accord:::`, `clock`, `clock_validation`, `compression`, `cross_dc_adapter`, `ddl_drain`, `dep_wait`, `durability`, `epoch`, `finalized`, `leaseholder`, `linearizable_read`, `metrics`, `reorder_buffer`, `test_cluster`, `two_phase_ddl`, `wire` |
| 12 | **19** | **0.09** | `abandon_durability`, `coordinator`, `coordinator::`, `AccordCoordinatorDriver`, `driver_cross_shard_e2e`, `handlers`, `handlers::`, `AccordState`, `empty_accord_state_slot`, `state_machine::AccordStateMachine`, `transaction_commit`, `apply::EngineStorageApplier`, `apply::StorageApplier`, `transport::AccordTransport`, `wire::{ApplyOkPayload,ApplyPayload,ReadPredicate}`, `shard_quorum::ParticipantSet` |
| 0,1,2,3,4,7,8,9,10,11,13 | 2–3 | 0.33–0.50 | mostly test-pair noise |

**Both dominant clusters are below the 50% "critical" cohesion line**, and A's own suggestions say so:

- `SplitPackage` / `Cluster 5` — `"Large cluster (17 elements) with low cohesion (0.06)"`
- `SplitPackage` / `Cluster 12` — `"Large cluster (19 elements) with low cohesion (0.09)"`
- `ExtractInterface` / `High` / `crate::accord:::` — `"fan-in of 26 (god element)"` `[A only — see §0.2; this is the facade, and the fan-in is real but the fan-out 0 is not]`

**A's load-bearing ranking (`fan_in`) and its partition layers:**

```
fan_in=26 crate::accord:::            (the mod.rs facade)
fan_in= 9 crate::accord::test_cluster::        <- test harness, not production
fan_in= 7 crate::accord::state_machine::AccordStateMachine
fan_in= 5 crate::accord::apply:: / transport::AccordTransport
fan_in= 4 crate::accord::handlers:: / state_machine::
fan_in= 3 crate::accord::coordinator:: / handlers::AccordState / wire:: / wire::ReadPredicate
```
`partition.feedback_count = 0`, layers `["Foundation", "Infrastructure"]` — i.e. A finds a
**2-layer, feedback-free** structure. Consistent with §3 but not decisive for it.

---

## 2. Hand-built module-level DSM (Method B) — the matrix to actually reason from

Production code only: comments stripped, everything from `mod tests` excluded; edges are
`crate::accord::X` reference **sites** (count, not boolean). Full machine-readable dump:
`/Users/bkearns/.hermes/cache/scratch/dsm/accord-prod-matrix.json`.

| from \ to | wire | handlers | state_machine | apply | shard_quorum | transport | compress | finalized | epoch_drain | dep_wait | **out** |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `coordinator` | **19** | **13** | 5 | 6 | 9 | 3 | 1 | . | . | . | **56** |
| `handlers` | 1 | . | **5** | . | . | . | . | . | . | . | 6 |
| `state_machine` | 1 | . | . | **7** | . | . | . | 1 | 1 | . | 10 |
| `transaction_commit` | 1 | 4 | . | 1 | . | 1 | . | . | . | . | 8 |
| `apply` | 1 | . | . | . | . | . | . | . | . | 1 | 2 |
| `quorum_availability` *(cfg(test))* | . | 1 | 1 | 1 | . | 1 | . | . | . | . | 5 |
| `wire` | . | . | . | . | . | . | . | . | . | . | **0** |
| **fan-in** | **5** | **3** | **3** | **4** | 1 | 3 | 1 | 1 | 1 | 1 | |

**Crate level (Method A, `frg dsm extract . --level summary`, plus `Cargo.toml` cross-check).** The
commit path crosses three crates and is a clean DAG:

```
ferrosa-postgres ──> ferrosa-cluster ──> ferrosa-storage ──> ferrosa-common
 (front-end txn        (accord:            (SyncWriter,        (TxnId,
  buffer, MVCC)         coordinator)       ConflictIndex,      Timestamp)
                                           StorageEngine)
```
`ferrosa-cluster/Cargo.toml:19` → `ferrosa-storage`; `ferrosa-postgres/Cargo.toml:59` →
`ferrosa-cluster`; `ferrosa-storage/Cargo.toml` has **no** dependency on `ferrosa-cluster` (line 86
mentions it only in a comment). Confirmed acyclic across crates.

---

## 3. Cycles — **there are none, and I verified it rather than inheriting it**

Method B, production-only graph, Tarjan SCC: **`SCCs > 1: 0`**. No multi-node cycles, no self-recursion.

Three apparent cycles existed in the naive (comments-included, test-included) matrix. All three
evaporated under classification, and each is worth recording because each is a *near-miss*:

| Apparent cycle | Where it actually came from | Verdict |
|---|---|---|
| `wire → apply` | `wire.rs:208` — a rustdoc intra-doc link `/// [StorageReader]: crate::accord::apply::StorageReader` | **Not code.** False positive of Method B, caught by B. |
| `apply → state_machine` | `apply.rs:2718`, `apply.rs:2729` — closure annotations inside `mod tests` (`mod tests` starts at `apply.rs:1990`) | **Test-only.** |
| `state_machine → handlers` | `state_machine.rs:1721` `use crate::accord::handlers::DEFAULT_TXN_TIMEOUT;` — inside `mod tests` (`mod tests` starts at `state_machine.rs:1718`) | **Test-only.** |

The surviving production cycle set is empty. So A's `num_cycles: 0` is **correct**, but for a reason A
could not demonstrate: the acyclic graph A saw was the facade-star, and the acyclic graph B sees is the
real one. Both agree.

### Why the graph is acyclic, and why that is *not* good news

The absence of cycles is **bought with a facade and with oversized modules**, not with clean seams:

- **Facade masking.** `mod.rs:57-95` re-exports **88 names from 21 modules**. Every module writes `crate::accord::X`, so
  the *module* graph is a star through `crate::accord:::` — edges that would otherwise be
  `apply → state_machine` are recorded as `apply → facade`. A dependency cycle can therefore be
  *created* in fact and *invisible* to both tools if it routes through the facade. The acyclicity is a
  property of the naming convention as much as of the design.
- **Cycles flattened into functions.** `AccordCoordinatorDriver::order_and_gate`
  (`coordinator.rs:1530`) is **993 lines** with **cognitive complexity 342** (threshold 10, band
  `severe`, nesting 7). It dispatches PreAccept/Accept/Commit/ReadVote for both the legacy and the
  capnp wire families — `coordinator.rs:1531-1533` alone imports
  `AcceptOkPayload, AcceptPayload, CommitOkPayload, CommitPayload, LegacyAcceptOkPayload,
  PreAcceptOkPayload, PreAcceptPayload, PreAcceptV2Payload, ReadVoteOkPayload, …`. A design whose
  phases each call back into a shared coordinator would be a cycle; here the cycle is *serialised
  inside one function*. Module-level DSM cannot see that, which is precisely why the matrix must be
  read alongside §5.
- **Data cycles remain, and DSM cannot show them.** Ground truth **G2** (`dep_wait.rs:196-224`): I
  verified by reading that `mark_applied` removes `waited_by[txn_id]` (the set of waiters *on*
  `txn_id`) and clears `waiting_on`/`wait_start` for `txn_id` — but when `txn_id` was itself a *waiter*
  on some `dep`, it never removes `txn_id` from `waited_by[dep]`. Two maps for one relation,
  hand-synchronised, and one goes stale. That is a **cycle in the data model** (`waiting_on` ⇄
  `waited_by`), invisible to every tool above.

---

## 4. Cohesive or grab-bag — per module, with hosted responsibilities

Responsibilities are labelled from cited code, not taste. The full legend and the evidence anchors are
in §6. Hosted counts are derived from the module outlines (Method C) plus the outline/import evidence.

| Module | Prod LOC | Prod/test LOC | Responsibilities **hosted** | Cohesive or grab-bag |
|---|---|---|---|---|
| `state_machine.rs` | **1717** | 1717 / 2595 | **R1** consensus-core (`:389,565,633`), **R4** durability-journal (`:892,1296`), **R7** read-path/MVCC (`:271,978,336`), **R8** recovery (`:921`), **R9** observability (`:276`) → **5** | **Grab-bag — the only module that hosts more than three responsibilities itself.** fan-in 3 (weighted 11); its `state_machine → apply:7` is the heaviest single internal edge after `coordinator`'s. Hosts the protocol phase machine, the fsync journal, the MVCC read index *and* the observability hook. |
| `coordinator.rs` | **3409** | 3409 / 4497 | **R1** consensus-core (`:297,408`), **R3** apply-fanout (`:1033,2545,2955`) → 2 by outline, but see below | **Grab-bag in depth.** Only 2 labelled responsibilities, but 3409 prod lines and the worst function in the repo (`order_and_gate`, cog **342**, 993 lines) which internally spans R1+R3+R6 wire-family dispatch. Largest fan-out (**56** reference sites). |
| `apply.rs` | **1989** | 1989 / 2433 | **R5** storage-mutation (`EngineStorageApplier`), **R9** observability | **Cohesive-ish** — 1989 lines but only **2** outgoing accord edges (`apply.rs:34 → dep_wait`, `apply.rs:552 → wire`). Near-leaf, narrow interface, wide body. Refactor target for *size*, not for *boundary*. |
| `handlers.rs` | **817** | 817 / 684 | **R7** read-path (`:198,504`), **R9** observability (`:65`) + RPC dispatch (`AccordHandler::handle`, `:429`, 386 lines, cog **47**) | **Grab-bag.** The inbound-RPC entry point for *every* accord message type — it is a dispatcher, and its responsibility count is bounded only by the protocol. |
| `transaction_commit.rs` | **533** | 533 / 789 | **R12** txn-commit-adapter (`:174`), **R9** observability (`:108`) | **Mixed.** Implements `ferrosa_storage::accord::TransactionCommitter` **and** registers a Postgres-MVCC observer — a commit adapter that also owns an observability concern. |
| `wire.rs` | **328** | 328 / 507 | **R3** apply-fanout payloads, **R6** codec | **Cohesive.** Fan-in 5 (highest), fan-out 0. A genuine, load-bearing, narrow module. **This is what a good seam looks like here.** |
| `dep_wait.rs` | 487 | 487 / 437 | **R2** depwait (`:86,135,196`) | **Cohesive interface, defective internals** (G2). Only dependents: `apply.rs:34`. |
| `mod.rs` | 95 | 95 / 0 | facade / re-export hub (88 names from 21 modules, `:57-95`) | **Cohesive** as a facade, but the facade is what makes the graph unreadable (§3). |
| `recovery.rs` | 358 | 358 / 373 | **R8** recovery | Cohesive; **zero in-degree in production** — reachable only from tests (`recovery_scenarios.rs`). |
| `finalized.rs` | 275 | 275 / 118 | retention horizon for R4 | Cohesive; fan-in 1 (`state_machine.rs:43`). |
| `epoch.rs`, `epoch_drain.rs`, `electorate.rs`, `leaseholder.rs`, `durability.rs` | 934 total | — | **R11** epoch/electorate/lease | Cohesive cluster; **zero in-degree from the commit path** — the epoch machinery is structurally detached. |
| `test_cluster.rs`, `proptests.rs`, `perf*.rs`, `jepsen_*.rs`, `chaos_minority_kill.rs`, `quorum_availability.rs`, `recovery_scenarios.rs`, `driver_cross_shard_e2e.rs`, `abandon_durability.rs`, `uda_integration.rs` | ~2 300 prod | — | **R14** harness | **The single biggest structural fact about testability.** |

### The test/testability numbers (measured)

`ferrosa-cluster/src/accord` = **35 208 lines**: **14 600 production, 20 608 test** (58.5% test). The
**top four production files hold 49%** of all production lines (`coordinator` 3409, `apply` 1989,
`state_machine` 1717, `handlers` 817). Six modules are `#[cfg(test)]`-gated (`mod.rs:6,18,43` plus
feature gates at `:33,35,40`).

---

## 5. Load-bearing vs seam — the modules spanning more than three responsibilities

This is the explicit question. Two measures are reported, because they answer two different questions.

**(a) Hosted responsibilities > 3 — the grab-bags.** Only one module hosts more than three distinct
responsibilities *itself*:

- **`state_machine.rs` — 5 hosted responsibilities** (R1 consensus-core, R4 durability-journal,
  R7 read-path/MVCC, R8 recovery, R9 observability). Evidence: `state_machine.rs:389/565/633`
  (phase machine), `:892` and `:1296` (`bookkeep_applied` — the fsync marker of G1),
  `:271/978/336` (read path + conflict index), `:921` (`handle_recover`), `:276`
  (`register_postgres_mvcc_observer`).

**(b) Neighbourhood span > 3 — the refactor seams.** A module is a seam if *itself, everything it
depends on, and everything that depends on it* together span more than three distinct
responsibilities. Computed over the Method-B graph:

| Module | Span | Own hosted | Neighbourhood |
|---|---|---|---|
| `apply` | **11** | R5, R9 | R1,R2,R3,R4,R5,R6,R7,R8,R9,R12,R14 |
| `coordinator` | **11** | R1, R3 | R1,R3,R4,R5,R6,R7,R8,R9,R12,R13,R14 |
| `state_machine` | **11** | R1,R4,R7,R8,R9 | R1,R3,R4,R5,R6,R7,R8,R9,R11,R12,R14 |
| `handlers` | **10** | R7, R9 | R1,R3,R4,R5,R6,R7,R8,R9,R12,R14 |
| `transaction_commit` | **10** | R12, R9 | same 10 |
| `wire` | **10** | R3, R6 | same 10 |
| `transport` | 5 | — | R1,R3,R9,R12,R14 |
| `finalized`, `compression`, `epoch_drain` | 6 | — | R1,R4,R7,R8,R9(+R11) |
| `proptests`, `test_cluster` | 4 | R14 | R1,R3,R8,R14 |

**⇒ Six production modules span more than three responsibilities: `apply`, `coordinator`,
`state_machine`, `handlers`, `transaction_commit`, `wire`.** These are the candidate seams.

**Load-bearing (many dependents), production graph, module level:**

| Module | fan-in | fan-out | Reading |
|---|---|---|---|
| `wire` | **5** | 0 | The most load-bearing module and the cleanest. Pure type/codec surface. |
| `apply` | **4** | 2 | Load-bearing *and* near-leaf — a good boundary with an oversized body. |
| `handlers`, `state_machine`, `transport` | 3 | 3/5/0 | `transport` is the injectable seam (`AccordTransport`, `coordinator.rs:33`) — correct. |
| `coordinator` | 1 | **7 (56 sites)** | The **top of the DAG**: nothing depends on it, it depends on almost everything. |

**Articulation points** of the production commit-path subgraph (removal would disconnect it):
**`apply`, `coordinator`, `state_machine`** (plus `test_cluster`/`perf*` from the harness side).
Those three are structurally indispensable — which is a warning, not a compliment: they are
indispensable because responsibilities are *concentrated* there, not because they are stable
abstractions.

**Churn corroboration (Method C, `--churn`).** The two highest-complexity accord files are also the two
highest-churn, matching the task's stated history:

| File | Commits | Cognitive total | Max cognitive | Hotspot score |
|---|---|---|---|---|
| `coordinator.rs` | **71** | 462 | **342** | 2437.8 |
| `handlers.rs` | **30** | 47 | 47 | 208.4 |

Only **4 functions** in the whole accord subtree reach cognitive ≥ 30, and all four are on the commit
path — holding **509 of 1134** total cognitive points (45%):

| Function | file:line | Lines | Cognitive | Nesting |
|---|---|---|---|---|
| `AccordCoordinatorDriver::order_and_gate` | `coordinator.rs:1530` | **993** | **342** | 7 |
| `AccordCoordinatorDriver::apply_fanout_bounded_within` | `coordinator.rs:2955` | 252 | 63 | 4 |
| `AccordCoordinatorDriver::apply_phase_within` | `coordinator.rs:2545` | 318 | 57 | 4 |
| `AccordHandler::handle` | `handlers.rs:429` | 386 | 47 | 3 |

---

## 6. Responsibility legend (evidence anchors)

| ID | Responsibility | Anchor |
|---|---|---|
| R1 | Consensus core: PreAccept / Accept / Commit phases | `state_machine.rs:389,565,633`; `coordinator.rs:297,408` |
| R2 | Dependency-wait graph | `dep_wait.rs:86,135,196` |
| R3 | Apply fan-out and ack | `coordinator.rs:1033,2545,2955`; `wire.rs` ApplyPayload/ApplyOkPayload |
| R4 | Durability / journal: protocol-log Applied marker, retention | `state_machine.rs:892,1296,1026`; `ferrosa-storage/src/accord/sync_writer.rs:112` |
| R5 | Storage mutation (rows + fragments) | `apply.rs` `EngineStorageApplier`; `ferrosa-storage/src/engine.rs:9658-9669` (G6) |
| R6 | Wire codec | `wire.rs` (whole module) |
| R7 | Read path / MVCC read index | `state_machine.rs:271,978,336`; `handlers.rs:198,504` |
| R8 | Recovery | `state_machine.rs:921`; `recovery.rs` |
| R9 | Observability (metrics + Postgres-MVCC hook) | `metrics.rs`; `state_machine.rs:276`; `handlers.rs:65`; `transaction_commit.rs:108` |
| R10 | Two-phase DDL | `two_phase_ddl.rs`; `ddl_drain.rs` |
| R11 | Epoch / electorate / lease / durability-health | `epoch.rs`; `epoch_drain.rs`; `electorate.rs`; `leaseholder.rs`; `durability.rs` |
| R12 | Transaction-commit adapter (`TransactionCommitter` impl) | `transaction_commit.rs:174`; trait at `ferrosa-storage/src/accord/transaction_committer.rs:311` |
| R13 | Shard quorum / cross-shard | `shard_quorum.rs`; `cross_shard.rs` |
| R14 | Test / benchmark harness | `test_cluster.rs`, `proptests.rs`, `perf*.rs`, `jepsen_*.rs`, `chaos_minority_kill.rs`, `quorum_availability.rs`, `recovery_scenarios.rs`, `driver_cross_shard_e2e.rs`, `abandon_durability.rs`, `uda_integration.rs` |

---

## 7. The re-cut I would propose — and the evidence for it

**Framing.** The DSM says the module graph is **already a DAG**, at module *and* crate level (§2, §3).
So this is *not* a cycle-breaking job, and any plan that starts by "untangling cycles" is solving a
problem the matrix does not show. The matrix shows three things instead: (i) responsibility
concentration in four files (49% of production lines); (ii) one function, `order_and_gate`
(993 lines, cog 342), that is doing the work of a state machine; (iii) a transaction whose state has
**three owners in three crates**. The re-cut is therefore by *responsibility and ownership*, not by
graph surgery.

### Re-cut 1 — `state_machine.rs` (the one module that genuinely spans > 3 responsibilities)

Split the 1717 production lines along the five hosted responsibilities. **Evidence:** §5(a); the
module hosts R1+R4+R7+R8+R9; its weighted in-degree (`state_machine → apply:7`) is the largest internal
weight in the graph after `coordinator`'s.

| Extract to | Responsibility | Moves from `:line` |
|---|---|---|
| `consensus/txn_phase.rs` | R1 — PreAccept/Accept/Commit transitions | `:389`, `:565`, `:633` |
| `durability/applied_journal.rs` | R4 — the fsync'd Applied marker (**the G1 mechanism**) | `:892`, `:1296`, `:1026` |
| `read/conflict_index.rs` | R7 — read-row / condition-holds / unapplied-conflicts | `:271`, `:978`, `:336` |
| keep in place | R8 recovery entry (`:921`), R9 hook (`:276`) | — |

The reason this is the **highest-value cut** is not cohesion: it is that **G1 lives here**. G1 is the
finding that an abandon can be a lie about durability, because `bookkeep_applied` returns `Err(())`
when the protocol-log `Applied` marker's fsync fails (`state_machine.rs:1296-1310`). Today that
correctness-critical marker write sits in the same 1717-line module as the phase machine, the read
index and the observability hook, so no test can assert "the marker is durable **or** the txn is not
reported Applied" without dragging in all four. Extracted to `applied_journal.rs` with a two-state
return type instead of `Result<(), ()>`, that invariant becomes a unit test.

### Re-cut 2 — `coordinator.rs`: decompose `order_and_gate`, do not move it

**Evidence:** `coordinator.rs:1530`, 993 lines, cognitive **342**, nesting 7, fan-out **56 reference
sites across 6 modules** — the single worst function in the accord subtree and in the churn ranking
(71 commits). It already reaches for every wire family in one `use` (`:1531-1533`).

Cut it by **phase**, each phase a type with its own transition function (PreAccept / Accept / Commit /
ReadVote), and by **wire family** behind the `R6` codec surface (legacy bincode vs capnp — ground
truth **G9** flags the mix as a defect, not a preference). The acceptance test changes shape from
"drive `order_and_gate` to a decision" to "assert the PreAccept transition", which is what makes the
correctness provable.

### Re-cut 3 — one transaction record, one owner per hop (the cross-crate cut)

**Evidence:** the transaction's state is owned by **three structures in two crates**, and the two
front-end structures sit in the *same* crate while disagreeing about who owns an uncommitted change:

| Phase of one transaction's life | Owner structure | Crate | Anchor |
|---|---|---|---|
| uncommitted changes (the *intended* concurrency buffer) | `TxnWriteSet` | `ferrosa-postgres` front end | `extended.rs:103` (`txn_writes: TxnWriteSet`), `txn_write_set.rs:47-52`; spilled at `WRITE_SET_SPILL_FLOOR_BYTES = 8 MiB`, `write_set_spill.rs:77` |
| committed version history (what *is* the concurrency buffer today) | `mvcc::State` | **`ferrosa-postgres`** | `ferrosa-postgres/src/mvcc.rs:259-260` — `versions: HashMap&lt;RowKey, BTreeMap&lt;u64, Option&lt;Row&gt;&gt;`, `distributed_versions: HashMap&lt;RowKey, BTreeMap&lt;Timestamp, Option&lt;Row&gt;&gt;&gt;` |
| in-flight coordination, dep graph, ack bookkeeping | `AccordStateMachine` + `DepWaitGraph` | `ferrosa-cluster` | `state_machine.rs:86`, `dep_wait.rs:86` |

This is ground truth **G3** verbatim: "The intended concurrency buffer is not where the code buffers
… The intent is therefore a design gap, not a description of the system." The DSM sharpens *where*
the gap sits, and it is not primarily a crate-boundary problem: **`ferrosa-postgres` owns both the
uncommitted buffer (`TxnWriteSet`) and the committed history (`mvcc::State`)** — two structures, in one
crate, whose intended relationship is that MVCC *be* the buffer. The third owner is across the crate
boundary in `ferrosa-cluster`, reached through a strictly one-way DAG
(`ferrosa-postgres → ferrosa-cluster → ferrosa-storage`), so no single module can hold the invariant.
**The handoffs are where the duplicate-and-hand-synchronise bugs live** — G2's two maps
(`dep_wait.rs:196-224`), G1's marker, G5's five-hop ownership chain
(`TxnWriteSet → StagedWriteSet → StagedAccordWriteSet → Vec<WriteSetEntry> → AccordWriteSet`).

The cut: make the transaction record a **first-class type in `ferrosa-common`** (where `TxnId`/
`Timestamp` already live, `ferrosa-common::accord`), with the front-end buffer, the coordinator and the
storage applier each holding a *reference* to it rather than a copy of part of it. Then G2's
`waiting_on`/`waited_by` pair collapses to **one** map, because a single owner removes the need for a
hand-maintained reverse index — and G5's five-hop chain becomes one pointer hop per boundary. Because
two of the three owners are already in one crate, the first milestone is achievable without touching
the crate graph at all.

### Re-cut 4 — the facade, so the matrix becomes readable

**Evidence:** `mod.rs:57-95` re-exports 62 names from 17 modules; Method A reports the facade as
`fan_in 26, fan_out 0`. Every real dependency routed through `crate::accord::X` is invisible to both
tools, which is why A's propagation cost (3.3%) is not usable as evidence (§0.2).

Replace the flat 62-name re-export with **three named facades** matching the cut above
(`accord::protocol`, `accord::apply`, `accord::read`). This is the cheapest cut and it is a
prerequisite for the others: without it, the DSM cannot detect a cycle that is introduced by the
refactor itself.

### What I would **not** cut

- **`wire.rs`** — fan-in 5, fan-out 0, 328 production lines, one responsibility pair (R3/R6). This is
  the template for the seams the others should be cut into.
- **`transport.rs`** — 124 lines, the `Arc<dyn AccordTransport>` injection point (`coordinator.rs:33`,
  `coordinator.rs:922-928`). It is already the testability seam; keep it.
- **`apply.rs`** — 1989 lines looks alarming, but only **2** outgoing accord edges. The problem is body
  size, not boundary. Splitting it will *add* coupling, not remove it. Reduce `apply_writeset_views`
  (cog 25) internally and leave the module boundary alone.
- **`epoch*` / `electorate` / `leaseholder` / `durability`** — zero in-degree from the commit path.
  Structurally detached already; touching them is out of scope for this refactor.

### Ordered plan (lowest risk first)

1. `mod.rs` → three named facades (Re-cut 4). *Enables measurement of 2–4.*
2. `state_machine.rs` → extract `applied_journal.rs` **first** — it carries G1, the highest-severity
   confirmed defect, and it is the smallest of the four extractions. *Test: the G1 invariant.*
3. `state_machine.rs` → extract `conflict_index` read surface (R7) and the phase machine (R1).
4. `coordinator.rs` → decompose `order_and_gate` by phase, then by wire family (Re-cut 2).
5. Transaction record in `ferrosa-common`; collapse `waiting_on`/`waited_by` to one map (Re-cut 3).
   *Largest, highest-payoff, and depends on 1–4 being in place to be verifiable.*

---

## 8. Reproduction

```bash
cd /Users/bkearns/src/ferrosa-suite/.wt-int2
git rev-parse HEAD                                   # 43ac79cb3023679c00cee4b4306e52fa2d5c52da

# Method A
frg dsm analyze ferrosa-cluster --level full --prefix 'crate::accord' --format json --pretty
frg dsm analyze ferrosa-cluster --level full --prefix 'crate::accord' --with-cognitive --format json
frg dsm extract ferrosa-cluster --level full
frg dsm extract . --level summary                    # crate-level graph

# Method B — dump written by the ad-hoc script
cat /Users/bkearns/.hermes/cache/scratch/dsm/accord-prod-matrix.json
cat /Users/bkearns/.hermes/cache/scratch/dsm/full-edges.json     # A's raw edges, 442 total / 100 accord-touching

# Method C
#   mcp__forge__module_outline  (file=ferrosa-cluster/src/accord/{coordinator,state_machine,handlers,transaction_commit,dep_wait}.rs)
#   mcp__forge__cognitive_complexity (path=.../accord, churn=true, exclude_tests=true, max_cognitive=30)
```

### Caveats stated plainly

- Method B's edge rule counts **reference sites**, including type annotations in signatures (e.g.
  `coordinator.rs:936` `state: crate::accord::handlers::AccordState`). It over-counts signature-level
  coupling relative to call-level coupling. It is a coupling *proxy*, consistently applied, and its
  ranking — not its absolute numbers — is what the conclusions rest on.
- Method B resolves only `crate::accord::…` absolute paths. A dependency written as `super::x` or
  `self::x` inside `accord/` would be missed. Spot-checking `mod.rs:57-95` suggests this is not a
  material gap for cross-module edges, but I did not prove exhaustiveness.
- `frg dsm`'s `--prefix 'crate::accord'` filter is **not strict**: `crate::controller::cluster` appears
  as an element (`cluster` is a caller of `accord`, not a member). Read A's element list accordingly.
- Method C's `module_outline` reports `coordinator.rs` as 5411 lines while `wc -l` says 7906 — the
  outline excludes test modules. All `module_outline` line numbers above are **pre-test-module** line
  numbers; the `sed`-verified line numbers in §3/§5 are raw file lines.
- **No claim in this file is verified by executing the code.** This is static analysis. G1/G2 are
  confirmed by reading source, consistent with the ground truth's own standard that a claim without a
  probe stays a hypothesis (U2, U6 remain open).
