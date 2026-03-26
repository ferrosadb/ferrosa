# DSM Dependency Analysis: Accord Transaction Integration

**Date:** 2026-03-21
**Updated:** 2026-03-23
**Scope:** Impact of Accord distributed transactions on the Ferrosa Rust workspace dependency graph
**Method:** Design Structure Matrix (DSM) with fan-in/fan-out metrics and propagation cost analysis

---

## 1. Current Dependency Graph

### 1.1 Verified Dependencies (from Cargo.toml)

The workspace contains 13 crates. Internal dependencies verified from each crate's `Cargo.toml`:

| Crate | Depends On (workspace crates) |
|-------|-------------------------------|
| `ferrosa-common` | _(none)_ |
| `ferrosa-index` | common |
| `ferrosa-udf` | common |
| `ferrosa-sstable` | common |
| `ferrosa-net` | _(none)_ |
| `ferrosa-schema` | common, index, sstable |
| `ferrosa-storage` | common, sstable, index, schema |
| `ferrosa-graph` | common, schema, sstable, storage |
| `ferrosa-cluster` | common, net, storage, schema, sstable |
| `ferrosa-cql` | common, schema, storage, index, sstable, cluster, udf |
| `ferrosa-worker` | common, index, sstable |
| `ferrosa-ctl` | cql |
| `ferrosa` (bin) | common, cql, graph, cluster, net, schema, storage, udf |

### 1.2 Current DSM Matrix

Rows depend on columns. `X` = direct dependency.

Crates ordered by topological depth (leaves first):

|                | common | net | index | udf | sstable | schema | storage | graph | cluster | cql | worker | ctl | bin |
|----------------|:------:|:---:|:-----:|:---:|:-------:|:------:|:-------:|:-----:|:-------:|:---:|:------:|:---:|:---:|
| **common**     |        |     |       |     |         |        |         |       |         |     |        |     |     |
| **net**        |        |     |       |     |         |        |         |       |         |     |        |     |     |
| **index**      | X      |     |       |     |         |        |         |       |         |     |        |     |     |
| **udf**        | X      |     |       |     |         |        |         |       |         |     |        |     |     |
| **sstable**    | X      |     |       |     |         |        |         |       |         |     |        |     |     |
| **schema**     | X      |     | X     |     | X       |        |         |       |         |     |        |     |     |
| **storage**    | X      |     | X     |     | X       | X      |         |       |         |     |        |     |     |
| **graph**      | X      |     |       |     | X       | X      | X       |       |         |     |        |     |     |
| **cluster**    | X      | X   |       |     | X       | X      | X       |       |         |     |        |     |     |
| **cql**        | X      |     | X     | X   | X       | X      | X       |       | X       |     |        |     |     |
| **worker**     | X      |     | X     |     | X       |        |         |       |         |     |        |     |     |
| **ctl**        |        |     |       |     |         |        |         |       |         | X   |        |     |     |
| **bin**        | X      | X   |       | X   |         | X      | X       | X     | X       | X   |        |     |     |

### 1.3 Current Fan-In / Fan-Out

| Crate | Fan-In (depended on by) | Fan-Out (depends on) | Instability |
|-------|:-----------------------:|:--------------------:|:------------------------:|
| common   | 10 | 0  | 0.00 |
| net      | 2  | 0  | 0.00 |
| index    | 4  | 1  | 0.20 |
| udf      | 2  | 1  | 0.33 |
| sstable  | 7  | 1  | 0.13 |
| schema   | 5  | 3  | 0.38 |
| storage  | 5  | 4  | 0.44 |
| graph    | 1  | 4  | 0.80 |
| cluster  | 2  | 5  | 0.71 |
| cql      | 2  | 7  | 0.78 |
| worker   | 0  | 3  | 1.00 |
| ctl      | 0  | 1  | 1.00 |
| bin      | 0  | 8  | 1.00 |

**Observations:**

- `ferrosa-common` is a pure stable abstraction (instability = 0.00, fan-in = 10).
- `ferrosa-net` is fully independent -- zero workspace dependencies, low fan-in (only cluster + bin).
- `ferrosa-sstable` has high fan-in (7) and minimal fan-out -- good stable component.
- Leaf crates (worker, ctl, bin) have instability 1.00 as expected for executables.

---

## 2. Implemented Changes for Accord (A1-A7 Complete)

### 2.1 Changes Per Crate

All Accord milestones (A1-A7) are implemented. 2,808 tests pass. The following
summarizes the actual module inventory as of 2026-03-23.

| Crate | Additions | New Deps |
|-------|-----------|----------|
| **ferrosa-common** | `HLC`, `Timestamp`, `TxnId`, `AcceptedBallot`, `PromisedBallot`, `Deps` (dependency set type) | _(none)_ |
| **ferrosa-net** | 11 new `Message` variants (`PreAccept`, `PreAcceptOk`, `Accept`, `AcceptOk`, `Commit`, `Apply`, `Recover`, `RecoverOk`, `Read`, `ReadOk`, `TxnResult`), heartbeat HLC piggyback | + **ferrosa-common** |
| **ferrosa-storage** | `accord/` submodule (10 modules): `conflict_index.rs`, `protocol_log.rs`, `sync_writer.rs`, `write_gate.rs`, `entries.rs`, `crash_recovery.rs`, `sidecar.rs`, `read_2i.rs`, `oversized_entry.rs`, `mod.rs` | _(no new deps)_ |
| **ferrosa-cluster** | `accord/` submodule (26 modules): `state_machine.rs`, `coordinator.rs`, `cross_shard.rs`, `recovery.rs`, `recovery_scenarios.rs`, `dep_wait.rs`, `ddl_drain.rs`, `leaseholder.rs`, `linearizable_read.rs`, `reorder_buffer.rs`, `test_cluster.rs`, `clock_validation.rs`, `durability.rs`, `electorate.rs`, `epoch.rs`, `epoch_drain.rs`, `two_phase_ddl.rs`, `metrics.rs`, `perf.rs`, `perf_regression.rs`, `proptests.rs`, `jepsen_bank.rs`, `jepsen_nemesis.rs`, `chaos_minority_kill.rs`, `uda_integration.rs`, `mod.rs` | _(no new deps -- already has common, net, storage, schema)_ |
| **ferrosa-cql** | `accord_router.rs`, `session.rs`, `transaction_keys.rs`, `transaction_limits.rs`, `paging.rs`; `BEGIN TRANSACTION`/`COMMIT`/`ROLLBACK` parsing, 2i query planner for transactional reads | _(no new deps)_ |
| **ferrosa-index** | Eager index build trigger on flush for transactional consistency | _(no new deps)_ |
| **ferrosa-jepsen** _(planned)_ | Standalone crate for Jepsen-style linearizability testing; will depend on `ferrosa-cluster`, `ferrosa-cql` | _(new crate, not yet created)_ |

### 2.2 New Dependencies Introduced

Only one new edge (implemented):

| From | To | Reason |
|------|----|--------|
| **ferrosa-net** | **ferrosa-common** | Message variants need `TxnId`, `Timestamp`, `AcceptedBallot`, `PromisedBallot`, `Deps` types |

### 2.3 Current DSM Matrix

Changed cell marked with `*`:

|                | common | net | index | udf | sstable | schema | storage | graph | cluster | cql | worker | ctl | bin |
|----------------|:------:|:---:|:-----:|:---:|:-------:|:------:|:-------:|:-----:|:-------:|:---:|:------:|:---:|:---:|
| **common**     |        |     |       |     |         |        |         |       |         |     |        |     |     |
| **net**        | **X*** |     |       |     |         |        |         |       |         |     |        |     |     |
| **index**      | X      |     |       |     |         |        |         |       |         |     |        |     |     |
| **udf**        | X      |     |       |     |         |        |         |       |         |     |        |     |     |
| **sstable**    | X      |     |       |     |         |        |         |       |         |     |        |     |     |
| **schema**     | X      |     | X     |     | X       |        |         |       |         |     |        |     |     |
| **storage**    | X      |     | X     |     | X       | X      |         |       |         |     |        |     |     |
| **graph**      | X      |     |       |     | X       | X      | X       |       |         |     |        |     |     |
| **cluster**    | X      | X   |       |     | X       | X      | X       |       |         |     |        |     |     |
| **cql**        | X      |     | X     | X   | X       | X      | X       |       | X       |     |        |     |     |
| **worker**     | X      |     | X     |     | X       |        |         |       |         |     |        |     |     |
| **ctl**        |        |     |       |     |         |        |         |       |         | X   |        |     |     |
| **bin**        | X      | X   |       | X   |         | X      | X       | X     | X       | X   |        |     |     |

---

## 3. Dependency Diagram

```mermaid
graph TD
    common["ferrosa-common"]
    net["ferrosa-net"]
    index["ferrosa-index"]
    udf["ferrosa-udf"]
    sstable["ferrosa-sstable"]
    schema["ferrosa-schema"]
    storage["ferrosa-storage"]
    graph["ferrosa-graph"]
    cluster["ferrosa-cluster"]
    cql["ferrosa-cql"]
    worker["ferrosa-worker"]
    ctl["ferrosa-ctl"]
    bin["ferrosa (bin)"]

    %% Existing edges (solid)
    index --> common
    udf --> common
    sstable --> common
    schema --> common
    schema --> index
    schema --> sstable
    storage --> common
    storage --> sstable
    storage --> index
    storage --> schema
    graph --> common
    graph --> schema
    graph --> sstable
    graph --> storage
    cluster --> common
    cluster --> net
    cluster --> storage
    cluster --> schema
    cluster --> sstable
    cql --> common
    cql --> schema
    cql --> storage
    cql --> index
    cql --> sstable
    cql --> cluster
    cql --> udf
    worker --> common
    worker --> index
    worker --> sstable
    ctl --> cql
    bin --> common
    bin --> net
    bin --> schema
    bin --> storage
    bin --> graph
    bin --> cluster
    bin --> cql
    bin --> udf

    %% NEW edge for Accord (dashed/red)
    net -. "NEW: Accord types" .-> common

    %% Styling
    style net fill:#fff3cd,stroke:#ffc107
    style common fill:#d4edda,stroke:#28a745
    style storage fill:#fff3cd,stroke:#ffc107
    style cluster fill:#fff3cd,stroke:#ffc107
    style cql fill:#fff3cd,stroke:#ffc107
    style index fill:#fff3cd,stroke:#ffc107

    linkStyle 29 stroke:#dc3545,stroke-width:3px,stroke-dasharray:5
```

**Legend:** Yellow-filled crates are modified by Accord. Green-filled = `ferrosa-common` (new types added). Dashed red edge = new dependency introduced.

---

## 4. Cycle Analysis

### 4.1 Cycle Detection

Topological sort of the proposed graph (Kahn's algorithm):

1. `ferrosa-common` (0 in-edges)
2. `ferrosa-net` (1 in-edge: common) -- **was layer 1, now layer 2**
3. `ferrosa-index` (1 in-edge: common)
4. `ferrosa-udf` (1 in-edge: common)
5. `ferrosa-sstable` (1 in-edge: common)
6. `ferrosa-schema` (3 in-edges: common, index, sstable)
7. `ferrosa-storage` (4 in-edges: common, sstable, index, schema)
8. `ferrosa-worker` (3 in-edges: common, index, sstable)
9. `ferrosa-graph` (4 in-edges: common, schema, sstable, storage)
10. `ferrosa-cluster` (5 in-edges: common, net, storage, schema, sstable)
11. `ferrosa-cql` (7 in-edges: common, schema, storage, index, sstable, cluster, udf)
12. `ferrosa-ctl` (1 in-edge: cql)
13. `ferrosa` (bin) (8 in-edges)

**Result: No cycles.** The graph remains a DAG. The new `net -> common` edge does not introduce any cycle because `common` has no outgoing workspace edges. `net` was previously at layer 0 (no deps); it moves to layer 1 alongside `index`, `udf`, and `sstable`.

### 4.2 Potential Future Cycle Risks

One risk to monitor: if `ferrosa-cluster` ever needs to export types that `ferrosa-net` uses for message serialization, a `net -> cluster` edge would create a cycle (`net -> cluster -> net`). The current design avoids this by placing all shared Accord types in `ferrosa-common`. This is correct.

---

## 5. Fan-In / Fan-Out: Before and After

| Crate | Fan-In (before) | Fan-In (after) | Fan-Out (before) | Fan-Out (after) | Instability (before) | Instability (after) |
|-------|:---------------:|:--------------:|:----------------:|:---------------:|:--------------------:|:-------------------:|
| common   | 10 | **11** (+1) | 0 | 0 | 0.00 | 0.00 |
| net      | 2  | 2           | 0 | **1** (+1) | 0.00 | **0.33** |
| index    | 4  | 4           | 1 | 1 | 0.20 | 0.20 |
| udf      | 2  | 2           | 1 | 1 | 0.33 | 0.33 |
| sstable  | 7  | 7           | 1 | 1 | 0.13 | 0.13 |
| schema   | 5  | 5           | 3 | 3 | 0.38 | 0.38 |
| storage  | 5  | 5           | 4 | 4 | 0.44 | 0.44 |
| graph    | 1  | 1           | 4 | 4 | 0.80 | 0.80 |
| cluster  | 2  | 2           | 5 | 5 | 0.71 | 0.71 |
| cql      | 2  | 2           | 7 | 7 | 0.78 | 0.78 |
| worker   | 0  | 0           | 3 | 3 | 1.00 | 1.00 |
| ctl      | 0  | 0           | 1 | 1 | 1.00 | 1.00 |
| bin      | 0  | 0           | 8 | 8 | 1.00 | 1.00 |

**Key changes:**

- `ferrosa-common` fan-in increases from 10 to 11 (net now depends on it). Already the hub; this is expected.
- `ferrosa-net` instability increases from 0.00 to 0.33. It moves from "fully stable" to "mostly stable," which is appropriate -- it should depend on shared types but remain a low-level transport crate.
- All other metrics unchanged. The Accord integration is structurally conservative.

---

## 6. Propagation Cost Analysis

Propagation cost measures what percentage of crates are transitively affected when a crate changes. Computed on the **proposed** graph.

### 6.1 Direct and Transitive Dependents

| Crate Changed | Direct Dependents | Transitive Dependents (total) | % of Workspace (N=13) |
|---------------|:-----------------:|:-----------------------------:|:---------------------:|
| **common**    | 11 | 12 | **92%** |
| **net**       | 2  | 2  | **15%** |
| **index**     | 4  | 7  | **54%** |
| **sstable**   | 7  | 10 | **77%** |
| **udf**       | 2  | 3  | **23%** |
| **schema**    | 5  | 7  | **54%** |
| **storage**   | 5  | 6  | **46%** |
| **cluster**   | 2  | 3  | **23%** |
| **cql**       | 2  | 2  | **15%** |
| **graph**     | 1  | 1  | **8%** |
| **worker**    | 0  | 0  | **0%** |
| **ctl**       | 0  | 0  | **0%** |
| **bin**       | 0  | 0  | **0%** |

### 6.2 Accord-Specific Propagation

The Accord types being added to `ferrosa-common` will trigger recompilation of all 12 downstream crates. However, this is mitigated by:

1. **Types are additive.** New types (`HLC`, `TxnId`, etc.) do not change existing type signatures.
2. **Incremental compilation.** Rust's incremental compiler will only recompile translation units that import the new types.
3. **Feature gating option.** Accord types could be behind a `feature = "accord"` flag in `ferrosa-common`, so crates that don't use them (e.g., `ferrosa-graph`, `ferrosa-worker`) would not see any change.

### 6.3 Change Propagation for Accord Protocol Messages

A change to Accord message types flows through:

```
ferrosa-common (types) -> ferrosa-net (Message variants)
                       -> ferrosa-storage (10 accord modules: conflict_index, protocol_log, ...)
                       -> ferrosa-cluster (26 accord modules: state_machine, coordinator, ...)
                       -> ferrosa-cql (5 accord modules: accord_router, session, ...)
                       -> ferrosa-jepsen (planned: jepsen_bank, jepsen_nemesis, chaos_minority_kill)
```

This is a 4-crate chain (5 with ferrosa-jepsen), depth 3 (common -> storage -> cluster -> cql). No fan-out explosion -- each step adds one downstream consumer. The total module count across the chain is 41, but changes propagate at the crate level, not the module level.

---

## 7. Module Boundary Analysis

### 7.1 God-Crate Risk Assessment

| Crate | Fan-In + Fan-Out | Risk Level | Notes |
|-------|:----------------:|:----------:|-------|
| common   | 11 + 0 = 11 | **Low** | High fan-in is expected for a types crate. Zero fan-out keeps it stable. Accord types are in scope. |
| cql      | 2 + 7 = 9   | **Medium** | Already the highest fan-out crate. Accord adds 5 modules (`accord_router.rs`, `session.rs`, `transaction_keys.rs`, `transaction_limits.rs`, `paging.rs`) but no new deps. Monitor for scope creep. |
| cluster  | 2 + 5 = 7   | **High** | The `accord/` submodule now contains **26 modules** including protocol core (state machine, coordinator, recovery), testing infrastructure (test_cluster, jepsen_bank, jepsen_nemesis, chaos_minority_kill, proptests), observability (metrics, perf, perf_regression), and DDL integration (two_phase_ddl, ddl_drain). This is the largest single component in the workspace. The `mod.rs` boundary provides encapsulation, but the module count warrants monitoring for extraction candidates. |
| storage  | 5 + 4 = 9   | **Medium** | The `accord/` submodule now contains **10 modules**: conflict_index, protocol_log, sync_writer, write_gate, entries, crash_recovery, sidecar, read_2i, oversized_entry. Well-scoped storage-layer concerns. |
| bin      | 0 + 8 = 8   | **Low** | Composition root. Expected to have high fan-out. |

### 7.2 Boundary Cleanliness

**ferrosa-common**: Boundary remains clean. Accord types (`HLC`, `TxnId`, `AcceptedBallot`, `PromisedBallot`, `Deps`) are leaf types with no dependencies on other workspace crates. They belong in `common` alongside existing `Token`, `PartitionKey`, `DecoratedKey`, `CellValue`.

**ferrosa-net**: Adding `ferrosa-common` as a dependency is the right call. The Message enum already needs to reference `Token`, `PartitionKey`, etc. for existing internode messages. Accord message variants are a natural extension. The alternative (duplicating types in net) would be worse.

**ferrosa-storage**: The `accord/` submodule (10 modules) operates on the same memtable/commit-log abstractions the crate already owns. Includes crash recovery, conflict indexing, protocol logging, and a write gate for fsync-before-ack. No boundary violation.

**ferrosa-cluster**: The `accord/` submodule now has 26 modules -- the largest single subsystem in the workspace. This includes protocol core (state_machine, coordinator, cross_shard, recovery, recovery_scenarios), consensus infrastructure (dep_wait, electorate, epoch, epoch_drain, durability, leaseholder, linearizable_read, reorder_buffer, clock_validation, two_phase_ddl, ddl_drain), testing (test_cluster, proptests, jepsen_bank, jepsen_nemesis, chaos_minority_kill, uda_integration), and observability (metrics, perf, perf_regression). The `mod.rs` boundary with `pub(crate)` internals provides encapsulation. The planned `ferrosa-jepsen` crate will extract testing modules (jepsen_bank, jepsen_nemesis, chaos_minority_kill), reducing the count. See section 8 analysis for extraction decision.

**ferrosa-cql**: Five Accord-related modules: `accord_router.rs` (transaction routing), `session.rs` (transaction session lifecycle), `transaction_keys.rs` (key extraction), `transaction_limits.rs` (configurable limits enforcement), `paging.rs` (transactional result paging). All squarely within CQL's scope.

**ferrosa-index**: Eager index build on flush is a small hook, not a structural change.

---

## 8. Should `ferrosa-accord` Be a Separate Crate?

### 8.1 Arguments For Splitting

| Factor | Weight | Rationale |
|--------|--------|-----------|
| Cohesion | Medium | Accord is a self-contained consensus protocol with its own state machine, recovery, and reorder buffer. |
| Testability | Medium | An isolated crate can be tested against mock storage/net without pulling in the full cluster stack. |
| Compile time | **High** | 26 modules is substantial. Splitting saves incremental compile time when Accord changes frequently while cluster does not. |
| Test isolation | **High** | The testing modules (jepsen_bank, jepsen_nemesis, chaos_minority_kill, proptests, test_cluster) represent ~6 modules that should not be compiled in production builds. |
| Optional builds | Low | If Accord is feature-gated, a separate crate makes the gate cleaner. |

### 8.2 Arguments Against Splitting

| Factor | Weight | Rationale |
|--------|--------|-----------|
| Integration depth | **High** | AccordStateMachine needs direct access to cluster's Raft metadata, electorate tracking, and schema routing. Splitting creates a wide trait interface or circular dependency risk. |
| Deployment coupling | **High** | Accord is not optional -- every node runs it. A separate crate adds a dep edge without enabling any independent deployment. |
| Proven boundary | **High** | The 26-module `accord/` submodule has been implemented and tested with 2,808 passing tests. The `mod.rs` boundary is validated by practice. |
| Dependency graph | Medium | A `ferrosa-accord` crate would need deps on `common`, `net`, `storage`, and `schema` -- the same deps `cluster` already has. It would also need to be consumed by `cluster`, adding an edge without reducing fan-out. |

### 8.3 Recommendation (Updated 2026-03-23)

**Do not create `ferrosa-accord` as a separate crate.** The integration depth argument remains decisive. However, with 26 modules the submodule is now the largest component in the workspace. Two actions:

1. **Use `ferrosa-cluster/src/accord/` as a module boundary.** The `mod accord` with `pub(crate)` visibility on internals provides encapsulation without the overhead of a crate boundary. This has proven effective through A1-A7 implementation.

2. **Create `ferrosa-jepsen` as a separate crate (planned).** Extract the Jepsen-style testing modules (`jepsen_bank.rs`, `jepsen_nemesis.rs`, `chaos_minority_kill.rs`) into a dedicated test crate. This reduces `ferrosa-cluster/src/accord/` from 26 to 23 modules and ensures chaos-testing tools are never compiled into production binaries. The `ferrosa-jepsen` crate depends on `ferrosa-cluster` and `ferrosa-cql` -- no new dependency edges from cluster to jepsen.

3. **Extract `ferrosa-accord-types` only if types need to be shared with more than 2 crates.** Currently, Accord types in `ferrosa-common` cover cross-crate needs (`TxnId`, `Timestamp`, `Ballot` types). The Accord-internal types (`CommandStore`, `WaitingQueue`) stay private in the cluster submodule.

4. **Feature-gate Accord in `ferrosa-common`.** Place Accord types behind `feature = "accord"` so non-transactional builds (worker, graph) do not pay for the type definitions:

   ```toml
   # ferrosa-common/Cargo.toml
   [features]
   accord = []
   ```

   ```rust
   // ferrosa-common/src/lib.rs
   #[cfg(feature = "accord")]
   pub mod accord;
   ```

   Crates that need Accord types (`net`, `storage`, `cluster`, `cql`) enable the feature. Others do not.

---

## 9. Summary of Findings

### 9.1 Structural Impact

| Metric | Before | After Accord (A1-A7) | Delta |
|--------|--------|----------------------|-------|
| Total crates | 13 | 13 (+1 planned: ferrosa-jepsen) | 0 (+1 planned) |
| Total internal edges | 38 | **39** | **+1** |
| Maximum topological depth | 6 | 6 | 0 |
| Dependency cycles | 0 | **0** | 0 |
| Crates modified by Accord | -- | **6** | -- |
| New modules added | -- | **41** (26 cluster + 10 storage + 5 cql) | -- |
| Tests passing | -- | **2,808** | -- |

### 9.2 Risk Assessment

| Risk | Level | Mitigation |
|------|-------|------------|
| `ferrosa-common` becomes bloated | Low | Feature-gate Accord types. Accord adds ~6 types to a crate that already defines core DB types. |
| `ferrosa-cluster` becomes a God crate | **High** | The `accord/` submodule now has 26 modules. Enforce `mod accord` boundary with `pub(crate)`. Extract Jepsen testing modules into `ferrosa-jepsen` crate (planned). Monitor for further extraction candidates if module count grows beyond 30. |
| `ferrosa-storage` accord submodule | **Medium** | 10 modules is manageable. The `mod.rs` boundary keeps concerns well-scoped (conflict indexing, protocol logging, crash recovery, write gating). |
| `ferrosa-cql` transaction scope | **Low** | 5 new modules are well-defined: routing, sessions, key extraction, limits, paging. No structural risk. |
| `ferrosa-net` coupling to common | Low | This is a natural dependency. Every other transport crate in the workspace already depends on common. Net was the outlier. |
| Recompilation cascade from common changes | Low | Accord types are additive. Feature-gating eliminates cascade for unaffected crates. |
| `ferrosa-jepsen` chaos tools in production | **Medium** | Planned crate must be `#[cfg(test)]` or a dev-dependency only. Must never be compiled into release binaries. See threat model AT-jepsen. |

### 9.3 Recommendations

1. **Add `ferrosa-common` as a dependency of `ferrosa-net`.** This is the only new edge and it is well-justified. Net was the last crate without access to shared types. (Implemented.)

2. **Feature-gate Accord types in `ferrosa-common`.** Use `feature = "accord"` to isolate Accord type definitions from crates that do not need them (`ferrosa-graph`, `ferrosa-worker`, `ferrosa-udf`).

3. **Do not create a `ferrosa-accord` crate.** Use `ferrosa-cluster/src/accord/` as a module boundary. The integration depth with cluster internals (Raft metadata, schema routing, electorate) makes a crate boundary more costly than beneficial. Validated by A1-A7 implementation.

4. **Create `ferrosa-jepsen` as a separate crate.** Extract Jepsen testing modules (`jepsen_bank`, `jepsen_nemesis`, `chaos_minority_kill`) from `ferrosa-cluster/src/accord/`. This reduces the cluster accord module count and prevents chaos tools from being compiled into production. (Planned.)

5. **Keep `ferrosa-storage` commit log extensions behind a shared `TxnEntryKind` enum** rather than adding 4 separate entry type structs. This keeps the commit log serialization path unified.

6. **Monitor `ferrosa-cluster` accord module count.** At 26 modules, this is the largest subsystem. If it grows beyond 30, reconsider partial extraction (e.g., observability modules `metrics`, `perf`, `perf_regression` into a shared observability crate).

7. **Define the Accord message types in `ferrosa-common::accord`**, not in `ferrosa-net`. Net should reference them by importing from common. This keeps the types in the stable layer and prevents net from becoming a type-definition crate.
