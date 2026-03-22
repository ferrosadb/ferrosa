# DSM Dependency Analysis: Accord Transaction Integration

**Date:** 2026-03-21
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

## 2. Proposed Changes for Accord

### 2.1 Changes Per Crate

| Crate | Additions | New Deps |
|-------|-----------|----------|
| **ferrosa-common** | `HLC`, `Timestamp`, `TxnId`, `AcceptedBallot`, `PromisedBallot`, `Deps` (dependency set type) | _(none)_ |
| **ferrosa-net** | 11 new `Message` variants (`PreAccept`, `PreAcceptOk`, `Accept`, `AcceptOk`, `Commit`, `Apply`, `Recover`, `RecoverOk`, `Read`, `ReadOk`, `TxnResult`), heartbeat HLC piggyback | + **ferrosa-common** |
| **ferrosa-storage** | `ConflictIndex`, `MemIndex`, 4 commit log entry types (`CL_PREACCEPT`, `CL_ACCEPT`, `CL_COMMIT`, `CL_APPLY`) | _(no new deps)_ |
| **ferrosa-cluster** | New `accord/` submodule: `AccordStateMachine`, `AccordCoordinator`, `ReorderBuffer`, `ElectorateConfig`, `RecoveryCoordinator` | _(no new deps -- already has common, net, storage, schema)_ |
| **ferrosa-cql** | `BEGIN TRANSACTION`/`COMMIT`/`ROLLBACK` parsing, 2i query planner for transactional reads | _(no new deps)_ |
| **ferrosa-index** | Eager index build trigger on flush for transactional consistency | _(no new deps)_ |

### 2.2 New Dependencies Introduced

Only one new edge:

| From | To | Reason |
|------|----|--------|
| **ferrosa-net** | **ferrosa-common** | Message variants need `TxnId`, `Timestamp`, `AcceptedBallot`, `PromisedBallot`, `Deps` types |

### 2.3 Proposed DSM Matrix

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
                       -> ferrosa-storage (ConflictIndex, commit log)
                       -> ferrosa-cluster (AccordStateMachine)
                       -> ferrosa-cql (BEGIN/COMMIT parsing)
```

This is a 4-crate chain, depth 3 (common -> storage -> cluster -> cql). No fan-out explosion -- each step adds one downstream consumer.

---

## 7. Module Boundary Analysis

### 7.1 God-Crate Risk Assessment

| Crate | Fan-In + Fan-Out | Risk Level | Notes |
|-------|:----------------:|:----------:|-------|
| common   | 11 + 0 = 11 | **Low** | High fan-in is expected for a types crate. Zero fan-out keeps it stable. Adding Accord types is within scope. |
| cql      | 2 + 7 = 9   | **Medium** | Already the highest fan-out crate. Accord adds transaction parsing but no new deps. Monitor for scope creep. |
| cluster  | 2 + 5 = 7   | **Medium** | Accord's `accord/` submodule adds significant internal complexity. The submodule boundary helps, but this crate is becoming the largest single component. |
| storage  | 5 + 4 = 9   | **Medium** | ConflictIndex and 4 new commit log entry types add complexity. The storage crate already handles memtable, commit log, flush, compaction, and S3 write-behind. |
| bin      | 0 + 8 = 8   | **Low** | Composition root. Expected to have high fan-out. |

### 7.2 Boundary Cleanliness

**ferrosa-common**: Boundary remains clean. Accord types (`HLC`, `TxnId`, `AcceptedBallot`, `PromisedBallot`, `Deps`) are leaf types with no dependencies on other workspace crates. They belong in `common` alongside existing `Token`, `PartitionKey`, `DecoratedKey`, `CellValue`.

**ferrosa-net**: Adding `ferrosa-common` as a dependency is the right call. The Message enum already needs to reference `Token`, `PartitionKey`, etc. for existing internode messages. Accord message variants are a natural extension. The alternative (duplicating types in net) would be worse.

**ferrosa-storage**: The ConflictIndex and commit log extensions operate on the same memtable/commit-log abstractions the crate already owns. No boundary violation.

**ferrosa-cluster**: The `accord/` submodule contains the most new code. The boundary question is whether the Accord state machine, coordinator, and recovery logic belong in `cluster` or in a separate `ferrosa-accord` crate. Analysis below.

**ferrosa-cql**: Transaction syntax parsing (`BEGIN`/`COMMIT`/`ROLLBACK`) is squarely within CQL's scope. The 2i query planner changes are modifications to existing code paths, not a new subsystem.

**ferrosa-index**: Eager index build on flush is a small hook, not a structural change.

---

## 8. Should `ferrosa-accord` Be a Separate Crate?

### 8.1 Arguments For Splitting

| Factor | Weight | Rationale |
|--------|--------|-----------|
| Cohesion | Medium | Accord is a self-contained consensus protocol with its own state machine, recovery, and reorder buffer. |
| Testability | Medium | An isolated crate can be tested against mock storage/net without pulling in the full cluster stack. |
| Compile time | Low | ~5 new modules is modest. Splitting saves incremental compile time only if Accord changes frequently while cluster does not. |
| Optional builds | Low | If Accord is feature-gated, a separate crate makes the gate cleaner. |

### 8.2 Arguments Against Splitting

| Factor | Weight | Rationale |
|--------|--------|-----------|
| Integration depth | **High** | AccordStateMachine needs direct access to cluster's Raft metadata, electorate tracking, and schema routing. Splitting creates a wide trait interface or circular dependency risk. |
| Deployment coupling | **High** | Accord is not optional -- every node runs it. A separate crate adds a dep edge without enabling any independent deployment. |
| Small surface | Medium | 5 modules (state machine, coordinator, reorder buffer, electorate config, recovery) is well within the scope of a single crate's submodule. |
| Dependency graph | Medium | A `ferrosa-accord` crate would need deps on `common`, `net`, `storage`, and `schema` -- the same deps `cluster` already has. It would also need to be consumed by `cluster`, adding an edge without reducing fan-out. |

### 8.3 Recommendation

**Do not create `ferrosa-accord` as a separate crate.** Instead:

1. **Use `ferrosa-cluster/src/accord/` as a module boundary.** The `mod accord` with `pub(crate)` visibility on internals provides encapsulation without the overhead of a crate boundary.

2. **Extract `ferrosa-accord-types` only if types need to be shared with more than 2 crates.** Currently, Accord types in `ferrosa-common` cover cross-crate needs (`TxnId`, `Timestamp`, `Ballot` types). The Accord-internal types (`CommandStore`, `WaitingQueue`) stay private in the cluster submodule.

3. **Feature-gate Accord in `ferrosa-common`.** Place Accord types behind `feature = "accord"` so non-transactional builds (worker, graph) do not pay for the type definitions:

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

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Total crates | 13 | 13 | 0 |
| Total internal edges | 38 | **39** | **+1** |
| Maximum topological depth | 6 | 6 | 0 |
| Dependency cycles | 0 | **0** | 0 |
| Crates modified by Accord | -- | **6** | -- |
| New crates required | -- | **0** | -- |

### 9.2 Risk Assessment

| Risk | Level | Mitigation |
|------|-------|------------|
| `ferrosa-common` becomes bloated | Low | Feature-gate Accord types. Accord adds ~6 types to a crate that already defines core DB types. |
| `ferrosa-cluster` becomes a God crate | **Medium** | Enforce `mod accord` boundary. Keep AccordStateMachine behind a clean trait interface. Limit `pub` exports from the submodule to the coordinator entry point. |
| `ferrosa-storage` commit log grows complex | **Medium** | The 4 new entry types should share the existing `LogEntry` serialization framework. Add a `TxnEntryKind` enum rather than 4 independent types. |
| `ferrosa-net` coupling to common | Low | This is a natural dependency. Every other transport crate in the workspace already depends on common. Net was the outlier. |
| Recompilation cascade from common changes | Low | Accord types are additive. Feature-gating eliminates cascade for unaffected crates. |

### 9.3 Recommendations

1. **Add `ferrosa-common` as a dependency of `ferrosa-net`.** This is the only new edge and it is well-justified. Net was the last crate without access to shared types.

2. **Feature-gate Accord types in `ferrosa-common`.** Use `feature = "accord"` to isolate Accord type definitions from crates that do not need them (`ferrosa-graph`, `ferrosa-worker`, `ferrosa-udf`).

3. **Do not create a `ferrosa-accord` crate.** Use `ferrosa-cluster/src/accord/` as a module boundary. The integration depth with cluster internals (Raft metadata, schema routing, electorate) makes a crate boundary more costly than beneficial.

4. **Keep `ferrosa-storage` commit log extensions behind a shared `TxnEntryKind` enum** rather than adding 4 separate entry type structs. This keeps the commit log serialization path unified.

5. **Monitor `ferrosa-cluster` fan-out.** At 5 dependencies, it is approaching the complexity threshold. If Accord causes `cluster` to grow beyond ~2000 lines of Accord-specific code, reconsider extraction.

6. **Define the Accord message types in `ferrosa-common::accord`**, not in `ferrosa-net`. Net should reference them by importing from common. This keeps the types in the stable layer and prevents net from becoming a type-definition crate.
