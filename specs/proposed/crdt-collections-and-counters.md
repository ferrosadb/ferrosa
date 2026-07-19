# Design Proposal: CRDT-encoded collections and counters (Cassandra-faithful per-element cells)

- **Status**: Proposed
- **Task**: `t_83c4f093` (blocks `t_12191f45` read-visibility / `t_d7ffb5b7` Elle list-append)
- **Author**: platform
- **Related**: PR #278 (Accord quorum + coordinator-apply fixes); `examples/accord_probe.rs` (isolation)

---

## 1. Problem

An Accord transaction (`BEGIN; UPDATE t SET v = v + [x] WHERE k=?; COMMIT`) on a
`list`/`set`/`map`/`counter` column **silently wrote nothing**, then read back
empty. Root cause (now fail-loud, `fix(cql): fail loud on collection/counter
updates in a transaction`): `materialize_update` — the sync materialization path
used by transactions and logged batches — only handled `Assignment::Simple`, and
`continue`d past `Add`/`Sub`/`Element`. The plain path (`route_update`,
`ferrosa-cql/src/router.rs` ~L7168–7250) does a **read-modify-write** for those;
the transactional path never called it.

Isolation (`accord_probe`, live 3-node RF=3):

| write | reads back |
|---|---|
| Accord txn `SET s = 42` (scalar) | `42` ✓ |
| Accord txn `SET v = v + [7]` (list) | `None` ✗ |
| plain `SET v = v + [5]` (list) | `[5]` ✓ |

Only **Accord + collection** is broken. The narrow fix ("apply-time RMW") would
thread a read-modify-write through Accord apply. The **deeper** issue is
representational: ferrosa stores a `list<int>` as **one whole-list `CellValue`**
(`Row.cells: Vec<(u16 col_idx, CellValue)>`, no cell-path), so an append is
inherently a non-commutative RMW.

### Why CRDTs

Cassandra's data model *is* a set of CRDTs, and that is precisely why replicas
converge without coordination:

- **scalar cell** = **LWW-Register** (merge by `CellValue.timestamp`). ferrosa
  already uses this.
- **collection** = **add-wins / Observed-Remove Set**: each element is its own
  cell keyed by a unique *cell-path*; add = live cell, remove = tombstone.
- **counter** = **PN-Counter**: per-replica sub-counters summed on read.

ferrosa adopted the LWW-Register but **not** the collection CRDT (single-cell
collections + a deferred SSTable writer). This proposal finishes adopting the
Cassandra/CRDT collection model, which:

- removes the RMW entirely for `add`/`put`/`increment` (append = insert a fresh
  commutative cell — no read),
- composes for free with Accord (Accord's execution timestamp `t` supplies the
  ordering key), **and**
- makes concurrent collection updates converge on the AP (tunable-CL) path too.

Accord is **not** replaced — it stays the CP layer for strict serializability.
CRDTs only change how a cell/collection is *represented and merged*.

## 2. Decisions (locked)

| Question | Decision |
|---|---|
| Migration for running clusters | **Lazy online / dual-read** — read both formats, write new, convert on compaction; no operator action |
| On-disk encoding | **Cassandra-exact** complex cells (cell-paths byte-compatible with C*) |
| Scope | **list + set + map + counter** (PN-Counter) in this work item |
| Consistency paths | **Both** — per-element cells are the storage model for AP *and* Accord writes |

Explicit non-goals: sequence CRDTs (RGA/LSEQ/Yjs) for arbitrary-position
concurrent insert (Cassandra `list` only appends/prepends; positional ops are
RMW and handled separately — see §6); `frozen<...>` collections stay a single
opaque LWW value.

## 3. The CRDT model, per column type

All non-frozen collections become **multiple cells per column**, each identified
by a `CellPath` (Cassandra-exact):

| type | cell-path | add / put | remove | merge |
|---|---|---|---|---|
| `list<T>` | 16-byte **timeuuid** (`time`,`seq`) | live cell, path = timeuuid derived from write `t` | delete-by-value ⇒ tombstone every matching path (a read); `list[i]=` ⇒ positional (RMW, §6) | union of live cells, **ordered by cell-path (timeuuid)** = append order |
| `set<T>` | serialized **element bytes** | live cell, path = element | `v = v - {e}` ⇒ tombstone path=e (no read) | add-wins OR-Set over paths |
| `map<K,V>` | serialized **key bytes** | live cell(value), path = key | `DELETE v[k]` ⇒ tombstone path=k (no read) | per-key LWW-Register |
| `counter` | per-node sub-counter (node id) | add delta to *this node's* sub-count | (no delete in C*) | **PN-Counter**: value = Σ sub-counts |
| `frozen<...>` | none (single cell) | whole value | whole value | LWW-Register (unchanged) |
| scalar | none | whole value | tombstone | LWW-Register (unchanged) |

Key property: `append`, `set-add`, `set-remove`, `map-put`, `map-delete`, and
`counter-increment` are **commutative and read-free** → no RMW, safe under
Accord's at-least-once Apply and safe under AP concurrency.

## 4. Storage-model changes

1. **Cell model** (`ferrosa-common` / `ferrosa-sstable::types::Row`): a row's
   cells become `(u16 col_idx, Option<CellPath>, CellValue)` (or a dedicated
   complex-column structure). `CellPath` = collection element key bytes.
   Complex column = many entries sharing `col_idx`, distinct `CellPath`.
2. **Memtable** (`ferrosa-storage/src/memtable`): merge per `(col_idx,
   cell_path)` — add-wins for set/list, LWW for map value, PN sum for counter.
3. **Commit-log `Mutation`** (`ferrosa-cql` build path): a list append encodes as
   **one element cell** (path=timeuuid(t)), not a full-list blob. Round-trips
   through serialize/deserialize (the Accord wire path) with cell-paths intact.
4. **SSTable BTI writer** (`ferrosa-sstable/src/writer.rs`): implement
   **complex-column writing** (currently "deferred", writer.rs L30–31). The
   reader (`data.rs`, "Complex columns (collections, UDTs, frozen types)")
   already *parses* Cassandra complex cells — this closes the write side.
5. **Read assembly** (`ferrosa-cql/src/bridge.rs`): assemble the per-element
   cells for a column into `CqlValue::List/Set/Map`, ordered by cell-path.

## 5. Both-paths integration

- **AP write** (`route_update`): stop the RMW for `Add`/`Sub`; emit per-element
  ADD/tombstone cells directly. Concurrent adds from different coordinators
  commute; the memtable/SSTable merge converges (add-wins). Net: fewer reads,
  and *correct* convergence instead of LWW-clobbering the whole list.
- **Accord write** (`materialize_update`): emit the same per-element cells into
  the transaction mutation. Replace the current fail-loud arm. Accord's Apply
  writes the element cell; idempotent by `(txn_id, key, t)` (already) **and** by
  cell-path, so at-least-once re-apply is a no-op. Accord's `t` is the source of
  the list element's timeuuid → replicas agree on order without a read.
- **Removes**: `set - {e}` / `DELETE map[k]` → tombstone the path (read-free).
  `list` delete-by-value and positional `list[i]=`/`list[i]` are RMW (Cassandra
  reads too) — see §6.
- **Counters**: `v = v + 1` adds to this node's PN sub-counter; commutes; Accord
  provides a total order but the PN-Counter converges regardless.

## 6. Positional list ops (the residual RMW)

`list[i] = x`, `DELETE list[i]`, and delete-by-value require reading the current
list — this is true in Cassandra as well (it does an internal read). Options,
recommend **(a)** for the first release:

- **(a)** Keep positional/by-value list ops **fail-loud inside a transaction**
  (they already are, post-fix), while append/prepend and all set/map/counter ops
  become fully supported. AP-path positional ops keep the existing read-based
  behavior.
- **(b)** Later: support them in a transaction via an **apply-time read**
  (`await_conflicting_deps_applied` then read-at-`t`, the machinery from the
  linearizable-read path) — deferred, not needed for Elle list-append.

## 7. Lazy dual-read migration (running clusters)

Durable state = BTI SSTables (local cache + S3), described by
`manifest.format_version` (currently `1`).

- **Version bump**: `format_version = 2` = "collections as per-element complex
  cells". An SSTable/manifest at v1 holds single-blob collection cells; v2 holds
  per-element cells.
- **Read (dual)**: the decoder accepts both. A v1 single-blob `list`/`set`/`map`
  cell is materialized as a **frozen baseline at its cell timestamp**; v2
  per-element cells assemble normally. A partition holding **both** (a v1 blob +
  v2 per-element cells for the same column, mid-migration) merges as: baseline
  blob at `ts_blob`, then per-element live/tombstone cells applied by their
  timestamps. Merge is deterministic (timestamp-ordered), so any read replica
  converges.
- **Write**: always v2 (per-element). No new v1 collection cells are ever
  produced after upgrade.
- **Compaction**: rewriting a v1 SSTable emits v2 cells (the blob is expanded to
  per-element cells at `ts_blob`, or retained as a baseline cell — see §8). Once
  every SSTable for a table is v2, the table is fully migrated; no flag day.
- **Operator action**: none. Normal compaction migrates opportunistically;
  `ferrosa-ctl` MAY expose a `force-compact`/`upgrade-collections` convenience to
  hasten it, but it is not required.
- **S3**: identical — S3 stores the same SSTables + manifest; dual-read applies to
  S3-fetched SSTables. No S3 re-write beyond normal compaction upload.
- **Retirement**: the v1 collection reader is retained until a future **major**
  release declares v1 collections unsupported (with a `ferrosa-ctl` check that a
  table has no residual v1 collection SSTables). Tracked as a follow-up.

## 8. Merge subtlety to nail down (the one correctness risk)

The mixed-partition merge (§7) must be specified exactly, because it is the only
place old and new formats coexist:

- A v1 `list` blob `[a,b]@ts0` + v2 appends `c@t1`, `d@t2` (t1,t2 > ts0) ⇒ `[a,b,c,d]`.
  Model the blob as a synthetic per-element run at `ts0` (paths ordered before any
  real timeuuid ≥ ts0), then apply v2 cells by timeuuid.
- A v1 `set` blob `{a,b}@ts0` + v2 `add c@t1` + v2 `remove a@t2` ⇒ `{b,c}`.
  Blob expands to `add a@ts0, add b@ts0`; then OR-Set add-wins/remove by timestamp.
- A v1 `map` blob `{k1:v1}@ts0` + v2 `put k1:v2@t1` ⇒ `{k1:v2}` (per-key LWW).

Property test: for any interleaving of {v1 blob, v2 element ops} across replicas,
all replicas that have seen the same op-set converge to the same materialized
collection.

## 9. Increments (each independently shippable + tested)

1. **Cell model + memtable**: `CellPath` on cells; per-element merge (add-wins /
   LWW / PN). In-memory only. Unit + property (convergence, idempotent re-apply).
2. **Commit-log Mutation** per-element encode/decode (serialize round-trip).
3. **AP path** (`route_update`) emits per-element ADD/remove; drop the RMW for
   commutative ops. `accord_probe`-style plain probe green for set/map.
4. **Accord path** (`materialize_update`) emits per-element; remove the fail-loud
   arm for commutative ops (keep it for positional list ops). Single-threaded
   `accord_probe` list/set/map/counter green.
5. **SSTable BTI writer** complex-column write + read assembly; flush/compaction
   round-trip test.
6. **Counters** (PN-Counter) end to end.
7. **Lazy dual-read + `format_version=2`** + mixed-partition merge (§8) + tests.
8. **Elle list-append** (`t_d7ffb5b7`) strict-serializable **green** — the
   acceptance gate — plus set/map/counter probes.

## 10. Acceptance criteria

- `accord_probe` (extended): Accord txn list/set/map/counter all read back
  correctly, single-threaded and concurrent.
- Elle `list-append` under `:strict-serializable` returns `:valid? true`.
- Property: N-replica concurrent add/remove converges regardless of delivery
  order; re-apply is idempotent.
- Migration: write v1 blob → upgrade binary → v2 append → read merged → compact →
  all-v2, at every step the read is correct.
- No `#[ignore]`, warnings-as-errors clean, per-crate docs updated (`ferrosa-cql`,
  `ferrosa-sstable`, `ferrosa-storage`, `ferrosa-common`).

## 11. Why not the narrower "apply-time RMW"

Apply-time RMW (read current list at Apply, append, write full) keeps the
single-cell model. It works single-writer but: (a) still reads on every append;
(b) concurrent appit — two txns ordered `A<B` — apply A `[x,a]@tA`, then B reads
`[x,a]`, writes `[x,a,b]@tB`; correct only if Apply strictly serializes the read
between them (fragile, extra dep-wait per append); (c) does nothing for the AP
path, which keeps LWW-clobbering whole lists. The per-element CRDT model removes
the read, converges on both paths, and matches Cassandra's on-disk format — a
strictly better shape for the same feature.
