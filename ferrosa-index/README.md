# ferrosa-index

> Secondary and vector index implementations for Ferrosa — the family of
> on-disk index kinds that map indexed column value(s) to `RowPosition`s within
> SSTables, plus the vector / full-text / geospatial search backends.

## What this crate is

`ferrosa-index` owns the **index data structures and their on-disk formats**. It
is the substrate the engine, query layer, and the standalone index builder all
build on: given a stream of rows (`partition_key`, `clustering_key`, `cells`),
each index kind extracts the configured column(s), encodes a key, and records
the row's position; at read time it answers point / range / nearest / scored
queries.

It is deliberately storage-shaped, not query-shaped: it knows `CellValue`
(`ferrosa-common`) and byte-encoded keys, and nothing about CQL parsing,
routing, schema DDL, or the SSTable file layout itself. Those callers
(`ferrosa-cql`, `ferrosa-cluster`, `ferrosa-storage`, `ferrosa-index-builder`,
`ferrosa-schema`, `ferrosa-sparql`, `ferrosa-worker`) drive it.

## What's implemented

The crate exposes **four distinct API surfaces** (an honest map matters here —
they are not one uniform trait):

### 1. Secondary indexes — the root `IndexFactory` / `IndexReader` traits

Mature. Each implements `IndexBuilder` (`add_row` → `finish`) and `IndexReader`
(`lookup` / `range` / `nearest` / `capabilities`):

- **B-tree** (`btree`) — sorted length-prefixed entries; point lookup (binary
  search) + range scan. `nearest` returns `Unsupported`.
- **Hash** (`hash`) — `HashMap<key, Vec<RowPosition>>`; O(1) point lookup.
  `range` and `nearest` return `Unsupported`.
- **Composite** (`composite`) — multi-column key (`col_count | len | bytes …`);
  full-key point lookup + prefix range scan.
- **Phonetic** (`phonetic`) — fuzzy name match via Soundex, Metaphone, Double
  Metaphone, or Caverphone; point lookup on the phonetic code. No range/nearest.
- **Filtered** (`filtered`) — wraps any inner factory and applies a
  `FilterPredicate` (conjunction of `FilterClause`s) at build time; delegates
  reads to the inner reader. Also exports the planner soundness helpers
  (`evaluate_predicate`, `query_constraint_implies_predicate`, …) used to prove
  a query is covered by a partial index.

### 2. Vector search — the `vector` subsystem (its own parallel traits)

`vector` defines its **own** `IndexFactory` / `IndexReader` / `IndexBuilder`,
`RowPosition`, and `IndexCapability` (separate from the crate-root ones, because
vector search has a different query shape and result/capability model):

- **HNSW** (`vector::hnsw`) — Hierarchical Navigable Small World graph for ANN;
  serialized to JSON on disk, loaded whole into memory on open.
- **IVFFlat** (`vector::ivfflat`) — k-means inverted-file + flat rerank; JSON on
  disk, simpler/cheaper build than HNSW, accuracy tuned by `probes`.
- **Quantized** (`vector::quantized`) — page-addressable `.qvec` artifacts:
  scalar codec, a deterministic quantized-IVFFlat builder, and a **staged**
  reader that range-reads centroid pages under a bounded page-read budget rather
  than materializing the whole artifact. The **Q1 (1-bit) tier is explicitly
  experimental** and self-labels its recall impact.
- Distance metrics: `L2`, `Cosine`, `InnerProduct`. Dimension caps: 4096 (f32),
  8192 (f16), perf warning above 2048.

### 3. Full-text — the `fulltext` pipeline (its own builder/reader)

Inverted-index pipeline for `column = fts_match(query)`: `analyzer` (Standard /
Simple / Keyword + stemmer), `builder` (`FullTextIndexBuilder` → bytes),
`reader` (`FullTextIndexReader`, BM25-scored queries returning `FtsHit`),
`query` (query-string → `FtsQuery` tree), `scoring` (BM25), `merge`
(compaction-time merge of two FTI byte buffers), `stream` (bounded-memory
single-term search straight off an on-disk FTI sidecar), and `topk` (bounded
top-k hit selection). Does **not** implement the root `IndexFactory` traits —
it has its own byte-buffer build/open API.

Memory model (t_ee98faa0 layer 2 — the replica-side `fts_match` OOM):

- `reader.search_top_k[_str](query, k)` — `LIMIT k` searches hold O(k) owned
  memory instead of score-everything-then-rank. `Term` streams postings
  through a bounded heap; `MultiTerm`/`Phrase` drive the smallest posting
  list against borrowed key maps; compound shapes (`And`/`Or`/`Not`/`Prefix`)
  reuse the exact evaluator with borrowed (pointer-sized) keys and clone only
  the k winners. Matching semantics are identical to `search`; `k` is always
  query-derived — never a server cap.
- `reader.search(query)` (no LIMIT) returns the complete match set; its
  evaluation maps borrow doc keys from the index (no per-doc key clones).
- `stream::stream_search_term(path, term, limit)` — single-term sidecar
  search that never reads or deserializes the whole index file: it walks the
  sorted term dictionary through a `BufReader`, skips other terms' postings,
  and scores the one matching posting list (bounded top-k when `limit` is
  set). Guarded by `tests/fulltext_topk_memory_bound.rs` (allocator-tracked
  peak: O(k), independent of matching-doc count).
- `FullTextIndexReader::from_index(fti)` wraps an in-memory index without a
  serialize→deserialize round trip (used by the transient memtable / fallback
  scans in `ferrosa-storage`).

**Analyzer parity (query side == index side).** Documents are indexed through
the index's `analyzer`, which for the default `StandardAnalyzer` strips English
stop words and splits on punctuation. The query side must apply the *same*
analyzer to its terms, or a token the analyzer would drop/rewrite looks up a
posting list that can never exist. `query::analyze_query(query, analyzer)`
re-analyzes a parsed `FtsQuery` before evaluation: a term that analyzes to
nothing (a stop word) is **dropped** from a conjunction rather than required, a
term that splits on punctuation becomes a conjunction of its tokens, `Prefix` is
left untouched, and a query that reduces to no searchable terms returns `None`
(matches nothing, without erroring). `FullTextIndexReader` carries its analyzer
(`open`/`from_index` default to the shared `analyzer::default_analyzer()`;
`open_with_analyzer`/`from_index_with_analyzer` accept a non-default one) and
normalizes inside `search`/`search_top_k`, so every caller — memtable, sidecar,
and missing-sidecar scans — inherits parity. The single-`Term` streaming fast
path in `ferrosa-storage` normalizes the term with the same analyzer before the
`stream` walk. Without this, `fts_match('reports were emitted')` returned zero
rows even when documents contained "reports … emitted" (live repro:
ferrosa-memory `fixed_document_search_survives_fts_stopword_absent_from_corpus`).

### 4. Geospatial — the `geo` library (Phase 1, pure functions)

A space-filling-curve **point** index: encode `(lat, lon)` to a sortable `u64`
cell id (`encode`), cover a bbox/radius/k-NN query as contiguous cell-id ranges
(`cover`, `knn`) over a **BTree sidecar**, then refine with exact
distance/containment (`refine`, `geometry`, `predicate`, `rtree`). It is **pure
— it never touches storage**; a geo-aware builder derives cell ids and writes
them through the BTree index. Marked **Phase 1**: point indexing + bbox/radius/
k-NN/ST_Contains/ST_Intersects; polygons-with-holes and some two-geometry
predicate combinations are not yet exact.

### Shared crate-root types

`IndexType` (BTree / Hash / Composite / Phonetic / Filtered / Vector / FullText
/ Geo, with stable bincode tags), `IndexConfig`, `IndexFiles`, `IndexKey`,
`RowPosition`, `IndexCapabilities`, `IndexError` / `IndexResult`, the
`FilterPredicate` / `FilterClause` / `FilterOp` model (with a back-compat custom
`Serialize`/`Deserialize` accepting both the legacy flat single-clause shape and
the v2 conjunction shape, in JSON and bincode), and the `vector<float, N>`
big-endian codec (`bytes_to_vec_f32` / `vec_f32_to_bytes`).

## How it works

```text
add_row(pk, ck, cells, column_positions)   ──build──▶  on-disk index file(s)
                                                              │
IndexKey / GeoPredicate / FtsQuery / query vec  ──read──▶  Vec<RowPosition> (+ score)
```

The engine streams rows into a builder during memtable flush / compaction (or
the standalone `ferrosa-index-builder` does it out of process), `finish()`
writes the artifact, and a reader is opened lazily to serve lookups. Secondary
indexes use compact length-prefixed binary; HNSW/IVFFlat use JSON; quantized
vectors use the paged `.qvec` container.

## Public API (key entry points)

| Area | Entry points |
|------|--------------|
| Root traits | `IndexFactory`, `IndexBuilder`, `IndexReader`, `IndexCapabilities` |
| Secondary factories | `btree::BTreeIndexFactory`, `hash::HashIndexFactory`, `composite::CompositeIndexFactory`, `phonetic::PhoneticIndexFactory`, `filtered::FilteredIndexFactory` |
| Filter model | `FilterPredicate`, `FilterClause`, `FilterOp`, `evaluate_predicate[_row]`, `query_constraint_implies_predicate[_clause]` |
| Vector | `vector::{IndexFactory, IndexReader}`, `vector::hnsw::HnswFactory`, `vector::ivfflat::IvfFlatFactory`, `vector::quantized::*`, `DistanceMetric`, `bytes_to_vec_f32`, `vec_f32_to_bytes` |
| Full-text | `fulltext::builder::FullTextIndexBuilder`, `fulltext::reader::FullTextIndexReader`, `fulltext::query::FtsQuery`, `fulltext::merge` |
| Geo | `geo::{encode_point, cover_bbox, cover_radius, nearest_k, st_contains, st_intersects, GeoPredicate, CellRange, GeoCrs}` |
| Core types | `IndexType`, `IndexConfig`, `IndexFiles`, `IndexKey`, `RowPosition`, `IndexError`, `IndexResult` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `CellValue` (the storage cell model each builder reads
  to extract indexed column bytes). This is the crate's only ferrosa dependency.

External: `bincode`, `crc32fast`, `rand`, `serde`, `serde_json`, `tracing`.

**Called by** (crates that depend on this):

- **`ferrosa-cluster`** — DDL/write/read paths and Raft state machine route index
  builds and lookups.
- **`ferrosa-cql`** — `IndexType`, `FilterOp`/`FilterClause`/`FilterPredicate`
  construction from CQL, vector codec, geo `CellRange` cover ranges.
- **`ferrosa-index-builder`** — standalone out-of-process index construction.
- **`ferrosa-schema`** — index metadata / DDL.
- **`ferrosa-sparql`** — index-backed lookups for the SPARQL endpoint.
- **`ferrosa-storage`** — memtable / SSTable index pipeline.
- **`ferrosa-worker`** — background index build tasks.

## Tests

251 in-crate `#[test]`s spread across every module (heaviest in `geo::geometry`,
`vector::mod`, `filtered`, `fulltext::{query,reader}`, `composite`, `btree`).
Coverage is uneven by kind — see [specs/fmea.md](specs/fmea.md): the quantized
IVFFlat builder/reader and the FTI builder are comparatively thin, and there is
no cross-kind property test of the `decode(encode(v)) == v` round-trip for the
vector codec beyond a couple of unit cases.

## Specs

- [Architecture overview](specs/overview.md) — module map, the four API surfaces, data flow, invariants
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps, RPN-ranked
- [Roadmap](specs/roadmap.md) — Now / Next / Later
