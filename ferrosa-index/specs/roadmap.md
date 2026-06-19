---
crate: ferrosa-index
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-index — Roadmap

Sourced from the in-code module notes (quantized/geo "Phase" markers,
`is_experimental` tier), the FMEA gaps ([fmea.md](fmea.md)), and the
dependency/usage review.

## Now (highest value)

- **Guardrail the experimental quantized tiers** (FMEA IDX-2). The Q1 (1-bit)
  tier self-labels experimental and reports recall impact, but nothing stops a
  caller building and serving it unaware. Require explicit opt-in for
  experimental tiers and/or enforce a recall floor at the build API boundary —
  fail loud rather than silently degrade ANN quality.
- **Close the quantized-IVF / FTI builder test gap** (FMEA IDX-8). Add
  build→read round-trip and golden-artifact tests for `vector::quantized::ivf`,
  `ivf_staged`, and `fulltext::builder`, which are the least-tested modules
  relative to their blast radius.

## Next

- **Property-test the vector codec round-trip** (`decode(encode(v)) == v`) across
  the f32 space, not just the two hand-built CQL-cell unit cases (FMEA IDX-1).
- **Pin composite prefix-ordering semantics** (FMEA IDX-5). Either property-test
  variable-length component ordering or reject/normalize at construction so the
  "exact only for equal-length components" caveat can't silently bite range scans.
- **Audit CRC coverage across artifact kinds** (FMEA IDX-9). Confirm every reader
  (B-tree / hash / composite / HNSW / IVFFlat / FTI), not just the `.qvec`
  container, validates integrity on open and fails loud on truncation.

## Later

- **Page-bounded HNSW / IVFFlat reads** (FMEA IDX-7). Extend the quantized staged
  reader's page-budget model to the JSON-backed vector indexes so large indexes
  no longer load whole into memory on open.
- **Geo Phase 2** — polygons with holes, the currently-unsupported two-geometry
  predicate combinations, and a true geo index *kind* wired to a factory rather
  than the pure cover/refine library over a BTree sidecar.
- **Unify or formally document the four API surfaces.** The root `IndexFactory`
  traits, the parallel `vector::` traits, the `fulltext` byte-buffer API, and the
  pure `geo` library are intentionally separate; either converge them behind a
  common dispatch or document the split as a decision so callers aren't surprised.

## Non-goals

- CQL/SPARQL parsing, query planning, routing, replication, or the SSTable
  container format — those belong to `ferrosa-cql`, `ferrosa-sparql`,
  `ferrosa-cluster`, and `ferrosa-storage`. This crate stays the index
  data-structure layer with a single `ferrosa-common` dependency.
