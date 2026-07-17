---
crate: ferrosa-index
doc: fmea
last_updated: 2026-07-16
---

# ferrosa-index — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This crate sits on the index build/read path: a wrong
index silently returns wrong query results, so detection difficulty (silent
corruption) dominates the high scores.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| IDX-1 | Vector cell codec disagrees with CQL byte order | ANN ranks byte-swapped garbage; nearest-neighbour results silently wrong | 9 | 2 | 6 | 108 | `bytes_to_vec_f32`/`vec_f32_to_bytes` are big-endian and pinned by `lib.rs` tests that rebuild a CQL cell and compare. **Risk: only a few unit cases, no property test across the f32 space.** |
| IDX-2 | Quantized vector recall loss accepted as correct (esp. Q1 1-bit tier) | Queries miss true neighbours; "experimental" tier used in prod unaware | 7 | 4 | 7 | 196 | Q1 self-labels `is_experimental()` and reports recall impact; codec round-trips within declared error bounds. **Gap: no recall floor enforced at the API boundary; a caller can build a Q1 index and get degraded results with no guardrail.** |
| IDX-3 | `IndexType` bincode tag order changes | Persisted index metadata (`system_schema.indexes`, Raft log) fails to deserialize after upgrade → indexes unreadable | 9 | 1 | 4 | 36 | Tag order asserted in `bincode_index_type_variant_tag_stability`; enum is append-only by convention. |
| IDX-4 | `FilterPredicate` wire incompatibility | Old partial-index rows fail to decode, or an empty conjunction silently indexes every row | 8 | 2 | 5 | 80 | Custom serde accepts legacy flat + v2 conjunction in JSON and bincode (round-trip tests); empty conjunction evaluates `false` (fail safe). |
| IDX-5 | Composite key ordering assumption violated | Prefix range scans miss rows / return extra rows | 7 | 3 | 6 | 126 | Encoding documented; lexicographic ordering is exact only when component values are equal-length, otherwise only exact-prefix match holds. **Documented limitation, not fully test-guarded for variable-length components.** |
| IDX-6 | Geo polygon / two-geometry predicates partial (Phase 1) | `ST_Contains`/`ST_Intersects` over polygons-with-holes or unsupported geometry combos return wrong or `PredicateError` | 6 | 4 | 4 | 96 | Single-ring polygons + bbox/radius/k-NN covered and tested (17 geometry + 10 predicate tests); holes / some combos explicitly unsupported and surfaced as `PredicateError` (fail loud), not silently wrong. |
| IDX-7 | HNSW/IVFFlat JSON artifact loaded whole into memory | Large vector indexes spike RSS on open; no paging | 5 | 4 | 3 | 60 | Known design limit; the quantized `.qvec` staged reader is the page-budgeted alternative but does not yet cover HNSW/IVFFlat. |
| IDX-8 | Thin test coverage on quantized IVF builder & FTI builder | A change passes `cargo test -p ferrosa-index` while breaking the artifact path | 7 | 4 | 6 | 168 | quantized `ivf.rs` has 1 test, `ivf_staged.rs` 3, `fulltext/builder.rs` 3. **Open gap** — uneven coverage relative to risk. |
| IDX-9 | Index file truncation/corruption read as valid | Partial/garbage index served as real results | 8 | 2 | 5 | 80 | `crc32fast` available; container/codec readers fail loud on malformed pages. **Verify CRC is checked on every artifact kind, not just `.qvec`.** |
| IDX-10 | `nearest`/`range` on an unsupported kind | Caller assumes empty result is "no matches" | 6 | 2 | 3 | 36 | Kinds return `IndexError::Unsupported` (fail loud) rather than `Ok(vec![])`; covered by per-kind unit tests. |
| IDX-11 | FTS query terms not run through the index analyzer (asymmetry) | Any natural-language `fts_match` query containing an English stop word (or a punctuation-bearing term) requires a posting list the analyzer never created → **deterministic false-empty result** even when documents match | 8 | 6 | 7 | 336 | **Fixed.** `query::analyze_query` normalizes the parsed query with the reader's analyzer inside `search`/`search_top_k`; the `ferrosa-storage` single-`Term` stream fast path normalizes the term too. Guarded by `reader.rs` parity tests, `query.rs` `analyze_*` unit tests, and `ferrosa-storage` `fts_stopword_bearing_query_matches_after_flush`. Live repro was ferrosa-memory `fixed_document_search_survives_fts_stopword_absent_from_corpus`. **Residual: analyzer is not persisted in the FTI; a future non-default per-index analyzer must be threaded to `open_with_analyzer` or parity breaks again.** |

## Top risks to act on

1. **IDX-2 (RPN 196)** — quantized recall has no enforced floor. The experimental
   Q1 tier is honestly labelled but nothing stops a caller building a Q1 index
   and silently shipping degraded ANN. Add a recall guardrail / require explicit
   opt-in for experimental tiers.
2. **IDX-8 (RPN 168)** — the quantized IVF builder and FTI builder are the least
   tested modules relative to their blast radius; green build is a weak signal
   there. Add build→read round-trip + golden-artifact tests.
3. **IDX-5 (RPN 126)** — composite prefix ordering is only exact for equal-length
   components; variable-length component ordering needs a property test or a
   documented prohibition enforced at construction.
4. **IDX-11 residual** — the query-vs-index analyzer asymmetry is fixed, but the
   analyzer choice is *not persisted* in the FTI. Parity holds today only
   because every builder and reader defaults to `analyzer::default_analyzer()`.
   When a per-index analyzer option (`WITH OPTIONS {'analyzer': ...}`) is
   actually threaded, the FTI must record it (or the read path must recover it
   from index metadata) and open via `open_with_analyzer`, or the false-empty
   failure returns for non-default analyzers.

## Detection assets

- `lib.rs` codec tests (big-endian vector round-trip vs a hand-built CQL cell).
- `bincode_*` tests pinning `IndexType` tags and `FilterPredicate` legacy/v2 wire
  shapes.
- Per-kind `nearest_returns_unsupported` / `range` tests asserting fail-loud.
- `geo` geometry + predicate suites (27 tests) for cover/refine/ST correctness.
- quantized codec characterization test labelling Q1 experimental + recall impact.
