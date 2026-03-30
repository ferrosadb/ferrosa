# Project Plan: NVMe Table Pinning + Full-Text Indexing

> Created: 2026-03-29
> Status: Draft
> Architecture: [nvme-pinning-architecture.md](nvme-pinning-architecture.md), [fulltext-index-architecture.md](fulltext-index-architecture.md)

---

## Threat Model Delta

### New Trust Boundaries

| Boundary | Feature | Threat |
|----------|---------|--------|
| Local NVMe ↔ Application | NVMe pin | Data at rest on unencrypted local disk; node theft exposes pinned data |
| CQL query ↔ FTS query parser | FTS | Malicious FTS query strings could cause ReDoS in tokenizer or OOM via wildcard expansion |
| FTS sidecar ↔ SSTable | FTS | Corrupted FTI sidecar returns wrong search results (silent data integrity issue) |

### STRIDE Analysis

| Threat | Category | Feature | Severity | Mitigation |
|--------|----------|---------|----------|------------|
| Pinned data exposed on disk theft | Info Disclosure | NVMe | Medium | Document trade-off; recommend at-rest encryption for sensitive tables |
| ReDoS via crafted FTS query | DoS | FTS | High | Query complexity limit (max 1000 terms expanded); timeout per query |
| Wildcard prefix `*` expands to all terms | DoS | FTS | High | Minimum prefix length = 2 chars; max expansion = 10000 terms |
| Corrupted FTI sidecar returns wrong results | Integrity | FTS | Medium | CRC32 footer checksum; rebuild from SSTable on mismatch |
| Pinned table fills NVMe disk | DoS | NVMe | High | `pin_max_bytes` cap; alert at 80% of cap; evict oldest SSTables beyond cap |
| Stale FTS index after failed compaction | Integrity | FTS | Medium | Sidecar rebuild triggered on any index checksum failure |

---

## FMEA

| # | Component | Failure Mode | Severity | Occurrence | Detection | RPN | Test Case |
|---|-----------|-------------|----------|-----------|-----------|-----|-----------|
| F-NV-01 | NVMe pin | Pinned table exceeds NVMe capacity → node OOM/disk full | 8 | 4 | 3 | 96 | `pinned_table_respects_max_bytes` |
| F-NV-02 | NVMe pin | Node replacement → pinned data permanently lost | 9 | 3 | 2 | 54 | `pinned_data_lost_on_node_replace` (documented behavior) |
| F-NV-03 | NVMe pin | ALTER TABLE from nvme→none fails mid-upload | 6 | 2 | 4 | 48 | `unpin_resumes_s3_upload` |
| F-NV-04 | NVMe pin | Pin config not persisted → reverts on restart | 7 | 2 | 3 | 42 | `pin_config_survives_restart` |
| F-FT-01 | FTS builder | Tokenizer panic on malformed UTF-8 | 8 | 3 | 5 | 120 | `fts_builder_handles_invalid_utf8` |
| F-FT-02 | FTS reader | Binary search off-by-one returns wrong term | 9 | 2 | 4 | 72 | `fts_term_lookup_exact_match` |
| F-FT-03 | FTS query | AND/OR precedence wrong → incorrect results | 8 | 3 | 3 | 72 | `fts_query_boolean_precedence` |
| F-FT-04 | FTS compaction | Merged index has duplicate postings | 7 | 3 | 4 | 84 | `fts_compaction_merge_deduplicates` |
| F-FT-05 | FTS sidecar | Checksum mismatch on read | 6 | 2 | 2 | 24 | `fts_sidecar_checksum_verified` |
| F-FT-06 | FTS query | Wildcard expands to millions of terms → OOM | 9 | 4 | 3 | 108 | `fts_wildcard_expansion_capped` |
| F-FT-07 | FTS analyzer | Stop words not applied → irrelevant results | 4 | 3 | 3 | 36 | `fts_stop_words_filtered` |
| F-FT-08 | FTS S3 | FTI sidecar not included in S3 upload | 8 | 2 | 3 | 48 | `fts_sidecar_uploaded_to_s3` |

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|-----------|
| NVMe pinning gives false sense of durability | High | High | Document clearly; log warning on startup for pinned tables without replication |
| FTS query performance degrades on large datasets | Medium | Medium | BM25 top-K cutoff; index sharding per SSTable bounds total scan |
| FTS tokenizer has edge cases with CJK/emoji | Medium | Low | Start with Latin-only StandardAnalyzer; add ICU tokenizer in v2 |
| Pin max_bytes race condition during concurrent flushes | Low | Medium | Atomic size tracking with compare-and-swap |

---

## Sprint Plan

### Sprint NV1: NVMe Table Pinning (1 week)

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| NV1.1 | Create `PinConfig` parser from table extensions | S | `PinConfig::from_extensions` returns correct mode for `storage.pin = nvme/none` | `pin_config_parses_nvme`, `pin_config_default_none` |
| NV1.2 | Wire `register_table` to read pin config and pass to TableStore | S | TableStore knows its pin mode at construction time | `table_store_knows_pin_mode` |
| NV1.3 | Skip S3 upload for pinned tables in `sync_sstables_to_s3` | M | Pinned table's SSTables not in S3 store after flush | `pinned_table_skips_s3_upload` |
| NV1.4 | Skip S3 upload for pinned tables in `poll_compactions` | S | Compacted output of pinned table not uploaded | `pinned_compaction_skips_s3` |
| NV1.5 | Pin SSTable IDs in LocalCache on flush | S | Pinned SSTables not evicted by `evict_if_needed` | `pinned_sstables_never_evicted` |
| NV1.6 | `pin_max_bytes` enforcement on flush | M | When total pinned bytes exceed cap, oldest SSTables evicted | `pinned_table_respects_max_bytes` |
| NV1.7 | ALTER TABLE pin/unpin transition | M | Unpin triggers S3 upload of existing SSTables; pin stops uploads | `unpin_resumes_s3_upload`, `pin_stops_s3_upload` |
| NV1.8 | Pin config survives restart (persisted in schema.json) | S | After restart, table is still pinned | `pin_config_survives_restart` |
| NV1.9 | Observability: Prometheus metrics for pinned tables | S | Gauges and counters emitted | `pinned_metrics_accurate` |

### Sprint FT1: Full-Text Index Core (2 weeks)

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| FT1.1 | Add `IndexType::FullText` to enum + `resolve_index_type` | S | `CREATE INDEX ... USING 'fulltext'` accepted by CQL router | `create_fulltext_index_accepted` |
| FT1.2 | Standard analyzer: lowercase + Unicode word tokenizer | M | `"Hello World! Rust is great"` → `["hello", "world", "rust", "great"]` (stop words removed) | `standard_analyzer_tokenizes`, `standard_analyzer_removes_stops` |
| FT1.3 | Simple analyzer: whitespace + lowercase | S | `"Hello World"` → `["hello", "world"]` | `simple_analyzer_tokenizes` |
| FT1.4 | English stemmer (Porter) | M | `"running"` → `"run"`, `"databases"` → `"databas"` | `porter_stemmer_basic`, `porter_stemmer_irregular` |
| FT1.5 | Inverted index builder (in-memory) | L | Build index from rows, serialize to FTI format | `fts_builder_single_doc`, `fts_builder_multi_doc`, `fts_builder_handles_invalid_utf8` |
| FT1.6 | Inverted index reader (binary search terms) | M | Read FTI file, look up terms, return postings | `fts_term_lookup_exact_match`, `fts_term_lookup_missing` |
| FT1.7 | FTI file format: header + terms dict + postings + CRC footer | M | Write + read roundtrip passes checksum | `fts_file_format_roundtrip`, `fts_sidecar_checksum_verified` |
| FT1.8 | Wire builder into IndexBuildScheduler | M | After flush, FTI sidecar file created alongside SSTable | `fts_sidecar_created_on_flush` |

### Sprint FT2: Full-Text Query + Integration (1-2 weeks)

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| FT2.1 | FTS query parser: term, phrase, AND, OR, NOT, prefix | M | `"rust AND \"S3 backed\""` parses to `And(Term("rust"), Phrase(["s3", "backed"]))` | `fts_query_parse_term`, `fts_query_parse_phrase`, `fts_query_parse_boolean`, `fts_query_boolean_precedence` |
| FT2.2 | BM25 scoring | M | Given term freq + doc freq + doc length, produces correct score | `bm25_single_term`, `bm25_multi_term_ranking` |
| FT2.3 | `fts_match()` CQL function wired to WHERE clause | L | `SELECT ... WHERE fts_match(col, 'query')` returns matching rows | `fts_match_simple_query`, `fts_match_boolean_query` |
| FT2.4 | Wildcard expansion with cap (max 10000 terms) | S | `compac*` expands to matching terms; `*` returns error | `fts_wildcard_expansion_capped`, `fts_wildcard_bare_star_rejected` |
| FT2.5 | Compaction merge: merge FTI sidecars | M | Two FTI files merged correctly; no duplicate postings | `fts_compaction_merge_deduplicates`, `fts_compaction_merge_preserves_scores` |
| FT2.6 | S3 upload includes FTI sidecar | S | After flush + S3 sync, FTI file in object store | `fts_sidecar_uploaded_to_s3` |
| FT2.7 | End-to-end: INSERT rows → CREATE INDEX → flush → query | L | Full pipeline works in single test | `fts_end_to_end_insert_query` |
| FT2.8 | Language analyzer options (analyzer, language, stop_words) | M | Custom stop words list applied; language stemmer selected | `fts_custom_stop_words`, `fts_language_analyzer` |

---

## Compiled Task Order and Dependencies

```
NV1.1 (PinConfig) ──→ NV1.2 (wire to TableStore) ──→ NV1.3 (skip S3 upload)
                                                    ──→ NV1.4 (skip in compaction)
                                                    ──→ NV1.5 (pin in LocalCache)
NV1.3 + NV1.5 ──→ NV1.6 (max_bytes)
NV1.3 + NV1.5 ──→ NV1.7 (ALTER TABLE pin/unpin)
NV1.2 ──→ NV1.8 (persist across restart)
NV1.6 ──→ NV1.9 (metrics)

FT1.1 (IndexType) ──→ FT1.8 (wire to scheduler)
FT1.2 + FT1.3 (analyzers) ──→ FT1.5 (builder)
FT1.4 (stemmer) ──→ FT1.2 (feeds into StandardAnalyzer)
FT1.5 (builder) ──→ FT1.7 (file format)
FT1.6 (reader) ──→ FT1.7 (file format)
FT1.7 ──→ FT1.8 (wire to scheduler)

FT1.7 + FT1.6 ──→ FT2.1 (query parser)
FT2.1 ──→ FT2.2 (BM25)
FT2.1 + FT2.2 ──→ FT2.3 (CQL function)
FT2.1 ──→ FT2.4 (wildcard cap)
FT1.7 ──→ FT2.5 (compaction merge)
FT1.8 ──→ FT2.6 (S3 upload)
FT2.3 + FT2.5 + FT2.6 ──→ FT2.7 (end-to-end)
FT1.2 ──→ FT2.8 (language options)
```

---

## Test Count

| Sprint | New Tests | Gate |
|--------|-----------|------|
| NV1 | ~14 | Pinned table skips S3, survives restart, respects max_bytes |
| FT1 | ~16 | Analyzer tokenizes, builder builds, reader reads, sidecar created on flush |
| FT2 | ~16 | Query parser works, BM25 ranks, CQL function returns results, end-to-end passes |
| **Total** | **~46** | |
