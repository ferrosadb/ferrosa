# TDD Plan: NVMe Table Pinning + Full-Text Indexing

> Created: 2026-03-30
> Updated: 2026-03-30
> Status: **COMPLETE** — all tests verified on `fix/fulltext-build-errors` at `a03ff27`
> Scope: Wire existing NVMe pin config + FTS core into the storage engine end-to-end. See [project-plan-nvme-fts.md](project-plan-nvme-fts.md) for sprint details.
> Runner: `cargo test`, `FERROSA_TEST_CONTAINERS=1 cargo test`
> TDD cycle: Red -> Green -> Refactor. One failing test at a time.

---

## Current State Summary

| Component | Code Status | What's Missing |
|-----------|-------------|----------------|
| `PinConfig` parser (`pin_config.rs`) | **DONE** | Nothing — parses `storage.pin` + `storage.pin_max_bytes` |
| `LocalCache` pinned set (`cache.rs`) | **DONE** | `evict_if_needed(&pinned)` works; need to wire pinned set into flush |
| `sync_sstables_to_s3` (`engine.rs`) | Exists | No pin_config check — always uploads |
| `poll_compactions` (`engine.rs`) | Exists | No pin_config check — always uploads |
| Pin in LocalCache on flush | **NOT WIRED** | Pinned SSTable IDs not added to pinned set |
| `pin_max_bytes` enforcement | **NOT WIRED** | No eviction logic for pinned tables exceeding cap |
| ALTER TABLE pin/unpin | **NOT WIRED** | No transition logic |
| Pin config restart persistence | **NOT WIRED** | Not verified on schema load |
| Pin metrics | **NOT DONE** | No Prometheus gauges/counters |
| `IndexType::FullText` enum | **DONE** | resolve_index_type maps `"fulltext"` |
| StandardAnalyzer | **DONE** | Lowercase + word tokenizer + stop words |
| SimpleAnalyzer | **DONE** | Whitespace + lowercase |
| Porter stemmer | **DONE** | In StandardAnalyzer pipeline |
| FTI builder | **DONE** | `FullTextIndexBuilder::finish() -> Vec<u8>` |
| FTI reader | **DONE** | `FullTextIndexReader::open(bytes)`, `search_str()` |
| FTI file format | **DONE** | Header + terms dict + postings + CRC |
| FTI sidecar on flush | **NOT WIRED** | Builder exists but flush doesn't call it |
| FTS query parser | **DONE** | term, phrase, AND, OR, NOT |
| BM25 scoring | **DONE** | `scoring.rs` |
| `fts_match()` CQL function | **DONE** | Router detects + calls `engine.fulltext_search()` |
| Wildcard expansion cap | **NOT DONE** | No prefix query limit |
| FTI compaction merge | **EXISTS** | `merge.rs` exists but not wired to `poll_compactions` |
| S3 upload of FTI sidecar | **PARTIAL** | `UploadTask::IndexFiles` variant exists |
| End-to-end FTS test | **NOT DONE** | No INSERT -> flush -> query integration test |
| Language analyzer options | **NOT DONE** | No custom stop words or language stemmer selection |

---

## Master Test List

### NV1: NVMe Table Pinning

- [ ] `pin_config_parses_nvme` (NV1.1)
- [ ] `pin_config_default_none` (NV1.1)
- [ ] `table_store_knows_pin_mode` (NV1.2)
- [ ] `pinned_table_skips_s3_upload` (NV1.3)
- [ ] `pinned_compaction_skips_s3` (NV1.4)
- [ ] `pinned_sstables_never_evicted` (NV1.5)
- [ ] `pinned_table_respects_max_bytes` (NV1.6)
- [ ] `unpin_resumes_s3_upload` (NV1.7)
- [ ] `pin_stops_s3_upload` (NV1.7)
- [ ] `pin_config_survives_restart` (NV1.8)
- [ ] `pinned_metrics_accurate` (NV1.9)

### FT1: Full-Text Index Core

- [ ] `create_fulltext_index_accepted` (FT1.1)
- [ ] `standard_analyzer_tokenizes` (FT1.2)
- [ ] `standard_analyzer_removes_stops` (FT1.2)
- [ ] `simple_analyzer_tokenizes` (FT1.3)
- [ ] `porter_stemmer_basic` (FT1.4)
- [ ] `porter_stemmer_irregular` (FT1.4)
- [ ] `fts_builder_single_doc` (FT1.5)
- [ ] `fts_builder_multi_doc` (FT1.5)
- [ ] `fts_builder_handles_invalid_utf8` (FT1.5)
- [ ] `fts_term_lookup_exact_match` (FT1.6)
- [ ] `fts_term_lookup_missing` (FT1.6)
- [ ] `fts_file_format_roundtrip` (FT1.7)
- [ ] `fts_sidecar_checksum_verified` (FT1.7)
- [ ] `fts_sidecar_created_on_flush` (FT1.8)

### FT2: Full-Text Query + Integration

- [ ] `fts_query_parse_term` (FT2.1)
- [ ] `fts_query_parse_phrase` (FT2.1)
- [ ] `fts_query_parse_boolean` (FT2.1)
- [ ] `fts_query_boolean_precedence` (FT2.1)
- [ ] `bm25_single_term` (FT2.2)
- [ ] `bm25_multi_term_ranking` (FT2.2)
- [ ] `fts_match_simple_query` (FT2.3)
- [ ] `fts_match_boolean_query` (FT2.3)
- [ ] `fts_wildcard_expansion_capped` (FT2.4)
- [ ] `fts_wildcard_bare_star_rejected` (FT2.4)
- [ ] `fts_compaction_merge_deduplicates` (FT2.5)
- [ ] `fts_compaction_merge_preserves_scores` (FT2.5)
- [ ] `fts_sidecar_uploaded_to_s3` (FT2.6)
- [ ] `fts_end_to_end_insert_query` (FT2.7)
- [ ] `fts_custom_stop_words` (FT2.8)
- [ ] `fts_language_analyzer` (FT2.8)

---

## Batch Execution Order

```
Batch A (no deps, pure unit tests — run in parallel):
  NV1.1 pin_config tests (verify existing code)
  FT1.1 create_fulltext_index_accepted (verify existing code)
  FT1.2 standard_analyzer tests (verify existing code)
  FT1.3 simple_analyzer (verify existing code)
  FT1.4 porter_stemmer (verify existing code)
  FT1.5 builder tests (verify existing code)
  FT1.6 reader tests (verify existing code)
  FT1.7 file format tests (verify existing code)
  FT2.1 query parser tests (verify existing code)
  FT2.2 BM25 scoring tests (verify existing code)

Batch B (depends on A — integration wiring):
  NV1.2 table_store_knows_pin_mode
  NV1.3 pinned_table_skips_s3_upload
  NV1.4 pinned_compaction_skips_s3
  NV1.5 pinned_sstables_never_evicted
  FT1.8 fts_sidecar_created_on_flush
  FT2.3 fts_match_simple_query / fts_match_boolean_query
  FT2.4 wildcard expansion cap

Batch C (depends on B — compound integration):
  NV1.6 pinned_table_respects_max_bytes
  NV1.7 unpin_resumes_s3_upload / pin_stops_s3_upload
  NV1.8 pin_config_survives_restart
  FT2.5 compaction merge (wired)
  FT2.6 fts_sidecar_uploaded_to_s3

Batch D (depends on C — end-to-end + observability):
  NV1.9 pinned_metrics_accurate
  FT2.7 fts_end_to_end_insert_query
  FT2.8 language analyzer options
```

---

## Batch A: Verify Existing Code (Pure Unit Tests)

These tests confirm existing implementations work. Most should pass immediately (green on first run). Any that fail reveal bugs to fix before integration wiring begins.

### Test 1 — `pin_config_parses_nvme` (NV1.1)

**File:** `ferrosa-storage/src/pin_config.rs` (existing test module)

```
Given: extensions map with {"storage.pin": "nvme"}
When:  PinConfig::from_extensions(&map)
Then:  config.mode == PinMode::NvMe, config.is_pinned() == true
```

**Expected:** GREEN immediately — `pin_config.rs` already has this logic.

### Test 2 — `pin_config_default_none` (NV1.1)

```
Given: empty extensions map
When:  PinConfig::from_extensions(&map)
Then:  config.mode == PinMode::None, config.is_pinned() == false
```

**Expected:** GREEN immediately.

### Test 3 — `create_fulltext_index_accepted` (FT1.1)

**File:** `ferrosa-cql/src/router.rs` (test module)

```
Given: table "ks.articles" with column "body" (text type)
When:  CQL "CREATE INDEX idx_body ON ks.articles (body) USING 'fulltext'" parsed
Then:  index created with IndexType::FullText, target_columns = ["body"]
```

**Expected:** GREEN — `resolve_index_type` maps `"fulltext"` already.

### Test 4 — `standard_analyzer_tokenizes` (FT1.2)

**File:** `ferrosa-index/src/fulltext/analyzer.rs` (test module)

```
Given: StandardAnalyzer
When:  analyze("Hello World! Rust is great")
Then:  tokens == ["hello", "world", "rust", "great"] (stop words "is" removed)
```

**Expected:** GREEN — StandardAnalyzer implemented.

### Test 5 — `standard_analyzer_removes_stops` (FT1.2)

```
Given: StandardAnalyzer
When:  analyze("the quick brown fox jumps over the lazy dog")
Then:  tokens do NOT contain "the", "over"
       tokens DO contain "quick", "brown", "fox", "jump" (stemmed), "lazi" (stemmed), "dog"
```

### Test 6 — `simple_analyzer_tokenizes` (FT1.3)

**File:** `ferrosa-index/src/fulltext/analyzer.rs`

```
Given: SimpleAnalyzer
When:  analyze("Hello World")
Then:  tokens == ["hello", "world"] (no stop word removal, no stemming)
```

**Expected:** GREEN.

### Test 7 — `porter_stemmer_basic` (FT1.4)

```
Given: StandardAnalyzer (includes Porter stemmer)
When:  analyze("running databases")
Then:  tokens include "run" (stemmed from "running"), "databas" (stemmed from "databases")
```

### Test 8 — `porter_stemmer_irregular` (FT1.4)

```
Given: StandardAnalyzer
When:  analyze("flies flying flown")
Then:  all three stem to "fli" or equivalent consistent stem
```

### Test 9 — `fts_builder_single_doc` (FT1.5)

**File:** `ferrosa-index/src/fulltext/builder.rs`

```
Given: FullTextIndexBuilder::new()
When:  add_document(pk=b"row1", "hello world")
       finish()
Then:  serialized bytes deserialize via FullTextIndexReader
       reader.doc_count() == 1
       reader.lookup("hello") returns posting for "row1"
```

**Expected:** GREEN — builder + reader implemented.

### Test 10 — `fts_builder_multi_doc` (FT1.5)

```
Given: builder with 3 documents:
       pk=b"a" -> "rust distributed database"
       pk=b"b" -> "cassandra compatible storage"
       pk=b"c" -> "rust storage engine"
When:  finish() -> reader
Then:  reader.doc_count() == 3
       reader.lookup("rust") returns postings for "a" and "c"
       reader.lookup("storag") returns postings for "b" and "c" (stemmed)
```

### Test 11 — `fts_builder_handles_invalid_utf8` (FT1.5)

```
Given: builder
When:  add_document(pk=b"bad", text with embedded \xFF bytes)
Then:  does NOT panic — skips invalid bytes or replaces with U+FFFD
       finish() succeeds
```

**Green path (if panics):**
- In `builder.rs`, wrap analyzer call in catch for invalid UTF-8
- Replace invalid sequences with `\u{FFFD}` before tokenizing
- File: `ferrosa-index/src/fulltext/builder.rs`, `add_document()`.

### Test 12 — `fts_term_lookup_exact_match` (FT1.6)

**File:** `ferrosa-index/src/fulltext/reader.rs`

```
Given: FTI bytes from builder with docs containing "rust", "database", "storage"
When:  reader.lookup("rust")
Then:  returns non-empty Vec<Posting> with correct partition keys
```

### Test 13 — `fts_term_lookup_missing` (FT1.6)

```
Given: same FTI bytes
When:  reader.lookup("nonexistent_term")
Then:  returns empty Vec
```

### Test 14 — `fts_file_format_roundtrip` (FT1.7)

```
Given: builder with 10 documents, each with 5-10 words
When:  bytes = builder.finish()
       reader = FullTextIndexReader::open(bytes)
Then:  reader.doc_count() == 10
       every term from every document is findable via lookup
       CRC32 checksum verified on open (no error)
```

### Test 15 — `fts_sidecar_checksum_verified` (FT1.7)

```
Given: valid FTI bytes
When:  corrupt 1 byte in the middle, then FullTextIndexReader::open()
Then:  returns Err (checksum mismatch), not silently wrong results
```

**Green path (if passes despite corruption):**
- `reader.rs` `open()` must read CRC32 from footer, compute CRC32 of payload, compare.
- If no CRC check exists, add one at the end of `open()`.

### Test 16 — `fts_query_parse_term` (FT2.1)

**File:** `ferrosa-index/src/fulltext/query.rs`

```
Given: query string "rust"
When:  parse_fts_query("rust")
Then:  returns FtsQuery::Term("rust")
```

### Test 17 — `fts_query_parse_phrase` (FT2.1)

```
Given: query string "\"distributed database\""
When:  parse_fts_query(...)
Then:  returns FtsQuery::Phrase(["distributed", "database"])
```

### Test 18 — `fts_query_parse_boolean` (FT2.1)

```
Given: query string "rust AND cassandra"
When:  parse_fts_query(...)
Then:  returns FtsQuery::And(Term("rust"), Term("cassandra"))
```

### Test 19 — `fts_query_boolean_precedence` (FT2.1)

```
Given: query string "a OR b AND c"
When:  parse_fts_query(...)
Then:  returns Or(Term("a"), And(Term("b"), Term("c")))
       (AND binds tighter than OR)
```

**Green path (if wrong precedence):**
- Query parser needs operator precedence: NOT > AND > OR
- File: `ferrosa-index/src/fulltext/query.rs`.

### Test 20 — `bm25_single_term` (FT2.2)

**File:** `ferrosa-index/src/fulltext/scoring.rs`

```
Given: corpus of 100 docs, term appears in 10, doc length = avgdl
When:  bm25_score(tf=3, df=10, dl=100, avgdl=100.0, n=100)
Then:  score > 0.0 (positive relevance)
       score is within expected BM25 range
```

### Test 21 — `bm25_multi_term_ranking` (FT2.2)

```
Given: FTI with 3 docs:
       doc A contains "rust" 5 times, "database" 1 time
       doc B contains "rust" 1 time, "database" 5 times
       doc C contains "rust" 3 times, "database" 3 times
When:  search("rust AND database")
Then:  doc C ranks highest (balanced frequency)
       all 3 docs returned with scores > 0
```

---

## Batch B: Integration Wiring (New Code Required)

### Test 22 — `table_store_knows_pin_mode` (NV1.2)

**File:** `ferrosa-storage/src/engine.rs` (test module)

```
Given: table created with extensions = {"storage.pin": "nvme"}
When:  engine.register_table(schema_with_nvme_extension)
Then:  engine.get_table_pin_config(&table_id).is_pinned() == true
```

```rust
#[test]
fn table_store_knows_pin_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();
    let mut schema = test_schema();
    schema.extensions.insert("storage.pin".into(), "nvme".into());
    engine.register_table(schema).unwrap();
    let tid = TableId::new("ks", "t");

    let pin_config = engine.get_table_pin_config(&tid);
    assert!(pin_config.is_pinned(), "table must know it is pinned after register_table");
}
```

**Green path (if test fails):**
- In `register_table()`, after creating the table state, parse extensions via
  `PinConfig::from_extensions(&schema.extensions)` and store in the table state struct.
- Add `get_table_pin_config(&self, tid) -> PinConfig` method to `StorageEngine`.
- File: `ferrosa-storage/src/engine.rs`, `register_table()`.

---

### Test 23 — `pinned_table_skips_s3_upload` (NV1.3)

**File:** `ferrosa-storage/src/engine.rs` (test module)

```
Given: engine with InMemory S3 store
       table registered with extensions = {"storage.pin": "nvme"}
       3 rows written + flushed
When:  sync_sstables_to_s3() called
Then:  InMemory store has zero SSTable objects
       local SSTable files exist on disk
```

```rust
#[tokio::test]
async fn pinned_table_skips_s3_upload() {
    let (engine, store, tid) = make_engine_with_s3_store().await;
    let mut schema = test_schema();
    schema.extensions.insert("storage.pin".into(), "nvme".into());
    engine.register_table(schema).unwrap();

    write_and_flush(&engine, &tid, 3).await;
    engine.sync_sstables_to_s3().await.unwrap();

    // S3 store must be empty — pinned tables skip upload
    let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert!(objects.is_empty(), "pinned table must not upload to S3");

    // Local files must exist
    let local_files = engine.local_sstable_files(&tid);
    assert!(!local_files.is_empty(), "local SSTable files must exist");
}
```

**Green path (if test fails):**
- In `sync_sstables_to_s3()`, before enqueuing `UploadTask::SSTable`:
  ```rust
  let pin_config = self.get_table_pin_config(&table_id);
  if pin_config.is_pinned() {
      // Pin in local cache (never evicted)
      self.local_cache.pin(&sstable_id);
      continue; // Skip S3 upload
  }
  ```
- File: `ferrosa-storage/src/engine.rs`, `sync_sstables_to_s3()`.

---

### Test 24 — `pinned_compaction_skips_s3` (NV1.4)

**File:** `ferrosa-storage/src/engine.rs` (test module)

```
Given: pinned table with 4 SSTables (triggers STCS compaction)
When:  poll_compactions() completes
Then:  compacted output SSTable is on local disk
       compacted output is NOT in S3 store
       output SSTable ID is in local cache pinned set
```

**Green path:**
- In `poll_compactions()`, check pin_config before Step 2 (upload to S3).
- If pinned: skip upload, skip manifest update, still do local swap + input cleanup.
- File: `ferrosa-storage/src/engine.rs`, `poll_compactions()`.

---

### Test 25 — `pinned_sstables_never_evicted` (NV1.5)

**File:** `ferrosa-storage/src/engine.rs` (test module)

```
Given: pinned table with 1 SSTable flushed
       LocalCache max_bytes set to 1 byte (forces eviction pressure)
When:  engine triggers eviction (via flush of another table that exceeds cache)
Then:  pinned SSTable's files still exist on disk
       pinned SSTable is still readable
```

```rust
#[test]
fn pinned_sstables_never_evicted() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.cache_max_bytes = 1; // Force extreme eviction pressure
    let engine = StorageEngine::new(config).unwrap();

    // Register pinned table and write+flush
    let mut pinned_schema = test_schema_named("ks", "pinned");
    pinned_schema.extensions.insert("storage.pin".into(), "nvme".into());
    engine.register_table(pinned_schema).unwrap();
    let pinned_tid = TableId::new("ks", "pinned");
    write_and_flush(&engine, &pinned_tid, 1);

    // Register normal table and write+flush (should trigger eviction)
    let normal_schema = test_schema_named("ks", "normal");
    engine.register_table(normal_schema).unwrap();
    let normal_tid = TableId::new("ks", "normal");
    write_and_flush(&engine, &normal_tid, 100);

    // Pinned table must still be readable
    let row = engine.read(&pinned_tid, &make_key("k0")).unwrap();
    assert!(row.is_some(), "pinned SSTable must survive eviction pressure");
}
```

**Green path (if test fails):**
- On flush of a pinned table, call `self.local_cache.pin(&sstable_id)` for each
  component file. The existing `evict_if_needed(&pinned)` already skips pinned entries.
- File: `ferrosa-storage/src/engine.rs`, flush path.

---

### Test 26 — `fts_sidecar_created_on_flush` (FT1.8)

**File:** `ferrosa-storage/src/engine.rs` (test module)

```
Given: table "ks.articles" with column "body" (text)
       fulltext index "idx_body" on "body"
       3 rows inserted: "rust distributed database", "cassandra storage", "hello world"
When:  engine.flush(&tid) called
Then:  file "{gen}-FTI-idx_body.db" exists alongside Data.db in SSTable dir
       FullTextIndexReader::open(read file) succeeds
       reader.doc_count() == 3
       reader.search_str("rust") returns partition key for row 1
```

```rust
#[test]
fn fts_sidecar_created_on_flush() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();

    let schema = test_schema_with_text_column("ks", "articles", "body");
    engine.register_table(schema).unwrap();
    let tid = TableId::new("ks", "articles");

    engine.create_index(IndexMetadata {
        keyspace: "ks".into(), table: "articles".into(),
        name: "idx_body".into(),
        index_type: IndexType::FullText,
        target_columns: vec!["body".into()],
        filter_predicate: None,
        options: HashMap::new(),
    }).unwrap();

    // Insert 3 rows with text
    insert_text_row(&engine, &tid, "r1", "rust distributed database");
    insert_text_row(&engine, &tid, "r2", "cassandra compatible storage");
    insert_text_row(&engine, &tid, "r3", "hello world");
    engine.flush(&tid).unwrap();

    // FTI sidecar must exist
    let sstable_dir = engine.sstable_dir(&tid);
    let fti_files: Vec<_> = std::fs::read_dir(&sstable_dir).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("FTI-idx_body"))
        .collect();
    assert_eq!(fti_files.len(), 1, "exactly one FTI sidecar file must exist");

    // Verify contents
    let fti_bytes = std::fs::read(fti_files[0].path()).unwrap();
    let reader = FullTextIndexReader::open(fti_bytes).unwrap();
    assert_eq!(reader.doc_count(), 3);

    let hits = reader.search_str("rust").unwrap();
    assert!(!hits.is_empty(), "search for 'rust' must return results");
}
```

**Green path (if test fails):**
- In `flush()`, after writing SSTable components, check if any FullText indexes
  exist for this table.
- For each FullText index: iterate memtable rows, extract target column text,
  feed to `FullTextIndexBuilder`, write `{gen}-FTI-{index_name}.db`.
- File: `ferrosa-storage/src/engine.rs`, flush path (after SSTable write, before commit-log truncation).

---

### Test 27 — `fts_match_simple_query` (FT2.3)

**File:** `ferrosa-cql/src/router.rs` or `ferrosa-storage/src/engine.rs` (test module)

```
Given: table with fulltext index on "body"
       3 rows flushed, one contains "rust distributed"
When:  engine.fulltext_search(&tid, "idx_body", "rust")
Then:  returns vec with partition key for the "rust distributed" row
```

**Expected:** GREEN — `engine.fulltext_search()` already implemented. But needs FTI sidecar
to exist on disk (depends on Test 26 green path).

### Test 28 — `fts_match_boolean_query` (FT2.3)

```
Given: same 3 rows
When:  engine.fulltext_search(&tid, "idx_body", "rust AND database")
Then:  returns only rows containing BOTH "rust" AND "database"
       row with just "cassandra compatible storage" excluded
```

---

### Test 29 — `fts_wildcard_expansion_capped` (FT2.4)

**File:** `ferrosa-index/src/fulltext/query.rs` or `reader.rs`

```
Given: FTI with 20,000 unique terms starting with "a"
When:  search("a*") — prefix wildcard
Then:  expansion capped at 10,000 terms (configurable)
       search completes without OOM
       result is approximate (top-K terms by doc frequency)
```

```rust
#[test]
fn fts_wildcard_expansion_capped() {
    let mut builder = FullTextIndexBuilder::new();
    // Insert docs with 20,000 unique "a..." terms
    for i in 0..20_000 {
        builder.add_document(
            format!("pk{i}").into_bytes(),
            &format!("a{i:05} some other words"),
        );
    }
    let bytes = builder.finish().unwrap();
    let reader = FullTextIndexReader::open(bytes).unwrap();

    // Prefix query "a*" must not expand to 20K terms
    let hits = reader.search_str("a*").unwrap();
    // Should return results (not error) but expansion is bounded
    assert!(hits.len() <= 10_000, "wildcard expansion must be capped");
}
```

**Green path (if OOMs or returns 20K results):**
- In `reader.rs` `search()`, when processing `FtsQuery::Prefix(p)`:
  - Scan terms dict for matching prefix
  - If matching terms > MAX_WILDCARD_EXPANSION (10,000), truncate to top-K by doc_frequency
  - Return error if prefix is empty string or "*" alone
- File: `ferrosa-index/src/fulltext/reader.rs`.

### Test 30 — `fts_wildcard_bare_star_rejected` (FT2.4)

```
Given: any FTI reader
When:  search_str("*")
Then:  returns Err — bare star wildcard rejected (would expand to all terms)
```

**Green path:** Add check at top of `search()`: if query is `Prefix("")`, return error.

---

## Batch C: Compound Integration

### Test 31 — `pinned_table_respects_max_bytes` (NV1.6)

**File:** `ferrosa-storage/src/engine.rs`

```
Given: table with extensions = {"storage.pin": "nvme", "storage.pin_max_bytes": "1024"}
When:  write enough data to exceed 1024 bytes (many flushes)
Then:  oldest pinned SSTables evicted from local cache when total exceeds cap
       total pinned bytes <= 1024 after eviction
       newest SSTables retained (FIFO eviction of oldest)
```

```rust
#[test]
fn pinned_table_respects_max_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();

    let mut schema = test_schema();
    schema.extensions.insert("storage.pin".into(), "nvme".into());
    schema.extensions.insert("storage.pin_max_bytes".into(), "1024".into());
    engine.register_table(schema).unwrap();
    let tid = TableId::new("ks", "t");

    // Write many rows across multiple flushes to exceed 1024 bytes
    for i in 0..20 {
        write_large_row(&engine, &tid, &format!("k{i}"), 200); // ~200 bytes each
        engine.flush(&tid).unwrap();
    }

    // Total pinned bytes must respect cap
    let pinned_bytes = engine.pinned_bytes(&tid);
    assert!(pinned_bytes <= 1024, "pinned bytes {pinned_bytes} must be <= 1024");

    // Most recent data must still be readable
    let recent = engine.read(&tid, &make_key("k19")).unwrap();
    assert!(recent.is_some(), "most recent row must survive");
}
```

**Green path:**
- After flush of pinned table, compute total pinned bytes for that table.
- If total > max_bytes, evict oldest pinned SSTables (by generation number):
  unpin from cache, delete local files.
- File: `ferrosa-storage/src/engine.rs`, post-flush for pinned tables.

---

### Test 32 — `unpin_resumes_s3_upload` (NV1.7)

**File:** `ferrosa-storage/src/engine.rs`

```
Given: pinned table with 2 SSTables (not in S3)
When:  ALTER TABLE extensions = {"storage.pin": "none"}
       then sync_sstables_to_s3()
Then:  both SSTables uploaded to S3
       SSTable IDs removed from pinned set
       future flushes also upload to S3
```

```rust
#[tokio::test]
async fn unpin_resumes_s3_upload() {
    let (engine, store, tid) = make_pinned_engine_with_2_sstables().await;

    // Verify nothing in S3 before unpin
    let before = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    assert!(before.is_empty(), "pinned table must not be in S3");

    // Unpin: change extensions
    engine.alter_table_extensions(&tid, hashmap!{"storage.pin" => "none"}).await.unwrap();
    engine.sync_sstables_to_s3().await.unwrap();

    // Now S3 must have the SSTables
    let after = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    assert!(!after.is_empty(), "unpinned table must upload existing SSTables to S3");
}
```

**Green path:**
- `alter_table_extensions()` must:
  1. Update schema registry extensions
  2. If old mode was NvMe and new is None: unpin all SSTable IDs, enqueue S3 uploads
  3. If old mode was None and new is NvMe: pin all SSTable IDs, cancel pending uploads
- File: `ferrosa-storage/src/engine.rs`.

---

### Test 33 — `pin_stops_s3_upload` (NV1.7)

```
Given: normal table with 2 SSTables already in S3
When:  ALTER TABLE extensions = {"storage.pin": "nvme"}
       then write + flush + sync_sstables_to_s3()
Then:  new SSTable NOT uploaded to S3
       new SSTable ID is in pinned set
       old SSTables remain in S3 (not deleted)
```

---

### Test 34 — `pin_config_survives_restart` (NV1.8)

**File:** `ferrosa-storage/src/engine.rs`

```
Given: pinned table created, 1 row written + flushed
When:  engine dropped (shutdown), new engine created at same data_dir
Then:  get_table_pin_config(&tid).is_pinned() == true
       SSTable still on local disk, not uploaded to S3
```

```rust
#[tokio::test]
async fn pin_config_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();
        let mut schema = test_schema();
        schema.extensions.insert("storage.pin".into(), "nvme".into());
        engine.register_table(schema).unwrap();
        let tid = TableId::new("ks", "t");
        write_and_flush(&engine, &tid, 1);
        // engine dropped here
    }
    let engine2 = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();
    let tid = TableId::new("ks", "t");

    let pin_config = engine2.get_table_pin_config(&tid);
    assert!(pin_config.is_pinned(), "pin config must survive restart");

    // Data readable without S3
    let row = engine2.read(&tid, &make_key("k0")).unwrap();
    assert!(row.is_some(), "pinned data must survive restart");
}
```

**Green path:** Pin config is stored in table extensions which are part of schema.json.
Schema persistence across restart (BUG-022, already fixed) should carry extensions.
If test fails, verify `load_local_schema` preserves `extensions` HashMap.

---

### Test 35 — `fts_compaction_merge_deduplicates` (FT2.5)

**File:** `ferrosa-index/src/fulltext/merge.rs` (test module) or `ferrosa-storage/src/engine.rs`

```
Given: two FTI files:
       FTI-A: doc "pk1" has term "rust" (tf=2), doc "pk2" has term "rust" (tf=1)
       FTI-B: doc "pk1" has term "rust" (tf=3) — same pk, updated doc
When:  merge_fti_indexes(fti_a_bytes, fti_b_bytes)
Then:  merged FTI has doc_count = 2 (not 3 — pk1 deduplicated)
       "rust" posting for pk1 has tf=3 (latest value wins, or sum — per design)
```

```rust
#[test]
fn fts_compaction_merge_deduplicates() {
    let mut builder_a = FullTextIndexBuilder::new();
    builder_a.add_document(b"pk1".to_vec(), "rust rust");
    builder_a.add_document(b"pk2".to_vec(), "rust");
    let bytes_a = builder_a.finish().unwrap();

    let mut builder_b = FullTextIndexBuilder::new();
    builder_b.add_document(b"pk1".to_vec(), "rust rust rust");
    let bytes_b = builder_b.finish().unwrap();

    let merged = merge_fti_indexes(&bytes_a, &bytes_b).unwrap();
    let reader = FullTextIndexReader::open(merged).unwrap();

    // pk1 should not appear twice
    assert_eq!(reader.doc_count(), 2);
    let postings = reader.lookup("rust");
    let pk1_postings: Vec<_> = postings.iter()
        .filter(|p| p.partition_key == b"pk1")
        .collect();
    assert_eq!(pk1_postings.len(), 1, "pk1 must appear exactly once after merge");
}
```

**Green path:** `merge.rs` exists but may not deduplicate by partition key.
Fix: during merge, when same pk appears in both sides, keep the one from the
newer SSTable (FTI-B, which is the compaction output side).

### Test 36 — `fts_compaction_merge_preserves_scores` (FT2.5)

```
Given: two FTI files merged
When:  search merged index for "rust"
Then:  BM25 scores are recalculated correctly with merged doc_count and avgdl
       ranking order matches expected relevance
```

---

### Test 37 — `fts_sidecar_uploaded_to_s3` (FT2.6)

**File:** `ferrosa-storage/src/engine.rs`

```
Given: engine with InMemory S3 store, table with fulltext index
When:  write rows + flush + sync_sstables_to_s3()
Then:  S3 store contains "{gen}-FTI-idx_body.db" alongside Data.db
       FTI file roundtrips through S3 (download + open succeeds)
```

```rust
#[tokio::test]
async fn fts_sidecar_uploaded_to_s3() {
    let (engine, store, tid) = make_engine_with_s3_and_fts_index().await;
    insert_text_row(&engine, &tid, "r1", "rust distributed database");
    engine.flush(&tid).unwrap();
    engine.sync_sstables_to_s3().await.unwrap();

    // Check S3 store for FTI file
    let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
    let fti_objects: Vec<_> = objects.iter()
        .filter(|o| o.location.to_string().contains("FTI-"))
        .collect();
    assert_eq!(fti_objects.len(), 1, "FTI sidecar must be uploaded to S3");
}
```

**Green path:** `sync_sstables_to_s3` already uploads all `.db` files in the SSTable
directory. If FTI sidecar uses `.db` extension and lives in the same directory,
it should be included automatically. If not, add FTI files to the component list
scanned during upload.

---

## Batch D: End-to-End + Observability

### Test 38 — `pinned_metrics_accurate` (NV1.9)

**File:** `ferrosa-storage/src/engine.rs`

```
Given: 2 pinned tables (one with 3 SSTables, one with 1)
When:  metrics scraped
Then:  ferrosa_storage_pinned_tables gauge == 2
       ferrosa_storage_pinned_bytes gauge == sum of all pinned SSTable sizes
       ferrosa_storage_pin_evictions_total counter == 0 (no evictions yet)
```

```rust
#[test]
fn pinned_metrics_accurate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();

    // Register 2 pinned tables
    for table in ["t1", "t2"] {
        let mut schema = test_schema_named("ks", table);
        schema.extensions.insert("storage.pin".into(), "nvme".into());
        engine.register_table(schema).unwrap();
    }
    let tid1 = TableId::new("ks", "t1");
    let tid2 = TableId::new("ks", "t2");

    // Write and flush
    for _ in 0..3 { write_and_flush(&engine, &tid1, 1); }
    write_and_flush(&engine, &tid2, 1);

    assert_eq!(engine.metrics.pinned_tables.get(), 2);
    assert!(engine.metrics.pinned_bytes.get() > 0);
    assert_eq!(engine.metrics.pin_evictions_total.get(), 0);
}
```

**Green path:**
- Add `pinned_tables` (gauge), `pinned_bytes` (gauge), `pin_evictions_total` (counter)
  to the engine metrics struct.
- Update on register_table (pinned_tables++), flush (pinned_bytes += size),
  eviction (pin_evictions_total++, pinned_bytes -= size).

---

### Test 39 — `fts_end_to_end_insert_query` (FT2.7)

**File:** `ferrosa-storage/src/engine.rs` (test module)

```
Given: engine with table "ks.articles" (pk text, body text)
       fulltext index "idx_body" on "body" column
When:  INSERT 5 rows with different text content
       flush()
       engine.fulltext_search(&tid, "idx_body", "distributed AND database")
Then:  returns partition keys of rows containing both "distributed" and "database"
       rows without those terms are NOT returned
       results are ranked by BM25 score (most relevant first)
```

```rust
#[test]
fn fts_end_to_end_insert_query() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();

    let schema = test_schema_with_text_column("ks", "articles", "body");
    engine.register_table(schema).unwrap();
    let tid = TableId::new("ks", "articles");

    engine.create_index(IndexMetadata {
        keyspace: "ks".into(), table: "articles".into(),
        name: "idx_body".into(),
        index_type: IndexType::FullText,
        target_columns: vec!["body".into()],
        filter_predicate: None,
        options: HashMap::new(),
    }).unwrap();

    // Insert 5 rows
    insert_text_row(&engine, &tid, "r1", "rust is a fast distributed database language");
    insert_text_row(&engine, &tid, "r2", "cassandra is a distributed database");
    insert_text_row(&engine, &tid, "r3", "hello world");
    insert_text_row(&engine, &tid, "r4", "distributed systems are complex");
    insert_text_row(&engine, &tid, "r5", "database normalization theory");
    engine.flush(&tid).unwrap();

    // Query: must find rows with both "distributed" AND "database"
    let results = engine.fulltext_search(&tid, "idx_body", "distributed AND database").unwrap();
    let result_keys: Vec<String> = results.iter()
        .map(|pk| String::from_utf8_lossy(pk).to_string())
        .collect();

    assert!(result_keys.contains(&"r1".to_string()), "r1 has both terms");
    assert!(result_keys.contains(&"r2".to_string()), "r2 has both terms");
    assert!(!result_keys.contains(&"r3".to_string()), "r3 has neither term");
    assert!(!result_keys.contains(&"r4".to_string()), "r4 has only 'distributed'");
    assert!(!result_keys.contains(&"r5".to_string()), "r5 has only 'database'");
}
```

**Green path:** This is the capstone test. If Tests 26 (sidecar on flush) and 27
(fts_match search) both pass, this test should pass. If it fails, trace through:
1. Is the FTI file being created on flush? (check disk)
2. Is `fulltext_search` finding the FTI file? (check glob pattern)
3. Is the query parser handling AND correctly? (check query.rs)

---

### Test 40 — `fts_custom_stop_words` (FT2.8)

**File:** `ferrosa-index/src/fulltext/analyzer.rs`

```
Given: StandardAnalyzer with custom stop_words = ["rust", "database"]
When:  analyze("rust is a fast distributed database")
Then:  tokens do NOT contain "rust" or "database" (custom stop words)
       tokens DO contain "fast", "distribut" (stemmed)
```

```rust
#[test]
fn fts_custom_stop_words() {
    let custom_stops: HashSet<String> = ["rust", "database"].iter().map(|s| s.to_string()).collect();
    let analyzer = StandardAnalyzer::with_stop_words(custom_stops);
    let tokens = analyzer.analyze("rust is a fast distributed database");
    let terms: Vec<&str> = tokens.iter().map(|t| t.text.as_ref()).collect();

    assert!(!terms.contains(&"rust"), "custom stop word 'rust' must be removed");
    assert!(!terms.contains(&"database"), "custom stop word 'database' must be removed");
    assert!(terms.iter().any(|t| t.starts_with("fast")), "'fast' must be present");
    assert!(terms.iter().any(|t| t.starts_with("distribut")), "'distribut' (stemmed) must be present");
}
```

**Green path:** Add `StandardAnalyzer::with_stop_words(custom: HashSet<String>)` constructor.
Replace default English stop words with the custom set.

### Test 41 — `fts_language_analyzer` (FT2.8)

```
Given: LanguageAnalyzer("english") with English stemmer + stop words
When:  analyze("the running dogs were chasing cats")
Then:  "the" and "were" removed (English stop words)
       "running" -> "run", "dogs" -> "dog", "chasing" -> "chase", "cats" -> "cat"
```

**Green path:** Create `LanguageAnalyzer` struct that selects stemmer + stop word list
based on language parameter. Start with English only; return error for unsupported languages.

---

## Implementation Notes

### Red -> Green rule
Write EXACTLY ONE test. Run `cargo test [test_name]`. It must fail (red).
Then write the minimum code to make it pass (green). Then refactor.
Do not write code for tests that don't exist yet.

### Starting point
Batch A tests verify existing code and should mostly pass immediately. Start with them
to establish confidence, then move to Batch B.

The optimal first NEW test (Batch B) is **Test 22 (`table_store_knows_pin_mode`)** — it
forces `register_table` to parse and store pin config, which is the prerequisite for all
other NV1 tests.

Second: **Test 26 (`fts_sidecar_created_on_flush`)** — the critical FTS integration point.
All other FT2 tests depend on FTI sidecars existing after flush.

### Dependency graph

```
NV1 path:
  Test 1-2 (pin_config) -> Test 22 (register_table) -> Test 23 (skip S3)
                                                     -> Test 24 (skip S3 compaction)
                                                     -> Test 25 (never evicted)
  Test 23 + 25 -> Test 31 (max_bytes)
  Test 23 + 25 -> Test 32-33 (ALTER TABLE)
  Test 22 -> Test 34 (restart)
  Test 31 -> Test 38 (metrics)

FTS path:
  Test 4-15 (verify core) -> Test 26 (sidecar on flush) -> Test 27-28 (fts_match)
  Test 26 -> Test 37 (S3 upload)
  Test 9 -> Test 35-36 (compaction merge)
  Test 29-30 (wildcard cap) — independent of flush
  Test 26 + 27 -> Test 39 (end-to-end)
  Test 4 -> Test 40-41 (language analyzers)
```

### Files changed summary

| Sprint | File | Change |
|--------|------|--------|
| NV1.2 | `ferrosa-storage/src/engine.rs` | `register_table()` stores PinConfig; add `get_table_pin_config()` |
| NV1.3 | `ferrosa-storage/src/engine.rs` | `sync_sstables_to_s3()` checks pin_config, skips if pinned |
| NV1.4 | `ferrosa-storage/src/engine.rs` | `poll_compactions()` checks pin_config, skips S3 if pinned |
| NV1.5 | `ferrosa-storage/src/engine.rs` | flush path calls `local_cache.pin()` for pinned tables |
| NV1.6 | `ferrosa-storage/src/engine.rs` | Post-flush: enforce max_bytes, evict oldest pinned SSTables |
| NV1.7 | `ferrosa-storage/src/engine.rs` | `alter_table_extensions()` handles pin/unpin transitions |
| NV1.9 | `ferrosa-storage/src/engine.rs` | Add pinned_tables, pinned_bytes, pin_evictions_total metrics |
| FT1.8 | `ferrosa-storage/src/engine.rs` | flush path builds FTI sidecar for FullText indexes |
| FT2.4 | `ferrosa-index/src/fulltext/reader.rs` | Prefix expansion cap in `search()` |
| FT2.5 | `ferrosa-storage/src/engine.rs` | `poll_compactions()` merges FTI sidecars |
| FT2.8 | `ferrosa-index/src/fulltext/analyzer.rs` | `with_stop_words()`, `LanguageAnalyzer` |

### CI order
1. `cargo test` — Batch A (verify existing) + Batch B-D unit tests
2. `FERROSA_TEST_CONTAINERS=1 cargo test` — integration tests requiring Docker/S3
