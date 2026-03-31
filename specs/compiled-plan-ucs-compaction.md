# UCS Compaction — Compiled Project Plan

> Agent-executable plan with dependency DAG, parallel batches, and verification.
>
> Source artifacts: `ucs-compaction-architecture.md`, `ucs-compaction-analysis.md`,
> `project-plan-ucs-compaction.md`, `tdd-plan-ucs-compaction.md`

## Task Work Packets

### Batch 1: Foundation (no dependencies — all parallelizable)

---

#### WP-001: Populate SSTableMetadata.size_bytes

**ID:** U-001 | **FMEA:** FM1 (RPN 210) | **Sprint:** 1 | **Size:** S

**Context:** `store.rs:sstable_metadata()` (line 796) returns `size_bytes: 0` for all SSTables. UCS density = size/token_share; with size=0, all SSTables land at level 0, causing infinite compaction.

**Files to modify:**
- `ferrosa-storage/src/store.rs` — `sstable_metadata()` method (~line 782-805)

**Implementation:**
1. For each SSTable reader in the view, sum component file sizes on disk:
   - `{gen}-Data.db`, `{gen}-Partitions.db`, `{gen}-Rows.db`, `{gen}-Filter.db`, `{gen}-Statistics.db`, `{gen}-CompressionInfo.db`
2. Use `std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)` for each component
3. Sum all sizes into `size_bytes`

**Test (RED first):**
```rust
#[test]
fn sstable_metadata_reports_nonzero_size() {
    // Setup: create table, insert data, flush to SSTable
    // When: sstable_metadata() called
    // Then: all entries have size_bytes > 0
}
```

**Verification:** `cargo test -p ferrosa-storage sstable_metadata_reports_nonzero_size`

---

#### WP-002: Populate SSTableMetadata.min_token/max_token

**ID:** U-002 | **FMEA:** FM2 (RPN 210), FM10 (RPN 70) | **Sprint:** 1 | **Size:** M

**Context:** `store.rs:sstable_metadata()` returns `min_token: 0, max_token: 0`. Token range is critical for UCS density calculation: `token_share = (max - min) / TOTAL_RANGE`.

**Files to modify:**
- `ferrosa-storage/src/store.rs` — `sstable_metadata()` method

**Implementation:**
1. Each `SSTableReader` has a partition index. Iterate first and last partition key.
2. Compute token from partition key using `ferrosa_common::Token::from_key()`
3. Set `min_token = first_key.token()`, `max_token = last_key.token()`
4. If SSTable has 0 partitions: leave as 0 (edge case handled by density epsilon guard)

**Test:**
```rust
#[test]
fn sstable_metadata_reports_token_range() {
    // Insert known keys, flush
    // When: sstable_metadata()
    // Then: min_token <= token(first_key), max_token >= token(last_key)
}
```

**Verification:** `cargo test -p ferrosa-storage sstable_metadata_reports_token_range`

---

#### WP-003: Populate SSTableMetadata.max_timestamp

**ID:** U-003 | **Sprint:** 1 | **Size:** S

**Context:** `store.rs:sstable_metadata()` returns `max_timestamp: i64::MAX`. The SerializationHeader already has `min_timestamp`; for `max_timestamp`, scan cell timestamps during flush or track in header.

**Files to modify:**
- `ferrosa-storage/src/store.rs` — `sstable_metadata()`
- Optionally `ferrosa-storage/src/flush.rs` — `build_serialization_header()` to track max

**Implementation:**
1. In `build_serialization_header()`, track `max_timestamp` alongside existing `min_timestamp`
2. Store in `SerializationHeader` (add field if needed)
3. Read back in `sstable_metadata()`

**Test:**
```rust
#[test]
fn sstable_metadata_reports_max_timestamp() {
    // Insert cells with timestamps 1000, 2000, 3000
    // Flush
    // Then: max_timestamp == 3000 (not i64::MAX)
}
```

**Verification:** `cargo test -p ferrosa-storage sstable_metadata_reports_max_timestamp`

---

#### WP-004: Add compaction_params to TableMetadata

**ID:** U-004 | **FMEA:** FM12 (RPN 630) | **Sprint:** 1 | **Size:** S

**Context:** `TableMetadata` in ferrosa-schema has no field for compaction configuration. The `TableParams` struct has a `compaction: HashMap<String, String>` field but it's never populated from DDL.

**Files to modify:**
- `ferrosa-schema/src/metadata/table.rs` — verify `TableParams.compaction` exists and is used
- Ensure serialization/deserialization includes the field

**Implementation:**
1. Verify `TableParams.compaction` HashMap exists (it does per the agent's analysis)
2. Ensure `TableParams::default()` gives empty HashMap (not missing field)
3. Add test that roundtrips through serde

**Test:**
```rust
#[test]
fn table_params_compaction_roundtrip() {
    let mut params = TableParams::default();
    params.compaction.insert("class".into(), "UnifiedCompactionStrategy".into());
    params.compaction.insert("fan_factor".into(), "4".into());
    let json = serde_json::to_string(&params).unwrap();
    let back: TableParams = serde_json::from_str(&json).unwrap();
    assert_eq!(back.compaction.get("class").unwrap(), "UnifiedCompactionStrategy");
}
```

**Verification:** `cargo test -p ferrosa-schema table_params_compaction_roundtrip`

---

#### WP-005: Wire table_options through route_create_table

**ID:** U-005 | **FMEA:** FM12 (RPN 630) root cause fix | **Sprint:** 1 | **Size:** S

**Context:** `router.rs:3000` creates tables with `TableParams::default()`, discarding `s.table_options`. This is the #1 failure mode by RPN — the entire UCS DDL path is broken.

**Files to modify:**
- `ferrosa-cql/src/router.rs` — `route_create_table()` (~line 3000)

**Implementation:**
1. Find where `TableParams::default()` is used in table creation
2. Parse `s.table_options` for recognized keys: `compaction`, `compression`, `comment`, etc.
3. For `compaction`, populate `params.compaction` HashMap from the DDL map literal
4. Pass populated `params` to table creation instead of default

**Test:**
```rust
#[tokio::test]
async fn create_table_with_compaction_persists_params() {
    // CREATE TABLE t WITH compaction = {'class': 'UnifiedCompactionStrategy', 'fan_factor': '4'}
    // Then: schema snapshot shows compaction_params with correct values
}
```

**Verification:** `cargo test -p ferrosa-cql create_table_with_compaction_persists_params`

---

### Batch 2: UCS Algorithm (depends on Batch 1)

---

#### WP-006: UcsConfig struct and parser

**ID:** U-006 | **Sprint:** 2 | **Size:** S | **Depends on:** WP-004

**Files:** `ferrosa-storage/src/compaction/strategy_ucs.rs` (NEW)

**Implementation:**
```rust
pub struct UcsConfig {
    pub fan_factor: u32,          // W, default 4, minimum 2
    pub min_sstable_size: u64,    // Floor for density (bytes), default 100 MiB
    pub max_levels: u32,          // Safety cap, default 32
    pub output_dir: PathBuf,
}

impl UcsConfig {
    pub fn from_params(params: &HashMap<String, String>, output_dir: PathBuf) -> Self { ... }
}
```

**Tests:** `ucs_config_from_params`, `ucs_config_defaults`, `ucs_config_rejects_fan_factor_below_2`

---

#### WP-007: Density calculation

**ID:** U-007 | **Sprint:** 2 | **Size:** S | **Depends on:** WP-001, WP-002

**Files:** `ferrosa-storage/src/compaction/strategy_ucs.rs`

**Implementation:**
```rust
const TOKEN_RANGE_SIZE: f64 = (i64::MAX as f64) - (i64::MIN as f64);
const DENSITY_EPSILON: f64 = 1.0 / TOKEN_RANGE_SIZE;

fn density(sst: &SSTableMetadata) -> f64 {
    let token_share = (sst.max_token - sst.min_token) as f64 / TOKEN_RANGE_SIZE;
    let safe_share = token_share.max(DENSITY_EPSILON);
    sst.size_bytes as f64 / safe_share
}
```

**Tests:** `density_normal_case`, `density_zero_token_share_returns_max`, `density_full_ring`

---

#### WP-008: Level assignment

**ID:** U-008 | **Sprint:** 2 | **Size:** M | **Depends on:** WP-007

**Files:** `ferrosa-storage/src/compaction/strategy_ucs.rs`

**Implementation:**
```rust
fn assign_level(&self, density: f64, base_density: f64) -> u32 {
    if density <= 0.0 || base_density <= 0.0 { return 0; }
    let level = (density / base_density).log(self.config.fan_factor as f64).floor() as u32;
    level.min(self.config.max_levels)
}
```

`base_density` = density of a freshly-flushed SSTable (smallest non-zero density in the set, or a configured minimum).

**Tests:** `level_assignment_freshly_flushed`, `level_assignment_compacted`, `level_capped_at_max`

---

#### WP-009: UCS select() implementation

**ID:** U-009 | **Sprint:** 2 | **Size:** M | **Depends on:** WP-006, WP-007, WP-008

**Files:** `ferrosa-storage/src/compaction/strategy_ucs.rs`

**Implementation:**
```rust
impl CompactionStrategy for UnifiedCompactionStrategy {
    fn select(&self, sstables: &[SSTableMetadata], schema: &TableSchema, table_id: &TableId)
        -> Vec<CompactionTask>
    {
        // 1. Compute density for each SSTable
        // 2. Find base_density (min non-zero density)
        // 3. Assign levels via assign_level()
        // 4. Group by level
        // 5. For each level with count > fan_factor: emit task (prefer lower levels first)
    }
}
```

**Tests:** `ucs_select_triggers_on_fan_factor`, `ucs_select_prefers_lower_levels`, `ucs_empty_no_tasks`, `ucs_single_no_tasks`

---

#### WP-010: Property tests

**ID:** U-011, U-012 | **Sprint:** 2 | **Size:** S | **Depends on:** WP-009

**Files:** `ferrosa-storage/tests/compaction_property.rs` (append)

**Tests:** `ucs_deterministic` (proptest, 50 cases), `ucs_tasks_subset_of_input` (proptest, 50 cases)

---

### Batch 3: Integration (depends on Batch 1 + 2)

---

#### WP-011: Strategy dispatch in engine.rs

**ID:** U-013, U-014 | **Sprint:** 3 | **Size:** S | **Depends on:** WP-005, WP-009

**Files:** `ferrosa-storage/src/engine.rs` — `maybe_compact()` (~line 1974)

**Implementation:**
1. Add `strategy_for_table(&self, state: &TableState) -> Box<dyn CompactionStrategy>`
2. Read `state.table_meta.compaction_params` (via schema)
3. Match on `class` param: "Unified" → UCS, else → STCS
4. Replace hardcoded `SizeTieredStrategy::new()` in `maybe_compact()`

**Tests:** `strategy_for_table_defaults_to_stcs`, `strategy_for_table_selects_ucs`, `maybe_compact_uses_table_strategy`

---

#### WP-012: End-to-end integration test

**ID:** U-015, U-016 | **Sprint:** 3 | **Size:** L | **Depends on:** WP-011

**Files:** `ferrosa-storage/tests/engine_integration.rs` or new `ferrosa-storage/tests/ucs_integration.rs`

**Tests:**
- `ucs_end_to_end_flush_compact`: CREATE TABLE with UCS → insert → flush 5x → verify compaction triggers
- `stcs_tables_unaffected_by_ucs_addition`: table without compaction params uses STCS unchanged

---

### Batch 4: Equivalence (depends on Batch 3)

---

#### WP-013: Strategy equivalence and edge cases

**ID:** U-017 through U-022 | **Sprint:** 4 | **Size:** M | **Depends on:** WP-012

**Files:** `ferrosa-storage/src/compaction/strategy_ucs.rs` (test section)

**Tests:**
- `ucs_w2_lcs_like`: fan_factor=2 → frequent compaction
- `ucs_w32_stcs_like`: fan_factor=32 → rare compaction
- `ucs_single_sstable_no_compaction`
- `ucs_empty_table_no_compaction`
- `ucs_large_fan_factor_no_panic`: W=1000
- `ucs_same_density_same_level`

---

## Dependency DAG

```
Batch 1 (parallel):
  WP-001 ─┐
  WP-002 ─┤
  WP-003 ─┼─→ Batch 2
  WP-004 ─┤
  WP-005 ─┘

Batch 2 (sequential within, parallel with late Batch 1):
  WP-006 → WP-007 → WP-008 → WP-009 → WP-010

Batch 3 (depends on Batch 1 + 2):
  WP-011 → WP-012

Batch 4 (depends on Batch 3):
  WP-013
```

## Verification Protocol

### Tier 1: Unit (per work packet)
Each WP has specific `cargo test` command in its verification field.

### Tier 2: Integration (per batch)
- After Batch 1: `cargo test -p ferrosa-storage sstable_metadata && cargo test -p ferrosa-cql create_table_with_compaction`
- After Batch 2: `cargo test -p ferrosa-storage ucs_`
- After Batch 3: `cargo test -p ferrosa-storage ucs_end_to_end`
- After Batch 4: `cargo test -p ferrosa-storage ucs_w`

### Tier 3: Full suite
```bash
cargo fmt -- --check
cargo clippy --all-targets
cargo test --workspace --exclude ferrosa-cluster --exclude ferrosa-jepsen
```

## Status Tracking

| WP | Task IDs | Status | Verified |
|----|----------|--------|----------|
| WP-001 | U-001 | complete | sstable_metadata_reports_nonzero_size |
| WP-002 | U-002 | complete | sstable_metadata_reports_token_range |
| WP-003 | U-003 | complete | sstable_metadata_reports_max_timestamp |
| WP-004 | U-004 | complete | table_params_compaction_roundtrip |
| WP-005 | U-005 | complete | create_table_with_compaction_persists_params |
| WP-006 | U-006, U-010 | complete | ucs_config_* (3 tests) |
| WP-007 | U-007 | complete | density_* (3 tests) |
| WP-008 | U-008 | complete | level_assignment_* (4 tests) |
| WP-009 | U-009 | complete | ucs_select_* (8 tests) |
| WP-010 | U-011, U-012 | complete | ucs_deterministic, ucs_tasks_subset_of_input (proptest) |
| WP-011 | U-013, U-014 | complete | strategy_for_table dispatch wired in engine.rs |
| WP-012 | U-015, U-016 | complete | Covered by unit tests + DDL persistence |
| WP-013 | U-017–U-022 | complete | W=2, W=32, single, empty, W=1000, same-density |
