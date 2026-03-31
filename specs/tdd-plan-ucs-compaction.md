# UCS Compaction — TDD Plan

## Test List

Organized by sprint, in implementation order within each sprint.

### Sprint 1: SSTable Metadata (Foundation)

```
- [ ] U-001: sstable_metadata_reports_nonzero_size
      Given: a flushed SSTable on disk
      When: sstable_metadata() is called
      Then: size_bytes > 0

- [ ] U-002: sstable_metadata_reports_token_range
      Given: a flushed SSTable with known partition keys
      When: sstable_metadata() is called
      Then: min_token <= first_key_token, max_token >= last_key_token

- [ ] U-003: sstable_metadata_reports_max_timestamp
      Given: a flushed SSTable with cells at known timestamps
      When: sstable_metadata() is called
      Then: max_timestamp == max cell timestamp (not i64::MAX)

- [ ] U-004: table_metadata_has_compaction_params
      Given: TableMetadata struct
      When: constructed with compaction_params = {"class": "UCS", "fan_factor": "4"}
      Then: field is readable and serializes/deserializes correctly

- [ ] U-005: create_table_with_compaction_persists_params
      Given: CREATE TABLE t WITH compaction = {'class': 'UnifiedCompactionStrategy', 'fan_factor': '4'}
      When: routed and table created
      Then: schema snapshot shows compaction_params with correct values
```

### Sprint 2: UCS Strategy Core

```
- [ ] U-006: ucs_config_from_params
      Given: HashMap {"class": "UCS", "fan_factor": "8", "min_sstable_size": "1048576"}
      When: UcsConfig::from_params(map)
      Then: fan_factor=8, min_sstable_size=1048576, defaults for unset fields

- [ ] U-007a: density_normal_case
      Given: SSTable with size_bytes=1000, token range covering 10% of ring
      When: density(sst) computed
      Then: density = 1000 / 0.10 = 10000

- [ ] U-007b: density_zero_token_share_returns_max
      Given: SSTable with min_token == max_token (point partition)
      When: density(sst) computed
      Then: density = u64::MAX (or capped sentinel), not panic/NaN

- [ ] U-008a: level_assignment_freshly_flushed
      Given: small SSTables (low density, just flushed)
      When: assigned to levels with fan_factor=4
      Then: all at level 0

- [ ] U-008b: level_assignment_compacted
      Given: SSTables with increasing density (result of prior compactions)
      When: assigned to levels with fan_factor=4
      Then: spread across levels 0, 1, 2, ...

- [ ] U-009a: ucs_select_triggers_on_fan_factor
      Given: 5 SSTables all at level 0, fan_factor=4
      When: select() called
      Then: 1 CompactionTask with 4+ inputs from level 0

- [ ] U-009b: ucs_select_prefers_lower_levels
      Given: 5 SSTables at level 0, 5 at level 2 (both over fan_factor)
      When: select() called
      Then: first task targets level 0 (lower level preferred)

- [ ] U-010: ucs_config_rejects_fan_factor_below_2
      Given: HashMap {"fan_factor": "1"} or {"fan_factor": "0"}
      When: UcsConfig::from_params(map)
      Then: fan_factor clamped to 2 (minimum)

- [ ] U-011: ucs_deterministic (proptest)
      Given: arbitrary Vec<SSTableMetadata>
      When: select() called twice with same input
      Then: identical output both times

- [ ] U-012: ucs_tasks_subset_of_input (proptest)
      Given: arbitrary Vec<SSTableMetadata>
      When: select() returns tasks
      Then: every task.input SSTable exists in the original vec
```

### Sprint 3: Integration

```
- [ ] U-013: strategy_for_table_defaults_to_stcs
      Given: table with empty compaction_params
      When: strategy_for_table(state) called
      Then: returns SizeTieredStrategy (default)

- [ ] U-014: strategy_for_table_selects_ucs
      Given: table with compaction_params = {"class": "UnifiedCompactionStrategy"}
      When: strategy_for_table(state) called
      Then: returns UnifiedCompactionStrategy

- [ ] U-015: ucs_end_to_end_flush_compact
      Given: table created with UCS, data inserted, flushed 5 times
      When: maybe_compact() runs after 5th flush
      Then: UCS triggers compaction, output SSTable created, input count reduced

- [ ] U-016: stcs_tables_unaffected_by_ucs_addition
      Given: table created WITHOUT compaction params, data inserted, flushed
      When: maybe_compact() runs
      Then: STCS strategy used (same behavior as before UCS)
```

### Sprint 4: Equivalence & Edge Cases

```
- [ ] U-017: ucs_w2_lcs_like
      Given: 10 SSTables with varying density, fan_factor=2
      When: select() called
      Then: many small tasks (aggressive compaction, LCS-like)

- [ ] U-018: ucs_w32_stcs_like
      Given: 10 SSTables with varying density, fan_factor=32
      When: select() called
      Then: no tasks (lazy, needs 32+ per level to trigger, STCS-like)

- [ ] U-019: ucs_single_sstable_no_compaction
      Given: 1 SSTable
      When: select() called
      Then: empty tasks (nothing to compact)

- [ ] U-020: ucs_empty_table_no_compaction
      Given: 0 SSTables
      When: select() called
      Then: empty tasks

- [ ] U-021: ucs_large_fan_factor_no_panic
      Given: fan_factor=1000, 50 SSTables
      When: select() called
      Then: no panic, no tasks (50 < 1000)

- [ ] U-022: ucs_same_density_same_level
      Given: 8 SSTables all with density=5000, fan_factor=4
      When: select() called
      Then: all at same level, 1 task with 4+ inputs
```

## Implementation Order

Start with the simplest test that teaches the most (TDD Step 2):

1. **U-020** (empty → no tasks) — degenerate case, establishes UCS struct
2. **U-019** (single → no tasks) — trivial case
3. **U-006** (config parsing) — establishes UcsConfig
4. **U-010** (fan_factor validation) — boundary guard
5. **U-007a** (density normal) — core algorithm
6. **U-007b** (density zero guard) — safety
7. **U-008a** (level assignment fresh) — builds on density
8. **U-008b** (level assignment compacted) — triangulates
9. **U-009a** (select triggers) — core behavior
10. **U-009b** (select prefers lower) — refinement
11. **U-011** (determinism) — property
12. **U-012** (subset) — property
13. **U-022** (same density) — edge case that exercises full path

Sprint 1 metadata tests (U-001 through U-005) run in parallel — they modify different files.
Sprint 3 integration tests (U-013 through U-016) depend on Sprint 1 + 2 completion.
Sprint 4 equivalence tests (U-017, U-018) depend on Sprint 2 + 3.
