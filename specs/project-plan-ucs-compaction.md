# UCS Compaction — Project Plan

## Summary

Add Unified Compaction Strategy (UCS) to ferrosa, following Cassandra 5.0 CEP-26. UCS subsumes STCS/LCS/TWCS into a single density-based, parameterized strategy. This plan covers the minimum viable implementation: UCS strategy selection, per-table DDL configuration, and accurate SSTable metadata.

## Prerequisites

- Compaction executor and merge logic (already implemented)
- SSTable reader/writer (already implemented)
- CompactionStrategy trait (already defined at strategy.rs:15-24)

## Sprint 1: Foundation (Priority 1 — Correctness)

Populate SSTableMetadata with real values. Without this, UCS density calculations are meaningless.

| ID | Task | Size | Success Criteria | Tests | Source |
|----|------|------|-----------------|-------|--------|
| U-001 | Populate `size_bytes` in `sstable_metadata()` | S | `store.rs:sstable_metadata()` returns actual component file sizes, not 0 | `sstable_metadata_reports_nonzero_size` | hazard scan |
| U-002 | Populate `min_token`/`max_token` in `sstable_metadata()` | M | Token range read from SSTable partition index or first/last decorated key | `sstable_metadata_reports_token_range` | hazard scan |
| U-003 | Populate `max_timestamp` in `sstable_metadata()` | S | Actual max timestamp from cells, not `i64::MAX` sentinel | `sstable_metadata_reports_max_timestamp` | hazard scan |
| U-004 | Add `compaction_params: HashMap<String, String>` to `TableMetadata` | S | Field exists, defaults to empty, serializes/deserializes | `table_metadata_has_compaction_params` | architect |
| U-005 | Persist `table_options` in `route_create_table` | S | `CREATE TABLE ... WITH compaction = {map}` stores params in `TableMetadata` | `create_table_with_compaction_persists_params` | architect |

## Sprint 2: UCS Strategy (Priority 1 — Core Feature)

Implement the UnifiedCompactionStrategy.

| ID | Task | Size | Success Criteria | Tests | Source |
|----|------|------|-----------------|-------|--------|
| U-006 | Create `strategy_ucs.rs` with `UcsConfig` | S | Config struct with `fan_factor`, `min_sstable_size`, `max_levels`, `output_dir`. `from_params()` parses DDL map. | `ucs_config_from_params` | architect |
| U-007 | Implement density calculation | S | `density(sst) = size_bytes / max(token_share, epsilon)` with division-by-zero guard | `density_zero_token_share_returns_max`, `density_normal_case` | hazard scan |
| U-008 | Implement level assignment | M | SSTables assigned to levels by density thresholds: `base_density * fan_factor^level` | `level_assignment_freshly_flushed`, `level_assignment_compacted` | architect |
| U-009 | Implement `select()` for UCS | M | Levels with > `fan_factor` SSTables emit CompactionTask. Lower levels preferred. | `ucs_select_triggers_on_fan_factor`, `ucs_select_prefers_lower_levels` | architect |
| U-010 | Guard fan_factor W < 2 | S | `UcsConfig::from_params` rejects W < 2, defaults to 4 | `ucs_config_rejects_fan_factor_below_2` | hazard scan |
| U-011 | Determinism property test | S | Same SSTable set always produces same tasks (proptest) | `ucs_deterministic` | STCS pattern |
| U-012 | Tasks-subset-of-input property test | S | Compaction never invents SSTables (proptest) | `ucs_tasks_subset_of_input` | STCS pattern |

## Sprint 3: Integration (Priority 1 — Wiring)

Wire UCS into the engine and DDL path.

| ID | Task | Size | Success Criteria | Tests | Source |
|----|------|------|-----------------|-------|--------|
| U-013 | Add `strategy_for_table()` to engine.rs | S | Reads `table_meta.compaction_params`, returns `Box<dyn CompactionStrategy>`. Defaults to STCS. | `strategy_for_table_defaults_to_stcs`, `strategy_for_table_selects_ucs` | architect |
| U-014 | Update `maybe_compact()` to use `strategy_for_table()` | S | Replaces hardcoded `SizeTieredStrategy::new()` with dispatch | `maybe_compact_uses_table_strategy` | architect |
| U-015 | End-to-end: CREATE TABLE with UCS → flush → compaction | L | Insert data, flush multiple times, verify UCS compaction triggers and produces correct output | `ucs_end_to_end_flush_compact` | integration |
| U-016 | Verify STCS tables unaffected | S | Existing tables without compaction params continue using STCS | `stcs_tables_unaffected_by_ucs_addition` | regression |

## Sprint 4: Equivalence & Edge Cases (Priority 2 — Correctness)

Verify UCS parameter equivalences and edge cases.

| ID | Task | Size | Success Criteria | Tests | Source |
|----|------|------|-----------------|-------|--------|
| U-017 | W=2 produces LCS-like behavior | M | With fan_factor=2, many small levels, frequent compaction, low space amp | `ucs_w2_lcs_like` | architect |
| U-018 | W=32 produces STCS-like behavior | M | With fan_factor=32, few levels, rare compaction, similar to STCS bucketing | `ucs_w32_stcs_like` | architect |
| U-019 | Single SSTable — no compaction | S | One SSTable per level never triggers | `ucs_single_sstable_no_compaction` | edge case |
| U-020 | Empty table — no compaction | S | Zero SSTables returns empty tasks | `ucs_empty_table_no_compaction` | edge case |
| U-021 | Very large fan factor (W=1000) | S | No panic, just very lazy compaction | `ucs_large_fan_factor_no_panic` | hazard scan |
| U-022 | All SSTables same density | S | All land in same level, triggers when count > W | `ucs_same_density_same_level` | edge case |

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| SSTableMetadata size_bytes stays 0 | High (current state) | Critical — UCS assigns all to level 0 | Sprint 1 (U-001) must land first |
| Token range 0..0 makes density ∞ | High (current state) | Critical — division by zero or wrong levels | U-002 + U-007 epsilon guard |
| Fan factor W=0 causes infinite compaction | Low (config validation) | High — CPU storm | U-010 rejects W < 2 |
| Per-table params not persisted on restart | Medium | High — UCS tables revert to STCS after restart | U-004/U-005 + schema persistence test |
| Existing STCS behavior changes | Low | High — regression in production | U-016 regression test |

## Dependencies

```
U-001 ──┐
U-002 ──┤
U-003 ──┼── U-007 ── U-008 ── U-009 ── U-015
U-004 ──┤                                 │
U-005 ──┘                                 │
                                          ├── U-017
U-006 ── U-010 ── U-009                   ├── U-018
                                          └── U-016
U-013 ── U-014 ── U-015
```

Sprint 1 (U-001 through U-005) is prerequisite for all UCS work.
Sprint 2 (U-006 through U-012) can proceed in parallel once Sprint 1 metadata is available.
Sprint 3 (U-013 through U-016) wires everything together.
Sprint 4 (U-017 through U-022) validates correctness properties.

## Estimated Effort

| Sprint | Tasks | T-Shirt | Notes |
|--------|-------|---------|-------|
| Sprint 1 | 5 | M | Metadata population is straightforward but needs careful testing |
| Sprint 2 | 7 | L | Core algorithm — density calc + level assignment + select() |
| Sprint 3 | 4 | M | Wiring — mostly plumbing existing interfaces |
| Sprint 4 | 6 | M | Property tests + edge cases |
| **Total** | **22** | **XL** | |
