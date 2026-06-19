---
type: todo
priority: P0
status: in_progress
created: 2026-05-20
updated: 2026-05-20
---

# Bound RRD ring memory and log thrashing

## Why

RRD ring buffers should be preallocated where useful, but they must degrade
gracefully when the configured ring count/capacity exceeds available budget.
The write path must prevent OOM risk by refusing or evicting rings, and
operators need warnings when ring churn indicates thrashing.

## Acceptance Criteria

- Configuration accepts an optional ring memory budget.
- Effective active-ring capacity is capped by both `max_rings` and the memory
  budget estimate.
- If the budget cannot fit one ring, writes skip ring allocation and increment
  metrics instead of panicking or allocating.
- Creating a ring above the effective cap evicts cold rings before allocating
  the new one.
- Repeated evictions increment a thrash-warning metric and emit a warning log.
- Focused tests cover budget rejection, budget-capped eviction, and thrash
  accounting.

## Implementation Notes

Implemented in the current branch:

- Added `consolidation.ring_memory_budget_bytes` as an optional aggregate heap
  budget for active rings.
- The default ring memory budget is derived from
  `FERROSA_RRD_RING_MEMORY_BUDGET_BYTES` when set, otherwise from cgroup/process
  memory limits at 5%, otherwise a conservative 64 MiB fallback.
- Table-level `consolidation.ring_memory_budget_bytes` overrides the derived
  process default.
- Active aggregators read ring budget and thrash thresholds from
  `TimeSeriesRuntimeSettings`, so those controls can change without rebuilding
  table metadata or recreating the aggregator.
- Added `system_observability.rrd_runtime_settings` as a virtual control table.
  Superusers can update `setting_value` for `ring_memory_budget_bytes` and
  `ring_thrash_warn_evictions` at runtime.
- Added `consolidation.ring_thrash_warn_evictions` to control warning cadence
  for ring churn.
- Added a conservative `RingBuffer::estimated_heap_bytes` helper so the
  aggregator can cap active rings before allocating.
- `TimeSeriesAggregator::effective_max_rings()` now applies both `max_rings`
  and the memory budget.
- The write path refuses to allocate a ring if the budget cannot fit one ring,
  increments `ring_budget_rejections`, and emits a warning log.
- Creating a new partition ring above the effective cap evicts cold rings first.
- Repeated evictions increment `ring_evictions`, cross
  `ring_thrash_warn_evictions`, increment `ring_thrash_warnings`, and emit a
  warning log.

Focused tests added:

- `aggregator_skips_ring_allocation_when_budget_cannot_fit_one_ring`
- `aggregator_evicts_before_allocating_above_budget_capped_ring_limit`
- `aggregator_counts_and_warns_when_ring_evictions_thrash`
- `config_parses_ring_memory_budget_and_thrash_threshold`
- `ring_memory_budget_env_override_wins_over_detected_limit`
- `ring_memory_budget_derives_from_detected_memory_limit`
- `ring_memory_budget_falls_back_when_no_memory_signal_exists`
- `table_extension_ring_memory_budget_overrides_derived_default`
- `aggregator_runtime_settings_adjust_ring_budget_without_rebuild`
- `rrd_runtime_settings_updates_ring_budget`
- `update_rrd_runtime_settings_virtual_table_adjusts_budget`
- `update_rrd_runtime_settings_requires_superuser`

Remaining follow-up:

- Consider whether 5% should become an operator-tunable fraction after
  production load testing.
- When the consolidator registry lands, register this virtual table against the
  registry's shared runtime settings rather than the startup placeholder.
