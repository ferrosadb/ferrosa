# FMEA — RRD Cascading Time-Series Aggregation

> Last updated: 2026-03-21
> Status: Implemented (mitigations in progress)

## Scoring Criteria

| Score | Severity (S) | Occurrence (O) | Detection (D) |
| ----- | ------------ | -------------- | -------------- |
| 1 | Negligible | Almost never | Always detected before impact |
| 2-3 | Minor degradation | Rare | Usually detected |
| 4-6 | Significant impact | Occasional | Sometimes detected |
| 7-8 | Major failure | Frequent | Rarely detected |
| 9-10 | Catastrophic / data loss | Very frequent | Undetectable |

RPN = Severity x Occurrence x Detection. **Action required for RPN >= 50.**

## Failure Mode Table

| # | Component | Failure Mode | S | O | D | RPN | Mitigation | Test Case |
| --- | --------- | ----------- | --- | --- | --- | --- | ---------- | --------- |
| FM1 | config.rs | Zero interval/capacity passes parsing, panics at RingBuffer::new() | 9 | 7 | 3 | **189** | Validate `interval > 0`, `capacity > 0` at parse time | config_rejects_zero_interval, config_rejects_zero_capacity |
| FM2 | aggregator.rs | Silent skip on failed numeric decode — user unaware rows dropped | 8 | 6 | 8 | **384** | Log tracing::warn + increment decode_failures counter | metrics_track_decode_failures |
| FM3 | ring.rs | Massive timestamp gap causes unbounded loop in boundary advance | 9 | 2 | 9 | **162** | Cap loop at 1000 iterations; direct calculation on overflow | boundary_advance_massive_gap_does_not_hang |
| FM4 | aggregator.rs | Channel full drops consolidation tasks with no recovery | 7 | 5 | 5 | **175** | Channel capacity configurable (default 1024); next boundary re-triggers | config_default_channel_capacity |
| FM5 | aggregator.rs | Type mismatch — 4-byte cell decoded as i32 instead of float | 9 | 3 | 7 | **189** | Use decode_typed_numeric() with schema metadata | extract_values_uses_typed_decode |
| FM6 | late_data.rs | Debounce map grows unbounded under high-cardinality late data | 7 | 4 | 6 | **168** | max_pending limit (default 10,000) with overflow eviction | debouncer_rejects_when_at_capacity |
| FM7 | consolidation.rs | Composite(Wasm{..}) produces NaN instead of UDF result | 8 | 2 | 6 | 96 | Validate at config parse — reject Wasm in Composite | composite_rejects_wasm |
| FM8 | consolidation.rs | NaN in input propagates silently through all aggregates | 7 | 3 | 7 | **147** | Filter NaN at extract_values boundary | nan_filtered_before_consolidation |
| FM9 | config.rs | Cascade multiplier overflow panics Duration::from_micros | 8 | 2 | 4 | 64 | Cap multipliers at 1000 at parse time | config_rejects_extreme_multiplier |
| FM10 | config.rs | Empty function list produces zero-column output | 5 | 3 | 4 | 60 | Reject empty function list at parse | config_rejects_empty_functions |

## Risk Priority Summary

| Priority | Count | Items |
| -------- | ----- | ----- |
| Critical (RPN >= 150) | 6 | FM1 (189), FM2 (384), FM3 (162), FM4 (175), FM5 (189), FM6 (168) |
| High (RPN 100-149) | 1 | FM8 (147) |
| Medium (RPN 50-99) | 3 | FM7 (96), FM9 (64), FM10 (60) |

## Implementation Status

> Updated: 2026-03-21

| ID | RPN | Status | Evidence |
| ---- | ----- | -------- | -------- |
| FM1 | 189 | **Mitigated** | `45e5d27` — validate interval > 0 and capacity > 0 at parse |
| FM2 | 384 | **Mitigated** | `d4f0782` — tracing::warn + decode_failures counter |
| FM3 | 162 | **Mitigated** | `54cb8d9` — cap loop at 1000 iterations, direct calc on overflow |
| FM4 | 175 | **Mitigated** | `a831458` — channel_capacity configurable, default 1024 |
| FM5 | 189 | **Mitigated** | `563ae61` — decode_typed_numeric with column type metadata |
| FM6 | 168 | **Mitigated** | `be94fca` — max_pending limit (default 10,000) with eviction |
| FM7 | 96 | Open | DDL validation needed |
| FM8 | 147 | Open | NaN filtering needed |
| FM9 | 64 | Open | Multiplier cap needed |
| FM10 | 60 | Open | Empty list rejection needed |

## Related Specs

- [RRD Time-Series Aggregation Design](../superpowers/specs/2026-03-21-rrd-timeseries-aggregation-design.md)
- [Storage Engine](storage.md)
