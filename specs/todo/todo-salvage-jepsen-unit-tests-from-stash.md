---
type: todo
priority: P3
reported-by: stash audit 2026-04-24
implemented-by: ""
verified-by: ""
created: 2026-04-24
updated: 2026-04-24
attachment: specs/todo/attachments/jepsen-test-stash-20260401.patch
source-stash: "stash@{0} on fix/grant-revoke-functions @ 391b194, 2026-04-01"
---

# Salvage 71 jepsen unit tests from stashed WIP

## Summary

A WIP stash on the (now deleted) `fix/grant-revoke-functions` branch accumulated
~1,500 lines of work on top of commit `391b194` (2026-03-30). Most of that WIP
has since been superseded by main — `engine.rs`, `commitlog`, `peer.rs`, and
`controller.rs` have been refactored past recognition. But **71 unit tests**
added to the jepsen harness in that stash never made it to main. The stash is
saved verbatim at `specs/todo/attachments/jepsen-test-stash-20260401.patch` and
then dropped from the local stash list.

This TODO exists to cherry-pick the surviving tests back in when someone
next touches the jepsen harness.

## Why these were lost

The stash was taken on a branch that was abandoned. All the production-code
changes on the stash were either re-implemented differently on main or became
obsolete due to surrounding refactors. The *tests* — being leaf-only additions
to `#[cfg(test)] mod tests` blocks — are lossless to port forward, but they
were never separately cherry-picked.

## Per-file test inventory

Each entry lists the test names the stash defines that don't exist on main.
The test code itself is in the attached patch; search for the `fn <name>(` line
and copy the body into the current test module.

### `ferrosa-jepsen/src/workload/bank.rs` — 6 tests

Edge-case coverage for `BankWorkload::check_invariant`.

- `bank_invariant_empty_history` — empty op list passes
- `bank_invariant_no_serial_reads` — writes/reads without `CurrentValues` pass
- `bank_invariant_multiple_serial_reads_all_conserved` — multiple snapshots, all sum to 10000
- `bank_invariant_second_read_violated` — second snapshot sums to 10001
- `bank_invariant_skips_error_results` — `OpResult::Err` / `OpResult::Timeout` don't false-positive
- `bank_invariant_unparseable_balance_lowers_total` — non-i64 balance parses as 0 → conservation fails

### `ferrosa-jepsen/src/workload/lwt.rs` — 17 tests

Execution + invariant tests for the 16 LWT workload patterns. Each pattern
gets an `..._executes` smoke test (the workload can be driven end-to-end
against a mocked session) and a subset have `..._invariant_*` tests for the
`check_invariant` edge cases.

- `lwt_pattern_2_update_if_executes`, `lwt_pattern_2_invariant_empty_history`
- `lwt_pattern_3_delete_if_executes`, `lwt_pattern_3_invariant_empty_history`
- `lwt_pattern_4_insert_ttl_executes`, `lwt_pattern_4_invariant_one_applied_ok`, `lwt_pattern_4_invariant_two_applied_fails`
- `lwt_pattern_5_update_if_exists_executes`
- `lwt_pattern_6_replace_if_executes`
- `lwt_pattern_8_batch_insert_executes`, `lwt_pattern_8_invariant_one_batch_applied_ok`
- `lwt_pattern_9_batch_mixed_executes`
- `lwt_pattern_10_collections_executes`
- `lwt_pattern_11_udt_executes`
- `lwt_pattern_12_counter_executes`
- `lwt_pattern_13_timestamp_executes`
- `lwt_pattern_15_serial_read_executes`

### `ferrosa-jepsen/src/history.rs` — 9 tests

Recorder panics and filter/serialization edge cases.

- `recorder_panics_on_empty_client_id` — HistoryRecorder refuses empty client id
- `recorder_panics_on_double_invoke` — two invokes without a complete panics
- `recorder_panics_on_complete_without_invoke` — stray complete panics
- `recorder_panics_on_finish_with_pending` — finish with in-flight ops panics
- `filter_key_matches_transaction_contents` — key-based filter inspects txn ops
- `filter_key_excludes_table_based_ops` — filter skips ops keyed only by table
- `jsonl_skips_blank_lines` — JSONL reader tolerates empty lines
- `jsonl_error_on_invalid_json` — JSONL reader surfaces parse errors
- `merge_preserves_all_operations` — `History::merge` keeps every op

### `ferrosa-jepsen/src/checker/knossos.rs` — 12 tests

Tests for `parse_output` and the `extract_count` helper used to scrape
Knossos CLI output.

- `parse_empty_output` — empty stdout yields a sane result
- `parse_output_valid_false_explicit` — `valid: false` is parsed
- `parse_output_without_valid_keyword` — missing `valid:` defaults
- `parse_output_with_extra_whitespace` — whitespace-tolerant parse
- `parse_output_multiline_clojure_format` — multi-line Clojure map output parses
- `parse_output_non_utf8_bytes` — non-UTF-8 bytes don't crash
- `extract_count_zero` — `"key: 0"` → 0
- `extract_count_large_number` — big numbers parse
- `extract_count_with_trailing_text` — text after the number is ignored
- `extract_count_no_digits_after_key` — missing digits → 0
- `extract_count_key_at_end_of_text` — key at EOF handled
- `knossos_result_serialization_roundtrip` — serde roundtrip

### `ferrosa-jepsen/src/checker/elle.rs` — 6 tests

Elle anomaly type + result struct tests.

- `elle_anomaly_type_roundtrip` — serde roundtrip on every variant
- `elle_anomaly_struct_serialization` — full anomaly struct serializes
- `elle_result_empty_anomalies` — empty anomaly list
- `elle_result_with_anomalies` — populated anomaly list
- `elle_result_all_anomaly_names` — enumerates every known anomaly name
- `elle_checker_new_stores_path` — constructor stores the binary path

### `ferrosa-jepsen/src/chaos/mod.rs` — 10 tests

Nemesis registry + schedule coverage.

- `registry_default_is_empty` — default registry has no nemeses
- `noop_nemesis_inject_and_heal_succeed` — Noop nemesis is truly a no-op
- `nemesis_context_single_node` — context builds correctly for 1-node cluster
- `nemesis_context_even_node_count` — even-node partition math
- `phase2_is_superset_of_phase1` — registry phase containment
- `full_is_superset_of_phase2` — registry phase containment
- `composed_nemesis_names` — composed nemesis exposes constituent names
- `wan_nemesis_names` — WAN family nemesis names
- `nemesis_schedule_zero_cycles` — zero-cycle schedule is a valid no-op
- `nemesis_schedule_roundtrip_preserves_durations` — schedule serde preserves Duration precision

### `ferrosa-jepsen/src/config.rs` — 11 items (10 tests + 1 helper)

`RunConfig` / `Tier` / `Topology` unit tests.

- `make_config` — helper, builds a RunConfig for a given Tier
- `run_duration_secs_per_tier` — duration per tier is correct
- `concurrency_levels_per_tier` — concurrency vector per tier
- `concurrency_levels_override` — CLI override path
- `concurrency_medium_values` — medium tier specifics
- `topologies_per_tier` — topology set per tier
- `topologies_override` — CLI override path
- `topology_dc_counts` — DC counts per topology
- `topology_quorum_sizes` — quorum size per topology
- `topology_fly_requirement` — which topologies require fly.io
- `backend_for_topology` — backend (docker/fly) per topology

## How to salvage

1. Pick one file at a time.
2. Extract the relevant `#[cfg(test)]` hunk from the attached patch. The
   patch context around each added test shows the imports and helpers it
   needs. Test-local helpers (`make_op`, `make_config`) may need porting too.
3. Paste the test into the current file's test module. Fix up any drift in
   the types under test (e.g., `Op` variants, `Workload` trait signature).
4. Run `cargo test -p ferrosa-jepsen` until it passes.

The 71 tests together add roughly 900 lines of `#[cfg(test)]` code. None of
them require extra production-code changes — every referenced API already
exists on main (that is why the salvage is possible at all).

## Non-goals

Do NOT attempt to apply the patch wholesale. The non-test hunks (`engine.rs`,
`commitlog`, `peer.rs`, `controller.rs`, `ddl_path.rs`, etc.) are against a
pre-refactor base and will conflict or silently undo current work.

## Acceptance Criteria

- [ ] All 71 tests ported into their respective files (may be split across
      several PRs per-file).
- [ ] `cargo test -p ferrosa-jepsen` passes with them included.
- [ ] `specs/todo/attachments/jepsen-test-stash-20260401.patch` removed once
      every listed test is landed.
