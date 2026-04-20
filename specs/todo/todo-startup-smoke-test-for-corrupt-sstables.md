---
type: todo
priority: P1
status: draft
created: 2026-04-19
updated: 2026-04-20
---

# Startup smoke-test: detect-and-quarantine SSTables whose open() succeeds but whose row iteration fails

## Why

The 2026-04-19 `agent_memory.tool_usage_log` corruption incident produced
~82 SSTables (across 3 nodes) with the following profile:

- `SSTableReader::open()` succeeds (header + trie parse fine)
- `read_all_partitions()` fails partway with
  `read_exact_at: wanted N, got M` (N > M) — Data.db is shorter than
  Partitions.db offsets claim

The existing startup quarantine
(`ferrosa-storage/src/engine.rs::load_existing_sstables_and_sidecars`)
catches two cases:

1. Zero-byte critical components (`Data.db`, `Partitions.db`) — quarantined
   at the first size check.
2. `open_sstable_from_dir` failures (bad header/trie) — quarantined in
   the Err arm.

**It does NOT catch the "open OK, read fails" class.** Those SSTables
stay in the view and produce a `read_range: skipping corrupted SSTable`
WARN on every query that touches them, forever. The 82 legacy files
uploaded to S3 before the writer-validation fix (2026-04-19) are
currently log-flooding the cluster.

## Proposed change

After `open_sstable_from_dir` succeeds in
`load_existing_sstables_and_sidecars`, call `read_all_partitions()` as
a smoke test. If it returns `Err`, quarantine the whole SSTable via the
existing `move_to_quarantine(gen_str, reason)` helper (already present
from today's fix) with reason `"open succeeded but read_all_partitions
failed: {e}"`.

Cost: an O(total SSTable bytes) read pass at startup. For this cluster
(~100 SSTables × low-hundreds KB each) ≈ seconds. Acceptable.

Gate it on a config flag (`FERROSA_STARTUP_SMOKE_TEST`, default `true`)
so operators can turn it off if startup time becomes a concern at scale.

## Acceptance criteria

- [ ] Unit test: synthesise an SSTable whose Data.db is truncated
      relative to Partitions.db offsets, verify startup quarantines it
      and does not include it in the view.
- [ ] Startup log emits a clear message showing how many SSTables were
      smoke-tested, how long the pass took, and how many were
      quarantined.
- [ ] Re-running the cluster on the 2026-04-19 corrupted data directory
      should quarantine all 82 known-bad files automatically without
      manual intervention.

## Relationship to the writer-validation fix

The writer-validation gates (`add_partition` clustering-shape check +
`finish()` self-readback, both landed in
`ferrosa-sstable/src/writer.rs`) prevent NEW corrupt SSTables. This
smoke-test closes the last loophole: legacy corrupt SSTables already
persisted (local or S3) can be auto-cleaned on the next boot.

## Related

- `specs/in-process/bug-read-path-memory-growth-bloats-coordinator.md`
  (parent bug).
- `ferrosa-storage/src/engine.rs::load_existing_sstables_and_sidecars`
  (extension point).
- `ferrosa-sstable/src/writer.rs::verify_output_readable` (sibling
  defensive check on the write side).
