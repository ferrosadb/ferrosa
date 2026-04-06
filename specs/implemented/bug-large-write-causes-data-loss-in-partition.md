
## Re-test After Fix a6d1006 (data scatter — Joining state)

**Result: DATA LOSS STILL OCCURRING.**

Full ingest sequence completed, verification immediately after showed 14,858 entities. After compaction ran (~minutes later), only 2,190 entities remain. 12,668 entities lost.

The Joining-state fix resolved the canary test (100/100 canaries survived immediately after ingest) but did NOT prevent subsequent data loss from compaction.

The corrupt SSTable evidence from earlier (DeletionTime flags 0x9d/0xca/0xbd/0xe6, cell value length overflow) is likely still the root cause — the fix addressed routing but not the SSTable writer corruption.

## Root Cause Analysis (fix/compaction-data-loss branch)

**Five bugs found, all TDD-verified:**

### Bug 1 (P0): Compaction executor silently skips unreadable SSTables
- `ferrosa-storage/src/compaction/executor.rs`: Every `continue` on SSTable read failure silently skipped the input but the task still succeeded
- `swap_compacted_sstables` then removed ALL input SSTables (including skipped ones), losing their data permanently
- **RED tests**: `compaction_fails_when_input_sstable_unreadable`, `compaction_fails_when_input_data_file_missing`

### Bug 2 (P0): Manifest save_with_retry loses compaction removals
- `ferrosa-storage/src/manifest.rs`: `save_with_retry` calls `merge_into` which starts from the latest S3 manifest and only ADDS entries — removals applied to `self` are lost because `latest` still has the old entries
- After compaction, the manifest never actually removes input SSTables — they grow unboundedly
- **RED test**: `save_with_retry_loses_compaction_removals` — CONFIRMED: `["sst1", "sst2", "sst3", "sst4"]` when expected `["sst3", "sst4"]`
- **Fix**: New `save_with_retry_and_removals` applies removals to `latest` BEFORE merge

### Bug 3 (P0): merge_into skips ID-colliding compaction output
- When compaction output reuses an input's generation ID (e.g., both ID "1"), `merge_into` saw the ID already in `latest` and SKIPPED adding the new entry
- **Fix**: `merge_into` now REPLACES entries with matching IDs instead of skipping

### Bug 4 (P1): debug_assert for timestamp delta underflow
- `ferrosa-sstable/src/writer.rs:556`: `debug_assert!` for cell timestamp underflow only fires in debug builds — release builds silently produce corrupt SSTables
- Also added assert for row deletion timestamp underflow (line 356)
- **Fix**: Upgraded to hard `assert!`

### Bug 5 (P2): build_serialization_header min_timestamp sentinel not reset
- `ferrosa-storage/src/flush.rs`: `max_timestamp` was reset to `i64::MAX` when no real timestamps found, but `min_timestamp` stayed at `NO_TIMESTAMP` (i64::MIN) — asymmetric sentinel handling
- **Fix**: Reset `min_timestamp` to 0 when no real timestamps found
