# TODO: sync_sstables_to_s3 Manifest Metadata is Placeholder

**Severity:** Medium
**Component:** ferrosa-storage

## Issue

`sync_sstables_to_s3()` (engine.rs:2526-2536) adds manifest entries with placeholder metadata:

```rust
manifest.add_sstable(table_id_str, ManifestEntry {
    id: gen_str,
    size: total_size,
    min_token: i64::MIN,   // ← placeholder
    max_token: i64::MAX,   // ← placeholder  
    min_timestamp: 0,       // ← placeholder
    max_timestamp: 0,       // ← placeholder
});
```

## Impact

- Token range metadata is wrong → compaction strategy can't filter by token range
- Timestamp metadata is wrong → time-window compaction strategies will misclassify SSTables
- Any manifest-based range queries will return incorrect results
- After restart + S3 download, SSTables appear to span the entire token range

## Fix

Read the SSTable's Statistics.db file to extract the actual token range and timestamp bounds before adding to the manifest. Or parse the partition index to get first/last tokens.
