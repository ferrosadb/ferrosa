---
title: Streaming audit bugfix checklist
status: in-process
created: 2026-05-23
updated: 2026-05-23
---

# Streaming Audit Bugfix Checklist

Goal: fix the concrete rust-streaming audit findings with tests first, run local CI, then push a PR and monitor GitHub CI until green.

## P0

- [x] Pending upload log removal must match both `table_id` and `sstable_id`.
  - Test: two tables with the same SSTable generation id; removing one entry preserves the other.
  - Verify: `cargo test -p ferrosa-storage pending_uploads_log_remove_entry_requires_table_id`.

- [x] Pending compaction upload replay must either update manifest/log state or remain durably retryable.
  - Test: simulate crash after compaction output is pending; restart replay cannot strand uploaded files outside the manifest.
  - Verify: `cargo test -p ferrosa-storage replay_pending_compaction_upload_finalizes_manifest_and_log`.

- [x] Streaming range coordinator must not materialize full local/remote result vectors.
  - Test: mock storage detects materialized local read; large synthetic stream remains incremental.
  - Verify: `cargo test -p ferrosa-cluster coordinate_streaming_range_read_does_not_call_vec_local_read`.

- [x] `RangeReadStreamCancel` must stop producers and unregister routes.
  - Test: slow/infinite reader receives cancel and stops within one batch.
  - Verify: `cargo test -p ferrosa-cluster stream_request_cancel_stops_reader_within_one_batch`.

- [x] Production SSTable replay tests must be fixture-backed and non-ignored.
  - Test: bundled fixture SSTables read to EOF and fail on decode/read errors.
  - Verify: `cargo test -p ferrosa-sstable --test p0_production_disk_replay`.

- [x] Compaction cleanup regression must run in local and GitHub CI.
  - Test: compaction cleanup updates manifest, enqueues deletes, evicts inputs, and leaves output readable.
  - Verify: `cargo test -p ferrosa-storage compaction_cleanup_updates_manifest_enqueues_deletes_and_evicts_inputs`.

## P1

- [x] Router backpressure or dropped chunks must fail the stream instead of returning partial success.
  - Verify: `cargo test -p ferrosa-cluster full_stream_buffer_closes_route_so_consumer_fails_loudly`.

- [x] Stream consumers must reject missing and reordered chunk sequence numbers.
  - Verify: `cargo test -p ferrosa-cluster stream_frame_router::tests`.

- [x] Bootstrap SSTable sender and receiver must stream file components/chunks without whole-file materialization.
  - Verify: `cargo test -p ferrosa-cluster send_sstable_files_reads_components_incrementally`.

- [x] S3 SSTable cleanup must delete Ferrosa's actual component files.
  - Test: upload all Ferrosa SSTable components, run `DeleteSSTable`, and assert `Data.db`, `Partitions.db`, `Rows.db`, `Filter.db`, `Statistics.db`, `TOC.txt`, and optional `CompressionInfo.db` are gone.
  - Verify: `cargo test -p ferrosa-storage delete_sstable_removes_ferrosa_components`.

- [x] Range merger must propagate SSTable read/decode errors instead of silently dropping table tails.
  - Verify: `cargo test -p ferrosa-storage range_merger_propagates_truncated_sstable_tail_error`.

- [x] RRD materialization must not lose rollups when the bounded queue is full.
  - Verify: `cargo test -p ferrosa-storage aggregator_backpressures_instead_of_dropping_when_task_queue_is_full`.

- [x] RRD worker failures must be retryable or visible as failed state.
  - Verify: `cargo test -p ferrosa-storage --test timeseries_materialization rrd_materialization_failure_is_visible_in_status_snapshot`.

- [x] Time-series materialization must exclude deleted rows and cells.
  - Verify: `cargo test -p ferrosa-storage --test timeseries_materialization materialization_drain_excludes_row_deleted_source_values`.

## CI Gate

- [x] `cargo fmt --check`
- [x] `cargo clippy --all-targets -- -D warnings`
- [x] `cargo test`
- [ ] PR opened and GitHub CI monitored every 15 minutes until green.
