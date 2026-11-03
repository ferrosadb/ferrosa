import Ferrosa

/-!
# Durable storage: what an acknowledged write survives

> *"Object storage is the source of truth. Local NVMe is a write-behind cache, not the record."*
> — ferrosa.ai

A write is admitted, appended to the commit log, applied to the memtable, acknowledged, and later
flushed to an SSTable that is uploaded to object storage. The guarantee a user relies on is about
the acknowledgement: what has the engine promised once it says `OK`? The answer depends on the
commit-log sync mode the operator chose, and this page states it for each mode.

The upload to object storage and the manifest that publishes it sit behind the durability boundary
in this model. The specification proves what reaches the local durable log and the SSTable; it does
not yet prove that a replacement node reading from object storage sees every acknowledged row. That
claim, "losing a node is a cache miss, not a restore", is listed on
*What the specification does not cover*.
-/

/-!
## No acknowledgement without a log record

A client is never told a row exists that the commit log does not hold. The acknowledged count can
never run ahead of the log, and every row in the memtable was logged before it was applied.
-/

#check @Ferrosa.WritePath.acked_le_logged
#check @Ferrosa.WritePath.active_subset_log
#check @Ferrosa.WritePath.synced_le_logged

/-!
## Durability is a configuration, and the default has a window

Under `batch` sync every acknowledged write is on disk. Under the default `periodic` sync there is a
reachable state in which a write is acknowledged and not yet fsynced, so a crash in that window loses
it. That is the documented trade-off of `periodic`, and the theorems state it in both directions.
-/

#check @Ferrosa.WritePath.batch_ack_is_durable
#check @Ferrosa.WritePath.periodic_ack_can_be_lost
#check @Ferrosa.Storage.Recovery.the_ack_guarantee_is_a_configuration_not_a_law

/-!
## A full disk stops writes rather than losing them

Once admission has failed, nothing more is appended to the commit log: the engine refuses rows
instead of acknowledging writes it cannot keep. A flush carries every row of the memtable into the
SSTable it publishes, and a rejected row is kept in a quarantine sidecar rather than dropped.
-/

#check @Ferrosa.WritePath.no_append_after_admission_failure
#check @Ferrosa.WritePath.flush_keeps_rows
#check @Ferrosa.WritePath.quarantine_retains_row

/-!
## Crash recovery

Replay stops at the first invalid record, so recovery restores a prefix of what was written and never
skips over a gap. Every node replaying the same log reaches the same state.

**Does not hold yet.** The commit-log record has no checksum, so a torn tail is detected but a
corrupted payload is not. A mutation with a zero id is replayed twice, which double-applies a counter.
And recovery deletes a corrupt segment where it should quarantine it.
-/

#check @Ferrosa.Storage.Recovery.truncation_is_detected
#check @Ferrosa.Storage.Recovery.stop_replays_a_prefix
#check @Ferrosa.Storage.Recovery.recovery_is_deterministic
#check @Ferrosa.Storage.Recovery.no_checksum_detects_only_truncation
#check @Ferrosa.Storage.Recovery.zero_id_replay_duplicates_a_nonidempotent_mutation
#check @Ferrosa.Storage.Recovery.only_quarantine_preserves_data

/-!
## The native file format cannot return a wrong value

Ferrosa's native SSTable format (`fnf`, ADR-022) frames each record with its own check. A torn file
reads as a verified prefix, one damaged byte costs one record and not the rest of the file, and the
index is a speed-up rather than an authority: a corrupt index can make a lookup miss, never return a
wrong row.
-/

#check @Ferrosa.Storage.Format.scan_is_a_prefix
#check @Ferrosa.Storage.Format.resync_loses_less_than_stopping
#check @Ferrosa.Storage.Format.trie_cannot_produce_a_wrong_value
#check @Ferrosa.Storage.Format.trie_answer_agrees_with_the_scan
#check @Ferrosa.Storage.Format.absent_footer_is_recoverable
#check @Ferrosa.Storage.Format.ack_at_data_fsync_is_recoverable

/-!
## Deletes, TTL and conflicting writes

Conflicting writes resolve by timestamp, and a tombstone beats a value written at the same instant.
A TTL expires at the stored local deletion time, so a replica's clock cannot change when a cell
disappears. A tombstone shadows every value at or below its timestamp, so no order of arrival
resurrects a deleted row, and purging a tombstone is safe once every replica has applied it.

**Does not hold yet.** Two cells with equal timestamps resolve differently depending on arrival
order, so replicas can disagree. The cell's 32-bit local deletion time overflows for TTLs that are
valid today. And the purge path does not check that every replica has applied the tombstone, so a
stale replica can bring deleted data back.
-/

#check @Ferrosa.Storage.Tombstone.merge_prefers_newer
#check @Ferrosa.Storage.Tombstone.merge_tombstone_wins_tie
#check @Ferrosa.Storage.Tombstone.expiry_depends_only_on_the_stored_ldt
#check @Ferrosa.Storage.Tombstone.a_tombstone_shadows_every_value_at_or_below_its_timestamp
#check @Ferrosa.Storage.Tombstone.purge_safe_after_all_replicas_applied
#check @Ferrosa.WritePath.lww_ties_are_order_dependent
#check @Ferrosa.Storage.Tombstone.ldt_overflows_before_2038_reachable_now
#check @Ferrosa.Storage.Tombstone.purge_before_propagation_resurrects
