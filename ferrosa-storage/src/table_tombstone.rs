//! Module: the **table-level tombstone** — a whole-table deletion watermark that
//! lives in the ordinary LSM as one reserved partition, so it is written,
//! replicated, buffered in a transaction and rolled back exactly like any other
//! row.
//!
//! Correctness: correct when (a) a committed table tombstone makes every row
//! older than its `marked_for_delete_at` invisible to every read path, (b) a row
//! written at or after that timestamp survives, (c) the marker is never dropped
//! while any replica could still hold the data it covers, and (d) a stale copy of
//! a pre-truncate row cannot resurrect it.
//!
//! ## Why this shape
//!
//! `TRUNCATE` on ferrosa is not a data-file delete with a cluster fan-out (the
//! old `TruncateExecutor`/`ClusterTruncate` path). It is a normal replicated
//! **write** of a deletion marker. Expressing it as a write is what makes it
//! transactional: it buffers inside a `BEGIN` … `COMMIT`, applies atomically with
//! the rest of the write set, and is discarded by `ROLLBACK`.
//!
//! The marker reuses the EXISTING tombstone shapes rather than inventing a second
//! mechanism:
//!
//! - Its row is an ordinary **partition tombstone** — empty clustering, no cells,
//!   a non-`LIVE` [`DeletionTime`] — the exact shape `DELETE FROM t WHERE pk = ?`
//!   produces (`ferrosa_storage::memtable::is_partition_tombstone`). The memtable
//!   lifts it into `Partition::deletion` for a partition, never storing it as a
//!   row.
//! - It is stored under a **reserved partition key** ([`table_tombstone_key`]),
//!   so it is a single partition in the table's own LSM and inherits durability,
//!   flush, compaction and the existing `DeletionTime` suppression predicate.
//!
//! The ONLY thing that is new is *scope*: the partition-tombstone mechanism is
//! **per-partition** (a table has many partition keys, so a truncate cannot be one
//! partition's tombstone). A table tombstone is therefore read *table-wide*: every
//! read path merges the reserved partition's deletion into the deletion it applies
//! to the partition it is reading, via
//! [`apply_table_deletion`](crate::merge::apply_table_deletion). Same predicate, same
//! timestamp ordering, table scope.
//!
//! ## Lifetime rule (no resurrection)
//!
//! A table tombstone may only be dropped once **every replica has purged every
//! row it covers**. ferrosa does not yet track that per-replica purge watermark,
//! so the marker is **never purged by compaction**: it is retained until the table
//! itself is dropped. Retaining it is the conservative, safe side of the rule —
//! the opposite (dropping it while a stale replica still holds older rows) is the
//! silent resurrection this module exists to prevent. See
//! [`crate::compaction::purge`] where the reserved key is exempted.
//!
//! ## Reserved-key collision
//!
//! The reserved key is a fixed magic byte string. A user partition key that happens
//! to encode to exactly those bytes would be shadowed by the marker. The magic is
//! long and namespaced specifically to make that accident astronomically unlikely;
//! it is documented here rather than silently assumed.
//!
//! Last revised: 2026-10-09
//! Last changed: New module — table-level tombstone as a replicated write.

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

/// The reserved partition key under which a table's tombstone is stored.
///
/// One constant key serves every table: the LSM (and every read path) is already
/// scoped to a single table, so the key only has to be unique *within* a table.
/// The bytes are a namespaced magic string.
const TABLE_TOMBSTONE_MAGIC: &[u8] = b"\x00ferrosa/table-tombstone/v1";

/// The reserved partition key a table's tombstone is written under and read from.
pub fn table_tombstone_key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(TABLE_TOMBSTONE_MAGIC.to_vec()))
}

/// True when `key` is the reserved table-tombstone partition key.
pub fn is_table_tombstone_key(key: &DecoratedKey) -> bool {
    key.key.as_bytes() == TABLE_TOMBSTONE_MAGIC
}

/// The **partition-tombstone marker row** that carries a table tombstone.
///
/// Shape is deliberately identical to a per-partition `DELETE` tombstone: empty
/// clustering, no cells, a non-`LIVE` [`DeletionTime`]. Written under
/// [`table_tombstone_key`], the memtable lifts it into that partition's
/// `deletion`; nothing about the write path needs to know it means "whole table".
///
/// - `marked_for_delete_at`: microseconds since the epoch — the write timestamp.
///   Rows whose primary-key liveness is older than this are suppressed.
/// - `local_deletion_time`: seconds since the epoch — when the marker was created.
///   Retained for the shared tombstone record; the marker is exempt from purge.
pub fn table_tombstone_row(marked_for_delete_at: i64, local_deletion_time: u32) -> Row {
    Row {
        clustering: Vec::new(),
        cells: Vec::new(),
        deletion: DeletionTime::new(marked_for_delete_at, local_deletion_time),
        primary_key_liveness: LivenessInfo::NONE,
    }
}

/// The newer of two deletion markers by `marked_for_delete_at`. Ties keep `a`.
///
/// Used to fold a table tombstone from several sources (memtable + SSTables) into
/// the single watermark a read applies.
pub fn newest_deletion(a: DeletionTime, b: DeletionTime) -> DeletionTime {
    if b.marked_for_delete_at > a.marked_for_delete_at {
        b
    } else {
        a
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_key_is_stable_and_recognised() {
        let key = table_tombstone_key();
        assert!(is_table_tombstone_key(&key));
        assert_eq!(key, table_tombstone_key());
        // A namespaced magic, not something the row bridge builds for a value.
        assert!(key.key.as_bytes().starts_with(b"\x00ferrosa/"));
    }

    #[test]
    fn marker_row_is_a_partition_tombstone_not_a_row() {
        let row = table_tombstone_row(1_700_000_000_000_000, 1_700_000_000);
        // The exact shape `Memtable::put` lifts into `Partition::deletion`:
        // empty clustering, no cells, a non-LIVE deletion.
        assert!(row.clustering.is_empty());
        assert!(row.cells.is_empty());
        assert_ne!(row.deletion, DeletionTime::LIVE);
        assert_eq!(row.deletion.marked_for_delete_at, 1_700_000_000_000_000);
        assert_eq!(row.deletion.local_deletion_time, 1_700_000_000);
    }

    #[test]
    fn newest_deletion_keeps_the_later_marker_and_ties_on_the_first() {
        let a = DeletionTime::new(10, 1);
        let b = DeletionTime::new(20, 2);
        assert_eq!(newest_deletion(a, b), b);
        assert_eq!(newest_deletion(b, a), b);
        assert_eq!(newest_deletion(a, a), a);
        // LIVE is the sentinel minimum, so a real marker always wins over it.
        assert_eq!(newest_deletion(DeletionTime::LIVE, a), a);
        assert_eq!(newest_deletion(a, DeletionTime::LIVE), a);
    }
}
