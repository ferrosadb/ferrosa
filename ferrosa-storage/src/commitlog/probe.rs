//! Safe, synchronous access to production segment operations for experiments.
//! Correctness: reservation and write remain paired; no raw offsets escape for writing.
//! Last revised: 2026-10-29
//! Last changed: Kept unsafe segment preconditions private behind exclusive operations.

use super::{CommitLogPosition, Mutation, Segment};
use std::path::Path;

/// An isolated experimental segment. Exclusive access prevents concurrent
/// mutation/flush and keeps raw reservation/buffer APIs out of the public surface.
pub struct ProbeSegment {
    segment: Segment,
}

struct Writer<'a>(&'a Segment);

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        self.0.writer_done();
    }
}

impl ProbeSegment {
    /// Prepare one segment; setup is deliberately outside measured windows.
    pub fn new(id: u64, capacity: usize, directory: &Path) -> Self {
        Self {
            segment: Segment::new(id, capacity, directory),
        }
    }

    /// Size of the real encoded entry, including its CRC framing.
    #[inline]
    pub fn entry_total_size(mutation: &Mutation) -> usize {
        Segment::entry_total_size(mutation)
    }

    /// Reserve and encode a mutation; return None if the prepared segment is full.
    #[inline]
    pub fn append(&mut self, mutation: &Mutation) -> Option<CommitLogPosition> {
        let offset = self
            .segment
            .allocate_and_begin_write(Self::entry_total_size(mutation))?;
        let _writer = Writer(&self.segment);
        Some(self.segment.write_entry(offset, mutation))
    }

    /// Exercise the borrowed single-row encoder without constructing/cloning rows.
    /// Refuse non-single-row input before reserving any buffer space.
    #[inline]
    pub fn append_single(&mut self, mutation: &Mutation) -> Option<CommitLogPosition> {
        let [row] = mutation.rows.as_slice() else {
            return None;
        };
        let offset = self
            .segment
            .allocate_and_begin_write(Self::entry_total_size(mutation))?;
        let _writer = Writer(&self.segment);
        Some(self.segment.write_single_row_entry(
            offset,
            mutation.mutation_id,
            &mutation.keyspace,
            &mutation.table,
            &mutation.key,
            row,
            mutation.timestamp,
        ))
    }

    /// Sync only this segment using the production durability implementation.
    pub fn flush_to_disk(&mut self) -> ferrosa_common::Result<()> {
        self.segment.flush_to_disk()
    }

    /// Path used to open a production incremental reader after dropping this owner.
    pub fn path(&self) -> &Path {
        self.segment.path()
    }
}
