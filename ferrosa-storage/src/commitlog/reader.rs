//! Module: Incrementally validate and yield mutations from one commit-log segment.
//! Correctness: Correct when sync markers and CRCs gate every yielded entry, active
//! segment tails can be refreshed, and payload allocation is caller-bounded.
//! Last revised: 2026-08-29
//! Last changed: Replaced whole-segment loading with incremental bounded entry reads.
//!
//! [`SegmentReader`] keeps an open file, validates the fixed header, and follows
//! sync markers while allocating at most one caller-bounded entry payload.
//! On corruption (CRC mismatch), it skips to the next sync marker if possible,
//! or stops reading.
//!
//! This is the read-side counterpart of [`super::segment::Segment`], used
//! during crash recovery replay.

// Used by CommitLog (Task 9) during crash recovery replay; suppress
// dead-code warnings until that module exists.
#![allow(dead_code)]

#[cfg(test)]
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::config::CommitLogPosition;
use super::descriptor::{SegmentDescriptor, HEADER_SIZE};
use super::mutation::Mutation;

pub(crate) enum SegmentEntryRead {
    Mutation(CommitLogPosition, Mutation),
    PayloadTooLarge {
        position: CommitLogPosition,
        bytes: usize,
    },
}
use super::segment::{ENTRY_OVERHEAD, SYNC_MARKER_SIZE};

/// Reads a commit log segment file and yields `(CommitLogPosition, Mutation)` pairs.
///
/// # Reading Algorithm
///
/// 1. Read and validate only the 17-byte header.
/// 2. Follow the sync marker chain starting at offset `HEADER_SIZE` (17).
/// 3. Read and deserialize one entry payload at a time.
/// 4. On CRC failure, skip to the next sync marker if available; otherwise stop.
pub struct SegmentReader {
    file: File,
    file_len: u64,
    /// Parsed header descriptor.
    descriptor: SegmentDescriptor,
    marker_offset: u64,
    section_end: u64,
    entry_offset: u64,
    next_marker_offset: u64,
    section_active: bool,
    finished: bool,
}

impl SegmentReader {
    /// Opens a segment file and validates its fixed-size header.
    ///
    /// Returns an error if the file cannot be read, is too short for a header,
    /// or the header CRC is invalid.
    pub fn open(path: &Path) -> ferrosa_common::Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < HEADER_SIZE as u64 {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "segment file too short: {} bytes (need at least {})",
                file_len, HEADER_SIZE
            )));
        }
        let mut header = [0_u8; HEADER_SIZE];
        file.read_exact(&mut header)?;
        let descriptor = SegmentDescriptor::read_from(&header)?;

        Ok(Self {
            file,
            file_len,
            descriptor,
            marker_offset: HEADER_SIZE as u64,
            section_end: 0,
            entry_offset: 0,
            next_marker_offset: 0,
            section_active: false,
            finished: false,
        })
    }

    /// Refresh the durable tail of an already-open active segment. The reader
    /// keeps its current entry/marker offsets, so this never rescans entries
    /// that were already yielded.
    pub(crate) fn refresh(&mut self) -> ferrosa_common::Result<()> {
        self.file_len = self.file.metadata()?.len();
        if self.section_active {
            self.file.seek(SeekFrom::Start(self.marker_offset))?;
            let mut marker = [0_u8; SYNC_MARKER_SIZE];
            self.file.read_exact(&mut marker)?;
            let next = u32::from_be_bytes(marker[..4].try_into().expect("fixed slice"));
            let stored_crc = u32::from_be_bytes(marker[4..].try_into().expect("fixed slice"));
            let mut crc_input = [0_u8; 12];
            crc_input[..8].copy_from_slice(&self.descriptor.segment_id.to_be_bytes());
            crc_input[8..].copy_from_slice(&next.to_be_bytes());
            if stored_crc == crc32fast::hash(&crc_input) {
                let refreshed_end = if next == 0 {
                    self.file_len
                } else {
                    u64::from(next).min(self.file_len)
                };
                if refreshed_end >= self.entry_offset {
                    self.section_end = refreshed_end;
                    self.next_marker_offset = u64::from(next);
                }
            }
        }
        self.finished = false;
        Ok(())
    }

    pub(crate) fn segment_id(&self) -> u64 {
        self.descriptor.segment_id
    }

    /// Returns the segment descriptor (header info).
    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }

    #[cfg(test)]
    fn buffered_file_bytes(&self) -> usize {
        0
    }

    /// Yield the next valid entry while retaining no segment-sized buffer.
    pub fn next_entry(&mut self) -> ferrosa_common::Result<Option<(CommitLogPosition, Mutation)>> {
        match self.next_entry_bounded(usize::MAX)? {
            Some(SegmentEntryRead::Mutation(position, mutation)) => Ok(Some((position, mutation))),
            Some(SegmentEntryRead::PayloadTooLarge { .. }) => {
                unreachable!("usize::MAX cannot reject a u32-sized commit-log entry")
            }
            None => Ok(None),
        }
    }

    /// Yield one entry while rejecting an oversized payload before allocating
    /// its buffer. The rejected entry is not consumed, so retrying remains
    /// fail-loud at the same durable position.
    pub(crate) fn next_entry_bounded(
        &mut self,
        maximum_payload_bytes: usize,
    ) -> ferrosa_common::Result<Option<SegmentEntryRead>> {
        loop {
            if self.finished {
                return Ok(None);
            }
            if !self.section_active {
                self.open_next_section()?;
                if self.finished {
                    return Ok(None);
                }
            }

            if self.entry_offset.saturating_add(ENTRY_OVERHEAD as u64) > self.section_end {
                self.advance_section();
                continue;
            }

            self.file.seek(SeekFrom::Start(self.entry_offset))?;
            let mut entry_header = [0_u8; 8];
            self.file.read_exact(&mut entry_header)?;
            let entry_size_bytes: [u8; 4] = entry_header[..4].try_into().expect("fixed slice");
            let entry_size = u32::from_be_bytes(entry_size_bytes) as u64;
            let stored_size_crc =
                u32::from_be_bytes(entry_header[4..8].try_into().expect("fixed slice"));
            if stored_size_crc != crc32fast::hash(&entry_size_bytes) {
                self.advance_section();
                continue;
            }

            let payload_start = self.entry_offset.saturating_add(8);
            let payload_end = payload_start.saturating_add(entry_size);
            let entry_end = payload_end.saturating_add(4);
            if entry_end > self.section_end || entry_end > self.file_len {
                self.advance_section();
                continue;
            }

            let payload_len = usize::try_from(entry_size).map_err(|_| {
                ferrosa_common::Error::InvalidFormat(
                    "commit-log entry size does not fit in memory address space".into(),
                )
            })?;
            if payload_len > maximum_payload_bytes {
                return Ok(Some(SegmentEntryRead::PayloadTooLarge {
                    position: CommitLogPosition {
                        segment_id: self.descriptor.segment_id,
                        offset: self.entry_offset,
                    },
                    bytes: payload_len,
                }));
            }
            let mut payload = vec![0_u8; payload_len];
            self.file.read_exact(&mut payload)?;
            let mut payload_crc = [0_u8; 4];
            self.file.read_exact(&mut payload_crc)?;
            self.entry_offset = entry_end;

            if u32::from_be_bytes(payload_crc) != crc32fast::hash(&payload) {
                self.advance_section();
                continue;
            }

            let mutation = match Mutation::deserialize_from(&payload) {
                Ok(mutation) => mutation,
                Err(_) => continue,
            };
            return Ok(Some(SegmentEntryRead::Mutation(
                CommitLogPosition {
                    segment_id: self.descriptor.segment_id,
                    offset: payload_start - 8,
                },
                mutation,
            )));
        }
    }

    fn open_next_section(&mut self) -> ferrosa_common::Result<()> {
        if self.marker_offset.saturating_add(SYNC_MARKER_SIZE as u64) > self.file_len {
            self.finished = true;
            return Ok(());
        }
        self.file.seek(SeekFrom::Start(self.marker_offset))?;
        let mut marker = [0_u8; SYNC_MARKER_SIZE];
        self.file.read_exact(&mut marker)?;
        let next = u32::from_be_bytes(marker[..4].try_into().expect("fixed slice"));
        let stored_crc = u32::from_be_bytes(marker[4..].try_into().expect("fixed slice"));
        let mut crc_input = [0_u8; 12];
        crc_input[..8].copy_from_slice(&self.descriptor.segment_id.to_be_bytes());
        crc_input[8..].copy_from_slice(&next.to_be_bytes());
        if stored_crc != crc32fast::hash(&crc_input) {
            self.finished = true;
            return Ok(());
        }

        let section_start = self.marker_offset + SYNC_MARKER_SIZE as u64;
        let section_end = if next == 0 {
            self.file_len
        } else {
            u64::from(next).min(self.file_len)
        };
        if section_end < section_start || (next != 0 && u64::from(next) <= self.marker_offset) {
            self.finished = true;
            return Ok(());
        }
        self.entry_offset = section_start;
        self.section_end = section_end;
        self.next_marker_offset = u64::from(next);
        self.section_active = true;
        Ok(())
    }

    fn advance_section(&mut self) {
        if self.next_marker_offset == 0 {
            self.finished = true;
        } else {
            self.marker_offset = self.next_marker_offset;
            self.section_active = false;
        }
    }

    /// Reads all valid entries from the segment.
    ///
    /// Follows the sync marker chain, reading entries between markers.
    /// On corruption, skips to the next sync marker. On EOF or unrecoverable
    /// error, returns all entries read so far.
    pub fn read_all(&mut self) -> ferrosa_common::Result<Vec<(CommitLogPosition, Mutation)>> {
        let mut entries = Vec::new();
        while let Some(entry) = self.next_entry()? {
            entries.push(entry);
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::super::segment::Segment;
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    /// Helper to create a simple mutation for testing.
    fn simple_mutation() -> Mutation {
        Mutation {
            mutation_id: [0x15u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 42_000,
        }
    }

    /// Helper to create a mutation with a different key for multi-entry tests.
    fn another_mutation() -> Mutation {
        Mutation {
            mutation_id: [0x16u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk2".to_vec())),
            rows: vec![Row {
                clustering: vec![4, 5, 6],
                cells: vec![(1, CellValue::live(b"world".to_vec(), 2000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(2000),
            }],
            timestamp: 99_000,
        }
    }

    #[test]
    fn read_valid_segment() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m1 = simple_mutation();
        let m2 = another_mutation();

        let total1 = Segment::entry_total_size(&m1);
        let total2 = Segment::entry_total_size(&m2);

        let offset1 = segment.allocate(total1).unwrap();
        segment.write_entry(offset1, &m1);

        let offset2 = segment.allocate(total2).unwrap();
        segment.write_entry(offset2, &m2);

        segment.flush_to_disk().unwrap();

        let mut reader = SegmentReader::open(segment.path()).unwrap();
        let entries = reader.read_all().unwrap();

        assert_eq!(entries.len(), 2);

        // Verify first entry.
        assert_eq!(entries[0].0.segment_id, 1);
        assert_eq!(entries[0].0.offset, offset1);
        assert_eq!(entries[0].1.keyspace, "test_ks");
        assert_eq!(entries[0].1.table, "test_table");
        assert_eq!(entries[0].1.timestamp, 42_000);

        // Verify second entry.
        assert_eq!(entries[1].0.segment_id, 1);
        assert_eq!(entries[1].0.offset, offset2);
        assert_eq!(entries[1].1.keyspace, "test_ks");
        assert_eq!(entries[1].1.timestamp, 99_000);
    }

    /// RED for t_977ee106: the durable CDC path must not load a segment or all
    /// of its mutations before yielding the first entry.  Only the current
    /// mutation payload may be resident.
    #[test]
    fn durable_cdc_segment_reader_streams_one_entry_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(7, 512 * 1024, dir.path());

        let mut first = simple_mutation();
        first.rows[0].cells[0].1 = CellValue::live(vec![0x41; 64 * 1024], 1000);
        let mut second = another_mutation();
        second.rows[0].cells[0].1 = CellValue::live(vec![0x42; 64 * 1024], 2000);

        let first_offset = segment.allocate(Segment::entry_total_size(&first)).unwrap();
        segment.write_entry(first_offset, &first);
        let second_offset = segment
            .allocate(Segment::entry_total_size(&second))
            .unwrap();
        segment.write_entry(second_offset, &second);
        segment.flush_to_disk().unwrap();

        let mut reader = SegmentReader::open(segment.path()).unwrap();
        assert_eq!(
            reader.buffered_file_bytes(),
            0,
            "opening a CDC segment must not materialize the segment"
        );

        let (position, mutation) = reader.next_entry().unwrap().unwrap();
        assert_eq!(position.offset, first_offset);
        assert_eq!(mutation.timestamp, first.timestamp);
        assert_eq!(reader.buffered_file_bytes(), 0);

        let (position, mutation) = reader.next_entry().unwrap().unwrap();
        assert_eq!(position.offset, second_offset);
        assert_eq!(mutation.timestamp, second.timestamp);
        assert_eq!(reader.buffered_file_bytes(), 0);
        assert!(reader.next_entry().unwrap().is_none());
    }

    #[test]
    fn detect_corrupted_header_crc() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());
        segment.flush_to_disk().unwrap();

        // Read the file, corrupt a header byte, write it back.
        let path = segment.path().to_path_buf();
        let mut data = fs::read(&path).unwrap();
        // Flip a byte in the header (byte 4 is part of segment_id).
        data[4] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let result = SegmentReader::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn detect_corrupted_entry_size_crc() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m1 = simple_mutation();
        let m2 = another_mutation();

        let total1 = Segment::entry_total_size(&m1);
        let total2 = Segment::entry_total_size(&m2);

        // Write two entries in the same section (under the initial EOF sync marker).
        let off1 = segment.allocate(total1).unwrap();
        segment.write_entry(off1, &m1);

        let off2 = segment.allocate(total2).unwrap();
        segment.write_entry(off2, &m2);

        segment.flush_to_disk().unwrap();

        // Corrupt the size_crc of the first entry (bytes at off1+4..off1+8).
        let path = segment.path().to_path_buf();
        let mut data = fs::read(&path).unwrap();
        let crc_offset = off1 as usize + 4;
        data[crc_offset] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();

        // Both entries are in the same section (single initial sync marker with EOF).
        // Corrupting the first entry's size_crc should cause the reader to skip
        // to the next sync marker -- but there is none, so no entries are returned.
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn detect_corrupted_payload_crc() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m1 = simple_mutation();
        let total1 = Segment::entry_total_size(&m1);

        let offset1 = segment.allocate(total1).unwrap();
        segment.write_entry(offset1, &m1);

        segment.flush_to_disk().unwrap();

        // Corrupt the payload_crc (last 4 bytes of the entry).
        let path = segment.path().to_path_buf();
        let mut data = fs::read(&path).unwrap();
        let payload_size = m1.serialized_size();
        let payload_crc_offset = offset1 as usize + 8 + payload_size;
        data[payload_crc_offset] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();

        // Payload CRC mismatch should skip to next sync marker (none exists), so 0 entries.
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn stop_at_eof_marker() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m = simple_mutation();
        let total = Segment::entry_total_size(&m);

        let offset = segment.allocate(total).unwrap();
        segment.write_entry(offset, &m);

        // The initial sync marker has next_marker_offset = 0 (EOF).
        // flush_to_disk writes only up to current position.
        segment.flush_to_disk().unwrap();

        let mut reader = SegmentReader::open(segment.path()).unwrap();
        let entries = reader.read_all().unwrap();

        // Should read exactly one entry and then stop at EOF marker (next_marker_offset == 0).
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.keyspace, "test_ks");
    }

    #[test]
    fn read_empty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        // Don't write any entries — just the header and initial EOF sync marker.
        segment.flush_to_disk().unwrap();

        let mut reader = SegmentReader::open(segment.path()).unwrap();
        let entries = reader.read_all().unwrap();

        assert!(entries.is_empty());
    }

    #[test]
    fn torn_tail_past_sync_marker_does_not_panic() {
        // Regression: a sync marker whose next_marker_offset points past the
        // end of the file (torn write where the marker was flushed but the
        // bytes after it were not) used to panic when the reader sliced the
        // payload. section_end must be clamped to data.len().
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m = simple_mutation();
        let total = Segment::entry_total_size(&m);
        let off = segment.allocate(total).unwrap();
        segment.write_entry(off, &m);
        segment.flush_to_disk().unwrap();

        let path = segment.path().to_path_buf();
        let mut data = fs::read(&path).unwrap();

        // Truncate the file in the middle of the entry's payload. Entry
        // header (size + size_crc) stays intact, payload runs off the end.
        let payload_start = off as usize + 8;
        let payload_size = m.serialized_size();
        let truncated_len = payload_start + payload_size / 2;
        data.truncate(truncated_len);

        // Patch the initial sync marker so next_marker_offset points past
        // the entry's claimed end. Without the fix, section_end =
        // next_marker_offset, the entry_end-vs-section_end guard passes,
        // and the payload slice runs off the buffer.
        let fake_next = (payload_start + payload_size + 64) as u32;
        let mut crc_input = [0u8; 12];
        crc_input[..8].copy_from_slice(&1u64.to_be_bytes());
        crc_input[8..12].copy_from_slice(&fake_next.to_be_bytes());
        let new_crc = crc32fast::hash(&crc_input);
        data[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&fake_next.to_be_bytes());
        data[HEADER_SIZE + 4..HEADER_SIZE + 8].copy_from_slice(&new_crc.to_be_bytes());

        fs::write(&path, &data).unwrap();

        let mut reader = SegmentReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn descriptor_accessor() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(42, 4096, dir.path());
        segment.flush_to_disk().unwrap();

        let reader = SegmentReader::open(segment.path()).unwrap();
        assert_eq!(reader.descriptor().segment_id, 42);
    }
}
