//! Segment reader: reads a commit log segment file and yields mutations.
//!
//! [`SegmentReader`] reads a segment file into memory, validates the 17-byte
//! header, then follows the sync marker chain to extract all valid mutation
//! entries. On corruption (CRC mismatch), it skips to the next sync marker
//! if possible, or stops reading.
//!
//! This is the read-side counterpart of [`super::segment::Segment`], used
//! during crash recovery replay.

// Used by CommitLog (Task 9) during crash recovery replay; suppress
// dead-code warnings until that module exists.
#![allow(dead_code)]

use std::fs;
use std::path::Path;

use super::config::CommitLogPosition;
use super::descriptor::{SegmentDescriptor, HEADER_SIZE};
use super::mutation::Mutation;
use super::segment::{ENTRY_OVERHEAD, SYNC_MARKER_SIZE};

/// Reads a commit log segment file and yields `(CommitLogPosition, Mutation)` pairs.
///
/// # Reading Algorithm
///
/// 1. Read entire file into memory.
/// 2. Validate the 17-byte header via [`SegmentDescriptor::read_from()`].
/// 3. Follow the sync marker chain starting at offset `HEADER_SIZE` (17).
/// 4. Within each section (between two sync markers), read entries sequentially.
/// 5. On CRC failure, skip to the next sync marker if available; otherwise stop.
pub struct SegmentReader {
    /// Raw file contents.
    data: Vec<u8>,
    /// Parsed header descriptor.
    descriptor: SegmentDescriptor,
}

impl SegmentReader {
    /// Opens a segment file, reads it into memory, and validates the header.
    ///
    /// Returns an error if the file cannot be read, is too short for a header,
    /// or the header CRC is invalid.
    pub fn open(path: &Path) -> ferrosa_common::Result<Self> {
        let data = fs::read(path)?;

        if data.len() < HEADER_SIZE {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "segment file too short: {} bytes (need at least {})",
                data.len(),
                HEADER_SIZE
            )));
        }

        let descriptor = SegmentDescriptor::read_from(&data[..HEADER_SIZE])?;

        Ok(Self { data, descriptor })
    }

    /// Returns the segment descriptor (header info).
    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }

    /// Reads all valid entries from the segment.
    ///
    /// Follows the sync marker chain, reading entries between markers.
    /// On corruption, skips to the next sync marker. On EOF or unrecoverable
    /// error, returns all entries read so far.
    pub fn read_all(&mut self) -> ferrosa_common::Result<Vec<(CommitLogPosition, Mutation)>> {
        let mut entries = Vec::new();
        let mut marker_offset = HEADER_SIZE;

        loop {
            // Check if we have enough bytes for a sync marker at this offset.
            if marker_offset + SYNC_MARKER_SIZE > self.data.len() {
                break;
            }

            // Read sync marker: next_marker_offset (u32) + marker_crc (u32).
            let next_marker_offset = u32::from_be_bytes([
                self.data[marker_offset],
                self.data[marker_offset + 1],
                self.data[marker_offset + 2],
                self.data[marker_offset + 3],
            ]);

            let stored_marker_crc = u32::from_be_bytes([
                self.data[marker_offset + 4],
                self.data[marker_offset + 5],
                self.data[marker_offset + 6],
                self.data[marker_offset + 7],
            ]);

            // Validate marker CRC: crc32(segment_id.to_be_bytes() || next_marker_offset.to_be_bytes())
            let mut crc_input = [0u8; 12];
            crc_input[..8].copy_from_slice(&self.descriptor.segment_id.to_be_bytes());
            crc_input[8..12].copy_from_slice(&next_marker_offset.to_be_bytes());
            let expected_marker_crc = crc32fast::hash(&crc_input);

            if stored_marker_crc != expected_marker_crc {
                // Marker CRC invalid — cannot trust next_marker_offset, stop reading.
                break;
            }

            // Determine the end of this section's entry data.
            let section_start = marker_offset + SYNC_MARKER_SIZE;
            let section_end = if next_marker_offset == 0 {
                // EOF marker: read entries until the end of the data.
                self.data.len()
            } else {
                // Clamp to file length: a torn tail can leave a valid sync
                // marker pointing past EOF. Treat the missing bytes as
                // truncated entries so the payload slice below can't panic.
                (next_marker_offset as usize).min(self.data.len())
            };

            // Read entries within this section.
            let mut pos = section_start;
            while pos + ENTRY_OVERHEAD <= section_end {
                // Read entry_size (u32).
                let entry_size_bytes = [
                    self.data[pos],
                    self.data[pos + 1],
                    self.data[pos + 2],
                    self.data[pos + 3],
                ];
                let entry_size = u32::from_be_bytes(entry_size_bytes) as usize;

                // Read size_crc (u32).
                let stored_size_crc = u32::from_be_bytes([
                    self.data[pos + 4],
                    self.data[pos + 5],
                    self.data[pos + 6],
                    self.data[pos + 7],
                ]);

                // Validate size CRC.
                let expected_size_crc = crc32fast::hash(&entry_size_bytes);
                if stored_size_crc != expected_size_crc {
                    // Size CRC mismatch — skip to next sync marker.
                    break;
                }

                // Check we have enough data for payload + payload_crc.
                let payload_start = pos + 8;
                let payload_end = payload_start + entry_size;
                let entry_end = payload_end + 4; // + payload_crc

                if entry_end > section_end {
                    // Truncated entry — stop reading this section.
                    break;
                }

                // Read and validate payload CRC.
                let payload = &self.data[payload_start..payload_end];
                let stored_payload_crc = u32::from_be_bytes([
                    self.data[payload_end],
                    self.data[payload_end + 1],
                    self.data[payload_end + 2],
                    self.data[payload_end + 3],
                ]);
                let expected_payload_crc = crc32fast::hash(payload);

                if stored_payload_crc != expected_payload_crc {
                    // Payload CRC mismatch — skip to next sync marker.
                    break;
                }

                // Deserialize the mutation.
                match Mutation::deserialize_from(payload) {
                    Ok(mutation) => {
                        let commit_pos = CommitLogPosition {
                            segment_id: self.descriptor.segment_id,
                            offset: pos as u64,
                        };
                        entries.push((commit_pos, mutation));
                    }
                    Err(_) => {
                        // Deserialization failure — skip this entry, continue to next.
                        pos = entry_end;
                        continue;
                    }
                }

                pos = entry_end;
            }

            // Move to next sync marker, or stop if this was the EOF marker.
            if next_marker_offset == 0 {
                break;
            }
            marker_offset = next_marker_offset as usize;
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
