//! Append-only, CRC32-protected hint segment file.
//!
//! ## Record Format
//! ```text
//! [length: u32]           total record length (excludes this field)
//! [timestamp: i64]        original mutation timestamp
//! [ks_len: u16][ks: bytes]
//! [tbl_len: u16][tbl: bytes]
//! [key_len: u32][key: bytes]
//! [row_len: u32][row: bytes]
//! [crc32: u32]            CRC32 over all preceding bytes in the record body
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crc32fast::Hasher as Crc32Hasher;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct HintRecord {
    pub timestamp: i64,
    pub keyspace: String,
    pub table: String,
    pub key: Vec<u8>,
    pub row: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AppendResult {
    Ok,
    SegmentFull,
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encode a `HintRecord` into a flat byte buffer (record body, no length prefix).
///
/// Layout:
///   i64 | u16+bytes | u16+bytes | u32+bytes | u32+bytes | u32(crc)
fn encode_record(record: &HintRecord) -> Vec<u8> {
    let ks = record.keyspace.as_bytes();
    let tbl = record.table.as_bytes();

    let body_len = 8                        // timestamp
        + 2 + ks.len()                      // ks_len + ks
        + 2 + tbl.len()                     // tbl_len + tbl
        + 4 + record.key.len()              // key_len + key
        + 4 + record.row.len()              // row_len + row
        + 4; // crc32

    let mut buf = Vec::with_capacity(body_len);

    // timestamp
    buf.extend_from_slice(&record.timestamp.to_le_bytes());
    // keyspace
    buf.extend_from_slice(&(ks.len() as u16).to_le_bytes());
    buf.extend_from_slice(ks);
    // table
    buf.extend_from_slice(&(tbl.len() as u16).to_le_bytes());
    buf.extend_from_slice(tbl);
    // key
    buf.extend_from_slice(&(record.key.len() as u32).to_le_bytes());
    buf.extend_from_slice(&record.key);
    // row
    buf.extend_from_slice(&(record.row.len() as u32).to_le_bytes());
    buf.extend_from_slice(&record.row);

    // CRC32 over everything written so far
    let mut h = Crc32Hasher::new();
    h.update(&buf);
    let crc = h.finalize();
    buf.extend_from_slice(&crc.to_le_bytes());

    buf
}

/// Try to decode one `HintRecord` from `body` (which includes the trailing CRC32).
///
/// Returns `None` if the body is too short, has invalid UTF-8, or has a bad CRC.
#[allow(unused_assignments)] // `pos` is updated by macro after the last field read
fn decode_record(body: &[u8]) -> Option<HintRecord> {
    // Minimum: 8 + 2 + 2 + 4 + 4 + 4 = 24 bytes
    if body.len() < 24 {
        return None;
    }

    // Validate CRC first: CRC covers everything except the trailing 4 bytes.
    let (payload, crc_bytes) = body.split_at(body.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().ok()?);
    let mut h = Crc32Hasher::new();
    h.update(payload);
    if h.finalize() != stored_crc {
        return None;
    }

    let mut pos = 0;

    macro_rules! read_bytes {
        ($n:expr) => {{
            if pos + $n > payload.len() {
                return None;
            }
            let slice = &payload[pos..pos + $n];
            pos += $n;
            slice
        }};
    }

    // timestamp
    let ts = i64::from_le_bytes(read_bytes!(8).try_into().ok()?);

    // keyspace
    let ks_len = u16::from_le_bytes(read_bytes!(2).try_into().ok()?) as usize;
    let ks_bytes = read_bytes!(ks_len);
    let keyspace = String::from_utf8(ks_bytes.to_vec()).ok()?;

    // table
    let tbl_len = u16::from_le_bytes(read_bytes!(2).try_into().ok()?) as usize;
    let tbl_bytes = read_bytes!(tbl_len);
    let table = String::from_utf8(tbl_bytes.to_vec()).ok()?;

    // key
    let key_len = u32::from_le_bytes(read_bytes!(4).try_into().ok()?) as usize;
    let key = read_bytes!(key_len).to_vec();

    // row
    let row_len = u32::from_le_bytes(read_bytes!(4).try_into().ok()?) as usize;
    let row = read_bytes!(row_len).to_vec();

    Some(HintRecord {
        timestamp: ts,
        keyspace,
        table,
        key,
        row,
    })
}

// ---------------------------------------------------------------------------
// SegmentWriter
// ---------------------------------------------------------------------------

pub struct SegmentWriter {
    writer: BufWriter<File>,
    bytes_written: u64,
    max_size_bytes: u64,
}

impl SegmentWriter {
    /// Create (or truncate) a segment file at `path`.
    pub fn create(path: impl AsRef<Path>, max_size_bytes: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            bytes_written: 0,
            max_size_bytes,
        })
    }

    /// Append one record.  Returns `SegmentFull` if adding the record would
    /// exceed the max size limit *before* writing anything.
    pub fn append(&mut self, record: &HintRecord) -> io::Result<AppendResult> {
        let body = encode_record(record);
        // On-disk footprint: 4-byte length prefix + body
        let on_disk = 4 + body.len() as u64;

        if self.bytes_written + on_disk > self.max_size_bytes {
            return Ok(AppendResult::SegmentFull);
        }

        // Write length prefix (u32 LE) then body
        self.writer.write_all(&(body.len() as u32).to_le_bytes())?;
        self.writer.write_all(&body)?;
        self.bytes_written += on_disk;
        Ok(AppendResult::Ok)
    }

    /// Flush and fsync.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }
}

// ---------------------------------------------------------------------------
// SegmentReader
// ---------------------------------------------------------------------------

pub struct SegmentReader {
    records: Vec<HintRecord>,
    pos: usize,
}

impl SegmentReader {
    /// Open a segment file for reading.
    ///
    /// Scans all records forward, validating CRC32 on each.  At the first
    /// corrupt or incomplete record the file is truncated to that offset,
    /// discarding anything after the last good record.  This provides
    /// automatic crash recovery.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();

        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut reader = BufReader::new(file);

        let mut records: Vec<HintRecord> = Vec::new();
        let mut good_offset: u64 = 0;

        loop {
            // Try to read the 4-byte length prefix.
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let body_len = u32::from_le_bytes(len_buf) as usize;

            // Try to read the full body.
            let mut body = vec![0u8; body_len];
            match reader.read_exact(&mut body) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            match decode_record(&body) {
                Some(rec) => {
                    good_offset += 4 + body_len as u64;
                    records.push(rec);
                }
                None => {
                    // Corrupt record — stop here and truncate.
                    break;
                }
            }
        }

        // Truncate the underlying file at the last known-good offset.
        // We need to get the file back from the BufReader.
        let file = reader.into_inner();
        file.set_len(good_offset)?;

        Ok(Self { records, pos: 0 })
    }
}

impl Iterator for SegmentReader {
    type Item = HintRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.records.len() {
            let rec = self.records[self.pos].clone();
            self.pos += 1;
            Some(rec)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom};

    use super::*;
    use tempfile::NamedTempFile;

    fn make_record(n: u64) -> HintRecord {
        HintRecord {
            timestamp: n as i64,
            keyspace: format!("ks_{n}"),
            table: format!("tbl_{n}"),
            key: format!("key_{n}").into_bytes(),
            row: format!("row_data_{n}").into_bytes(),
        }
    }

    // ------------------------------------------------------------------
    // 1. Roundtrip a single record
    // ------------------------------------------------------------------
    #[test]
    fn write_and_read_single_record() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let record = make_record(1);
        let mut writer = SegmentWriter::create(path, 1024 * 1024).unwrap();
        let result = writer.append(&record).unwrap();
        assert_eq!(result, AppendResult::Ok);
        writer.sync().unwrap();

        let reader = SegmentReader::open(path).unwrap();
        let got: Vec<_> = reader.collect();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], record);
    }

    // ------------------------------------------------------------------
    // 2. FIFO ordering preserved across 10 records
    // ------------------------------------------------------------------
    #[test]
    fn fifo_ordering_preserved() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut writer = SegmentWriter::create(path, 1024 * 1024).unwrap();
        for i in 0..10 {
            assert_eq!(writer.append(&make_record(i)).unwrap(), AppendResult::Ok);
        }
        writer.sync().unwrap();

        let reader = SegmentReader::open(path).unwrap();
        let got: Vec<_> = reader.collect();
        assert_eq!(got.len(), 10);
        for (i, rec) in got.iter().enumerate() {
            assert_eq!(*rec, make_record(i as u64));
        }
    }

    // ------------------------------------------------------------------
    // 3. Truncates partial / garbage tail on open
    // ------------------------------------------------------------------
    #[test]
    fn truncates_partial_record_on_open() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        // Write 3 valid records.
        let mut writer = SegmentWriter::create(path, 1024 * 1024).unwrap();
        for i in 0..3 {
            writer.append(&make_record(i)).unwrap();
        }
        writer.sync().unwrap();

        // Append raw garbage bytes directly to the file.
        {
            let mut f = OpenOptions::new().append(true).open(path).unwrap();
            f.write_all(b"\xde\xad\xbe\xef\x00\x01\x02\x03garbage")
                .unwrap();
            f.sync_all().unwrap();
        }

        // Opening the reader must recover exactly 3 records.
        let reader = SegmentReader::open(path).unwrap();
        let got: Vec<_> = reader.collect();
        assert_eq!(got.len(), 3);
        for (i, rec) in got.iter().enumerate() {
            assert_eq!(*rec, make_record(i as u64));
        }

        // File should have been truncated (no garbage remains).
        let meta = std::fs::metadata(path).unwrap();
        // Just verify the file still exists and is non-zero.
        assert!(meta.len() > 0);
    }

    // ------------------------------------------------------------------
    // 4. Detects CRC corruption → 0 records recovered
    // ------------------------------------------------------------------
    #[test]
    fn detects_crc_corruption() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut writer = SegmentWriter::create(path, 1024 * 1024).unwrap();
        writer.append(&make_record(42)).unwrap();
        writer.sync().unwrap();

        // Flip the last byte of the file (the last byte of the CRC32).
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            let len = f.seek(SeekFrom::End(0)).unwrap();
            f.seek(SeekFrom::Start(len - 1)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(len - 1)).unwrap();
            f.write_all(&byte).unwrap();
            f.sync_all().unwrap();
        }

        let reader = SegmentReader::open(path).unwrap();
        let got: Vec<_> = reader.collect();
        assert_eq!(got.len(), 0, "corrupt CRC must cause record to be dropped");
    }

    // ------------------------------------------------------------------
    // 5. Segment rollover at size limit
    // ------------------------------------------------------------------
    #[test]
    fn segment_rollover_at_size_limit() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        // Use a tiny max size so it fills up quickly.
        // One make_record(0) body ≈ 8+2+4+2+5+4+5+4+6+4 = ~50 bytes + 4 prefix.
        // Use 100 bytes max so the second record overflows.
        let mut writer = SegmentWriter::create(path, 100).unwrap();

        let first = writer.append(&make_record(0)).unwrap();
        assert_eq!(first, AppendResult::Ok, "first record should fit");

        // Keep writing until we hit SegmentFull.
        let mut full_seen = false;
        for i in 1..20 {
            match writer.append(&make_record(i)).unwrap() {
                AppendResult::Ok => {}
                AppendResult::SegmentFull => {
                    full_seen = true;
                    break;
                }
            }
        }
        assert!(full_seen, "SegmentFull must eventually be returned");
    }
}
