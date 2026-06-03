//! Data.db reader for Cassandra BTI SSTables.
//!
//! The Data.db file contains serialized partitions in token order. Each
//! partition consists of a header (key + deletion time) followed by zero or
//! more rows, terminated by an `END_OF_PARTITION` sentinel byte.
//!
//! Timestamps, TTLs, and local deletion times are **delta-encoded** against
//! the baseline values stored in the [`SerializationHeader`] from Statistics.db.
//! Deltas are written as **unsigned varints** (not zigzag-encoded).
//!
//! # Deferred
//!
//! - Range tombstone markers
//! - Complex columns (collections, UDTs, frozen types)
//!
//! [`SerializationHeader`]: crate::statistics::SerializationHeader

use ferrosa_common::{CellValue, DecoratedKey, Error, PartitionKey, Result};

use crate::io::ReadAt;
use crate::marshal;
use crate::statistics::SerializationHeader;
use crate::types::{DeletionTime, LivenessInfo, Partition, Row};
use crate::varint;

/// Upper bound on any single length-prefixed buffer (clustering value or cell
/// value) read from a Data.db file. A larger on-disk length means the SSTable
/// is corrupt; it must be rejected *before* it drives an allocation, otherwise
/// a bogus varint (a corrupt SSTable once encoded a multi-terabyte length here)
/// would attempt a pathological `Vec` allocation and OOM the process.
const MAX_VALUE_LEN: usize = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Row flags (bit positions in the flags byte preceding each unfiltered row)
// Reference: UnfilteredSerializer.java lines 108-118
// ---------------------------------------------------------------------------

/// Marks the end of a partition's row sequence.
const END_OF_PARTITION: u8 = 0x01;
/// Whether the encoded unfiltered is a range tombstone marker (not a row).
const IS_MARKER: u8 = 0x02;
/// Row has a non-default timestamp (delta-encoded).
const HAS_TIMESTAMP: u8 = 0x04;
/// Row has a TTL (delta-encoded).
const HAS_TTL: u8 = 0x08;
/// Row has a row-level deletion time.
const HAS_DELETION: u8 = 0x10;
/// All columns in the schema are present (no missing-column bitmap).
const HAS_ALL_COLUMNS: u8 = 0x20;
/// Row has complex column deletion for at least one column.
#[allow(dead_code)]
const HAS_COMPLEX_DELETION: u8 = 0x40;
/// Extended flags byte follows.
const EXTENSION_FLAG: u8 = 0x80;

// ---------------------------------------------------------------------------
// Extended flags (second byte, only present if EXTENSION_FLAG is set)
// Reference: UnfilteredSerializer.java lines 120-131
// ---------------------------------------------------------------------------

/// This row is a static row (no clustering key).
const EXT_IS_STATIC: u8 = 0x01;

// ---------------------------------------------------------------------------
// Cell flags
// Reference: Cell.java lines 279-283
// ---------------------------------------------------------------------------

/// Cell is a tombstone (deleted).
const CELL_IS_DELETED: u8 = 0x01;
/// Cell is expiring (has TTL).
const CELL_IS_EXPIRING: u8 = 0x02;
/// Cell has an empty value (tombstones, counters).
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
/// Cell inherits the row-level timestamp (no per-cell timestamp encoded).
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
/// Cell inherits the row-level TTL.
const CELL_USE_ROW_TTL: u8 = 0x10;

// ---------------------------------------------------------------------------
// Partition-level DeletionTime format (Cassandra 5.x UInt format)
// Reference: DeletionTime.java lines 216-254
// ---------------------------------------------------------------------------

/// Byte marker for a live (not deleted) partition. Since real timestamps are
/// positive, the MSB of `markedForDeleteAt` is always 0 for non-live
/// deletions, so `0x80` in the first byte signals LIVE.
const DELETION_IS_LIVE: u8 = 0x80;

// ---------------------------------------------------------------------------
// DataReader
// ---------------------------------------------------------------------------

/// Reads partitions sequentially from a Data.db file.
///
/// The reader tracks its current byte position and advances through the file
/// as partitions are read. It requires a [`SerializationHeader`] for delta
/// decoding timestamps and TTLs.
pub struct DataReader<'a, R: ReadAt> {
    reader: &'a R,
    header: &'a SerializationHeader,
    pos: u64,
    legacy_fixed_value_lengths: bool,
}

impl<'a, R: ReadAt> DataReader<'a, R> {
    /// Create a new `DataReader` starting at `start_pos` in the data file.
    pub fn new(reader: &'a R, header: &'a SerializationHeader, start_pos: u64) -> Self {
        Self {
            reader,
            header,
            pos: start_pos,
            legacy_fixed_value_lengths: false,
        }
    }

    fn missing_partition_end_error() -> Error {
        Error::from(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unexpected EOF: missing END_OF_PARTITION marker after row data",
        ))
    }

    /// Read the next partition from the data file.
    ///
    /// Returns `Ok(None)` when the reader has reached EOF.
    pub fn read_partition(&mut self) -> Result<Option<Partition>> {
        self.read_partition_limited_rows(0)
    }

    /// Read the next partition, retaining at most `row_limit` clustered rows.
    ///
    /// A `row_limit` of 0 means unlimited.  When a positive limit is reached,
    /// remaining rows are skipped using the encoded row-body size instead of
    /// being fully decoded and materialized.  This keeps range scans with
    /// per-partition row caps bounded even when individual partitions contain
    /// many rows.
    /// Read the next partition's header (key, partition-level
    /// deletion, optional static row) and invoke `on_row` once per
    /// clustered row in order. Each row is decoded into a
    /// freshly-allocated `Row`, handed to the callback by reference,
    /// and dropped before the next row's bytes are read from the
    /// underlying reader. **Peak working set during the call is one
    /// row** — independent of how many rows the partition has or
    /// how big each cell value is.
    ///
    /// This is the read-side counterpart of
    /// `ferrosa_cluster::raft::handlers::PartitionDigestStream`:
    /// together they let anti-entropy repair hash a multi-MB
    /// partition without ever materialising a `Partition` struct.
    ///
    /// Returns `Ok(None)` at EOF. On any decode error the call
    /// stops at the failing row boundary and returns the error;
    /// `self.pos` is left at a partition-boundary or end-of-stream
    /// position depending on how far the walk got.
    /// 2-phase streaming entry point. Reads the partition header
    /// (key + deletion + optional static row) and **leaves
    /// `self.pos` parked at the first clustered row** (or at the
    /// `END_OF_PARTITION` marker if there are no clustered rows).
    ///
    /// The static row, if present, comes FIRST in the BTI row
    /// section. This function peeks the first row's flags byte
    /// and decodes only if `IS_STATIC`; otherwise it leaves the
    /// byte un-consumed so the follow-up [`Self::stream_clustered_rows`]
    /// call re-reads it.
    ///
    /// Returns `Ok(None)` at EOF.
    pub fn read_partition_header_only(
        &mut self,
    ) -> Result<Option<(DecoratedKey, DeletionTime, Option<Row>)>> {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }
        let (key_bytes, deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        // Static-row peek: read the first row's flags + (optional)
        // extended flags WITHOUT moving the data pointer past them
        // unless we end up decoding the static row body. If the
        // first row is clustered, we rewind self.pos so
        // `stream_clustered_rows` sees the same byte we just
        // peeked at.
        let saved_pos = self.pos;
        if saved_pos >= file_len {
            return Ok(Some((key, deletion, None)));
        }
        let mut flags_buf = [0u8; 1];
        self.reader.read_exact_at(&mut flags_buf, self.pos)?;
        let flags = flags_buf[0];

        if flags & END_OF_PARTITION != 0 {
            // No clustered rows at all — consume the marker and
            // stream_clustered_rows will be a no-op.
            self.pos += 1;
            return Ok(Some((key, deletion, None)));
        }
        if flags & IS_MARKER != 0 {
            // Range tombstone marker — same fallback as
            // read_rows_limited; leave pos at the marker so the
            // streaming continuation can treat it as an end
            // condition.
            return Ok(Some((key, deletion, None)));
        }

        // Peek the extended flags so we can detect IS_STATIC.
        let ext_offset = saved_pos + 1;
        let (extended_flags, ext_len) = if flags & EXTENSION_FLAG != 0 {
            let mut ext_buf = [0u8; 1];
            self.reader.read_exact_at(&mut ext_buf, ext_offset)?;
            (ext_buf[0], 1u64)
        } else {
            (0, 0u64)
        };
        let is_static = extended_flags & EXT_IS_STATIC != 0;
        if is_static {
            // Commit pos past the flags + extended_flags we just
            // peeked, then decode the static row body.
            self.pos = saved_pos + 1 + ext_len;
            let row = self.read_row(flags, true)?;
            Ok(Some((key, deletion, Some(row))))
        } else {
            // No static row. Leave pos at saved_pos so the
            // continuation sees the same flags byte.
            self.pos = saved_pos;
            Ok(Some((key, deletion, None)))
        }
    }

    /// One-row-at-a-time partner of [`Self::stream_clustered_rows`].
    /// Reads the next clustered row at `self.pos`, returning
    /// `Ok(None)` at end-of-partition (or a range-tombstone
    /// marker — same fallback as the other streaming readers).
    /// Used by the cross-source streaming merge in the
    /// `walk_token_range_for_digest` multi-source path: each
    /// source's iterator is advanced one row at a time so the
    /// k-way merge by clustering key has full control over the
    /// pull rate.
    ///
    /// Must be preceded by [`Self::read_partition_header_only`] (or
    /// run inside a partition's row section after a prior
    /// `next_clustered_row` returned `Some`).
    pub fn read_next_clustered_row(&mut self) -> Result<Option<Row>> {
        loop {
            // EOF check first — a prior `END_OF_PARTITION` may
            // have moved us to file_len, in which case any further
            // call is just "nothing to do" rather than an I/O error.
            let file_len = self.reader.len()?;
            if self.pos >= file_len {
                return Ok(None);
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];
            if flags & END_OF_PARTITION != 0 {
                return Ok(None);
            }
            if flags & IS_MARKER != 0 {
                return Ok(None);
            }
            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };
            let is_static = extended_flags & EXT_IS_STATIC != 0;
            if is_static {
                // Skip stray static rows defensively (Cassandra
                // writes them first; `read_partition_header_only`
                // already consumed any in this partition).
                self.skip_row_body(flags, true)?;
                continue;
            }
            let row = self.read_row(flags, false)?;
            return Ok(Some(row));
        }
    }

    /// 2-phase streaming continuation: read clustered rows until
    /// `END_OF_PARTITION` (or a range-tombstone marker), invoking
    /// `on_row` once per row. Each row is dropped before the
    /// next is decoded.
    ///
    /// Must be preceded by [`Self::read_partition_header_only`]; calling
    /// it on a fresh `DataReader` will mis-parse the data stream.
    pub fn stream_clustered_rows<F>(&mut self, mut on_row: F) -> Result<()>
    where
        F: FnMut(&Row) -> Result<()>,
    {
        let file_len = self.reader.len()?;
        loop {
            if self.pos >= file_len {
                return Err(Self::missing_partition_end_error());
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }
            if flags & IS_MARKER != 0 {
                break;
            }

            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };
            let is_static = extended_flags & EXT_IS_STATIC != 0;
            if is_static {
                // A static row inside the clustered section is
                // not expected (Cassandra writes static first;
                // `read_partition_header_only` consumed it). If
                // we hit one anyway, skip its body — keeping the
                // contract that this function yields only
                // clustered rows.
                self.skip_row_body(flags, true)?;
                continue;
            }
            let row = self.read_row(flags, false)?;
            on_row(&row)?;
            drop(row);
        }
        Ok(())
    }

    pub fn read_partition_streaming<F>(
        &mut self,
        mut on_row: F,
    ) -> Result<Option<(DecoratedKey, DeletionTime, Option<Row>)>>
    where
        F: FnMut(&Row) -> Result<()>,
    {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }
        let (key_bytes, deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        let mut static_row: Option<Row> = None;
        loop {
            if self.pos >= file_len {
                if static_row.is_some() {
                    return Err(Self::missing_partition_end_error());
                }
                break;
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }
            if flags & IS_MARKER != 0 {
                // See `read_rows_limited` for the rationale —
                // range tombstones aren't written by Ferrosa and
                // Cassandra-imported ones drop range-deleted data
                // on this fast path.
                break;
            }

            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };
            let is_static = extended_flags & EXT_IS_STATIC != 0;
            let row = self.read_row(flags, is_static)?;
            if is_static {
                static_row = Some(row);
            } else {
                on_row(&row)?;
                drop(row);
            }
        }
        Ok(Some((key, deletion, static_row)))
    }

    pub fn read_partition_limited_rows(&mut self, row_limit: usize) -> Result<Option<Partition>> {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }

        let (key_bytes, deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        let (static_row, rows) = self.read_rows_limited(row_limit)?;

        Ok(Some(Partition {
            key,
            deletion,
            static_row,
            rows,
        }))
    }

    /// Read the next partition, retaining at most `row_limit` clustered rows
    /// without advancing to the partition boundary after the cap is reached.
    ///
    /// This is only valid for point lookups where the reader is discarded after
    /// the partition is returned. Range iterators must use
    /// [`Self::read_partition_limited_rows`] so the reader remains aligned on
    /// the next partition.
    pub fn read_partition_prefix_rows(&mut self, row_limit: usize) -> Result<Option<Partition>> {
        if row_limit == 0 {
            return self.read_partition_limited_rows(0);
        }

        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }

        let (key_bytes, deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));
        let (static_row, rows) = self.read_rows_prefix_limited(row_limit)?;

        Ok(Some(Partition {
            key,
            deletion,
            static_row,
            rows,
        }))
    }

    /// Returns the current read position in the data file.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Read the next partition's KEY + ROW COUNT only.
    ///
    /// Walks the partition body using `read_partition_header` then
    /// `skip_row_body` for each clustered row — no cell payloads
    /// are decoded. Used by the COUNT(*) fast path in
    /// `ferrosa-cluster::coordinator::range_count` (ADR-020) where
    /// the caller needs row counts per partition but not the row
    /// data. Memory: `O(1)` per call; CPU: roughly proportional
    /// to row count but ~10-100× cheaper than `read_partition`
    /// because cell payloads are byte-skipped, not parsed.
    ///
    /// Returns `Ok(None)` at EOF. Static rows count as part of the
    /// row count returned (mirrors `read_rows_limited` which puts
    /// the static row in `static_row` rather than `rows`); callers
    /// who need to distinguish static from clustered can use the
    /// full `read_partition_limited_rows` path.
    pub fn read_partition_count(&mut self) -> Result<Option<(DecoratedKey, u64)>> {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }

        let (key_bytes, _deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        let mut row_count: u64 = 0;
        loop {
            if self.pos >= file_len {
                if row_count > 0 {
                    return Err(Self::missing_partition_end_error());
                }
                break;
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }
            if flags & IS_MARKER != 0 {
                // Range tombstones aren't rows — mirror
                // read_rows_limited's "stop at marker" behavior.
                break;
            }

            // Extended flags byte (only present if EXTENSION_FLAG set).
            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };

            let is_static = extended_flags & EXT_IS_STATIC != 0;
            self.skip_row_body(flags, is_static)?;
            row_count += 1;
        }

        Ok(Some((key, row_count)))
    }

    /// Peek the next partition's key **without** decoding the body.
    ///
    /// Reads only the partition header (key bytes + DeletionTime) and
    /// **does not advance** `self.pos` — the next `read_partition*`
    /// call will re-read from the same offset and produce the full
    /// partition (or, on EOF, `Ok(None)`).
    ///
    /// Used by the range merger to populate its priming heap with
    /// keys without paying the full partition decode cost up front.
    /// On cold cache the per-source partition-body decode dominates
    /// scan latency; deferring it until a source is popped (after
    /// the heap proves it's the min) collapses
    /// `O(num_sources × cold_body_decode)` into
    /// `O(num_sources × cold_header_read) + O(emitted × cold_body_decode)`.
    ///
    /// Memory: `O(key_len)` per call. CPU: 2-3 small `pread`s of
    /// well under 100 bytes total.
    /// Returns `Ok(None)` at EOF.
    pub fn peek_partition_key(&mut self) -> Result<Option<DecoratedKey>> {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }
        let saved_pos = self.pos;
        let (key_bytes, _deletion) = self.read_partition_header()?;
        self.pos = saved_pos;
        Ok(Some(DecoratedKey::new(PartitionKey::new(key_bytes))))
    }

    /// Read the next partition with full row metadata (clustering
    /// keys, row-level deletion, liveness) but **no cell payloads**.
    ///
    /// Cells are byte-skipped via `body_end = body_start + row_size`,
    /// so cross-source dedup (memtable + flushing + N SSTables, plus
    /// replica overlap) can still happen via the existing
    /// `merge::merge_partitions` — which only needs partition key +
    /// clustering key + deletion timestamps to be correct. The
    /// returned `Partition`'s `rows[*].cells` is always empty.
    /// Returns `Ok(None)` at EOF.
    pub fn read_partition_metadata(&mut self) -> Result<Option<Partition>> {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }

        let (key_bytes, deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        let mut static_row = None;
        let mut rows = Vec::new();

        loop {
            if self.pos >= file_len {
                if static_row.is_some() || !rows.is_empty() {
                    return Err(Self::missing_partition_end_error());
                }
                break;
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }
            if flags & IS_MARKER != 0 {
                break;
            }

            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };

            let is_static = extended_flags & EXT_IS_STATIC != 0;
            let row = self.read_row_metadata(flags, is_static)?;
            if is_static {
                static_row = Some(row);
            } else {
                rows.push(row);
            }
        }

        Ok(Some(Partition {
            key,
            deletion,
            static_row,
            rows,
        }))
    }

    /// Decode a row's clustering key + liveness + row deletion,
    /// then byte-skip past the cells. The returned `Row` has
    /// `cells: Vec::new()`. Used by `read_partition_metadata`.
    fn read_row_metadata(&mut self, flags: u8, is_static: bool) -> Result<Row> {
        self.legacy_fixed_value_lengths = false;
        // Clustering (same shape as read_row).
        let clustering = if is_static {
            Vec::new()
        } else {
            let num_clustering = self.header.clustering_types.len();

            let mut ck_bytes = Vec::new();
            if num_clustering > 0 {
                let (header_bits, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;

                for i in 0..num_clustering {
                    let is_null = (header_bits & (1u64 << (2 * i))) != 0;
                    let is_empty = (header_bits & (1u64 << (2 * i + 1))) != 0;
                    if is_null || is_empty {
                        continue;
                    }
                    self.read_clustering_value_into(i, num_clustering, &mut ck_bytes)?;
                }
            }
            ck_bytes
        };
        self.maybe_skip_legacy_empty_clustering_prefix(flags, is_static)?;

        // Row body size — pins where the body ends so we can skip
        // cells regardless of liveness/deletion encoding length.
        let (row_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;
        let (_prev_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;
        let body_start = self.pos;
        let body_end = body_start + row_size;

        // Liveness.
        let mut liveness = LivenessInfo::NONE;
        if flags & HAS_TIMESTAMP != 0 {
            let (delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            liveness.timestamp = self.header.min_timestamp.wrapping_add(delta as i64);

            if flags & HAS_TTL != 0 {
                let (ttl_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.ttl = self.header.min_ttl.wrapping_add(ttl_delta as i32);

                let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.local_deletion_time = self
                    .header
                    .min_local_deletion_time
                    .wrapping_add(ldt_delta as i32);
            }
        }

        // Row-level deletion.
        let deletion = if flags & HAS_DELETION != 0 {
            let (ts_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let marked_for_delete_at = self.header.min_timestamp + ts_delta as i64;
            let local_deletion_time =
                (self.header.min_local_deletion_time as i64).wrapping_add(ldt_delta as i64) as u32;
            DeletionTime::new(marked_for_delete_at, local_deletion_time)
        } else {
            DeletionTime::LIVE
        };

        // Skip the rest of the row body (columns bitmap + all cells).
        if body_end > self.pos {
            self.pos = body_end;
        }

        Ok(Row {
            clustering,
            cells: Vec::new(),
            deletion,
            primary_key_liveness: liveness,
        })
    }

    // -----------------------------------------------------------------------
    // Internal: partition header
    // -----------------------------------------------------------------------

    /// Read the partition header: key bytes + partition-level deletion time.
    ///
    /// Cassandra 5.x DeletionTime (UInt format):
    /// - **LIVE**: single byte `0x80`
    /// - **Non-live**: first byte is MSB of `markedForDeleteAt` (sign bit = 0),
    ///   followed by 7 more bytes to complete the i64, then 4-byte u32
    ///   `localDeletionTime`. Total: 12 bytes.
    fn read_partition_header(&mut self) -> Result<(Vec<u8>, DeletionTime)> {
        // Key: u16 BE length + key bytes
        let mut len_buf = [0u8; 2];
        self.reader.read_exact_at(&mut len_buf, self.pos)?;
        self.pos += 2;
        let key_len = u16::from_be_bytes(len_buf) as usize;

        let mut key_bytes = vec![0u8; key_len];
        self.reader.read_exact_at(&mut key_bytes, self.pos)?;
        self.pos += key_len as u64;

        // DeletionTime: read first byte to determine format
        let mut first = [0u8; 1];
        self.reader.read_exact_at(&mut first, self.pos)?;
        self.pos += 1;

        let deletion = if first[0] & DELETION_IS_LIVE != 0 {
            // LIVE — verify the byte is exactly 0x80
            if first[0] != DELETION_IS_LIVE {
                return Err(Error::InvalidData(format!(
                    "corrupted DeletionTime flags: {:#04x}",
                    first[0]
                )));
            }
            DeletionTime::LIVE
        } else {
            // Non-live: first byte is MSB of markedForDeleteAt (i64 BE).
            // Read remaining 7 bytes of the long, then 4-byte ldt.
            let mut remaining = [0u8; 11];
            self.reader.read_exact_at(&mut remaining, self.pos)?;
            self.pos += 11;

            let mut mfda_bytes = [0u8; 8];
            mfda_bytes[0] = first[0];
            mfda_bytes[1..8].copy_from_slice(&remaining[0..7]);
            let marked_for_delete_at = i64::from_be_bytes(mfda_bytes);

            let local_deletion_time = u32::from_be_bytes(remaining[7..11].try_into().unwrap());

            DeletionTime::new(marked_for_delete_at, local_deletion_time)
        };

        Ok((key_bytes, deletion))
    }

    // -----------------------------------------------------------------------
    // Internal: row reading
    // -----------------------------------------------------------------------

    /// Read rows until `END_OF_PARTITION`, retaining at most `row_limit`
    /// clustered rows while still advancing to the next partition boundary.
    fn read_rows_limited(&mut self, row_limit: usize) -> Result<(Option<Row>, Vec<Row>)> {
        let file_len = self.reader.len()?;
        let mut static_row = None;
        let mut rows = Vec::new();

        loop {
            if self.pos >= file_len {
                if static_row.is_some() || !rows.is_empty() {
                    return Err(Self::missing_partition_end_error());
                }
                break;
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }

            if flags & IS_MARKER != 0 {
                // Range tombstone markers are not written by Ferrosa's SSTable
                // writer. Encountering one in a Ferrosa-written SSTable indicates
                // data corruption (misaligned read). Skip the rest of this
                // partition and return whatever rows were already parsed.
                //
                // For Cassandra-written SSTables (S3 bootstrap), this drops
                // range-deleted data which is acceptable — the live rows
                // before the marker are still returned.
                break;
            }

            // Extended flags byte (only present if EXTENSION_FLAG is set)
            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };

            let is_static = extended_flags & EXT_IS_STATIC != 0;
            if !is_static && row_limit > 0 && rows.len() >= row_limit {
                self.skip_row_body(flags, is_static)?;
                continue;
            }

            let row = self.read_row(flags, is_static)?;

            if is_static {
                static_row = Some(row);
            } else {
                rows.push(row);
            }
        }

        Ok((static_row, rows))
    }

    /// Read rows for a point lookup and stop as soon as the requested clustered
    /// row prefix is decoded. Unlike `read_rows_limited`, this deliberately
    /// does not skip the unretained tail of the partition because there is no
    /// subsequent partition iteration to keep aligned.
    fn read_rows_prefix_limited(&mut self, row_limit: usize) -> Result<(Option<Row>, Vec<Row>)> {
        let file_len = self.reader.len()?;
        let mut static_row = None;
        let mut rows = Vec::new();

        loop {
            if self.pos >= file_len {
                if static_row.is_some() || !rows.is_empty() {
                    return Err(Self::missing_partition_end_error());
                }
                break;
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }
            if flags & IS_MARKER != 0 {
                break;
            }

            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };

            let is_static = extended_flags & EXT_IS_STATIC != 0;
            let row = self.read_row(flags, is_static)?;
            if is_static {
                static_row = Some(row);
            } else {
                rows.push(row);
                if rows.len() >= row_limit {
                    break;
                }
            }
        }

        Ok((static_row, rows))
    }

    /// Skip a row after its flags and extended flags have already been consumed.
    fn skip_row_body(&mut self, flags: u8, is_static: bool) -> Result<()> {
        self.legacy_fixed_value_lengths = false;
        let num_clustering = self.header.clustering_types.len();
        if !is_static && num_clustering > 0 {
            let (header_bits, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;

            for i in 0..num_clustering {
                let is_null = (header_bits & (1u64 << (2 * i))) != 0;
                let is_empty = (header_bits & (1u64 << (2 * i + 1))) != 0;

                if is_null || is_empty {
                    continue;
                }

                let type_name = &self.header.clustering_types[i];
                let vlen = match marshal::value_length_if_fixed(type_name) {
                    Some(fixed_len) => fixed_len,
                    None => {
                        let (len, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                        self.pos += n as u64;
                        len as usize
                    }
                };
                self.pos += vlen as u64;
            }
        }
        self.maybe_skip_legacy_empty_clustering_prefix(flags, is_static)?;

        let (row_body_len, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;
        let (_prev_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;
        self.pos += row_body_len;
        Ok(())
    }

    /// Older Ferrosa SSTables wrote an empty ClusteringPrefix varint (`0`) even
    /// for tables with no clustering columns. Current files correctly start the
    /// row body with `row_body_len`. Treat an otherwise-invalid zero body length
    /// followed by a positive, in-bounds candidate body size as that legacy
    /// prefix, but keep true in-row truncation as an error.
    fn maybe_skip_legacy_empty_clustering_prefix(
        &mut self,
        flags: u8,
        is_static: bool,
    ) -> Result<()> {
        if is_static || !self.header.clustering_types.is_empty() {
            return Ok(());
        }

        let saved = self.pos;
        let (first, n1) = varint::read_unsigned_vint_at(self.reader, saved)?;
        if first != 0 || flags & (HAS_TIMESTAMP | HAS_TTL | HAS_DELETION | HAS_ALL_COLUMNS) == 0 {
            return Ok(());
        }

        let file_len = self.reader.len()?;
        let candidate_pos = saved + n1 as u64;
        if candidate_pos >= file_len {
            return Ok(());
        }
        let (candidate_row_size, n2) = varint::read_unsigned_vint_at(self.reader, candidate_pos)?;
        if candidate_row_size == 0 {
            return Ok(());
        }
        let prev_pos = candidate_pos + n2 as u64;
        if prev_pos >= file_len {
            return Ok(());
        }
        let (_prev_size, n3) = varint::read_unsigned_vint_at(self.reader, prev_pos)?;
        let body_start = prev_pos + n3 as u64;
        if body_start.saturating_add(candidate_row_size) <= file_len {
            self.legacy_fixed_value_lengths = true;
            self.pos = candidate_pos;
        }
        Ok(())
    }

    /// Read a single row given its already-consumed flags byte.
    fn read_row(&mut self, flags: u8, is_static: bool) -> Result<Row> {
        self.legacy_fixed_value_lengths = false;
        // Clustering key (not present for static rows)
        //
        // Cassandra 5.x ClusteringPrefix format:
        //   - Header varint: 2 bits per component (null/empty flags, batched per 32)
        //   - Per non-null/non-empty component:
        //     - Fixed-length types (Int32Type etc.): raw bytes, no length prefix
        //     - Variable-length types (UTF8Type etc.): varint(size) + value bytes
        //
        // Reference: ClusteringPrefix.Serializer + AbstractType.writeValue
        let clustering = if is_static {
            Vec::new()
        } else {
            let num_clustering = self.header.clustering_types.len();

            let mut ck_bytes = Vec::new();
            if num_clustering > 0 {
                let (header_bits, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;

                for i in 0..num_clustering {
                    let is_null = (header_bits & (1u64 << (2 * i))) != 0;
                    let is_empty = (header_bits & (1u64 << (2 * i + 1))) != 0;

                    if is_null || is_empty {
                        continue;
                    }

                    self.read_clustering_value_into(i, num_clustering, &mut ck_bytes)?;
                }
            }
            ck_bytes
        };
        self.maybe_skip_legacy_empty_clustering_prefix(flags, is_static)?;

        // Row body size (serialized row size varint — we skip this, it's used
        // for skipping rows during filtering but we always read fully).
        let (_row_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;

        // Previous unfiltered size (used for reverse iteration — skip).
        let (_prev_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;

        // Liveness info (unsigned varint deltas against SerializationHeader)
        let mut liveness = LivenessInfo::NONE;
        if flags & HAS_TIMESTAMP != 0 {
            let (delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            liveness.timestamp = self.header.min_timestamp.wrapping_add(delta as i64);

            if flags & HAS_TTL != 0 {
                let (ttl_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.ttl = self.header.min_ttl.wrapping_add(ttl_delta as i32);

                let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.local_deletion_time = self
                    .header
                    .min_local_deletion_time
                    .wrapping_add(ldt_delta as i32);
            }
        }

        // Row-level deletion (unsigned varint deltas via SerializationHeader)
        let deletion = if flags & HAS_DELETION != 0 {
            let (ts_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;

            let marked_for_delete_at = self.header.min_timestamp + ts_delta as i64;
            let local_deletion_time =
                (self.header.min_local_deletion_time as i64).wrapping_add(ldt_delta as i64) as u32;
            DeletionTime::new(marked_for_delete_at, local_deletion_time)
        } else {
            DeletionTime::LIVE
        };

        // Columns present bitmap (only if not HAS_ALL_COLUMNS)
        let columns = if is_static {
            &self.header.static_columns
        } else {
            &self.header.regular_columns
        };
        let num_columns = columns.len();

        let present_columns: Vec<usize> = if flags & HAS_ALL_COLUMNS != 0 {
            (0..num_columns).collect()
        } else {
            self.read_columns_subset(num_columns)?
        };

        // Read cells for present columns
        let mut cells = Vec::with_capacity(present_columns.len());
        for &col_idx in &present_columns {
            let cell = self.read_cell(&liveness, &columns[col_idx].1)?;
            cells.push((col_idx as u16, cell));
        }

        Ok(Row {
            clustering,
            cells,
            deletion,
            primary_key_liveness: liveness,
        })
    }

    /// Read the next partition with a column projection — only the
    /// cells whose ordinals are in `wanted` are decoded; all others
    /// are byte-skipped via `read_cell_skip`. Used by the CQL fast
    /// path for `SELECT col1, col2, ... FROM t` where many columns
    /// (especially wide embedding vectors) live in the table but
    /// only a few are projected.
    ///
    /// `wanted` is interpreted against the SerializationHeader's
    /// `regular_columns` ordinal space (and `static_columns` for
    /// static rows). An empty `wanted` slice means "no cells at all"
    /// — the row is returned with clustering + metadata only, same
    /// shape as `read_partition_metadata` but going through the
    /// per-cell skip path so it works even when the body-end
    /// arithmetic isn't trusted.
    ///
    /// Returns `Ok(None)` at EOF.
    pub fn read_partition_projected(&mut self, wanted: &[u16]) -> Result<Option<Partition>> {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }

        let (key_bytes, deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        let mut static_row = None;
        let mut rows = Vec::new();

        loop {
            if self.pos >= file_len {
                if static_row.is_some() || !rows.is_empty() {
                    return Err(Self::missing_partition_end_error());
                }
                break;
            }
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }
            if flags & IS_MARKER != 0 {
                break;
            }

            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };

            let is_static = extended_flags & EXT_IS_STATIC != 0;
            let row = self.read_row_projected(flags, is_static, wanted)?;
            if is_static {
                static_row = Some(row);
            } else {
                rows.push(row);
            }
        }

        Ok(Some(Partition {
            key,
            deletion,
            static_row,
            rows,
        }))
    }

    /// Decode a row's clustering key + liveness + row deletion, then
    /// for each present column decode the cell if its ordinal is in
    /// `wanted`, otherwise skip it via `read_cell_skip`. The
    /// returned `Row.cells` contains only the cells the caller asked
    /// for.
    fn read_row_projected(&mut self, flags: u8, is_static: bool, wanted: &[u16]) -> Result<Row> {
        self.legacy_fixed_value_lengths = false;
        // Clustering (same as read_row).
        let clustering = if is_static {
            Vec::new()
        } else {
            let num_clustering = self.header.clustering_types.len();

            let mut ck_bytes = Vec::new();
            if num_clustering > 0 {
                let (header_bits, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;

                for i in 0..num_clustering {
                    let is_null = (header_bits & (1u64 << (2 * i))) != 0;
                    let is_empty = (header_bits & (1u64 << (2 * i + 1))) != 0;
                    if is_null || is_empty {
                        continue;
                    }
                    self.read_clustering_value_into(i, num_clustering, &mut ck_bytes)?;
                }
            }
            ck_bytes
        };
        self.maybe_skip_legacy_empty_clustering_prefix(flags, is_static)?;

        // Row body size + prev size. The size bounds the liveness,
        // deletion, missing-column subset, and cell bytes. Key-only
        // projections must be able to skip the cell area without decoding it;
        // older Ferrosa SSTables can contain legacy sparse rows whose body
        // metadata is sufficient for clustering-key scans but whose cell area
        // is not decodable by the current cell reader.
        let (row_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;
        let (_prev_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;
        let body_start = self.pos;
        let body_end = body_start + row_size;

        // Liveness.
        let mut liveness = LivenessInfo::NONE;
        if flags & HAS_TIMESTAMP != 0 {
            let (delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            liveness.timestamp = self.header.min_timestamp.wrapping_add(delta as i64);

            if flags & HAS_TTL != 0 {
                let (ttl_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.ttl = self.header.min_ttl.wrapping_add(ttl_delta as i32);

                let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.local_deletion_time = self
                    .header
                    .min_local_deletion_time
                    .wrapping_add(ldt_delta as i32);
            }
        }

        // Row-level deletion.
        let deletion = if flags & HAS_DELETION != 0 {
            let (ts_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let marked_for_delete_at = self.header.min_timestamp + ts_delta as i64;
            let local_deletion_time =
                (self.header.min_local_deletion_time as i64).wrapping_add(ldt_delta as i64) as u32;
            DeletionTime::new(marked_for_delete_at, local_deletion_time)
        } else {
            DeletionTime::LIVE
        };

        // Columns present (same as read_row).
        let columns = if is_static {
            &self.header.static_columns
        } else {
            &self.header.regular_columns
        };
        let num_columns = columns.len();

        if wanted.is_empty() {
            let skipped_cells = (|| -> Result<()> {
                let present_columns: Vec<usize> = if flags & HAS_ALL_COLUMNS != 0 {
                    (0..num_columns).collect()
                } else {
                    self.read_columns_subset(num_columns)?
                };
                for &col_idx in &present_columns {
                    self.read_cell_skip(&liveness, &columns[col_idx].1)?;
                }
                Ok(())
            })();

            if skipped_cells.is_err() {
                self.pos = body_end;
            }

            return Ok(Row {
                clustering,
                cells: Vec::new(),
                deletion,
                primary_key_liveness: liveness,
            });
        }

        let present_columns: Vec<usize> = if flags & HAS_ALL_COLUMNS != 0 {
            (0..num_columns).collect()
        } else {
            self.read_columns_subset(num_columns)?
        };

        // For each present column: decode if wanted, skip otherwise.
        // `wanted` is small (typical SELECT projects a few cols), so
        // linear contains() is faster than a HashSet for the common
        // case.
        let mut cells = Vec::with_capacity(wanted.len().min(present_columns.len()));
        for &col_idx in &present_columns {
            let col_u16 = col_idx as u16;
            if wanted.contains(&col_u16) {
                let cell = self.read_cell(&liveness, &columns[col_idx].1)?;
                cells.push((col_u16, cell));
            } else {
                self.read_cell_skip(&liveness, &columns[col_idx].1)?;
            }
        }

        Ok(Row {
            clustering,
            cells,
            deletion,
            primary_key_liveness: liveness,
        })
    }

    fn read_columns_subset(&mut self, num_columns: usize) -> Result<Vec<usize>> {
        let saved_pos = self.pos;
        let (encoded, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;
        if encoded == 0 {
            return Ok((0..num_columns).collect());
        }

        if num_columns < 64 {
            let mut missing = encoded;
            let mut present = Vec::with_capacity(num_columns);
            for idx in 0..num_columns {
                if missing & 1 == 0 {
                    present.push(idx);
                }
                missing >>= 1;
            }
            if missing != 0 {
                return self.read_legacy_raw_present_columns_subset(saved_pos, num_columns);
            }
            return Ok(present);
        }

        let missing_count = encoded as usize;
        if missing_count > num_columns {
            return Err(Error::InvalidData(format!(
                "columns subset missing count {missing_count} exceeds {num_columns}"
            )));
        }
        let present_count = num_columns - missing_count;
        if present_count < num_columns / 2 {
            let mut present = Vec::with_capacity(present_count);
            for _ in 0..present_count {
                let (idx, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                present.push(idx as usize);
            }
            Ok(present)
        } else {
            let mut is_missing = vec![false; num_columns];
            for _ in 0..missing_count {
                let (idx, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                let idx = idx as usize;
                if idx >= num_columns {
                    return Err(Error::InvalidData(format!(
                        "columns subset missing index {idx} exceeds {num_columns}"
                    )));
                }
                is_missing[idx] = true;
            }
            Ok((0..num_columns).filter(|idx| !is_missing[*idx]).collect())
        }
    }

    fn read_legacy_raw_present_columns_subset(
        &mut self,
        start_pos: u64,
        num_columns: usize,
    ) -> Result<Vec<usize>> {
        let bitmap_bytes = num_columns.div_ceil(8);
        if bitmap_bytes == 0 {
            self.pos = start_pos;
            return Ok(Vec::new());
        }

        let mut bitmap = vec![0u8; bitmap_bytes];
        self.reader.read_exact_at(&mut bitmap, start_pos)?;
        self.pos = start_pos + bitmap_bytes as u64;
        self.legacy_fixed_value_lengths = true;

        let mut present = Vec::with_capacity(num_columns);
        for idx in 0..num_columns {
            let byte_idx = idx / 8;
            let bit_idx = 7 - (idx % 8);
            if bitmap[byte_idx] & (1 << bit_idx) != 0 {
                present.push(idx);
            }
        }
        Ok(present)
    }

    // -----------------------------------------------------------------------
    // Internal: cell reading
    // Reference: Cell.Serializer in Cell.java lines 377-419
    // -----------------------------------------------------------------------

    /// Advance the read position past a single cell without
    /// materializing its value. Reads the same flag + varint sequence
    /// as `read_cell` but discards the parsed timestamps and skips
    /// the value bytes via a position bump rather than allocating
    /// a buffer + read_exact_at — saves one syscall, one heap alloc,
    /// and the value-byte memcpy per skipped cell. Used by the
    /// projection-aware decode path so cells outside the SELECT list
    /// never pay the read+decode cost (especially big values like
    /// vector embeddings).
    fn read_cell_skip(&mut self, row_liveness: &LivenessInfo, column_type: &str) -> Result<()> {
        let mut cell_flags_buf = [0u8; 1];
        self.reader.read_exact_at(&mut cell_flags_buf, self.pos)?;
        self.pos += 1;
        let cell_flags = cell_flags_buf[0];

        let is_deleted = cell_flags & CELL_IS_DELETED != 0;
        let is_expiring = cell_flags & CELL_IS_EXPIRING != 0;
        let has_empty_value = cell_flags & CELL_HAS_EMPTY_VALUE != 0;
        let use_row_timestamp = cell_flags & CELL_USE_ROW_TIMESTAMP != 0;
        let use_row_ttl = cell_flags & CELL_USE_ROW_TTL != 0;
        let _ = row_liveness; // borrow only to mirror read_cell signature.

        if !use_row_timestamp {
            let (_, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
        }
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (_, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
        }
        if !use_row_ttl && is_expiring {
            let (_, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
        }
        if !has_empty_value {
            let vlen = if let Some(fixed_len) = marshal::value_length_if_fixed(column_type) {
                if self.legacy_fixed_value_lengths {
                    let saved = self.pos;
                    let (encoded_len, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                    if encoded_len == fixed_len as u64 {
                        self.pos += n as u64;
                    } else {
                        self.pos = saved;
                    }
                }
                fixed_len as u64
            } else {
                let (vlen, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                vlen
            };
            // Same corruption guard as read_value_bytes so a bogus vlen
            // doesn't carry us past EOF silently. This path only skips the
            // value (no allocation), so it guards without reading.
            if vlen > MAX_VALUE_LEN as u64 {
                return Err(Error::from(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cell value length {vlen} exceeds maximum ({MAX_VALUE_LEN}), likely corrupt SSTable"
                    ),
                )));
            }
            self.pos += vlen;
        }
        Ok(())
    }

    /// Allocate and fill a `len`-byte value buffer at the cursor, rejecting a
    /// corrupt length *before* allocation. A length above [`MAX_VALUE_LEN`]
    /// indicates a corrupt SSTable; surfacing a clean error lets the engine
    /// exclude the file instead of OOMing on a pathological allocation.
    /// Advances `self.pos` past the value on success.
    fn read_value_bytes(&mut self, len: usize, what: &str) -> Result<Vec<u8>> {
        if len > MAX_VALUE_LEN {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{what} length {len} exceeds maximum ({MAX_VALUE_LEN}), likely corrupt SSTable"
                ),
            )));
        }
        let mut buf = vec![0u8; len];
        self.reader.read_exact_at(&mut buf, self.pos)?;
        self.pos += len as u64;
        Ok(buf)
    }

    /// Read clustering component `i` (`self.header.clustering_types[i]`) at the
    /// cursor and append it to `ck_bytes`, length-prefixing each component when
    /// the clustering key has more than one so the CQL layer's
    /// `decode_clustering` can split them back. The on-disk length is bounded
    /// via [`Self::read_value_bytes`].
    fn read_clustering_value_into(
        &mut self,
        i: usize,
        num_clustering: usize,
        ck_bytes: &mut Vec<u8>,
    ) -> Result<()> {
        let vlen = match marshal::value_length_if_fixed(&self.header.clustering_types[i]) {
            Some(fixed_len) => fixed_len,
            None => {
                let (len, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                len as usize
            }
        };
        let vbuf = self.read_value_bytes(vlen, "clustering value")?;
        if num_clustering > 1 {
            ck_bytes.extend_from_slice(&(vbuf.len() as u16).to_be_bytes());
        }
        ck_bytes.extend_from_slice(&vbuf);
        Ok(())
    }

    /// Read a single cell value.
    fn read_cell(&mut self, row_liveness: &LivenessInfo, column_type: &str) -> Result<CellValue> {
        let mut cell_flags_buf = [0u8; 1];
        self.reader.read_exact_at(&mut cell_flags_buf, self.pos)?;
        self.pos += 1;
        let cell_flags = cell_flags_buf[0];

        let is_deleted = cell_flags & CELL_IS_DELETED != 0;
        let is_expiring = cell_flags & CELL_IS_EXPIRING != 0;
        let has_empty_value = cell_flags & CELL_HAS_EMPTY_VALUE != 0;
        let use_row_timestamp = cell_flags & CELL_USE_ROW_TIMESTAMP != 0;
        let use_row_ttl = cell_flags & CELL_USE_ROW_TTL != 0;

        // Timestamp (unsigned varint delta)
        let timestamp = if use_row_timestamp {
            row_liveness.timestamp
        } else {
            let (delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            self.header.min_timestamp.wrapping_add(delta as i64)
        };

        // Local deletion time (for tombstones and expiring cells)
        let local_deletion_time = if use_row_ttl {
            row_liveness.local_deletion_time
        } else if is_deleted || is_expiring {
            let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            self.header
                .min_local_deletion_time
                .wrapping_add(ldt_delta as i32)
        } else {
            ferrosa_common::NO_DELETION_TIME
        };

        // TTL (for expiring cells only)
        let ttl = if use_row_ttl {
            row_liveness.ttl
        } else if is_expiring {
            let (ttl_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            self.header.min_ttl.wrapping_add(ttl_delta as i32)
        } else {
            ferrosa_common::NO_TTL
        };

        // Value (absent if HAS_EMPTY_VALUE is set)
        let value = if has_empty_value {
            None
        } else {
            let vlen = if let Some(fixed_len) = marshal::value_length_if_fixed(column_type) {
                if self.legacy_fixed_value_lengths {
                    let saved = self.pos;
                    let (encoded_len, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                    if encoded_len == fixed_len as u64 {
                        self.pos += n as u64;
                    } else {
                        self.pos = saved;
                    }
                }
                fixed_len
            } else {
                let (vlen, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                vlen as usize
            };
            Some(self.read_value_bytes(vlen, "cell value")?)
        };

        if is_deleted {
            Ok(CellValue::tombstone(timestamp, local_deletion_time))
        } else if is_expiring {
            Ok(CellValue::expiring(
                value.unwrap_or_default(),
                timestamp,
                ttl,
                local_deletion_time,
            ))
        } else {
            Ok(CellValue::live(value.unwrap_or_default(), timestamp))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;

    /// Helper: write an unsigned varint to a buffer.
    fn push_unsigned_vint(out: &mut Vec<u8>, value: u64) {
        let mut buf = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut buf, value);
        out.extend_from_slice(&buf[..n]);
    }

    /// Write a LIVE DeletionTime (single byte 0x80).
    fn push_live_deletion(out: &mut Vec<u8>) {
        out.push(DELETION_IS_LIVE);
    }

    /// Build a minimal serialization header for testing.
    fn test_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        }
    }

    /// Build a minimal Data.db blob for one partition with one row and one cell.
    ///
    /// Partition: key = b"pk1", live deletion
    /// Row: clustering = [0x00, 0x00, 0x00, 0x01] (int 1), timestamp delta = 42
    /// Cell: uses row timestamp, value = b"hello"
    fn build_one_partition_blob(_header: &SerializationHeader) -> Vec<u8> {
        let mut data = Vec::new();

        // -- Partition header --
        let key = b"pk1";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // -- Row --
        // Flags: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering key (ClusteringPrefix format): header varint + raw fixed-length bytes
        // Int32Type is fixed-length (4 bytes), so no varint length prefix.
        let clustering = [0x00u8, 0x00, 0x00, 0x01]; // int32 = 1
        push_unsigned_vint(&mut data, 0); // header: all non-null, non-empty
        data.extend_from_slice(&clustering);

        // Row body size (dummy — reader skips it)
        push_unsigned_vint(&mut data, 20);
        // Previous unfiltered size
        push_unsigned_vint(&mut data, 0);

        // Liveness info: timestamp delta = 42 (unsigned varint)
        push_unsigned_vint(&mut data, 42);

        // Cell: use row timestamp, has value
        data.push(CELL_USE_ROW_TIMESTAMP);

        // Value: varint length + bytes
        let value = b"hello";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        // -- End of partition --
        data.push(END_OF_PARTITION);

        data
    }

    /// A corrupt clustering-value length must be rejected *before* it drives a
    /// `Vec` allocation — otherwise a bogus multi-terabyte varint in a corrupt
    /// SSTable OOM-kills the process. Regression for the previously unbounded
    /// `vec![0u8; vlen]` in the clustering read path.
    #[test]
    fn corrupt_clustering_length_is_rejected_before_alloc() {
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            // Variable-length clustering type → the reader takes a varint
            // length prefix, which is where corruption injects a huge length.
            clustering_types: vec!["org.apache.cassandra.db.marshal.UTF8Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut data = Vec::new();
        let key = b"pk1";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);
        // Clustering prefix header: all components present, non-empty.
        push_unsigned_vint(&mut data, 0);
        // Corrupt clustering-value length: one byte past the bound. The guard
        // must fire here, before any allocation or read of the (absent) value.
        push_unsigned_vint(&mut data, MAX_VALUE_LEN as u64 + 1);

        let mut reader = DataReader::new(&data, &header, 0);
        let err = reader
            .read_partition()
            .expect_err("corrupt clustering length must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("exceeds maximum"),
            "expected a bounds-guard error, got: {msg}"
        );
    }

    #[test]
    fn read_single_partition_one_row() {
        let header = test_header();
        let data = build_one_partition_blob(&header);
        let mut reader = DataReader::new(&data, &header, 0);

        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        // Verify key
        assert_eq!(partition.key.key.as_bytes(), b"pk1");

        // Verify partition deletion is live
        assert!(partition.deletion.is_live());

        // No static row
        assert!(partition.static_row.is_none());

        // One row
        assert_eq!(partition.rows.len(), 1);
        let row = &partition.rows[0];

        // Clustering key
        assert_eq!(row.clustering, vec![0x00, 0x00, 0x00, 0x01]);

        // Liveness: min_timestamp + delta = 1_000_000 + 42 = 1_000_042
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_042);

        // Row deletion is live
        assert!(row.deletion.is_live());

        // One cell
        assert_eq!(row.cells.len(), 1);
        let (col_idx, ref cell) = row.cells[0];
        assert_eq!(col_idx, 0);
        assert!(cell.is_live());
        assert_eq!(cell.value.as_deref(), Some(b"hello".as_slice()));
        // Cell uses row timestamp
        assert_eq!(cell.timestamp, 1_000_042);

        // No more partitions
        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_empty_partition() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header: key = b"empty"
        let key = b"empty";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Immediately end of partition
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        assert_eq!(partition.key.key.as_bytes(), b"empty");
        assert!(partition.deletion.is_live());
        assert!(partition.static_row.is_none());
        assert!(partition.rows.is_empty());

        // EOF
        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_partition_with_own_cell_timestamp() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header
        let key = b"pk2";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x02];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 30);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 100 (unsigned varint)
        push_unsigned_vint(&mut data, 100);

        // Cell: does NOT use row timestamp (flags = 0), has its own timestamp
        data.push(0x00);

        // Cell timestamp delta = 200 (unsigned varint)
        push_unsigned_vint(&mut data, 200);

        // Value
        let value = b"world";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        // End of partition
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        let row = &partition.rows[0];
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_100);

        let (_, ref cell) = row.cells[0];
        // Cell has its own timestamp: min_timestamp + 200 = 1_000_200
        assert_eq!(cell.timestamp, 1_000_200);
        assert_eq!(cell.value.as_deref(), Some(b"world".as_slice()));
    }

    #[test]
    fn read_partition_with_deleted_cell() {
        // Use a header where min_local_deletion_time allows positive unsigned deltas
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: 50,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"pk3";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x03];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 10);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 50 (unsigned)
        push_unsigned_vint(&mut data, 50);

        // Cell: deleted + empty value, does not use row timestamp
        data.push(CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE);

        // Cell timestamp delta = 60 (unsigned)
        push_unsigned_vint(&mut data, 60);

        // Local deletion time delta = 50 (actual = min(50) + 50 = 100)
        push_unsigned_vint(&mut data, 50);

        // No value (HAS_EMPTY_VALUE set)

        // End of partition
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        let row = &partition.rows[0];
        let (_, ref cell) = row.cells[0];
        assert!(cell.is_tombstone());
        assert_eq!(cell.timestamp, 1_000_060);
        assert_eq!(cell.local_deletion_time, 100);
        assert!(cell.value.is_none());
    }

    #[test]
    fn read_two_partitions() {
        let header = test_header();
        let mut data = build_one_partition_blob(&header);

        // Second partition: key = b"pk2", empty (no rows)
        let key2 = b"pk2";
        data.extend_from_slice(&(key2.len() as u16).to_be_bytes());
        data.extend_from_slice(key2);
        push_live_deletion(&mut data);
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);

        let p1 = reader.read_partition().unwrap().expect("partition 1");
        assert_eq!(p1.key.key.as_bytes(), b"pk1");
        assert_eq!(p1.rows.len(), 1);

        let p2 = reader.read_partition().unwrap().expect("partition 2");
        assert_eq!(p2.key.key.as_bytes(), b"pk2");
        assert!(p2.rows.is_empty());

        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_partition_with_row_deletion() {
        // Use a header where min_local_deletion_time allows positive unsigned deltas
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: 0,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"pkdel";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_DELETION | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_DELETION | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x07];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 30);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 10 (unsigned)
        push_unsigned_vint(&mut data, 10);

        // Row deletion: timestamp delta = 20, local_deletion_time delta = 500
        // actual mfda = 1_000_000 + 20 = 1_000_020
        // actual ldt = 0 + 500 = 500
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 500);

        // Cell: use row timestamp
        data.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"x";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");
        let row = &partition.rows[0];

        assert!(!row.deletion.is_live());
        assert_eq!(row.deletion.marked_for_delete_at, 1_000_020);
        assert_eq!(row.deletion.local_deletion_time, 500);
    }

    #[test]
    fn read_partition_with_missing_columns() {
        // Header with 2 regular columns
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"col_a".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"col_b".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
            ],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"pk_sparse";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP, no HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x05];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 10 (unsigned)
        push_unsigned_vint(&mut data, 10);

        // Cassandra Columns subset: unsigned vint missing-column bitmap.
        // We want column 0 present, column 1 missing.
        // For <64 columns, bit i = 1 means column i is missing.
        push_unsigned_vint(&mut data, 0b10);

        // Only column 0's cell: use row timestamp
        data.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"only_a";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        let row = &partition.rows[0];
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].0, 0); // column index 0
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"only_a".as_slice()));
    }

    #[test]
    fn read_partition_accepts_legacy_raw_present_column_bitmap() {
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"col_a".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"col_b".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
            ],
        };

        let mut row_body = Vec::new();
        push_unsigned_vint(&mut row_body, 10);
        // Legacy Ferrosa raw bitmap: bit 7 = column 0 present,
        // bit 6 = column 1 missing. This is not a Cassandra vint.
        row_body.push(0x80);
        row_body.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"legacy_a";
        push_unsigned_vint(&mut row_body, value.len() as u64);
        row_body.extend_from_slice(value);

        let mut data = Vec::new();
        let key = b"pk_legacy_bitmap";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.push(HAS_TIMESTAMP);
        let clustering = [0x00u8, 0x00, 0x00, 0x06];
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&clustering);
        push_unsigned_vint(&mut data, row_body.len() as u64);
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&row_body);
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .expect("legacy raw present bitmap must be readable")
            .expect("expected partition");

        let row = &partition.rows[0];
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].0, 0);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(value.as_slice()));
    }

    #[test]
    fn legacy_raw_bitmap_rows_accept_fixed_width_value_length_prefixes() {
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"created_at".to_vec(),
                "org.apache.cassandra.db.marshal.TimestampType".into(),
            )],
        };

        let created_at = 1_800_000i64.to_be_bytes();
        let mut row_body = Vec::new();
        push_unsigned_vint(&mut row_body, 10);
        row_body.push(0x80); // legacy raw bitmap: column 0 present
        row_body.push(CELL_USE_ROW_TIMESTAMP);
        row_body.push(8); // legacy writer length-prefixed fixed-width values
        row_body.extend_from_slice(&created_at);

        let mut data = Vec::new();
        let key = b"pk_legacy_fixed";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.push(HAS_TIMESTAMP);
        let clustering = [0x00u8, 0x00, 0x00, 0x07];
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&clustering);
        push_unsigned_vint(&mut data, row_body.len() as u64);
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&row_body);
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .expect("legacy fixed-width cell must be readable")
            .expect("expected partition");

        let row = &partition.rows[0];
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].0, 0);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(created_at.as_slice()));
    }

    #[test]
    fn read_partition_without_clustering_columns_starts_at_row_body_length() {
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut row_body = Vec::new();
        push_unsigned_vint(&mut row_body, 42);
        row_body.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"hello";
        push_unsigned_vint(&mut row_body, value.len() as u64);
        row_body.extend_from_slice(value);

        let mut data = Vec::new();
        let key = b"pk_no_ck";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);
        push_unsigned_vint(&mut data, row_body.len() as u64);
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&row_body);
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        let row = &partition.rows[0];
        assert!(row.clustering.is_empty());
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_042);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(value.as_slice()));
    }

    #[test]
    fn read_projected_without_clustering_columns_accepts_legacy_empty_prefix() {
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"applied_at".to_vec(),
                    "org.apache.cassandra.db.marshal.TimestampType".into(),
                ),
                (
                    b"val".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
            ],
        };

        let mut row_body = Vec::new();
        push_unsigned_vint(&mut row_body, 42);
        row_body.push(CELL_USE_ROW_TIMESTAMP);
        row_body.push(8);
        let timestamp = 1_800_000i64.to_be_bytes();
        row_body.extend_from_slice(&timestamp);
        row_body.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"legacy";
        push_unsigned_vint(&mut row_body, value.len() as u64);
        row_body.extend_from_slice(value);

        let mut data = Vec::new();
        let key = b"pk_no_ck_legacy";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);
        push_unsigned_vint(&mut data, 0);
        push_unsigned_vint(&mut data, row_body.len() as u64);
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&row_body);
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition_projected(&[0, 1])
            .unwrap()
            .expect("expected partition");

        let row = &partition.rows[0];
        assert!(row.clustering.is_empty());
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_042);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(timestamp.as_slice()));
        assert_eq!(row.cells[1].1.value.as_deref(), Some(value.as_slice()));
    }

    #[test]
    fn read_partition_with_fixed_width_cell_without_value_length_prefix() {
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"v_int".to_vec(),
                    "org.apache.cassandra.db.marshal.Int32Type".into(),
                ),
                (
                    b"v_text".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
            ],
        };

        let mut row_body = Vec::new();
        push_unsigned_vint(&mut row_body, 42);
        push_unsigned_vint(&mut row_body, 0b10);
        row_body.push(CELL_USE_ROW_TIMESTAMP);
        row_body.extend_from_slice(&42i32.to_be_bytes());

        let mut data = Vec::new();
        let key = b"pk_fixed";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.push(HAS_TIMESTAMP);
        push_unsigned_vint(&mut data, row_body.len() as u64);
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&row_body);
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        let row = &partition.rows[0];
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].0, 0);
        assert_eq!(
            row.cells[0].1.value.as_deref(),
            Some(42i32.to_be_bytes().as_slice())
        );
    }

    #[test]
    fn projected_empty_projection_skips_sparse_row_body_without_cell_decode() {
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.UUIDType".into()],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"entity_name".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"entity_type".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
            ],
        };

        let clustering = [0x33u8; 16];
        let mut row_body = Vec::new();
        push_unsigned_vint(&mut row_body, 42);
        // Legacy sparse row marker observed in old entity_store SSTables:
        // parsing this as "all regular columns are present" forces cell
        // decoding into the END_OF_PARTITION byte and drops the whole SSTable.
        push_unsigned_vint(&mut row_body, 0);
        row_body.push(0);

        let mut data = Vec::new();
        let key = b"pk_projected_keys";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.push(HAS_TIMESTAMP);
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&clustering);
        push_unsigned_vint(&mut data, row_body.len() as u64);
        push_unsigned_vint(&mut data, 0);
        data.extend_from_slice(&row_body);
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition_projected(&[])
            .expect("key-only projection must not decode sparse row cells")
            .expect("expected partition");

        assert_eq!(partition.key.key.as_bytes(), key);
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].clustering, clustering);
        assert!(partition.rows[0].cells.is_empty());
        assert!(reader.read_partition_projected(&[]).unwrap().is_none());
    }

    #[test]
    fn projected_empty_projection_counts_rows_when_legacy_row_size_overstates_body() {
        let header = test_header();

        fn row_bytes(
            clustering_value: i32,
            value: &[u8],
            row_size_override: Option<u64>,
        ) -> Vec<u8> {
            let mut body = Vec::new();
            push_unsigned_vint(&mut body, clustering_value as u64);
            body.push(CELL_USE_ROW_TIMESTAMP);
            push_unsigned_vint(&mut body, value.len() as u64);
            body.extend_from_slice(value);

            let mut row = Vec::new();
            row.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);
            push_unsigned_vint(&mut row, 0);
            row.extend_from_slice(&clustering_value.to_be_bytes());
            push_unsigned_vint(&mut row, row_size_override.unwrap_or(body.len() as u64));
            push_unsigned_vint(&mut row, 0);
            row.extend_from_slice(&body);
            row
        }

        let second_row = row_bytes(2, b"two", None);
        let first_body_len = {
            let mut body = Vec::new();
            push_unsigned_vint(&mut body, 1);
            body.push(CELL_USE_ROW_TIMESTAMP);
            push_unsigned_vint(&mut body, 3);
            body.extend_from_slice(b"one");
            body.len() as u64
        };
        let first_row = row_bytes(1, b"one", Some(first_body_len + second_row.len() as u64));

        let mut data = Vec::new();
        let key = b"pk_legacy_oversized_row";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);
        data.extend_from_slice(&first_row);
        data.extend_from_slice(&second_row);
        data.push(END_OF_PARTITION);

        let full = DataReader::new(&data, &header, 0)
            .read_partition()
            .unwrap()
            .expect("expected full partition");
        assert_eq!(full.rows.len(), 2, "full decode establishes row truth");

        let projected = DataReader::new(&data, &header, 0)
            .read_partition_projected(&[])
            .unwrap()
            .expect("expected projected partition");
        assert_eq!(
            projected.rows.len(),
            full.rows.len(),
            "key-only projection must not skip later rows when a legacy SSTable overstates row_size"
        );
    }

    #[test]
    fn eof_returns_none() {
        let header = test_header();
        let data: Vec<u8> = Vec::new();
        let mut reader = DataReader::new(&data, &header, 0);
        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_partition_with_delta_decoded_timestamps() {
        // Use non-trivial min_timestamp to verify delta decoding
        let header = SerializationHeader {
            min_timestamp: 1_700_000_000_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"data".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"ts_test";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x0A];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 999 (unsigned)
        push_unsigned_vint(&mut data, 999);

        // Cell: own timestamp, delta = 1500 (unsigned)
        data.push(0x00); // no cell flags
        push_unsigned_vint(&mut data, 1500);
        let value = b"ts_value";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        let row = &partition.rows[0];
        assert_eq!(row.primary_key_liveness.timestamp, 1_700_000_000_000_999);

        let (_, ref cell) = row.cells[0];
        assert_eq!(cell.timestamp, 1_700_000_000_001_500);
    }

    #[test]
    fn position_advances_correctly() {
        let header = test_header();
        let data = build_one_partition_blob(&header);
        let data_len = data.len() as u64;

        let mut reader = DataReader::new(&data, &header, 0);
        assert_eq!(reader.position(), 0);

        let _ = reader.read_partition().unwrap();
        assert_eq!(reader.position(), data_len);
    }

    /// Verify that delta decoding with wrapping_add does not panic when
    /// min_local_deletion_time is near i32::MAX and delta causes overflow.
    #[test]
    fn delta_decode_i32_near_max_does_not_panic() {
        // Simulate the wrapping_add arithmetic used in row reading:
        //   liveness.local_deletion_time = min_local_deletion_time.wrapping_add(delta as i32)
        let min_local_deletion_time: i32 = i32::MAX - 10;
        let delta: u64 = 20;
        // This would panic with checked add; wrapping_add should not panic.
        let result = min_local_deletion_time.wrapping_add(delta as i32);
        // The result wraps around: (i32::MAX - 10) + 20 = i32::MIN + 9
        assert_eq!(result, i32::MIN + 9);
    }

    /// Verify that delta decoding with wrapping_add does not panic when
    /// min_timestamp is near i64::MAX and delta causes overflow.
    #[test]
    fn delta_decode_i64_near_max_does_not_panic() {
        // Simulate the wrapping_add arithmetic used in row/cell reading:
        //   liveness.timestamp = min_timestamp.wrapping_add(delta as i64)
        let min_timestamp: i64 = i64::MAX - 10;
        let delta: u64 = 20;
        // This would panic with checked add; wrapping_add should not panic.
        let result = min_timestamp.wrapping_add(delta as i64);
        // The result wraps around: (i64::MAX - 10) + 20 = i64::MIN + 9
        assert_eq!(result, i64::MIN + 9);
    }

    #[test]
    fn read_non_live_partition_deletion() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header: key = b"del"
        let key = b"del";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);

        // Non-live DeletionTime: 8-byte markedForDeleteAt + 4-byte localDeletionTime
        let mfda: i64 = 1_700_000_000;
        let ldt: u32 = 1_700_000;
        data.extend_from_slice(&mfda.to_be_bytes());
        data.extend_from_slice(&ldt.to_be_bytes());

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        assert!(!partition.deletion.is_live());
        assert_eq!(partition.deletion.marked_for_delete_at, mfda);
        assert_eq!(partition.deletion.local_deletion_time, ldt);
    }
}
