//! Per-SSTable scalar index sidecar files, read through a memory map.
//!
//! Correctness: correct when a key's postings come back in row order —
//! `(partition key, clustering)` — exactly once each, a corrupt file is
//! refused at open rather than half-read, and a reader's heap does not grow
//! with the file.
//! Last revised: 2026-09-11
//! Last changed: v2 format with an entry-offset table, read through a memory
//! map instead of loaded onto the heap; streaming atomic writer; v1 files are
//! converted at open with a spilling sort (t_7ac6b0e3).
//!
//! ## Why a map
//!
//! The store view holds a reader for every SSTable's every index for as long
//! as the SSTable lives. v1 readers `std::fs::read` the whole file and
//! deserialize it into owned entries, so index memory was O(every posting on
//! the node), resident. A v2 reader maps the file: its heap is a handful of
//! words, entries are decoded in place from the mapping, and the mapped pages
//! are clean and file-backed — the kernel reclaims them under memory pressure
//! (and a cgroup reclaims them before an OOM kill) and faults them back in on
//! the next read. Random access is a binary search over the offset table.
//!
//! Mapping is only safe because a sidecar file is immutable once published:
//! every writer writes a temp file, fsyncs it and renames it into place, so a
//! mapped file is never truncated underneath a reader (truncation is what
//! turns a mapped read into SIGBUS). Deleting a mapped file is safe; the
//! mapping keeps the inode alive.
//!
//! ## File format (v2)
//!
//! ```text
//! +------- Header (17 bytes) --------+
//! | magic:       b"FXSI" (4 bytes)   |
//! | version:     u8 = 2  (1 byte)    |
//! | entry_count: u64 LE  (8 bytes)   |
//! | header_crc:  u32 LE  (4 bytes)   |  <- CRC32 of the first 13 bytes
//! +----------------------------------+
//! | entries, sorted by (key, partition key, clustering), unique:
//! |   key_len u32 | key | pk_len u32 | pk | ck_len u32 | ck
//! +----------------------------------+
//! | offsets: entry_count x u64 LE — file offset of each entry
//! +------- Footer (16 bytes) --------+
//! | offsets_start: u64 LE            |
//! | body_crc:      u32 LE            |  <- CRC32 of entries + offsets
//! | footer_magic:  b"FXSE"           |
//! +----------------------------------+
//! ```
//!
//! v1 (the same header with version 1, then entries sorted by index key only,
//! no offsets, no footer) cannot be binary-searched in place and its
//! within-key order is arrival order. [`SidecarReader::open`] converts a v1
//! file to v2 — streamed through [`crate::external_sort::ExternalSorter`], so
//! the conversion's memory is bounded by the spill threshold — rewrites it
//! atomically, and maps the result.

use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrosa_index::{IndexError, IndexKey, IndexResult, RowPosition};

/// Magic bytes identifying a sidecar index file.
const SIDECAR_MAGIC: &[u8; 4] = b"FXSI";

/// The legacy format: entries sorted by key only, no offsets, no footer.
const SIDECAR_VERSION_V1: u8 = 1;

/// Current sidecar format version.
const SIDECAR_VERSION: u8 = 2;

/// Header size: magic(4) + version(1) + entry_count(8) + crc(4) = 17.
const HEADER_SIZE: usize = 17;

/// Bytes before the CRC field: magic(4) + version(1) + entry_count(8) = 13.
const CRC_INPUT_SIZE: usize = 13;

/// Footer magic, closing a v2 file.
const FOOTER_MAGIC: &[u8; 4] = b"FXSE";

/// Footer size: offsets_start(8) + body_crc(4) + magic(4) = 16.
const FOOTER_SIZE: usize = 16;

/// Width of one offset-table slot.
const OFFSET_SIZE: usize = 8;

// ── Borrowed postings ────────────────────────────────────────────────────────

/// A row position borrowed from a sidecar mapping (or a memtable posting
/// list). Ordered like [`RowPosition`] — `(partition_key, clustering_key)`
/// bytes — so borrowed and owned positions merge in the same order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowPositionRef<'a> {
    pub partition_key: &'a [u8],
    pub clustering_key: &'a [u8],
}

impl RowPositionRef<'_> {
    /// An owned copy.
    pub fn to_owned_position(&self) -> RowPosition {
        RowPosition {
            partition_key: self.partition_key.to_vec(),
            clustering_key: self.clustering_key.to_vec(),
        }
    }
}

impl<'a> From<&'a RowPosition> for RowPositionRef<'a> {
    fn from(position: &'a RowPosition) -> Self {
        Self {
            partition_key: &position.partition_key,
            clustering_key: &position.clustering_key,
        }
    }
}

/// One decoded entry, borrowed from the sidecar bytes.
#[derive(Debug, Clone, Copy)]
struct EntryRef<'a> {
    key: &'a [u8],
    position: RowPositionRef<'a>,
    /// Encoded length, for walking the body sequentially.
    len: usize,
}

// ── Writer ───────────────────────────────────────────────────────────────────

/// Writes a sidecar index file with CRC32-validated header.
pub struct SidecarWriter;

impl SidecarWriter {
    /// Write a sidecar index file. Entries are sorted by index key, then row
    /// position, and deduplicated before writing (see `posting_order`).
    pub fn write(path: &Path, entries: &[(IndexKey, RowPosition)]) -> IndexResult<()> {
        let mut sorted: Vec<_> = entries.to_vec();
        sorted.sort_by(posting_order);
        sorted.dedup();
        Self::write_sorted(path, sorted.into_iter().map(Ok))?;
        Ok(())
    }

    /// Stream entries already in `(key, row)` order into a v2 sidecar at
    /// `path`, returning the number written. Adjacent duplicates are dropped;
    /// an entry out of order is refused (the reader's binary search and the
    /// store's ordered merge both depend on the order).
    ///
    /// Memory is one entry plus two write buffers: the body and the offset
    /// table stream to separate temp files, which are joined, fsynced and
    /// renamed into place — so a published sidecar is never truncated or
    /// rewritten under a reader that has it mapped.
    pub fn write_sorted<I>(path: &Path, entries: I) -> IndexResult<u64>
    where
        I: IntoIterator<Item = IndexResult<(IndexKey, RowPosition)>>,
    {
        let body_path = temp_sibling(path, "body");
        let offsets_path = temp_sibling(path, "offsets");
        let result = write_v2_files(&body_path, &offsets_path, entries);
        if let Err(error) = std::fs::remove_file(&offsets_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(%error, path = %offsets_path.display(), "sidecar: could not remove the offset-table temp file");
            }
        }
        let count = match result {
            Ok(count) => count,
            Err(error) => {
                if let Err(cleanup) = std::fs::remove_file(&body_path) {
                    tracing::warn!(%cleanup, path = %body_path.display(), "sidecar: could not remove a failed write's temp file");
                }
                return Err(error);
            }
        };
        std::fs::rename(&body_path, path)?;
        sync_parent_dir(path)?;
        Ok(count)
    }
}

/// Sidecar entry order: index key, then row position — `(partition key,
/// clustering)` bytes. Row-ordered postings let an index read merge the
/// memtable's and every sidecar's postings, and resume after a row, holding
/// only one cursor per source (t_50c8bc7d).
fn posting_order(a: &(IndexKey, RowPosition), b: &(IndexKey, RowPosition)) -> std::cmp::Ordering {
    a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1))
}

/// A temp path beside `path` — same directory, so the final rename is atomic.
fn temp_sibling(path: &Path, role: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!(".{name}.{role}.{}.{nanos}.tmp", std::process::id()))
}

/// fsync the directory holding `path`, so a completed rename survives a crash.
fn sync_parent_dir(path: &Path) -> IndexResult<()> {
    let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) else {
        return Ok(());
    };
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Write the body to `body_path` and the offset table to `offsets_path`, then
/// append the table and footer to the body and fill in the header.
fn write_v2_files<I>(body_path: &Path, offsets_path: &Path, entries: I) -> IndexResult<u64>
where
    I: IntoIterator<Item = IndexResult<(IndexKey, RowPosition)>>,
{
    let mut body = BufWriter::new(std::fs::File::create(body_path)?);
    let mut offsets = BufWriter::new(std::fs::File::create(offsets_path)?);
    body.write_all(&[0u8; HEADER_SIZE])?;
    let mut crc = crc32fast::Hasher::new();
    let mut position = HEADER_SIZE as u64;
    let mut count = 0u64;
    let mut previous: Option<(IndexKey, RowPosition)> = None;
    for entry in entries {
        let entry = entry?;
        if let Some(prev) = &previous {
            match posting_order(prev, &entry) {
                std::cmp::Ordering::Equal => continue,
                std::cmp::Ordering::Greater => {
                    return Err(IndexError::Corrupt(
                        "sidecar writer: entries out of (key, row) order".into(),
                    ))
                }
                std::cmp::Ordering::Less => {}
            }
        }
        let encoded = encode_entry(&entry.0, &entry.1);
        body.write_all(&encoded)?;
        crc.update(&encoded);
        let slot = position.to_le_bytes();
        offsets.write_all(&slot)?;
        position += encoded.len() as u64;
        count += 1;
        previous = Some(entry);
    }
    offsets.flush()?;
    drop(offsets);

    let offsets_start = position;
    let mut table = std::io::BufReader::new(std::fs::File::open(offsets_path)?);
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let read = table.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        body.write_all(&chunk[..read])?;
        crc.update(&chunk[..read]);
    }
    body.write_all(&offsets_start.to_le_bytes())?;
    body.write_all(&crc.finalize().to_le_bytes())?;
    body.write_all(FOOTER_MAGIC)?;
    let mut file = body.into_inner().map_err(|error| error.into_error())?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header_bytes(SIDECAR_VERSION, count))?;
    file.sync_all()?;
    Ok(count)
}

/// The 17-byte header for `version` and `count`.
fn header_bytes(version: u8, count: u64) -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    header[..4].copy_from_slice(SIDECAR_MAGIC);
    header[4] = version;
    header[5..CRC_INPUT_SIZE].copy_from_slice(&count.to_le_bytes());
    let crc = crc32fast::hash(&header[..CRC_INPUT_SIZE]);
    header[CRC_INPUT_SIZE..].copy_from_slice(&crc.to_le_bytes());
    header
}

/// One entry in the wire encoding shared by v1 and v2.
fn encode_entry(key: &IndexKey, position: &RowPosition) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        12 + key.0.len() + position.partition_key.len() + position.clustering_key.len(),
    );
    for field in [&key.0, &position.partition_key, &position.clustering_key] {
        buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
        buf.extend_from_slice(field);
    }
    buf
}

/// Serialize sorted, unique entries into a complete v2 image in memory — for
/// in-memory flush targets, which have no file to map.
fn encode_v2_in_memory(sorted: &[(IndexKey, RowPosition)]) -> Vec<u8> {
    let mut image = header_bytes(SIDECAR_VERSION, sorted.len() as u64).to_vec();
    let mut offsets = Vec::with_capacity(sorted.len() * OFFSET_SIZE);
    for (key, position) in sorted {
        offsets.extend_from_slice(&(image.len() as u64).to_le_bytes());
        image.extend_from_slice(&encode_entry(key, position));
    }
    let offsets_start = image.len() as u64;
    image.extend_from_slice(&offsets);
    let crc = crc32fast::hash(&image[HEADER_SIZE..]);
    image.extend_from_slice(&offsets_start.to_le_bytes());
    image.extend_from_slice(&crc.to_le_bytes());
    image.extend_from_slice(FOOTER_MAGIC);
    image
}

// ── Reader ───────────────────────────────────────────────────────────────────

/// A memory-mapped sidecar, counted in the `index_sidecar_mapped_*` gauges
/// for exactly the life of the mapping.
struct MappedSidecar {
    map: memmap2::Mmap,
}

impl MappedSidecar {
    fn new(map: memmap2::Mmap) -> Self {
        crate::metrics::index_sidecar_mapped(map.len() as u64);
        Self { map }
    }
}

impl Drop for MappedSidecar {
    fn drop(&mut self) {
        crate::metrics::index_sidecar_unmapped(self.map.len() as u64);
    }
}

/// The bytes a reader decodes from.
#[derive(Clone)]
enum SidecarBytes {
    /// A published file, memory-mapped: reclaimable page cache, not heap.
    Mapped(Arc<MappedSidecar>),
    /// An in-memory image, for flush targets with no file (in-memory stores).
    Heap(Arc<[u8]>),
    /// A file with no entries.
    Empty,
}

impl SidecarBytes {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mapped(mapped) => &mapped.map,
            Self::Heap(bytes) => bytes,
            Self::Empty => &[],
        }
    }
}

/// Reads a v2 sidecar: header and footer validated, the whole body checked
/// once at open, then entries decoded in place from the mapping. Cloning is
/// cheap — clones share the mapping.
#[derive(Clone)]
pub struct SidecarReader {
    bytes: SidecarBytes,
    entry_count: u64,
    offsets_start: usize,
}

impl std::fmt::Debug for SidecarReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let backing = match &self.bytes {
            SidecarBytes::Mapped(_) => "mapped",
            SidecarBytes::Heap(_) => "heap",
            SidecarBytes::Empty => "empty",
        };
        f.debug_struct("SidecarReader")
            .field("backing", &backing)
            .field("entry_count", &self.entry_count)
            .field("bytes", &self.bytes.as_slice().len())
            .finish()
    }
}

impl SidecarReader {
    /// Construct a reader over an in-memory image of `entries` (no disk I/O).
    /// Used by flush targets that have no file to map; a store backed by
    /// files maps the published sidecar instead.
    pub fn from_entries(entries: Vec<(IndexKey, RowPosition)>) -> Self {
        let mut sorted = entries;
        sorted.sort_by(posting_order);
        sorted.dedup();
        let image = encode_v2_in_memory(&sorted);
        Self::from_bytes(SidecarBytes::Heap(Arc::from(image)))
            .expect("an image this module encoded is a valid v2 sidecar")
    }

    /// Open, validate and map a sidecar file. A v1 file is first converted to
    /// v2 in place (bounded memory, atomic rename). Returns
    /// `IndexError::Corrupt` for a bad magic, CRC, footer, entry or order.
    pub fn open(path: &Path) -> IndexResult<Self> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        if (len as usize) < HEADER_SIZE {
            return Err(IndexError::Corrupt(
                "sidecar file too short for header".into(),
            ));
        }
        // SAFETY: sidecar files are immutable once published — every writer
        // renames a complete temp file into place and nothing truncates or
        // rewrites a published file — so the mapping's bytes cannot change
        // or shrink while this reader holds it.
        let map = unsafe { memmap2::Mmap::map(&file)? };
        drop(file);
        let version = validate_header(&map)?.0;
        if version == SIDECAR_VERSION_V1 {
            convert_v1_file(path, &map)?;
            drop(map);
            return Self::open(path);
        }
        #[cfg(unix)]
        if let Err(error) = map.advise(memmap2::Advice::Random) {
            // A readahead hint only; lookups are correct without it.
            tracing::debug!(%error, path = %path.display(), "sidecar: madvise(RANDOM) refused");
        }
        Self::from_bytes(SidecarBytes::Mapped(Arc::new(MappedSidecar::new(map))))
    }

    /// Validate a v2 image and build a reader over it.
    fn from_bytes(bytes: SidecarBytes) -> IndexResult<Self> {
        let data = bytes.as_slice();
        let (version, entry_count) = validate_header(data)?;
        if version != SIDECAR_VERSION {
            return Err(IndexError::Corrupt(format!(
                "unsupported sidecar version: {version}"
            )));
        }
        let offsets_start = validate_v2_body(data, entry_count)?;
        let bytes = if entry_count == 0 {
            SidecarBytes::Empty
        } else {
            bytes
        };
        Ok(Self {
            bytes,
            entry_count,
            offsets_start,
        })
    }

    /// Number of entries in the sidecar index.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Bytes this reader maps (or holds, for an in-memory image).
    pub fn byte_len(&self) -> usize {
        self.bytes.as_slice().len()
    }

    /// Whether the reader decodes from a memory map (not the heap).
    pub fn is_mapped(&self) -> bool {
        matches!(self.bytes, SidecarBytes::Mapped(_) | SidecarBytes::Empty)
    }

    /// Entry `index`, decoded in place. The body was checked entry by entry
    /// at open and the bytes are immutable, so decoding cannot fail here.
    fn entry(&self, index: u64) -> EntryRef<'_> {
        let data = self.bytes.as_slice();
        let slot = self.offsets_start + index as usize * OFFSET_SIZE;
        let offset = u64::from_le_bytes(
            data[slot..slot + OFFSET_SIZE]
                .try_into()
                .expect("offset slot is 8 bytes"),
        ) as usize;
        decode_entry(data, offset, self.offsets_start)
            .expect("sidecar entries were validated when the reader was opened")
    }

    /// The first entry index for which `before` is false; `before` must be
    /// true for a prefix of the entries and false after it.
    fn partition_point(&self, mut before: impl FnMut(&EntryRef<'_>) -> bool) -> u64 {
        let (mut low, mut high) = (0u64, self.entry_count);
        while low < high {
            let mid = low + (high - low) / 2;
            if before(&self.entry(mid)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    /// Entries from index `start` while they carry `key`.
    fn key_run<'a>(&'a self, start: u64, key: &'a [u8]) -> impl Iterator<Item = EntryRef<'a>> + 'a {
        (start..self.entry_count)
            .map(move |index| self.entry(index))
            .take_while(move |entry| entry.key == key)
    }

    /// Every entry as `(key, position)`, in `(key, row)` order, borrowed from
    /// the mapping — one entry decoded at a time.
    pub fn entries_in_order(&self) -> impl Iterator<Item = (&[u8], RowPositionRef<'_>)> + '_ {
        (0..self.entry_count).map(move |index| {
            let entry = self.entry(index);
            (entry.key, entry.position)
        })
    }

    /// Returns all entries as `(IndexKey, RowPosition)` pairs. Materializes
    /// the file — for tests and offline tools, not the read path.
    pub fn all_entries(&self) -> Vec<(IndexKey, RowPosition)> {
        (0..self.entry_count)
            .map(|index| {
                let entry = self.entry(index);
                (
                    IndexKey(entry.key.to_vec()),
                    entry.position.to_owned_position(),
                )
            })
            .collect()
    }

    /// Point lookup: returns all `RowPosition`s whose key exactly matches.
    pub fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let start = self.partition_point(|entry| entry.key < key.0.as_slice());
        Ok(self
            .key_run(start, &key.0)
            .map(|entry| entry.position.to_owned_position())
            .collect())
    }

    /// The key's postings at or after `from` (all of them when `None`), in
    /// row order, borrowed from the mapping — a binary search to the start,
    /// then a walk that ends at the first entry of another key.
    pub fn postings_from<'a>(
        &'a self,
        key: &'a IndexKey,
        from: Option<&RowPosition>,
    ) -> impl Iterator<Item = RowPositionRef<'a>> + 'a {
        let start = self.partition_point(|entry| match entry.key.cmp(key.0.as_slice()) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => {
                from.is_some_and(|start| entry.position < RowPositionRef::from(start))
            }
            std::cmp::Ordering::Greater => false,
        });
        self.key_run(start, &key.0).map(|entry| entry.position)
    }

    /// Visit exact-key postings in place without allocating a result vector.
    /// The visitor may stop the scan after a page has been filled.
    pub fn visit(
        &self,
        key: &IndexKey,
        visitor: &mut dyn FnMut(RowPosition) -> ControlFlow<()>,
    ) -> IndexResult<()> {
        let start = self.partition_point(|entry| entry.key < key.0.as_slice());
        for entry in self.key_run(start, &key.0) {
            if visitor(entry.position.to_owned_position()).is_break() {
                break;
            }
        }
        Ok(())
    }

    /// Range query: returns all `RowPosition`s for keys in `[start, end]`
    /// inclusive.
    pub fn range(&self, start: &IndexKey, end: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let first = self.partition_point(|entry| entry.key < start.0.as_slice());
        Ok((first..self.entry_count)
            .map(|index| self.entry(index))
            .take_while(|entry| entry.key <= end.0.as_slice())
            .map(|entry| entry.position.to_owned_position())
            .collect())
    }
}

// ── Validation and decoding ──────────────────────────────────────────────────

/// Check magic and header CRC; return `(version, entry_count)`.
fn validate_header(data: &[u8]) -> IndexResult<(u8, u64)> {
    if data.len() < HEADER_SIZE {
        return Err(IndexError::Corrupt(
            "sidecar file too short for header".into(),
        ));
    }
    if &data[..4] != SIDECAR_MAGIC {
        return Err(IndexError::Corrupt("invalid sidecar magic bytes".into()));
    }
    let computed_crc = crc32fast::hash(&data[..CRC_INPUT_SIZE]);
    let stored_crc = u32::from_le_bytes(
        data[CRC_INPUT_SIZE..HEADER_SIZE]
            .try_into()
            .expect("4-byte slice"),
    );
    if computed_crc != stored_crc {
        return Err(IndexError::Corrupt(format!(
            "sidecar CRC mismatch: stored={stored_crc:#x}, computed={computed_crc:#x}"
        )));
    }
    let version = data[4];
    if version != SIDECAR_VERSION && version != SIDECAR_VERSION_V1 {
        return Err(IndexError::Corrupt(format!(
            "unsupported sidecar version: {version}"
        )));
    }
    let entry_count = u64::from_le_bytes(data[5..CRC_INPUT_SIZE].try_into().expect("8-byte slice"));
    Ok((version, entry_count))
}

/// Validate a v2 body in one pass over the bytes — footer, CRC, every offset,
/// every entry's bounds, and strictly increasing `(key, row)` order — holding
/// only the previous entry's borrowed slices. Returns the offset table start.
fn validate_v2_body(data: &[u8], entry_count: u64) -> IndexResult<usize> {
    if data.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(IndexError::Corrupt(
            "sidecar file too short for footer".into(),
        ));
    }
    let footer = &data[data.len() - FOOTER_SIZE..];
    if &footer[12..] != FOOTER_MAGIC {
        return Err(IndexError::Corrupt("sidecar footer magic missing".into()));
    }
    let offsets_start = u64::from_le_bytes(footer[..8].try_into().expect("8-byte slice")) as usize;
    let stored_crc = u32::from_le_bytes(footer[8..12].try_into().expect("4-byte slice"));
    let table_len = (entry_count as usize)
        .checked_mul(OFFSET_SIZE)
        .ok_or_else(|| IndexError::Corrupt("sidecar entry count overflows".into()))?;
    if offsets_start < HEADER_SIZE || offsets_start + table_len + FOOTER_SIZE != data.len() {
        return Err(IndexError::Corrupt(
            "sidecar offset table does not match the file length".into(),
        ));
    }
    let computed_crc = crc32fast::hash(&data[HEADER_SIZE..offsets_start + table_len]);
    if computed_crc != stored_crc {
        return Err(IndexError::Corrupt(format!(
            "sidecar body CRC mismatch: stored={stored_crc:#x}, computed={computed_crc:#x}"
        )));
    }
    let mut expected_offset = HEADER_SIZE;
    let mut previous: Option<EntryRef<'_>> = None;
    for index in 0..entry_count as usize {
        let slot = offsets_start + index * OFFSET_SIZE;
        let offset = u64::from_le_bytes(
            data[slot..slot + OFFSET_SIZE]
                .try_into()
                .expect("8-byte slice"),
        ) as usize;
        if offset != expected_offset {
            return Err(IndexError::Corrupt(format!(
                "sidecar entry {index} offset {offset} is not contiguous (expected {expected_offset})"
            )));
        }
        let entry = decode_entry(data, offset, offsets_start)?;
        if let Some(prev) = previous {
            if (prev.key, prev.position) >= (entry.key, entry.position) {
                return Err(IndexError::Corrupt(format!(
                    "sidecar entry {index} is out of (key, row) order"
                )));
            }
        }
        expected_offset = offset + entry.len;
        previous = Some(entry);
    }
    if expected_offset != offsets_start {
        return Err(IndexError::Corrupt(
            "sidecar body length does not match its entries".into(),
        ));
    }
    Ok(offsets_start)
}

/// Decode the entry at `offset`, which must end by `limit`.
fn decode_entry(data: &[u8], offset: usize, limit: usize) -> IndexResult<EntryRef<'_>> {
    let mut cursor = offset;
    let key = read_field(data, &mut cursor, limit)?;
    let partition_key = read_field(data, &mut cursor, limit)?;
    let clustering_key = read_field(data, &mut cursor, limit)?;
    Ok(EntryRef {
        key,
        position: RowPositionRef {
            partition_key,
            clustering_key,
        },
        len: cursor - offset,
    })
}

/// Read a `u32 LE` length and that many bytes, borrowed, ending by `limit`.
fn read_field<'a>(data: &'a [u8], cursor: &mut usize, limit: usize) -> IndexResult<&'a [u8]> {
    if *cursor + 4 > limit {
        return Err(IndexError::Corrupt(format!(
            "unexpected EOF at offset {cursor} reading length"
        )));
    }
    let len =
        u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().expect("4-byte slice")) as usize;
    *cursor += 4;
    if *cursor + len > limit {
        return Err(IndexError::Corrupt(format!(
            "unexpected EOF at offset {cursor} reading {len} bytes"
        )));
    }
    let field = &data[*cursor..*cursor + len];
    *cursor += len;
    Ok(field)
}

// ── v1 conversion ────────────────────────────────────────────────────────────

/// One posting as the spilling sort carries it.
#[derive(serde::Serialize, serde::Deserialize)]
struct SpillPosting {
    key: Vec<u8>,
    partition_key: Vec<u8>,
    clustering_key: Vec<u8>,
}

impl crate::external_sort::SpillRow for SpillPosting {
    fn estimated_bytes(&self) -> usize {
        72 + self.key.len() + self.partition_key.len() + self.clustering_key.len()
    }
}

/// `(key, row)` order for [`SpillPosting`].
#[derive(Clone)]
struct SpillPostingOrder;

impl crate::external_sort::SpillOrder<SpillPosting> for SpillPostingOrder {
    fn compare(&self, a: &SpillPosting, b: &SpillPosting) -> std::cmp::Ordering {
        (&a.key, &a.partition_key, &a.clustering_key).cmp(&(
            &b.key,
            &b.partition_key,
            &b.clustering_key,
        ))
    }
}

/// Rewrite a v1 sidecar at `path` as v2: stream its entries off the mapping
/// into a spilling sort (memory bounded by the spill threshold), then stream
/// the sorted, deduplicated result through the atomic v2 writer.
fn convert_v1_file(path: &Path, v1: &[u8]) -> IndexResult<()> {
    let (_, entry_count) = validate_header(v1)?;
    let spill_dir = temp_sibling(path, "convert");
    std::fs::create_dir_all(&spill_dir)?;
    let converted = convert_v1_through(&spill_dir, path, v1, entry_count);
    if let Err(error) = std::fs::remove_dir_all(&spill_dir) {
        tracing::warn!(%error, dir = %spill_dir.display(), "sidecar: could not remove the v1 conversion spill directory");
    }
    let written = converted?;
    tracing::info!(
        path = %path.display(),
        entries = entry_count,
        written,
        "sidecar: converted a v1 sidecar to the mapped v2 format"
    );
    Ok(())
}

fn convert_v1_through(
    spill_dir: &Path,
    path: &Path,
    v1: &[u8],
    entry_count: u64,
) -> IndexResult<u64> {
    let spill_error = |error: ferrosa_common::Error| {
        IndexError::Corrupt(format!("v1 sidecar conversion: {error}"))
    };
    let mut sorter = crate::external_sort::ExternalSorter::new(
        spill_dir,
        SpillPostingOrder,
        crate::spill_budget::process_spill_threshold_bytes(),
    );
    let mut cursor = HEADER_SIZE;
    for _ in 0..entry_count {
        let entry = decode_entry(v1, cursor, v1.len())?;
        cursor += entry.len;
        sorter
            .push(SpillPosting {
                key: entry.key.to_vec(),
                partition_key: entry.position.partition_key.to_vec(),
                clustering_key: entry.position.clustering_key.to_vec(),
            })
            .map_err(spill_error)?;
    }
    let sorted = sorter.finish().map_err(spill_error)?;
    SidecarWriter::write_sorted(
        path,
        sorted.map(|posting| {
            posting.map_err(spill_error).map(|posting| {
                (
                    IndexKey(posting.key),
                    RowPosition {
                        partition_key: posting.partition_key,
                        clustering_key: posting.clustering_key,
                    },
                )
            })
        }),
    )
}

/// Encode `entries` in the v1 layout (header, then entries in the order
/// given). Test support for the conversion path.
#[cfg(test)]
fn encode_v1_for_test(entries: &[(IndexKey, RowPosition)]) -> Vec<u8> {
    let mut image = header_bytes(SIDECAR_VERSION_V1, entries.len() as u64).to_vec();
    for (key, position) in entries {
        image.extend_from_slice(&encode_entry(key, position));
    }
    image
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_index::{IndexKey, RowPosition};
    use tempfile::tempdir;

    fn row(pk: &[u8], ck: &[u8]) -> RowPosition {
        RowPosition {
            partition_key: pk.to_vec(),
            clustering_key: ck.to_vec(),
        }
    }

    fn visit_all(reader: &SidecarReader, key: &IndexKey) -> Vec<RowPosition> {
        let mut visited = Vec::new();
        reader
            .visit(key, &mut |position| {
                visited.push(position);
                ControlFlow::Continue(())
            })
            .unwrap();
        visited
    }

    /// A key's postings are visited in row order — (partition key,
    /// clustering) — whether the sidecar was written now or by the previous
    /// writer, which sorted by index key only and left each key's postings in
    /// arrival order. Ordered postings let an index read merge sources and
    /// resume from a cursor without holding the result (t_50c8bc7d).
    #[test]
    fn a_keys_postings_are_visited_in_row_order_new_and_legacy() {
        let tenant = IndexKey(b"tenant-a".to_vec());
        let arrival = vec![
            (tenant.clone(), row(b"pk3", b"")),
            (tenant.clone(), row(b"pk1", b"ck2")),
            (IndexKey(b"other".to_vec()), row(b"pk0", b"")),
            (tenant.clone(), row(b"pk1", b"ck1")),
        ];
        let expected = vec![row(b"pk1", b"ck1"), row(b"pk1", b"ck2"), row(b"pk3", b"")];

        let dir = tempdir().unwrap();
        let fresh = dir.path().join("fresh.sidecar");
        SidecarWriter::write(&fresh, &arrival).unwrap();
        assert_eq!(
            visit_all(&SidecarReader::open(&fresh).unwrap(), &tenant),
            expected
        );

        // The previous writer's layout: v1, sorted by index key only. Opening
        // it converts it to the mapped v2 format in place.
        let mut legacy_order = arrival.clone();
        legacy_order.sort_by(|a, b| a.0.cmp(&b.0));
        let legacy_path = dir.path().join("legacy.sidecar");
        std::fs::write(&legacy_path, encode_v1_for_test(&legacy_order)).unwrap();
        let legacy = SidecarReader::open(&legacy_path).unwrap();
        assert_eq!(visit_all(&legacy, &tenant), expected);
        assert!(
            legacy.is_mapped(),
            "a converted v1 sidecar is read through a map"
        );
        assert_eq!(
            std::fs::read(&legacy_path).unwrap()[4],
            SIDECAR_VERSION,
            "the v1 file was rewritten as v2"
        );

        assert_eq!(
            visit_all(&SidecarReader::from_entries(arrival), &tenant),
            expected
        );
    }

    /// A reader holding a mapping keeps reading correctly when the sidecar is
    /// rewritten: writers publish by rename, never by truncating the file in
    /// place (a truncated mapped file faults with SIGBUS).
    #[test]
    fn rewriting_a_sidecar_leaves_a_mapped_reader_intact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("7-idx.sidecar");
        let key = IndexKey(b"k".to_vec());
        let before: Vec<_> = (0..500)
            .map(|i| (key.clone(), row(format!("pk{i:04}").as_bytes(), b"")))
            .collect();
        SidecarWriter::write(&path, &before).unwrap();
        let mapped = SidecarReader::open(&path).unwrap();
        assert!(mapped.is_mapped());

        SidecarWriter::write(&path, &[(key.clone(), row(b"only", b""))]).unwrap();

        assert_eq!(
            visit_all(&mapped, &key).len(),
            500,
            "the old mapping is untouched"
        );
        assert_eq!(
            visit_all(&SidecarReader::open(&path).unwrap(), &key),
            vec![row(b"only", b"")],
            "a new reader sees the new file"
        );
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp files left behind: {leftovers:?}"
        );
    }

    /// Mapped sidecars are counted: bytes and files are exported as gauges,
    /// held for exactly the life of the mapping. Lower bounds only — other
    /// tests map sidecars concurrently.
    #[test]
    fn mapped_sidecars_are_counted_for_the_life_of_the_mapping() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("7-idx.sidecar");
        SidecarWriter::write(&path, &sample_entries()).unwrap();
        let reader = SidecarReader::open(&path).unwrap();
        let clone = reader.clone();
        assert!(crate::metrics::index_sidecar_mapped_files() >= 1);
        assert!(crate::metrics::index_sidecar_mapped_bytes() >= reader.byte_len() as i64);
        drop(reader);
        assert!(
            crate::metrics::index_sidecar_mapped_files() >= 1,
            "a clone shares the mapping, so it is still counted"
        );
        drop(clone);

        let rendered = crate::metrics::render_prometheus();
        assert!(rendered.contains("ferrosa_storage_index_sidecar_mapped_bytes"));
        assert!(rendered.contains("ferrosa_storage_index_sidecar_mapped_files"));
    }

    fn sample_entries() -> Vec<(IndexKey, RowPosition)> {
        vec![
            (
                IndexKey(b"alice".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: b"ck1".to_vec(),
                },
            ),
            (
                IndexKey(b"bob".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: b"ck2".to_vec(),
                },
            ),
            (
                IndexKey(b"charlie".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ]
    }

    // ── Task 3: Sidecar file format tests ─────────────────────────────────────

    #[test]
    fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sidecar");

        let entries = sample_entries();
        SidecarWriter::write(&path, &entries).unwrap();

        let reader = SidecarReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 3);

        let results = reader.lookup(&IndexKey(b"bob".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[0].clustering_key, b"ck2");
    }

    #[test]
    fn empty_sidecar_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sidecar");

        SidecarWriter::write(&path, &[]).unwrap();

        let reader = SidecarReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 0);
        let results = reader.lookup(&IndexKey(b"anything".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn corrupt_magic_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_magic.sidecar");

        SidecarWriter::write(&path, &sample_entries()).unwrap();

        // Corrupt the magic bytes
        let mut data = std::fs::read(&path).unwrap();
        data[0] = 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = SidecarReader::open(&path);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("corrupt") || err_msg.contains("magic"),
            "expected corruption error, got: {err_msg}"
        );
    }

    #[test]
    fn corrupt_crc_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_crc.sidecar");

        SidecarWriter::write(&path, &sample_entries()).unwrap();

        // Corrupt the entry_count field (byte 5-12) to trigger CRC mismatch
        let mut data = std::fs::read(&path).unwrap();
        data[6] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = SidecarReader::open(&path);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("corrupt") || err_msg.contains("CRC"),
            "expected CRC error, got: {err_msg}"
        );
    }

    #[test]
    fn file_too_short_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("short.sidecar");
        std::fs::write(&path, [0u8; 10]).unwrap();

        let result = SidecarReader::open(&path);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("too short"),
            "expected too-short error, got: {err_msg}"
        );
    }

    #[test]
    fn unsorted_entries_are_sorted_on_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("unsorted.sidecar");

        // Provide entries in reverse order
        let entries = vec![
            (
                IndexKey(b"zzz".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"aaa".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"mmm".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&path, &entries).unwrap();
        let reader = SidecarReader::open(&path).unwrap();

        // Range query should work correctly since entries are sorted
        let results = reader
            .range(&IndexKey(b"aaa".to_vec()), &IndexKey(b"mmm".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk1");
        assert_eq!(results[1].partition_key, b"pk2");
    }

    // ── Task 4: BTree adapter + range query tests ─────────────────────────────

    #[test]
    fn build_sidecar_from_btree_entries_and_lookup() {
        let dir = tempdir().unwrap();
        let sidecar_path = dir.path().join("from_btree.sidecar");

        // Build entries using the same row data that BTreeBuilder would process
        let entries = vec![
            (
                IndexKey(b"alpha".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: b"ck1".to_vec(),
                },
            ),
            (
                IndexKey(b"beta".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: b"ck2".to_vec(),
                },
            ),
            (
                IndexKey(b"gamma".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&sidecar_path, &entries).unwrap();
        let reader = SidecarReader::open(&sidecar_path).unwrap();

        // Point lookup
        let results = reader.lookup(&IndexKey(b"beta".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");

        // Missing key
        let results = reader.lookup(&IndexKey(b"missing".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn sidecar_range_query() {
        let dir = tempdir().unwrap();
        let sidecar_path = dir.path().join("range.sidecar");

        let entries = vec![
            (
                IndexKey(b"aaa".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"bbb".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"ccc".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"ddd".to_vec()),
                RowPosition {
                    partition_key: b"pk4".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&sidecar_path, &entries).unwrap();
        let reader = SidecarReader::open(&sidecar_path).unwrap();

        let results = reader
            .range(&IndexKey(b"bbb".to_vec()), &IndexKey(b"ccc".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[1].partition_key, b"pk3");
    }

    #[test]
    fn sidecar_range_query_full_range() {
        let dir = tempdir().unwrap();
        let sidecar_path = dir.path().join("full_range.sidecar");

        let entries = vec![
            (
                IndexKey(b"aaa".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"zzz".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&sidecar_path, &entries).unwrap();
        let reader = SidecarReader::open(&sidecar_path).unwrap();

        let results = reader
            .range(&IndexKey(b"aaa".to_vec()), &IndexKey(b"zzz".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    // ── Task 4.4: CRC32 validation tests ─────────────────────────────────────

    #[test]
    fn single_bit_flip_in_header_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bitflip.sidecar");
        SidecarWriter::write(&path, &sample_entries()).unwrap();
        let mut data = std::fs::read(&path).unwrap();
        data[7] ^= 0x01; // flip one bit in entry_count
        std::fs::write(&path, &data).unwrap();
        assert!(SidecarReader::open(&path).is_err());
    }

    #[test]
    fn valid_sidecar_opens_without_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("valid.sidecar");
        SidecarWriter::write(&path, &sample_entries()).unwrap();
        assert!(SidecarReader::open(&path).is_ok());
    }

    #[test]
    fn multiple_rows_same_key_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dupes.sidecar");

        let entries = vec![
            (
                IndexKey(b"same".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: b"ck1".to_vec(),
                },
            ),
            (
                IndexKey(b"same".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: b"ck2".to_vec(),
                },
            ),
            (
                IndexKey(b"other".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&path, &entries).unwrap();
        let reader = SidecarReader::open(&path).unwrap();

        let results = reader.lookup(&IndexKey(b"same".to_vec())).unwrap();
        assert_eq!(results.len(), 2);
        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"pk1".as_slice()));
        assert!(pks.contains(&b"pk2".as_slice()));
    }
}
