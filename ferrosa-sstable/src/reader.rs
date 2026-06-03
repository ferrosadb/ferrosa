//! SSTableReader -- compose all components into a single read interface.
//!
//! Opens a BTI SSTable from component file handles and provides:
//! - Partition lookup by DecoratedKey
//! - Full partition iteration in token order

use ferrosa_common::{DecoratedKey, Result};

use crate::bloom::BloomFilter;
use crate::compression::CompressionInfo;
use crate::data::DataReader;
use crate::io::{CachedReadAt, ReadAt};
use crate::partition_index::{PartitionIndex, PartitionLookup};
use crate::row_index::{lookup_clustering_in_entry, RowIndex};
use crate::statistics::{read_statistics, SerializationHeader};
use crate::types::Partition;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

const DEFAULT_DECOMPRESSED_CHUNK_CACHE_ENTRIES: usize = 128;

fn decompressed_chunk_cache_capacity() -> NonZeroUsize {
    let requested = std::env::var("FERROSA_SSTABLE_CHUNK_CACHE_ENTRIES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_DECOMPRESSED_CHUNK_CACHE_ENTRIES);
    NonZeroUsize::new(requested).expect("decompressed chunk cache capacity is non-zero")
}

fn read_compressed_chunk<R: ReadAt>(
    data: &R,
    ci: &CompressionInfo,
    compressed_file_len: u64,
    chunk_index: usize,
) -> Result<Vec<u8>> {
    let Some(&chunk_offset) = ci.chunk_offsets.get(chunk_index) else {
        return Err(ferrosa_common::Error::InvalidData(format!(
            "compressed chunk index {chunk_index} out of bounds"
        )));
    };
    let next_offset = ci
        .chunk_offsets
        .get(chunk_index + 1)
        .copied()
        .unwrap_or(compressed_file_len);
    if next_offset < chunk_offset {
        return Err(ferrosa_common::Error::InvalidFormat(format!(
            "compressed chunk offsets are not monotonic at index {chunk_index}"
        )));
    }

    let chunk_size = (next_offset - chunk_offset) as usize;
    if chunk_size < std::mem::size_of::<u32>() {
        return Err(ferrosa_common::Error::InvalidFormat(
            "compressed chunk shorter than CRC trailer".into(),
        ));
    }

    let mut compressed = vec![0u8; chunk_size];
    data.read_exact_at(&mut compressed, chunk_offset)?;

    let payload_len = chunk_size - std::mem::size_of::<u32>();
    let payload = &compressed[..payload_len];
    let stored_crc = u32::from_be_bytes([
        compressed[payload_len],
        compressed[payload_len + 1],
        compressed[payload_len + 2],
        compressed[payload_len + 3],
    ]);
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != stored_crc {
        return Err(ferrosa_common::Error::InvalidData(format!(
            "compressed chunk CRC mismatch at offset {chunk_offset}: expected {stored_crc:#010x}, got {actual_crc:#010x}"
        )));
    }

    let chunk = ci.compression.decompress(payload, ci.chunk_length)?;
    let chunk_start = chunk_index as u64 * ci.chunk_length as u64;
    let max_len = ci.data_length.saturating_sub(chunk_start);
    if chunk.len() as u64 > max_len.min(ci.chunk_length as u64) {
        return Err(ferrosa_common::Error::InvalidData(format!(
            "decompressed chunk {chunk_index} length {} exceeds expected bound",
            chunk.len()
        )));
    }
    Ok(chunk)
}

struct ChunkedCompressedData<'a, R: ReadAt> {
    data: &'a R,
    ci: &'a CompressionInfo,
    compressed_file_len: u64,
    cache: &'a Mutex<lru::LruCache<usize, Arc<Vec<u8>>>>,
}

impl<'a, R: ReadAt> ChunkedCompressedData<'a, R> {
    fn new(
        data: &'a R,
        ci: &'a CompressionInfo,
        cache: &'a Mutex<lru::LruCache<usize, Arc<Vec<u8>>>>,
    ) -> Result<Self> {
        if ci.chunk_length == 0 {
            return Err(ferrosa_common::Error::InvalidFormat(
                "compressed SSTable has zero chunk length".into(),
            ));
        }
        Ok(Self {
            data,
            ci,
            compressed_file_len: data.len()?,
            cache,
        })
    }

    fn chunk(&self, chunk_index: usize) -> Result<Arc<Vec<u8>>> {
        {
            let mut guard = self.cache.lock().expect("sstable chunk cache poisoned");
            if let Some(chunk) = guard.get(&chunk_index) {
                return Ok(Arc::clone(chunk));
            }
        }

        let chunk = Arc::new(read_compressed_chunk(
            self.data,
            self.ci,
            self.compressed_file_len,
            chunk_index,
        )?);

        let mut guard = self.cache.lock().expect("sstable chunk cache poisoned");
        if let Some(existing) = guard.get(&chunk_index) {
            return Ok(Arc::clone(existing));
        }
        guard.put(chunk_index, Arc::clone(&chunk));
        Ok(chunk)
    }
}

impl<R: ReadAt> ReadAt for ChunkedCompressedData<'_, R> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if buf.is_empty() || offset >= self.ci.data_length {
            return Ok(0);
        }

        let available = (self.ci.data_length - offset) as usize;
        let target = buf.len().min(available);
        let mut copied = 0usize;
        while copied < target {
            let pos = offset + copied as u64;
            let chunk_index = (pos / self.ci.chunk_length as u64) as usize;
            let chunk_offset = (pos % self.ci.chunk_length as u64) as usize;
            let chunk = self.chunk(chunk_index)?;
            if chunk_offset >= chunk.len() {
                break;
            }
            let n = (target - copied).min(chunk.len() - chunk_offset);
            buf[copied..copied + n].copy_from_slice(&chunk[chunk_offset..chunk_offset + n]);
            copied += n;
        }
        Ok(copied)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.ci.data_length)
    }
}

/// Handles to all component files for an SSTable.
pub struct SSTableComponents<R> {
    /// Data.db file handle.
    pub data: R,
    /// Partitions.db file handle.
    pub partitions: R,
    /// Rows.db file handle.
    pub rows: R,
    /// Bloom filter bytes (Filter.db, read fully into memory).
    pub filter: Vec<u8>,
    /// CompressionInfo.db bytes (`None` if uncompressed).
    pub compression_info: Option<Vec<u8>>,
    /// Statistics.db bytes.
    pub statistics: Vec<u8>,
}

/// Composes all SSTable component readers into a single read interface.
pub struct SSTableReader<R: ReadAt> {
    partition_index: PartitionIndex<CachedReadAt<R>>,
    bloom_filter: BloomFilter,
    compression_info: Option<CompressionInfo>,
    header: SerializationHeader,
    data: R,
    /// Bounded cache of decompressed compressed Data.db chunks. Point reads use
    /// this to avoid decompressing the whole SSTable for one partition/row.
    decompressed_chunks: Mutex<lru::LruCache<usize, Arc<Vec<u8>>>>,
    #[allow(dead_code)]
    rows: CachedReadAt<R>,
    /// Lazily-populated sorted list of partition start offsets in
    /// `data`. Built by walking the data file once via
    /// `DataReader::read_partition_count` (no cell decode, only row
    /// header walking) on the first call to `partition_offsets`.
    /// Used by `PartitionIter::skip_to_next_partition` so the merger
    /// can advance past duplicate-key sources in O(log N) without
    /// decoding any partition body — the cold-cache dominant cost
    /// (see bug-streaming-range-read-perf-50x-floor).
    partition_offsets: std::sync::OnceLock<std::sync::Arc<Vec<u64>>>,
    /// Lazily-populated `(token, byte-offset)` pairs in Data.db
    /// order (which is token-sorted). Built by walking the index
    /// with `peek_partition_key + skip_to_next_partition` once.
    /// Backs `PartitionIter::seek_to_token` so a token-bounded read
    /// can jump straight to the first matching partition instead of
    /// decoding every preceding partition — the difference between
    /// O(matches) and O(table_size) per repair session.
    partition_token_offsets: std::sync::OnceLock<std::sync::Arc<Vec<(i64, u64)>>>,
}

impl<R: ReadAt> SSTableReader<R> {
    /// Open an SSTable from its component file handles.
    ///
    /// Parses the bloom filter, compression info, and statistics from their
    /// in-memory byte buffers, and opens the partition index from its reader.
    pub fn open(components: SSTableComponents<R>) -> Result<Self> {
        let bloom_filter = BloomFilter::read(&components.filter)?;

        let compression_info = match components.compression_info {
            Some(ref ci_bytes) => Some(CompressionInfo::read(ci_bytes)?),
            None => None,
        };

        let stats = read_statistics(&components.statistics)?;
        let header = stats.header;

        let partition_index = PartitionIndex::open(CachedReadAt::new(components.partitions)?)?;
        let rows = CachedReadAt::new(components.rows)?;

        Ok(SSTableReader {
            partition_index,
            bloom_filter,
            compression_info,
            header,
            data: components.data,
            decompressed_chunks: Mutex::new(
                lru::LruCache::new(decompressed_chunk_cache_capacity()),
            ),
            rows,
            partition_offsets: std::sync::OnceLock::new(),
            partition_token_offsets: std::sync::OnceLock::new(),
        })
    }

    /// Sorted list of partition start offsets in this SSTable's
    /// `Data.db`. Lazily built on first call by walking the data
    /// file with `read_partition_count` (no cell decode); cached for
    /// the SSTable's lifetime. Subsequent calls are O(1).
    ///
    /// Used by `PartitionIter::skip_to_next_partition` for the
    /// merger's duplicate-key dedup path — pre-decoded offsets
    /// reduce a "skip past this partition" call from `O(rows × cold
    /// page faults)` (the row-walking cost of
    /// `next_partition_metadata`) to one binary search + one pos
    /// assignment.
    ///
    /// On error the offsets list is left empty and a warning is
    /// logged; callers fall back to body-decode advancement.
    pub fn partition_offsets(&self) -> std::sync::Arc<Vec<u64>> {
        self.partition_offsets
            .get_or_init(|| {
                let mut offsets: Vec<u64> = Vec::new();
                let mut iter = match self.partitions_iter() {
                    Ok(it) => it,
                    Err(_) => {
                        // Caller falls back to body-decode advance.
                        return std::sync::Arc::new(offsets);
                    }
                };
                loop {
                    let pos_before = iter.pos;
                    match iter.next_partition_count() {
                        Ok(Some(_)) => offsets.push(pos_before),
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
                std::sync::Arc::new(offsets)
            })
            .clone()
    }

    /// Sorted `(token, byte-offset-in-Data.db)` pairs for every
    /// partition in this SSTable, in token order. Lazily built on
    /// first call by walking the file with `peek_partition_key +
    /// skip_to_next_partition` (key decode only — no row bodies).
    /// Cached for the SSTable's lifetime; subsequent calls are O(1).
    ///
    /// Used by `PartitionIter::seek_to_token` to turn anti-entropy
    /// repair's "give me the partitions in token range `[a, b)`"
    /// query from O(table_size) per session into O(log N + matches)
    /// per session — the structural fix that makes repair viable on
    /// a multi-GB table.
    ///
    /// On error the list is left empty and a warning is logged;
    /// callers can detect the empty cache and fall back to a
    /// linear scan from byte 0.
    pub fn partition_token_offsets(&self) -> std::sync::Arc<Vec<(i64, u64)>> {
        self.partition_token_offsets
            .get_or_init(|| {
                let mut out: Vec<(i64, u64)> = Vec::new();
                let mut iter = match self.partitions_iter() {
                    Ok(it) => it,
                    Err(_) => {
                        // Match `partition_offsets()`: silent empty
                        // cache; callers detect and fall back.
                        return std::sync::Arc::new(out);
                    }
                };
                loop {
                    let pos = iter.pos;
                    match iter.peek_partition_key() {
                        Ok(Some(dk)) => out.push((dk.token.0, pos)),
                        Ok(None) => break,
                        Err(_) => break,
                    }
                    if iter.skip_to_next_partition().is_err() {
                        break;
                    }
                }
                std::sync::Arc::new(out)
            })
            .clone()
    }

    /// Look up a partition by its decorated key.
    ///
    /// 1. Checks the bloom filter; returns `None` immediately if the key is
    ///    definitely absent.
    /// 2. Looks up the key in the partition index trie.
    /// 3. Reads the partition from Data.db at the resolved position.
    pub fn get_partition(&self, key: &DecoratedKey) -> Result<Option<Partition>> {
        self.get_partition_limited_rows(key, 0)
    }

    /// Look up a partition by its decorated key, retaining at most
    /// `row_limit` clustered rows when the limit is non-zero.
    ///
    /// This is the point-read counterpart to
    /// [`Self::read_partitions_limited_rows`]. CQL single-partition
    /// `LIMIT` queries use it so a wide partition can return its first
    /// requested rows without decoding the full row body first.
    pub fn get_partition_limited_rows(
        &self,
        key: &DecoratedKey,
        row_limit: usize,
    ) -> Result<Option<Partition>> {
        // Step 1: bloom filter check
        let (h1, h2) = key.filter_hash();
        if !self.bloom_filter.is_present(h1, h2) {
            return Ok(None);
        }

        // Step 2: partition index lookup
        let lookup = self.partition_index.lookup(key)?;

        let data_position = match lookup {
            PartitionLookup::RowIndex { position } => {
                let entry = RowIndex::read_entry(&self.rows, position)?;
                entry.data_position
            }
            PartitionLookup::DataDirect { position } => position,
            PartitionLookup::NotFound => return Ok(None),
        };

        // Step 3: read partition from Data.db. For compressed tables, expose
        // an uncompressed-offset view that decompresses only addressed chunks.
        if let Some(ref ci) = self.compression_info {
            let chunked = ChunkedCompressedData::new(&self.data, ci, &self.decompressed_chunks)?;
            let mut data_reader = DataReader::new(&chunked, &self.header, data_position);
            if row_limit > 0 {
                data_reader.read_partition_prefix_rows(row_limit)
            } else {
                data_reader.read_partition_limited_rows(0)
            }
        } else {
            let mut data_reader = DataReader::new(&self.data, &self.header, data_position);
            if row_limit > 0 {
                data_reader.read_partition_prefix_rows(row_limit)
            } else {
                data_reader.read_partition_limited_rows(0)
            }
        }
    }

    /// Look up a single clustered row by partition key and clustering bytes.
    ///
    /// New SSTables may carry a Rows.db entry for wide clustered partitions;
    /// in that case this jumps directly to the row offset. Legacy SSTables
    /// without Rows.db fall back to a streaming in-partition scan.
    pub fn get_clustering_row(
        &self,
        key: &DecoratedKey,
        clustering: &[u8],
    ) -> Result<Option<Partition>> {
        let (h1, h2) = key.filter_hash();
        if !self.bloom_filter.is_present(h1, h2) {
            return Ok(None);
        }

        let lookup = self.partition_index.lookup(key)?;
        let (data_position, row_index_position) = match lookup {
            PartitionLookup::RowIndex { position } => {
                let entry = RowIndex::read_entry(&self.rows, position)?;
                let Some(row_offset) = lookup_clustering_in_entry(&self.rows, &entry, clustering)?
                else {
                    return Ok(None);
                };
                (entry.data_position + row_offset, Some(entry))
            }
            PartitionLookup::DataDirect { position } => (position, None),
            PartitionLookup::NotFound => return Ok(None),
        };

        if let Some(entry) = row_index_position {
            let (partition_key, deletion, static_row, row) =
                if let Some(ref ci) = self.compression_info {
                    let chunked =
                        ChunkedCompressedData::new(&self.data, ci, &self.decompressed_chunks)?;
                    let mut header_reader =
                        DataReader::new(&chunked, &self.header, entry.data_position);
                    let Some((partition_key, deletion, static_row)) =
                        header_reader.read_partition_header_only()?
                    else {
                        return Ok(None);
                    };
                    let mut row_reader = DataReader::new(&chunked, &self.header, data_position);
                    let Some(row) = row_reader.read_next_clustered_row()? else {
                        return Ok(None);
                    };
                    (partition_key, deletion, static_row, row)
                } else {
                    let mut header_reader =
                        DataReader::new(&self.data, &self.header, entry.data_position);
                    let Some((partition_key, deletion, static_row)) =
                        header_reader.read_partition_header_only()?
                    else {
                        return Ok(None);
                    };

                    let mut row_reader = DataReader::new(&self.data, &self.header, data_position);
                    let Some(row) = row_reader.read_next_clustered_row()? else {
                        return Ok(None);
                    };
                    (partition_key, deletion, static_row, row)
                };
            if row.clustering != clustering {
                return Ok(None);
            }
            return Ok(Some(Partition {
                key: partition_key,
                deletion,
                static_row,
                rows: vec![row],
            }));
        }

        if let Some(ref ci) = self.compression_info {
            let chunked = ChunkedCompressedData::new(&self.data, ci, &self.decompressed_chunks)?;
            return self.get_clustering_row_by_scan(&chunked, data_position, clustering);
        }

        self.get_clustering_row_by_scan(&self.data, data_position, clustering)
    }

    fn get_clustering_row_by_scan(
        &self,
        data: &impl ReadAt,
        data_position: u64,
        clustering: &[u8],
    ) -> Result<Option<Partition>> {
        let mut data_reader = DataReader::new(data, &self.header, data_position);
        let Some((partition_key, deletion, static_row)) =
            data_reader.read_partition_header_only()?
        else {
            return Ok(None);
        };
        while let Some(row) = data_reader.read_next_clustered_row()? {
            match row.clustering.as_slice().cmp(clustering) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => {
                    return Ok(Some(Partition {
                        key: partition_key,
                        deletion,
                        static_row,
                        rows: vec![row],
                    }));
                }
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Returns the number of partitions in this SSTable.
    pub fn key_count(&self) -> u64 {
        self.partition_index.key_count()
    }

    /// Returns a reference to the bloom filter.
    pub fn bloom_filter(&self) -> &BloomFilter {
        &self.bloom_filter
    }

    /// Returns a reference to the serialization header.
    pub fn header(&self) -> &SerializationHeader {
        &self.header
    }

    /// Returns a reference to the compression info, if present.
    pub fn compression_info(&self) -> Option<&CompressionInfo> {
        self.compression_info.as_ref()
    }

    /// Returns the length of the Data.db file (or buffer) in bytes.
    pub fn data_file_length(&self) -> Result<u64> {
        self.data.len()
    }

    /// Returns the approximate total size of this SSTable in bytes.
    ///
    /// Sums the sizes of the data file and the partition index. For
    /// in-memory readers (tests), this is the byte count of the `Vec<u8>`
    /// buffers. For file-backed readers, it reflects the actual file sizes.
    pub fn total_size(&self) -> u64 {
        let data_len = self.data.len().unwrap_or(0);
        let partitions_len = self.partition_index.file_size();
        data_len + partitions_len
    }

    /// Returns the smallest key stored in the partition index as raw
    /// byte-comparable encoded bytes. Decode with `byte_comparable::decode`.
    pub fn smallest_key_bytes(&self) -> &[u8] {
        self.partition_index.smallest_key()
    }

    /// Returns the largest key stored in the partition index as raw
    /// byte-comparable encoded bytes. Decode with `byte_comparable::decode`.
    pub fn largest_key_bytes(&self) -> &[u8] {
        self.partition_index.largest_key()
    }

    /// Read all partitions from this SSTable in storage order.
    ///
    /// Scans the Data.db file sequentially from position 0, reading each
    /// partition until EOF. **Materializes the entire SSTable into memory.**
    ///
    /// Prefer [`Self::partitions_iter`] for compaction and other large-scan
    /// callers — full materialization here was OOM-ing the compaction
    /// executor on tombstone-heavy workloads (`cql_timeseries2`, IoT TTL
    /// patterns).  See `specs/in-process/streaming-compaction.md`.
    pub fn read_all_partitions(&self) -> Result<Vec<crate::types::Partition>> {
        self.read_partitions_limited(usize::MAX)
    }

    /// Stream partitions from this SSTable in storage (token) order, one at
    /// a time, without materializing the whole file into memory.
    ///
    /// The returned iterator borrows this `SSTableReader` for its lifetime
    /// and decompresses the Data.db once up-front (for compressed
    /// SSTables) or reads directly from the underlying [`ReadAt`] (for
    /// uncompressed). Each call to [`PartitionIter::next_partition`] yields
    /// at most one partition; the iterator returns `Ok(None)` at EOF.
    ///
    /// Memory: `O(decompressed_data_size)` for compressed SSTables (single
    /// pre-decompressed buffer held for the iterator's lifetime), `O(1)`
    /// for uncompressed.  Independent of partition count.
    pub fn partitions_iter(&self) -> Result<PartitionIter<'_, R>> {
        PartitionIter::new(self)
    }

    /// Scan partitions sequentially, stopping once `limit` partitions have
    /// been decoded. This bounds range-read materialization while preserving
    /// the existing all-partitions API for compaction callers.
    pub fn read_partitions_limited(&self, limit: usize) -> Result<Vec<crate::types::Partition>> {
        self.read_partitions_limited_rows(limit, 0)
    }

    /// Scan partitions sequentially, retaining at most `row_limit` rows per
    /// decoded partition when `row_limit > 0`.
    pub fn read_partitions_limited_rows(
        &self,
        limit: usize,
        row_limit: usize,
    ) -> Result<Vec<crate::types::Partition>> {
        let mut partitions = Vec::new();
        if limit == 0 {
            return Ok(partitions);
        }
        if let Some(ref ci) = self.compression_info {
            let chunked = ChunkedCompressedData::new(&self.data, ci, &self.decompressed_chunks)?;
            let mut reader = crate::data::DataReader::new(&chunked, &self.header, 0);
            while partitions.len() < limit {
                let is_final_requested_partition = row_limit > 0 && partitions.len() + 1 == limit;
                let partition = if is_final_requested_partition {
                    reader.read_partition_prefix_rows(row_limit)?
                } else {
                    reader.read_partition_limited_rows(row_limit)?
                };
                let Some(partition) = partition else {
                    break;
                };
                partitions.push(partition);
            }
        } else {
            let mut reader = crate::data::DataReader::new(&self.data, &self.header, 0);
            while partitions.len() < limit {
                let is_final_requested_partition = row_limit > 0 && partitions.len() + 1 == limit;
                let partition = if is_final_requested_partition {
                    reader.read_partition_prefix_rows(row_limit)?
                } else {
                    reader.read_partition_limited_rows(row_limit)?
                };
                let Some(partition) = partition else {
                    break;
                };
                partitions.push(partition);
            }
        }
        Ok(partitions)
    }
}

/// Streaming partition iterator over an SSTable.
///
/// Returned by [`SSTableReader::partitions_iter`]. Yields partitions in
/// storage (token) order, one at a time. Compressed SSTables are read through a
/// bounded decompressed-chunk cache instead of inflating the entire Data.db.
///
/// Memory cost is constant in the number of partitions — only the
/// currently-yielded `Partition` is materialized.
pub struct PartitionIter<'a, R: ReadAt> {
    sst: &'a SSTableReader<R>,
    pos: u64,
    /// Chunked decompression view for compressed SSTables. `None` for
    /// uncompressed, where the iterator reads directly from `sst.data`.
    compressed: Option<ChunkedCompressedData<'a, R>>,
}

impl<'a, R: ReadAt> PartitionIter<'a, R> {
    fn new(sst: &'a SSTableReader<R>) -> Result<Self> {
        let compressed = match &sst.compression_info {
            Some(ci) => Some(ChunkedCompressedData::new(
                &sst.data,
                ci,
                &sst.decompressed_chunks,
            )?),
            None => None,
        };
        Ok(Self {
            sst,
            pos: 0,
            compressed,
        })
    }

    /// Yield the next partition in storage order. Returns `Ok(None)` when
    /// the iterator has reached EOF.
    /// Phase 1 of the row-streamed partition read: decode header
    /// (key + deletion + optional static row), park the iterator
    /// at the first clustered row. The follow-up call is
    /// [`Self::stream_clustered_rows`]. Together they let callers see
    /// the header **before** providing a row consumer — which is
    /// what the digest path needs (seed `PartitionDigestStream`
    /// with the header, then fold rows in).
    ///
    /// Returns `Ok(None)` at EOF.
    pub fn next_partition_header_only(
        &mut self,
    ) -> Result<
        Option<(
            ferrosa_common::DecoratedKey,
            crate::types::DeletionTime,
            Option<crate::types::Row>,
        )>,
    > {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.read_partition_header_only()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_header_only()?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// One-row-at-a-time companion to [`Self::stream_clustered_rows`].
    /// After [`Self::next_partition_header_only`] has parked the iter
    /// at the first clustered row, repeatedly call this to pull
    /// rows in storage order. Returns `Ok(None)` at
    /// end-of-partition; the iterator is then ready for another
    /// `next_partition_header_only`.
    ///
    /// Used by `TableStore::walk_token_range_for_digest`'s
    /// multi-source path: each source's iter is advanced one
    /// row at a time so the cross-source k-way merge by
    /// clustering key controls the pull rate, holding at most
    /// one row per source in flight at any moment.
    pub fn next_clustered_row(&mut self) -> Result<Option<crate::types::Row>> {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.read_next_clustered_row()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_next_clustered_row()?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Phase 2 of the row-streamed partition read: walk clustered
    /// rows until end-of-partition, invoking `on_row` once per row
    /// in storage order. Each row is decoded into a fresh `Row`,
    /// handed to the callback by reference, and dropped before
    /// the next is read.
    ///
    /// Must be preceded by [`Self::next_partition_header_only`] — calling
    /// it without phase 1 mis-aligns the data pointer.
    pub fn stream_clustered_rows<F>(&mut self, on_row: F) -> Result<()>
    where
        F: FnMut(&crate::types::Row) -> Result<()>,
    {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            reader.stream_clustered_rows(on_row)?;
            self.pos = reader.position();
            Ok(())
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            reader.stream_clustered_rows(on_row)?;
            self.pos = reader.position();
            Ok(())
        }
    }

    /// Yield the next partition's header (key, deletion, static
    /// row) and call `on_row` once per clustered row in storage
    /// order. Each row is decoded into a freshly-allocated `Row`,
    /// handed to the callback by reference, and dropped before the
    /// next row is read.
    ///
    /// Peak working set during the call is **one row** — used by
    /// anti-entropy repair to hash a multi-MB partition into a
    /// `PartitionDigestStream` without ever materialising a full
    /// `Partition` struct (which the bigger
    /// `next_partition`/`read_partition` paths do).
    pub fn next_partition_streaming<F>(
        &mut self,
        on_row: F,
    ) -> Result<
        Option<(
            ferrosa_common::DecoratedKey,
            crate::types::DeletionTime,
            Option<crate::types::Row>,
        )>,
    >
    where
        F: FnMut(&crate::types::Row) -> Result<()>,
    {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.read_partition_streaming(on_row)?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_streaming(on_row)?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    pub fn next_partition(&mut self) -> Result<Option<crate::types::Partition>> {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.read_partition()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition()?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Yield `(partition_key, row_count)` for the next partition without
    /// decoding any cell payloads. Cells are byte-skipped via
    /// `DataReader::read_partition_count`. Used by the COUNT(*) fast
    /// path so a full-table count never pays the per-cell decode cost.
    /// Returns `Ok(None)` at EOF.
    pub fn next_partition_count(
        &mut self,
    ) -> Result<Option<(ferrosa_common::key::DecoratedKey, u64)>> {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.read_partition_count()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_count()?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Yield the next partition decoding only the cells whose
    /// ordinals are in `wanted`. Cells outside the projection are
    /// byte-skipped via `DataReader::read_cell_skip` — saves one
    /// syscall, one heap alloc, and the value-byte memcpy per
    /// skipped cell. Used by the CQL projection fast path so a
    /// `SELECT a, b FROM t` on a wide table (especially with
    /// embedding columns) doesn't pay the read+decode cost for
    /// columns the caller doesn't want.
    ///
    /// An empty `wanted` slice yields rows with empty `cells` —
    /// useful when only clustering keys / metadata are needed
    /// (similar to `next_partition_metadata` but going through
    /// the per-cell skip path).
    pub fn next_partition_projected(
        &mut self,
        wanted: &[u16],
    ) -> Result<Option<crate::types::Partition>> {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.read_partition_projected(wanted)?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_projected(wanted)?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Advance past the partition currently at `self.pos` WITHOUT
    /// decoding it. Returns `Ok(())` when there is a next partition
    /// (pos is moved to its start), `Ok(())` AT EOF too (pos is
    /// moved to file_len so subsequent peek/next yield None).
    ///
    /// Uses the SSTable's cached `partition_offsets` for O(log N)
    /// lookup of the next partition's start offset. The cache is
    /// built lazily on first call (one-time O(file) walk via
    /// `next_partition_count`).
    ///
    /// Used by the merger's duplicate-key dedup path: when we've
    /// already popped the partition for key K from one source, the
    /// OTHER sources holding key K need to advance past it but their
    /// decoded body is discarded — `skip_to_next_partition` does the
    /// advance without paying that wasted decode cost.
    /// Position the iterator at the first partition whose token is
    /// `>= target`. If no such partition exists the iterator is
    /// parked at EOF (subsequent `peek/next` yield `None`).
    ///
    /// Uses the SSTable's cached `partition_token_offsets` for an
    /// O(log N) lookup. On the first call per SSTable the cache is
    /// populated by a single token-only walk (no row-body decode);
    /// every subsequent `seek_to_token` is O(log N).
    ///
    /// This is the anti-entropy repair hot-path primitive: each of
    /// repair's `num_tokens × peers` sessions asks for partitions in
    /// a tiny token sub-range out of a multi-GB SSTable. Without a
    /// seek the streaming iterator pays O(table_size) per session;
    /// with one, it pays O(matches_in_range).
    ///
    /// If the cache is empty (build failed), this is a no-op and the
    /// iterator stays at its current position — callers fall back to
    /// the existing linear `next_partition + token-filter` shape.
    pub fn seek_to_token(&mut self, target: i64) -> Result<()> {
        let tokens = self.sst.partition_token_offsets();
        if tokens.is_empty() {
            return Ok(());
        }
        // First entry with `token >= target`. partition_point splits
        // the slice on the first element NOT satisfying the
        // predicate; we want the first NOT `< target`.
        let idx = tokens.partition_point(|(t, _)| *t < target);
        self.pos = match tokens.get(idx) {
            Some(&(_, pos)) => pos,
            None => self.sst.data.len()?, // every token < target → EOF
        };
        Ok(())
    }

    pub fn skip_to_next_partition(&mut self) -> Result<()> {
        let offsets = self.sst.partition_offsets();
        if offsets.is_empty() {
            // Cache build failed; fall back to body-decode advance.
            // (We can't return error here without changing the API;
            // caller should use next_partition* as a fallback.)
            let _ = self.next_partition_metadata()?;
            return Ok(());
        }
        // Find the first offset strictly greater than self.pos.
        let next_idx = match offsets.binary_search(&self.pos) {
            Ok(i) => i + 1, // self.pos sits AT a partition start; advance to next
            Err(i) => i,    // self.pos is inside a partition; i is the next start
        };
        self.pos = match offsets.get(next_idx) {
            Some(&p) => p,
            None => self.sst.data.len()?, // past last partition → EOF
        };
        Ok(())
    }

    /// Peek the next partition's key WITHOUT advancing iteration
    /// state. The following `next_partition*` call yields the same
    /// partition (decoded), at the same `self.pos`.
    ///
    /// Used by the range merger to populate its priming heap with
    /// `(key, source_id)` pairs cheaply: on cold cache the full
    /// per-source partition-body decode is the dominant cost of
    /// any range scan with `LIMIT N` (especially small N), so
    /// deferring body decode until the merger actually pops that
    /// source collapses the cold-cache wall from
    /// `O(num_sources × body_decode)` to
    /// `O(num_sources × header_read) + O(N × body_decode)`.
    ///
    /// Returns `Ok(None)` at EOF.
    pub fn peek_partition_key(&mut self) -> Result<Option<ferrosa_common::key::DecoratedKey>> {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.peek_partition_key()?;
            // peek does not advance pos
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.peek_partition_key()?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Yield the next partition with full row metadata (clustering
    /// keys, row-level deletion, liveness) but **no cell payloads**
    /// — `Partition.rows[*].cells` is always empty. Used by the
    /// COUNT(*) fast path where the storage layer needs row-level
    /// dedup via `merge::merge_partitions` but doesn't need cell
    /// data. Returns `Ok(None)` at EOF.
    pub fn next_partition_metadata(&mut self) -> Result<Option<crate::types::Partition>> {
        let header = &self.sst.header;
        if let Some(ref data) = self.compressed {
            let mut reader = crate::data::DataReader::new(data, header, self.pos);
            let result = reader.read_partition_metadata()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_metadata()?;
            self.pos = reader.position();
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bloom::BloomFilter;
    use crate::statistics::{
        write_statistics, CompactionMetadata, SerializationHeader, Statistics, StatsMetadata,
        ValidationMetadata,
    };
    use crate::trie::builder::{TrieBuilder, TriePayload};
    use crate::{byte_comparable, varint};
    use ferrosa_common::PartitionKey;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// Write an unsigned varint to a buffer.
    fn push_unsigned_vint(out: &mut Vec<u8>, value: u64) {
        let mut buf = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut buf, value);
        out.extend_from_slice(&buf[..n]);
    }

    /// Build a test SerializationHeader with one regular column.
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

    /// Build a full Statistics.db blob from the given header.
    fn build_statistics(header: SerializationHeader) -> Vec<u8> {
        let stats = Statistics {
            validation: ValidationMetadata {
                partitioner_class: "org.apache.cassandra.dht.Murmur3Partitioner".into(),
                bloom_fp_chance: 0.01,
            },
            compaction: CompactionMetadata { data: vec![0x00] },
            stats: StatsMetadata { data: vec![0x00] },
            header,
        };
        write_statistics(&stats)
    }

    /// Row flags constants (mirrored from data.rs for test use).
    const HAS_TIMESTAMP: u8 = 0x04;
    const HAS_ALL_COLUMNS: u8 = 0x20;
    const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
    const END_OF_PARTITION: u8 = 0x01;
    const DELETION_IS_LIVE: u8 = 0x80;

    /// Build a Data.db blob for a single partition.
    ///
    /// Key is the raw partition key bytes. Produces one row with clustering
    /// key `[0,0,0,1]`, timestamp delta 42, and cell value `b"hello-0"`.
    fn build_data_blob(key: &[u8]) -> Vec<u8> {
        build_data_blob_with_rows(key, 1)
    }

    fn build_data_blob_with_rows(key: &[u8], rows: usize) -> Vec<u8> {
        let mut data = Vec::new();

        // Partition header: u16 BE key len + key bytes
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);

        // Live deletion time (Cassandra 5.x: single byte 0x80)
        data.push(DELETION_IS_LIVE);

        for row_idx in 0..rows {
            // Row flags: HAS_TIMESTAMP | HAS_ALL_COLUMNS
            data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

            // Clustering key (ClusteringPrefix format, Int32Type = fixed-length)
            let clustering = (row_idx as i32 + 1).to_be_bytes();
            push_unsigned_vint(&mut data, 0); // clustering header: all non-null, non-empty
            data.extend_from_slice(&clustering);

            let value = format!("hello-{row_idx}");
            let mut row_body = Vec::new();
            push_unsigned_vint(&mut row_body, 42 + row_idx as u64);
            row_body.push(CELL_USE_ROW_TIMESTAMP);
            push_unsigned_vint(&mut row_body, value.len() as u64);
            row_body.extend_from_slice(value.as_bytes());

            // Row body size + prev unfiltered size + body
            push_unsigned_vint(&mut data, row_body.len() as u64);
            push_unsigned_vint(&mut data, 0);
            data.extend_from_slice(&row_body);
        }

        // End of partition
        data.push(END_OF_PARTITION);

        data
    }

    fn build_legacy_header_only_partition(key: &[u8]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        data.push(DELETION_IS_LIVE);
        data
    }

    /// Build a Partitions.db file from entries.
    ///
    /// Each entry is `(DecoratedKey, data_position)`. The position is encoded
    /// as a negative idxpos (DataDirect) via bitwise NOT.
    fn build_partition_index(entries: &[(&DecoratedKey, u64)]) -> Vec<u8> {
        let mut encoded_entries: Vec<(Vec<u8>, u8, i64)> = entries
            .iter()
            .map(|(dk, pos)| {
                let encoded = byte_comparable::encode(dk);
                let (_h1, h2) = dk.filter_hash();
                let hash = (h2 & 0xFF) as u8;
                // Negative idxpos -> DataDirect (bitwise NOT of position)
                let idxpos = !(*pos as i64);
                (encoded, hash, idxpos)
            })
            .collect();
        encoded_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut builder = TrieBuilder::new();
        for (encoded, hash, idxpos) in &encoded_entries {
            builder
                .add(
                    encoded,
                    TriePayload {
                        hash: Some(*hash),
                        position: *idxpos,
                    },
                )
                .unwrap();
        }
        let (trie_data, root_pos) = builder.finish().unwrap();

        // Assemble: trie data + key bounds + footer
        let mut buf = Vec::new();
        buf.extend_from_slice(&trie_data);

        // Key bounds
        let key_bounds_offset = buf.len() as i64;
        // Use first and last partition keys as bounds
        let smallest = entries.first().map(|(dk, _)| dk.key.as_bytes()).unwrap();
        let largest = entries.last().map(|(dk, _)| dk.key.as_bytes()).unwrap();
        buf.extend_from_slice(&(smallest.len() as u16).to_be_bytes());
        buf.extend_from_slice(smallest);
        buf.extend_from_slice(&(largest.len() as u16).to_be_bytes());
        buf.extend_from_slice(largest);

        // Footer: key_bounds_offset, key_count, root_pos
        buf.extend_from_slice(&key_bounds_offset.to_be_bytes());
        buf.extend_from_slice(&(entries.len() as i64).to_be_bytes());
        buf.extend_from_slice(&(root_pos as i64).to_be_bytes());

        buf
    }

    /// Build a BloomFilter containing the given keys.
    fn build_bloom_filter(keys: &[&DecoratedKey]) -> Vec<u8> {
        let mut bf = BloomFilter::new(keys.len().max(10), 0.01);
        for dk in keys {
            let (h1, h2) = dk.filter_hash();
            bf.add(h1, h2);
        }
        bf.write()
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn open_and_read_single_partition() {
        let header = test_header();

        let dk = DecoratedKey::new(PartitionKey::from(b"pk1".as_slice()));

        // Build Data.db
        let data_bytes = build_data_blob(b"pk1");

        // Build Partitions.db pointing to position 0 in Data.db
        let partitions_bytes = build_partition_index(&[(&dk, 0)]);

        // Build Filter.db
        let filter_bytes = build_bloom_filter(&[&dk]);

        // Build Statistics.db
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();

        // Verify key count
        assert_eq!(reader.key_count(), 1);

        // Read the partition
        let partition = reader
            .get_partition(&dk)
            .unwrap()
            .expect("expected partition");

        assert_eq!(partition.key.key.as_bytes(), b"pk1");
        assert!(partition.deletion.is_live());
        assert_eq!(partition.rows.len(), 1);

        let row = &partition.rows[0];
        assert_eq!(row.clustering, vec![0x00, 0x00, 0x00, 0x01]);
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_042);
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"hello-0".as_slice()));
    }

    #[test]
    fn bloom_filter_rejects_absent_key() {
        let header = test_header();

        let dk = DecoratedKey::new(PartitionKey::from(b"pk1".as_slice()));
        let missing = DecoratedKey::new(PartitionKey::from(b"nonexistent".as_slice()));

        // Build Data.db with just dk
        let data_bytes = build_data_blob(b"pk1");

        // Build Partitions.db with just dk
        let partitions_bytes = build_partition_index(&[(&dk, 0)]);

        // Build Filter.db with ONLY dk (missing key not added)
        let filter_bytes = build_bloom_filter(&[&dk]);

        // Build Statistics.db
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();

        // The missing key should not be present in the bloom filter.
        // Due to the probabilistic nature, we verify via the bloom_filter
        // accessor directly.
        let (h1, h2) = missing.filter_hash();
        if !reader.bloom_filter().is_present(h1, h2) {
            // Bloom filter correctly rejects -- get_partition should return None
            let result = reader.get_partition(&missing).unwrap();
            assert!(result.is_none(), "expected None for bloom-rejected key");
        }
        // If bloom filter has a false positive, the partition index lookup
        // will return NotFound, which is also correct behavior.
        let result = reader.get_partition(&missing).unwrap();
        assert!(result.is_none(), "expected None for absent key");
    }

    #[test]
    fn read_partitions_limited_rows_skips_unretained_rows_and_continues() {
        let header = test_header();

        let dk1 = DecoratedKey::new(PartitionKey::from(b"k1".as_slice()));
        let dk2 = DecoratedKey::new(PartitionKey::from(b"k2".as_slice()));

        let mut data_bytes = Vec::new();
        let pos1 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob_with_rows(b"k1", 3));
        let pos2 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob_with_rows(b"k2", 2));

        let partitions_bytes = build_partition_index(&[(&dk1, pos1), (&dk2, pos2)]);
        let filter_bytes = build_bloom_filter(&[&dk1, &dk2]);
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();
        let partitions = reader.read_partitions_limited_rows(2, 1).unwrap();

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].key.key.as_bytes(), b"k1");
        assert_eq!(partitions[1].key.key.as_bytes(), b"k2");
        assert_eq!(partitions[0].rows.len(), 1);
        assert_eq!(partitions[1].rows.len(), 1);
        assert_eq!(partitions[0].rows[0].clustering, 1_i32.to_be_bytes());
        assert_eq!(partitions[1].rows[0].clustering, 1_i32.to_be_bytes());
    }

    #[test]
    fn get_partition_limited_rows_returns_prefix_without_full_partition() {
        let header = test_header();
        let dk = DecoratedKey::new(PartitionKey::from(b"wide".as_slice()));

        let data_bytes = build_data_blob_with_rows(b"wide", 5);
        let partitions_bytes = build_partition_index(&[(&dk, 0)]);
        let filter_bytes = build_bloom_filter(&[&dk]);
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();
        let partition = reader
            .get_partition_limited_rows(&dk, 2)
            .unwrap()
            .expect("wide partition should exist");

        assert_eq!(partition.rows.len(), 2);
        assert_eq!(partition.rows[0].clustering, 1_i32.to_be_bytes());
        assert_eq!(partition.rows[1].clustering, 2_i32.to_be_bytes());
    }

    #[derive(Clone)]
    struct CountingReadAt {
        data: Vec<u8>,
        max_read_end: std::sync::Arc<std::sync::atomic::AtomicU64>,
        bytes_read: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl CountingReadAt {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                max_read_end: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
                bytes_read: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }
        }

        fn max_read_end(&self) -> u64 {
            self.max_read_end.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn bytes_read(&self) -> u64 {
            self.bytes_read.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl crate::io::ReadAt for CountingReadAt {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            let n = self.data.read_at(buf, offset)?;
            self.bytes_read
                .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            let end = offset.saturating_add(n as u64);
            let mut current = self.max_read_end.load(std::sync::atomic::Ordering::Relaxed);
            while end > current {
                match self.max_read_end.compare_exchange_weak(
                    current,
                    end,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(next) => current = next,
                }
            }
            Ok(n)
        }

        fn len(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    #[test]
    fn get_partition_limited_rows_does_not_drain_wide_partition_tail() {
        let header = test_header();
        let dk = DecoratedKey::new(PartitionKey::from(b"wide".as_slice()));

        let data_bytes = build_data_blob_with_rows(b"wide", 1_000);
        let data_len = data_bytes.len() as u64;
        let data = CountingReadAt::new(data_bytes);
        let data_probe = data.clone();
        let partitions = CountingReadAt::new(build_partition_index(&[(&dk, 0)]));
        let rows = CountingReadAt::new(Vec::new());
        let filter = build_bloom_filter(&[&dk]);
        let stats = build_statistics(header);

        let components = SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info: None,
            statistics: stats,
        };

        let reader = SSTableReader::open(components).unwrap();
        let partition = reader
            .get_partition_limited_rows(&dk, 1)
            .unwrap()
            .expect("wide partition should exist");

        assert_eq!(partition.rows.len(), 1);
        assert!(
            data_probe.max_read_end() < data_len / 4,
            "point lookup LIMIT 1 must not walk the unreturned wide-partition tail; read_end={} data_len={}",
            data_probe.max_read_end(),
            data_len
        );
    }

    #[test]
    fn compressed_get_partition_limited_rows_reads_only_needed_chunks() {
        use crate::compression::Compression;
        use crate::types::{DeletionTime, LivenessInfo, Row};
        use crate::writer::{SSTableWriter, WriteOptions};
        use ferrosa_common::CellValue;

        for (name, compression) in [
            ("lz4", Compression::Lz4),
            ("zstd", Compression::Zstd { level: 3 }),
        ] {
            let header = test_header();
            let dk = DecoratedKey::new(PartitionKey::from(
                format!("wide-compressed-{name}").as_bytes(),
            ));
            let rows = (0_i32..2_000)
                .map(|idx| {
                    let timestamp = 1_000_000 + i64::from(idx);
                    Row {
                        clustering: (idx + 1).to_be_bytes().to_vec(),
                        cells: vec![(
                            0,
                            CellValue::live(
                                format!("compressed-value-{name}-{idx:04}").into_bytes(),
                                timestamp,
                            ),
                        )],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                    }
                })
                .collect();
            let partition = Partition {
                key: dk.clone(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows,
            };

            let mut writer = SSTableWriter::new(
                WriteOptions {
                    compression: Some(compression),
                    bloom_fp_chance: 0.01,
                    chunk_size: 512,
                    verify_output: false,
                },
                header,
            );
            writer.add_partition(&partition).unwrap();
            let output = writer.finish().unwrap();
            let compression_info = output.compression_info.clone().expect("compressed output");
            let ci = CompressionInfo::read(&compression_info).unwrap();
            assert!(
                ci.chunk_offsets.len() > 8,
                "{name} test must produce several compressed chunks"
            );

            let compressed_data_len = output.data.len() as u64;
            let data = CountingReadAt::new(output.data);
            let data_probe = data.clone();
            let components = SSTableComponents {
                data,
                partitions: CountingReadAt::new(output.partitions),
                rows: CountingReadAt::new(output.rows),
                filter: output.filter,
                compression_info: Some(compression_info),
                statistics: output.statistics,
            };

            let reader = SSTableReader::open(components).unwrap();
            let partition = reader
                .get_partition_limited_rows(&dk, 1)
                .unwrap()
                .expect("compressed partition should exist");

            assert_eq!(partition.rows.len(), 1);
            let bytes_after_first_read = data_probe.bytes_read();
            assert!(
                bytes_after_first_read < compressed_data_len / 4,
                "{name} LIMIT point read must not decompress the full Data.db; bytes_read={} compressed_len={}",
                bytes_after_first_read,
                compressed_data_len
            );

            let partition = reader
                .get_partition_limited_rows(&dk, 1)
                .unwrap()
                .expect("compressed partition should still exist");
            assert_eq!(partition.rows.len(), 1);
            assert_eq!(
                data_probe.bytes_read(),
                bytes_after_first_read,
                "{name} second read should hit the decompressed chunk cache"
            );
        }
    }

    #[test]
    fn get_clustering_row_uses_rows_index_for_wide_partition() {
        use crate::types::{DeletionTime, LivenessInfo, Row};
        use crate::writer::{SSTableWriter, WriteOptions};
        use ferrosa_common::CellValue;

        let header = test_header();
        let dk = DecoratedKey::new(PartitionKey::from(b"wide-indexed".as_slice()));
        let rows = (0_i32..1_000)
            .map(|idx| {
                let timestamp = 1_000_000 + i64::from(idx);
                Row {
                    clustering: (idx + 1).to_be_bytes().to_vec(),
                    cells: vec![(
                        0,
                        CellValue::live(format!("hello-{idx}").into_bytes(), timestamp),
                    )],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                }
            })
            .collect();
        let partition = Partition {
            key: dk.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        };

        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: None,
                bloom_fp_chance: 0.01,
                chunk_size: 65_536,
                verify_output: true,
            },
            header,
        );
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();
        assert!(
            !output.rows.is_empty(),
            "wide clustered partitions must write a Rows.db row index"
        );

        let data_len = output.data.len() as u64;
        let data = CountingReadAt::new(output.data);
        let data_probe = data.clone();
        let components = SSTableComponents {
            data,
            partitions: CountingReadAt::new(output.partitions),
            rows: CountingReadAt::new(output.rows),
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        };

        let reader = SSTableReader::open(components).unwrap();
        let target = 900_i32.to_be_bytes();
        let partition = reader
            .get_clustering_row(&dk, &target)
            .unwrap()
            .expect("target row should exist");

        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].clustering, target);
        assert!(
            data_probe.max_read_end() > data_len / 2,
            "test should request a row deep in the partition; read_end={} data_len={}",
            data_probe.max_read_end(),
            data_len
        );
        assert!(
            data_probe.bytes_read() < data_len / 8,
            "row-indexed exact lookup must not scan the wide partition; bytes_read={} data_len={}",
            data_probe.bytes_read(),
            data_len
        );
    }

    #[test]
    fn compressed_get_clustering_row_uses_rows_index() {
        use crate::compression::Compression;
        use crate::types::{DeletionTime, LivenessInfo, Row};
        use crate::writer::{SSTableWriter, WriteOptions};
        use ferrosa_common::CellValue;

        let header = test_header();
        let dk = DecoratedKey::new(PartitionKey::from(b"wide-compressed-indexed".as_slice()));
        let rows = (0_i32..2_000)
            .map(|idx| {
                let timestamp = 1_000_000 + i64::from(idx);
                Row {
                    clustering: (idx + 1).to_be_bytes().to_vec(),
                    cells: vec![(
                        0,
                        CellValue::live(
                            format!("indexed-compressed-value-{idx:04}").into_bytes(),
                            timestamp,
                        ),
                    )],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                }
            })
            .collect();
        let partition = Partition {
            key: dk.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        };

        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: Some(Compression::Lz4),
                bloom_fp_chance: 0.01,
                chunk_size: 512,
                verify_output: false,
            },
            header,
        );
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();
        assert!(
            !output.rows.is_empty(),
            "wide clustered partitions must write a Rows.db row index"
        );
        let compression_info = output.compression_info.clone().expect("compressed output");
        let ci = CompressionInfo::read(&compression_info).unwrap();
        assert!(
            ci.chunk_offsets.len() > 8,
            "test must produce several compressed chunks"
        );

        let compressed_data_len = output.data.len() as u64;
        let data = CountingReadAt::new(output.data);
        let data_probe = data.clone();
        let components = SSTableComponents {
            data,
            partitions: CountingReadAt::new(output.partitions),
            rows: CountingReadAt::new(output.rows),
            filter: output.filter,
            compression_info: Some(compression_info),
            statistics: output.statistics,
        };

        let reader = SSTableReader::open(components).unwrap();
        let target = 1_800_i32.to_be_bytes();
        let partition = reader
            .get_clustering_row(&dk, &target)
            .unwrap()
            .expect("target row should exist");

        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].clustering, target);
        assert!(
            data_probe.bytes_read() < compressed_data_len / 4,
            "compressed exact-row lookup must not decompress the full Data.db; bytes_read={} compressed_len={}",
            data_probe.bytes_read(),
            compressed_data_len
        );
    }

    #[test]
    fn get_partition_limited_rows_uses_data_position_from_rows_index() {
        use crate::types::{DeletionTime, LivenessInfo, Row};
        use crate::writer::{SSTableWriter, WriteOptions};
        use ferrosa_common::CellValue;

        let header = test_header();
        let dk = DecoratedKey::new(PartitionKey::from(b"wide-indexed".as_slice()));
        let rows = (0_i32..64)
            .map(|idx| {
                let timestamp = 1_000_000 + i64::from(idx);
                Row {
                    clustering: (idx + 1).to_be_bytes().to_vec(),
                    cells: vec![(
                        0,
                        CellValue::live(format!("hello-{idx}").into_bytes(), timestamp),
                    )],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                }
            })
            .collect();
        let partition = Partition {
            key: dk.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        };

        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: None,
                bloom_fp_chance: 0.01,
                chunk_size: 65_536,
                verify_output: true,
            },
            header,
        );
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();
        assert!(
            !output.rows.is_empty(),
            "test must exercise a row-indexed partition"
        );

        let components = SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        };

        let reader = SSTableReader::open(components).unwrap();
        let partition = reader
            .get_partition_limited_rows(&dk, 5)
            .unwrap()
            .expect("row-indexed partition should be found");

        assert_eq!(partition.rows.len(), 5);
        assert_eq!(partition.rows[0].clustering, 1_i32.to_be_bytes());
        assert_eq!(partition.rows[4].clustering, 5_i32.to_be_bytes());
    }

    #[test]
    fn key_count_is_correct() {
        let header = test_header();

        let dk1 = DecoratedKey::new(PartitionKey::from(b"k1".as_slice()));
        let dk2 = DecoratedKey::new(PartitionKey::from(b"k2".as_slice()));
        let dk3 = DecoratedKey::new(PartitionKey::from(b"k3".as_slice()));

        // Build Data.db with three partitions concatenated
        let mut data_bytes = Vec::new();
        let pos1 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob(b"k1"));
        let pos2 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob(b"k2"));
        let pos3 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob(b"k3"));

        // Build Partitions.db
        let partitions_bytes = build_partition_index(&[(&dk1, pos1), (&dk2, pos2), (&dk3, pos3)]);

        // Build Filter.db
        let filter_bytes = build_bloom_filter(&[&dk1, &dk2, &dk3]);

        // Build Statistics.db
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();
        assert_eq!(reader.key_count(), 3);

        // Verify header accessor
        assert_eq!(
            reader.header().key_type,
            "org.apache.cassandra.db.marshal.UTF8Type"
        );

        // Verify each partition is readable
        for (dk, key_bytes) in [(&dk1, b"k1"), (&dk2, b"k2"), (&dk3, b"k3")] {
            let partition = reader
                .get_partition(dk)
                .unwrap()
                .expect("expected partition");
            assert_eq!(partition.key.key.as_bytes(), key_bytes.as_slice());
            assert_eq!(partition.rows.len(), 1);
        }

        // Verify compression_info is None
        assert!(reader.compression_info().is_none());
    }

    /// Parity: streaming `partitions_iter()` yields the same sequence as
    /// the materializing `read_all_partitions()`.  This is the regression
    /// guard for the streaming-compaction refactor.
    #[test]
    fn partitions_iter_matches_read_all_partitions() {
        let header = test_header();
        let dks: Vec<_> = (0..7u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        let materialized = reader.read_all_partitions().expect("read_all");
        let mut streamed = Vec::new();
        let mut iter = reader.partitions_iter().expect("partitions_iter");
        while let Some(p) = iter.next_partition().expect("next") {
            streamed.push(p);
        }

        assert_eq!(materialized.len(), streamed.len(), "same partition count");
        for (m, s) in materialized.iter().zip(streamed.iter()) {
            assert_eq!(m.key, s.key);
            assert_eq!(m.rows.len(), s.rows.len());
        }
        // EOF stays at EOF
        assert!(iter.next_partition().unwrap().is_none());
        assert!(iter.next_partition().unwrap().is_none());
    }

    /// `seek_to_token(T)` must land the iterator at the first
    /// partition with token >= T, regardless of how many partitions
    /// come before it in the SSTable. This is what makes
    /// anti-entropy repair viable on a multi-GB table without
    /// linearly scanning all partitions for every (range, peer)
    /// session — repair runs O(#sessions × matches_per_range)
    /// instead of O(#sessions × table_size).
    #[test]
    fn seek_to_token_starts_at_or_after_target() {
        let header = test_header();
        let n = 20usize;
        let mut dks: Vec<_> = (0..n)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:03}").as_bytes())))
            .collect();
        dks.sort_by_key(|dk| dk.token.0);

        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Seek to the token of the 10th partition. After seek, the
        // iterator's NEXT decoded partition must be that one — every
        // partition before it must have been skipped without decode.
        let target = dks[n / 2].token.0;
        let mut iter = reader.partitions_iter().expect("partitions_iter");
        iter.seek_to_token(target).expect("seek_to_token");
        let first = iter
            .next_partition()
            .expect("next after seek")
            .expect("a partition exists at or after target");
        assert_eq!(
            first.key.token.0, target,
            "first decoded partition's token must equal the seek target"
        );
        // Note: `first.key` is a DecoratedKey; its `token` field name matches
        // the Partition struct convention.

        // After exhausting the rest, exactly half (10) partitions
        // should remain.
        let mut yielded_after_seek = 1usize;
        while iter.next_partition().expect("next").is_some() {
            yielded_after_seek += 1;
        }
        assert_eq!(yielded_after_seek, n - n / 2);

        // Seeking to a token greater than every partition's token
        // must put the iterator at EOF.
        let above_max = dks.last().unwrap().token.0.saturating_add(1);
        let mut iter = reader.partitions_iter().expect("partitions_iter");
        iter.seek_to_token(above_max).expect("seek above max");
        assert!(iter.next_partition().expect("next").is_none());
    }

    /// After `next_partition_header_only` parks the iterator at
    /// the first clustered row, `next_clustered_row` yields rows
    /// one at a time, returning `Ok(None)` at end-of-partition.
    /// Companion to `stream_clustered_rows` for callers that need
    /// fine-grained control over advancement — specifically the
    /// cross-source row merge in
    /// `TableStore::walk_token_range_for_digest`'s multi-source
    /// path, which pulls one row from each source's iterator and
    /// k-way merges by clustering key.
    ///
    /// Calling this on a `PartitionIter` not parked inside a
    /// partition's row section is undefined.
    #[test]
    fn next_clustered_row_yields_rows_one_at_a_time() {
        let header = test_header();
        let dk = DecoratedKey::new(PartitionKey::from(b"ncr_key".as_slice()));
        let data_bytes = build_data_blob_with_rows(dk.key.as_bytes(), 5);
        let partitions_bytes = build_partition_index(&[(&dk, 0)]);
        let filter_bytes = build_bloom_filter(&[&dk]);
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        let mut iter = reader.partitions_iter().unwrap();
        let (key, _deletion, static_row) = iter.next_partition_header_only().unwrap().unwrap();
        assert_eq!(key, dk);
        assert!(static_row.is_none());

        let mut got = Vec::new();
        while let Some(row) = iter.next_clustered_row().unwrap() {
            got.push(row);
        }
        assert_eq!(got.len(), 5, "should yield exactly the 5 clustered rows");
        // After exhaust, repeated calls should keep returning None
        // — and the iterator should be ready for the next
        // partition (or EOF).
        assert!(iter.next_clustered_row().unwrap().is_none());
    }

    /// `next_partition_header_only` reads the partition header
    /// (key, deletion, static row) and leaves the iterator parked
    /// at the first **clustered** row. `stream_clustered_rows` is
    /// the continuation that walks those rows one at a time. The
    /// two together let a caller process the header (e.g. seed a
    /// `PartitionDigestStream`) BEFORE the rows arrive — which is
    /// what `next_partition_streaming`'s one-shot
    /// `(header, on_row)` API doesn't support since the header
    /// can only be returned after the row callback has consumed
    /// everything.
    ///
    /// Calling `stream_clustered_rows` on a fresh `PartitionIter`
    /// (without a preceding `next_partition_header_only`) is
    /// undefined; the iterator must be parked at the start of a
    /// row sequence inside a partition.
    #[test]
    fn next_partition_header_only_then_stream_clustered_rows_matches_legacy() {
        let header = test_header();
        let dks: Vec<_> = (0..3u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("hsk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob_with_rows(dk.key.as_bytes(), 3));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Baseline via `next_partition`.
        let mut baseline = Vec::new();
        let mut iter = reader.partitions_iter().unwrap();
        while let Some(p) = iter.next_partition().unwrap() {
            baseline.push(p);
        }

        // 2-phase streaming.
        let mut streamed = Vec::new();
        let mut iter = reader.partitions_iter().unwrap();
        while let Some((key, deletion, static_row)) = iter.next_partition_header_only().unwrap() {
            let mut rows = Vec::new();
            iter.stream_clustered_rows(|row| {
                rows.push(row.clone());
                Ok(())
            })
            .unwrap();
            streamed.push((key, deletion, static_row, rows));
        }

        assert_eq!(baseline.len(), streamed.len());
        for (p, (key, deletion, static_row, rows)) in baseline.iter().zip(streamed.iter()) {
            assert_eq!(p.key, *key);
            assert_eq!(p.deletion, *deletion);
            assert_eq!(p.static_row, *static_row);
            assert_eq!(p.rows.len(), rows.len());
            for (a, b) in p.rows.iter().zip(rows.iter()) {
                assert_eq!(a, b);
            }
        }
    }

    /// `next_partition_streaming` yields the same partition
    /// header and same sequence of clustered rows as
    /// `next_partition`. This is the read-side primitive used by
    /// anti-entropy repair to hash a multi-MB partition without
    /// ever holding a full `Partition` in memory: peak working
    /// set during the call is one row at a time.
    #[test]
    fn next_partition_streaming_matches_next_partition() {
        let header = test_header();
        let dks: Vec<_> = (0..3u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("psk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            // build_data_blob_with_rows ensures multiple clustered
            // rows so the streaming callback fires more than once.
            data_bytes.extend_from_slice(&build_data_blob_with_rows(dk.key.as_bytes(), 4));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Baseline: collect via `next_partition`.
        let mut materialised = Vec::new();
        let mut iter = reader.partitions_iter().unwrap();
        while let Some(p) = iter.next_partition().unwrap() {
            materialised.push(p);
        }

        // Streaming: collect via `next_partition_streaming`.
        let mut streamed_headers = Vec::new();
        let mut streamed_rows: Vec<Vec<crate::types::Row>> = Vec::new();
        let mut iter = reader.partitions_iter().unwrap();
        loop {
            let mut my_rows = Vec::new();
            let result = iter
                .next_partition_streaming(|row| {
                    my_rows.push(row.clone());
                    Ok(())
                })
                .unwrap();
            match result {
                Some((key, deletion, static_row)) => {
                    streamed_headers.push((key, deletion, static_row));
                    streamed_rows.push(my_rows);
                }
                None => break,
            }
        }

        assert_eq!(materialised.len(), streamed_headers.len());
        for ((p, (key, deletion, static_row)), rows) in materialised
            .iter()
            .zip(streamed_headers.iter())
            .zip(streamed_rows.iter())
        {
            assert_eq!(p.key, *key);
            assert_eq!(p.deletion, *deletion);
            assert_eq!(p.static_row, *static_row);
            assert_eq!(p.rows.len(), rows.len());
            for (a, b) in p.rows.iter().zip(rows.iter()) {
                assert_eq!(a, b);
            }
        }
    }

    /// ADR-020 COUNT(*) fast path: next_partition_count yields the
    /// same partition keys as next_partition, with row_count
    /// matching `partition.rows.len()`. Crucially does NOT decode
    /// any cell payloads — `read_partition_count` advances by
    /// byte-skipping via `skip_row_body`.
    #[test]
    fn next_partition_count_matches_partition_rows_len() {
        let header = test_header();
        let dks: Vec<_> = (0..5u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Reference: full partition iteration with row counts.
        let mut iter_full = reader.partitions_iter().expect("partitions_iter");
        let mut expected: Vec<(_, u64)> = Vec::new();
        while let Some(p) = iter_full.next_partition().expect("next") {
            expected.push((p.key, p.rows.len() as u64));
        }

        // Under test: counts-only iteration.
        let mut iter_counts = reader.partitions_iter().expect("partitions_iter (counts)");
        let mut got: Vec<(_, u64)> = Vec::new();
        while let Some(pc) = iter_counts.next_partition_count().expect("next_count") {
            got.push(pc);
        }

        assert_eq!(got, expected, "count iterator must match full iter");
        // EOF stable.
        assert!(iter_counts.next_partition_count().unwrap().is_none());
        assert!(iter_counts.next_partition_count().unwrap().is_none());
    }

    /// `skip_to_next_partition` advances `pos` past the current
    /// partition WITHOUT decoding it. Combined with `peek_partition_key`
    /// it lets the merger advance duplicate-key sources without
    /// paying any cell decode cost — the cold-cache dominant cost
    /// (see bug-streaming-range-read-perf-50x-floor).
    ///
    /// Verifies:
    ///  - skip advances to the NEXT partition's key (matches what
    ///    a non-skip iteration would yield next).
    ///  - skip-then-skip-...-then-skip eventually reaches EOF cleanly.
    ///  - alternating peek+skip yields the same key sequence as
    ///    pure next_partition iteration.
    #[test]
    fn skip_to_next_partition_advances_without_decode() {
        let header = test_header();
        let dks: Vec<_> = (0..5u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Reference iteration: full next_partition over every partition.
        let mut iter_ref = reader.partitions_iter().unwrap();
        let mut ref_keys = Vec::new();
        while let Some(p) = iter_ref.next_partition().unwrap() {
            ref_keys.push(p.key);
        }
        assert_eq!(ref_keys, dks, "sanity: reference iteration yields all keys");

        // Skip-only iteration: peek+skip the whole file. Must yield
        // the same key sequence (peek before each skip).
        let mut iter = reader.partitions_iter().unwrap();
        let mut skip_keys = Vec::new();
        loop {
            let peeked = iter.peek_partition_key().unwrap();
            let Some(k) = peeked else { break };
            skip_keys.push(k);
            iter.skip_to_next_partition().unwrap();
        }
        assert_eq!(
            skip_keys, dks,
            "peek+skip yields the same key sequence as full iteration"
        );

        // EOF stable: extra peek + skip yields None / no-op.
        assert!(
            iter.peek_partition_key().unwrap().is_none(),
            "peek at EOF returns None"
        );
        iter.skip_to_next_partition()
            .expect("skip at EOF must be a no-op");
        assert!(
            iter.peek_partition_key().unwrap().is_none(),
            "peek at EOF stays None after redundant skip"
        );
    }

    /// `peek_partition_key` returns the same key as the subsequent
    /// `next_partition*` call WITHOUT advancing iterator state.
    /// Critical for the range-merger cold-cache fast-path: priming
    /// the heap with peeked keys is `O(num_sources × header_read)`,
    /// then bodies are decoded only on pop (`O(emitted)`).
    /// Verifies: peek does not advance, repeated peek is idempotent,
    /// peek-then-next yields the same key, and EOF is stable.
    #[test]
    fn peek_partition_key_does_not_advance_iterator() {
        let header = test_header();
        let dks: Vec<_> = (0..4u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        let mut iter = reader.partitions_iter().expect("partitions_iter");
        for expected in &dks {
            let pos_before = iter.pos;
            let peeked1 = iter
                .peek_partition_key()
                .expect("peek1")
                .expect("peek1 some");
            assert_eq!(&peeked1, expected, "peek yields next key");
            assert_eq!(iter.pos, pos_before, "peek must not advance pos");

            // Idempotent: a second peek returns the same key without advancing.
            let peeked2 = iter
                .peek_partition_key()
                .expect("peek2")
                .expect("peek2 some");
            assert_eq!(peeked2, peeked1, "repeated peek is stable");
            assert_eq!(iter.pos, pos_before, "repeated peek must not advance");

            // The full decode now consumes this partition and advances.
            let partition = iter.next_partition().expect("next").expect("partition");
            assert_eq!(&partition.key, expected, "next_partition yields peeked key");
            assert!(iter.pos > pos_before, "next_partition must advance pos");
        }
        // EOF: peek and next both yield None, repeatedly.
        assert!(iter.peek_partition_key().unwrap().is_none(), "peek at EOF");
        assert!(
            iter.peek_partition_key().unwrap().is_none(),
            "peek EOF stable"
        );
        assert!(iter.next_partition().unwrap().is_none(), "next at EOF");
    }

    /// ADR-020 fast COUNT(*) metadata path: next_partition_metadata
    /// yields partitions with the same key + same row count + same
    /// per-row clustering keys as the full path, but with empty
    /// `cells`. Verifies the body-end skip arithmetic stays aligned
    /// across rows.
    #[test]
    fn next_partition_metadata_matches_keys_drops_cells() {
        let header = test_header();
        let dks: Vec<_> = (0..5u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        let mut iter_full = reader.partitions_iter().expect("partitions_iter (full)");
        let mut full = Vec::new();
        while let Some(p) = iter_full.next_partition().expect("next full") {
            full.push(p);
        }

        let mut iter_meta = reader.partitions_iter().expect("partitions_iter (meta)");
        let mut meta = Vec::new();
        while let Some(p) = iter_meta.next_partition_metadata().expect("next meta") {
            meta.push(p);
        }

        assert_eq!(meta.len(), full.len(), "same partition count");
        for (f, m) in full.iter().zip(meta.iter()) {
            assert_eq!(m.key, f.key, "partition key matches");
            assert_eq!(m.rows.len(), f.rows.len(), "row count matches");
            for (fr, mr) in f.rows.iter().zip(m.rows.iter()) {
                assert_eq!(mr.clustering, fr.clustering, "clustering matches");
                assert!(
                    mr.cells.is_empty(),
                    "metadata path must NOT decode cells (got {} cells)",
                    mr.cells.len()
                );
            }
        }
        // EOF stable.
        assert!(iter_meta.next_partition_metadata().unwrap().is_none());
    }

    /// ADR-020 projection-aware decode: `next_partition_projected`
    /// returns the same partition keys + same row count + same
    /// clustering keys as the full path, but `cells` contains only
    /// the cells the caller named in `wanted`. Cells outside the
    /// projection are byte-skipped via `read_cell_skip`.
    #[test]
    fn next_partition_projected_filters_cells_to_wanted_set() {
        let header = test_header();
        let dks: Vec<_> = (0..3u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Reference: full partition iteration.
        let mut iter_full = reader.partitions_iter().expect("partitions_iter (full)");
        let mut full = Vec::new();
        while let Some(p) = iter_full.next_partition().expect("next full") {
            full.push(p);
        }

        // Projection: wanted = {0} → only column 0's cell per row.
        let wanted: Vec<u16> = vec![0];
        let mut iter_proj = reader.partitions_iter().expect("partitions_iter (proj)");
        let mut proj = Vec::new();
        while let Some(p) = iter_proj
            .next_partition_projected(&wanted)
            .expect("next projected")
        {
            proj.push(p);
        }

        assert_eq!(proj.len(), full.len(), "same partition count");
        for (f, p) in full.iter().zip(proj.iter()) {
            assert_eq!(p.key, f.key, "key matches");
            assert_eq!(p.rows.len(), f.rows.len(), "row count matches");
            for (fr, pr) in f.rows.iter().zip(p.rows.iter()) {
                assert_eq!(pr.clustering, fr.clustering, "clustering matches");
                // Projected: only column 0 cells should remain.
                let proj_col_ids: Vec<u16> = pr.cells.iter().map(|(c, _)| *c).collect();
                assert!(
                    proj_col_ids.iter().all(|c| wanted.contains(c)),
                    "projected row only has wanted cells: {proj_col_ids:?}"
                );
                // Full had column 0; ensure projection didn't drop it.
                let full_has_col0 = fr.cells.iter().any(|(c, _)| *c == 0);
                let proj_has_col0 = pr.cells.iter().any(|(c, _)| *c == 0);
                assert_eq!(proj_has_col0, full_has_col0, "column 0 presence preserved");
            }
        }

        // Empty projection = no cells.
        let mut iter_empty = reader.partitions_iter().expect("partitions_iter (empty)");
        if let Some(p) = iter_empty
            .next_partition_projected(&[])
            .expect("next empty projection")
        {
            for r in &p.rows {
                assert!(r.cells.is_empty(), "empty projection leaves cells empty");
            }
        }
    }

    #[test]
    fn next_partition_projected_accepts_legacy_header_only_partition_at_eof() {
        let header = test_header();
        let dk = DecoratedKey::new(PartitionKey::from(b"pk-header-only".as_slice()));
        let data_bytes = build_legacy_header_only_partition(b"pk-header-only");
        let partitions_bytes = build_partition_index(&[(&dk, 0)]);
        let filter_bytes = build_bloom_filter(&[&dk]);
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();
        let mut iter = reader.partitions_iter().unwrap();

        let partition = iter
            .next_partition_projected(&[])
            .expect("header-only legacy partition should not error")
            .expect("partition should be returned");

        assert_eq!(partition.key.key.as_bytes(), b"pk-header-only");
        assert!(partition.rows.is_empty());
        assert!(partition.static_row.is_none());
        assert!(iter.next_partition_projected(&[]).unwrap().is_none());
    }
}
