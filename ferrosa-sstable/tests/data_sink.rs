//! T-038: `DataSink`/`StreamSink`/`ChunkCompressor` — the writer streams
//! through the aligned pump, no more `Data.raw`.
//!
//! These are black-box tests against the public `SSTableWriter` API (there is
//! no white-box seam into `DataSink` et al. — they are crate-private, and the
//! existing test style in this crate already drives the writer end to end and
//! reads components back, e.g. `tests/oracle.rs`, `tests/cassandra_compat.rs`).
//! `test-specification.md` L1 (U1-U12), the `CompressionInfo` header-patch
//! cases, and P2/P3 are covered here; P1/P4-ish byte-identity is the
//! existing `oracle_*` suite (extended by T-038's golden-manifest commit to
//! include `Digest.crc32`/`CRC.db`); RE5 and the allocation counts are in
//! `tests/data_sink_alloc.rs`.

mod support;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::compression::{Compression, CompressionInfo};
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableOutputFiles, SSTableWriter, WriteOptions};

use support::generators::{ColumnKind, Schema};

/// A schema with one variable-length regular column ("Bytes"), no
/// clustering, no static columns — the minimal shape a single big value can
/// be hung on.
fn single_value_schema() -> Schema {
    Schema {
        clustering: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnKind::Bytes],
        complex_collections: false,
        min_timestamp: 0,
    }
}

/// One partition, one row, one value of exactly `value_len` bytes.
fn one_big_value_partition(key: &[u8], value_len: usize, fill: u8) -> Partition {
    Partition {
        key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(vec![fill; value_len], 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }],
    }
}

/// Writes a single one-big-value partition, adjusting the value length until
/// the resulting Data.db `data_length` (uncompressed) is exactly
/// `n_chunks * chunk_size` — needed to hit exact chunk-count boundaries (U2,
/// U9, the header-patch test).
///
/// This converges by measurement rather than by subtracting a hand-derived
/// "framing overhead" constant: the row-body size vints and the cell's own
/// value-length vint are variable-width (Cassandra's unsigned-vint scheme
/// widens by a byte at size thresholds), so a single constant measured at one
/// value length does not hold at another — confirmed the hard way (an
/// earlier version of this file measured overhead once at `value_len=1` and
/// was correct for small `n_chunks*chunk_size` targets but silently one byte
/// short at `n_chunks=511/512`, where the value length crosses a vint-width
/// boundary the small measurement never reached). The framing is monotonic
/// and changes slowly relative to `value_len`, so this converges in at most
/// a couple of iterations; the loop bound (Power-of-10 rule 2) turns "would
/// have looped forever" into a clear panic instead.
fn one_partition_with_exact_data_length(
    options: &WriteOptions,
    key: &[u8],
    n_chunks: usize,
    chunk_size: usize,
    fill: u8,
) -> (tempfile::TempDir, WrittenFiles) {
    let target = n_chunks * chunk_size;
    let header = single_value_schema().header();
    let mut value_len = target;
    const MAX_ITERATIONS: usize = 8;
    for attempt in 0..MAX_ITERATIONS {
        let partition = one_big_value_partition(key, value_len, fill);
        let (dir, written) = write_file_backed(options.clone(), header.clone(), &[partition]);
        let actual = if written.files.compression_info.is_some() {
            read_compression_info(&written).data_length as usize
        } else {
            written.files.data_len as usize
        };
        if actual == target {
            return (dir, written);
        }
        assert!(
            attempt + 1 < MAX_ITERATIONS,
            "one_partition_with_exact_data_length: did not converge on target={target} \
             (chunk_size={chunk_size}, n_chunks={n_chunks}) after {MAX_ITERATIONS} attempts; \
             last value_len={value_len} produced data_length={actual}"
        );
        let diff = actual as i64 - target as i64;
        value_len = (value_len as i64 - diff).max(0) as usize;
    }
    unreachable!("loop always returns or asserts before exhausting MAX_ITERATIONS");
}

/// A schema with one Int32 clustering column and one Bytes regular column —
/// enough clustering columns for `row_index` to build once
/// `ROW_INDEX_MIN_ROWS` (32) rows are written (U11).
fn clustered_schema() -> Schema {
    Schema {
        clustering: vec![ColumnKind::Int32],
        static_columns: vec![],
        regular_columns: vec![ColumnKind::Bytes],
        complex_collections: false,
        min_timestamp: 0,
    }
}

/// `n` rows of `row_value_len` bytes each, clustering `0..n`, in one
/// partition — used to land many small `write_all` calls (one row's worth
/// each) across chunk/segment boundaries (U2, U4, U5, U9, U11), unlike
/// `one_big_value_partition`'s single wide write (U3, U6).
fn many_rows_partition(
    key: &[u8],
    n: i32,
    row_value_len: usize,
    fill_from_index: bool,
) -> Partition {
    let rows = (0..n)
        .map(|i| Row {
            clustering: i.to_be_bytes().to_vec(),
            cells: vec![(
                0,
                CellValue::live(
                    vec![
                        if fill_from_index {
                            (i % 251) as u8
                        } else {
                            0u8
                        };
                        row_value_len
                    ],
                    (i as i64) + 1,
                ),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp((i as i64) + 1),
        })
        .collect();
    Partition {
        key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows,
    }
}

struct WrittenFiles {
    files: SSTableOutputFiles,
}

/// Drives the file-backed writer path end to end: `new_file_backed`,
/// `add_partition` for each partition, `finish_to_directory`. Returns the
/// still-on-disk `SSTableOutputFiles` (caller decides whether to read them
/// into memory or open a `SSTableReader` directly over the paths) plus the
/// header, so callers can inspect `CompressionInfo.db`/`Digest.crc32`/
/// `CRC.db` bytes as well as read the table back.
fn write_file_backed(
    options: WriteOptions,
    header: SerializationHeader,
    partitions: &[Partition],
) -> (tempfile::TempDir, WrittenFiles) {
    let dir = tempfile::tempdir().expect("tempdir");
    let staging_dir = dir.path().join("staging");
    let raw_data_path = staging_dir.join("Data.raw"); // T-038: filename ignored, parent is the staging dir
    let mut writer = SSTableWriter::new_file_backed(options, header.clone(), raw_data_path)
        .expect("new_file_backed");
    for partition in partitions {
        writer.add_partition(partition).expect("add_partition");
    }
    let files = writer
        .finish_to_directory(&staging_dir)
        .expect("finish_to_directory");
    (dir, WrittenFiles { files })
}

fn open_reader(written: &WrittenFiles) -> SSTableReader<Vec<u8>> {
    let components = SSTableComponents {
        data: std::fs::read(&written.files.data).expect("read Data.db"),
        partitions: std::fs::read(&written.files.partitions).expect("read Partitions.db"),
        rows: std::fs::read(&written.files.rows).expect("read Rows.db"),
        filter: std::fs::read(&written.files.filter).expect("read Filter.db"),
        compression_info: written
            .files
            .compression_info
            .as_ref()
            .map(|p| std::fs::read(p).expect("read CompressionInfo.db")),
        statistics: std::fs::read(&written.files.statistics).expect("read Statistics.db"),
    };
    let mut reader = SSTableReader::open(components).expect("SSTableReader::open");
    // `SSTableComponents` doesn't carry Digest.crc32/CRC.db (they're loaded
    // separately — `load_digest`/`load_crc_table`); without this,
    // `verify_digest()` would silently no-op (logged once) rather than
    // actually check anything, which would make `assert_digest_verifies`
    // below a no-op assertion instead of a real one.
    let digest_bytes = std::fs::read(&written.files.digest).expect("read Digest.crc32");
    reader.load_digest(&digest_bytes).expect("load_digest");
    if let Some(crc_path) = written.files.crc.as_ref() {
        let crc_bytes = std::fs::read(crc_path).expect("read CRC.db");
        reader.load_crc_table(&crc_bytes).expect("load_crc_table");
    }
    reader
}

fn read_compression_info(written: &WrittenFiles) -> CompressionInfo {
    let path = written
        .files
        .compression_info
        .as_ref()
        .expect("case must be compressed");
    let bytes = std::fs::read(path).expect("read CompressionInfo.db");
    CompressionInfo::read(&bytes).expect("CompressionInfo::read")
}

/// Every case in this file's Data.db must verify: `Digest.crc32` matches the
/// on-disk bytes, exactly as a real publication verify step would check
/// (D6). Failing this anywhere in this file is evidence the pump/compressor
/// rewrite produced Data.db bytes its own checksum doesn't agree with.
fn assert_digest_verifies(reader: &SSTableReader<Vec<u8>>) {
    assert!(
        reader.digest_loaded(),
        "test bug: Digest.crc32 was not loaded, so verify_digest() below would silently no-op"
    );
    reader
        .verify_digest()
        .expect("Digest.crc32 must verify against the Data.db bytes the writer just produced");
}

/// `CompressionInfo` invariants that must hold regardless of how many chunks
/// or how the compressor batched them (L2 P2): offsets start at 0 and are
/// strictly increasing by `stored_size`, the last offset plus its stored size
/// equals Data.db's length, `chunk_count == ceil(data_length / chunk_length)`
/// (except for zero data, one empty chunk emitted for nothing — see
/// `data_sink_u10_empty_table_compressed`), and no stored size exceeds
/// `max_compressed_size`.
fn assert_compression_info_invariants(info: &CompressionInfo, data_db_len: u64) {
    if info.data_length == 0 {
        assert_eq!(
            info.chunk_offsets.len(),
            0,
            "empty input must produce zero chunks"
        );
        return;
    }
    assert_eq!(info.chunk_offsets[0], 0, "first chunk offset must be 0");
    assert_eq!(
        info.chunk_offsets.len() as u64,
        info.data_length.div_ceil(info.chunk_length as u64),
        "chunk_count must equal ceil(data_length / chunk_length)"
    );
    // Reconstruct each stored size from consecutive offsets (the last one
    // from data_db_len) and check monotonicity plus the max_compressed_size
    // bound.
    for i in 0..info.chunk_offsets.len() {
        let start = info.chunk_offsets[i];
        let end = if i + 1 < info.chunk_offsets.len() {
            info.chunk_offsets[i + 1]
        } else {
            data_db_len
        };
        assert!(end > start, "chunk {i}: offsets must strictly increase");
        let stored_size = end - start;
        assert!(
            stored_size as usize <= info.max_compressed_size,
            "chunk {i}: stored_size {stored_size} exceeds max_compressed_size {}",
            info.max_compressed_size
        );
    }
    assert!(
        *info.chunk_offsets.last().unwrap() < data_db_len,
        "the last chunk's offset must be strictly before Data.db's end"
    );
}

// ---------------------------------------------------------------------------
// U1: data_length at chunk boundaries
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u1_data_length_at_chunk_boundaries_compressed() {
    let chunk = 64usize;
    for &value_len in &[
        1usize,
        chunk - 1,
        chunk,
        chunk + 1,
        2 * chunk,
        3 * chunk - 1,
    ] {
        let options = WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            chunk_size: chunk,
            verify_output: true,
        };
        let header = single_value_schema().header();
        let partition = one_big_value_partition(b"k", value_len, 0xAB);
        let (_dir, written) = write_file_backed(options, header, &[partition]);
        let info = read_compression_info(&written);
        let data_db_len = std::fs::metadata(&written.files.data).unwrap().len();
        assert_compression_info_invariants(&info, data_db_len);
        let reader = open_reader(&written);
        assert_digest_verifies(&reader);
        assert_eq!(reader.key_count(), 1, "value_len={value_len}");
    }
}

#[test]
fn data_sink_u1_data_length_at_chunk_boundaries_uncompressed() {
    let chunk = 64usize;
    for &value_len in &[
        1usize,
        chunk - 1,
        chunk,
        chunk + 1,
        2 * chunk,
        3 * chunk - 1,
    ] {
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: chunk,
            verify_output: true,
        };
        let header = single_value_schema().header();
        let partition = one_big_value_partition(b"k", value_len, 0xCD);
        let (_dir, written) = write_file_backed(options, header, &[partition]);
        assert!(written.files.compression_info.is_none());
        let crc_bytes = std::fs::read(written.files.crc.as_ref().unwrap()).unwrap();
        let table = ferrosa_sstable::checksum::ChunkCrcTable::parse(&crc_bytes).unwrap();
        assert_eq!(table.chunk_size as usize, chunk);
        assert_eq!(
            table.crcs.len() as u64,
            table.expected_chunk_count(written.files.data_len),
            "value_len={value_len}"
        );
        let reader = open_reader(&written);
        assert_digest_verifies(&reader);
    }
}

// ---------------------------------------------------------------------------
// U2: batch boundaries (default batch_chunks = 16)
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u2_chunk_count_at_batch_boundaries() {
    let chunk = 32usize;
    for &n_chunks in &[15usize, 16, 17, 32] {
        let options = WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            chunk_size: chunk,
            verify_output: true,
        };
        let (_dir, written) =
            one_partition_with_exact_data_length(&options, b"k", n_chunks, chunk, 0x11);
        let info = read_compression_info(&written);
        assert_eq!(
            info.chunk_offsets.len(),
            n_chunks,
            "batch boundary case n_chunks={n_chunks}"
        );
        let reader = open_reader(&written);
        assert_digest_verifies(&reader);
    }
}

// ---------------------------------------------------------------------------
// U3: a single write_all spanning 3+ chunks (a wide row)
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u3_wide_row_spans_many_chunks() {
    let chunk = 128usize;
    let value_len = chunk * 5 + 37; // spans 6 chunks, last one partial
    let options = WriteOptions {
        compression: Some(Compression::Zstd { level: 3 }),
        bloom_fp_chance: 0.01,
        chunk_size: chunk,
        verify_output: true,
    };
    let header = single_value_schema().header();
    let partition = one_big_value_partition(b"wide", value_len, 0x77);
    let (_dir, written) = write_file_backed(options, header, &[partition]);
    let info = read_compression_info(&written);
    assert_eq!(info.chunk_offsets.len(), 6);
    assert!(info.data_length as usize >= value_len);
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
    let partition = reader
        .get_partition(&DecoratedKey::new(PartitionKey::new(b"wide".to_vec())))
        .expect("get_partition")
        .expect("partition must exist");
    let (_, cell) = &partition.rows[0].cells[0];
    assert_eq!(cell.value.as_ref().unwrap().len(), value_len);
}

// ---------------------------------------------------------------------------
// U4/U5: many small (per-row) writes crossing chunk boundaries; partition
// header, first row and END_OF_PARTITION landing in different chunks.
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u4_u5_many_small_writes_cross_chunk_boundaries() {
    let chunk = 96usize; // small enough that a handful of ~40-byte rows cross it
    let options = WriteOptions {
        compression: Some(Compression::Lz4),
        bloom_fp_chance: 0.01,
        chunk_size: chunk,
        verify_output: true,
    };
    let header = clustered_schema().header();
    let partition = many_rows_partition(b"straddle", 50, 40, true);
    let expected_row_count = partition.rows.len();
    let (_dir, written) = write_file_backed(options, header, &[partition]);
    let info = read_compression_info(&written);
    assert!(
        info.chunk_offsets.len() > 3,
        "50 rows of ~40+ framing bytes at a 96-byte chunk must span more than 3 chunks"
    );
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
    let read_back = reader
        .get_partition(&DecoratedKey::new(PartitionKey::new(b"straddle".to_vec())))
        .expect("get_partition")
        .expect("partition must exist");
    assert_eq!(read_back.rows.len(), expected_row_count);
    for (i, row) in read_back.rows.iter().enumerate() {
        let (_, cell) = &row.cells[0];
        assert_eq!(
            cell.value.as_ref().unwrap(),
            &vec![(i as u8) % 251; 40],
            "row {i} value corrupted — a per-byte push crossing a chunk boundary landed wrong"
        );
    }
}

// ---------------------------------------------------------------------------
// U6: a compressed payload straddling a pump SEGMENT boundary (default
// segment = 1 MiB), and one ending exactly on it is covered by the sweep
// below via round-trip + digest verification (exact-boundary framing bytes
// aren't practical to hit precisely from outside the writer, so this sweeps
// the region rather than one exact byte count — see U7 too).
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u6_payload_straddles_pump_segment_boundary() {
    let options = WriteOptions {
        compression: None,
        bloom_fp_chance: 0.01,
        chunk_size: 65536,
        verify_output: true,
    };
    let header = single_value_schema().header();
    // Comfortably more than one default 1 MiB segment.
    let value_len = 3 * 1024 * 1024 + 12345;
    let partition = one_big_value_partition(b"big", value_len, 0x5A);
    let (_dir, written) = write_file_backed(options, header, &[partition]);
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
    let partition = reader
        .get_partition(&DecoratedKey::new(PartitionKey::new(b"big".to_vec())))
        .unwrap()
        .unwrap();
    let (_, cell) = &partition.rows[0].cells[0];
    assert_eq!(cell.value.as_ref().unwrap().len(), value_len);
    assert!(cell.value.as_ref().unwrap().iter().all(|&b| b == 0x5A));
}

// ---------------------------------------------------------------------------
// U7: Data.db length swept across block (4096 on this platform) and segment
// (1 MiB) boundaries.
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u7_data_db_length_swept_across_block_and_segment() {
    let block = 4096i64;
    let segment = 1024 * 1024i64;
    for boundary in [block, segment] {
        for delta in [-1i64, 0, 1] {
            let value_len = (boundary + delta).max(1) as usize;
            let options = WriteOptions {
                compression: None,
                bloom_fp_chance: 0.01,
                chunk_size: 65536,
                verify_output: true,
            };
            let header = single_value_schema().header();
            let partition = one_big_value_partition(b"k", value_len, 0x33);
            let (_dir, written) = write_file_backed(options, header, &[partition]);
            let reader = open_reader(&written);
            assert_digest_verifies(&reader);
            let partition = reader
                .get_partition(&DecoratedKey::new(PartitionKey::new(b"k".to_vec())))
                .unwrap()
                .unwrap();
            let (_, cell) = &partition.rows[0].cells[0];
            assert_eq!(
                cell.value.as_ref().unwrap().len(),
                value_len,
                "boundary={boundary} delta={delta}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// U8: incompressible input — a compressed chunk may exceed chunk_size.
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u8_incompressible_input_chunk_may_exceed_chunk_size() {
    let chunk = 256usize;
    let options = WriteOptions {
        compression: Some(Compression::Lz4),
        bloom_fp_chance: 0.01,
        chunk_size: chunk,
        verify_output: true,
    };
    let header = single_value_schema().header();
    // Deterministic pseudo-random (xorshift), matching compression.rs's own
    // test generator, so it does not compress away to nothing.
    let mut state = 0xC0FFEEu64 | 1;
    let value: Vec<u8> = (0..chunk * 4)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xff) as u8
        })
        .collect();
    let partition = Partition {
        key: DecoratedKey::new(PartitionKey::new(b"rand".to_vec())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(value.clone(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }],
    };
    let (_dir, written) = write_file_backed(options, header, &[partition]);
    let info = read_compression_info(&written);
    // LZ4's worst case adds a header/length byte overhead per chunk, so
    // max_compressed_size for incompressible data is allowed to exceed
    // chunk_size (this is exactly what compress_bound accounts for).
    assert!(
        info.max_compressed_size >= chunk,
        "incompressible input's max_compressed_size ({}) should be at or above chunk_size ({chunk})",
        info.max_compressed_size
    );
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
    let partition = reader
        .get_partition(&DecoratedKey::new(PartitionKey::new(b"rand".to_vec())))
        .unwrap()
        .unwrap();
    let (_, cell) = &partition.rows[0].cells[0];
    assert_eq!(cell.value.as_ref().unwrap(), &value);
}

// ---------------------------------------------------------------------------
// U9: all-zero (highly compressible) input — many tiny chunks.
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u9_all_zero_input_many_tiny_chunks_in_one_block() {
    let chunk = 64usize;
    let n_chunks = 800usize; // way more offsets than fit in a "few" bytes, but the offsets file itself stays under one segment
    let options = WriteOptions {
        compression: Some(Compression::Lz4),
        bloom_fp_chance: 0.01,
        chunk_size: chunk,
        verify_output: true,
    };
    let (_dir, written) =
        one_partition_with_exact_data_length(&options, b"zeros", n_chunks, chunk, 0);
    let info = read_compression_info(&written);
    assert_eq!(info.chunk_offsets.len(), n_chunks);
    let data_db_len = std::fs::metadata(&written.files.data).unwrap().len();
    assert_compression_info_invariants(&info, data_db_len);
    // Every all-zero chunk should compress to (much) less than chunk_size —
    // otherwise this test isn't exercising "tiny payloads" at all.
    assert!(
        (data_db_len as usize) < (chunk * n_chunks) / 2,
        "all-zero data should compress well below the raw length"
    );
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
}

// ---------------------------------------------------------------------------
// U10: empty SSTable, compressed and uncompressed, file-backed.
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u10_empty_table_compressed() {
    let options = WriteOptions {
        compression: Some(Compression::Lz4),
        bloom_fp_chance: 0.01,
        chunk_size: 4096,
        verify_output: true,
    };
    let header = single_value_schema().header();
    let (_dir, written) = write_file_backed(options, header, &[]);
    let info = read_compression_info(&written);
    assert_eq!(info.data_length, 0);
    assert_eq!(info.chunk_offsets.len(), 0);
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
    assert_eq!(reader.key_count(), 0);
}

#[test]
fn data_sink_u10_empty_table_uncompressed() {
    let options = WriteOptions {
        compression: None,
        bloom_fp_chance: 0.01,
        chunk_size: 4096,
        verify_output: true,
    };
    let header = single_value_schema().header();
    let (_dir, written) = write_file_backed(options, header, &[]);
    assert!(written.files.compression_info.is_none());
    assert_eq!(written.files.data_len, 0);
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
    assert_eq!(reader.key_count(), 0);
}

// ---------------------------------------------------------------------------
// U11: partition/row positions recorded in Partitions.db/Rows.db are correct
// — proven by reading back every row of a wide-clustered (row-indexed)
// partition through the real reader.
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u11_row_index_positions_correct_across_chunks() {
    let chunk = 200usize; // small enough that 60 rows definitely straddle several chunks
    let options = WriteOptions {
        compression: Some(Compression::Zstd { level: 1 }),
        bloom_fp_chance: 0.01,
        chunk_size: chunk,
        verify_output: true,
    };
    let header = clustered_schema().header();
    // 60 rows: above ROW_INDEX_MIN_ROWS (32), so Rows.db carries a real
    // per-partition row-index trie, not just a Data.db offset.
    let partition = many_rows_partition(b"wide-partition", 60, 55, true);
    let (_dir, written) = write_file_backed(options, header, &[partition]);
    let reader = open_reader(&written);
    assert_digest_verifies(&reader);
    let read_back = reader
        .get_partition(&DecoratedKey::new(PartitionKey::new(
            b"wide-partition".to_vec(),
        )))
        .unwrap()
        .unwrap();
    assert_eq!(read_back.rows.len(), 60);
    for (i, row) in read_back.rows.iter().enumerate() {
        assert_eq!(
            i32::from_be_bytes(row.clustering.clone().try_into().unwrap()),
            i as i32,
            "row {i}: clustering key corrupted — a wrong row-index position"
        );
        let (_, cell) = &row.cells[0];
        assert_eq!(cell.value.as_ref().unwrap(), &vec![(i as u8) % 251; 55]);
    }
}

// ---------------------------------------------------------------------------
// U12: CompressionInfo round-trips through write()/read() — the real bytes a
// writer run produced, not just the unit's own hand-built struct
// (`compression.rs`'s `compression_info_round_trip` covers the latter).
// ---------------------------------------------------------------------------

#[test]
fn data_sink_u12_compression_info_round_trips_from_a_real_writer_run() {
    let chunk = 512usize;
    let options = WriteOptions {
        compression: Some(Compression::Zstd { level: 5 }),
        bloom_fp_chance: 0.01,
        chunk_size: chunk,
        verify_output: true,
    };
    let header = single_value_schema().header();
    let partition = one_big_value_partition(b"k", chunk * 7 + 3, 0x9); // 8 chunks
    let (_dir, written) = write_file_backed(options, header, &[partition]);
    let path = written.files.compression_info.as_ref().unwrap();
    let bytes = std::fs::read(path).unwrap();
    let info = CompressionInfo::read(&bytes).unwrap();
    let rewritten = info.write().unwrap();
    assert_eq!(
        bytes, rewritten,
        "CompressionInfo.db must round-trip byte-for-byte through read()/write()"
    );
}

// ---------------------------------------------------------------------------
// CompressionInfo header-patch byte identity: the patched header (written
// last, after the offsets) must equal exactly what `CompressionInfo::write()`
// would produce from the same field values, for chunk counts sweeping the
// held-back-block boundary (block/8 offsets fill exactly one block on this
// platform's 4096-byte block: block/8 = 512).
// ---------------------------------------------------------------------------

#[test]
fn data_sink_header_patch_matches_compression_info_write_at_chunk_count_boundaries() {
    let chunk = 16usize;
    for &n_chunks in &[0usize, 1, 511, 512] {
        let options = WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            chunk_size: chunk,
            verify_output: true,
        };
        let (_dir, written) = if n_chunks == 0 {
            write_file_backed(options, single_value_schema().header(), &[])
        } else {
            one_partition_with_exact_data_length(&options, b"k", n_chunks, chunk, 0x22)
        };
        let path = written.files.compression_info.as_ref().unwrap();
        let on_disk = std::fs::read(path).unwrap();
        let info = CompressionInfo::read(&on_disk).unwrap();
        assert_eq!(
            info.chunk_offsets.len(),
            n_chunks,
            "chunk_count did not match the requested boundary case: n_chunks={n_chunks}"
        );
        let expected = info.write().unwrap();
        assert_eq!(
            on_disk, expected,
            "n_chunks={n_chunks}: the patched on-disk header+offsets must equal \
             CompressionInfo::write()'s own encoding byte-for-byte"
        );
    }
}
