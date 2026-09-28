//! P0 2026-09-28: an SSTable holding many partitions that carry ONLY a
//! partition-level deletion (no rows, no static row) must read back intact.
//!
//! Flush began writing such partitions on 2026-09-26 (50430b91, which stopped
//! flush from dropping them and losing the delete). In a real three-node
//! cluster the resulting SSTables failed the startup smoke test with
//! `read_exact_at: wanted N bytes, got M` and partition-order violations, were
//! quarantined, and the node served a near-empty table. 400 such partitions
//! read back; 800 did not.

use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, Partition};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";

fn header() -> SerializationHeader {
    SerializationHeader {
        min_timestamp: 0,
        min_local_deletion_time: 0,
        min_ttl: 0,
        max_timestamp: i64::MAX,
        key_type: UTF8.to_string(),
        clustering_types: Vec::new(),
        static_columns: Vec::new(),
        regular_columns: vec![(b"val".to_vec(), UTF8.to_string())],
        complex_collections: false,
    }
}

/// `n` partitions, each only a partition-level delete, in token order.
fn tombstone_partitions(n: u32) -> Vec<Partition> {
    let mut partitions: Vec<Partition> = (0..n)
        .map(|i| Partition {
            key: DecoratedKey::new(PartitionKey::new(i.to_string().into_bytes())),
            deletion: DeletionTime::new(1_790_000_000_000_000 + i64::from(i), 1_790_000_000),
            static_row: None,
            rows: Vec::new(),
        })
        .collect();
    partitions.sort_by(|a, b| a.key.cmp(&b.key));
    partitions
}

fn assert_reads_back(reader: &SSTableReader<Vec<u8>>, expected: &[Partition]) {
    let mut iter = reader.partitions_iter().expect("partitions_iter");
    let mut read = Vec::with_capacity(expected.len());
    for _ in 0..=expected.len() {
        match iter.next_partition() {
            Ok(Some(p)) => read.push(p),
            Ok(None) => break,
            Err(e) => panic!(
                "scanning tombstone partitions failed after {} of {}: {e}",
                read.len(),
                expected.len()
            ),
        }
    }
    assert_eq!(
        read.len(),
        expected.len(),
        "every tombstone partition must read back"
    );
    for (got, want) in read.iter().zip(expected) {
        assert_eq!(got.key, want.key);
        assert_eq!(got.deletion, want.deletion);
        assert!(got.rows.is_empty() && got.static_row.is_none());
    }
    // The two-phase streaming read the startup smoke test, self-heal and the
    // range merger use. Before the fix, a partition with no rows left the
    // row phase reading the NEXT partition's key bytes as a row.
    let mut stream = reader.partitions_iter().expect("partitions_iter");
    for (idx, want) in expected.iter().enumerate() {
        let (key, deletion, static_row) = stream
            .next_partition_header_only()
            .unwrap_or_else(|e| panic!("streamed header {idx} failed: {e}"))
            .unwrap_or_else(|| panic!("streamed EOF after {idx} partitions"));
        assert_eq!(key, want.key, "streamed partition {idx} out of place");
        assert_eq!(deletion, want.deletion);
        assert!(static_row.is_none());
        let mut rows = 0usize;
        stream
            .stream_clustered_rows(|_| {
                rows += 1;
                Ok(())
            })
            .unwrap_or_else(|e| panic!("streamed rows of partition {idx}: {e}"));
        assert_eq!(rows, 0, "a tombstone-only partition has no rows to stream");
    }
    assert!(stream.next_partition_header_only().expect("EOF").is_none());

    // The one-row-at-a-time variant must also stop at the partition end.
    let mut pull = reader.partitions_iter().expect("partitions_iter");
    for want in expected {
        let (key, ..) = pull
            .next_partition_header_only()
            .expect("header")
            .expect("some");
        assert_eq!(key, want.key);
        assert!(pull.next_clustered_row().expect("row pull").is_none());
    }

    for want in expected {
        let got = reader
            .get_partition(&want.key)
            .unwrap_or_else(|e| panic!("point lookup of {:?} failed: {e}", want.key))
            .unwrap_or_else(|| panic!("point lookup of {:?} found nothing", want.key));
        assert_eq!(got.deletion, want.deletion);
    }
}

fn in_memory_roundtrip(n: u32) {
    let partitions = tombstone_partitions(n);
    let mut writer = SSTableWriter::new(WriteOptions::default(), header());
    for p in &partitions {
        writer.add_partition(p).expect("add_partition");
    }
    let out = writer.finish().expect("finish");
    let reader = SSTableReader::open(SSTableComponents {
        data: out.data,
        partitions: out.partitions,
        rows: out.rows,
        filter: out.filter,
        compression_info: out.compression_info,
        statistics: out.statistics,
    })
    .expect("open");
    assert_reads_back(&reader, &partitions);
}

fn file_backed_roundtrip(n: u32) {
    let partitions = tombstone_partitions(n);
    let dir = tempfile::tempdir().expect("tempdir");
    let staging = dir.path().join("staging");
    let mut writer =
        SSTableWriter::new_file_backed(WriteOptions::default(), header(), staging.join("Data.raw"))
            .expect("new_file_backed");
    for p in &partitions {
        writer.add_partition(p).expect("add_partition");
    }
    let files = writer
        .finish_to_directory(&staging)
        .expect("finish_to_directory");
    let read = |p: &std::path::Path| std::fs::read(p).expect("read component");
    let reader = SSTableReader::open(SSTableComponents {
        data: read(&files.data),
        partitions: read(&files.partitions),
        rows: read(&files.rows),
        filter: read(&files.filter),
        compression_info: files.compression_info.as_deref().map(read),
        statistics: read(&files.statistics),
    })
    .expect("open");
    assert_reads_back(&reader, &partitions);
}

#[test]
fn in_memory_writer_round_trips_400_partition_tombstones() {
    in_memory_roundtrip(400);
}

#[test]
fn in_memory_writer_round_trips_1600_partition_tombstones() {
    in_memory_roundtrip(1600);
}

#[test]
fn file_backed_writer_round_trips_400_partition_tombstones() {
    file_backed_roundtrip(400);
}

#[test]
fn file_backed_writer_round_trips_1600_partition_tombstones() {
    file_backed_roundtrip(1600);
}
