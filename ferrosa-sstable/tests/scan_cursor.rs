//! Cursor guards complement the public-executor allocation red; these API tests
//! were added after the incremental API and assert parity, pull rate and failure safety.
//! Last revised: 2026-10-23
//! Last changed: Cover reusable buffers, BTI lookup and incomplete staged output.
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Result};
use ferrosa_sstable::{
    compression::Compression,
    io::ReadAt,
    reader::{SSTableComponents, SSTableReader},
    statistics::SerializationHeader,
    types::{DeletionTime, LivenessInfo, Partition, Row},
    writer::{SSTableOutput, SSTableWriter, WriteOptions},
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
struct Counter;
static EVENTS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static ALLOCATOR: Counter = Counter;
struct Traced {
    bytes: Vec<u8>,
    read: Arc<AtomicUsize>,
}
impl ReadAt for Traced {
    fn len(&self) -> Result<u64> {
        Ok(self.bytes.len() as u64)
    }
    fn read_at(&self, out: &mut [u8], offset: u64) -> Result<usize> {
        let n = self.bytes.as_slice().read_at(out, offset)?;
        self.read.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
}
fn header(complex: bool) -> SerializationHeader {
    SerializationHeader{min_timestamp:0,max_timestamp:10000,min_ttl:0,min_local_deletion_time:0,key_type:"BytesType".into(),clustering_types:vec!["Int32Type".into()],static_columns:vec![(b"s".to_vec(),"BytesType".into())],regular_columns:vec![(b"v".to_vec(),if complex {"org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.BytesType)"}else{"BytesType"}.into())],complex_collections:complex}
}
fn row(i: i32, complex: bool) -> Row {
    let mut cell = CellValue::live(vec![17; 128], 1000);
    if complex {
        cell = cell.with_path(7i32.to_be_bytes().to_vec());
    }
    Row {
        clustering: i.to_be_bytes().to_vec(),
        cells: vec![(0, cell)],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: if i % 2 == 0 {
            LivenessInfo::with_timestamp(1000)
        } else {
            LivenessInfo::with_ttl(1000, 600, 900)
        },
    }
}
fn reader(output: SSTableOutput) -> (SSTableReader<Traced>, Arc<AtomicUsize>, usize) {
    let read = Arc::new(AtomicUsize::new(0));
    let size = output.data.len();
    let traced = |bytes| Traced {
        bytes,
        read: read.clone(),
    };
    let mut r = SSTableReader::open(SSTableComponents {
        data: traced(output.data),
        partitions: traced(output.partitions),
        rows: traced(output.rows),
        filter: output.filter,
        statistics: output.statistics,
        compression_info: output.compression_info,
    })
    .unwrap();
    if let Some(crc) = output.crc {
        r.load_crc_table(&crc).unwrap();
    }
    r.load_digest(&output.digest).unwrap();
    (r, read, size)
}
#[test]
fn scan_cursor_parity_allocation_pull_and_failure_contracts() {
    for compression in [
        None,
        Some(Compression::Lz4),
        Some(Compression::Zstd { level: 3 }),
    ] {
        for complex in [false, true] {
            let h = header(complex);
            let expected = Partition {
                key: DecoratedKey::new(PartitionKey::from(b"p".as_slice())),
                deletion: DeletionTime::LIVE,
                static_row: Some(Row {
                    clustering: Vec::new(),
                    cells: vec![(0, CellValue::live(b"static".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::NONE,
                }),
                rows: (0..1024).map(|i| row(i, complex)).collect(),
            };
            let options = WriteOptions {
                compression: compression.clone(),
                verify_output: false,
                ..WriteOptions::default()
            };
            let mut owned = SSTableWriter::new(options.clone(), h.clone());
            owned.add_partition(&expected).unwrap();
            let owned = owned.finish().unwrap();
            let mut writer = SSTableWriter::new(options.clone(), h.clone());
            writer
                .begin_partition(
                    &expected.key,
                    expected.deletion,
                    expected.static_row.as_ref(),
                )
                .unwrap();
            for r in &expected.rows {
                writer.write_partition_row(r).unwrap();
            }
            writer.end_partition().unwrap();
            let output = writer.finish().unwrap();
            assert_eq!(output.data, owned.data, "incremental row encoding changed");
            let (r, reads, size) = reader(output);
            let mut it = r.scan_iter().unwrap();
            assert_eq!(
                it.next_partition_header_only().unwrap(),
                Some((
                    expected.key.clone(),
                    expected.deletion,
                    expected.static_row.clone()
                ))
            );
            let mut decoded = row(0, complex);
            assert!(it.next_clustered_row_into(&mut decoded).unwrap());
            assert_eq!(decoded, expected.rows[0]);
            assert!(
                reads.load(Ordering::Relaxed) < size,
                "cursor read the full data before its first row"
            );
            // Warm several chunks to cover codec state and maximum compressed size.
            for e in &expected.rows[1..128] {
                assert!(it.next_clustered_row_into(&mut decoded).unwrap());
                assert_eq!(&decoded, e);
            }
            let before = EVENTS.load(Ordering::Relaxed);
            for e in &expected.rows[128..] {
                assert!(it.next_clustered_row_into(&mut decoded).unwrap());
                assert_eq!(&decoded, e);
            }
            let events = EVENTS.load(Ordering::Relaxed) - before;
            assert!(events<=4,"reusable scan allocated {events} times for 896 rows ({compression:?},complex={complex})");
            assert!(!it.next_clustered_row_into(&mut decoded).unwrap());
            assert!(it.next_partition_header_only().unwrap().is_none());
            assert_eq!(
                r.get_partition(&expected.key).unwrap().as_ref(),
                Some(&expected)
            );
            for ck in [0i32, 511, 1023] {
                let point = r
                    .get_clustering_row(&expected.key, &ck.to_be_bytes())
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    point.rows,
                    std::slice::from_ref(&expected.rows[ck as usize])
                );
            }

            // Metadata heads retain identity/liveness but do not own values.
            // Fetching each selected span must recover the exact original bytes.
            let mut descriptors = r.scan_iter().unwrap();
            descriptors.next_partition_header_only().unwrap().unwrap();
            let mut spans = Vec::new();
            let mut value = Vec::new();
            for expected_row in &expected.rows {
                assert!(descriptors
                    .next_clustered_row_metadata_into(&mut decoded, &mut spans)
                    .unwrap());
                assert_eq!(decoded.clustering, expected_row.clustering);
                assert_eq!(
                    decoded.primary_key_liveness,
                    expected_row.primary_key_liveness
                );
                assert_eq!(decoded.cells[0].1.value.as_deref(), Some(b"".as_slice()));
                descriptors.read_value_span(spans[0], &mut value).unwrap();
                assert_eq!(
                    Some(value.as_slice()),
                    expected_row.cells[0].1.value.as_deref()
                );
                assert_eq!(decoded.cells[0].1.path, expected_row.cells[0].1.path);
            }
            assert!(!descriptors
                .next_clustered_row_metadata_into(&mut decoded, &mut spans)
                .unwrap());

            let mut unfinished = SSTableWriter::new(options.clone(), h.clone());
            unfinished
                .begin_partition(&expected.key, DeletionTime::LIVE, None)
                .unwrap();
            assert!(unfinished.finish().is_err());
            let mut poisoned = SSTableWriter::new(options, h);
            poisoned
                .begin_partition(&expected.key, DeletionTime::LIVE, None)
                .unwrap();
            let mut bad = row(0, complex);
            bad.clustering = vec![1];
            assert!(poisoned.write_partition_row(&bad).is_err());
            assert!(poisoned.end_partition().is_err());
            assert!(poisoned.finish().is_err());
        }
    }
    let h = header(false);
    let mut w = SSTableWriter::new(
        WriteOptions {
            compression: None,
            verify_output: false,
            ..WriteOptions::default()
        },
        h,
    );
    let key = DecoratedKey::new(PartitionKey::from(b"p".as_slice()));
    let mut large = row(0, false);
    large.cells[0].1.value = Some(vec![7; 128 * 1024].into());
    w.begin_partition(&key, DeletionTime::LIVE, None).unwrap();
    w.write_partition_row(&large).unwrap();
    w.end_partition().unwrap();
    let mut output = w.finish().unwrap();
    output.data[65536] ^= 1;
    let (r, _, _) = reader(output);
    let mut it = r.scan_iter().unwrap();
    it.next_partition_header_only().unwrap().unwrap();
    let mut decoded = row(0, false);
    let mut spans = Vec::new();
    assert!(
        it.next_clustered_row_metadata_into(&mut decoded, &mut spans)
            .is_err(),
        "deferred loser bytes bypassed a middle-chunk CRC"
    );

    // Fully decoding a streamed verification must still reject a missing end marker.
    let h = header(false);
    let mut w = SSTableWriter::new(
        WriteOptions {
            compression: None,
            verify_output: false,
            ..WriteOptions::default()
        },
        h,
    );
    let key = DecoratedKey::new(PartitionKey::from(b"p".as_slice()));
    w.begin_partition(&key, DeletionTime::LIVE, None).unwrap();
    w.write_partition_row(&row(0, false)).unwrap();
    w.end_partition().unwrap();
    let mut o = w.finish().unwrap();
    o.data.pop();
    o.crc = None; // isolate structural validation from CRC/digest
    let (r, _, _) = reader(o);
    let mut it = r.scan_iter().unwrap();
    it.next_partition_header_only().unwrap().unwrap();
    let mut decoded = row(0, false);
    assert!(it.next_clustered_row_into(&mut decoded).unwrap());
    assert!(it.next_clustered_row_into(&mut decoded).is_err());
}
