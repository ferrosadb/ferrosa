//! Deferred values must not turn metadata seeks into full read-ahead refills.
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Result};
use ferrosa_sstable::{
    io::ReadAt,
    reader::{SSTableComponents, SSTableReader},
    scan::ReadAheadReader,
    statistics::SerializationHeader,
    types::{DeletionTime, LivenessInfo, Partition, Row},
    writer::{SSTableWriter, WriteOptions},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Counted {
    bytes: Vec<u8>,
    reads: Arc<AtomicUsize>,
}
impl ReadAt for Counted {
    fn len(&self) -> Result<u64> {
        Ok(self.bytes.len() as u64)
    }
    fn read_at(&self, out: &mut [u8], offset: u64) -> Result<usize> {
        let n = self.bytes.as_slice().read_at(out, offset)?;
        self.reads.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
}

#[test]
fn deferred_scan_keeps_physical_reads_near_a_single_pass() {
    check_deferred_scan(false);
    check_deferred_scan(true);
}

fn check_deferred_scan(prefetch: bool) {
    const WINDOW: usize = 1024 * 1024;
    const VALUE: usize = 2 * WINDOW;
    let header = SerializationHeader {
        min_timestamp: 0,
        max_timestamp: 1000,
        min_ttl: 0,
        min_local_deletion_time: 0,
        key_type: "BytesType".into(),
        clustering_types: vec!["Int32Type".into()],
        static_columns: vec![],
        regular_columns: vec![
            (b"v".to_vec(), "BytesType".into()),
            (b"w".to_vec(), "BytesType".into()),
        ],
        complex_collections: false,
    };
    let mut writer = SSTableWriter::new(
        WriteOptions {
            compression: None,
            verify_output: false,
            ..WriteOptions::default()
        },
        header,
    );
    writer
        .add_partition(&Partition {
            key: DecoratedKey::new(PartitionKey::from(b"p".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0i32..2)
                .map(|i| Row {
                    clustering: i.to_be_bytes().to_vec(),
                    cells: vec![
                        (0, CellValue::live(vec![17 + i as u8; VALUE], 1000)),
                        (1, CellValue::live(vec![33; VALUE], 1000)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::NONE,
                })
                .collect(),
        })
        .unwrap();
    let output = writer.finish().unwrap();
    let size = output.data.len();
    let reads = Arc::new(AtomicUsize::new(0));
    let wrap = |bytes, reads| {
        let source = Counted { bytes, reads };
        if prefetch {
            ReadAheadReader::with_prefetch(source, WINDOW).unwrap()
        } else {
            ReadAheadReader::new(source, WINDOW).unwrap()
        }
    };
    let mut reader = SSTableReader::open(SSTableComponents {
        data: wrap(output.data, reads.clone()),
        partitions: wrap(output.partitions, Arc::new(AtomicUsize::new(0))),
        rows: wrap(output.rows, Arc::new(AtomicUsize::new(0))),
        filter: output.filter,
        statistics: output.statistics,
        compression_info: output.compression_info,
    })
    .unwrap();
    reader
        .load_crc_table(output.crc.as_ref().expect("uncompressed CRC"))
        .unwrap();
    let reader = std::sync::Arc::new(reader);
    reads.store(0, Ordering::Relaxed);
    let mut cursor = reader.scan_iter().unwrap();
    assert!(cursor.next_partition_header_only().unwrap().is_some());
    let mut row = Row {
        clustering: vec![],
        cells: vec![],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::NONE,
    };
    let mut spans = vec![];
    let mut value = vec![];
    for i in 0i32..2 {
        assert!(cursor
            .next_clustered_row_pending_into(&mut row, &mut spans)
            .unwrap());
        assert_eq!(row.clustering, i.to_be_bytes());
        assert_eq!(spans.len(), 2);
        cursor.read_value_span(spans[0], &mut value).unwrap();
        assert_eq!(value.len(), VALUE);
        assert!(value.iter().all(|&b| b == 17 + i as u8));
        cursor.validate_value_span(spans[1]).unwrap();
    }
    assert!(!cursor
        .next_clustered_row_pending_into(&mut row, &mut spans)
        .unwrap());
    drop(cursor);
    drop(reader);
    let actual = reads.load(Ordering::Relaxed);
    eprintln!("deferred scan prefetch={prefetch}: {actual} physical bytes / {size} logical bytes");
    assert!(
        actual <= size + 2 * WINDOW,
        "metadata seeks amplified Data.db reads: {actual} physical bytes for {size} logical bytes"
    );
}
