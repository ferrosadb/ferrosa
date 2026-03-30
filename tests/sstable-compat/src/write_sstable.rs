//! Write test SSTables for Cassandra compatibility testing.
//!
//! Usage: write-sstable <output-dir>
//!
//! Writes a single SSTable to `<output-dir>/compat/simple_types/` containing
//! one partition with all simple CQL types, suitable for import by
//! `sstableloader` into a Cassandra 5.1 instance.
//!
//! The schema matches:
//!
//! ```sql
//! CREATE TABLE compat.simple_types (
//!   id      int     PRIMARY KEY,
//!   v_text  text,
//!   v_bool  boolean,
//!   v_bigint bigint,
//!   v_float float,
//!   v_double double,
//!   v_blob  blob
//! );
//! ```

use std::fs;
use std::path::Path;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::toc;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

fn main() -> anyhow::Result<()> {
    let output_dir = std::env::args()
        .nth(1)
        .expect("Usage: write-sstable <output-dir>");
    write_simple_types_sstable(&output_dir)?;
    println!("SSTable written to {output_dir}/compat/simple_types/");
    Ok(())
}

/// Write a simple_types SSTable to `<output_dir>/compat/simple_types/`.
///
/// The SSTable contains one partition (id = 1) with live cells for every
/// simple CQL type supported by the schema above.
fn write_simple_types_sstable(output_dir: &str) -> anyhow::Result<()> {
    // sstableloader expects: <output_dir>/<keyspace>/<table>/
    let dir = Path::new(output_dir).join("compat").join("simple_types");
    fs::create_dir_all(&dir)?;

    // Serialization header: describes the schema for Data.db delta-encoding.
    //
    // Column index mapping (matches the CQL schema declaration order):
    //   0 -> v_text    (UTF8Type)
    //   1 -> v_bool    (BooleanType)
    //   2 -> v_bigint  (LongType)
    //   3 -> v_float   (FloatType)
    //   4 -> v_double  (DoubleType)
    //   5 -> v_blob    (BytesType)
    let header = SerializationHeader {
        min_timestamp: 1_000_000,
        min_local_deletion_time: i32::MAX,
        min_ttl: 0,
        key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
        clustering_types: vec![],
        static_columns: vec![],
        regular_columns: vec![
            (
                b"v_text".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            ),
            (
                b"v_bool".to_vec(),
                "org.apache.cassandra.db.marshal.BooleanType".into(),
            ),
            (
                b"v_bigint".to_vec(),
                "org.apache.cassandra.db.marshal.LongType".into(),
            ),
            (
                b"v_float".to_vec(),
                "org.apache.cassandra.db.marshal.FloatType".into(),
            ),
            (
                b"v_double".to_vec(),
                "org.apache.cassandra.db.marshal.DoubleType".into(),
            ),
            (
                b"v_blob".to_vec(),
                "org.apache.cassandra.db.marshal.BytesType".into(),
            ),
        ],
        extensions: Default::default(),
    };

    let options = WriteOptions {
        compression: None,
        bloom_fp_chance: 0.01,
        chunk_size: 65536,
    };

    let timestamp = 1_743_120_000_000_000i64; // 2025-03-28 00:00:00 UTC in microseconds

    // Partition key: id = 1, encoded as big-endian int32.
    let pk_bytes: [u8; 4] = 1i32.to_be_bytes();
    let partition = Partition {
        key: DecoratedKey::new(PartitionKey::from(pk_bytes.as_slice())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![
                // v_text = "hello"
                (0, CellValue::live(b"hello".to_vec(), timestamp)),
                // v_bool = true (Cassandra encodes boolean as single byte: 0x01 = true)
                (1, CellValue::live(vec![0x01], timestamp)),
                // v_bigint = 42 (big-endian i64)
                (2, CellValue::live(42i64.to_be_bytes().to_vec(), timestamp)),
                // v_float = 3.14 (big-endian f32)
                (
                    3,
                    CellValue::live(
                        (3.14f32).to_bits().to_be_bytes().to_vec(),
                        timestamp,
                    ),
                ),
                // v_double = 2.718281828 (big-endian f64)
                (
                    4,
                    CellValue::live(
                        (2.718_281_828f64).to_bits().to_be_bytes().to_vec(),
                        timestamp,
                    ),
                ),
                // v_blob = 0xDEADBEEF
                (5, CellValue::live(vec![0xDE, 0xAD, 0xBE, 0xEF], timestamp)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }],
    };

    let mut writer = SSTableWriter::new(options, header);
    writer.add_partition(&partition)?;
    let output = writer.finish()?;

    // Write component files using the standard BTI naming convention expected
    // by sstableloader.  Cassandra 5.1 uses the "nb" format prefix.
    let prefix = "nb-1-big";
    fs::write(dir.join(format!("{prefix}-Data.db")), &output.data)?;
    fs::write(dir.join(format!("{prefix}-Partitions.db")), &output.partitions)?;
    fs::write(dir.join(format!("{prefix}-Rows.db")), &output.rows)?;
    fs::write(dir.join(format!("{prefix}-Filter.db")), &output.filter)?;
    fs::write(dir.join(format!("{prefix}-Statistics.db")), &output.statistics)?;
    // CompressionInfo.db only written if compression was enabled.
    if let Some(ref ci) = output.compression_info {
        fs::write(dir.join(format!("{prefix}-CompressionInfo.db")), ci)?;
    }
    // TOC lists the present component suffixes.
    let toc_entries: Vec<&str> = if output.compression_info.is_some() {
        vec![
            toc::COMPRESSION_INFO,
            toc::DATA,
            toc::FILTER,
            toc::PARTITIONS,
            toc::ROWS,
            toc::STATISTICS,
            toc::TOC,
        ]
    } else {
        vec![
            toc::CRC,
            toc::DATA,
            toc::FILTER,
            toc::PARTITIONS,
            toc::ROWS,
            toc::STATISTICS,
            toc::TOC,
        ]
    };
    let toc_bytes = toc::write_toc(&toc_entries);
    fs::write(dir.join(format!("{prefix}-TOC.txt")), &toc_bytes)?;

    Ok(())
}
