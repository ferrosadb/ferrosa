//! Dump an SSTable to human-readable output.
//!
//! Usage: `ferrosa-sstable-dump <sstable-dir> <generation-id>`
//!
//! Reads a Cassandra BTI-format SSTable and prints partition keys,
//! row counts, and cell summaries.

use std::path::PathBuf;

use ferrosa_sstable::io::FileReadAt;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: ferrosa-sstable-dump <sstable-dir> <generation-id>");
        eprintln!();
        eprintln!("Example: ferrosa-sstable-dump data/sstables/ks.tbl/ 1775519778510328");
        std::process::exit(1);
    }

    let dir = PathBuf::from(&args[1]);
    let gen = &args[2];

    let data_path = dir.join(format!("{gen}-Data.db"));
    if !data_path.exists() {
        eprintln!("Error: Data.db not found: {}", data_path.display());
        std::process::exit(1);
    }

    let data = FileReadAt::open(&data_path).expect("open Data.db");
    let partitions_file =
        FileReadAt::open(dir.join(format!("{gen}-Partitions.db"))).expect("open Partitions.db");
    let rows = FileReadAt::open(dir.join(format!("{gen}-Rows.db"))).expect("open Rows.db");
    let filter = std::fs::read(dir.join(format!("{gen}-Filter.db"))).unwrap_or_default();
    let statistics = std::fs::read(dir.join(format!("{gen}-Statistics.db"))).unwrap_or_default();
    let compression_info = std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok();

    let reader = match SSTableReader::open(SSTableComponents {
        data,
        partitions: partitions_file,
        rows,
        filter,
        compression_info,
        statistics,
    }) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error opening SSTable: {e}");
            std::process::exit(1);
        }
    };

    let mut iter = match reader.partitions_iter() {
        Ok(iter) => iter,
        Err(e) => {
            eprintln!("Error opening partition stream: {e}");
            std::process::exit(1);
        }
    };

    println!("SSTable: {}/{gen}", dir.display());
    println!("---");
    let mut partition_count = 0usize;
    let mut total_rows = 0usize;
    loop {
        match iter.next_partition_header_only() {
            Ok(Some((key, _deletion, static_row))) => {
                let i = partition_count;
                partition_count += 1;
                let key_str = String::from_utf8_lossy(key.key.as_bytes());
                println!("[{i}] key={key_str:?} token={}", key.token.0);
                if let Some(row) = static_row {
                    println!(
                        "  static ck_len={} cells={}",
                        row.clustering.len(),
                        row.cells.len()
                    );
                }
                if let Err(e) = iter.stream_clustered_rows(|row| {
                    total_rows += 1;
                    println!(
                        "  row ck_len={} cells={}",
                        row.clustering.len(),
                        row.cells.len()
                    );
                    Ok(())
                }) {
                    eprintln!("Error reading partition rows: {e}");
                    std::process::exit(1);
                }
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading partition stream: {e}");
                std::process::exit(1);
            }
        }
    }
    println!("---");
    println!("Partitions: {partition_count}");
    println!("Total rows: {total_rows}");
}
