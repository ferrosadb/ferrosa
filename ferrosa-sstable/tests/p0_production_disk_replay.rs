//! P0 repro: replay a production on-disk SSTable and report exactly
//! where (if anywhere) the reader fails. This is diagnostic — the test is
//! ignored by default so CI doesn't depend on the local data directory.
//!
//! Run with:
//!   cargo test -p ferrosa-sstable --test p0_production_disk_replay -- --ignored --nocapture

use std::fs;
use std::path::{Path, PathBuf};

use ferrosa_sstable::data::DataReader;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::read_statistics;

fn open_on_disk(dir: &Path, gen_prefix: &str) -> SSTableReader<Vec<u8>> {
    let load = |suffix: &str| -> Vec<u8> {
        let path = dir.join(format!("{gen_prefix}-{suffix}"));
        fs::read(&path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e))
    };
    let compression_info_path = dir.join(format!("{gen_prefix}-CompressionInfo.db"));
    let compression_info = if compression_info_path.exists() {
        Some(fs::read(&compression_info_path).unwrap())
    } else {
        None
    };
    let components = SSTableComponents {
        data: load("Data.db"),
        partitions: load("Partitions.db"),
        rows: load("Rows.db"),
        filter: load("Filter.db"),
        compression_info,
        statistics: load("Statistics.db"),
    };
    SSTableReader::open(components).expect("open SSTable")
}

#[test]
#[ignore]
fn replay_entity_store_sstable_from_node1() {
    let dir = PathBuf::from(
        "/Users/bkearns/data/ferrosa-memory/node1/sstables/agent_memory.entity_store",
    );
    if !dir.exists() {
        eprintln!("skip: {} missing", dir.display());
        return;
    }

    let mut gens: Vec<String> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            name.strip_suffix("-Data.db").map(|s| s.to_string())
        })
        .collect();
    gens.sort();

    for gen in &gens {
        eprintln!("\n=== SSTable {gen} ===");
        let reader = open_on_disk(&dir, gen);
        let file_len = reader.data_file_length().unwrap_or(0);
        eprintln!(
            "  key_count (from Partitions.db trie): {}",
            reader.key_count()
        );
        eprintln!("  data_file_length: {file_len}");

        let _ = reader; // close the reader; we reload raw bytes for diagnostics
        let data = fs::read(dir.join(format!("{gen}-Data.db"))).unwrap();
        let stats_bytes = fs::read(dir.join(format!("{gen}-Statistics.db"))).unwrap();
        let stats = read_statistics(&stats_bytes).expect("parse stats");
        let header = stats.header;
        let mut dr = DataReader::new(&data, &header, 0);
        let mut idx = 0;
        loop {
            let before = dr.position();
            match dr.read_partition() {
                Ok(Some(p)) => {
                    let after = dr.position();
                    eprintln!(
                        "  partition[{idx}]: start={before} end={after} delta={} rows={} key_len={}",
                        after - before,
                        p.rows.len(),
                        p.key.key.as_bytes().len(),
                    );
                    idx += 1;
                }
                Ok(None) => {
                    eprintln!(
                        "  reached end-of-stream cleanly at pos={} (file_len={})",
                        dr.position(),
                        data.len()
                    );
                    break;
                }
                Err(e) => {
                    eprintln!(
                        "  partition[{idx}] FAILED at pos={} (file_len={}): {e}",
                        dr.position(),
                        data.len()
                    );
                    // Dump surrounding bytes to aid diagnosis
                    let start = dr.position().saturating_sub(8) as usize;
                    let end = (dr.position() as usize + 24).min(data.len());
                    let window = &data[start..end];
                    eprintln!("    context bytes [{start}..{end}] = {:02x?}", window);
                    break;
                }
            }
        }
    }
}

#[test]
#[ignore]
fn replay_typed_edges_sstable_from_node1() {
    let dir = PathBuf::from(
        "/Users/bkearns/data/ferrosa-memory/node1/sstables/agent_memory.typed_edges",
    );
    if !dir.exists() {
        eprintln!("skip: {} missing", dir.display());
        return;
    }

    let mut gens: Vec<String> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            name.strip_suffix("-Data.db").map(|s| s.to_string())
        })
        .collect();
    gens.sort();

    for gen in &gens {
        eprintln!("\n=== SSTable {gen} ===");
        let reader = open_on_disk(&dir, gen);
        eprintln!("  key_count: {}", reader.key_count());
        match reader.read_all_partitions() {
            Ok(parts) => eprintln!("  read_all_partitions: OK, {} partitions", parts.len()),
            Err(e) => eprintln!("  read_all_partitions: FAILED with {e}"),
        }
    }
}
