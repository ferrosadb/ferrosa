//! P0 replay guard: bundled on-disk SSTable fixtures must read to EOF.
//!
//! These tests are intentionally not ignored. They cover the same reader paths
//! used for production SSTable replay without depending on a developer-local
//! data directory.

use std::fs;
use std::path::{Path, PathBuf};

use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

fn fixture_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn load(dir: &Path, suffix: &str) -> Vec<u8> {
    let path = dir.join(suffix);
    fs::read(&path).unwrap_or_else(|e| panic!("read fixture component {}: {e}", path.display()))
}

fn open_fixture(dir: &Path) -> SSTableReader<Vec<u8>> {
    let compression_info_path = dir.join("CompressionInfo.db");
    let compression_info = compression_info_path
        .exists()
        .then(|| fs::read(&compression_info_path).expect("read CompressionInfo.db"));
    let components = SSTableComponents {
        data: load(dir, "Data.db"),
        partitions: load(dir, "Partitions.db"),
        rows: load(dir, "Rows.db"),
        filter: load(dir, "Filter.db"),
        compression_info,
        statistics: load(dir, "Statistics.db"),
    };
    SSTableReader::open(components).expect("open SSTable fixture")
}

#[test]
fn replay_entity_store_fixture_sstable_end_to_end() {
    let dir = fixture_dir("multi_partition");
    let reader = open_fixture(&dir);

    let partitions = reader
        .read_all_partitions()
        .expect("reader replay must decode all partitions");
    assert_eq!(partitions.len() as u64, reader.key_count());
    assert!(!partitions.is_empty());
}

#[test]
fn replay_typed_edges_fixture_sstable_end_to_end() {
    let dir = fixture_dir("wide_partition");
    let reader = open_fixture(&dir);

    let partitions = reader
        .read_all_partitions()
        .expect("reader replay must decode all partitions");
    assert_eq!(partitions.len() as u64, reader.key_count());
    assert!(!partitions.is_empty());
}
