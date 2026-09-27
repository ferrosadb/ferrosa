//! Frozen oracle for `SSTableWriter`'s current output.
//!
//! # Why this drives the public API instead of copying private internals
//!
//! T-035 (`ferrosa-suite/specs/sstable-write-pump/compiled-project-plan.md`)
//! asks that this file freeze today's writer output as a test oracle before
//! T-037/T-038 rewrite how `writer.rs` produces `Data.db`. The plan's own
//! text floats a "verbatim copy" of `DataBuffer` / `build_data_file_to_file`;
//! we deliberately did NOT do that:
//!
//! - `DataBuffer`, `TrieBuilder` and `BloomFilter` are crate-private. A real
//!   verbatim copy would mean either duplicating several hundred lines of
//!   private serialization logic into `tests/`, or loosening `writer.rs`'s
//!   visibility purely so a test twin can see it — both raise maintenance
//!   burden (two copies to keep in sync) without adding protection: at
//!   *this* commit, "today's writer" and any hand-copy of it are
//!   byte-identical by construction, so a copy buys nothing here.
//! - The actual oracle value doesn't come from a frozen copy of the code —
//!   it comes from the checked-in golden corpus (`tests/golden/`, bytes
//!   captured today). Once T-037/T-038 change `writer.rs`, `legacy_write`
//!   below starts calling the *new* writer, and `oracle_golden_reproduction`
//!   (`tests/oracle.rs`) is what actually catches drift, by diffing its
//!   output against the bytes captured before the change.
//!
//! So `legacy_write` / `legacy_write_file_backed` simply drive
//! `SSTableWriter`'s two existing public entry points
//! (`new`/`finish` and `new_file_backed`/`finish_to_directory`) and capture
//! every component's bytes into one common shape. Comparing those two
//! entry points against each other is also a real regression class in its
//! own right (a pump/streaming rewrite could easily make one path diverge
//! from the other), which is exercised by `oracle_file_backed_matches_in_memory`.

use std::path::{Path, PathBuf};

use ferrosa_common::Result;
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::writer::{SSTableOutput, SSTableWriter, WriteOptions};

/// Every SSTable component's raw bytes, independent of whether they came
/// from the in-memory (`SSTableOutput`) or file-backed
/// (`SSTableOutputFiles::read_to_memory`) writer path.
#[derive(Debug, Clone)]
pub struct ComponentBytes {
    pub data: Vec<u8>,
    pub partitions: Vec<u8>,
    pub rows: Vec<u8>,
    pub filter: Vec<u8>,
    pub compression_info: Option<Vec<u8>>,
    pub statistics: Vec<u8>,
    /// `Digest.crc32` content (decimal ASCII CRC32 of the on-disk Data.db
    /// bytes). Present for every table (T-011). Folded into the golden
    /// corpus's byte-exact and manifest comparisons (T-038 item 8) so a
    /// rewrite of how Data.db is produced cannot silently change the
    /// checksum computed over it without failing the oracle.
    pub digest: Vec<u8>,
    /// `CRC.db` content (per-chunk CRC32 table). `Some` only for
    /// uncompressed tables (T-011).
    pub crc: Option<Vec<u8>>,
    pub toc: Vec<u8>,
}

impl From<SSTableOutput> for ComponentBytes {
    fn from(output: SSTableOutput) -> Self {
        ComponentBytes {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
            digest: output.digest,
            crc: output.crc,
            toc: output.toc,
        }
    }
}

impl ComponentBytes {
    /// `(component file name, bytes)` pairs, in TOC order. `CompressionInfo.db`
    /// is included only when compression produced one; `CRC.db` only when the
    /// table is uncompressed. `Digest.crc32` is always present (T-011).
    pub fn named_components(&self) -> Vec<(&'static str, &[u8])> {
        let mut named: Vec<(&'static str, &[u8])> = vec![
            ("Data.db", self.data.as_slice()),
            ("Partitions.db", self.partitions.as_slice()),
            ("Rows.db", self.rows.as_slice()),
            ("Filter.db", self.filter.as_slice()),
            ("Statistics.db", self.statistics.as_slice()),
            ("Digest.crc32", self.digest.as_slice()),
            ("TOC.txt", self.toc.as_slice()),
        ];
        if let Some(info) = self.compression_info.as_deref() {
            named.push(("CompressionInfo.db", info));
        }
        if let Some(crc) = self.crc.as_deref() {
            named.push(("CRC.db", crc));
        }
        named
    }
}

/// Drive the in-memory writer path: `SSTableWriter::new` + `add_partition`
/// (in order) + `finish`.
pub fn legacy_write(
    partitions: &[Partition],
    header: &SerializationHeader,
    options: WriteOptions,
) -> Result<ComponentBytes> {
    let mut writer = SSTableWriter::new(options, header.clone());
    for partition in partitions {
        writer.add_partition(partition)?;
    }
    Ok(writer.finish()?.into())
}

/// Drive the file-backed writer path: `SSTableWriter::new_file_backed` +
/// `add_partition` + `finish_to_directory`, then read every component back
/// into memory (`SSTableOutputFiles::read_to_memory`, which also removes
/// `staging_dir` — mirroring the production cleanup) for comparison against
/// the in-memory path.
pub fn legacy_write_file_backed(
    partitions: &[Partition],
    header: &SerializationHeader,
    options: WriteOptions,
    staging_dir: impl AsRef<Path>,
    raw_data_path: impl Into<PathBuf>,
) -> Result<ComponentBytes> {
    let mut writer = SSTableWriter::new_file_backed(options, header.clone(), raw_data_path)?;
    for partition in partitions {
        writer.add_partition(partition)?;
    }
    let files = writer.finish_to_directory(staging_dir)?;
    Ok(files.read_to_memory()?.into())
}

/// Compares every component and panics naming the first differing component
/// and the first differing byte offset within it (or the length mismatch,
/// when lengths themselves differ).
pub fn assert_components_identical(a: &ComponentBytes, b: &ComponentBytes, context: &str) {
    let a_components = a.named_components();
    let b_components = b.named_components();
    let a_names: Vec<&str> = a_components.iter().map(|(name, _)| *name).collect();
    let b_names: Vec<&str> = b_components.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        a_names, b_names,
        "{context}: component sets differ (a compression-presence mismatch?)"
    );

    for ((name, a_bytes), (_, b_bytes)) in a_components.iter().zip(b_components.iter()) {
        if a_bytes == b_bytes {
            continue;
        }
        if a_bytes.len() != b_bytes.len() {
            panic!(
                "{context}: component {name} differs in length: {} vs {} bytes",
                a_bytes.len(),
                b_bytes.len()
            );
        }
        let offset = a_bytes
            .iter()
            .zip(b_bytes.iter())
            .position(|(x, y)| x != y)
            .expect("equal-length byte slices that are != must have a differing index");
        panic!(
            "{context}: component {name} first differs at byte offset {offset}: \
             {:#04x} vs {:#04x}",
            a_bytes[offset], b_bytes[offset]
        );
    }
}
