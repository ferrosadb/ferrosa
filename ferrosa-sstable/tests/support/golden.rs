//! The golden SSTable corpus: `CASES` names every case, `build_case`
//! deterministically reconstructs each case's `(header, partitions)` from
//! nothing but its recorded seed and kind, and the read/write helpers below
//! move `ComponentBytes` to and from `tests/golden/<case name>/`.
//!
//! Regeneration lives in `tests/golden_regen.rs`, gated by
//! `FERROSA_REGEN_GOLDEN=1`. Reproduction is checked by
//! `oracle_golden_reproduction` in `tests/oracle.rs`, which never writes to
//! this directory.

use std::path::PathBuf;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::compression::Compression;
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::WriteOptions;
use sha2::{Digest, Sha256};

use super::generators::{arb_partitions, arb_schema, sample, ColumnKind, Schema, SizeProfile};
use super::legacy_writer::ComponentBytes;

/// Which compression codec (if any) a case uses. Kept separate from
/// `ferrosa_sstable::compression::Compression` so `CaseSpec` can be a `const`
/// table (`Compression` has no `const fn` constructors).
#[derive(Debug, Clone, Copy)]
pub enum CompressionSpec {
    Lz4,
    Zstd(i32),
}

impl CompressionSpec {
    pub fn to_compression(self) -> Compression {
        match self {
            CompressionSpec::Lz4 => Compression::Lz4,
            CompressionSpec::Zstd(level) => Compression::Zstd { level },
        }
    }

    pub fn label(self) -> String {
        match self {
            CompressionSpec::Lz4 => "lz4".to_string(),
            CompressionSpec::Zstd(level) => format!("zstd:{level}"),
        }
    }
}

/// How a case's `(header, partitions)` are built. `Random` covers the bulk
/// of the compression/chunk-size/file-backed matrix; the other three
/// variants guarantee specific boundary shapes land in the corpus rather
/// than being left to a random seed's luck.
#[derive(Debug, Clone, Copy)]
pub enum CaseKind {
    /// Zero partitions (L1 U10: whatever the writer does with none).
    Empty,
    /// Random schema + partitions from `SizeProfile::corpus()`.
    Random,
    /// One partition, one row, one ~256 KiB value.
    LargeRow,
    /// One partition with many small rows, meant to be paired with a small
    /// `chunk_size` so the compressed output straddles several chunks.
    ChunkStraddle,
}

/// One golden-corpus case: everything needed to deterministically rebuild
/// its input and to know where its bytes live on disk.
#[derive(Debug, Clone, Copy)]
pub struct CaseSpec {
    pub name: &'static str,
    pub seed: u64,
    pub compression: Option<CompressionSpec>,
    pub chunk_size: usize,
    pub file_backed: bool,
    pub kind: CaseKind,
}

pub fn write_options(spec: &CaseSpec) -> WriteOptions {
    WriteOptions {
        compression: spec.compression.map(CompressionSpec::to_compression),
        bloom_fp_chance: 0.01,
        chunk_size: spec.chunk_size,
        verify_output: true,
    }
}

/// Deterministically rebuilds `(header, partitions)` for a case. Called both
/// when regenerating the corpus and when checking that today's writer still
/// reproduces it — the only difference between those two call sites is
/// whether the resulting bytes get written to disk or compared against what
/// is already there.
pub fn build_case(spec: &CaseSpec) -> (SerializationHeader, Vec<Partition>) {
    match spec.kind {
        CaseKind::Empty => {
            let schema = Schema {
                clustering: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnKind::Utf8],
                complex_collections: false,
                min_timestamp: 0,
            };
            (schema.header(), Vec::new())
        }
        CaseKind::Random => {
            let profile = SizeProfile::corpus();
            let schema = sample(spec.seed, arb_schema(profile));
            // A different seed for the partitions than for the schema, so
            // the two draws don't reuse the same RNG output.
            let partitions = sample(
                spec.seed ^ 0xA5A5_A5A5_A5A5_A5A5,
                arb_partitions(schema.clone(), profile),
            );
            (schema.header(), partitions)
        }
        CaseKind::LargeRow => {
            let schema = Schema {
                clustering: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnKind::Bytes],
                complex_collections: false,
                min_timestamp: 0,
            };
            let value = vec![0xABu8; 256 * 1024];
            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::new(b"large-row".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(value, 1))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1),
                }],
            };
            (schema.header(), vec![partition])
        }
        CaseKind::ChunkStraddle => {
            let schema = Schema {
                clustering: vec![ColumnKind::Int32],
                static_columns: vec![],
                regular_columns: vec![ColumnKind::Bytes],
                complex_collections: false,
                min_timestamp: 0,
            };
            let rows = (0..400i32)
                .map(|i| Row {
                    clustering: i.to_be_bytes().to_vec(),
                    cells: vec![(
                        0,
                        CellValue::live(vec![(i % 251) as u8; 200], (i as i64) + 1),
                    )],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp((i as i64) + 1),
                })
                .collect();
            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::new(b"straddle".to_vec())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows,
            };
            (schema.header(), vec![partition])
        }
    }
}

/// ~20 small SSTables spanning: uncompressed and every supported codec
/// (LZ4, Zstd), chunk sizes 4K/16K/64K, file-backed and in-memory writer
/// paths, plus the deliberate large-row and chunk-straddling boundary cases.
pub const CASES: &[CaseSpec] = &[
    CaseSpec {
        name: "empty_uncompressed_in_memory",
        seed: 0,
        compression: None,
        chunk_size: 65536,
        file_backed: false,
        kind: CaseKind::Empty,
    },
    CaseSpec {
        name: "random_uncompressed_in_memory_1",
        seed: 1,
        compression: None,
        chunk_size: 65536,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_uncompressed_file_backed_2",
        seed: 2,
        compression: None,
        chunk_size: 65536,
        file_backed: true,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_lz4_chunk4096_in_memory_3",
        seed: 3,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 4096,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_lz4_chunk4096_file_backed_4",
        seed: 4,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 4096,
        file_backed: true,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_lz4_chunk16384_in_memory_5",
        seed: 5,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 16384,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_lz4_chunk16384_file_backed_6",
        seed: 6,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 16384,
        file_backed: true,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_lz4_chunk65536_in_memory_7",
        seed: 7,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 65536,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_zstd1_chunk4096_in_memory_8",
        seed: 8,
        compression: Some(CompressionSpec::Zstd(1)),
        chunk_size: 4096,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_zstd1_chunk16384_file_backed_9",
        seed: 9,
        compression: Some(CompressionSpec::Zstd(1)),
        chunk_size: 16384,
        file_backed: true,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_zstd19_chunk65536_in_memory_10",
        seed: 10,
        compression: Some(CompressionSpec::Zstd(19)),
        chunk_size: 65536,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_zstd19_chunk4096_file_backed_11",
        seed: 11,
        compression: Some(CompressionSpec::Zstd(19)),
        chunk_size: 4096,
        file_backed: true,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_uncompressed_in_memory_12",
        seed: 12,
        compression: None,
        chunk_size: 65536,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_uncompressed_file_backed_13",
        seed: 13,
        compression: None,
        chunk_size: 65536,
        file_backed: true,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_lz4_chunk16384_in_memory_14",
        seed: 14,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 16384,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_zstd1_chunk65536_file_backed_15",
        seed: 15,
        compression: Some(CompressionSpec::Zstd(1)),
        chunk_size: 65536,
        file_backed: true,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_lz4_chunk4096_in_memory_16",
        seed: 16,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 4096,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "random_uncompressed_in_memory_17",
        seed: 17,
        compression: None,
        chunk_size: 65536,
        file_backed: false,
        kind: CaseKind::Random,
    },
    CaseSpec {
        name: "large_row_lz4_chunk4096_in_memory",
        seed: 100,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 4096,
        file_backed: false,
        kind: CaseKind::LargeRow,
    },
    CaseSpec {
        name: "large_row_uncompressed_file_backed",
        seed: 101,
        compression: None,
        chunk_size: 65536,
        file_backed: true,
        kind: CaseKind::LargeRow,
    },
    CaseSpec {
        name: "chunk_straddle_lz4_chunk4096_file_backed",
        seed: 200,
        compression: Some(CompressionSpec::Lz4),
        chunk_size: 4096,
        file_backed: true,
        kind: CaseKind::ChunkStraddle,
    },
];

pub fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
}

pub fn case_dir(name: &str) -> PathBuf {
    golden_dir().join(name)
}

pub fn manifest_path() -> PathBuf {
    golden_dir().join("manifest.txt")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Reads a case's components back off disk into a `ComponentBytes`, for
/// comparison against a freshly-written one.
pub fn read_case_from_disk(name: &str, has_compression: bool) -> ComponentBytes {
    let dir = case_dir(name);
    let read = |file: &str| {
        std::fs::read(dir.join(file)).unwrap_or_else(|e| panic!("read {file} for case {name}: {e}"))
    };
    ComponentBytes {
        data: read("Data.db"),
        partitions: read("Partitions.db"),
        rows: read("Rows.db"),
        filter: read("Filter.db"),
        compression_info: if has_compression {
            Some(read("CompressionInfo.db"))
        } else {
            None
        },
        statistics: read("Statistics.db"),
        digest: read("Digest.crc32"),
        crc: if has_compression {
            None
        } else {
            Some(read("CRC.db"))
        },
        toc: read("TOC.txt"),
    }
}

/// Writes a case's components to disk. Only called from `golden_regen.rs`,
/// never from `oracle.rs`.
pub fn write_case_to_disk(name: &str, bytes: &ComponentBytes) {
    let dir = case_dir(name);
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("mkdir {}: {e}", dir.display()));
    for (component, data) in bytes.named_components() {
        std::fs::write(dir.join(component), data)
            .unwrap_or_else(|e| panic!("write {component} for case {name}: {e}"));
    }
}

/// Renders one case's manifest block: its recorded inputs plus a SHA-256 per
/// component, as a human-readable fingerprint independent of the byte-exact
/// comparison `assert_components_identical` already performs.
pub fn format_manifest_entry(spec: &CaseSpec, digests: &[(&str, String)]) -> String {
    let mut out = String::new();
    out.push_str(&format!("case: {}\n", spec.name));
    out.push_str(&format!("seed: {}\n", spec.seed));
    out.push_str(&format!("kind: {:?}\n", spec.kind));
    out.push_str(&format!(
        "compression: {}\n",
        spec.compression
            .map(CompressionSpec::label)
            .unwrap_or_else(|| "none".to_string())
    ));
    out.push_str(&format!("chunk_size: {}\n", spec.chunk_size));
    out.push_str(&format!("file_backed: {}\n", spec.file_backed));
    for (component, digest) in digests {
        out.push_str(&format!("sha256 {component}: {digest}\n"));
    }
    out.push_str("---\n");
    out
}
