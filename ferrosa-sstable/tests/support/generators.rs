//! Proptest generators for synthetic `(schema, WriteOptions, partitions)`
//! SSTable inputs.
//!
//! Covers the space `test-specification.md` L2 asks for: 0-5 clustering
//! columns (fixed- and variable-length CQL types), static rows,
//! row/partition deletions, TTL/expiring cells, simple and complex
//! (non-frozen collection) columns, empty values, and row bodies from 0 B up
//! to ~256 KiB. Partitions straddling a compression chunk boundary follow
//! from combining a small `chunk_size` with these partitions; deliberate,
//! not-left-to-chance coverage of that plus the 256 KiB extreme lives in
//! `support::golden::CaseKind::{LargeRow, ChunkStraddle}`.
//!
//! `ferrosa_common::test_generators` documents that `Row`/`Partition`
//! generators belong in consuming crates because they depend on
//! `ferrosa-sstable` types — this module is that consumer.

use std::fmt;

use ferrosa_common::test_generators::arb_decorated_key;
use ferrosa_common::CellValue;
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::WriteOptions;
use ferrosa_sstable::Compression;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

// ---------------------------------------------------------------------------
// Column/type vocabulary
// ---------------------------------------------------------------------------

/// A CQL column type this generator knows how to encode, tagged with what
/// the writer needs to know about it (fixed length, complex/multicell).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Int32,
    Long,
    Uuid,
    Utf8,
    Bytes,
    ListUtf8,
    SetUtf8,
    MapUtf8Utf8,
}

impl ColumnKind {
    pub fn type_name(self) -> &'static str {
        match self {
            ColumnKind::Int32 => "org.apache.cassandra.db.marshal.Int32Type",
            ColumnKind::Long => "org.apache.cassandra.db.marshal.LongType",
            ColumnKind::Uuid => "org.apache.cassandra.db.marshal.UUIDType",
            ColumnKind::Utf8 => "org.apache.cassandra.db.marshal.UTF8Type",
            ColumnKind::Bytes => "org.apache.cassandra.db.marshal.BytesType",
            ColumnKind::ListUtf8 => {
                "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)"
            }
            ColumnKind::SetUtf8 => {
                "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
            }
            ColumnKind::MapUtf8Utf8 => {
                "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,\
                 org.apache.cassandra.db.marshal.UTF8Type)"
            }
        }
    }

    /// Mirrors `ferrosa_sstable::marshal::value_length_if_fixed`.
    pub fn fixed_len(self) -> Option<usize> {
        match self {
            ColumnKind::Int32 => Some(4),
            ColumnKind::Long => Some(8),
            ColumnKind::Uuid => Some(16),
            _ => None,
        }
    }

    pub fn is_complex(self) -> bool {
        matches!(
            self,
            ColumnKind::ListUtf8 | ColumnKind::SetUtf8 | ColumnKind::MapUtf8Utf8
        )
    }
}

const CLUSTERING_KINDS: &[ColumnKind] = &[
    ColumnKind::Int32,
    ColumnKind::Long,
    ColumnKind::Uuid,
    ColumnKind::Utf8,
    ColumnKind::Bytes,
];
const SIMPLE_COLUMN_KINDS: &[ColumnKind] = &[
    ColumnKind::Int32,
    ColumnKind::Long,
    ColumnKind::Uuid,
    ColumnKind::Utf8,
    ColumnKind::Bytes,
];
const COMPLEX_COLUMN_KINDS: &[ColumnKind] = &[
    ColumnKind::ListUtf8,
    ColumnKind::SetUtf8,
    ColumnKind::MapUtf8Utf8,
];

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// A synthetic table shape: enough to build a `SerializationHeader` and to
/// know how to encode clustering keys and cells that match it.
#[derive(Debug, Clone)]
pub struct Schema {
    pub clustering: Vec<ColumnKind>,
    pub static_columns: Vec<ColumnKind>,
    pub regular_columns: Vec<ColumnKind>,
    pub complex_collections: bool,
    pub min_timestamp: i64,
}

impl Schema {
    pub fn header(&self) -> SerializationHeader {
        SerializationHeader {
            complex_collections: self.complex_collections,
            min_timestamp: self.min_timestamp,
            min_local_deletion_time: 0,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_types: self
                .clustering
                .iter()
                .map(|k| k.type_name().to_string())
                .collect(),
            static_columns: self
                .static_columns
                .iter()
                .enumerate()
                .map(|(i, k)| (format!("s{i}").into_bytes(), k.type_name().to_string()))
                .collect(),
            regular_columns: self
                .regular_columns
                .iter()
                .enumerate()
                .map(|(i, k)| (format!("r{i}").into_bytes(), k.type_name().to_string()))
                .collect(),
        }
    }
}

/// One (schema, options, partitions) input to the writer.
#[derive(Debug, Clone)]
pub struct Case {
    pub schema: Schema,
    pub options: WriteOptions,
    pub partitions: Vec<Partition>,
}

// ---------------------------------------------------------------------------
// Size tuning
// ---------------------------------------------------------------------------

/// Controls how large generated partitions/rows/values get. `corpus()` stays
/// tiny (the golden corpus must stay well under 2 MiB in total); `property()`
/// occasionally reaches into the tens/hundreds of KiB so the 1000-case
/// property test exercises large row bodies too, without every case paying
/// for it (the large branch is low-probability).
#[derive(Debug, Clone, Copy)]
pub struct SizeProfile {
    pub max_partitions: usize,
    pub max_rows_per_partition: usize,
    pub max_static_columns: usize,
    pub max_regular_columns: usize,
    pub small_value_max: usize,
    pub large_value_range: (usize, usize),
    pub small_weight: u32,
    pub large_weight: u32,
}

impl SizeProfile {
    pub fn corpus() -> Self {
        SizeProfile {
            max_partitions: 5,
            max_rows_per_partition: 5,
            max_static_columns: 2,
            max_regular_columns: 4,
            small_value_max: 48,
            large_value_range: (128, 1024),
            small_weight: 20,
            large_weight: 1,
        }
    }

    pub fn property() -> Self {
        SizeProfile {
            max_partitions: 4,
            max_rows_per_partition: 4,
            max_static_columns: 2,
            max_regular_columns: 4,
            small_value_max: 512,
            large_value_range: (4096, 256 * 1024),
            small_weight: 12,
            large_weight: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Small combinators
// ---------------------------------------------------------------------------

/// Uniformly picks one element out of an owned `Vec`, cloning it into the
/// generated value. Used instead of `proptest::sample::select` so the
/// element type only needs `Clone + Debug`, not a specific `select`-bound
/// container type.
fn pick_vec<T: Clone + fmt::Debug + 'static>(options: Vec<T>) -> impl Strategy<Value = T> {
    (0..options.len()).prop_map(move |i| options[i].clone())
}

/// Combines a `Vec` of independently-boxed strategies (of possibly different
/// underlying strategy types, all producing the same `Value`) into one
/// strategy producing a `Vec<T>` — the heterogeneous-arity analogue of
/// `prop::collection::vec`, needed because each column's cell strategy
/// differs by column kind/index.
fn combine<T: Clone + fmt::Debug + 'static>(
    strategies: Vec<BoxedStrategy<T>>,
) -> BoxedStrategy<Vec<T>> {
    strategies
        .into_iter()
        .fold(Just(Vec::new()).boxed(), |acc, s| {
            (acc, s)
                .prop_map(|(mut v, item)| {
                    v.push(item);
                    v
                })
                .boxed()
        })
}

// ---------------------------------------------------------------------------
// Values, cells, deletions, liveness
// ---------------------------------------------------------------------------

fn arb_value_bytes(profile: SizeProfile) -> impl Strategy<Value = Vec<u8>> {
    let (lo, hi) = profile.large_value_range;
    prop_oneof![
        profile.small_weight => prop::collection::vec(any::<u8>(), 0..=profile.small_value_max),
        profile.large_weight => prop::collection::vec(any::<u8>(), lo..hi),
    ]
}

/// Value bytes for a cell, respecting the column's fixed width if it has
/// one: `serialize_cell` hard-asserts a fixed-width simple column's non-empty
/// value is exactly `fixed_len` bytes (variable-length columns, and every
/// complex-column element value, have no such constraint). An empty value is
/// always legal regardless of width — that is how "empty values" for a
/// fixed-width column are represented.
fn arb_value_bytes_for(fixed_len: Option<usize>, profile: SizeProfile) -> BoxedStrategy<Vec<u8>> {
    match fixed_len {
        Some(n) => prop_oneof![
            1 => Just(Vec::new()),
            6 => prop::collection::vec(any::<u8>(), n..=n),
        ]
        .boxed(),
        None => arb_value_bytes(profile).boxed(),
    }
}

fn arb_deletion(min_ts: i64, live_weight: u32) -> impl Strategy<Value = DeletionTime> {
    prop_oneof![
        live_weight => Just(DeletionTime::LIVE),
        1 => ((min_ts + 1)..(min_ts + 500_000), 0i32..2_000_000)
            .prop_map(|(ts, ldt)| DeletionTime::new(ts, ldt as u32)),
    ]
}

fn arb_liveness(min_ts: i64) -> impl Strategy<Value = LivenessInfo> {
    prop_oneof![
        2 => Just(LivenessInfo::NONE),
        5 => ((min_ts + 1)..(min_ts + 500_000)).prop_map(LivenessInfo::with_timestamp),
        3 => ((min_ts + 1)..(min_ts + 500_000), 1i32..86400, 0i32..2_000_000)
            .prop_map(|(ts, ttl, ldt)| LivenessInfo::with_ttl(ts, ttl, ldt)),
    ]
}

/// A simple (non-complex) cell: live, tombstone, or expiring. `fixed_len` is
/// the column's fixed width, if it has one (`None` for a variable-length
/// column or a complex-column element, whose value type is always
/// variable-length UTF8Type here).
fn arb_simple_cell(
    min_ts: i64,
    profile: SizeProfile,
    fixed_len: Option<usize>,
) -> impl Strategy<Value = CellValue> {
    prop_oneof![
        5 => (arb_value_bytes_for(fixed_len, profile), (min_ts + 1)..(min_ts + 500_000))
            .prop_map(|(v, ts)| CellValue::live(v, ts)),
        2 => ((min_ts + 1)..(min_ts + 500_000), 0i32..2_000_000)
            .prop_map(|(ts, ldt)| CellValue::tombstone(ts, ldt)),
        3 => (
            arb_value_bytes_for(fixed_len, profile),
            (min_ts + 1)..(min_ts + 500_000),
            1i32..86400,
            0i32..2_000_000
        )
            .prop_map(|(v, ts, ttl, ldt)| CellValue::expiring(v, ts, ttl, ldt)),
    ]
}

/// One element cell of a complex (non-frozen collection) column.
/// `path_index` becomes the cell's whole path: a single distinguishing byte
/// is sufficient for uniqueness (the writer sorts elements by path but does
/// not require any particular byte layout), and keeps generation order
/// already sorted so no extra reordering step is needed.
fn arb_complex_element_cell(
    min_ts: i64,
    profile: SizeProfile,
    path_index: u8,
) -> impl Strategy<Value = CellValue> {
    arb_simple_cell(min_ts, profile, None).prop_map(move |cell| cell.with_path(vec![path_index]))
}

/// Cells for one column of a row: for a complex column (only possible when
/// `complex_collections` is set on the schema and the kind is a collection),
/// 0-6 element cells sharing this column's index; otherwise the column is
/// either missing from the row or carries exactly one simple cell.
fn arb_column_cells(
    idx: u16,
    kind: ColumnKind,
    complex_collections: bool,
    min_ts: i64,
    profile: SizeProfile,
) -> BoxedStrategy<Vec<(u16, CellValue)>> {
    if complex_collections && kind.is_complex() {
        (0usize..=6)
            .prop_flat_map(move |n| {
                let elements: Vec<BoxedStrategy<CellValue>> = (0..n)
                    .map(|i| arb_complex_element_cell(min_ts, profile, i as u8).boxed())
                    .collect();
                combine(elements)
            })
            .prop_map(move |cells| cells.into_iter().map(|c| (idx, c)).collect())
            .boxed()
    } else {
        let fixed_len = kind.fixed_len();
        prop_oneof![
            1 => Just(Vec::<(u16, CellValue)>::new()),
            4 => arb_simple_cell(min_ts, profile, fixed_len).prop_map(move |c| vec![(idx, c)]),
        ]
        .boxed()
    }
}

/// All cells for a row, built column-by-column in ascending index order so
/// the writer's "cells sorted by col_idx" invariant holds by construction.
fn arb_row_cells(
    columns: &[ColumnKind],
    complex_collections: bool,
    min_ts: i64,
    profile: SizeProfile,
) -> BoxedStrategy<Vec<(u16, CellValue)>> {
    let per_column: Vec<BoxedStrategy<Vec<(u16, CellValue)>>> = columns
        .iter()
        .enumerate()
        .map(|(i, kind)| arb_column_cells(i as u16, *kind, complex_collections, min_ts, profile))
        .collect();
    combine(per_column)
        .prop_map(|groups| groups.into_iter().flatten().collect())
        .boxed()
}

// ---------------------------------------------------------------------------
// Clustering
// ---------------------------------------------------------------------------

fn arb_clustering_component(kind: ColumnKind) -> BoxedStrategy<Vec<u8>> {
    match kind.fixed_len() {
        Some(n) => prop::collection::vec(any::<u8>(), n..=n).boxed(),
        // Non-empty: required by `validate_clustering_shape` for the
        // single-clustering-column case, and harmless for the composite case.
        None => prop::collection::vec(any::<u8>(), 1..48).boxed(),
    }
}

/// Matches `SSTableWriter::serialize_row`'s clustering encoding: raw bytes
/// for a single clustering column, u16-length-prefixed composite for 2+.
fn encode_clustering(components: &[Vec<u8>]) -> Vec<u8> {
    if components.len() <= 1 {
        components.first().cloned().unwrap_or_default()
    } else {
        let mut out = Vec::new();
        for component in components {
            out.extend_from_slice(&(component.len() as u16).to_be_bytes());
            out.extend_from_slice(component);
        }
        out
    }
}

fn arb_clustering(clustering: &[ColumnKind]) -> BoxedStrategy<Vec<u8>> {
    if clustering.is_empty() {
        return Just(Vec::new()).boxed();
    }
    let per_column: Vec<BoxedStrategy<Vec<u8>>> = clustering
        .iter()
        .map(|kind| arb_clustering_component(*kind))
        .collect();
    combine(per_column)
        .prop_map(|components| encode_clustering(&components))
        .boxed()
}

// ---------------------------------------------------------------------------
// Rows, partitions, schema, case
// ---------------------------------------------------------------------------

fn arb_row(schema: &Schema, is_static: bool, profile: SizeProfile) -> BoxedStrategy<Row> {
    let columns = if is_static {
        schema.static_columns.clone()
    } else {
        schema.regular_columns.clone()
    };
    let min_ts = schema.min_timestamp;
    let complex_collections = schema.complex_collections;
    let clustering_strategy: BoxedStrategy<Vec<u8>> = if is_static {
        Just(Vec::new()).boxed()
    } else {
        arb_clustering(&schema.clustering)
    };

    (
        clustering_strategy,
        arb_row_cells(&columns, complex_collections, min_ts, profile),
        arb_deletion(min_ts, 9),
        arb_liveness(min_ts),
    )
        .prop_map(|(clustering, cells, deletion, primary_key_liveness)| Row {
            clustering,
            cells,
            deletion,
            primary_key_liveness,
        })
        .boxed()
}

fn arb_partition(schema: Schema, profile: SizeProfile) -> BoxedStrategy<Partition> {
    let has_static = !schema.static_columns.is_empty();
    let static_strategy: BoxedStrategy<Option<Row>> = if has_static {
        proptest::option::of(arb_row(&schema, true, profile)).boxed()
    } else {
        Just(None).boxed()
    };
    let rows_strategy = prop::collection::vec(
        arb_row(&schema, false, profile),
        0..=profile.max_rows_per_partition,
    );
    let partition_deletion = arb_deletion(schema.min_timestamp, 9);

    // Reuses the shared `ferrosa-common` proptest generator for the
    // partition key itself (`arb_decorated_key`, gated behind the
    // `test-generators` feature) rather than hand-rolling one; the token it
    // computes is exactly what `add_partition` requires callers to insert in
    // order, which the outer `arb_partitions` sort enforces.
    (
        arb_decorated_key(),
        partition_deletion,
        static_strategy,
        rows_strategy,
    )
        .prop_map(|(key, deletion, static_row, mut rows)| {
            // Rows within a partition must be in clustering order with
            // distinct clustering keys, matching what a real merge iterator
            // would hand the writer.
            rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));
            rows.dedup_by(|a, b| a.clustering == b.clustering);
            Partition {
                key,
                deletion,
                static_row,
                rows,
            }
        })
        .boxed()
}

/// Regular/static column pool for a schema: simple types only, plus the
/// complex (collection) kinds when `complex_collections` is enabled — never
/// a complex kind on a schema that doesn't opt into complex-cell encoding
/// (the writer would otherwise treat it as simple and reject multi-cell
/// columns).
fn column_pool(complex_collections: bool) -> Vec<ColumnKind> {
    let mut pool = SIMPLE_COLUMN_KINDS.to_vec();
    if complex_collections {
        pool.extend_from_slice(COMPLEX_COLUMN_KINDS);
    }
    pool
}

pub fn arb_schema(profile: SizeProfile) -> impl Strategy<Value = Schema> {
    (
        prop::collection::vec(pick_vec(CLUSTERING_KINDS.to_vec()), 0..=5),
        any::<bool>(),
        0usize..=profile.max_static_columns,
        0usize..=profile.max_regular_columns,
        0i64..1000i64,
    )
        .prop_flat_map(
            move |(clustering, complex_collections, num_static, num_regular, min_ts)| {
                let pool = column_pool(complex_collections);
                (
                    prop::collection::vec(pick_vec(pool.clone()), num_static),
                    prop::collection::vec(pick_vec(pool), num_regular),
                )
                    .prop_map(move |(static_columns, regular_columns)| Schema {
                        clustering: clustering.clone(),
                        static_columns,
                        regular_columns,
                        complex_collections,
                        min_timestamp: min_ts,
                    })
            },
        )
}

pub fn arb_partitions(
    schema: Schema,
    profile: SizeProfile,
) -> impl Strategy<Value = Vec<Partition>> {
    prop::collection::vec(arb_partition(schema, profile), 0..=profile.max_partitions).prop_map(
        |mut partitions| {
            // Partitions must be added to the writer in token order with
            // distinct keys.
            partitions.sort_by(|a, b| {
                a.key
                    .token
                    .0
                    .cmp(&b.key.token.0)
                    .then_with(|| a.key.key.as_bytes().cmp(b.key.key.as_bytes()))
            });
            partitions.dedup_by(|a, b| a.key.key == b.key.key);
            partitions
        },
    )
}

fn arb_write_options() -> impl Strategy<Value = WriteOptions> {
    (
        prop_oneof![
            3 => Just(None),
            3 => Just(Some(Compression::Lz4)),
            2 => (1i32..=19).prop_map(|level| Some(Compression::Zstd { level })),
        ],
        pick_vec(vec![4096usize, 16384, 65536]),
    )
        .prop_map(|(compression, chunk_size)| WriteOptions {
            compression,
            bloom_fp_chance: 0.01,
            chunk_size,
            verify_output: true,
        })
}

/// A full random `(schema, options, partitions)` case.
pub fn arb_case(profile: SizeProfile) -> impl Strategy<Value = Case> {
    (arb_schema(profile), arb_write_options()).prop_flat_map(move |(schema, options)| {
        let schema_for_case = schema.clone();
        let options_for_case = options.clone();
        arb_partitions(schema, profile).prop_map(move |partitions| Case {
            schema: schema_for_case.clone(),
            options: options_for_case.clone(),
            partitions,
        })
    })
}

/// Deterministically samples one value from `strategy`, seeded from `seed`.
/// Used by the golden corpus (`support::golden`) so a case's bytes can be
/// reproduced byte-for-byte from nothing but its recorded seed, without
/// depending on proptest's own regression-file persistence.
pub fn sample<S: Strategy>(seed: u64, strategy: S) -> S::Value {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[0..8].copy_from_slice(&seed.to_le_bytes());
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    strategy
        .new_tree(&mut runner)
        .expect("strategy generation must not fail")
        .current()
}
