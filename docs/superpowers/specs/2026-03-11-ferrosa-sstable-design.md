# ferrosa-sstable Design

> **Date:** 2026-03-11
> **Status:** Approved — Phase 1 leaf components (Part A) complete, Part B pending
> **Approach:** Bottom-up by component, strict TDD (red-green-refactor)
> **Methodology:** Literate programming (swdev) — module docs, doc-tests, property tests

## Goal

Implement the `ferrosa-sstable` crate: read and write Cassandra-compatible BTI (Big Trie-Indexed) SSTables.

The formal format specification lives at `specs/sstable.md`. This document covers implementation design — module structure, dependencies, test strategy, and fixture generation.

## Crate Structure

```
ferrosa-sstable/
  Cargo.toml              # ferrosa-common, lz4_flex, zstd
  src/
    lib.rs                # Public API re-exports
    io.rs                 # ReadAt / WriteAt traits + FileReadAt / FileWriteAt
    varint.rs             # Unsigned/signed varint encoding
    compression.rs        # LZ4/Zstd compress/decompress, CompressionInfo read/write
    bloom.rs              # Bloom filter read/write, Cassandra-compatible double-hashing
    trie/
      mod.rs              # Re-exports
      node.rs             # 16 node types, encoding/decoding
      walker.rs           # Trie traversal (lookup, floor, ceiling, iteration)
      builder.rs          # Bottom-up page-aware incremental trie builder
    byte_comparable.rs    # OSS50 byte-comparable key encoding/decoding
    partition_index.rs    # Partitions.db reader/writer (trie + footer + payload)
    row_index.rs          # Rows.db reader/writer (per-partition tries)
    types.rs              # Row, Partition, PartitionIterator, DeletionTime
    data.rs               # Data.db reading (partition deserialization)
    statistics.rs         # Statistics.db reader/writer
    toc.rs                # TOC.txt reader/writer
    reader.rs             # SSTableReader — composes all components
    writer.rs             # SSTableWriter — composes all components
  tests/
    fixtures/             # Cassandra-generated BTI SSTable files
```

### External Dependencies

| Crate | Purpose |
|-------|---------|
| `ferrosa-common` | Token, DecoratedKey, CellValue, Error/Result, Murmur3 |
| `lz4_flex` | LZ4 compression (default) |
| `zstd` | Zstd compression |
| `proptest` (dev) | Property-based testing |
| `tempfile` (dev) | Temporary files for I/O tests |

No async runtime. All I/O through synchronous `ReadAt`/`WriteAt` traits. Async wrappers (S3) live in `ferrosa-storage`.

## SSTable-Specific Types

These types live in `ferrosa-sstable` (not `ferrosa-common`) because they are format-specific deserialized views:

```rust
/// A deserialized row from an SSTable.
pub struct Row {
    pub clustering: Vec<u8>,          // Raw clustering key bytes
    pub cells: Vec<(u16, CellValue)>, // (column index, cell value) pairs
    pub deletion: DeletionTime,       // Row-level deletion
    pub primary_key_liveness: LivenessInfo, // PK liveness timestamp + TTL
}

/// A deserialized partition from an SSTable.
pub struct Partition {
    pub key: DecoratedKey,
    pub deletion: DeletionTime,       // Partition-level deletion
    pub static_row: Option<Row>,      // Static columns (if any)
    pub rows: Vec<Row>,               // Clustered rows in order
}

/// Partition-level or row-level deletion marker.
pub struct DeletionTime {
    pub marked_for_delete_at: i64,    // Microseconds since epoch (i64::MIN = live)
    pub local_deletion_time: u32,     // Seconds since epoch (u32::MAX = live)
}

/// Primary key liveness info for a row.
pub struct LivenessInfo {
    pub timestamp: i64,               // Microseconds since epoch (i64::MIN = no liveness)
    pub ttl: i32,                     // 0 = no TTL
    pub local_deletion_time: i32,     // i32::MAX = no expiry
}
```

`DeletionTime` is used by `partition_index.rs`, `row_index.rs`, and `data.rs` — it should be in `types.rs` as a shared utility within the crate.

`PartitionIterator` is a lazy iterator that reads partitions from the data file on demand (not materializing all partitions into memory).

## Build Order

Bottom-up by component. Each phase is independently testable before the next begins.

### Phase 1: Leaf Components (no internal deps)

| Order | Module | Purpose | Key Tests |
|-------|--------|---------|-----------|
| 1 | `io.rs` | `ReadAt`/`WriteAt` traits + `FileReadAt`/`FileWriteAt` | Round-trip bytes to temp files |
| 2 | `varint.rs` | Unsigned/signed varint encoding/decoding | Known values, 0, max, negative, round-trip |
| 3 | `compression.rs` | LZ4/Zstd + CompressionInfo reader/writer | Round-trip, Cassandra fixture compat |
| 4 | `bloom.rs` | Bloom filter read/write | False-positive rate, Cassandra fixture compat |
| 5 | `byte_comparable.rs` | OSS50 key encoding/decoding | Cassandra-generated key vectors |

### Phase 2: Trie (hardest piece, isolated)

| Order | Module | Purpose | Key Tests |
|-------|--------|---------|-----------|
| 6 | `trie/node.rs` | 16 node types encode/decode | All type codes 0x0-0xF, size formulas |
| 7 | `trie/walker.rs` | Trie traversal | Lookup/floor/ceiling on hand-built tries |
| 8 | `trie/builder.rs` | Page-aware incremental builder | Build from sorted keys, walker finds all, page boundary correctness |

### Phase 3: File Format Readers/Writers (compose earlier pieces)

| Order | Module | Purpose | Key Tests |
|-------|--------|---------|-----------|
| 9 | `statistics.rs`, `toc.rs` | Simple file formats | Round-trip |
| 10 | `partition_index.rs` | Partitions.db (trie + footer + payload) | Cassandra fixture: look up known keys |
| 11 | `row_index.rs` | Rows.db (per-partition tries) | Cassandra fixture: resolve row offsets |
| 12 | `data.rs` | Data.db (partition deserialization) | Cassandra fixture: read known partitions |

### Phase 4: Public API (compose everything)

| Order | Module | Purpose | Key Tests |
|-------|--------|---------|-----------|
| 13 | `reader.rs` | `SSTableReader` | Read real Cassandra SSTables end-to-end |
| 14 | `writer.rs` | `SSTableWriter` | Write + read-back round-trip, cross-verify with `sstabledump` |

## Test Strategy

### TDD Discipline

Every component follows strict red-green-refactor:

1. Write a failing test that specifies the behavior
1. Write the minimal implementation to make it pass
1. Refactor while keeping tests green
1. Repeat

### Regression Testing Rule

When a bug is found:

1. Create a failing test that demonstrates the bug
1. Confirm the test fails with current code
1. Fix the bug
1. Verify the regression test passes
1. Include bug context in the test name/comment

### Test Layers

| Layer | Purpose | Tools |
|-------|---------|-------|
| Unit (TDD) | Red-green-refactor per component | `#[test]` |
| Oracle | Binary-exact match vs Cassandra output | Fixtures in `tests/fixtures/` |
| Round-trip | Write then read back, assert equality | `#[test]` with temp files |
| Property | Invariants over random input | `proptest` |
| Doc-tests | Examples in rustdoc that compile and run | `///` code blocks |

### Property Tests

| Module | Properties |
|--------|-----------|
| `varint.rs` | Round-trip: `decode(encode(n)) == n` for all valid integers |
| `compression.rs` | Round-trip: `decompress(compress(data)) == data` for arbitrary bytes |
| `bloom.rs` | No false negatives: all inserted keys are found |
| `trie/builder.rs` + `walker.rs` | Arbitrary sorted key sets produce a trie that resolves every key |
| `byte_comparable.rs` | Round-trip: `decode(encode(key)) == key`; ordering preserved |
| `data.rs` | Data serialization round-trip: `deserialize(serialize(partition)) == partition` |

### Cassandra Fixture Generation

**`tools/generate_sstable_fixtures.java`** uses Cassandra's internal APIs to create BTI SSTables with known data:

| Fixture | Purpose | Triggers |
|---------|---------|----------|
| Multi-partition (~100 rows) | General read/write testing | Partition index, bloom filter, compression |
| Single partition | Minimal case, no row index | Direct data pointer (negative `idxpos`) |
| Wide partition (many clustering keys) | Row index with multiple blocks | Row index trie, block offsets |
| Empty table | Edge case | Empty trie, zero-count statistics |

Each fixture includes all 7 component files. Committed to `tests/fixtures/` for deterministic CI. Individual component tests read only the relevant file (e.g., trie tests read `Partitions.db`).

Fixture generation scripts live in `tools/` alongside existing `generate_murmur3_vectors.java`.

## Public API

See `specs/sstable.md` for format specification and full API signatures.

```rust
/// Positional read — read bytes at an offset without seeking.
pub trait ReadAt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn len(&self) -> Result<u64>;
}

/// Reader composes all components via SSTableComponents<R: ReadAt>.
pub struct SSTableReader<R: ReadAt> { /* ... */ }

/// Writer accumulates to in-memory Vec<u8> buffers (no WriteAt trait).
pub struct SSTableWriter { /* ... */ }

/// Component handles for reading. filter/statistics/compression_info
/// are pre-read into Vec<u8>; data/partitions use generic R: ReadAt.
pub struct SSTableComponents<R> {
    pub data: R,
    pub partitions: R,
    pub rows: Option<R>,
    pub filter: Vec<u8>,
    pub compression_info: Option<Vec<u8>>,
    pub statistics: Vec<u8>,
}

/// Writer output — all component buffers.
pub struct WrittenSSTable {
    pub data: Vec<u8>,
    pub partitions: Vec<u8>,
    pub filter: Vec<u8>,
    pub statistics: Vec<u8>,
    pub toc: Vec<u8>,
}

pub struct WriteOptions {
    pub compression: Option<Compression>,
    pub bloom_fp_chance: f64,
    pub partitioner: String,
}

pub enum Compression { Lz4, Zstd { level: i32 } }
```

## Literate Programming (swdev compliance)

Every module follows the swdev template:

1. `//!` module docs: purpose, constraints/invariants, edge cases, rationale, test pointers
1. `///` docs on all public types and methods with doc-tests
1. Narrative explains algorithmic elements (especially trie encoding, byte-comparable keys)
1. `cargo doc --no-deps` with `RUSTDOCFLAGS="-D warnings"` in CI
1. Intra-doc links for cross-references between modules

## ferrosa-common Improvements

The swdev evaluation identified gaps in ferrosa-common. High-priority items (H1-H4) were completed during Part A.

| Priority | Action | Status |
|----------|--------|--------|
| H1 | Add `//!` module docs to `murmur3.rs`, `token.rs`, `key.rs`, `cell.rs`, `error.rs` | Done |
| H2 | Add doc-tests to `Token::from_key`, `CellValue::live`/`tombstone`, `DecoratedKey::new`, `hash3_x64_128` | Done |
| H3 | Add `cargo doc --no-deps` with `RUSTDOCFLAGS="-D warnings"` to CI | Done |
| H4 | Add `proptest` dev-dep; property tests for murmur3 determinism, DecoratedKey ordering | Done |
| M1-M4 | Rationale docs, edge case docs, undocumented public methods, module ordering | Deferred |

## Related Documents

- [SSTable Format Specification](../../../specs/sstable.md) — byte-level BTI format
- [Component Architecture](../../../specs/components.md) — crate dependency graph
- [ADR-004](../../../specs/decisions/004-layered-sstable.md) — layered format strategy
- [Plan 1: ferrosa-common](2026-03-11-ferrosa-workspace-and-common.md) — completed, template for this plan
