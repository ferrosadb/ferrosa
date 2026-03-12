# ferrosa-storage Part B: Commit Log Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a segment-based commit log (WAL) for ferrosa-storage with lock-free CAS allocation, three configurable sync strategies, and crash-recovery replay.

**Architecture:** Write-ahead log with fixed-size segments. Each segment is a pre-allocated byte buffer; writers CAS-allocate slices and serialize mutations directly into them. A background sync strategy fsyncs segment data to disk. On crash recovery, segments after the last checkpoint are replayed to restore memtable state.

**Tech Stack:** Rust, `crc32fast`, `serde`/`serde_json`, `arc_swap`, `parking_lot`, `proptest`

**Spec:** `docs/superpowers/specs/2026-03-11-ferrosa-storage-part-b-design.md`

---

## File Structure

```
ferrosa-common/
  Cargo.toml                    # Add [features] test-generators = ["proptest"], proptest optional dep
  src/
    lib.rs                      # Add `pub mod test_generators;` (cfg-gated)
    test_generators.rs          # NEW: shared proptest strategies for CellValue, Row, DecoratedKey

ferrosa-storage/
  Cargo.toml                    # Add crc32fast, serde, serde_json
  src/
    lib.rs                      # Add `pub mod commitlog;`
    commitlog/
      mod.rs                    # NEW: CommitLog struct, public API, re-exports
      config.rs                 # NEW: CommitLogConfig, SyncStrategyConfig, TableId, CommitLogPosition
      descriptor.rs             # NEW: SegmentDescriptor (17-byte header write/read/validate)
      mutation.rs               # NEW: Mutation struct, serialize_into/deserialize_from/serialized_size
      segment.rs                # NEW: Segment buffer, CAS allocation, entry writing, sync markers
      sync.rs                   # NEW: SyncStrategy trait + Periodic/Batch/Group impls
      reader.rs                 # NEW: SegmentReader (parse entries from segment file)
      checkpoint.rs             # NEW: CommitLogCheckpoint (JSON, atomic write)
  tests/
    commitlog_integration.rs    # NEW: cross-module integration tests
    commitlog_property.rs       # NEW: property-based tests using shared generators
```

---

## Chunk 1: Foundation — Generators, Config, Descriptor, Mutation Serialization

### Task 1: Shared Proptest Generators in ferrosa-common

**Files:**

- Modify: `ferrosa-common/Cargo.toml`
- Create: `ferrosa-common/src/test_generators.rs`
- Modify: `ferrosa-common/src/lib.rs`

**Context:** These generators produce arbitrary `CellValue`, `Row`, `DecoratedKey`, and `Partition` values for property tests across all crates. They live behind `#[cfg(feature = "test-generators")]` so they don't pollute the public API.

**Important types (from `ferrosa-sstable::types`):**

- `Row { clustering: Vec<u8>, cells: Vec<(u16, CellValue)>, deletion: DeletionTime, primary_key_liveness: LivenessInfo }`
- `DeletionTime { marked_for_delete_at: i64, local_deletion_time: u32 }` — note `u32`, not `i32`
- `LivenessInfo { timestamp: i64, ttl: i32, local_deletion_time: i32 }`
- `Partition { key: DecoratedKey, deletion: DeletionTime, static_row: Option<Row>, rows: Vec<Row> }`

**Important types (from `ferrosa-common`):**

- `CellValue { value: Option<Vec<u8>>, timestamp: i64, ttl: i32, local_deletion_time: i32 }`
- `DecoratedKey { token: Token, key: PartitionKey }`
- `PartitionKey(Vec<u8>)` — constructor: `PartitionKey::new(vec)` or `PartitionKey::from(slice)`

- [ ] **Step 1: Update ferrosa-common/Cargo.toml**

Add `proptest` as an optional dependency and a `test-generators` feature:

```toml
[package]
name = "ferrosa-common"
description = "Shared types for the Ferrosa distributed database"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
proptest = { version = "1", optional = true }

[features]
test-generators = ["proptest"]

[dev-dependencies]
proptest = "1"
```

Note: `proptest` stays in `[dev-dependencies]` too, so unit tests within ferrosa-common can use it without the feature flag.

- [ ] **Step 2: Create test_generators.rs**

Create `ferrosa-common/src/test_generators.rs` with generators for `CellValue`, `(u16, CellValue)`, `Row`, `DecoratedKey`, and `Partition`. The generators must also depend on `ferrosa-sstable` types (`Row`, `DeletionTime`, `LivenessInfo`, `Partition`). However, `ferrosa-common` cannot depend on `ferrosa-sstable` (it would be circular).

**Resolution:** The generators for `CellValue`, `DecoratedKey`, and `PartitionKey` live in `ferrosa-common`. The generators for `Row`, `DeletionTime`, `LivenessInfo`, and `Partition` must live in `ferrosa-storage` (or whichever crate has both `ferrosa-common` and `ferrosa-sstable` as deps). We split:

- `ferrosa-common/src/test_generators.rs` — `arb_cell_value`, `arb_cell`, `arb_key`, `arb_partition_key`
- Generators that reference `Row`, `DeletionTime`, `LivenessInfo`, `Partition`, `Mutation`, `TableId` will be defined in `ferrosa-storage/src/commitlog/test_helpers.rs` (internal test module) and in the test files.

```rust
//! Shared proptest generators for ferrosa types.
//!
//! Enabled by the `test-generators` feature. These produce arbitrary
//! [`CellValue`], [`DecoratedKey`], and [`PartitionKey`] values for
//! property-based testing across crates.
//!
//! Generators for [`Row`], [`Partition`], etc. live in consuming crates
//! (e.g., `ferrosa-storage`) because they depend on `ferrosa-sstable` types.

use proptest::prelude::*;

use crate::cell::CellValue;
use crate::key::{DecoratedKey, PartitionKey};

/// Arbitrary cell value: live, tombstone, or expiring (with TTL).
pub fn arb_cell_value() -> impl Strategy<Value = CellValue> {
    prop_oneof![
        // Live cell with arbitrary bytes
        (prop::collection::vec(any::<u8>(), 0..1024), 1i64..1_000_000)
            .prop_map(|(v, ts)| CellValue::live(v, ts)),
        // Tombstone
        (1i64..1_000_000, 1_700_000_000i32..1_700_100_000)
            .prop_map(|(ts, ldt)| CellValue::tombstone(ts, ldt)),
        // Expiring cell with TTL
        (
            prop::collection::vec(any::<u8>(), 0..256),
            1i64..1_000_000,
            1i32..86400,
            1_700_000_000i32..1_700_100_000,
        )
            .prop_map(|(v, ts, ttl, ldt)| CellValue::expiring(v, ts, ttl, ldt)),
    ]
}

/// Arbitrary cell: (column_index, CellValue) pair.
pub fn arb_cell() -> impl Strategy<Value = (u16, CellValue)> {
    (0u16..64, arb_cell_value())
}

/// Arbitrary partition key (1-128 random bytes).
pub fn arb_partition_key() -> impl Strategy<Value = PartitionKey> {
    prop::collection::vec(any::<u8>(), 1..128).prop_map(PartitionKey::new)
}

/// Arbitrary decorated key (partition key + auto-computed Murmur3 token).
pub fn arb_decorated_key() -> impl Strategy<Value = DecoratedKey> {
    arb_partition_key().prop_map(DecoratedKey::new)
}
```

- [ ] **Step 3: Add module to ferrosa-common/src/lib.rs**

Add the cfg-gated module declaration:

```rust
#[cfg(feature = "test-generators")]
pub mod test_generators;
```

- [ ] **Step 4: Run tests to verify generators compile**

Run: `cargo test -p ferrosa-common --features test-generators`
Expected: All existing tests pass, no compile errors.

- [ ] **Step 5: Commit**

```bash
git add ferrosa-common/
git commit -m "feat(common): add shared proptest generators behind test-generators feature"
```

---

### Task 2: CommitLog Config and Types

**Files:**

- Modify: `ferrosa-storage/Cargo.toml`
- Create: `ferrosa-storage/src/commitlog/config.rs`
- Create: `ferrosa-storage/src/commitlog/mod.rs` (minimal, just `pub mod config;`)
- Modify: `ferrosa-storage/src/lib.rs`

**Context:** This task establishes the configuration types, `TableId`, `CommitLogPosition`, and the `commitlog` module. All other tasks depend on these types.

- [ ] **Step 1: Update ferrosa-storage/Cargo.toml**

Add new dependencies:

```toml
[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
ferrosa-sstable = { path = "../ferrosa-sstable" }
arc-swap = "1.7"
parking_lot = "0.12"
crc32fast = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[dev-dependencies]
ferrosa-common = { path = "../ferrosa-common", features = ["test-generators"] }
proptest = "1"
tempfile = "3"
```

- [ ] **Step 2: Write failing tests for config types**

Create `ferrosa-storage/src/commitlog/config.rs`:

```rust
//! Configuration for the commit log.
//!
//! [`CommitLogConfig`] collects all tunables: segment size, rotation age,
//! sync strategy, and directory paths. [`SyncStrategyConfig`] selects
//! which [`SyncStrategy`](super::sync::SyncStrategy) to instantiate.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Position in the commit log: segment ID + byte offset.
///
/// Ordered first by segment_id, then by offset. Used to track how
/// far each table has been flushed so old segments can be deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitLogPosition {
    pub segment_id: u64,
    pub offset: u64,
}

/// Identifies a table for flush tracking.
///
/// Two tables are considered the same if both keyspace and table name match.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId {
    pub keyspace: String,
    pub table: String,
}

impl TableId {
    pub fn new(keyspace: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
        }
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.keyspace, self.table)
    }
}

/// Sync strategy selection.
///
/// | Strategy | Throughput | Latency | Durability Window |
/// |----------|-----------|---------|-------------------|
/// | Periodic | Highest | Lowest | Up to sync_interval |
/// | Batch | Lowest | Highest | Zero |
/// | Group | Good | Bounded | Up to max_wait |
#[derive(Debug, Clone)]
pub enum SyncStrategyConfig {
    /// Fsync on a timer. Best throughput, small durability window.
    Periodic {
        /// Interval between fsyncs (default 10ms).
        sync_interval: Duration,
    },
    /// Fsync per write. Zero data loss, highest latency.
    Batch,
    /// Fsync batches of writes. Bounded latency, good throughput.
    Group {
        /// Max time to wait before fsyncing a batch (default 1ms).
        max_wait: Duration,
    },
}

impl Default for SyncStrategyConfig {
    fn default() -> Self {
        SyncStrategyConfig::Periodic {
            sync_interval: Duration::from_millis(10),
        }
    }
}

/// Default segment size: 32 MB.
pub const DEFAULT_SEGMENT_SIZE: usize = 32 * 1024 * 1024;

/// Default max segment age before rotation: 5 minutes.
pub const DEFAULT_MAX_SEGMENT_AGE: Duration = Duration::from_secs(300);

/// Commit log configuration.
///
/// All sizes are configurable. Defaults are suitable for general workloads:
/// - 32 MB segments with 5-minute max age
/// - Periodic sync every 10ms (best throughput, up to 10ms data loss on crash)
#[derive(Debug, Clone)]
pub struct CommitLogConfig {
    /// Segment size in bytes (default 32 MB).
    pub segment_size: usize,
    /// Maximum segment age before rotation (default 5 minutes).
    pub max_segment_age: Duration,
    /// Sync strategy selection.
    pub sync_strategy: SyncStrategyConfig,
    /// Directory for commit log segment files.
    pub log_dir: PathBuf,
    /// Directory for checkpoint file (may be same as log_dir).
    pub checkpoint_dir: PathBuf,
}

impl CommitLogConfig {
    /// Create a config for testing with small segments and a temp directory.
    #[cfg(test)]
    pub fn test_config(dir: &std::path::Path) -> Self {
        Self {
            segment_size: 4096, // 4 KB for fast rotation in tests
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch, // immediate fsync for deterministic tests
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
        }
    }
}

impl Default for CommitLogConfig {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_segment_age: DEFAULT_MAX_SEGMENT_AGE,
            sync_strategy: SyncStrategyConfig::default(),
            log_dir: PathBuf::from("/var/lib/ferrosa/commitlog"),
            checkpoint_dir: PathBuf::from("/var/lib/ferrosa/commitlog"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = CommitLogConfig::default();
        assert_eq!(config.segment_size, 32 * 1024 * 1024);
        assert_eq!(config.max_segment_age, Duration::from_secs(300));
        assert!(matches!(
            config.sync_strategy,
            SyncStrategyConfig::Periodic { sync_interval }
            if sync_interval == Duration::from_millis(10)
        ));
    }

    #[test]
    fn commit_log_position_ordering() {
        let a = CommitLogPosition { segment_id: 1, offset: 100 };
        let b = CommitLogPosition { segment_id: 1, offset: 200 };
        let c = CommitLogPosition { segment_id: 2, offset: 50 };
        assert!(a < b);
        assert!(b < c); // segment_id takes precedence
    }

    #[test]
    fn table_id_equality() {
        let a = TableId::new("ks1", "users");
        let b = TableId::new("ks1", "users");
        let c = TableId::new("ks1", "orders");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn table_id_display() {
        let id = TableId::new("my_ks", "my_table");
        assert_eq!(format!("{id}"), "my_ks.my_table");
    }

    #[test]
    fn sync_strategy_default_is_periodic() {
        let strategy = SyncStrategyConfig::default();
        assert!(matches!(strategy, SyncStrategyConfig::Periodic { .. }));
    }
}
```

- [ ] **Step 3: Create minimal commitlog/mod.rs**

```rust
//! Commit log (write-ahead log) for durability.
//!
//! The commit log records every mutation before it reaches the memtable.
//! On crash recovery, uncommitted mutations are replayed from segment
//! files to restore memtable state.

pub(crate) mod config;

pub use config::{CommitLogConfig, CommitLogPosition, SyncStrategyConfig, TableId};
```

- [ ] **Step 4: Wire commitlog module into lib.rs**

Add to `ferrosa-storage/src/lib.rs`:

```rust
pub mod commitlog;
```

And add re-exports:

```rust
pub use commitlog::{CommitLogConfig, CommitLogPosition, SyncStrategyConfig, TableId};
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass (existing Part A tests + new config tests).

- [ ] **Step 6: Commit**

```bash
git add ferrosa-storage/
git commit -m "feat(storage): add commit log config types and module skeleton"
```

---

### Task 3: Segment Descriptor (17-byte header)

**Files:**

- Create: `ferrosa-storage/src/commitlog/descriptor.rs`
- Modify: `ferrosa-storage/src/commitlog/mod.rs`

**Context:** The segment header is a 17-byte fixed structure at the start of every segment file. It contains a format version byte, segment ID, config flags, and CRC. This is the first piece of the binary format and is tested independently.

**Binary layout (17 bytes):**

```
version: u8 (1 byte) — format version, starts at 1
segment_id: u64 (8 bytes, big-endian)
config_flags: u32 (4 bytes, big-endian)
header_crc: u32 (4 bytes, big-endian) — CRC32 over [version || segment_id || config_flags] (13 bytes)
```

- [ ] **Step 1: Write descriptor.rs with tests**

Create `ferrosa-storage/src/commitlog/descriptor.rs`:

```rust
//! Segment descriptor: the 17-byte header at the start of every segment file.
//!
//! # Binary Format
//!
//! ```text
//! version:      u8    (1 byte)  — format version, currently 1
//! segment_id:   u64   (8 bytes) — monotonic segment identifier
//! config_flags: u32   (4 bytes) — reserved for future use (compression, encryption)
//! header_crc:   u32   (4 bytes) — CRC32 of [version || segment_id || config_flags]
//! ```
//!
//! Total: 17 bytes. All multi-byte integers are big-endian.

use ferrosa_common::Result;

/// Current format version. Increment on breaking format changes.
pub const FORMAT_VERSION: u8 = 1;

/// Size of the segment header in bytes.
pub const HEADER_SIZE: usize = 17;

/// Segment descriptor read from or written to a segment file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub version: u8,
    pub segment_id: u64,
    pub config_flags: u32,
}

impl SegmentDescriptor {
    /// Create a new descriptor for the current format version.
    pub fn new(segment_id: u64) -> Self {
        Self {
            version: FORMAT_VERSION,
            segment_id,
            config_flags: 0,
        }
    }

    /// Serialize the descriptor into a 17-byte buffer.
    pub fn write_to(&self, buf: &mut [u8]) {
        assert!(buf.len() >= HEADER_SIZE, "buffer too small for header");
        buf[0] = self.version;
        buf[1..9].copy_from_slice(&self.segment_id.to_be_bytes());
        buf[9..13].copy_from_slice(&self.config_flags.to_be_bytes());
        let crc = crc32fast::hash(&buf[..13]);
        buf[13..17].copy_from_slice(&crc.to_be_bytes());
    }

    /// Deserialize a descriptor from a 17-byte buffer, validating the CRC.
    pub fn read_from(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "segment header too short: {} bytes (need {})",
                buf.len(),
                HEADER_SIZE
            )));
        }

        let expected_crc = crc32fast::hash(&buf[..13]);
        let stored_crc = u32::from_be_bytes([buf[13], buf[14], buf[15], buf[16]]);

        if expected_crc != stored_crc {
            return Err(ferrosa_common::Error::ChecksumMismatch {
                expected: expected_crc,
                actual: stored_crc,
            });
        }

        let version = buf[0];
        let segment_id = u64::from_be_bytes([
            buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
        ]);
        let config_flags = u32::from_be_bytes([buf[9], buf[10], buf[11], buf[12]]);

        Ok(Self {
            version,
            segment_id,
            config_flags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read_round_trip() {
        let desc = SegmentDescriptor::new(42);
        let mut buf = [0u8; HEADER_SIZE];
        desc.write_to(&mut buf);

        let read_back = SegmentDescriptor::read_from(&buf).unwrap();
        assert_eq!(read_back, desc);
    }

    #[test]
    fn crc_catches_corruption() {
        let desc = SegmentDescriptor::new(42);
        let mut buf = [0u8; HEADER_SIZE];
        desc.write_to(&mut buf);

        // Corrupt the segment_id
        buf[4] ^= 0xFF;

        let result = SegmentDescriptor::read_from(&buf);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), ferrosa_common::Error::ChecksumMismatch { .. })
        );
    }

    #[test]
    fn version_byte_preserved() {
        let desc = SegmentDescriptor {
            version: 2, // future version
            segment_id: 100,
            config_flags: 0,
        };
        let mut buf = [0u8; HEADER_SIZE];
        desc.write_to(&mut buf);

        let read_back = SegmentDescriptor::read_from(&buf).unwrap();
        assert_eq!(read_back.version, 2);
    }

    #[test]
    fn buffer_too_short() {
        let buf = [0u8; 10]; // less than 17
        let result = SegmentDescriptor::read_from(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn config_flags_preserved() {
        let desc = SegmentDescriptor {
            version: FORMAT_VERSION,
            segment_id: 1,
            config_flags: 0xDEAD_BEEF,
        };
        let mut buf = [0u8; HEADER_SIZE];
        desc.write_to(&mut buf);

        let read_back = SegmentDescriptor::read_from(&buf).unwrap();
        assert_eq!(read_back.config_flags, 0xDEAD_BEEF);
    }

    #[test]
    fn header_is_exactly_17_bytes() {
        assert_eq!(HEADER_SIZE, 17);
        assert_eq!(
            1 + 8 + 4 + 4, // version + segment_id + config_flags + crc
            HEADER_SIZE
        );
    }
}
```

- [ ] **Step 2: Add module to commitlog/mod.rs**

```rust
pub(crate) mod descriptor;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/commitlog/descriptor.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "feat(storage): add segment descriptor (17-byte header) with CRC validation"
```

---

### Task 4: Mutation Type and Binary Serialization

**Files:**

- Create: `ferrosa-storage/src/commitlog/mutation.rs`
- Modify: `ferrosa-storage/src/commitlog/mod.rs`

**Context:** The `Mutation` struct groups one or more row writes to a single table. It has hand-rolled binary serialization that writes directly into a destination buffer (no intermediate allocations). `serialized_size()` computes the exact byte count upfront for CAS buffer allocation.

**Type note:** `DeletionTime.local_deletion_time` is `u32`; `CellValue.local_deletion_time` is `i32`. The serialization format matches the Rust types exactly.

**Why not reuse SSTable serialization?** SSTable row format uses delta-encoding against a `SerializationHeader`. The commit log has no header context — entries must be self-describing.

**Binary layouts (from spec):**

Mutation: `keyspace_len:u16 | keyspace | table_len:u16 | table | key_len:u16 | key_bytes | token:i64 | timestamp:i64 | row_count:u16 | rows`

Row: `clustering_len:u16 | clustering | deletion_marked_for_delete_at:i64 | deletion_local_deletion_time:u32 | liveness_timestamp:i64 | liveness_ttl:i32 | liveness_local_deletion_time:i32 | cell_count:u16 | cells`

Cell: `column_index:u16 | timestamp:i64 | ttl:i32 | local_deletion_time:i32 | value_len:i32 (-1=tombstone) | value`

- [ ] **Step 1: Write mutation.rs with serialization, deserialization, and tests**

Create `ferrosa-storage/src/commitlog/mutation.rs`. The file should contain:

1. `Mutation` struct with fields: `keyspace: String`, `table: String`, `key: DecoratedKey`, `rows: Vec<Row>`, `timestamp: i64`
2. `Mutation::serialized_size(&self) -> usize` — computes exact byte count
3. `Mutation::serialize_into(&self, buf: &mut [u8])` — writes into a pre-sized buffer, panics if too small
4. `Mutation::deserialize_from(buf: &[u8]) -> Result<Self>` — reads from a byte slice

All multi-byte integers are big-endian. String/byte-array fields are length-prefixed with `u16`.

Cell serialization: `value_len` is `i32`; `-1` means tombstone (no value bytes follow). Otherwise `value_len` bytes follow.

**Tests to include:**

| Test | What it proves |
|------|---------------|
| `round_trip_simple` | Serialize a mutation with one row and one live cell, deserialize, compare |
| `round_trip_tombstone` | Cell with `value: None` serializes as `value_len = -1` |
| `round_trip_expiring` | Cell with TTL and local_deletion_time preserved |
| `round_trip_empty_rows` | Mutation with zero rows round-trips |
| `round_trip_multiple_rows` | Multiple rows with different clustering keys |
| `serialized_size_matches_actual` | `serialized_size()` equals actual bytes written |
| `deserialize_truncated_fails` | Truncated buffer returns error |

- [ ] **Step 2: Add module to commitlog/mod.rs and re-export**

```rust
pub(crate) mod mutation;
pub use mutation::Mutation;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage commitlog::mutation`
Expected: All tests pass.

- [ ] **Step 4: Add proptest round-trip using shared generators**

Add to the `#[cfg(test)]` section of `mutation.rs`:

```rust
mod prop_tests {
    use super::*;
    use ferrosa_common::test_generators::{arb_cell_value, arb_decorated_key};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};
    use proptest::prelude::*;

    fn arb_row() -> impl Strategy<Value = Row> {
        (
            prop::collection::vec(any::<u8>(), 0..32),
            prop::collection::vec(
                (0u16..64, arb_cell_value()),
                0..16,
            ),
            prop_oneof![
                Just(DeletionTime::LIVE),
                (1i64..1_000_000, 1u32..100_000)
                    .prop_map(|(ts, ldt)| DeletionTime::new(ts, ldt)),
            ],
            1i64..1_000_000,
        )
            .prop_map(|(clustering, mut cells, deletion, ts)| {
                cells.sort_by_key(|(idx, _)| *idx);
                cells.dedup_by_key(|(idx, _)| *idx);
                Row {
                    clustering,
                    cells,
                    deletion,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }
            })
    }

    fn arb_mutation() -> impl Strategy<Value = Mutation> {
        (
            "[a-z]{1,8}",
            "[a-z]{1,8}",
            arb_decorated_key(),
            prop::collection::vec(arb_row(), 0..8),
            1i64..1_000_000,
        )
            .prop_map(|(keyspace, table, key, rows, timestamp)| Mutation {
                keyspace,
                table,
                key,
                rows,
                timestamp,
            })
    }

    proptest! {
        #[test]
        fn serialization_round_trip(mutation in arb_mutation()) {
            let size = mutation.serialized_size();
            let mut buf = vec![0u8; size];
            mutation.serialize_into(&mut buf);
            let deserialized = Mutation::deserialize_from(&buf).unwrap();
            prop_assert_eq!(mutation.keyspace, deserialized.keyspace);
            prop_assert_eq!(mutation.table, deserialized.table);
            prop_assert_eq!(mutation.key, deserialized.key);
            prop_assert_eq!(mutation.rows.len(), deserialized.rows.len());
            prop_assert_eq!(mutation.timestamp, deserialized.timestamp);
            for (orig, deser) in mutation.rows.iter().zip(deserialized.rows.iter()) {
                prop_assert_eq!(&orig.clustering, &deser.clustering);
                prop_assert_eq!(orig.cells.len(), deser.cells.len());
                prop_assert_eq!(orig.deletion, deser.deletion);
                prop_assert_eq!(orig.primary_key_liveness, deser.primary_key_liveness);
            }
        }
    }
}
```

- [ ] **Step 5: Run all tests including proptests**

Run: `cargo test -p ferrosa-storage commitlog::mutation`
Expected: All tests pass including proptest round-trip.

- [ ] **Step 6: Commit**

```bash
git add ferrosa-storage/src/commitlog/mutation.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "feat(storage): add Mutation type with binary serialization and proptest round-trip"
```

---

### Task 5: Segment — Buffer, CAS Allocation, Entry Writing

**Files:**

- Create: `ferrosa-storage/src/commitlog/segment.rs`
- Modify: `ferrosa-storage/src/commitlog/mod.rs`

**Context:** A `Segment` is a fixed-size byte buffer that writers CAS-allocate slices from. This is the hot path — no locks. Each writer:

1. Calls `allocate(size)` which does an `AtomicU64::compare_exchange` loop
2. Gets back an exclusive `&mut [u8]` slice at the reserved offset
3. Writes the entry (size + size_crc + payload + payload_crc) directly into that slice

If `allocate()` would exceed the segment capacity, it returns `None` — the caller must trigger rotation.

**Entry format per mutation:**

```
entry_size:   u32 (4 bytes) — size of payload only
size_crc:     u32 (4 bytes) — CRC32 of entry_size bytes
payload:      [u8; entry_size] — serialized Mutation
payload_crc:  u32 (4 bytes) — CRC32 of payload
```

Entry overhead: 12 bytes (4 + 4 + 4).

**Sync marker format (8 bytes):**

```
next_marker_offset: u32 — absolute byte offset of next sync marker (0 = EOF)
marker_crc:         u32 — CRC32 of (segment_id as u64 || next_marker_offset as u32)
```

Sync markers are written at sync boundaries by the sync strategy. The first sync marker starts at offset `HEADER_SIZE` (17). Entries follow after the marker.

- [ ] **Step 1: Write segment.rs**

The `Segment` struct should contain:

- `id: u64` — segment identifier
- `buffer: Vec<u8>` — pre-allocated to `segment_size`, zeroed
- `position: AtomicU64` — next write offset (starts after header + first sync marker = 17 + 8 = 25)
- `capacity: usize` — max usable bytes (= `segment_size`)
- `created_at: Instant` — for age-based rotation
- `path: PathBuf` — file path for this segment
- `dirty_tables: Mutex<HashMap<TableId, CommitLogPosition>>` — which tables have data here

Public API:

- `Segment::new(id: u64, size: usize, dir: &Path) -> Result<Self>` — creates segment file, writes header
- `Segment::allocate(&self, entry_total_size: usize) -> Option<u64>` — CAS loop, returns start offset or None if full
- `Segment::write_entry(&self, offset: u64, mutation: &Mutation) -> CommitLogPosition` — writes entry at offset
- `Segment::write_sync_marker(&self, next_offset: u32)` — writes sync marker at current sync position
- `Segment::flush_to_disk(&self) -> Result<()>` — writes buffer to file + fsync
- `Segment::mark_table_dirty(&self, table_id: &TableId, position: CommitLogPosition)` — updates dirty tracking
- `Segment::is_expired(&self, max_age: Duration) -> bool` — checks age

Entry total size = 12 (overhead) + `mutation.serialized_size()`.

**Tests to include:**

| Test | What it proves |
|------|---------------|
| `allocate_returns_sequential_offsets` | Two allocations return non-overlapping offsets |
| `allocate_returns_none_when_full` | Allocation past capacity returns None |
| `write_entry_round_trip` | Write an entry, read raw bytes, verify CRCs match |
| `concurrent_allocations_no_overlap` | N threads allocate, all offsets are disjoint |
| `entry_overhead_is_12_bytes` | Verify constant |
| `segment_starts_after_header_and_marker` | First allocation offset = HEADER_SIZE + 8 |

- [ ] **Step 2: Add module to commitlog/mod.rs**

```rust
pub(crate) mod segment;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage commitlog::segment`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/commitlog/segment.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "feat(storage): add Segment with lock-free CAS allocation and entry writing"
```

---

### Task 6: Sync Strategies

**Files:**

- Create: `ferrosa-storage/src/commitlog/sync.rs`
- Modify: `ferrosa-storage/src/commitlog/mod.rs`

**Context:** Three sync strategies control when segment buffers are fsynced to disk. The `SyncStrategy` trait has three methods: `on_write` (called after each entry is written), `start` (launches background thread if needed), and `stop` (clean shutdown).

- [ ] **Step 1: Write sync.rs with trait and three implementations**

```rust
pub trait SyncStrategy: Send + Sync {
    /// Called after each mutation is written to the segment buffer.
    /// May block (Batch/Group) or return immediately (Periodic).
    fn on_write(&self, segment: &Segment, position: u64);

    /// Start background sync work (if any).
    fn start(&self);

    /// Shut down cleanly. Fsync any pending data.
    fn stop(&self);
}
```

**PeriodicSync:**

- Spawns a background thread that wakes every `sync_interval`
- Calls `segment.flush_to_disk()` on each wake
- `on_write()` returns immediately (no blocking)
- `stop()` signals the thread via `AtomicBool` + `Condvar`, joins, does final fsync

**BatchSync:**

- `on_write()` calls `segment.flush_to_disk()` synchronously
- No background thread
- `start()` / `stop()` are no-ops

**GroupSync:**

- Background thread wakes on `Condvar` signal or `max_wait` timeout
- `on_write()` increments pending count, signals condvar, then waits on a "completed" condvar
- Background thread: fsync, then notify all waiters
- `stop()` signals thread, joins, final fsync

**Tests to include:**

| Test | What it proves |
|------|---------------|
| `batch_sync_flushes_immediately` | After `on_write`, file on disk contains the written data |
| `periodic_sync_does_not_block` | `on_write` returns in < 1ms (timing test) |
| `group_sync_batches_writes` | Two writes, one fsync call |
| `stop_flushes_pending` | After `stop()`, all data is on disk |

**Note:** These tests need real segment files (use `tempfile::TempDir`). The sync strategy tests are inherently timing-dependent for Periodic/Group — use generous timeouts and assert observable side effects rather than exact timing.

- [ ] **Step 2: Add module to commitlog/mod.rs**

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage commitlog::sync`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/commitlog/sync.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "feat(storage): add SyncStrategy trait with Periodic, Batch, and Group implementations"
```

---

### Task 7: Segment Reader

**Files:**

- Create: `ferrosa-storage/src/commitlog/reader.rs`
- Modify: `ferrosa-storage/src/commitlog/mod.rs`

**Context:** `SegmentReader` reads a segment file and yields `Mutation` entries. It validates CRCs and follows sync marker chains. On corruption, it skips to the next sync marker (if possible) or stops.

- [ ] **Step 1: Write reader.rs**

Public API:

- `SegmentReader::open(path: &Path) -> Result<Self>` — reads and validates header
- `SegmentReader::read_all(&mut self) -> Result<Vec<(CommitLogPosition, Mutation)>>` — reads all valid entries
- `SegmentReader::descriptor(&self) -> &SegmentDescriptor` — access header info

Reading algorithm:

1. Read and validate 17-byte header
2. Start at offset `HEADER_SIZE` (17)
3. Read sync marker (8 bytes): `next_marker_offset` + `marker_crc`
4. Validate marker CRC: `crc32(segment_id || next_marker_offset)`
5. Read entries until reaching `next_marker_offset` (or EOF marker where `next_marker_offset == 0`)
6. For each entry: read `entry_size` (4), validate `size_crc` (4), read `payload` (entry_size bytes), validate `payload_crc` (4)
7. Deserialize payload via `Mutation::deserialize_from`
8. If any CRC fails: skip to next sync marker if available, otherwise stop

**Tests to include:**

| Test | What it proves |
|------|---------------|
| `read_valid_segment` | Write entries via Segment, read back via SegmentReader, data matches |
| `detect_corrupted_header_crc` | Flip header byte, reader returns error |
| `detect_corrupted_entry_size_crc` | Corrupt size_crc, reader skips entry |
| `detect_corrupted_payload_crc` | Corrupt payload_crc, reader skips entry |
| `stop_at_eof_marker` | All-zeros sync marker terminates reading |
| `read_empty_segment` | Segment with only header + EOF marker returns no entries |

- [ ] **Step 2: Add module to commitlog/mod.rs**

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage commitlog::reader`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/commitlog/reader.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "feat(storage): add SegmentReader for replay with CRC validation and corruption recovery"
```

---

### Task 8: Checkpoint File

**Files:**

- Create: `ferrosa-storage/src/commitlog/checkpoint.rs`
- Modify: `ferrosa-storage/src/commitlog/mod.rs`

**Context:** The checkpoint file tracks the last flushed `CommitLogPosition` per table. It's a JSON file with `format_version`. Writes are atomic: write to temp file, then rename.

**JSON format:**

```json
{
  "format_version": 1,
  "flushed_positions": {
    "ks1.table1": { "segment_id": 42, "offset": 8192 }
  },
  "timestamp": "2026-03-11T12:00:00Z"
}
```

- [ ] **Step 1: Write checkpoint.rs**

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use ferrosa_common::Result;
use super::config::{CommitLogPosition, TableId};

const CHECKPOINT_FORMAT_VERSION: u32 = 1;
const CHECKPOINT_FILENAME: &str = "commitlog_checkpoint.json";

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointFile {
    format_version: u32,
    flushed_positions: HashMap<String, PositionEntry>,
    timestamp: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PositionEntry {
    segment_id: u64,
    offset: u64,
}
```

Public API:

- `CommitLogCheckpoint::load(dir: &Path) -> Result<HashMap<TableId, CommitLogPosition>>` — reads checkpoint, returns empty map if file doesn't exist
- `CommitLogCheckpoint::save(dir: &Path, positions: &HashMap<TableId, CommitLogPosition>) -> Result<()>` — atomic write (write to `.tmp`, rename)

**Tests to include:**

| Test | What it proves |
|------|---------------|
| `write_read_round_trip` | Save positions, load, compare |
| `load_nonexistent_returns_empty` | No file → empty HashMap |
| `atomic_update` | Write, verify file exists at expected path |
| `format_version_check` | Manually write a file with version 99, load fails |

- [ ] **Step 2: Add module to commitlog/mod.rs**

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage commitlog::checkpoint`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/commitlog/checkpoint.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "feat(storage): add CommitLogCheckpoint with atomic JSON writes"
```

---

## Chunk 2: CommitLog Composition, Integration Tests, Property Tests

### Task 9: CommitLog Public API

**Files:**

- Modify: `ferrosa-storage/src/commitlog/mod.rs`

**Context:** `CommitLog` composes `Segment`, `SyncStrategy`, `SegmentReader`, and `CommitLogCheckpoint` into the complete commit log API. It manages the segment lifecycle (active → closed → deleted) and coordinates flush tracking.

**Module visibility:** Internal modules (`descriptor`, `segment`, `reader`, `checkpoint`, `sync`) should be `pub(crate) mod` — only the types re-exported from `commitlog/mod.rs` are part of the public API. This keeps internals hidden from downstream crate users.

**Key internals:**

- `active: ArcSwap<Segment>` — lock-free access to the current segment
- `closed_segments: Mutex<Vec<Arc<Segment>>>` — segments waiting for all tables to flush
- `segment_tracker: SegmentTracker` — tracks which tables are dirty in which segments (a `HashMap<u64, HashMap<TableId, CommitLogPosition>>` keyed by segment ID, wrapped in a `Mutex`). This is a simple struct defined inline in `mod.rs` — not a separate file.
- `next_segment: Mutex<Option<Segment>>` — pre-allocated segment for fast rotation
- `sync_strategy: Box<dyn SyncStrategy>` — controls fsync timing
- `next_segment_id: AtomicU64` — monotonic ID generator

- [ ] **Step 1: Implement CommitLog in mod.rs**

Public methods:

- `CommitLog::new(config: CommitLogConfig) -> Result<Self>` — create dirs, allocate first segment + next segment, start sync strategy
- `CommitLog::open_and_replay(config: CommitLogConfig) -> Result<(Self, Vec<Mutation>)>` — load checkpoint, scan segment files, replay entries after checkpoint positions
- `CommitLog::append(&self, mutation: &Mutation) -> Result<CommitLogPosition>` — CAS allocate in active segment, if full → rotate, write entry, call `sync_strategy.on_write()`, return position
- `CommitLog::discard_completed(&self, table_id: &TableId, position: CommitLogPosition) -> Result<()>` — update tracking, delete fully-clean segments, update checkpoint
- `CommitLog::force_rotate(&self) -> Result<()>` — close active segment, swap in pre-allocated next
- `CommitLog::shutdown(&self) -> Result<()>` — stop sync strategy, close active segment, write checkpoint

**Rotation flow:**

1. Move active segment to closed list
2. Swap in pre-allocated `next_segment`
3. Pre-allocate a new `next_segment` in the background

**Append flow:**

1. Load active segment via `ArcSwap::load()`
2. Compute entry total size: `12 + mutation.serialized_size()`
3. `segment.allocate(total_size)` — if `None`, call `force_rotate()` and retry
4. `segment.write_entry(offset, mutation)`
5. `segment.mark_table_dirty(table_id, position)`
6. `sync_strategy.on_write(segment, offset)`
7. Return `CommitLogPosition { segment_id, offset }`

- [ ] **Step 2: Write unit tests for CommitLog**

Tests (using `tempfile::TempDir` and `CommitLogConfig::test_config`):

| Test | What it proves |
|------|---------------|
| `new_creates_segment_files` | After `new()`, segment file exists on disk |
| `append_returns_positions` | Two appends return increasing positions |
| `append_and_shutdown` | Append, shutdown, verify segment file contains data |
| `rotation_on_full_segment` | Fill a small segment, verify rotation creates second file |
| `discard_deletes_clean_segments` | Append to segment, flush all tables past it, verify file deleted |
| `discard_keeps_partially_dirty` | Two tables in segment, flush one, segment stays |

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage commitlog`
Expected: All tests pass.

- [ ] **Step 4: Update lib.rs re-exports**

Add re-exports for the full public API:

```rust
pub use commitlog::{CommitLog, CommitLogConfig, CommitLogPosition, Mutation, SyncStrategyConfig, TableId};
```

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/src/commitlog/mod.rs ferrosa-storage/src/lib.rs
git commit -m "feat(storage): add CommitLog with append, replay, rotation, and flush tracking"
```

---

### Task 10: Integration Tests

**Files:**

- Create: `ferrosa-storage/tests/commitlog_integration.rs`

**Context:** These tests exercise the commit log as a complete system: append mutations, shut down, replay, verify recovery. They use real files via `tempfile::TempDir`.

- [ ] **Step 1: Write integration tests**

Each test creates a `TempDir`, builds a `CommitLogConfig::test_config`, and exercises the full lifecycle.

```rust
use ferrosa_storage::commitlog::*;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_common::CellValue;
use tempfile::TempDir;

fn make_mutation(ks: &str, table: &str, key: &[u8], value: &[u8], ts: i64) -> Mutation {
    Mutation {
        keyspace: ks.to_string(),
        table: table.to_string(),
        key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
        rows: vec![Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }],
        timestamp: ts,
    }
}
```

**Tests to implement:**

| Test | What it proves |
|------|---------------|
| `append_replay_round_trip` | Write 10 mutations, shutdown, `open_and_replay`, all 10 returned in order |
| `concurrent_appends_no_data_loss` | 8 threads × 100 mutations, shutdown, replay, all 800 present |
| `flush_tracking_cleans_segments` | Append mutations, flush all tables, verify old segment files deleted |
| `segment_rotation_on_size` | Use 4KB segments, write enough to fill 3 segments, verify 3 segment files created |
| `crash_mid_entry` | Write entries, truncate segment file mid-entry, replay recovers all entries before truncation |
| `crash_mid_sync_marker` | Truncate at sync marker boundary, replay recovers previous sections |
| `checkpoint_survives_restart` | Write checkpoint, create new CommitLog, replay starts after checkpoint |
| `multiple_tables_independent_flush` | Two tables in same segment, flush one, segment stays; flush both, segment deleted |
| `periodic_sync_strategy` | Use Periodic config, append mutation, wait `sync_interval + margin`, verify file contains data |
| `batch_sync_strategy` | Use Batch config, append mutation, verify file immediately contains data |
| `group_sync_strategy` | Use Group config, append mutations from 4 threads, verify all durable after shutdown |
| `segment_rotation_on_age` | Use 1-second max_age, write one mutation, sleep 1.5s, write another, verify rotation occurred |

- [ ] **Step 2: Run integration tests**

Run: `cargo test -p ferrosa-storage --test commitlog_integration`
Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add ferrosa-storage/tests/commitlog_integration.rs
git commit -m "test(storage): add commit log integration tests"
```

---

### Task 11: Property Tests

**Files:**

- Create: `ferrosa-storage/tests/commitlog_property.rs`

**Context:** Property-based tests verify invariants that must hold for *all* inputs, not just specific examples. These use `proptest` with the shared generators from `ferrosa-common` and commit-log-specific generators.

- [ ] **Step 1: Write property tests**

```rust
use proptest::prelude::*;
use ferrosa_common::test_generators::*;
use ferrosa_storage::commitlog::*;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use tempfile::TempDir;
```

**Properties to test:**

| Property | Generator | Invariant |
|----------|-----------|-----------|
| `serialization_round_trip` | `arb_mutation()` | For any Mutation, serialize then deserialize is identity |
| `append_replay_round_trip` | `arb_mutation_sequence(1..20)` | For any sequence, append all then replay recovers all in order |
| `cas_allocation_non_overlapping` | Explicit: N threads × random sizes | All `(offset, len)` ranges are disjoint with no gaps |
| `segment_rotation_preserves_data` | `arb_mutation_sequence(1..50)` with 1KB segments | All mutations recoverable even when spanning segment boundaries |
| `flush_tracking_correctness` | Mutations + flush schedule | A segment is deleted iff every dirty table has flushed past it |
| `crash_recovery_completeness` | `arb_mutation_sequence(1..20)` + `arb_crash_point()` | Write N mutations, truncate segment at random position, replay recovers all mutations before the last successful sync point |
| `crash_recovery_no_duplicates` | `arb_mutation_sequence(1..20)` | Write mutations, checkpoint, replay after checkpoint produces no already-flushed mutations |
| `sync_marker_chain_integrity` | `arb_mutation_sequence(1..30)` | Following the marker chain visits every sync section exactly once and terminates at EOF marker |
| `commutativity_of_discard` | Two `TableId`s + mutations | `discard(A); discard(B)` and `discard(B); discard(A)` produce the same segment cleanup |
| `checkpoint_atomicity` | `arb_mutation_sequence(1..10)` | Write checkpoint, corrupt the temp file mid-rename (simulate crash), old checkpoint is still valid |

Define `arb_mutation()`, `arb_row()`, and `arb_mutation_sequence()` locally in this file. These duplicate the versions in `mutation.rs` because those are behind `#[cfg(test)]` and are not accessible from integration test files. They compose `arb_decorated_key()` and `arb_cell_value()` from `ferrosa-common`, with `Row`/`DeletionTime`/`LivenessInfo` from `ferrosa-sstable`.

Also define commit-log-specific generators locally:

- `arb_crash_point(segment_size: usize)` — random byte offset after header (17..segment_size)
- `arb_flush_schedule(num_mutations: usize)` — random indices where flushes occur

- [ ] **Step 2: Run property tests**

Run: `cargo test -p ferrosa-storage --test commitlog_property`
Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add ferrosa-storage/tests/commitlog_property.rs
git commit -m "test(storage): add commit log property tests with shared generators"
```

---

### Task 12: Final Verification and Cleanup

**Files:**

- Modify: `ferrosa-storage/src/lib.rs` (update doc comment)
- Modify: `ferrosa-storage/src/commitlog/mod.rs` (ensure all re-exports)

- [ ] **Step 1: Update lib.rs doc comment**

Update the module-level doc comment to reflect Part B:

```rust
//! Single-node storage engine for Ferrosa.
//!
//! # Components
//!
//! - **Memtable**: in-memory write buffer (Part A)
//! - **Flush**: memtable → SSTable (Part A)
//! - **Merge**: read-path merge across sources (Part A)
//! - **TableStore**: lock-free composition (Part A)
//! - **CommitLog**: write-ahead log for durability (Part B)
```

- [ ] **Step 2: Run full test suite**

Run: `cargo test -p ferrosa-storage`
Expected: All tests pass (Part A + Part B).

- [ ] **Step 3: Run clippy and fmt**

Run: `cargo clippy -p ferrosa-storage --all-targets` and `cargo fmt -p ferrosa-storage --check`
Expected: No warnings, no format issues.

- [ ] **Step 4: Run full workspace tests**

Run: `cargo test`
Expected: All workspace tests pass.

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/src/lib.rs ferrosa-storage/src/commitlog/mod.rs
git commit -m "docs(storage): update module docs with Part B commit log"
```
