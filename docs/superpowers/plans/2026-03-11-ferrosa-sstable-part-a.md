# ferrosa-sstable Implementation Plan — Part A

> **Status:** COMPLETED (2026-03-11). All 12 tasks executed successfully. 107 tests passing across workspace. See [Implementation Notes](#implementation-notes) for deviations from plan.
>
> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Implement the `ferrosa-sstable` crate: read and write Cassandra-compatible BTI (Big Trie-Indexed) SSTables.

**Architecture:** Bottom-up by component. Each module is independently testable before the next begins. Strict TDD (red-green-refactor). All I/O through synchronous `ReadAt`/`WriteAt` traits. External deps: `ferrosa-common`, `lz4_flex`, `zstd`, `proptest` (dev), `tempfile` (dev).

**Tech Stack:** Rust 2021, ferrosa-common, lz4_flex, zstd

**Split into two parts:**

- **Part A** (this file): Crate setup, ferrosa-common swdev improvements, leaf components (io, varint, types, compression, bloom, byte_comparable)
- **[Part B](2026-03-11-ferrosa-sstable-part-b.md)**: Trie, file format readers/writers, public API

**Reference documents:**

- [SSTable Format Specification](../../../specs/sstable.md) — byte-level BTI format
- [Design Doc](../specs/2026-03-11-ferrosa-sstable-design.md) — module structure, dependencies, test strategy

---

## Chunk 1: Crate Scaffolding + ferrosa-common swdev Improvements

### Task 1: Scaffold ferrosa-sstable Crate

**Files:**

- Create: `ferrosa-sstable/Cargo.toml`
- Create: `ferrosa-sstable/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)

- [x] **Step 1: Create crate Cargo.toml**

```toml
[package]
name = "ferrosa-sstable"
description = "Cassandra-compatible BTI SSTable reader/writer for Ferrosa"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
lz4_flex = "0.11"
zstd = "0.13"

[dev-dependencies]
proptest = "1"
tempfile = "3"
```

- [x] **Step 2: Create lib.rs stub**

```rust
//! Cassandra-compatible BTI SSTable reader and writer.
//!
//! This crate reads and writes [BTI (Big Trie-Indexed)][bti] SSTables,
//! the default on-disk format in Apache Cassandra 5.x. It provides:
//!
//! - [`ReadAt`] / [`WriteAt`] — positional I/O traits decoupled from filesystem vs S3
//! - [`SSTableReader`] — open and query BTI SSTables
//! - [`SSTableWriter`] — write new BTI SSTables from sorted input
//!
//! # Architecture
//!
//! SSTables consist of 7 component files (Data.db, Partitions.db, Rows.db,
//! Filter.db, CompressionInfo.db, Statistics.db, TOC.txt). This crate handles
//! all components through composable internal modules, with the public API
//! exposed via [`SSTableReader`] and [`SSTableWriter`].
//!
//! All I/O is synchronous via the [`ReadAt`]/[`WriteAt`] traits. Async
//! wrappers (for S3) live in `ferrosa-storage`.
//!
//! [bti]: https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html

pub mod io;
```

- [x] **Step 3: Add ferrosa-sstable to workspace members**

In the root `Cargo.toml`, add `"ferrosa-sstable"` to the members list:

```toml
[workspace]
resolver = "2"
members = [
    "ferrosa-common",
    "ferrosa-sstable",
]
```

- [x] **Step 4: Verify workspace builds**

Run: `cargo build`
Expected: Compiles with 0 errors

- [x] **Step 5: Commit**

```bash
git add Cargo.toml ferrosa-sstable/
git commit -m "feat(sstable): scaffold ferrosa-sstable crate"
```

### Task 2: Add cargo doc to CI

**Files:**

- Modify: `.github/workflows/ci.yml`

- [x] **Step 1: Add cargo doc step to CI**

Add after the `cargo test` line:

```yaml
      - run: cargo doc --no-deps -D warnings
```

- [x] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add cargo doc --no-deps -D warnings to CI"
```

### Task 3: Add module docs to ferrosa-common

**Files:**

- Modify: `ferrosa-common/src/murmur3.rs`
- Modify: `ferrosa-common/src/token.rs`
- Modify: `ferrosa-common/src/key.rs`
- Modify: `ferrosa-common/src/cell.rs`
- Modify: `ferrosa-common/src/error.rs`

- [x] **Step 1: Add `//!` module doc to murmur3.rs**

Add at the top of `ferrosa-common/src/murmur3.rs` (before the existing `///` doc):

```rust
//! Cassandra-compatible Murmur3 x64_128 hash function.
//!
//! Produces identical output to Cassandra's
//! `org.apache.cassandra.utils.MurmurHash.hash3_x64_128`. Returns `(h1, h2)`:
//! - `h1` is the [`Token`](crate::Token) value (hash ring position)
//! - Both `h1` and `h2` are used for Bloom filter double-hashing
//!
//! # Cassandra Sign Bug
//!
//! The tail bytes use sign-extension (`as i8 as i64`) matching Java's signed
//! byte cast, while the body uses zero-extension (`& 0xff`). This asymmetry
//! is intentional and must be preserved for compatibility.
//!
//! # Testing
//!
//! Characterization vectors generated from Cassandra source ensure exact
//! compatibility. See [`tests`](mod@tests) and `tools/generate_murmur3_vectors.java`.
```

- [x] **Step 2: Add `//!` module doc to token.rs**

Add at the top of `ferrosa-common/src/token.rs`:

```rust
//! Token type representing a position on the Murmur3 hash ring.
//!
//! A [`Token`] wraps an `i64` produced by [`murmur3::hash3_x64_128`](crate::murmur3::hash3_x64_128).
//! The full `i64::MIN..=i64::MAX` range is used, matching Cassandra's `LongToken`.
//! Tokens determine data placement: which node owns a partition and where it
//! appears in SSTables.
```

- [x] **Step 3: Add `//!` module doc to key.rs**

Add at the top of `ferrosa-common/src/key.rs`:

```rust
//! Partition key types: [`PartitionKey`] (raw bytes) and [`DecoratedKey`]
//! (key + cached token).
//!
//! [`DecoratedKey`] is the primary key type throughout the storage engine.
//! It pairs raw partition key bytes with their Murmur3 token, computed once
//! at construction. Ordering is by token first, then by raw key bytes for
//! tie-breaking (matching Cassandra's `DecoratedKey.compareTo`).
//!
//! # Bloom Filter Integration
//!
//! [`DecoratedKey::filter_hash`] returns both `(h1, h2)` from Murmur3 for
//! Bloom filter double-hashing in `ferrosa-sstable`.
```

- [x] **Step 4: Add `//!` module doc to cell.rs**

Add at the top of `ferrosa-common/src/cell.rs`:

```rust
//! Cell values: the atomic unit of data in Cassandra/Ferrosa.
//!
//! A [`CellValue`] belongs to a specific column in a specific row and carries
//! a value along with metadata (timestamp, TTL, local deletion time) used for
//! last-write-wins conflict resolution and TTL expiration.
//!
//! Three cell states:
//! - **Live**: value + timestamp, no expiration
//! - **Expiring**: value + timestamp + TTL + local deletion time
//! - **Tombstone**: no value, marks deletion at a specific timestamp
//!
//! Sentinel constants: [`NO_TIMESTAMP`], [`NO_TTL`], [`NO_DELETION_TIME`].
```

- [x] **Step 5: Add `//!` module doc to error.rs**

Add at the top of `ferrosa-common/src/error.rs`:

```rust
//! Error types shared across all Ferrosa crates.
//!
//! [`Error`] is `#[non_exhaustive]` so new variants can be added without
//! breaking downstream crates. The [`Result`] type alias is re-exported
//! from the crate root for convenience.
//!
//! Key variants for SSTable operations:
//! - [`Error::InvalidFormat`] — file doesn't match expected structure
//! - [`Error::ChecksumMismatch`] — data corruption detected
//! - [`Error::UnsupportedCompression`] — algorithm not yet implemented
```

- [x] **Step 6: Verify cargo doc builds cleanly**

Run: `cargo doc --no-deps -D warnings`
Expected: No warnings, docs build successfully

- [x] **Step 7: Commit**

```bash
git add ferrosa-common/src/
git commit -m "docs(common): add module-level docs to all modules (swdev compliance)"
```

### Task 4: Add doc-tests to ferrosa-common

**Files:**

- Modify: `ferrosa-common/src/token.rs`
- Modify: `ferrosa-common/src/key.rs`
- Modify: `ferrosa-common/src/cell.rs`
- Modify: `ferrosa-common/src/murmur3.rs`

- [x] **Step 1: Add doc-test to `Token::from_key`**

Replace the existing `///` doc on `Token::from_key` with:

```rust
    /// Compute the token for a raw partition key by hashing with Murmur3.
    ///
    /// ```
    /// use ferrosa_common::Token;
    ///
    /// let token = Token::from_key(b"hello");
    /// assert_eq!(token, Token::from_key(b"hello")); // deterministic
    /// assert_ne!(token, Token::from_key(b"world")); // different keys differ
    /// ```
```

- [x] **Step 2: Add doc-test to `DecoratedKey::new`**

Replace the existing `///` doc on `DecoratedKey::new` with:

```rust
    /// Create a DecoratedKey by hashing the partition key with Murmur3.
    ///
    /// ```
    /// use ferrosa_common::{DecoratedKey, PartitionKey, Token};
    ///
    /// let dk = DecoratedKey::new(PartitionKey::from(b"hello".as_slice()));
    /// assert_eq!(dk.token, Token::from_key(b"hello"));
    /// ```
```

- [x] **Step 3: Add doc-test to `CellValue::live`**

Replace the existing `///` doc on `CellValue::live` with:

```rust
    /// A live cell with a value and timestamp, no expiration.
    ///
    /// ```
    /// use ferrosa_common::CellValue;
    ///
    /// let cell = CellValue::live(b"hello".to_vec(), 1000);
    /// assert!(cell.is_live());
    /// assert!(!cell.is_tombstone());
    /// ```
```

- [x] **Step 4: Add doc-test to `CellValue::tombstone`**

Replace the existing `///` doc on `CellValue::tombstone` with:

```rust
    /// A tombstone marking deletion at the given timestamp.
    ///
    /// ```
    /// use ferrosa_common::CellValue;
    ///
    /// let cell = CellValue::tombstone(1000, 1700000000);
    /// assert!(cell.is_tombstone());
    /// assert!(cell.value.is_none());
    /// ```
```

- [x] **Step 5: Add doc-test to `hash3_x64_128`**

Replace the existing `///` doc on `hash3_x64_128` with:

```rust
/// Cassandra-compatible Murmur3 x64_128 hash function.
///
/// Returns `(h1, h2)` where:
/// - `h1` is used as the Token value (position on the hash ring)
/// - Both `h1` and `h2` are used for Bloom filter hashing
///
/// Reference: `org.apache.cassandra.utils.MurmurHash.hash3_x64_128`
///
/// ```
/// use ferrosa_common::murmur3::hash3_x64_128;
///
/// let (h1, h2) = hash3_x64_128(b"hello", 0);
/// assert_eq!(h1, -3758069500696749310_i64);
/// assert_eq!(h2, 6565844092913065241_i64);
/// ```
```

- [x] **Step 6: Run doc-tests**

Run: `cargo test --doc -p ferrosa-common`
Expected: All doc-tests PASS

- [x] **Step 7: Commit**

```bash
git add ferrosa-common/src/
git commit -m "docs(common): add doc-tests to key public methods (swdev compliance)"
```

### Task 5: Add proptest Property Tests to ferrosa-common

**Files:**

- Modify: `ferrosa-common/Cargo.toml`
- Create: `ferrosa-common/tests/property_tests.rs`

- [x] **Step 1: Add proptest dev-dependency**

Add to `ferrosa-common/Cargo.toml`:

```toml
[dev-dependencies]
proptest = "1"
```

- [x] **Step 2: Write property tests**

Create `ferrosa-common/tests/property_tests.rs`:

```rust
//! Property-based tests for ferrosa-common types.

use ferrosa_common::murmur3::hash3_x64_128;
use ferrosa_common::{DecoratedKey, PartitionKey, Token};
use proptest::prelude::*;

proptest! {
    /// Murmur3 is deterministic: same input always produces same output.
    #[test]
    fn murmur3_deterministic(data: Vec<u8>, seed: i64) {
        let a = hash3_x64_128(&data, seed);
        let b = hash3_x64_128(&data, seed);
        prop_assert_eq!(a, b);
    }

    /// Token::from_key is deterministic.
    #[test]
    fn token_from_key_deterministic(key: Vec<u8>) {
        let a = Token::from_key(&key);
        let b = Token::from_key(&key);
        prop_assert_eq!(a, b);
    }

    /// DecoratedKey ordering is consistent: token-first, then key bytes.
    #[test]
    fn decorated_key_ordering_consistent(
        key_a: Vec<u8>,
        key_b: Vec<u8>,
    ) {
        let dk_a = DecoratedKey::new(PartitionKey::new(key_a));
        let dk_b = DecoratedKey::new(PartitionKey::new(key_b));

        // Ordering must be total and consistent
        let cmp_ab = dk_a.cmp(&dk_b);
        let cmp_ba = dk_b.cmp(&dk_a);
        prop_assert_eq!(cmp_ab, cmp_ba.reverse());

        // Token comparison drives the primary order
        if dk_a.token != dk_b.token {
            prop_assert_eq!(cmp_ab, dk_a.token.cmp(&dk_b.token));
        }
    }

    /// DecoratedKey::filter_hash h1 always equals the token value.
    #[test]
    fn filter_hash_h1_equals_token(key: Vec<u8>) {
        let dk = DecoratedKey::new(PartitionKey::new(key));
        let (h1, _h2) = dk.filter_hash();
        prop_assert_eq!(h1, dk.token.0);
    }
}
```

- [x] **Step 3: Run property tests**

Run: `cargo test -p ferrosa-common --test property_tests`
Expected: All property tests PASS

- [x] **Step 4: Commit**

```bash
git add ferrosa-common/
git commit -m "test(common): add proptest property tests (swdev compliance)"
```

---

## Chunk 2: I/O Traits, VarInt Encoding, SSTable Types

### Task 6: io.rs — ReadAt / WriteAt Traits + File Implementations

**Files:**

- Create: `ferrosa-sstable/src/io.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [x] **Step 1: Write failing tests**

Create `ferrosa-sstable/src/io.rs`:

```rust
//! Positional I/O traits and file-system implementations.
//!
//! [`ReadAt`] and [`WriteAt`] decouple SSTable logic from the backing store.
//! This module provides file-system implementations ([`FileReadAt`] and
//! [`FileWriteAt`]) using `pread`/`pwrite` on Unix. The S3 implementation
//! lives in `ferrosa-storage`.
//!
//! # Design
//!
//! Positional I/O avoids shared file offset state, making it safe to read
//! from multiple threads without external synchronization (each call specifies
//! its offset). This matches SSTable access patterns where multiple index
//! lookups happen concurrently.

use ferrosa_common::Result;

/// Positional read — read bytes at an offset without seeking.
///
/// ```no_run
/// use ferrosa_sstable::io::ReadAt;
/// use ferrosa_common::Result;
///
/// fn read_header(reader: &impl ReadAt) -> Result<[u8; 4]> {
///     let mut buf = [0u8; 4];
///     reader.read_at(&mut buf, 0)?;
///     Ok(buf)
/// }
/// ```
pub trait ReadAt {
    /// Read bytes into `buf` starting at `offset`.
    /// Returns the number of bytes read (may be less than `buf.len()` at EOF).
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Returns the total length of the underlying data.
    fn len(&self) -> Result<u64>;

    /// Returns true if the underlying data is empty.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Read exactly `buf.len()` bytes at `offset`, or return an error.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let n = self.read_at(buf, offset)?;
        if n != buf.len() {
            return Err(ferrosa_common::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("read_exact_at: wanted {} bytes, got {}", buf.len(), n),
            )));
        }
        Ok(())
    }
}

/// Positional write — write bytes at an offset.
pub trait WriteAt {
    /// Write `buf` starting at `offset`.
    /// Returns the number of bytes written.
    fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<usize>;

    /// Flush any buffered data to the underlying store.
    fn flush(&mut self) -> Result<()>;

    /// Write all bytes in `buf` at `offset`, or return an error.
    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        let n = self.write_at(buf, offset)?;
        if n != buf.len() {
            return Err(ferrosa_common::Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("write_all_at: wanted {} bytes, wrote {}", buf.len(), n),
            )));
        }
        Ok(())
    }
}

/// File-system implementation of [`ReadAt`] using `pread` on Unix.
pub struct FileReadAt {
    file: std::fs::File,
}

impl FileReadAt {
    /// Open a file for positional reading.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        Ok(Self { file })
    }
}

impl ReadAt for FileReadAt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            Ok(self.file.read_at(buf, offset)?)
        }
        #[cfg(not(unix))]
        {
            // Fallback: seek + read (not thread-safe, but functional)
            use std::io::{Read, Seek, SeekFrom};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            Ok(file.read(buf)?)
        }
    }

    fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

/// File-system implementation of [`WriteAt`] using `pwrite` on Unix.
pub struct FileWriteAt {
    file: std::fs::File,
}

impl FileWriteAt {
    /// Create a new file for positional writing.
    pub fn create(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self { file })
    }
}

impl WriteAt for FileWriteAt {
    fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            Ok(self.file.write_at(buf, offset)?)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            self.file.seek(SeekFrom::Start(offset))?;
            Ok(self.file.write(buf)?)
        }
    }

    fn flush(&mut self) -> Result<()> {
        use std::io::Write;
        Ok(self.file.flush()?)
    }
}

/// In-memory implementation of [`ReadAt`] for testing.
impl ReadAt for &[u8] {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset = offset as usize;
        if offset >= self.len() {
            return Ok(0);
        }
        let available = &self[offset..];
        let n = buf.len().min(available.len());
        buf[..n].copy_from_slice(&available[..n]);
        Ok(n)
    }

    fn len(&self) -> Result<u64> {
        Ok((*self).len() as u64)
    }
}

/// In-memory implementation of [`ReadAt`] for testing.
impl ReadAt for Vec<u8> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.as_slice().read_at(buf, offset)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_read_at_basic() {
        let data: &[u8] = b"hello world";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn slice_read_at_offset() {
        let data: &[u8] = b"hello world";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 6).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn slice_read_at_past_eof() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 100).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn slice_read_at_partial() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
    }

    #[test]
    fn slice_len() {
        let data: &[u8] = b"hello";
        assert_eq!(ReadAt::len(&data).unwrap(), 5);
    }

    #[test]
    fn slice_is_empty() {
        let empty: &[u8] = b"";
        assert!(ReadAt::is_empty(&empty).unwrap());
        let nonempty: &[u8] = b"x";
        assert!(!ReadAt::is_empty(&nonempty).unwrap());
    }

    #[test]
    fn read_exact_at_success() {
        let data: &[u8] = b"hello";
        let mut buf = [0u8; 5];
        data.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn read_exact_at_eof_error() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let err = data.read_exact_at(&mut buf, 0).unwrap_err();
        assert!(err.to_string().contains("wanted 5 bytes, got 2"));
    }

    #[test]
    fn file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let data = b"hello world from ferrosa";
        {
            let mut writer = FileWriteAt::create(&path).unwrap();
            writer.write_all_at(data, 0).unwrap();
            writer.flush().unwrap();
        }

        let reader = FileReadAt::open(&path).unwrap();
        assert_eq!(reader.len().unwrap(), data.len() as u64);

        let mut buf = vec![0u8; data.len()];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, data);

        // Partial read at offset
        let mut buf2 = [0u8; 5];
        reader.read_exact_at(&mut buf2, 6).unwrap();
        assert_eq!(&buf2, b"world");
    }

    #[test]
    fn file_write_at_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("offset.dat");

        let mut writer = FileWriteAt::create(&path).unwrap();
        // Write "hello" at offset 0
        writer.write_all_at(b"hello", 0).unwrap();
        // Write "world" at offset 10
        writer.write_all_at(b"world", 10).unwrap();
        writer.flush().unwrap();

        let reader = FileReadAt::open(&path).unwrap();
        assert_eq!(reader.len().unwrap(), 15);

        let mut buf = [0u8; 5];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");

        reader.read_exact_at(&mut buf, 10).unwrap();
        assert_eq!(&buf, b"world");
    }
}
```

- [x] **Step 2: Register module in lib.rs**

Already done in Task 1 (`pub mod io;`).

- [x] **Step 3: Run tests**

Run: `cargo test -p ferrosa-sstable`
Expected: All tests PASS

- [x] **Step 4: Run clippy**

Run: `cargo clippy -p ferrosa-sstable -- -D warnings`
Expected: No warnings

- [x] **Step 5: Commit**

```bash
git add ferrosa-sstable/src/io.rs
git commit -m "feat(sstable): add ReadAt/WriteAt traits + file and slice impls"
```

### Task 7: varint.rs — Unsigned/Signed Variable-Length Integer Encoding

**Files:**

- Create: `ferrosa-sstable/src/varint.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [x] **Step 1: Write tests and implementation**

Create `ferrosa-sstable/src/varint.rs`:

```rust
//! Cassandra-compatible variable-length integer encoding.
//!
//! Uses a leading-ones prefix (NOT protobuf-style). The number of leading
//! 1-bits in the first byte indicates extra bytes to read. Remaining bits
//! in the first byte are the most-significant value bits, followed by
//! subsequent bytes in big-endian order.
//!
//! **Signed varints** use zigzag encoding before the unsigned encoding:
//! `0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4, …`
//!
//! Reference: `org.apache.cassandra.utils.vint.VIntCoding`
//!
//! # Examples
//!
//! ```
//! use ferrosa_sstable::varint;
//!
//! // Unsigned round-trip
//! let mut buf = [0u8; 9];
//! let n = varint::write_unsigned_vint(&mut buf, 128);
//! assert_eq!(n, 2);
//! assert_eq!(&buf[..2], &[0x80, 0x80]);
//!
//! let (value, consumed) = varint::read_unsigned_vint(&buf).unwrap();
//! assert_eq!(value, 128);
//! assert_eq!(consumed, 2);
//! ```

use ferrosa_common::{Error, Result};

/// Write an unsigned varint to `buf`. Returns the number of bytes written.
///
/// `buf` must be at least 9 bytes long (maximum varint size).
pub fn write_unsigned_vint(buf: &mut [u8], value: u64) -> usize {
    let extra_bytes = unsigned_vint_size(value) - 1;
    if extra_bytes == 0 {
        buf[0] = value as u8;
        return 1;
    }

    let total = extra_bytes + 1;

    // Write value bytes in big-endian order (rightmost first)
    let mut remaining = value;
    for i in (1..total).rev() {
        buf[i] = remaining as u8;
        remaining >>= 8;
    }

    // First byte: leading ones prefix + remaining value bits
    if extra_bytes < 8 {
        let mask = !0u8 >> (8 - extra_bytes); // leading ones
        let shift = 8 - extra_bytes;
        buf[0] = (mask << shift) | (remaining as u8);
    } else {
        // 9-byte encoding: first byte is 0xFF, then 8 raw bytes
        buf[0] = 0xFF;
    }

    total
}

/// Read an unsigned varint from `buf`. Returns `(value, bytes_consumed)`.
pub fn read_unsigned_vint(buf: &[u8]) -> Result<(u64, usize)> {
    if buf.is_empty() {
        return Err(Error::InvalidData("empty buffer for varint".into()));
    }

    let first = buf[0];
    let extra_bytes = first.leading_ones() as usize;

    if extra_bytes == 0 {
        return Ok((first as u64, 1));
    }

    let total = extra_bytes + 1;
    if buf.len() < total {
        return Err(Error::InvalidData(format!(
            "varint needs {} bytes, only {} available",
            total,
            buf.len()
        )));
    }

    let mut value: u64;
    if extra_bytes < 8 {
        // Mask off the leading ones from the first byte
        value = (first & (0xFF >> (extra_bytes + 1))) as u64;
    } else {
        // 9-byte form: first byte is 0xFF, ignore it
        value = 0;
    }

    for i in 1..total {
        value = (value << 8) | buf[i] as u64;
    }

    Ok((value, total))
}

/// Write a signed varint (zigzag-encoded). Returns bytes written.
pub fn write_signed_vint(buf: &mut [u8], value: i64) -> usize {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    write_unsigned_vint(buf, zigzag)
}

/// Read a signed varint (zigzag-decoded). Returns `(value, bytes_consumed)`.
pub fn read_signed_vint(buf: &[u8]) -> Result<(i64, usize)> {
    let (raw, consumed) = read_unsigned_vint(buf)?;
    let value = ((raw >> 1) as i64) ^ (-((raw & 1) as i64));
    Ok((value, consumed))
}

/// Returns the number of bytes needed to encode `value` as an unsigned varint.
pub fn unsigned_vint_size(value: u64) -> usize {
    // Number of significant bits, then map to byte count
    let bits = 64 - value.leading_zeros() as usize;
    match bits {
        0..=7 => 1,
        8..=14 => 2,
        15..=21 => 3,
        22..=28 => 4,
        29..=35 => 5,
        36..=42 => 6,
        43..=49 => 7,
        50..=56 => 8,
        _ => 9,
    }
}

/// Read an unsigned varint from a [`ReadAt`](crate::io::ReadAt) source at the given offset.
/// Returns `(value, bytes_consumed)`.
pub fn read_unsigned_vint_at(reader: &impl crate::io::ReadAt, offset: u64) -> Result<(u64, usize)> {
    let mut first = [0u8; 1];
    reader.read_exact_at(&mut first, offset)?;

    let extra_bytes = first[0].leading_ones() as usize;
    let total = extra_bytes + 1;

    if total == 1 {
        return Ok((first[0] as u64, 1));
    }

    let mut buf = [0u8; 9];
    buf[0] = first[0];
    reader.read_exact_at(&mut buf[1..total], offset + 1)?;

    read_unsigned_vint(&buf[..total])
}

/// Read a signed varint from a [`ReadAt`](crate::io::ReadAt) source at the given offset.
/// Returns `(value, bytes_consumed)`.
pub fn read_signed_vint_at(reader: &impl crate::io::ReadAt, offset: u64) -> Result<(i64, usize)> {
    let (raw, consumed) = read_unsigned_vint_at(reader, offset)?;
    let value = ((raw >> 1) as i64) ^ (-((raw & 1) as i64));
    Ok((value, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Unsigned varint ---

    #[test]
    fn unsigned_zero() {
        let mut buf = [0u8; 9];
        let n = write_unsigned_vint(&mut buf, 0);
        assert_eq!(n, 1);
        assert_eq!(buf[0], 0x00);

        let (val, consumed) = read_unsigned_vint(&buf[..n]).unwrap();
        assert_eq!(val, 0);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn unsigned_single_byte_max() {
        let mut buf = [0u8; 9];
        let n = write_unsigned_vint(&mut buf, 127);
        assert_eq!(n, 1);
        assert_eq!(buf[0], 0x7F);

        let (val, consumed) = read_unsigned_vint(&buf[..n]).unwrap();
        assert_eq!(val, 127);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn unsigned_128_two_bytes() {
        let mut buf = [0u8; 9];
        let n = write_unsigned_vint(&mut buf, 128);
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], &[0x80, 0x80]);

        let (val, consumed) = read_unsigned_vint(&buf[..n]).unwrap();
        assert_eq!(val, 128);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn unsigned_255_two_bytes() {
        let mut buf = [0u8; 9];
        let n = write_unsigned_vint(&mut buf, 255);
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], &[0x80, 0xFF]);

        let (val, consumed) = read_unsigned_vint(&buf[..n]).unwrap();
        assert_eq!(val, 255);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn unsigned_two_byte_max() {
        // Max 2-byte value: 16383 (14 value bits)
        let mut buf = [0u8; 9];
        let n = write_unsigned_vint(&mut buf, 16383);
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], &[0xBF, 0xFF]);

        let (val, consumed) = read_unsigned_vint(&buf[..n]).unwrap();
        assert_eq!(val, 16383);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn unsigned_nine_byte_max() {
        let mut buf = [0u8; 9];
        let n = write_unsigned_vint(&mut buf, i64::MAX as u64);
        assert_eq!(n, 9);
        assert_eq!(buf[0], 0xFF);

        let (val, consumed) = read_unsigned_vint(&buf[..n]).unwrap();
        assert_eq!(val, i64::MAX as u64);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn unsigned_round_trip_boundary_values() {
        let values = [
            0u64, 1, 127, 128, 255, 256,
            16383, 16384,          // 2-3 byte boundary
            2097151, 2097152,      // 3-4 byte boundary
            268435455, 268435456,  // 4-5 byte boundary
            i64::MAX as u64,
        ];
        for &value in &values {
            let mut buf = [0u8; 9];
            let n = write_unsigned_vint(&mut buf, value);
            let (decoded, consumed) = read_unsigned_vint(&buf[..n]).unwrap();
            assert_eq!(decoded, value, "round-trip failed for {value}");
            assert_eq!(consumed, n, "consumed mismatch for {value}");
        }
    }

    #[test]
    fn unsigned_size_function() {
        assert_eq!(unsigned_vint_size(0), 1);
        assert_eq!(unsigned_vint_size(127), 1);
        assert_eq!(unsigned_vint_size(128), 2);
        assert_eq!(unsigned_vint_size(16383), 2);
        assert_eq!(unsigned_vint_size(16384), 3);
        assert_eq!(unsigned_vint_size(i64::MAX as u64), 9);
    }

    // --- Signed varint ---

    #[test]
    fn signed_zero() {
        let mut buf = [0u8; 9];
        let n = write_signed_vint(&mut buf, 0);
        assert_eq!(n, 1);

        let (val, consumed) = read_signed_vint(&buf[..n]).unwrap();
        assert_eq!(val, 0);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn signed_zigzag_mapping() {
        // Verify zigzag: 0→0, -1→1, 1→2, -2→3, 2→4
        let cases: &[(i64, u64)] = &[
            (0, 0), (-1, 1), (1, 2), (-2, 3), (2, 4),
            (i64::MAX, u64::MAX - 1), (i64::MIN, u64::MAX),
        ];
        for &(signed, expected_zigzag) in cases {
            let zigzag = ((signed << 1) ^ (signed >> 63)) as u64;
            assert_eq!(zigzag, expected_zigzag, "zigzag({signed})");
        }
    }

    #[test]
    fn signed_round_trip_boundary_values() {
        let values = [
            0i64, 1, -1, 63, -64, 64, -65,
            i64::MAX, i64::MIN, i64::MIN + 1,
        ];
        for &value in &values {
            let mut buf = [0u8; 9];
            let n = write_signed_vint(&mut buf, value);
            let (decoded, consumed) = read_signed_vint(&buf[..n]).unwrap();
            assert_eq!(decoded, value, "round-trip failed for {value}");
            assert_eq!(consumed, n, "consumed mismatch for {value}");
        }
    }

    // --- Error cases ---

    #[test]
    fn empty_buffer_error() {
        let result = read_unsigned_vint(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_buffer_error() {
        // First byte says 2 total, but only 1 byte available
        let result = read_unsigned_vint(&[0x80]);
        assert!(result.is_err());
    }

    // --- ReadAt integration ---

    #[test]
    fn read_unsigned_vint_at_basic() {
        let data: &[u8] = &[0x00, 0x80, 0x80, 0x7F];
        // At offset 0: single-byte 0
        let (val, n) = read_unsigned_vint_at(&data, 0).unwrap();
        assert_eq!(val, 0);
        assert_eq!(n, 1);

        // At offset 1: two-byte 128
        let (val, n) = read_unsigned_vint_at(&data, 1).unwrap();
        assert_eq!(val, 128);
        assert_eq!(n, 2);

        // At offset 3: single-byte 127
        let (val, n) = read_unsigned_vint_at(&data, 3).unwrap();
        assert_eq!(val, 127);
        assert_eq!(n, 1);
    }
}
```

- [x] **Step 2: Register module in lib.rs**

Add to `ferrosa-sstable/src/lib.rs`:

```rust
pub mod varint;
```

- [x] **Step 3: Run tests**

Run: `cargo test -p ferrosa-sstable varint`
Expected: All tests PASS

- [x] **Step 4: Commit**

```bash
git add ferrosa-sstable/src/varint.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add VInt encoding/decoding (Cassandra-compatible)"
```

### Task 8: types.rs — DeletionTime, LivenessInfo, Row, Partition

**Files:**

- Create: `ferrosa-sstable/src/types.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [x] **Step 1: Write types with tests**

Create `ferrosa-sstable/src/types.rs`:

```rust
//! SSTable-specific deserialized types.
//!
//! These types represent the in-memory view of data read from SSTables.
//! They live in `ferrosa-sstable` (not `ferrosa-common`) because they are
//! format-specific: fields like `DeletionTime` and `LivenessInfo` map directly
//! to on-disk SSTable encoding, not to the abstract CQL data model.
//!
//! # Key Types
//!
//! - [`DeletionTime`] — partition or row-level deletion marker
//! - [`LivenessInfo`] — primary key liveness (timestamp + TTL)
//! - [`Row`] — a deserialized row from an SSTable
//! - [`Partition`] — a deserialized partition from an SSTable

use ferrosa_common::{CellValue, DecoratedKey};

/// Partition-level or row-level deletion marker.
///
/// # Encoding
///
/// On disk, a live DeletionTime is a single `0x80` byte. A deleted
/// DeletionTime is 12 bytes: 8-byte i64 timestamp + 4-byte u32 local
/// deletion time.
///
/// ```
/// use ferrosa_sstable::types::DeletionTime;
///
/// let live = DeletionTime::LIVE;
/// assert!(live.is_live());
///
/// let deleted = DeletionTime::new(1000, 1700000000);
/// assert!(!deleted.is_live());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionTime {
    /// Microseconds since epoch. `i64::MIN` = live (not deleted).
    pub marked_for_delete_at: i64,
    /// Seconds since epoch. `u32::MAX` = live (not deleted).
    pub local_deletion_time: u32,
}

impl DeletionTime {
    /// Sentinel for live (not deleted) partitions and rows.
    pub const LIVE: DeletionTime = DeletionTime {
        marked_for_delete_at: i64::MIN,
        local_deletion_time: u32::MAX,
    };

    /// Create a deletion marker with the given timestamp and local deletion time.
    pub fn new(marked_for_delete_at: i64, local_deletion_time: u32) -> Self {
        Self {
            marked_for_delete_at,
            local_deletion_time,
        }
    }

    /// Returns true if this represents a live (not deleted) entry.
    pub fn is_live(&self) -> bool {
        self.marked_for_delete_at == i64::MIN && self.local_deletion_time == u32::MAX
    }
}

impl Default for DeletionTime {
    fn default() -> Self {
        Self::LIVE
    }
}

/// Primary key liveness info for a row.
///
/// Every CQL INSERT sets liveness on the primary key. If a row exists only
/// because of cell-level writes (UPDATE), it may have no liveness info.
///
/// ```
/// use ferrosa_sstable::types::LivenessInfo;
///
/// let no_liveness = LivenessInfo::NONE;
/// assert!(!no_liveness.has_timestamp());
///
/// let live = LivenessInfo::with_timestamp(1000);
/// assert!(live.has_timestamp());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessInfo {
    /// Microseconds since epoch. `i64::MIN` = no liveness.
    pub timestamp: i64,
    /// TTL in seconds. 0 = no TTL.
    pub ttl: i32,
    /// Local deletion time in seconds. `i32::MAX` = no expiry.
    pub local_deletion_time: i32,
}

impl LivenessInfo {
    /// No liveness information.
    pub const NONE: LivenessInfo = LivenessInfo {
        timestamp: i64::MIN,
        ttl: 0,
        local_deletion_time: i32::MAX,
    };

    /// Create liveness with a timestamp and no TTL.
    pub fn with_timestamp(timestamp: i64) -> Self {
        Self {
            timestamp,
            ttl: 0,
            local_deletion_time: i32::MAX,
        }
    }

    /// Create liveness with a timestamp, TTL, and expiry time.
    pub fn with_ttl(timestamp: i64, ttl: i32, local_deletion_time: i32) -> Self {
        Self {
            timestamp,
            ttl,
            local_deletion_time,
        }
    }

    pub fn has_timestamp(&self) -> bool {
        self.timestamp != i64::MIN
    }

    pub fn has_ttl(&self) -> bool {
        self.ttl != 0
    }
}

impl Default for LivenessInfo {
    fn default() -> Self {
        Self::NONE
    }
}

/// A deserialized row from an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// Raw clustering key bytes.
    pub clustering: Vec<u8>,
    /// Column data: `(column_index, cell_value)` pairs.
    pub cells: Vec<(u16, CellValue)>,
    /// Row-level deletion.
    pub deletion: DeletionTime,
    /// Primary key liveness (set by INSERT, absent for UPDATE-only rows).
    pub primary_key_liveness: LivenessInfo,
}

/// A deserialized partition from an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    /// The partition's decorated key (key bytes + token).
    pub key: DecoratedKey,
    /// Partition-level deletion.
    pub deletion: DeletionTime,
    /// Static row (columns shared across all clustered rows).
    pub static_row: Option<Row>,
    /// Clustered rows in clustering order.
    pub rows: Vec<Row>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::PartitionKey;

    #[test]
    fn deletion_time_live() {
        assert!(DeletionTime::LIVE.is_live());
        assert!(DeletionTime::default().is_live());
    }

    #[test]
    fn deletion_time_deleted() {
        let dt = DeletionTime::new(1000, 1700000000);
        assert!(!dt.is_live());
        assert_eq!(dt.marked_for_delete_at, 1000);
        assert_eq!(dt.local_deletion_time, 1700000000);
    }

    #[test]
    fn liveness_none() {
        assert!(!LivenessInfo::NONE.has_timestamp());
        assert!(!LivenessInfo::NONE.has_ttl());
    }

    #[test]
    fn liveness_with_timestamp() {
        let li = LivenessInfo::with_timestamp(1000);
        assert!(li.has_timestamp());
        assert!(!li.has_ttl());
        assert_eq!(li.timestamp, 1000);
    }

    #[test]
    fn liveness_with_ttl() {
        let li = LivenessInfo::with_ttl(1000, 3600, 1700003600);
        assert!(li.has_timestamp());
        assert!(li.has_ttl());
        assert_eq!(li.ttl, 3600);
        assert_eq!(li.local_deletion_time, 1700003600);
    }

    #[test]
    fn row_construction() {
        let row = Row {
            clustering: vec![1, 2, 3],
            cells: vec![
                (0, CellValue::live(b"hello".to_vec(), 1000)),
                (1, CellValue::tombstone(2000, 1700000000)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        assert_eq!(row.cells.len(), 2);
        assert!(row.cells[0].1.is_live());
        assert!(row.cells[1].1.is_tombstone());
    }

    #[test]
    fn partition_construction() {
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"test".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![1],
                cells: vec![(0, CellValue::live(b"v".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
        };
        assert!(partition.deletion.is_live());
        assert!(partition.static_row.is_none());
        assert_eq!(partition.rows.len(), 1);
    }
}
```

- [x] **Step 2: Register module in lib.rs**

Add to `ferrosa-sstable/src/lib.rs`:

```rust
pub mod types;
```

- [x] **Step 3: Run tests**

Run: `cargo test -p ferrosa-sstable types`
Expected: All tests PASS

- [x] **Step 4: Commit**

```bash
git add ferrosa-sstable/src/types.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add DeletionTime, LivenessInfo, Row, Partition types"
```

---

## Chunk 3: Compression, Bloom Filter, Byte-Comparable Keys

### Task 9: compression.rs — LZ4/Zstd + CompressionInfo

**Files:**

- Create: `ferrosa-sstable/src/compression.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [x] **Step 1: Write compression module with tests**

Create `ferrosa-sstable/src/compression.rs`:

```rust
//! Compression and CompressionInfo for SSTable data chunks.
//!
//! SSTable data is stored in compressed chunks (default 16KB uncompressed).
//! Chunks are position-based — every N uncompressed bytes, regardless of
//! partition boundaries.
//!
//! Supported algorithms:
//! - **LZ4** (`lz4_flex`): Default. Fast compression/decompression.
//! - **Zstd** (`zstd`): Better ratio, slightly slower.
//! - **None**: Uncompressed. Uses CRC.db instead of CompressionInfo.db.
//!
//! # CompressionInfo Format
//!
//! See `specs/sstable.md` § Compression Info for the full byte-level format.
//! Key fields: compressor name (Java UTF-8), chunk length, data length,
//! chunk offsets (i64 array).
//!
//! Reference: `org.apache.cassandra.io.compress.CompressionMetadata`

use ferrosa_common::{Error, Result};

/// Compression algorithm selection.
#[derive(Debug, Clone, PartialEq)]
pub enum Compression {
    /// No compression. CRC.db written instead of CompressionInfo.db.
    None,
    /// LZ4 compression (default in Cassandra).
    Lz4,
    /// Zstd compression with configurable level.
    Zstd { level: i32 },
}

impl Compression {
    /// Default chunk size matching Cassandra's DEFAULT_CHUNK_LENGTH.
    pub const DEFAULT_CHUNK_SIZE: usize = 16384;

    /// Compress a block of data.
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            Compression::Zstd { level } => {
                zstd::encode_all(data, *level).map_err(|e| Error::Io(e.into()))
            }
        }
    }

    /// Decompress a block of data.
    pub fn decompress(&self, data: &[u8], _uncompressed_len: usize) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Lz4 => lz4_flex::decompress_size_prepended(data).map_err(|e| {
                Error::InvalidData(format!("LZ4 decompression failed: {e}"))
            }),
            Compression::Zstd { .. } => {
                zstd::decode_all(data).map_err(|e| Error::Io(e.into()))
            }
        }
    }

    /// Returns the compressor name as stored in CompressionInfo.db.
    pub fn compressor_name(&self) -> Option<&str> {
        match self {
            Compression::None => None,
            Compression::Lz4 => Some("LZ4Compressor"),
            Compression::Zstd { .. } => Some("ZstdCompressor"),
        }
    }

    /// Parse a compressor name from CompressionInfo.db.
    pub fn from_compressor_name(name: &str) -> Result<Self> {
        match name {
            "LZ4Compressor" => Ok(Compression::Lz4),
            "ZstdCompressor" => Ok(Compression::Zstd { level: 3 }),
            "SnappyCompressor" | "DeflateCompressor" => {
                Err(Error::UnsupportedCompression(name.into()))
            }
            other => Err(Error::UnsupportedCompression(other.into())),
        }
    }
}

/// Parsed CompressionInfo.db metadata.
#[derive(Debug, Clone)]
pub struct CompressionInfo {
    /// Compression algorithm.
    pub compression: Compression,
    /// Uncompressed chunk size in bytes.
    pub chunk_length: usize,
    /// Maximum compressed chunk size.
    pub max_compressed_size: usize,
    /// Total uncompressed data length.
    pub data_length: u64,
    /// File offset of each compressed chunk in Data.db.
    pub chunk_offsets: Vec<u64>,
}

impl CompressionInfo {
    /// Read CompressionInfo from a byte buffer.
    pub fn read(data: &[u8]) -> Result<Self> {
        let mut pos = 0;

        // Compressor name: Java UTF-8 (u16 len + bytes)
        if data.len() < pos + 2 {
            return Err(Error::InvalidFormat("truncated compressor name length".into()));
        }
        let name_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if data.len() < pos + name_len {
            return Err(Error::InvalidFormat("truncated compressor name".into()));
        }
        let name = std::str::from_utf8(&data[pos..pos + name_len])
            .map_err(|e| Error::InvalidFormat(format!("invalid compressor name UTF-8: {e}")))?;
        pos += name_len;

        let compression = Compression::from_compressor_name(name)?;

        // Option count: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated option count".into()));
        }
        let option_count = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        // Skip options (key-value pairs, each Java UTF-8)
        for _ in 0..option_count {
            for _ in 0..2 {
                if data.len() < pos + 2 {
                    return Err(Error::InvalidFormat("truncated option".into()));
                }
                let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2 + len;
            }
        }

        // Chunk length: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated chunk length".into()));
        }
        let chunk_length = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Max compressed size: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated max compressed size".into()));
        }
        let max_compressed_size = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Data length: i64
        if data.len() < pos + 8 {
            return Err(Error::InvalidFormat("truncated data length".into()));
        }
        let data_length = i64::from_be_bytes([
            data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
            data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
        ]) as u64;
        pos += 8;

        // Chunk count: i32
        if data.len() < pos + 4 {
            return Err(Error::InvalidFormat("truncated chunk count".into()));
        }
        let chunk_count = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Chunk offsets: i64[chunk_count]
        let mut chunk_offsets = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            if data.len() < pos + 8 {
                return Err(Error::InvalidFormat("truncated chunk offset".into()));
            }
            let offset = i64::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
            ]) as u64;
            chunk_offsets.push(offset);
            pos += 8;
        }

        Ok(CompressionInfo {
            compression,
            chunk_length,
            max_compressed_size,
            data_length,
            chunk_offsets,
        })
    }

    /// Write CompressionInfo to a byte buffer.
    pub fn write(&self) -> Result<Vec<u8>> {
        let name = self.compression.compressor_name()
            .ok_or_else(|| Error::InvalidData("cannot write CompressionInfo for Compression::None".into()))?;

        let mut buf = Vec::new();

        // Compressor name: Java UTF-8
        buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
        buf.extend_from_slice(name.as_bytes());

        // Options: 0
        buf.extend_from_slice(&0i32.to_be_bytes());

        // Chunk length
        buf.extend_from_slice(&(self.chunk_length as i32).to_be_bytes());

        // Max compressed size
        buf.extend_from_slice(&(self.max_compressed_size as i32).to_be_bytes());

        // Data length
        buf.extend_from_slice(&(self.data_length as i64).to_be_bytes());

        // Chunk count + offsets
        buf.extend_from_slice(&(self.chunk_offsets.len() as i32).to_be_bytes());
        for &offset in &self.chunk_offsets {
            buf.extend_from_slice(&(offset as i64).to_be_bytes());
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Compression round-trip ---

    #[test]
    fn lz4_round_trip() {
        let data = b"hello world hello world hello world";
        let compression = Compression::Lz4;
        let compressed = compression.compress(data).unwrap();
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn zstd_round_trip() {
        let data = b"hello world hello world hello world";
        let compression = Compression::Zstd { level: 3 };
        let compressed = compression.compress(data).unwrap();
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn none_round_trip() {
        let data = b"hello world";
        let compression = Compression::None;
        let compressed = compression.compress(data).unwrap();
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compressor_names() {
        assert_eq!(Compression::Lz4.compressor_name(), Some("LZ4Compressor"));
        assert_eq!(Compression::Zstd { level: 3 }.compressor_name(), Some("ZstdCompressor"));
        assert_eq!(Compression::None.compressor_name(), None);
    }

    #[test]
    fn from_compressor_name() {
        assert!(matches!(Compression::from_compressor_name("LZ4Compressor").unwrap(), Compression::Lz4));
        assert!(matches!(Compression::from_compressor_name("ZstdCompressor").unwrap(), Compression::Zstd { .. }));
        assert!(Compression::from_compressor_name("SnappyCompressor").is_err());
        assert!(Compression::from_compressor_name("Unknown").is_err());
    }

    // --- CompressionInfo round-trip ---

    #[test]
    fn compression_info_round_trip() {
        let info = CompressionInfo {
            compression: Compression::Lz4,
            chunk_length: 16384,
            max_compressed_size: 16393,
            data_length: 65536,
            chunk_offsets: vec![0, 4096, 8192, 12000],
        };

        let written = info.write().unwrap();
        let parsed = CompressionInfo::read(&written).unwrap();

        assert!(matches!(parsed.compression, Compression::Lz4));
        assert_eq!(parsed.chunk_length, 16384);
        assert_eq!(parsed.max_compressed_size, 16393);
        assert_eq!(parsed.data_length, 65536);
        assert_eq!(parsed.chunk_offsets, vec![0, 4096, 8192, 12000]);
    }

    #[test]
    fn compression_info_write_none_errors() {
        let info = CompressionInfo {
            compression: Compression::None,
            chunk_length: 16384,
            max_compressed_size: 0,
            data_length: 0,
            chunk_offsets: vec![],
        };
        assert!(info.write().is_err());
    }

    // --- Empty and large data ---

    #[test]
    fn lz4_empty_data() {
        let compression = Compression::Lz4;
        let compressed = compression.compress(b"").unwrap();
        let decompressed = compression.decompress(&compressed, 0).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn zstd_large_data() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compression = Compression::Zstd { level: 3 };
        let compressed = compression.compress(&data).unwrap();
        assert!(compressed.len() < data.len()); // should compress well
        let decompressed = compression.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }
}
```

- [x] **Step 2: Register module in lib.rs**

Add to `ferrosa-sstable/src/lib.rs`:

```rust
pub mod compression;
```

- [x] **Step 3: Run tests**

Run: `cargo test -p ferrosa-sstable compression`
Expected: All tests PASS

- [x] **Step 4: Commit**

```bash
git add ferrosa-sstable/src/compression.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add LZ4/Zstd compression + CompressionInfo read/write"
```

### Task 10: bloom.rs — Bloom Filter Read/Write

**Files:**

- Create: `ferrosa-sstable/src/bloom.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [x] **Step 1: Write bloom filter module with tests**

Create `ferrosa-sstable/src/bloom.rs`:

```rust
//! Cassandra-compatible Bloom filter using Murmur3 double-hashing.
//!
//! Uses the hash function family `h_i(key) = h1 + i * h2` where `(h1, h2)`
//! come from `murmur3::hash3_x64_128`. The number of hash functions `k` and
//! bit count `m` are computed from the target false-positive rate and expected
//! key count.
//!
//! # File Format (new format, Cassandra 5.x)
//!
//! ```text
//! [hash_count: i32] [word_count: i32] [bit_array: word_count * 8 bytes]
//! ```
//!
//! The bit array is raw bytes (NOT big-endian word-swapped).
//!
//! Reference: `org.apache.cassandra.utils.BloomFilter`,
//!            `org.apache.cassandra.utils.OffHeapBitSet`

use ferrosa_common::{Error, Result};

/// Cassandra-compatible Bloom filter.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Number of hash functions.
    hash_count: i32,
    /// Bit array stored as i64 words.
    bits: Vec<u64>,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for the expected key count and FP rate.
    ///
    /// ```
    /// use ferrosa_sstable::bloom::BloomFilter;
    ///
    /// let bf = BloomFilter::new(100, 0.01);
    /// assert!(!bf.is_present(42, 99)); // empty filter
    /// ```
    pub fn new(expected_keys: usize, fp_rate: f64) -> Self {
        let (num_bits, hash_count) = optimal_params(expected_keys, fp_rate);
        let word_count = (num_bits + 63) / 64;
        BloomFilter {
            hash_count: hash_count as i32,
            bits: vec![0u64; word_count],
        }
    }

    /// Add a key to the filter using its Murmur3 h1 and h2 values.
    ///
    /// Call with `DecoratedKey::filter_hash()` to get `(h1, h2)`.
    pub fn add(&mut self, h1: i64, h2: i64) {
        let num_bits = (self.bits.len() * 64) as i64;
        for i in 0..self.hash_count as i64 {
            let hash = h1.wrapping_add(i.wrapping_mul(h2));
            let bit_index = (hash % num_bits + num_bits) % num_bits;
            let word = bit_index as usize / 64;
            let bit = bit_index as usize % 64;
            self.bits[word] |= 1u64 << bit;
        }
    }

    /// Test if a key might be present (false positives possible).
    pub fn is_present(&self, h1: i64, h2: i64) -> bool {
        let num_bits = (self.bits.len() * 64) as i64;
        for i in 0..self.hash_count as i64 {
            let hash = h1.wrapping_add(i.wrapping_mul(h2));
            let bit_index = (hash % num_bits + num_bits) % num_bits;
            let word = bit_index as usize / 64;
            let bit = bit_index as usize % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Read a Bloom filter from its serialized form (new format).
    pub fn read(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::InvalidFormat("bloom filter too short".into()));
        }

        let hash_count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let word_count = i32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;

        let expected_len = 8 + word_count * 8;
        if data.len() < expected_len {
            return Err(Error::InvalidFormat(format!(
                "bloom filter: expected {} bytes, got {}",
                expected_len,
                data.len()
            )));
        }

        let mut bits = Vec::with_capacity(word_count);
        let mut pos = 8;
        for _ in 0..word_count {
            // Raw byte order (NOT big-endian swapped) — new format
            let word = u64::from_ne_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
            ]);
            bits.push(word);
            pos += 8;
        }

        Ok(BloomFilter { hash_count, bits })
    }

    /// Serialize the Bloom filter (new format).
    pub fn write(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.bits.len() * 8);
        buf.extend_from_slice(&self.hash_count.to_be_bytes());
        buf.extend_from_slice(&(self.bits.len() as i32).to_be_bytes());
        for &word in &self.bits {
            buf.extend_from_slice(&word.to_ne_bytes());
        }
        buf
    }
}

/// Compute optimal Bloom filter parameters.
/// Returns `(num_bits, hash_count)`.
fn optimal_params(expected_keys: usize, fp_rate: f64) -> (usize, usize) {
    let n = expected_keys.max(1) as f64;
    let m = -(n * fp_rate.ln()) / (2.0_f64.ln().powi(2));
    let num_bits = m.ceil() as usize;
    let k = (num_bits as f64 / n * 2.0_f64.ln()).round() as usize;
    let hash_count = k.max(1);
    (num_bits, hash_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_find() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(12345, 67890);
        assert!(bf.is_present(12345, 67890));
    }

    #[test]
    fn absent_key() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(!bf.is_present(12345, 67890));
    }

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::new(1000, 0.01);
        let keys: Vec<(i64, i64)> = (0..1000)
            .map(|i| {
                let data = i.to_le_bytes();
                ferrosa_common::murmur3::hash3_x64_128(&data, 0)
            })
            .collect();

        for &(h1, h2) in &keys {
            bf.add(h1, h2);
        }

        for &(h1, h2) in &keys {
            assert!(bf.is_present(h1, h2), "false negative!");
        }
    }

    #[test]
    fn false_positive_rate_reasonable() {
        let mut bf = BloomFilter::new(1000, 0.01);
        // Insert 1000 keys
        for i in 0..1000u64 {
            let data = i.to_le_bytes();
            let (h1, h2) = ferrosa_common::murmur3::hash3_x64_128(&data, 0);
            bf.add(h1, h2);
        }

        // Test 10000 non-inserted keys
        let mut false_positives = 0;
        for i in 1000..11000u64 {
            let data = i.to_le_bytes();
            let (h1, h2) = ferrosa_common::murmur3::hash3_x64_128(&data, 0);
            if bf.is_present(h1, h2) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / 10000.0;
        // Allow up to 5% (generous tolerance for probabilistic test)
        assert!(
            fp_rate < 0.05,
            "false positive rate {fp_rate:.4} exceeds 5%"
        );
    }

    #[test]
    fn read_write_round_trip() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(111, 222);
        bf.add(333, 444);

        let serialized = bf.write();
        let deserialized = BloomFilter::read(&serialized).unwrap();

        assert!(deserialized.is_present(111, 222));
        assert!(deserialized.is_present(333, 444));
        assert!(!deserialized.is_present(555, 666));
    }

    #[test]
    fn read_too_short() {
        assert!(BloomFilter::read(&[0; 4]).is_err());
    }

    #[test]
    fn optimal_params_sanity() {
        let (bits, k) = optimal_params(1000, 0.01);
        assert!(bits > 0);
        assert!(k > 0 && k < 20);
    }
}
```

- [x] **Step 2: Register module in lib.rs**

Add to `ferrosa-sstable/src/lib.rs`:

```rust
pub mod bloom;
```

- [x] **Step 3: Run tests**

Run: `cargo test -p ferrosa-sstable bloom`
Expected: All tests PASS

- [x] **Step 4: Commit**

```bash
git add ferrosa-sstable/src/bloom.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add Cassandra-compatible Bloom filter read/write"
```

### Task 11: byte_comparable.rs — OSS50 Byte-Comparable Key Encoding

**Files:**

- Create: `ferrosa-sstable/src/byte_comparable.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [x] **Step 1: Write byte-comparable encoding module with tests**

Create `ferrosa-sstable/src/byte_comparable.rs`:

```rust
//! OSS50 byte-comparable key encoding for the BTI partition index.
//!
//! The BTI partition index trie stores keys in byte-ordered form so that
//! trie traversal follows token order. For Murmur3-partitioned tables,
//! the encoding is a multi-component sequence:
//!
//! 1. `0x40` (NEXT_COMPONENT separator)
//! 2. Token bytes (8 bytes, big-endian, XOR sign bit)
//! 3. `0x00` (ESCAPE — end of token component)
//! 4. `0x40` (NEXT_COMPONENT separator)
//! 5. Partition key bytes with null-escape encoding
//! 6. `0x00` (ESCAPE — end of key component)
//! 7. `0x38` (TERMINATOR)
//!
//! **Null-escape encoding**: `0x00` in key data becomes `0x00 0xFF`.
//! Consecutive zeros become `0x00` + (n-1) `0xFE` + `0xFF`.
//! The component ends with a bare `0x00` (ESCAPE).
//!
//! Reference: `ByteComparable.java`, `ByteSource.java` (version OSS50)
//!
//! # Examples
//!
//! ```
//! use ferrosa_sstable::byte_comparable;
//! use ferrosa_common::{DecoratedKey, PartitionKey, Token};
//!
//! let dk = DecoratedKey {
//!     token: Token(1),
//!     key: PartitionKey::from(b"AB".as_slice()),
//! };
//! let encoded = byte_comparable::encode(&dk);
//! // 0x40, token(1 XOR sign bit), 0x00, 0x40, 0x41, 0x42, 0x00, 0x38
//! assert_eq!(encoded.len(), 14);
//!
//! let decoded = byte_comparable::decode(&encoded).unwrap();
//! assert_eq!(decoded.token, dk.token);
//! assert_eq!(decoded.key.as_bytes(), dk.key.as_bytes());
//! ```

use ferrosa_common::{DecoratedKey, Error, PartitionKey, Result, Token};

// Constants from ByteSource.java
const ESCAPE: u8 = 0x00;
const NEXT_COMPONENT: u8 = 0x40;
const TERMINATOR: u8 = 0x38;
const ESCAPED_0_CONT: u8 = 0xFE;
const ESCAPED_0_DONE: u8 = 0xFF;

/// Sign bit mask for converting signed i64 token to unsigned byte-order.
const SIGN_BIT: u64 = 0x8000000000000000;

/// Encode a DecoratedKey into its byte-comparable form.
pub fn encode(key: &DecoratedKey) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14 + key.key.as_bytes().len() * 2);

    // Component 1: Token
    buf.push(NEXT_COMPONENT);
    let token_bytes = ((key.token.0 as u64) ^ SIGN_BIT).to_be_bytes();
    buf.extend_from_slice(&token_bytes);
    buf.push(ESCAPE); // end of token component

    // Component 2: Partition key with null-escape encoding
    buf.push(NEXT_COMPONENT);
    encode_bytes_with_null_escape(&mut buf, key.key.as_bytes());
    buf.push(ESCAPE); // end of key component

    // Terminator
    buf.push(TERMINATOR);

    buf
}

/// Decode a byte-comparable representation back to a DecoratedKey.
pub fn decode(data: &[u8]) -> Result<DecoratedKey> {
    let mut pos = 0;

    // Expect NEXT_COMPONENT
    if data.get(pos) != Some(&NEXT_COMPONENT) {
        return Err(Error::InvalidFormat("expected NEXT_COMPONENT at start".into()));
    }
    pos += 1;

    // Read 8 token bytes
    if data.len() < pos + 8 {
        return Err(Error::InvalidFormat("truncated token bytes".into()));
    }
    let mut token_bytes = [0u8; 8];
    token_bytes.copy_from_slice(&data[pos..pos + 8]);
    let token_unsigned = u64::from_be_bytes(token_bytes);
    let token = Token((token_unsigned ^ SIGN_BIT) as i64);
    pos += 8;

    // Expect ESCAPE (end of token component)
    if data.get(pos) != Some(&ESCAPE) {
        return Err(Error::InvalidFormat("expected ESCAPE after token".into()));
    }
    pos += 1;

    // Expect NEXT_COMPONENT
    if data.get(pos) != Some(&NEXT_COMPONENT) {
        return Err(Error::InvalidFormat("expected NEXT_COMPONENT before key".into()));
    }
    pos += 1;

    // Decode null-escaped key bytes until bare ESCAPE
    let (key_bytes, consumed) = decode_null_escaped_bytes(&data[pos..])?;
    pos += consumed;

    // Expect TERMINATOR
    if data.get(pos) != Some(&TERMINATOR) {
        return Err(Error::InvalidFormat("expected TERMINATOR at end".into()));
    }

    Ok(DecoratedKey {
        token,
        key: PartitionKey::new(key_bytes),
    })
}

/// Encode bytes with null-escape encoding (does NOT append the trailing ESCAPE).
fn encode_bytes_with_null_escape(buf: &mut Vec<u8>, data: &[u8]) {
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0x00 {
            buf.push(ESCAPE);
            // Count consecutive zeros
            let mut count = 1;
            while i + count < data.len() && data[i + count] == 0x00 {
                count += 1;
            }
            // Write (count - 1) CONT bytes
            for _ in 0..count - 1 {
                buf.push(ESCAPED_0_CONT);
            }
            buf.push(ESCAPED_0_DONE);
            i += count;
        } else {
            buf.push(data[i]);
            i += 1;
        }
    }
}

/// Decode null-escaped bytes. Returns `(decoded_bytes, consumed_from_input)`.
/// Stops at a bare ESCAPE (0x00 not followed by 0xFE or 0xFF).
fn decode_null_escaped_bytes(data: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut result = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if data[pos] == ESCAPE {
            // Check what follows
            if pos + 1 >= data.len() {
                // Bare ESCAPE at end = end of component
                pos += 1;
                break;
            }
            match data[pos + 1] {
                ESCAPED_0_DONE => {
                    // Single zero
                    result.push(0x00);
                    pos += 2;
                }
                ESCAPED_0_CONT => {
                    // Consecutive zeros: count CONT bytes, then DONE
                    result.push(0x00);
                    pos += 1; // skip ESCAPE
                    while pos < data.len() && data[pos] == ESCAPED_0_CONT {
                        result.push(0x00);
                        pos += 1;
                    }
                    if pos < data.len() && data[pos] == ESCAPED_0_DONE {
                        pos += 1;
                    } else {
                        return Err(Error::InvalidFormat("unterminated null-escape sequence".into()));
                    }
                }
                _ => {
                    // Bare ESCAPE = end of component
                    pos += 1;
                    break;
                }
            }
        } else {
            result.push(data[pos]);
            pos += 1;
        }
    }

    Ok((result, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_key() {
        let dk = DecoratedKey {
            token: Token(1),
            key: PartitionKey::from(b"AB".as_slice()),
        };
        let encoded = encode(&dk);

        // Expected: 40 80_00_00_00_00_00_00_01 00 40 41_42 00 38
        assert_eq!(encoded[0], NEXT_COMPONENT);
        // Token 1 XOR sign bit = 0x8000000000000001
        assert_eq!(&encoded[1..9], &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        assert_eq!(encoded[9], ESCAPE);
        assert_eq!(encoded[10], NEXT_COMPONENT);
        assert_eq!(&encoded[11..13], b"AB");
        assert_eq!(encoded[13], ESCAPE);
        assert_eq!(encoded[14], TERMINATOR);
    }

    #[test]
    fn round_trip_simple() {
        let dk = DecoratedKey {
            token: Token(42),
            key: PartitionKey::from(b"hello".as_slice()),
        };
        let encoded = encode(&dk);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.token, dk.token);
        assert_eq!(decoded.key.as_bytes(), dk.key.as_bytes());
    }

    #[test]
    fn round_trip_negative_token() {
        let dk = DecoratedKey {
            token: Token(-100),
            key: PartitionKey::from(b"test".as_slice()),
        };
        let encoded = encode(&dk);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.token, dk.token);
    }

    #[test]
    fn round_trip_min_max_tokens() {
        for token_val in [i64::MIN, i64::MAX, 0] {
            let dk = DecoratedKey {
                token: Token(token_val),
                key: PartitionKey::from(b"k".as_slice()),
            };
            let decoded = decode(&encode(&dk)).unwrap();
            assert_eq!(decoded.token.0, token_val);
        }
    }

    #[test]
    fn key_with_zeros() {
        let dk = DecoratedKey {
            token: Token(1),
            key: PartitionKey::new(vec![0x00]),
        };
        let encoded = encode(&dk);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.key.as_bytes(), &[0x00]);
    }

    #[test]
    fn key_with_consecutive_zeros() {
        let dk = DecoratedKey {
            token: Token(1),
            key: PartitionKey::new(vec![0x00, 0x00, 0x00]),
        };
        let encoded = encode(&dk);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.key.as_bytes(), &[0x00, 0x00, 0x00]);
    }

    #[test]
    fn key_with_mixed_zeros() {
        let dk = DecoratedKey {
            token: Token(1),
            key: PartitionKey::new(vec![0x41, 0x00, 0x42, 0x00, 0x00, 0x43]),
        };
        let encoded = encode(&dk);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.key.as_bytes(), &[0x41, 0x00, 0x42, 0x00, 0x00, 0x43]);
    }

    #[test]
    fn empty_key() {
        let dk = DecoratedKey {
            token: Token(0),
            key: PartitionKey::new(vec![]),
        };
        let encoded = encode(&dk);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.key.as_bytes().is_empty());
    }

    #[test]
    fn token_ordering_preserved() {
        // Tokens should sort in i64 order when encoded
        let tokens: Vec<i64> = vec![i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let encoded: Vec<Vec<u8>> = tokens
            .iter()
            .map(|&t| {
                encode(&DecoratedKey {
                    token: Token(t),
                    key: PartitionKey::from(b"k".as_slice()),
                })
            })
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "ordering violated: token {} vs {}",
                tokens[i],
                tokens[i + 1]
            );
        }
    }

    #[test]
    fn decode_invalid_no_separator() {
        assert!(decode(&[0xFF]).is_err());
    }

    #[test]
    fn decode_invalid_truncated() {
        assert!(decode(&[NEXT_COMPONENT, 0x00]).is_err());
    }
}
```

- [x] **Step 2: Register module in lib.rs**

Add to `ferrosa-sstable/src/lib.rs`:

```rust
pub mod byte_comparable;
```

- [x] **Step 3: Run tests**

Run: `cargo test -p ferrosa-sstable byte_comparable`
Expected: All tests PASS

- [x] **Step 4: Commit**

```bash
git add ferrosa-sstable/src/byte_comparable.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add OSS50 byte-comparable key encoding/decoding"
```

### Task 12: Add proptest Property Tests for Leaf Components

**Files:**

- Create: `ferrosa-sstable/tests/property_tests.rs`

- [x] **Step 1: Write property tests for varint, compression, bloom, byte_comparable**

Create `ferrosa-sstable/tests/property_tests.rs`:

```rust
//! Property-based tests for ferrosa-sstable leaf components.

use ferrosa_common::{DecoratedKey, PartitionKey, Token};
use ferrosa_sstable::{bloom::BloomFilter, byte_comparable, compression::Compression, varint};
use proptest::prelude::*;

proptest! {
    /// Unsigned varint round-trip: decode(encode(n)) == n.
    #[test]
    fn varint_unsigned_round_trip(value: u64) {
        // VInt only supports up to i64::MAX
        let value = value % (i64::MAX as u64 + 1);
        let mut buf = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut buf, value);
        let (decoded, consumed) = varint::read_unsigned_vint(&buf[..n]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(consumed, n);
    }

    /// Signed varint round-trip: decode(encode(n)) == n.
    #[test]
    fn varint_signed_round_trip(value: i64) {
        let mut buf = [0u8; 9];
        let n = varint::write_signed_vint(&mut buf, value);
        let (decoded, consumed) = varint::read_signed_vint(&buf[..n]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(consumed, n);
    }

    /// LZ4 compression round-trip: decompress(compress(data)) == data.
    #[test]
    fn lz4_round_trip(data: Vec<u8>) {
        let comp = Compression::Lz4;
        let compressed = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&compressed, data.len()).unwrap();
        prop_assert_eq!(decompressed, data);
    }

    /// Zstd compression round-trip: decompress(compress(data)) == data.
    #[test]
    fn zstd_round_trip(data: Vec<u8>) {
        let comp = Compression::Zstd { level: 1 };
        let compressed = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&compressed, data.len()).unwrap();
        prop_assert_eq!(decompressed, data);
    }

    /// Bloom filter: no false negatives. All inserted keys are found.
    #[test]
    fn bloom_no_false_negatives(keys in prop::collection::vec(any::<u64>(), 1..100)) {
        let mut bf = BloomFilter::new(keys.len(), 0.01);
        let hashes: Vec<(i64, i64)> = keys
            .iter()
            .map(|k| ferrosa_common::murmur3::hash3_x64_128(&k.to_le_bytes(), 0))
            .collect();

        for &(h1, h2) in &hashes {
            bf.add(h1, h2);
        }

        for &(h1, h2) in &hashes {
            prop_assert!(bf.is_present(h1, h2), "false negative in bloom filter");
        }
    }

    /// Byte-comparable round-trip: decode(encode(key)) == key.
    #[test]
    fn byte_comparable_round_trip(
        token: i64,
        key_bytes in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let dk = DecoratedKey {
            token: Token(token),
            key: PartitionKey::new(key_bytes.clone()),
        };
        let encoded = byte_comparable::encode(&dk);
        let decoded = byte_comparable::decode(&encoded).unwrap();
        prop_assert_eq!(decoded.token, dk.token);
        prop_assert_eq!(decoded.key.as_bytes(), key_bytes.as_slice());
    }

    /// Byte-comparable encoding preserves token ordering.
    #[test]
    fn byte_comparable_ordering(
        t1: i64,
        t2: i64,
    ) {
        let key = b"k";
        let e1 = byte_comparable::encode(&DecoratedKey {
            token: Token(t1),
            key: PartitionKey::from(key.as_slice()),
        });
        let e2 = byte_comparable::encode(&DecoratedKey {
            token: Token(t2),
            key: PartitionKey::from(key.as_slice()),
        });

        // Same key, so ordering should follow token ordering
        prop_assert_eq!(e1.cmp(&e2), t1.cmp(&t2));
    }
}
```

- [x] **Step 2: Run property tests**

Run: `cargo test -p ferrosa-sstable --test property_tests`
Expected: All property tests PASS

- [x] **Step 3: Commit**

```bash
git add ferrosa-sstable/tests/
git commit -m "test(sstable): add proptest property tests for leaf components"
```

---

## File Summary (Part A)

After executing this plan, the ferrosa-sstable crate will have:

```
ferrosa-sstable/
  Cargo.toml
  src/
    lib.rs               # Crate root with module declarations
    io.rs                # ReadAt/WriteAt traits + FileReadAt/FileWriteAt
    varint.rs            # Unsigned/signed varint encoding
    types.rs             # DeletionTime, LivenessInfo, Row, Partition
    compression.rs       # LZ4/Zstd + CompressionInfo read/write
    bloom.rs             # Bloom filter read/write
    byte_comparable.rs   # OSS50 key encoding/decoding
  tests/
    property_tests.rs    # proptest round-trip tests
```

Plus ferrosa-common improvements:

- `//!` module docs on all 5 modules
- Doc-tests on key public methods
- proptest property tests
- `cargo doc --no-deps -D warnings` in CI

**Continue to [Part B](2026-03-11-ferrosa-sstable-part-b.md) for:**

- Trie (node types, walker, builder)
- File format readers/writers (statistics, toc, partition index, row index, data)
- Public API (SSTableReader, SSTableWriter)
- Cassandra fixture generation

---

## Implementation Notes

Deviations from the plan discovered during execution. All issues were resolved; this section records what changed and why.

### Task 2: CI cargo doc syntax

**Plan said:** `cargo doc --no-deps -D warnings`

**Actual:** `cargo doc` doesn't accept `-D` directly. The correct approach is to set the environment variable:

```yaml
- run: cargo doc --no-deps
  env:
    RUSTDOCFLAGS: "-D warnings"
```

### Task 3: murmur3.rs `mod@tests` link

**Plan said:** Include `[`tests`](mod@tests)` intra-doc link.

**Actual:** `cfg(test)` modules are not visible to rustdoc, so `mod@tests` is a broken link under `-D warnings`. Changed to plain text "tests".

### Task 6: `self.len()` ambiguity in ReadAt impls

**Problem:** In `impl ReadAt for &[u8]` and `impl ReadAt for Vec<u8>`, `self.len()` is ambiguous between the inherent `<[u8]>::len()` (returns `usize`) and `ReadAt::len()` (returns `Result<u64>`).

**Fix:** Used explicit UFCS: `<[u8]>::len(self)` and `Vec::len(self)`.

### Task 7: CRITICAL — VInt 8-byte encoding overflow (proptest discovery)

**Bug:** `read_unsigned_vint` at plan line 862 had `extra_bytes < 8` which allowed `extra_bytes=7` to reach `0xFF >> (7+1)` = `0xFF >> 8`. This panics on `u8` (shift amount exceeds type width). Values with 50–56 significant bits (8-byte encoding, first byte `0xFE`) triggered this.

**Discovery:** proptest found this — hand-written boundary tests did not cover the 8-byte encoding boundary.

**Fix:** Changed condition from `extra_bytes < 8` to `extra_bytes <= 6`. The 8-byte encoding (`extra_bytes=7`) has no value bits in the first byte, same as the 9-byte case.

**Regression tests added:**

- `unsigned_8_byte_round_trip` — value `562949953421312` (50 significant bits)
- `signed_8_byte_round_trip` — value `281474976710656` (zigzag-encodes to 8-byte varint)

### Task 10: clippy `manual_div_ceil`

**Plan said:** `(num_bits + 63) / 64`

**Actual:** clippy recommended `num_bits.div_ceil(64)`. Applied.

### Task 11: byte_comparable doc-test length

**Plan said:** `assert_eq!(encoded.len(), 14)`

**Actual:** Correct length is 15 bytes: `NEXT_COMPONENT(1) + token(8) + ESCAPE(1) + NEXT_COMPONENT(1) + key(2) + ESCAPE(1) + TERMINATOR(1) = 15`. Fixed to `assert_eq!(encoded.len(), 15)`.

### lib.rs doc links

**Plan said:** `[`ReadAt`]`, `[`WriteAt`]`, `[`SSTableReader`]`, `[`SSTableWriter`]`

**Actual:** `ReadAt`/`WriteAt` needed path qualification: `[`io::ReadAt`]`/`[`io::WriteAt`]`. `SSTableReader`/`SSTableWriter` don't exist yet, so used plain text (not links) to avoid `-D warnings` failure.

### Additional clippy fixes applied across modules

- `needless_range_loop`: `for i in 1..total { ... buf[i] ... }` → `for &byte in &buf[1..total]`
- `redundant_closure`: `|e| Error::Io(e)` → `Error::Io`
- `useless_conversion`: `Error::Io(e.into())` where `e` is already `std::io::Error` → `Error::Io(e)`

### Test counts

| Crate | Unit tests | Doc-tests | Property tests | Total |
|-------|-----------|-----------|---------------|-------|
| ferrosa-common | 14 | 5 | 4 | 23 |
| ferrosa-sstable | 65 | 4 | 7 | 76 |
| **Workspace** | **79** | **9** | **11** | **107** (includes integration) |
