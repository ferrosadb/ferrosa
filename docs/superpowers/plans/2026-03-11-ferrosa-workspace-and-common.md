# Ferrosa Workspace + Common Types Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up the Rust workspace and implement ferrosa-common with all shared types needed by downstream crates.

**Architecture:** Cargo workspace at the repo root with ferrosa-common as the first leaf crate. Contains Token (Murmur3 hash ring position), PartitionKey, DecoratedKey, CellValue, and error types. Murmur3 x64_128 hash is implemented from scratch for exact Cassandra compatibility. No external crate dependencies.

**Tech Stack:** Rust 2021 edition, zero external dependencies for ferrosa-common

**Deferred to later plans:** `ByteBuffer` wrapper (deferred until ferrosa-sstable determines if `Vec<u8>` suffices), config types (deferred until ferrosa-storage defines what's configurable).

**Subsequent plans:** ferrosa-sstable (Plan 2), then one plan per remaining crate in build order.

---

## Chunk 1: Workspace and Crate Scaffolding

### Task 1: Create Workspace Root Cargo.toml

**Files:**

- Create: `Cargo.toml`

- [ ] **Step 1: Create workspace Cargo.toml**

```toml
[workspace]
resolver = "2"
members = [
    "ferrosa-common",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"
repository = "https://github.com/bkearns/ferrosa"
```

- [ ] **Step 2: Verify file created**

Run: `head Cargo.toml`
Expected: Content matches above

### Task 2: Scaffold ferrosa-common Crate

**Files:**

- Create: `ferrosa-common/Cargo.toml`
- Create: `ferrosa-common/src/lib.rs`

- [ ] **Step 1: Create crate Cargo.toml**

```toml
[package]
name = "ferrosa-common"
description = "Shared types for the Ferrosa distributed database"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
```

- [ ] **Step 2: Create lib.rs stub**

```rust
//! Shared types for the Ferrosa distributed database.
//!
//! This crate provides the low-level types used across all Ferrosa crates:
//! Token, PartitionKey, DecoratedKey, CellValue, and error types.
//!
//! CQL-level type definitions (text, int, collections, UDTs) live in
//! `ferrosa-cql`, not here. Crates below `ferrosa-cql` in the dependency
//! graph work with raw bytes and cell values, not CQL-typed values.
```

- [ ] **Step 3: Verify workspace builds**

Run: `cargo build`
Expected: Compiles with 0 errors, 0 warnings

- [ ] **Step 4: Commit**

```bash
git checkout -b feat/ferrosa-common
git add Cargo.toml ferrosa-common/
git commit -m "feat: scaffold workspace and ferrosa-common crate"
```

### Task 3: Update .gitignore and CI

**Files:**

- Modify: `.gitignore`
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Add Rust build artifacts to .gitignore**

Append to the existing `.gitignore` (which already contains `.superpowers/`):

```
/target/
```

Use `Edit` tool or `echo '/target/' >> .gitignore` — do NOT overwrite the file.

- [ ] **Step 2: Create GitHub Actions CI workflow**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo test
```

- [ ] **Step 3: Commit**

```bash
git add .gitignore .github/
git commit -m "ci: add GitHub Actions workflow and Rust .gitignore"
```

---

## Chunk 2: Murmur3 Hash + Token Type

### Task 4: Murmur3 x64_128 Hash Implementation

The Murmur3 hash is the foundation of data placement. It MUST produce identical output to Cassandra's `org.apache.cassandra.utils.MurmurHash.hash3_x64_128()`. Any divergence breaks SSTable compatibility and token ring placement.

**Reference:** `cassandra/src/java/org/apache/cassandra/utils/MurmurHash.java`

**Files:**

- Create: `ferrosa-common/src/murmur3.rs`
- Modify: `ferrosa-common/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `ferrosa-common/src/murmur3.rs`:

```rust
/// Cassandra-compatible Murmur3 x64_128 hash function.
///
/// Returns `(h1, h2)` where:
/// - `h1` is used as the Token value (position on the hash ring)
/// - Both `h1` and `h2` are used for Bloom filter hashing
///
/// Reference: `org.apache.cassandra.utils.MurmurHash.hash3_x64_128`

pub fn hash3_x64_128(data: &[u8], seed: i64) -> (i64, i64) {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_zero() {
        // Empty input with seed 0: no blocks, no tail, fmix64(0) = 0
        assert_eq!(hash3_x64_128(&[], 0), (0, 0));
    }

    #[test]
    fn deterministic() {
        let input = b"ferrosa";
        let a = hash3_x64_128(input, 0);
        let b = hash3_x64_128(input, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn different_inputs_differ() {
        let a = hash3_x64_128(b"hello", 0);
        let b = hash3_x64_128(b"world", 0);
        assert_ne!(a, b);
    }

    #[test]
    fn different_seeds_differ() {
        let a = hash3_x64_128(b"hello", 0);
        let b = hash3_x64_128(b"hello", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn single_block_16_bytes() {
        // Exactly one 16-byte block, no tail
        let input: Vec<u8> = (0..16).collect();
        let (h1, h2) = hash3_x64_128(&input, 0);
        // Must not be zero (extremely unlikely for non-empty input)
        assert!(h1 != 0 || h2 != 0);
    }

    #[test]
    fn block_plus_tail() {
        // 17 bytes: one block + 1-byte tail
        let input: Vec<u8> = (0..17).collect();
        let (h1, h2) = hash3_x64_128(&input, 0);
        assert!(h1 != 0 || h2 != 0);
    }

    // Characterization tests: exact values verified against Cassandra.
    // Generated by running the generate_murmur3_vectors task (Task 6).
    // DO NOT change these values without re-verifying against Cassandra.
    //
    // To regenerate:
    //   cd cassandra && ant build
    //   javac -cp build/classes/main GenerateMurmur3Vectors.java
    //   java -cp build/classes/main:. GenerateMurmur3Vectors
}
```

- [ ] **Step 2: Register module in lib.rs**

Add to `ferrosa-common/src/lib.rs`:

```rust
pub mod murmur3;
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p ferrosa-common`
Expected: FAIL — `not yet implemented`

- [ ] **Step 4: Implement hash3_x64_128**

Replace the `todo!()` in `ferrosa-common/src/murmur3.rs`:

```rust
pub fn hash3_x64_128(data: &[u8], seed: i64) -> (i64, i64) {
    let len = data.len();
    let nblocks = len / 16;
    let mut h1 = seed;
    let mut h2 = seed;

    const C1: i64 = 0x87c37b91114253d5_u64 as i64;
    const C2: i64 = 0x4cf5ad432745937f_i64;

    // Body: process 16-byte blocks
    for i in 0..nblocks {
        let mut k1 = get_block_i64(data, i * 2);
        let mut k2 = get_block_i64(data, i * 2 + 1);

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
    }

    // Tail: process remaining bytes
    let tail = &data[nblocks * 16..];
    let mut k1: i64 = 0;
    let mut k2: i64 = 0;

    // Equivalent to C fall-through switch on (tail.len())
    // k2 accumulates bytes 8..14, k1 accumulates bytes 0..7
    //
    // CRITICAL: Cassandra's Java reads bytes as signed `byte` (-128..127)
    // and casts to `long` with SIGN EXTENSION. The body's getBlock() masks
    // with `& 0xff` (zero-extending), but the tail does NOT. This is
    // Cassandra's "sign bug" (see MurmurHash.java header comment). We must
    // replicate it exactly: `as i8 as i64` sign-extends, matching Java's
    // `(long) key.get(offset)` behavior.
    if tail.len() >= 15 { k2 ^= (tail[14] as i8 as i64) << 48; }
    if tail.len() >= 14 { k2 ^= (tail[13] as i8 as i64) << 40; }
    if tail.len() >= 13 { k2 ^= (tail[12] as i8 as i64) << 32; }
    if tail.len() >= 12 { k2 ^= (tail[11] as i8 as i64) << 24; }
    if tail.len() >= 11 { k2 ^= (tail[10] as i8 as i64) << 16; }
    if tail.len() >= 10 { k2 ^= (tail[9] as i8 as i64) << 8; }
    if tail.len() >= 9 {
        k2 ^= tail[8] as i8 as i64;
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }

    if tail.len() >= 8 { k1 ^= (tail[7] as i8 as i64) << 56; }
    if tail.len() >= 7 { k1 ^= (tail[6] as i8 as i64) << 48; }
    if tail.len() >= 6 { k1 ^= (tail[5] as i8 as i64) << 40; }
    if tail.len() >= 5 { k1 ^= (tail[4] as i8 as i64) << 32; }
    if tail.len() >= 4 { k1 ^= (tail[3] as i8 as i64) << 24; }
    if tail.len() >= 3 { k1 ^= (tail[2] as i8 as i64) << 16; }
    if tail.len() >= 2 { k1 ^= (tail[1] as i8 as i64) << 8; }
    if !tail.is_empty() {
        k1 ^= tail[0] as i8 as i64;
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    // Finalization
    h1 ^= len as i64;
    h2 ^= len as i64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

/// Read a little-endian i64 from `data` at the given 8-byte block index.
fn get_block_i64(data: &[u8], index: usize) -> i64 {
    let offset = index * 8;
    i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ])
}

/// Murmur3 finalization mix — forces all bits to avalanche.
/// Uses unsigned right shift to match Java's `>>>` operator.
fn fmix64(mut k: i64) -> i64 {
    k ^= (k as u64 >> 33) as i64;
    k = k.wrapping_mul(0xff51afd7ed558ccd_u64 as i64);
    k ^= (k as u64 >> 33) as i64;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53_u64 as i64);
    k ^= (k as u64 >> 33) as i64;
    k
}
```

**Critical implementation notes:**

- **Sign-extension in tail (Cassandra's "sign bug"):** Java's `key.get()` returns signed `byte` (-128..127). Casting to `long` sign-extends: byte `0xFF` becomes `0xFFFFFFFFFFFFFFFF`. The body's `getBlock()` masks with `& 0xff` (zero-extending), but the tail does NOT mask. We replicate this with `as i8 as i64` in the tail. Using `as i64` directly would zero-extend (byte `0xFF` → `0x00000000000000FF`), silently breaking hashes for any key containing bytes >= 128.
- `fmix64` uses `(k as u64 >> 33) as i64` for unsigned right shift, matching Java's `>>>` operator. Rust's `>>` on i64 is arithmetic (sign-extending) which would produce wrong results.
- `i64::rotate_left` matches Java's `Long.rotateLeft` — both rotate the bit pattern.
- `wrapping_mul` / `wrapping_add` match Java's silent overflow on `long` arithmetic.
- `get_block_i64` reads little-endian via `i64::from_le_bytes`, matching Cassandra's `getBlock()` which uses `& 0xff` masking (zero-extension). The body and tail use DIFFERENT byte-extension behavior — this asymmetry is intentional and must be preserved.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-common`
Expected: All tests PASS

- [ ] **Step 6: Run clippy**

Run: `cargo clippy -p ferrosa-common -- -D warnings`
Expected: No warnings

- [ ] **Step 7: Commit**

```bash
git add ferrosa-common/src/
git commit -m "feat(common): add Murmur3 x64_128 hash (Cassandra-compatible)"
```

### Task 5: Token Type

**Files:**

- Create: `ferrosa-common/src/token.rs`
- Modify: `ferrosa-common/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `ferrosa-common/src/token.rs`:

```rust
/// Position on the Murmur3 hash ring.
///
/// Wraps an `i64` produced by `murmur3::hash3_x64_128`. The full range
/// `i64::MIN..=i64::MAX` is used, matching Cassandra's `LongToken`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(pub i64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_and_max() {
        assert_eq!(Token::MIN.0, i64::MIN);
        assert_eq!(Token::MAX.0, i64::MAX);
        assert!(Token::MIN < Token::MAX);
    }

    #[test]
    fn from_key_deterministic() {
        let a = Token::from_key(b"hello");
        let b = Token::from_key(b"hello");
        assert_eq!(a, b);
    }

    #[test]
    fn from_key_different_keys_differ() {
        let a = Token::from_key(b"hello");
        let b = Token::from_key(b"world");
        assert_ne!(a, b);
    }

    #[test]
    fn from_empty_key() {
        let t = Token::from_key(b"");
        assert_eq!(t, Token(0));
    }

    #[test]
    fn ordering_matches_i64() {
        let a = Token(-100);
        let b = Token(0);
        let c = Token(100);
        assert!(a < b);
        assert!(b < c);
    }
}
```

- [ ] **Step 2: Add Token constants and from_key**

Add above the `#[cfg(test)]` block:

```rust
impl Token {
    pub const MIN: Token = Token(i64::MIN);
    pub const MAX: Token = Token(i64::MAX);

    /// Compute the token for a raw partition key by hashing with Murmur3.
    pub fn from_key(key: &[u8]) -> Token {
        let (h1, _) = crate::murmur3::hash3_x64_128(key, 0);
        Token(h1)
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
```

- [ ] **Step 3: Register module in lib.rs**

Add to `ferrosa-common/src/lib.rs`:

```rust
pub mod token;
pub use token::Token;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-common`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-common/src/
git commit -m "feat(common): add Token type (Murmur3 hash ring position)"
```

### Task 6: Generate Characterization Test Vectors from Cassandra

This task uses the Java codebase as a behavioral oracle (per ADR-005) to generate exact test vectors for the Murmur3 implementation.

**Files:**

- Create: `tools/generate_murmur3_vectors.java` (one-off tool, not part of build)
- Modify: `ferrosa-common/src/murmur3.rs` (add characterization tests)

**Prerequisites:** Java 17+, Cassandra submodule initialized and buildable.

- [ ] **Step 0: Verify Cassandra submodule is available**

Run:

```bash
git submodule update --init cassandra
ls cassandra/src/java/org/apache/cassandra/utils/MurmurHash.java
```

Expected: File exists. If the submodule is not initialized, `git submodule update --init` will fetch it (may take several minutes).

- [ ] **Step 1: Create vector generator**

```java
// tools/generate_murmur3_vectors.java
// Run: cd cassandra && ant build
//      javac -cp build/classes/main ../tools/generate_murmur3_vectors.java -d ../tools/
//      java -cp build/classes/main:../tools generate_murmur3_vectors

import org.apache.cassandra.utils.MurmurHash;
import java.nio.ByteBuffer;

public class generate_murmur3_vectors {
    public static void main(String[] args) {
        System.out.println("// Murmur3 x64_128 test vectors from Cassandra");
        System.out.println("// Format: (input_bytes, seed, expected_h1, expected_h2)");
        System.out.println();

        Object[][] cases = {
            {new byte[]{}, 0L},
            {new byte[]{0}, 0L},
            {new byte[]{1}, 0L},
            {new byte[]{0, 1, 2, 3}, 0L},
            {"hello".getBytes(), 0L},
            {"cassandra".getBytes(), 0L},
            {"ferrosa".getBytes(), 0L},
            // One full 16-byte block
            {new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}, 0L},
            // 17 bytes: one block + 1-byte tail
            {new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}, 0L},
            // All tail lengths (1-15 bytes)
            {new byte[]{42}, 0L},
            {new byte[]{42, 43}, 0L},
            {new byte[]{42, 43, 44}, 0L},
            {new byte[]{42, 43, 44, 45}, 0L},
            {new byte[]{42, 43, 44, 45, 46}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47, 48}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47, 48, 49}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47, 48, 49, 50}, 0L},
            // High-byte values: critical for verifying sign-extension behavior.
            // Java's signed byte cast sign-extends 0x80-0xFF in the tail.
            {new byte[]{(byte)0xFF}, 0L},
            {new byte[]{(byte)0x80}, 0L},
            {new byte[]{(byte)0xFF, (byte)0xFE, (byte)0xFD}, 0L},
            {new byte[]{0, (byte)0x80, (byte)0xFF, 1, (byte)0x81, (byte)0xFE, 2, (byte)0x82}, 0L},
            // 16 high bytes: full block (body path, masked with & 0xff)
            {new byte[]{(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,
                        (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF}, 0L},
            // 17 high bytes: full block + 1-byte tail (tests body vs tail sign behavior)
            {new byte[]{(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,
                        (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,
                        (byte)0xFF}, 0L},
            // Non-zero seed
            {"hello".getBytes(), 42L},
        };

        for (Object[] c : cases) {
            byte[] input = (byte[]) c[0];
            long seed = (Long) c[1];
            long[] result = MurmurHash.hash3_x64_128(
                ByteBuffer.wrap(input), 0, input.length, seed);
            System.out.printf("(&%s, %d, %d_i64, %d_i64),\n",
                formatBytes(input), seed, result[0], result[1]);
        }
    }

    static String formatBytes(byte[] b) {
        if (b.length == 0) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < b.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(b[i] & 0xff);
        }
        sb.append("]");
        return sb.toString();
    }
}
```

- [ ] **Step 2: Build Cassandra and generate vectors**

Note: `ant build` may take 5-10 minutes on first run.

Run:

```bash
cd cassandra && ant build
javac -cp build/classes/main ../tools/generate_murmur3_vectors.java -d ../tools/
java -cp build/classes/main:../tools generate_murmur3_vectors | tee ../tools/murmur3_vectors.txt
```

Expected: Prints test vector tuples to stdout and saves to `tools/murmur3_vectors.txt`.

- [ ] **Step 3: Add characterization tests to murmur3.rs**

Add to the `mod tests` block in `ferrosa-common/src/murmur3.rs`, replacing the comment about characterization tests:

```rust
    // Paste the generated vectors as a data-driven test:
    #[test]
    fn cassandra_characterization_vectors() {
        // Values generated from Cassandra's MurmurHash.hash3_x64_128
        let vectors: &[(&[u8], u64, i64, i64)] = &[
            // Paste generated output here, e.g.:
            // (&[], 0, 0_i64, 0_i64),
            // (&[0], 0, <h1>, <h2>),
            // ...
        ];
        for (input, seed, expected_h1, expected_h2) in vectors {
            let (h1, h2) = hash3_x64_128(input, *seed);
            assert_eq!(
                (h1, h2),
                (*expected_h1, *expected_h2),
                "mismatch for input {:?} seed {}",
                input,
                seed
            );
        }
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-common murmur3`
Expected: All tests PASS, including characterization vectors

- [ ] **Step 5: Commit**

```bash
git add tools/ ferrosa-common/src/murmur3.rs
git commit -m "test(common): add Murmur3 characterization vectors from Cassandra"
```

---

## Chunk 3: Key Types, Cell Types, and Errors

### Task 7: PartitionKey and DecoratedKey

**Files:**

- Create: `ferrosa-common/src/key.rs`
- Modify: `ferrosa-common/src/lib.rs`

- [ ] **Step 1: Write tests and implementation**

Create `ferrosa-common/src/key.rs`:

```rust
use std::cmp::Ordering;

use crate::Token;

/// Raw partition key bytes.
///
/// For simple partition keys, this is the CQL-serialized value of the
/// partition column. For composite keys, it's the length-prefixed
/// concatenation of component values (serialization handled by ferrosa-cql).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey(Vec<u8>);

impl PartitionKey {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Ord for PartitionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for PartitionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Vec<u8>> for PartitionKey {
    fn from(data: Vec<u8>) -> Self {
        Self(data)
    }
}

impl From<&[u8]> for PartitionKey {
    fn from(data: &[u8]) -> Self {
        Self(data.to_vec())
    }
}

/// A partition key decorated with its token (position on the hash ring).
///
/// The token is computed once via Murmur3 and cached. DecoratedKey is the
/// primary key type used throughout the storage engine — it determines
/// both which node owns the partition and where it lives within SSTables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecoratedKey {
    pub token: Token,
    pub key: PartitionKey,
}

impl DecoratedKey {
    /// Create a DecoratedKey by hashing the partition key with Murmur3.
    pub fn new(key: PartitionKey) -> Self {
        let token = Token::from_key(key.as_bytes());
        Self { token, key }
    }

    /// Returns both Murmur3 hash values `(h1, h2)`.
    /// `h1` is the token. Both are needed for Bloom filter hashing.
    pub fn filter_hash(&self) -> (i64, i64) {
        crate::murmur3::hash3_x64_128(self.key.as_bytes(), 0)
    }
}

impl Ord for DecoratedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.token
            .cmp(&other.token)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for DecoratedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_from_bytes() {
        let pk = PartitionKey::from(b"hello".as_slice());
        assert_eq!(pk.as_bytes(), b"hello");
        assert_eq!(pk.len(), 5);
        assert!(!pk.is_empty());
    }

    #[test]
    fn partition_key_empty() {
        let pk = PartitionKey::new(vec![]);
        assert!(pk.is_empty());
        assert_eq!(pk.len(), 0);
    }

    #[test]
    fn decorated_key_computes_token() {
        let dk = DecoratedKey::new(PartitionKey::from(b"hello".as_slice()));
        let expected_token = Token::from_key(b"hello");
        assert_eq!(dk.token, expected_token);
    }

    #[test]
    fn decorated_key_ordering_by_token() {
        let a = DecoratedKey::new(PartitionKey::from(b"aaa".as_slice()));
        let b = DecoratedKey::new(PartitionKey::from(b"bbb".as_slice()));
        // Ordering is by token, not by key bytes
        if a.token < b.token {
            assert!(a < b);
        } else {
            assert!(a > b);
        }
    }

    #[test]
    fn decorated_key_filter_hash_matches_token() {
        let dk = DecoratedKey::new(PartitionKey::from(b"test".as_slice()));
        let (h1, _h2) = dk.filter_hash();
        assert_eq!(h1, dk.token.0);
    }

    #[test]
    fn decorated_key_same_token_breaks_tie_by_key() {
        // Construct two keys with the same token (unlikely but must handle)
        let dk1 = DecoratedKey {
            token: Token(42),
            key: PartitionKey::from(b"aaa".as_slice()),
        };
        let dk2 = DecoratedKey {
            token: Token(42),
            key: PartitionKey::from(b"bbb".as_slice()),
        };
        assert!(dk1 < dk2); // tie-broken by key bytes
    }
}
```

- [ ] **Step 2: Register module in lib.rs**

Add to `ferrosa-common/src/lib.rs`:

```rust
pub mod key;
pub use key::{DecoratedKey, PartitionKey};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-common`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add ferrosa-common/src/
git commit -m "feat(common): add PartitionKey and DecoratedKey types"
```

### Task 8: CellValue Type

**Files:**

- Create: `ferrosa-common/src/cell.rs`
- Modify: `ferrosa-common/src/lib.rs`

- [ ] **Step 1: Write tests and implementation**

Create `ferrosa-common/src/cell.rs`:

```rust
/// Timestamp for a cell value (microseconds since epoch).
/// Matches Cassandra's `Cell.timestamp()`.
pub type Timestamp = i64;

/// Sentinel value indicating no timestamp has been set.
pub const NO_TIMESTAMP: Timestamp = i64::MIN;

/// Sentinel: cell has no TTL (lives forever unless explicitly deleted).
pub const NO_TTL: i32 = 0;

/// Sentinel: cell is not deleted. Cassandra uses `Integer.MAX_VALUE`.
pub const NO_DELETION_TIME: i32 = i32::MAX;

/// A single cell value within a row.
///
/// Cells are the atomic unit of data in Cassandra/Ferrosa. A cell belongs
/// to a specific column in a specific row, and carries a value along with
/// metadata (timestamp, TTL, deletion time) used for conflict resolution
/// and expiration.
///
/// Three cell states:
/// - **Live**: has a value and timestamp, no expiration
/// - **Expiring**: has a value, timestamp, TTL, and deletion time
/// - **Tombstone**: no value, marks deletion at a specific timestamp
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellValue {
    /// The cell's bytes. `None` for tombstones.
    pub value: Option<Vec<u8>>,
    /// Microseconds since epoch. Used for last-write-wins conflict resolution.
    pub timestamp: Timestamp,
    /// Time-to-live in seconds. 0 means no expiration.
    pub ttl: i32,
    /// Local deletion time (seconds since epoch). `i32::MAX` means not deleted.
    pub local_deletion_time: i32,
}

impl CellValue {
    /// A live cell with a value and timestamp, no expiration.
    pub fn live(value: Vec<u8>, timestamp: Timestamp) -> Self {
        Self {
            value: Some(value),
            timestamp,
            ttl: NO_TTL,
            local_deletion_time: NO_DELETION_TIME,
        }
    }

    /// A cell with a TTL that will expire.
    pub fn expiring(
        value: Vec<u8>,
        timestamp: Timestamp,
        ttl: i32,
        local_deletion_time: i32,
    ) -> Self {
        Self {
            value: Some(value),
            timestamp,
            ttl,
            local_deletion_time,
        }
    }

    /// A tombstone marking deletion at the given timestamp.
    pub fn tombstone(timestamp: Timestamp, local_deletion_time: i32) -> Self {
        Self {
            value: None,
            timestamp,
            ttl: NO_TTL,
            local_deletion_time,
        }
    }

    pub fn is_live(&self) -> bool {
        self.value.is_some() && self.local_deletion_time == NO_DELETION_TIME
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    pub fn is_expiring(&self) -> bool {
        self.value.is_some() && self.ttl != NO_TTL
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_cell() {
        let cell = CellValue::live(b"hello".to_vec(), 1000);
        assert!(cell.is_live());
        assert!(!cell.is_tombstone());
        assert!(!cell.is_expiring());
        assert_eq!(cell.value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(cell.timestamp, 1000);
    }

    #[test]
    fn expiring_cell() {
        let cell = CellValue::expiring(b"temp".to_vec(), 1000, 3600, 1700000000);
        assert!(!cell.is_live()); // has deletion time set
        assert!(!cell.is_tombstone());
        assert!(cell.is_expiring());
        assert_eq!(cell.ttl, 3600);
    }

    #[test]
    fn tombstone() {
        let cell = CellValue::tombstone(1000, 1700000000);
        assert!(!cell.is_live());
        assert!(cell.is_tombstone());
        assert!(!cell.is_expiring());
        assert!(cell.value.is_none());
    }

    #[test]
    fn no_timestamp_sentinel() {
        assert_eq!(NO_TIMESTAMP, i64::MIN);
    }
}
```

- [ ] **Step 2: Register module in lib.rs**

Add to `ferrosa-common/src/lib.rs`:

```rust
pub mod cell;
pub use cell::{CellValue, Timestamp, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-common`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add ferrosa-common/src/
git commit -m "feat(common): add CellValue type (live, expiring, tombstone)"
```

### Task 9: Error Types

**Files:**

- Create: `ferrosa-common/src/error.rs`
- Modify: `ferrosa-common/src/lib.rs`

- [ ] **Step 1: Write error types**

Create `ferrosa-common/src/error.rs`:

```rust
use std::fmt;

/// Errors that can occur across Ferrosa crates.
///
/// `#[non_exhaustive]` allows adding variants without semver breakage.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// I/O error (disk, network, S3).
    Io(std::io::Error),
    /// Data does not conform to expected format.
    InvalidData(String),
    /// File or structure format not recognized.
    InvalidFormat(String),
    /// Checksum verification failed.
    ChecksumMismatch { expected: u32, actual: u32 },
    /// SSTable or protocol version not supported.
    UnsupportedVersion(String),
    /// Compression algorithm not supported.
    UnsupportedCompression(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Error::InvalidFormat(msg) => write!(f, "invalid format: {msg}"),
            Error::ChecksumMismatch { expected, actual } => {
                write!(f, "checksum mismatch: expected {expected:#x}, got {actual:#x}")
            }
            Error::UnsupportedVersion(v) => write!(f, "unsupported version: {v}"),
            Error::UnsupportedCompression(c) => write!(f, "unsupported compression: {c}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Result type alias used throughout Ferrosa.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("gone"));
    }

    #[test]
    fn checksum_mismatch_display() {
        let err = Error::ChecksumMismatch {
            expected: 0xDEAD,
            actual: 0xBEEF,
        };
        let s = err.to_string();
        assert!(s.contains("0xdead"));
        assert!(s.contains("0xbeef"));
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Error>();
        assert_sync::<Error>();
    }
}
```

- [ ] **Step 2: Register module in lib.rs**

Add to `ferrosa-common/src/lib.rs`:

```rust
pub mod error;
pub use error::{Error, Result};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-common`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add ferrosa-common/src/
git commit -m "feat(common): add Error and Result types"
```

### Task 10: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `cargo test -p ferrosa-common`
Expected: All tests PASS (murmur3, token, key, cell, error modules)

- [ ] **Step 2: Run clippy and fmt**

Run: `cargo clippy -p ferrosa-common -- -D warnings && cargo fmt --check`
Expected: No warnings, no formatting issues

- [ ] **Step 3: Verify lib.rs re-exports**

Final `ferrosa-common/src/lib.rs` should be:

```rust
//! Shared types for the Ferrosa distributed database.
//!
//! This crate provides the low-level types used across all Ferrosa crates:
//! Token, PartitionKey, DecoratedKey, CellValue, and error types.
//!
//! CQL-level type definitions (text, int, collections, UDTs) live in
//! `ferrosa-cql`, not here. Crates below `ferrosa-cql` in the dependency
//! graph work with raw bytes and cell values, not CQL-typed values.

pub mod cell;
pub mod error;
pub mod key;
pub mod murmur3;
pub mod token;

pub use cell::{CellValue, Timestamp, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
pub use error::{Error, Result};
pub use key::{DecoratedKey, PartitionKey};
pub use token::Token;
```

- [ ] **Step 4: Commit any final adjustments**

```bash
git add ferrosa-common/src/lib.rs
git commit -m "feat(common): finalize ferrosa-common public API"
```

---

## File Summary

After executing this plan, the following files will exist:

```
Cargo.toml                          # Workspace root
.gitignore                          # Updated with /target/
.github/workflows/ci.yml            # CI pipeline
ferrosa-common/
  Cargo.toml
  src/
    lib.rs                          # Re-exports all public types
    murmur3.rs                      # Murmur3 x64_128 hash + tests
    token.rs                        # Token type + tests
    key.rs                          # PartitionKey, DecoratedKey + tests
    cell.rs                         # CellValue + tests
    error.rs                        # Error enum, Result alias + tests
tools/
  generate_murmur3_vectors.java     # One-off Cassandra vector generator
```

**Zero external dependencies.** ferrosa-common is pure Rust with no crate deps.

**Next plan:** ferrosa-sstable (SSTable reading and writing).
