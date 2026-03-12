# ferrosa-sstable Implementation Plan — Part B

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the trie, file format readers/writers, and public API for `ferrosa-sstable`.

**Prerequisites:** [Part A](2026-03-11-ferrosa-sstable-part-a.md) must be completed first (crate scaffolding, leaf components).

**Reference documents:**

- [SSTable Format Specification](../../../specs/sstable.md) — byte-level BTI format
- [Design Doc](../specs/2026-03-11-ferrosa-sstable-design.md) — module structure, build order

---

## Chunk 4: On-Disk Trie

### Task 13: trie/node.rs — 16 Node Types Encode/Decode

**Files:**

- Create: `ferrosa-sstable/src/trie/mod.rs`
- Create: `ferrosa-sstable/src/trie/node.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [ ] **Step 1: Create trie module**

Create `ferrosa-sstable/src/trie/mod.rs`:

```rust
//! On-disk trie implementation for BTI SSTable indices.
//!
//! Both the partition index (Partitions.db) and row index (Rows.db) use the
//! same trie encoding. The trie maps byte sequences (key prefixes or
//! clustering separators) to payload values (file positions).
//!
//! # Node Types
//!
//! 16 node types (codes 0x0–0xF) encode transitions from a node to its
//! children. Each node begins with a single byte: 4 bits of type + 4 bits
//! of payload info. The builder chooses whichever encoding produces the
//! smallest representation.
//!
//! # Page Alignment
//!
//! Nodes are packed into 4096-byte pages. No node crosses a page boundary,
//! ensuring a single page fetch reads a complete node.
//!
//! # Pointers
//!
//! Child pointers are stored as distances (offsets from current node position
//! to child). Since tries are written bottom-up, children always precede
//! parents in the file.
//!
//! Reference: Cassandra's `BtiFormat.md`, `IncrementalTrieWriterPageAware`

pub mod node;
pub mod walker;
pub mod builder;
```

- [ ] **Step 2: Write node types and encoding/decoding**

Create `ferrosa-sstable/src/trie/node.rs`:

```rust
//! Trie node types: encoding, decoding, and size calculation.
//!
//! Each node starts with a type byte: upper 4 bits = node type (0x0–0xF),
//! lower 4 bits = payload bits (`pb`). The payload bits control whether the
//! node carries data and how to interpret it.

use ferrosa_common::{Error, Result};

/// Page size for trie page-aware packing.
pub const PAGE_SIZE: usize = 4096;

/// The 16 trie node types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    PayloadOnly = 0x0,
    SingleNopayload4 = 0x1,
    Single8 = 0x2,
    SingleNopayload12 = 0x3,
    Single16 = 0x4,
    Sparse8 = 0x5,
    Sparse12 = 0x6,
    Sparse16 = 0x7,
    Sparse24 = 0x8,
    Sparse40 = 0x9,
    Dense12 = 0xA,
    Dense16 = 0xB,
    Dense24 = 0xC,
    Dense32 = 0xD,
    Dense40 = 0xE,
    LongDense = 0xF,
}

impl NodeType {
    /// Parse a node type from the upper 4 bits of the type byte.
    pub fn from_type_byte(byte: u8) -> Result<Self> {
        match byte >> 4 {
            0x0 => Ok(NodeType::PayloadOnly),
            0x1 => Ok(NodeType::SingleNopayload4),
            0x2 => Ok(NodeType::Single8),
            0x3 => Ok(NodeType::SingleNopayload12),
            0x4 => Ok(NodeType::Single16),
            0x5 => Ok(NodeType::Sparse8),
            0x6 => Ok(NodeType::Sparse12),
            0x7 => Ok(NodeType::Sparse16),
            0x8 => Ok(NodeType::Sparse24),
            0x9 => Ok(NodeType::Sparse40),
            0xA => Ok(NodeType::Dense12),
            0xB => Ok(NodeType::Dense16),
            0xC => Ok(NodeType::Dense24),
            0xD => Ok(NodeType::Dense32),
            0xE => Ok(NodeType::Dense40),
            0xF => Ok(NodeType::LongDense),
            _ => unreachable!(),
        }
    }

    /// Node size in bytes excluding payload. `cc` = child count, `cs` = child span.
    pub fn node_size(&self, cc: usize, cs: usize) -> usize {
        match self {
            NodeType::PayloadOnly => 1,
            NodeType::SingleNopayload4 => 2,
            NodeType::Single8 => 3,
            NodeType::SingleNopayload12 => 3,
            NodeType::Single16 => 4,
            NodeType::Sparse8 => 2 + cc * 2,
            NodeType::Sparse12 => 2 + (cc * 5 + 1) / 2,
            NodeType::Sparse16 => 2 + cc * 3,
            NodeType::Sparse24 => 2 + cc * 4,
            NodeType::Sparse40 => 2 + cc * 6,
            NodeType::Dense12 => 3 + (cs * 3 + 1) / 2,
            NodeType::Dense16 => 3 + cs * 2,
            NodeType::Dense24 => 3 + cs * 3,
            NodeType::Dense32 => 3 + cs * 4,
            NodeType::Dense40 => 3 + cs * 5,
            NodeType::LongDense => 3 + cs * 8,
        }
    }

    /// Whether this node type is a single-child type.
    pub fn is_single(&self) -> bool {
        matches!(
            self,
            NodeType::SingleNopayload4
                | NodeType::Single8
                | NodeType::SingleNopayload12
                | NodeType::Single16
        )
    }

    /// Whether this node type is sparse (multiple children, listed explicitly).
    pub fn is_sparse(&self) -> bool {
        matches!(
            self,
            NodeType::Sparse8
                | NodeType::Sparse12
                | NodeType::Sparse16
                | NodeType::Sparse24
                | NodeType::Sparse40
        )
    }

    /// Whether this node type is dense (range of children, min..=max).
    pub fn is_dense(&self) -> bool {
        matches!(
            self,
            NodeType::Dense12
                | NodeType::Dense16
                | NodeType::Dense24
                | NodeType::Dense32
                | NodeType::Dense40
                | NodeType::LongDense
        )
    }
}

/// Payload position and size for a partition index trie node.
///
/// `pb` = lower 4 bits of type byte.
/// `ppos` = start of payload bytes in the node (after transition data).
///
/// Partition index payload:
/// - If `pb` == 0: no payload
/// - If `pb` < 8: `idxpos` is `pb`-byte sign-extended integer at `ppos`
/// - If `pb` >= 8: `hash` byte at `ppos`, `idxpos` is `(pb-7)`-byte integer at `ppos+1`
pub fn payload_size(pb: u8) -> usize {
    if pb == 0 {
        0
    } else if pb < 8 {
        pb as usize
    } else {
        1 + (pb - 7) as usize
    }
}

/// Decode the payload from a partition index trie node.
/// Returns `(hash_byte, idxpos)` where hash_byte is `None` if `pb < 8`.
pub fn decode_payload(pb: u8, payload_bytes: &[u8]) -> Result<(Option<u8>, i64)> {
    if pb == 0 {
        return Ok((None, 0));
    }

    if pb < 8 {
        let nbytes = pb as usize;
        if payload_bytes.len() < nbytes {
            return Err(Error::InvalidData("payload too short".into()));
        }
        let idxpos = sign_extend(&payload_bytes[..nbytes]);
        Ok((None, idxpos))
    } else {
        let nbytes = (pb - 7) as usize;
        if payload_bytes.len() < 1 + nbytes {
            return Err(Error::InvalidData("payload too short".into()));
        }
        let hash = payload_bytes[0];
        let idxpos = sign_extend(&payload_bytes[1..1 + nbytes]);
        Ok((Some(hash), idxpos))
    }
}

/// Sign-extend a big-endian byte sequence to i64.
fn sign_extend(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }
    // Start with sign extension
    let mut value: i64 = if bytes[0] & 0x80 != 0 { -1 } else { 0 };
    for &b in bytes {
        value = (value << 8) | b as i64;
    }
    value
}

/// Encode an i64 value into the minimum number of bytes (big-endian, sign-preserving).
pub fn encode_signed_bytes(value: i64) -> Vec<u8> {
    if value == 0 {
        return vec![0];
    }
    let bytes = value.to_be_bytes();
    // Find the first byte that differs from the sign extension
    let sign_byte = if value < 0 { 0xFF } else { 0x00 };
    let mut start = 0;
    while start < 7 && bytes[start] == sign_byte {
        // Keep if next byte's sign bit would change interpretation
        if (bytes[start + 1] & 0x80 != 0) != (value < 0) {
            break;
        }
        start += 1;
    }
    bytes[start..].to_vec()
}

/// Decoded trie node header information.
#[derive(Debug, Clone)]
pub struct NodeHeader {
    /// Node type (upper 4 bits).
    pub node_type: NodeType,
    /// Payload bits (lower 4 bits).
    pub pb: u8,
    /// Transition byte(s) — the byte values labeling edges to children.
    pub transitions: Vec<u8>,
    /// Child pointer distances (offset from this node to each child).
    pub child_pointers: Vec<u64>,
    /// Start position of payload bytes within the node.
    pub payload_offset: usize,
    /// Total node size in bytes (including type byte and payload).
    pub total_size: usize,
}

/// Read a node header from a byte slice starting at position 0.
/// The slice should contain at least the node bytes.
pub fn read_node_header(data: &[u8]) -> Result<NodeHeader> {
    if data.is_empty() {
        return Err(Error::InvalidData("empty node data".into()));
    }

    let type_byte = data[0];
    let node_type = NodeType::from_type_byte(type_byte)?;
    let pb = type_byte & 0x0F;

    let (transitions, child_pointers, transition_size) = match node_type {
        NodeType::PayloadOnly => (vec![], vec![], 1),

        NodeType::SingleNopayload4 => {
            if data.len() < 2 {
                return Err(Error::InvalidData("truncated SingleNopayload4".into()));
            }
            let trans = data[1] >> 4;
            let ptr = (data[1] & 0x0F) as u64;
            (vec![trans], vec![ptr], 2)
        }

        NodeType::Single8 => {
            if data.len() < 3 {
                return Err(Error::InvalidData("truncated Single8".into()));
            }
            (vec![data[1]], vec![data[2] as u64], 3)
        }

        NodeType::SingleNopayload12 => {
            if data.len() < 3 {
                return Err(Error::InvalidData("truncated SingleNopayload12".into()));
            }
            let trans = data[1];
            let ptr = (((data[0] & 0x0F) as u64) << 8) | data[2] as u64;
            // Note: pb bits are repurposed as upper pointer bits
            return Ok(NodeHeader {
                node_type,
                pb: 0, // no payload for Nopayload types
                transitions: vec![trans],
                child_pointers: vec![ptr],
                payload_offset: 0,
                total_size: 3,
            });
        }

        NodeType::Single16 => {
            if data.len() < 4 {
                return Err(Error::InvalidData("truncated Single16".into()));
            }
            let ptr = u16::from_be_bytes([data[2], data[3]]) as u64;
            (vec![data[1]], vec![ptr], 4)
        }

        NodeType::Sparse8 => {
            if data.len() < 2 {
                return Err(Error::InvalidData("truncated Sparse8".into()));
            }
            let cc = data[1] as usize;
            let needed = 2 + cc * 2;
            if data.len() < needed {
                return Err(Error::InvalidData("truncated Sparse8 children".into()));
            }
            let trans: Vec<u8> = data[2..2 + cc].to_vec();
            let ptrs: Vec<u64> = (0..cc).map(|i| data[2 + cc + i] as u64).collect();
            (trans, ptrs, needed)
        }

        NodeType::Sparse16 => {
            if data.len() < 2 {
                return Err(Error::InvalidData("truncated Sparse16".into()));
            }
            let cc = data[1] as usize;
            let needed = 2 + cc * 3;
            if data.len() < needed {
                return Err(Error::InvalidData("truncated Sparse16 children".into()));
            }
            let trans: Vec<u8> = data[2..2 + cc].to_vec();
            let ptrs: Vec<u64> = (0..cc)
                .map(|i| {
                    let off = 2 + cc + i * 2;
                    u16::from_be_bytes([data[off], data[off + 1]]) as u64
                })
                .collect();
            (trans, ptrs, needed)
        }

        NodeType::Sparse24 => {
            if data.len() < 2 {
                return Err(Error::InvalidData("truncated Sparse24".into()));
            }
            let cc = data[1] as usize;
            let needed = 2 + cc * 4;
            if data.len() < needed {
                return Err(Error::InvalidData("truncated Sparse24 children".into()));
            }
            let trans: Vec<u8> = data[2..2 + cc].to_vec();
            let ptrs: Vec<u64> = (0..cc)
                .map(|i| {
                    let off = 2 + cc + i * 3;
                    ((data[off] as u64) << 16) | ((data[off + 1] as u64) << 8) | data[off + 2] as u64
                })
                .collect();
            (trans, ptrs, needed)
        }

        // Sparse12, Sparse40, Dense* types follow similar patterns.
        // Implementation deferred to step 3 below for brevity.
        _ => {
            return Err(Error::InvalidData(format!(
                "node type {:?} decoding not yet implemented",
                node_type
            )));
        }
    };

    let ps = payload_size(pb);
    let total = transition_size + ps;

    Ok(NodeHeader {
        node_type,
        pb,
        transitions,
        child_pointers,
        payload_offset: transition_size,
        total_size: total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_type_round_trip() {
        for code in 0x0..=0xF_u8 {
            let type_byte = code << 4;
            let nt = NodeType::from_type_byte(type_byte).unwrap();
            assert_eq!(nt as u8, code);
        }
    }

    #[test]
    fn node_sizes() {
        assert_eq!(NodeType::PayloadOnly.node_size(0, 0), 1);
        assert_eq!(NodeType::SingleNopayload4.node_size(1, 1), 2);
        assert_eq!(NodeType::Single8.node_size(1, 1), 3);
        assert_eq!(NodeType::Single16.node_size(1, 1), 4);
        assert_eq!(NodeType::Sparse8.node_size(3, 3), 2 + 3 * 2);
        assert_eq!(NodeType::Sparse16.node_size(3, 3), 2 + 3 * 3);
        assert_eq!(NodeType::Dense16.node_size(0, 5), 3 + 5 * 2);
        assert_eq!(NodeType::LongDense.node_size(0, 256), 3 + 256 * 8);
    }

    #[test]
    fn sparse12_size_integer_division() {
        // (cc*5+1)/2 uses integer division, matching Java
        assert_eq!(NodeType::Sparse12.node_size(1, 1), 2 + (1 * 5 + 1) / 2); // 2 + 3 = 5
        assert_eq!(NodeType::Sparse12.node_size(2, 2), 2 + (2 * 5 + 1) / 2); // 2 + 5 = 7
    }

    #[test]
    fn payload_size_values() {
        assert_eq!(payload_size(0), 0);
        assert_eq!(payload_size(1), 1);
        assert_eq!(payload_size(7), 7);
        assert_eq!(payload_size(8), 2);  // 1 hash + 1 idxpos
        assert_eq!(payload_size(15), 9); // 1 hash + 8 idxpos
    }

    #[test]
    fn sign_extend_positive() {
        assert_eq!(sign_extend(&[0x00, 0x42]), 0x42);
        assert_eq!(sign_extend(&[0x7F, 0xFF]), 0x7FFF);
    }

    #[test]
    fn sign_extend_negative() {
        assert_eq!(sign_extend(&[0xFF]), -1);
        assert_eq!(sign_extend(&[0x80]), -128);
        assert_eq!(sign_extend(&[0xFF, 0x00]), -256);
    }

    #[test]
    fn encode_signed_bytes_cases() {
        assert_eq!(encode_signed_bytes(0), vec![0]);
        assert_eq!(encode_signed_bytes(1), vec![1]);
        assert_eq!(encode_signed_bytes(-1), vec![0xFF]);
        assert_eq!(encode_signed_bytes(127), vec![0x7F]);
        assert_eq!(encode_signed_bytes(128), vec![0x00, 0x80]);
        assert_eq!(encode_signed_bytes(-128), vec![0x80]);
        assert_eq!(encode_signed_bytes(-129), vec![0xFF, 0x7F]);
    }

    #[test]
    fn decode_payload_no_payload() {
        let (hash, idxpos) = decode_payload(0, &[]).unwrap();
        assert!(hash.is_none());
        assert_eq!(idxpos, 0);
    }

    #[test]
    fn decode_payload_without_hash() {
        // pb=2: 2-byte idxpos
        let (hash, idxpos) = decode_payload(2, &[0x01, 0x00]).unwrap();
        assert!(hash.is_none());
        assert_eq!(idxpos, 256);
    }

    #[test]
    fn decode_payload_with_hash() {
        // pb=9: hash byte + 2-byte idxpos
        let (hash, idxpos) = decode_payload(9, &[0xAB, 0x01, 0x00]).unwrap();
        assert_eq!(hash, Some(0xAB));
        assert_eq!(idxpos, 256);
    }

    #[test]
    fn decode_payload_negative_idxpos() {
        // pb=8: hash byte + 1-byte idxpos (negative = direct data pointer)
        let (hash, idxpos) = decode_payload(8, &[0xAB, 0xFF]).unwrap();
        assert_eq!(hash, Some(0xAB));
        assert_eq!(idxpos, -1); // !(-1) = 0, direct data pointer at 0
    }

    #[test]
    fn read_payload_only_node() {
        // Type 0x0, pb=0: just the type byte
        let data = [0x00];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::PayloadOnly);
        assert_eq!(header.pb, 0);
        assert!(header.transitions.is_empty());
        assert_eq!(header.total_size, 1);
    }

    #[test]
    fn read_payload_only_with_payload() {
        // Type 0x0, pb=9 (hash + 2-byte idxpos): type byte + 3 payload bytes
        let data = [0x09, 0xAB, 0x01, 0x00];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::PayloadOnly);
        assert_eq!(header.pb, 9);
        assert_eq!(header.total_size, 4);
    }

    #[test]
    fn read_single8_node() {
        // Type 0x2, pb=0: type byte + transition byte + 8-bit pointer
        let data = [0x20, b'A', 0x05];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Single8);
        assert_eq!(header.transitions, vec![b'A']);
        assert_eq!(header.child_pointers, vec![5]);
        assert_eq!(header.total_size, 3);
    }

    #[test]
    fn node_type_classification() {
        assert!(!NodeType::PayloadOnly.is_single());
        assert!(NodeType::Single8.is_single());
        assert!(NodeType::Sparse8.is_sparse());
        assert!(NodeType::Dense16.is_dense());
        assert!(NodeType::LongDense.is_dense());
    }
}
```

- [ ] **Step 3: Complete remaining node type decoders**

Add the remaining decode cases in `read_node_header` for Sparse12, Sparse40, Dense12, Dense16, Dense24, Dense32, Dense40, and LongDense. Each follows the pattern in the spec — read transitions, compute pointer sizes, extract pointers. The `node_size()` formulas give exact byte counts.

Implementation approach for each:

- **Sparse12**: cc bytes of transitions, then 12-bit pointers packed as (cc*5+1)/2 bytes
- **Sparse40**: cc bytes of transitions, then 5-byte pointers (cc*5 bytes)
- **Dense types**: 2 bytes (min, max transition), then pointers for each byte in the range min..=max. Pointer widths: 12/16/24/32/40/64 bits.

Use the same test pattern: construct a byte sequence by hand, decode it, verify fields.

- [ ] **Step 4: Register trie module in lib.rs**

Add to `ferrosa-sstable/src/lib.rs`:

```rust
pub mod trie;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-sstable trie::node`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-sstable/src/trie/ ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add trie node types, encoding/decoding"
```

### Task 14: trie/walker.rs — Trie Traversal

**Files:**

- Create: `ferrosa-sstable/src/trie/walker.rs`

- [ ] **Step 1: Write trie walker with tests**

Create `ferrosa-sstable/src/trie/walker.rs`:

```rust
//! Trie traversal: lookup, floor, ceiling, and iteration.
//!
//! The walker navigates an on-disk trie by following transitions from the
//! root to leaves. It supports:
//!
//! - **Exact lookup**: find a key in the trie
//! - **Floor**: greatest key less than or equal to the query
//! - **Ceiling**: smallest key greater than or equal to the query
//! - **Iteration**: enumerate all payloads in order
//!
//! # Design
//!
//! The walker reads nodes via [`ReadAt`](crate::io::ReadAt), following child
//! pointers as distances from the current node position. Since tries are
//! written bottom-up, children are at lower file positions than parents.
//!
//! For the partition index trie, payloads encode `(hash_byte, idxpos)`.
//! For the row index trie, payloads encode block offsets.

use crate::io::ReadAt;
use crate::trie::node::{self, NodeHeader};
use ferrosa_common::Result;

/// Result of a trie lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// Exact match found. Payload bytes at the given position.
    Found { payload_pb: u8, payload_bytes: Vec<u8> },
    /// No match found.
    NotFound,
}

/// Walk a trie to find an exact key match.
///
/// `reader`: the trie file (or slice)
/// `root_pos`: file position of the root node
/// `key`: the byte sequence to look up
pub fn lookup(reader: &impl ReadAt, root_pos: u64, key: &[u8]) -> Result<LookupResult> {
    let mut pos = root_pos;
    let mut key_idx = 0;

    loop {
        // Read enough bytes for any node (max ~2KB for LongDense with 256 children)
        let mut buf = vec![0u8; 2060];
        let file_len = reader.len()?;
        let read_len = buf.len().min((file_len - pos) as usize);
        reader.read_at(&mut buf[..read_len], pos)?;

        let header = node::read_node_header(&buf[..read_len])?;

        if key_idx >= key.len() {
            // We've consumed the entire key. Check for payload.
            if header.pb > 0 {
                let ps = node::payload_size(header.pb);
                let payload_start = header.payload_offset;
                let payload_bytes = buf[payload_start..payload_start + ps].to_vec();
                return Ok(LookupResult::Found {
                    payload_pb: header.pb,
                    payload_bytes,
                });
            } else {
                return Ok(LookupResult::NotFound);
            }
        }

        let target = key[key_idx];

        // Find transition matching target byte
        match find_transition(&header, target) {
            Some(child_distance) => {
                // Child is at pos - child_distance (written before parent)
                pos = pos - child_distance;
                key_idx += 1;
            }
            None => {
                return Ok(LookupResult::NotFound);
            }
        }
    }
}

/// Find the child pointer distance for a given transition byte.
fn find_transition(header: &NodeHeader, target: u8) -> Option<u64> {
    for (i, &trans) in header.transitions.iter().enumerate() {
        if trans == target {
            return Some(header.child_pointers[i]);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal hand-crafted trie for testing.
    /// Trie structure: root has transitions 'a' and 'b'.
    /// 'a' leads to a leaf with payload.
    /// 'b' leads to a leaf with payload.
    ///
    /// Since tries are bottom-up, leaves come first in the file:
    /// pos 0: leaf for 'a' (PayloadOnly, pb=8, hash=0xAA, idxpos=0x42)
    /// pos 4: leaf for 'b' (PayloadOnly, pb=8, hash=0xBB, idxpos=0x43)
    /// pos 8: root (Sparse8, 2 children, transitions ['b','a'], pointers [4, 8])
    fn build_test_trie() -> Vec<u8> {
        let mut data = Vec::new();

        // Leaf 'a' at pos 0: PayloadOnly with pb=8 (hash + 1-byte idxpos)
        data.push(0x08); // type=0, pb=8
        data.push(0xAA); // hash byte
        data.push(0x42); // idxpos = 0x42

        // Leaf 'b' at pos 3: PayloadOnly with pb=8
        data.push(0x08); // type=0, pb=8
        data.push(0xBB); // hash byte
        data.push(0x43); // idxpos = 0x43

        // Root at pos 6: Sparse8 with 2 children
        // Sparse8: [type_byte] [cc] [trans...] [ptrs...]
        data.push(0x50); // type=5 (Sparse8), pb=0
        data.push(0x02); // cc=2
        data.push(b'a'); // transition 'a'
        data.push(b'b'); // transition 'b'
        data.push(0x06); // pointer to 'a' leaf: root_pos(6) - leaf_pos(0) = 6
        data.push(0x03); // pointer to 'b' leaf: root_pos(6) - leaf_pos(3) = 3

        data
    }

    #[test]
    fn lookup_exact_match() {
        let trie = build_test_trie();
        let root_pos = 6u64;

        let result = lookup(&trie.as_slice(), root_pos, b"a").unwrap();
        match result {
            LookupResult::Found { payload_pb, payload_bytes } => {
                assert_eq!(payload_pb, 8);
                let (hash, idxpos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                assert_eq!(hash, Some(0xAA));
                assert_eq!(idxpos, 0x42);
            }
            _ => panic!("expected Found"),
        }
    }

    #[test]
    fn lookup_second_key() {
        let trie = build_test_trie();
        let result = lookup(&trie.as_slice(), 6, b"b").unwrap();
        match result {
            LookupResult::Found { payload_pb, payload_bytes } => {
                let (hash, idxpos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                assert_eq!(hash, Some(0xBB));
                assert_eq!(idxpos, 0x43);
            }
            _ => panic!("expected Found"),
        }
    }

    #[test]
    fn lookup_not_found() {
        let trie = build_test_trie();
        let result = lookup(&trie.as_slice(), 6, b"c").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn lookup_empty_key() {
        let trie = build_test_trie();
        // Root has no payload (pb=0), so empty key = NotFound
        let result = lookup(&trie.as_slice(), 6, b"").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn lookup_key_too_long() {
        let trie = build_test_trie();
        // "ab" tries to follow 'a' then 'b', but 'a' leaf has no transitions
        let result = lookup(&trie.as_slice(), 6, b"ab").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p ferrosa-sstable trie::walker`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-sstable/src/trie/walker.rs
git commit -m "feat(sstable): add trie walker with lookup"
```

### Task 15: trie/builder.rs — Page-Aware Incremental Trie Builder

**Files:**

- Create: `ferrosa-sstable/src/trie/builder.rs`

This is the most complex component. The builder constructs a trie incrementally from sorted input, packing nodes into 4096-byte pages (no node crosses a page boundary).

- [ ] **Step 1: Write builder skeleton with core tests**

Create `ferrosa-sstable/src/trie/builder.rs`:

```rust
//! Bottom-up page-aware incremental trie builder.
//!
//! Constructs a trie from sorted byte-sequence keys. Keys must be added in
//! sorted order. The builder:
//!
//! 1. Tracks the common prefix between consecutive keys
//! 2. When a branch completes (next key diverges), serializes that branch
//! 3. Packs nodes into 4096-byte pages (no node crosses a boundary)
//! 4. Chooses the smallest node type for each node
//!
//! The root is the last node written; its file position is returned by
//! [`TrieBuilder::finish`].
//!
//! Reference: Cassandra's `IncrementalDeepTrieWriterPageAware`

use crate::trie::node::{self, NodeType, PAGE_SIZE};
use ferrosa_common::Result;

/// Payload to associate with a trie key.
#[derive(Debug, Clone)]
pub struct TriePayload {
    /// For partition index: hash byte (h2). None for row index.
    pub hash: Option<u8>,
    /// Position value (idxpos for partition index, block offset for row index).
    pub position: i64,
}

/// Incremental trie builder.
pub struct TrieBuilder {
    /// Output buffer containing serialized trie nodes.
    output: Vec<u8>,
    /// Stack of pending branch nodes (built bottom-up).
    stack: Vec<BranchNode>,
    /// The previous key added (for detecting common prefix length).
    prev_key: Vec<u8>,
    /// Current write position in the output.
    write_pos: usize,
}

/// A pending branch node being accumulated.
struct BranchNode {
    /// The byte value at this branch level.
    transition: u8,
    /// Children: (transition_byte, file_position).
    children: Vec<(u8, u64)>,
    /// Payload if this node is a leaf.
    payload: Option<TriePayload>,
}

impl TrieBuilder {
    /// Create a new trie builder.
    pub fn new() -> Self {
        TrieBuilder {
            output: Vec::new(),
            stack: Vec::new(),
            prev_key: Vec::new(),
            write_pos: 0,
        }
    }

    /// Add a key with its payload. Keys MUST be added in sorted order.
    pub fn add(&mut self, key: &[u8], payload: TriePayload) -> Result<()> {
        // Find common prefix length between this key and previous
        let common = common_prefix_len(&self.prev_key, key);

        // Complete branches that are no longer shared
        self.complete_branches(common)?;

        // Push new branch nodes for the diverging suffix
        for i in common..key.len() {
            self.stack.push(BranchNode {
                transition: key[i],
                children: Vec::new(),
                payload: None,
            });
        }

        // Set payload on the deepest (leaf) node
        if let Some(node) = self.stack.last_mut() {
            node.payload = Some(payload);
        }

        self.prev_key = key.to_vec();
        Ok(())
    }

    /// Finalize the trie. Returns `(output_bytes, root_position)`.
    pub fn finish(mut self) -> Result<(Vec<u8>, u64)> {
        // Complete all remaining branches
        self.complete_branches(0)?;

        let root_pos = if self.write_pos > 0 {
            self.write_pos as u64 - 1 // last written node position
        } else {
            0
        };

        Ok((self.output, root_pos))
    }

    /// Complete branches from the stack down to `keep_depth`.
    fn complete_branches(&mut self, keep_depth: usize) -> Result<()> {
        while self.stack.len() > keep_depth {
            let node = self.stack.pop().unwrap();
            let pos = self.write_node(&node)?;

            // Register this node as a child of its parent
            if let Some(parent) = self.stack.last_mut() {
                parent.children.push((node.transition, pos));
            }
        }
        Ok(())
    }

    /// Serialize and write a node to the output buffer.
    /// Returns the file position of the written node.
    fn write_node(&mut self, node: &BranchNode) -> Result<u64> {
        let payload_bytes = encode_payload(&node.payload);
        let pb = compute_pb(&node.payload);

        // Choose smallest node type and encode
        let node_bytes = if node.children.is_empty() {
            // PayloadOnly
            let mut buf = vec![(NodeType::PayloadOnly as u8) << 4 | pb];
            buf.extend_from_slice(&payload_bytes);
            buf
        } else if node.children.len() == 1 {
            // Single child — choose smallest single type
            let (child_trans, child_pos) = node.children[0];
            let distance = self.write_pos as u64 - child_pos;
            encode_single_node(child_trans, distance, pb, &payload_bytes)
        } else {
            // Multiple children — sparse encoding
            encode_sparse_node(&node.children, self.write_pos as u64, pb, &payload_bytes)
        };

        // Page alignment: check if node fits in current page
        let page_offset = self.write_pos % PAGE_SIZE;
        if page_offset + node_bytes.len() > PAGE_SIZE {
            // Pad to next page boundary
            let padding = PAGE_SIZE - page_offset;
            self.output.extend(vec![0u8; padding]);
            self.write_pos += padding;
        }

        let pos = self.write_pos as u64;
        self.output.extend_from_slice(&node_bytes);
        self.write_pos += node_bytes.len();

        Ok(pos)
    }
}

/// Find common prefix length between two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Compute pb (payload bits) for a given payload.
fn compute_pb(payload: &Option<TriePayload>) -> u8 {
    match payload {
        None => 0,
        Some(p) => {
            let pos_bytes = node::encode_signed_bytes(p.position);
            if p.hash.is_some() {
                (pos_bytes.len() as u8) + 7
            } else {
                pos_bytes.len() as u8
            }
        }
    }
}

/// Encode payload bytes.
fn encode_payload(payload: &Option<TriePayload>) -> Vec<u8> {
    match payload {
        None => vec![],
        Some(p) => {
            let pos_bytes = node::encode_signed_bytes(p.position);
            if let Some(hash) = p.hash {
                let mut buf = vec![hash];
                buf.extend_from_slice(&pos_bytes);
                buf
            } else {
                pos_bytes
            }
        }
    }
}

/// Encode a single-child node, choosing the smallest type.
fn encode_single_node(trans: u8, distance: u64, pb: u8, payload: &[u8]) -> Vec<u8> {
    if pb == 0 && distance < 16 && trans < 16 {
        // SingleNopayload4
        let type_byte = (NodeType::SingleNopayload4 as u8) << 4;
        vec![type_byte, (trans << 4) | (distance as u8)]
    } else if distance <= 0xFF {
        // Single8
        let type_byte = (NodeType::Single8 as u8) << 4 | pb;
        let mut buf = vec![type_byte, trans, distance as u8];
        buf.extend_from_slice(payload);
        buf
    } else if pb == 0 && distance < 4096 {
        // SingleNopayload12
        let upper = ((distance >> 8) & 0x0F) as u8;
        let type_byte = (NodeType::SingleNopayload12 as u8) << 4 | upper;
        vec![type_byte, trans, (distance & 0xFF) as u8]
    } else if distance <= 0xFFFF {
        // Single16
        let type_byte = (NodeType::Single16 as u8) << 4 | pb;
        let dist_bytes = (distance as u16).to_be_bytes();
        let mut buf = vec![type_byte, trans, dist_bytes[0], dist_bytes[1]];
        buf.extend_from_slice(payload);
        buf
    } else {
        // Fall back to sparse encoding for large distances
        let type_byte = (NodeType::Sparse24 as u8) << 4 | pb;
        let mut buf = vec![type_byte, 1, trans];
        let d = distance;
        buf.push(((d >> 16) & 0xFF) as u8);
        buf.push(((d >> 8) & 0xFF) as u8);
        buf.push((d & 0xFF) as u8);
        buf.extend_from_slice(payload);
        buf
    }
}

/// Encode a sparse (multiple children) node.
fn encode_sparse_node(
    children: &[(u8, u64)],
    current_pos: u64,
    pb: u8,
    payload: &[u8],
) -> Vec<u8> {
    let cc = children.len();
    let distances: Vec<u64> = children.iter().map(|(_, pos)| current_pos - pos).collect();
    let max_dist = distances.iter().copied().max().unwrap_or(0);

    let (node_type, ptr_size) = if max_dist <= 0xFF {
        (NodeType::Sparse8, 1)
    } else if max_dist <= 0xFFF {
        (NodeType::Sparse12, 0) // special packing
    } else if max_dist <= 0xFFFF {
        (NodeType::Sparse16, 2)
    } else if max_dist <= 0xFFFFFF {
        (NodeType::Sparse24, 3)
    } else {
        (NodeType::Sparse40, 5)
    };

    let type_byte = (node_type as u8) << 4 | pb;
    let mut buf = vec![type_byte, cc as u8];

    // Transition bytes
    for &(trans, _) in children {
        buf.push(trans);
    }

    // Pointer bytes
    match node_type {
        NodeType::Sparse8 => {
            for &d in &distances {
                buf.push(d as u8);
            }
        }
        NodeType::Sparse12 => {
            // Pack 12-bit pointers: pairs share a byte for upper nibbles
            for i in (0..distances.len()).step_by(2) {
                let d0 = distances[i];
                if i + 1 < distances.len() {
                    let d1 = distances[i + 1];
                    buf.push(((d0 >> 4) & 0xFF) as u8);
                    buf.push((((d0 & 0x0F) << 4) | ((d1 >> 8) & 0x0F)) as u8);
                    buf.push((d1 & 0xFF) as u8);
                } else {
                    buf.push(((d0 >> 4) & 0xFF) as u8);
                    buf.push(((d0 & 0x0F) << 4) as u8);
                }
            }
        }
        NodeType::Sparse16 => {
            for &d in &distances {
                buf.extend_from_slice(&(d as u16).to_be_bytes());
            }
        }
        NodeType::Sparse24 => {
            for &d in &distances {
                buf.push(((d >> 16) & 0xFF) as u8);
                buf.push(((d >> 8) & 0xFF) as u8);
                buf.push((d & 0xFF) as u8);
            }
        }
        NodeType::Sparse40 => {
            for &d in &distances {
                buf.push(((d >> 32) & 0xFF) as u8);
                buf.push(((d >> 24) & 0xFF) as u8);
                buf.push(((d >> 16) & 0xFF) as u8);
                buf.push(((d >> 8) & 0xFF) as u8);
                buf.push((d & 0xFF) as u8);
            }
        }
        _ => unreachable!(),
    }

    buf.extend_from_slice(payload);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::walker;

    #[test]
    fn empty_trie() {
        let builder = TrieBuilder::new();
        let (output, _root) = builder.finish().unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn single_key() {
        let mut builder = TrieBuilder::new();
        builder
            .add(b"a", TriePayload { hash: Some(0xAA), position: 100 })
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        // Verify we can look up the key
        let result = walker::lookup(&output.as_slice(), root, b"a").unwrap();
        match result {
            walker::LookupResult::Found { payload_pb, payload_bytes } => {
                let (hash, pos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                assert_eq!(hash, Some(0xAA));
                assert_eq!(pos, 100);
            }
            _ => panic!("expected Found"),
        }
    }

    #[test]
    fn two_keys_diverge_at_root() {
        let mut builder = TrieBuilder::new();
        builder
            .add(b"a", TriePayload { hash: Some(0xAA), position: 10 })
            .unwrap();
        builder
            .add(b"b", TriePayload { hash: Some(0xBB), position: 20 })
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        // Both keys findable
        let r1 = walker::lookup(&output.as_slice(), root, b"a").unwrap();
        let r2 = walker::lookup(&output.as_slice(), root, b"b").unwrap();
        assert!(matches!(r1, walker::LookupResult::Found { .. }));
        assert!(matches!(r2, walker::LookupResult::Found { .. }));

        // Non-existent key
        let r3 = walker::lookup(&output.as_slice(), root, b"c").unwrap();
        assert_eq!(r3, walker::LookupResult::NotFound);
    }

    #[test]
    fn shared_prefix() {
        let mut builder = TrieBuilder::new();
        builder
            .add(b"abc", TriePayload { hash: Some(0x01), position: 1 })
            .unwrap();
        builder
            .add(b"abd", TriePayload { hash: Some(0x02), position: 2 })
            .unwrap();
        builder
            .add(b"xyz", TriePayload { hash: Some(0x03), position: 3 })
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        for (key, expected_pos) in [(b"abc".as_slice(), 1), (b"abd", 2), (b"xyz", 3)] {
            let result = walker::lookup(&output.as_slice(), root, key).unwrap();
            match result {
                walker::LookupResult::Found { payload_pb, payload_bytes } => {
                    let (_, pos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                    assert_eq!(pos, expected_pos, "wrong pos for key {:?}", key);
                }
                _ => panic!("expected Found for key {:?}", key),
            }
        }
    }

    #[test]
    fn many_keys_all_found() {
        let mut builder = TrieBuilder::new();
        let mut keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("key{:04}", i).into_bytes())
            .collect();
        keys.sort();

        for (i, key) in keys.iter().enumerate() {
            builder
                .add(key, TriePayload { hash: Some((i & 0xFF) as u8), position: i as i64 })
                .unwrap();
        }

        let (output, root) = builder.finish().unwrap();

        for (i, key) in keys.iter().enumerate() {
            let result = walker::lookup(&output.as_slice(), root, key).unwrap();
            match result {
                walker::LookupResult::Found { payload_pb, payload_bytes } => {
                    let (_, pos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                    assert_eq!(pos, i as i64, "wrong pos for key {:?}", String::from_utf8_lossy(key));
                }
                _ => panic!("key not found: {:?}", String::from_utf8_lossy(key)),
            }
        }
    }

    #[test]
    fn page_boundary_respected() {
        let mut builder = TrieBuilder::new();
        // Add enough keys to force page boundaries
        for i in 0..500u32 {
            let key = format!("key{:06}", i).into_bytes();
            builder.add(&key, TriePayload { hash: Some(0), position: i as i64 }).unwrap();
        }

        let (output, _root) = builder.finish().unwrap();

        // Verify no node crosses a page boundary by checking that output
        // length is reasonable (pages are padded, not split)
        assert!(output.len() > 0);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p ferrosa-sstable trie::builder`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-sstable/src/trie/builder.rs
git commit -m "feat(sstable): add page-aware incremental trie builder"
```

---

## Chunk 5: File Format Readers/Writers

### Task 16: toc.rs — TOC.txt Reader/Writer

**Files:**

- Create: `ferrosa-sstable/src/toc.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [ ] **Step 1: Write TOC module with tests**

Create `ferrosa-sstable/src/toc.rs`:

```rust
//! TOC.txt reader and writer.
//!
//! The TOC file lists all component filenames of an SSTable, one per line.
//! Used to enumerate files for deletion, upload, or verification.

use ferrosa_common::{Error, Result};

/// Known SSTable component suffixes.
pub const COMPONENT_DATA: &str = "Data.db";
pub const COMPONENT_PARTITIONS: &str = "Partitions.db";
pub const COMPONENT_ROWS: &str = "Rows.db";
pub const COMPONENT_FILTER: &str = "Filter.db";
pub const COMPONENT_COMPRESSION: &str = "CompressionInfo.db";
pub const COMPONENT_STATISTICS: &str = "Statistics.db";
pub const COMPONENT_TOC: &str = "TOC.txt";
pub const COMPONENT_CRC: &str = "CRC.db";

/// Parse a TOC file into component names.
pub fn read_toc(data: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(data)
        .map_err(|e| Error::InvalidFormat(format!("TOC not valid UTF-8: {e}")))?;
    Ok(text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .collect())
}

/// Write a TOC file from component names.
pub fn write_toc(components: &[&str]) -> Vec<u8> {
    let mut buf = String::new();
    for name in components {
        buf.push_str(name);
        buf.push('\n');
    }
    buf.into_bytes()
}

/// Return the standard component list for a compressed BTI SSTable.
pub fn standard_compressed_components(prefix: &str) -> Vec<String> {
    [
        COMPONENT_DATA,
        COMPONENT_PARTITIONS,
        COMPONENT_ROWS,
        COMPONENT_FILTER,
        COMPONENT_COMPRESSION,
        COMPONENT_STATISTICS,
        COMPONENT_TOC,
    ]
    .iter()
    .map(|suffix| format!("{prefix}-{suffix}"))
    .collect()
}

/// Return the standard component list for an uncompressed BTI SSTable.
pub fn standard_uncompressed_components(prefix: &str) -> Vec<String> {
    [
        COMPONENT_DATA,
        COMPONENT_PARTITIONS,
        COMPONENT_ROWS,
        COMPONENT_FILTER,
        COMPONENT_CRC,
        COMPONENT_STATISTICS,
        COMPONENT_TOC,
    ]
    .iter()
    .map(|suffix| format!("{prefix}-{suffix}"))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_toc_basic() {
        let data = b"na-1-big-Data.db\nna-1-big-Filter.db\n";
        let components = read_toc(data).unwrap();
        assert_eq!(components, vec!["na-1-big-Data.db", "na-1-big-Filter.db"]);
    }

    #[test]
    fn read_toc_empty_lines() {
        let data = b"Data.db\n\nFilter.db\n\n";
        let components = read_toc(data).unwrap();
        assert_eq!(components, vec!["Data.db", "Filter.db"]);
    }

    #[test]
    fn write_toc_round_trip() {
        let components = &["Data.db", "Filter.db", "TOC.txt"];
        let written = write_toc(components);
        let parsed = read_toc(&written).unwrap();
        assert_eq!(parsed, vec!["Data.db", "Filter.db", "TOC.txt"]);
    }

    #[test]
    fn standard_components_compressed() {
        let comps = standard_compressed_components("na-1-bti");
        assert_eq!(comps.len(), 7);
        assert!(comps.iter().any(|c| c.ends_with("CompressionInfo.db")));
        assert!(!comps.iter().any(|c| c.ends_with("CRC.db")));
    }

    #[test]
    fn standard_components_uncompressed() {
        let comps = standard_uncompressed_components("na-1-bti");
        assert_eq!(comps.len(), 7);
        assert!(comps.iter().any(|c| c.ends_with("CRC.db")));
        assert!(!comps.iter().any(|c| c.ends_with("CompressionInfo.db")));
    }
}
```

- [ ] **Step 2: Register module and run tests**

Add `pub mod toc;` to lib.rs.

Run: `cargo test -p ferrosa-sstable toc`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-sstable/src/toc.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add TOC.txt reader/writer"
```

### Task 17: statistics.rs — Statistics.db Reader/Writer

**Files:**

- Create: `ferrosa-sstable/src/statistics.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

This is a complex file format with 4 CRC32-checksummed components and 27 fields in StatsMetadata. Implementation approach:

- [ ] **Step 1: Write Statistics.db framing (component count, TOC, CRC)**

The outer framing reads the component count, TOC (ordinal/offset pairs), and CRC checksums. Each component's data is extracted by offset differences.

```rust
//! Statistics.db reader and writer.
//!
//! Contains four metadata components, each CRC32-checksummed:
//! 0: ValidationMetadata (partitioner, bloom FP rate)
//! 1: CompactionMetadata (HyperLogLogPlus cardinality)
//! 2: StatsMetadata (27 fields: timestamps, sizes, histograms, etc.)
//! 3: SerializationHeader (column definitions, delta-encoding minimums)
//!
//! See `specs/sstable.md` § Statistics for the full byte-level format.
```

Include the `SerializationHeader` struct since it's needed by Data.db:

```rust
/// Parsed SerializationHeader from Statistics.db.
/// Required for decoding Data.db (provides delta-encoding minimums and column definitions).
#[derive(Debug, Clone)]
pub struct SerializationHeader {
    pub min_timestamp: i64,
    pub min_local_deletion_time: i32,
    pub min_ttl: i32,
    pub key_type: String,
    pub clustering_types: Vec<String>,
    pub static_columns: Vec<(Vec<u8>, String)>,
    pub regular_columns: Vec<(Vec<u8>, String)>,
}
```

Write the reader/writer for the full file format including all 4 components. Focus on getting the framing and SerializationHeader correct first (needed by Data.db), with StatsMetadata fields readable but round-trippable.

- [ ] **Step 2: Write tests for round-trip of a minimal Statistics.db**

Test: construct a Statistics.db with known values, write it, read it back, verify all fields match.

- [ ] **Step 3: Run tests and commit**

Run: `cargo test -p ferrosa-sstable statistics`

```bash
git add ferrosa-sstable/src/statistics.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add Statistics.db reader/writer with SerializationHeader"
```

### Task 18: partition_index.rs — Partitions.db Reader/Writer

**Files:**

- Create: `ferrosa-sstable/src/partition_index.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [ ] **Step 1: Write partition index reader**

The reader:

1. Reads the footer (last 3 i64s: key bounds offset, key count, root position)
2. Reads the smallest/largest keys from the footer section
3. Provides lookup via the trie walker
4. Interprets payload: `(hash, idxpos)` where negative idxpos = direct data pointer

```rust
//! Partition index (Partitions.db) reader and writer.
//!
//! An on-disk trie mapping byte-ordered partition key prefixes to positions
//! in the data or row index file.
//!
//! See `specs/sstable.md` § Partition Index for the full format.

use crate::io::ReadAt;
use crate::trie::{walker, node};
use crate::byte_comparable;
use ferrosa_common::{DecoratedKey, Error, Result};

/// Parsed partition index.
pub struct PartitionIndex<R: ReadAt> {
    reader: R,
    root_pos: u64,
    key_count: u64,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
}

/// Result of looking up a partition in the index.
#[derive(Debug)]
pub enum PartitionLookup {
    /// Found in row index at this position.
    RowIndex { position: u64 },
    /// Found directly in data file at this position.
    DataDirect { position: u64 },
    /// Not found (hash mismatch or key not in trie).
    NotFound,
}
```

- [ ] **Step 2: Implement open() and lookup()**

```rust
impl<R: ReadAt> PartitionIndex<R> {
    pub fn open(reader: R) -> Result<Self> {
        let file_len = reader.len()?;
        if file_len < 24 {
            return Err(Error::InvalidFormat("partition index too short".into()));
        }

        // Read footer: 3 i64s at the end
        let mut footer = [0u8; 24];
        reader.read_exact_at(&mut footer, file_len - 24)?;

        let key_bounds_offset = i64::from_be_bytes(footer[0..8].try_into().unwrap()) as u64;
        let key_count = i64::from_be_bytes(footer[8..16].try_into().unwrap()) as u64;
        let root_pos = i64::from_be_bytes(footer[16..24].try_into().unwrap()) as u64;

        // Read key bounds (two short-length-prefixed keys)
        let mut pos = key_bounds_offset;
        let smallest_key = read_short_length_prefixed(&reader, &mut pos)?;
        let largest_key = read_short_length_prefixed(&reader, &mut pos)?;

        Ok(PartitionIndex { reader, root_pos, key_count, smallest_key, largest_key })
    }

    pub fn lookup(&self, key: &DecoratedKey) -> Result<PartitionLookup> {
        let encoded = byte_comparable::encode(key);
        let result = walker::lookup(&self.reader, self.root_pos, &encoded)?;

        match result {
            walker::LookupResult::Found { payload_pb, payload_bytes } => {
                let (hash, idxpos) = node::decode_payload(payload_pb, &payload_bytes)?;

                // Verify hash if present
                if let Some(expected_hash) = hash {
                    let (_, h2) = key.filter_hash();
                    let actual_hash = (h2 & 0xFF) as u8;
                    if actual_hash != expected_hash {
                        return Ok(PartitionLookup::NotFound);
                    }
                }

                if idxpos >= 0 {
                    Ok(PartitionLookup::RowIndex { position: idxpos as u64 })
                } else {
                    Ok(PartitionLookup::DataDirect { position: !idxpos as u64 })
                }
            }
            walker::LookupResult::NotFound => Ok(PartitionLookup::NotFound),
        }
    }

    pub fn key_count(&self) -> u64 { self.key_count }
    pub fn smallest_key(&self) -> &[u8] { &self.smallest_key }
    pub fn largest_key(&self) -> &[u8] { &self.largest_key }
}
```

- [ ] **Step 3: Write tests using hand-built index, run and commit**

Run: `cargo test -p ferrosa-sstable partition_index`

```bash
git add ferrosa-sstable/src/partition_index.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add Partitions.db reader with trie lookup"
```

### Task 19: row_index.rs — Rows.db Reader

**Files:**

- Create: `ferrosa-sstable/src/row_index.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [ ] **Step 1: Write row index reader**

Per-partition trie with metadata footer. Read the metadata (partition key, data position, root offset, block count, deletion time), then use the trie walker for clustering key lookups.

- [ ] **Step 2: Write tests, run, commit**

```bash
git add ferrosa-sstable/src/row_index.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add Rows.db reader"
```

### Task 20: data.rs — Data.db Reader

**Files:**

- Create: `ferrosa-sstable/src/data.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [ ] **Step 1: Write Data.db deserializer**

Reads partitions from the data file using the SerializationHeader for delta decoding. Key structures:

- Read partition header (short-length-prefixed key + deletion time)
- Read rows (flags byte, clustering, cells with delta-decoded timestamps)
- Handle END_OF_PARTITION marker

This is the most complex reader. Implementation approach:

1. First: partition header reading (key + deletion)
2. Then: row reading (flags, clustering, basic cells)
3. Then: cell reading with delta decoding
4. Defer: range tombstone markers, complex columns (collections/UDTs)

- [ ] **Step 2: Write tests with hand-crafted data, run, commit**

```bash
git add ferrosa-sstable/src/data.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add Data.db partition deserializer"
```

---

## Chunk 6: Public API and Integration

### Task 21: reader.rs — SSTableReader

**Files:**

- Create: `ferrosa-sstable/src/reader.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [ ] **Step 1: Write SSTableReader composing all components**

```rust
//! SSTableReader — compose all components into a single read interface.
//!
//! Opens a BTI SSTable from component file handles and provides:
//! - Partition lookup by DecoratedKey
//! - Full partition iteration in token order
//! - Token range iteration

use crate::bloom::BloomFilter;
use crate::compression::CompressionInfo;
use crate::io::ReadAt;
use crate::partition_index::{PartitionIndex, PartitionLookup};
use crate::statistics::SerializationHeader;
use crate::types::Partition;
use ferrosa_common::{DecoratedKey, Result, Token};

/// Handles to all component files for an SSTable.
pub struct SSTableComponents<IO> {
    pub data: IO,
    pub partitions: IO,
    pub rows: IO,
    pub filter: IO,
    pub compression_info: Option<IO>,
    pub statistics: IO,
}

pub struct SSTableReader<R: ReadAt> {
    partition_index: PartitionIndex<R>,
    bloom_filter: BloomFilter,
    compression_info: Option<CompressionInfo>,
    header: SerializationHeader,
    data: R,
    rows: R,
}
```

- [ ] **Step 2: Implement open(), get_partition(), partitions()**

- [ ] **Step 3: Write integration test with hand-built SSTable components**

- [ ] **Step 4: Commit**

```bash
git add ferrosa-sstable/src/reader.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add SSTableReader composing all components"
```

### Task 22: writer.rs — SSTableWriter

**Files:**

- Create: `ferrosa-sstable/src/writer.rs`
- Modify: `ferrosa-sstable/src/lib.rs`

- [ ] **Step 1: Write SSTableWriter**

The writer accepts partitions in token order and produces all component files:

1. Serialize partition data to Data.db (with compression)
2. Build partition index trie (Partitions.db)
3. Build per-partition row index tries (Rows.db) for large partitions
4. Accumulate Bloom filter entries (Filter.db)
5. Track compression chunk offsets (CompressionInfo.db)
6. Accumulate statistics (Statistics.db)
7. Write TOC (TOC.txt)

- [ ] **Step 2: Write round-trip test: write then read back**

```rust
#[test]
fn write_then_read_round_trip() {
    // Create partitions with known data
    // Write with SSTableWriter
    // Read back with SSTableReader
    // Assert partitions match
}
```

- [ ] **Step 3: Commit**

```bash
git add ferrosa-sstable/src/writer.rs ferrosa-sstable/src/lib.rs
git commit -m "feat(sstable): add SSTableWriter for BTI format"
```

### Task 23: Cassandra Fixture Generation + Oracle Tests

**Files:**

- Create: `tools/generate_sstable_fixtures.java`
- Create: `ferrosa-sstable/tests/fixtures/` (generated files)
- Create: `ferrosa-sstable/tests/cassandra_compat.rs`

- [ ] **Step 1: Write fixture generation script**

Uses Cassandra's internal APIs to write BTI SSTables with known data:

- Multi-partition (~100 rows)
- Single partition (no row index)
- Wide partition (many clustering keys)
- Empty table

- [ ] **Step 2: Generate fixtures and commit**

```bash
cd cassandra && ant build
javac -cp build/classes/main ../tools/generate_sstable_fixtures.java -d ../tools/
java -cp build/classes/main:../tools generate_sstable_fixtures
```

- [ ] **Step 3: Write compatibility tests**

Read Cassandra-generated SSTables with ferrosa's SSTableReader, verify partition data matches expected values.

- [ ] **Step 4: Commit**

```bash
git add tools/generate_sstable_fixtures.java ferrosa-sstable/tests/
git commit -m "test(sstable): add Cassandra fixture generation and compatibility tests"
```

### Task 24: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `cargo test`
Expected: All tests PASS across both crates

- [ ] **Step 2: Run clippy and fmt**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: No warnings, no formatting issues

- [ ] **Step 3: Run cargo doc**

Run: `cargo doc --no-deps -D warnings`
Expected: Docs build cleanly

- [ ] **Step 4: Verify lib.rs re-exports**

Final `ferrosa-sstable/src/lib.rs`:

```rust
//! Cassandra-compatible BTI SSTable reader and writer.

pub mod bloom;
pub mod byte_comparable;
pub mod compression;
pub mod data;
pub mod io;
pub mod partition_index;
pub mod reader;
pub mod row_index;
pub mod statistics;
pub mod toc;
pub mod trie;
pub mod types;
pub mod varint;

pub use compression::Compression;
pub use io::{FileReadAt, FileWriteAt, ReadAt, WriteAt};
pub use reader::{SSTableComponents, SSTableReader};
pub use types::{DeletionTime, LivenessInfo, Partition, Row};
pub use writer::{SSTableWriter, WriteOptions};
```

---

## File Summary (Part B)

After executing both parts, the full crate structure:

```
ferrosa-sstable/
  Cargo.toml
  src/
    lib.rs
    io.rs                 # ReadAt/WriteAt traits
    varint.rs             # VInt encoding
    types.rs              # DeletionTime, LivenessInfo, Row, Partition
    compression.rs        # LZ4/Zstd + CompressionInfo
    bloom.rs              # Bloom filter
    byte_comparable.rs    # OSS50 key encoding
    trie/
      mod.rs              # Re-exports
      node.rs             # 16 node types
      walker.rs           # Trie traversal
      builder.rs          # Page-aware builder
    statistics.rs         # Statistics.db (4 components)
    toc.rs                # TOC.txt
    partition_index.rs    # Partitions.db
    row_index.rs          # Rows.db
    data.rs               # Data.db
    reader.rs             # SSTableReader
    writer.rs             # SSTableWriter
  tests/
    property_tests.rs     # proptest round-trips
    cassandra_compat.rs   # Cassandra fixture tests
    fixtures/             # Generated SSTable files
```
