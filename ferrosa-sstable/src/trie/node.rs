//! Trie node types and binary encoding/decoding.
//!
//! Each node starts with a single byte: upper 4 bits = node type (0x0–0xF),
//! lower 4 bits = payload flags. The type determines how child transitions
//! and pointers are laid out.
//!
//! This module is the Rust equivalent of Cassandra's `TrieNode.java`.

use ferrosa_common::{Error, Result};

/// Trie page size in bytes. Nodes never cross a page boundary.
pub const PAGE_SIZE: usize = 4096;

/// All 16 node type codes used in the BTI trie format.
///
/// The discriminant value matches the 4-bit type code stored in the upper
/// nibble of the first node byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    /// Leaf node with payload but no child transitions.
    PayloadOnly = 0,
    /// Single child, 4-bit pointer, no payload.
    SingleNopayload4 = 1,
    /// Single child, 8-bit pointer, has payload.
    Single8 = 2,
    /// Single child, 12-bit pointer, no payload.
    SingleNopayload12 = 3,
    /// Single child, 16-bit pointer, has payload.
    Single16 = 4,
    /// Sparse children, 8-bit pointers.
    Sparse8 = 5,
    /// Sparse children, 12-bit pointers.
    Sparse12 = 6,
    /// Sparse children, 16-bit pointers.
    Sparse16 = 7,
    /// Sparse children, 24-bit pointers.
    Sparse24 = 8,
    /// Sparse children, 40-bit pointers.
    Sparse40 = 9,
    /// Dense children, 12-bit pointers.
    Dense12 = 10,
    /// Dense children, 16-bit pointers.
    Dense16 = 11,
    /// Dense children, 24-bit pointers.
    Dense24 = 12,
    /// Dense children, 32-bit pointers.
    Dense32 = 13,
    /// Dense children, 40-bit pointers.
    Dense40 = 14,
    /// Dense children, 64-bit pointers (catch-all).
    LongDense = 15,
}

impl NodeType {
    /// Parse a node type from the first byte of a node.
    ///
    /// Extracts the upper 4 bits to determine the type code.
    pub fn from_type_byte(byte: u8) -> Result<Self> {
        let code = byte >> 4;
        match code {
            0 => Ok(NodeType::PayloadOnly),
            1 => Ok(NodeType::SingleNopayload4),
            2 => Ok(NodeType::Single8),
            3 => Ok(NodeType::SingleNopayload12),
            4 => Ok(NodeType::Single16),
            5 => Ok(NodeType::Sparse8),
            6 => Ok(NodeType::Sparse12),
            7 => Ok(NodeType::Sparse16),
            8 => Ok(NodeType::Sparse24),
            9 => Ok(NodeType::Sparse40),
            10 => Ok(NodeType::Dense12),
            11 => Ok(NodeType::Dense16),
            12 => Ok(NodeType::Dense24),
            13 => Ok(NodeType::Dense32),
            14 => Ok(NodeType::Dense40),
            15 => Ok(NodeType::LongDense),
            _ => Err(Error::InvalidData(format!(
                "invalid trie node type code: {code}"
            ))),
        }
    }

    /// Compute the size in bytes of a node (excluding payload).
    ///
    /// - `cc`: child count (for sparse types) or child span (for dense types)
    /// - `cs`: child span = max_transition - min_transition + 1 (dense only)
    ///
    /// For single/payload-only types, both arguments are ignored.
    /// For sparse types, `cc` is the child count.
    /// For dense types, `cs` is the child span.
    pub fn node_size(self, cc: usize, cs: usize) -> usize {
        match self {
            NodeType::PayloadOnly => 1,
            NodeType::SingleNopayload4 => 2,
            NodeType::Single8 => 3,
            NodeType::SingleNopayload12 => 3,
            NodeType::Single16 => 4,
            // Sparse: 2 + cc * (1 + bytes_per_pointer)
            // Sparse12 is special: 2 + ceil(cc * 5 / 2)
            NodeType::Sparse8 => 2 + cc * 2,
            NodeType::Sparse12 => 2 + (cc * 5).div_ceil(2),
            NodeType::Sparse16 => 2 + cc * 3,
            NodeType::Sparse24 => 2 + cc * 4,
            NodeType::Sparse40 => 2 + cc * 6,
            // Dense: 3 + cs * bytes_per_pointer
            // Dense12 is special: 3 + ceil(cs * 3 / 2)
            NodeType::Dense12 => 3 + (cs * 3).div_ceil(2),
            NodeType::Dense16 => 3 + cs * 2,
            NodeType::Dense24 => 3 + cs * 3,
            NodeType::Dense32 => 3 + cs * 4,
            NodeType::Dense40 => 3 + cs * 5,
            NodeType::LongDense => 3 + cs * 8,
        }
    }

    /// Returns `true` for single-child node types.
    pub fn is_single(self) -> bool {
        matches!(
            self,
            NodeType::SingleNopayload4
                | NodeType::Single8
                | NodeType::SingleNopayload12
                | NodeType::Single16
        )
    }

    /// Returns `true` for sparse (list-of-transitions) node types.
    pub fn is_sparse(self) -> bool {
        matches!(
            self,
            NodeType::Sparse8
                | NodeType::Sparse12
                | NodeType::Sparse16
                | NodeType::Sparse24
                | NodeType::Sparse40
        )
    }

    /// Returns `true` for dense (range-of-transitions) node types.
    pub fn is_dense(self) -> bool {
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

// ---------------------------------------------------------------------------
// Payload helpers
// ---------------------------------------------------------------------------

/// Return the number of payload bytes for a given payload-bits nibble.
///
/// - `pb == 0`: no payload (returns 0)
/// - `pb < 8`: `pb` bytes of sign-extended integer (idxpos)
/// - `pb >= 8`: 1 hash byte + (`pb - 7`) bytes of sign-extended integer
pub fn payload_size(pb: u8) -> usize {
    if pb == 0 {
        0
    } else if pb < 8 {
        pb as usize
    } else {
        1 + (pb - 7) as usize
    }
}

/// Decode a trie payload into (optional hash byte, idxpos).
///
/// - `pb`: the 4-bit payload flags from the node header
/// - `payload_bytes`: the raw payload bytes starting at ppos
///
/// Returns `(None, 0)` if `pb == 0`.
/// Returns `(None, idxpos)` if `pb` in 1..=7.
/// Returns `(Some(hash), idxpos)` if `pb` >= 8.
pub fn decode_payload(pb: u8, payload_bytes: &[u8]) -> Result<(Option<u8>, i64)> {
    if pb == 0 {
        return Ok((None, 0));
    }
    if pb < 8 {
        let len = pb as usize;
        if payload_bytes.len() < len {
            return Err(Error::InvalidData(format!(
                "payload too short: need {len} bytes, have {}",
                payload_bytes.len()
            )));
        }
        let val = sign_extend(&payload_bytes[..len]);
        Ok((None, val))
    } else {
        let idx_len = (pb - 7) as usize;
        let total = 1 + idx_len;
        if payload_bytes.len() < total {
            return Err(Error::InvalidData(format!(
                "payload too short: need {total} bytes, have {}",
                payload_bytes.len()
            )));
        }
        let hash = payload_bytes[0];
        let val = sign_extend(&payload_bytes[1..1 + idx_len]);
        Ok((Some(hash), val))
    }
}

/// Sign-extend a big-endian byte sequence of 0..=8 bytes into an i64.
///
/// An empty slice returns 0. Otherwise the most significant bit of the
/// first byte is the sign bit, and the value is sign-extended to 64 bits.
pub fn sign_extend(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }
    // Start with sign extension: if MSB is set, start with all-ones
    let mut val: i64 = if bytes[0] & 0x80 != 0 { -1 } else { 0 };
    for &b in bytes {
        val = (val << 8) | (b as i64);
    }
    val
}

/// Encode a signed i64 value into the minimum number of big-endian bytes.
///
/// Returns 0 bytes for value 0, otherwise the smallest representation
/// that preserves the sign bit.
pub fn encode_signed_bytes(value: i64) -> Vec<u8> {
    if value == 0 {
        return Vec::new();
    }
    // Determine the number of bytes needed (same as Java's SizedInts.nonZeroSize)
    let abs = if value < 0 { !value } else { value };
    let lz = abs.leading_zeros(); // 1..=63 for non-zero, 64 for zero
    let num_bytes = (64 - lz + 1).div_ceil(8) as usize; // significant bits + 1 sign bit, rounded up

    let mut result = Vec::with_capacity(num_bytes);
    for i in (0..num_bytes).rev() {
        result.push((value >> (i * 8)) as u8);
    }
    result
}

// ---------------------------------------------------------------------------
// 12-bit pointer reading helper
// ---------------------------------------------------------------------------

/// Read a 12-bit value at the given index from a packed 12-bit array.
///
/// The array starts at `base` offset in `data`. Two 12-bit values are
/// packed into 3 bytes: `[hi0, lo0|hi1, lo1]`.
///
/// Even index: value = (data[base + idx*3/2] << 4) | (data[base + idx*3/2 + 1] >> 4)
/// Odd index:  value = ((data[base + idx*3/2] & 0x0F) << 8) | data[base + idx*3/2 + 1]
fn read_12bits(data: &[u8], base: usize, index: usize) -> Result<u16> {
    let byte_offset = base + (3 * index) / 2;
    if byte_offset + 1 >= data.len() {
        return Err(Error::InvalidData(
            "12-bit pointer read out of bounds".to_string(),
        ));
    }
    let b0 = data[byte_offset] as u16;
    let b1 = data[byte_offset + 1] as u16;
    let val = if (index & 1) == 0 {
        // Even: use upper byte fully and high nibble of next byte
        (b0 << 4) | (b1 >> 4)
    } else {
        // Odd: low nibble of first byte and full next byte
        ((b0 & 0x0F) << 8) | b1
    };
    Ok(val & 0xFFF)
}

/// Read an unsigned big-endian integer of `n` bytes from `data[offset..]`.
fn read_unsigned_be(data: &[u8], offset: usize, n: usize) -> Result<u64> {
    if offset + n > data.len() {
        return Err(Error::InvalidData(format!(
            "read_unsigned_be: need {} bytes at offset {}, have {}",
            n,
            offset,
            data.len()
        )));
    }
    let mut val: u64 = 0;
    for i in 0..n {
        val = (val << 8) | (data[offset + i] as u64);
    }
    Ok(val)
}

// ---------------------------------------------------------------------------
// NodeHeader — decoded representation of a trie node
// ---------------------------------------------------------------------------

/// Decoded trie node header.
///
/// Contains the node type, payload flags, transitions (child bytes and
/// pointer distances), and the position just past the node where payload
/// data starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHeader {
    /// The type of this node.
    pub node_type: NodeType,
    /// 4-bit payload flags (0 = no payload).
    pub payload_bits: u8,
    /// Transition bytes for each child (sorted for sparse, implied range for dense).
    pub transitions: Vec<u8>,
    /// Pointer distance for each transition (negated: subtract from node position to get child).
    /// For dense nodes a value of 0 means "no child at this slot".
    pub pointers: Vec<u64>,
    /// Byte offset just past the node header (where payload is stored).
    pub payload_position: usize,
}

/// Read and decode a trie node header from raw bytes.
///
/// `data` should start at the node position (offset 0 = type byte).
/// Returns the decoded header including transitions, pointers, and
/// the byte offset of the payload within `data`.
pub fn read_node_header(data: &[u8]) -> Result<NodeHeader> {
    if data.is_empty() {
        return Err(Error::InvalidData("empty node data".to_string()));
    }
    let type_byte = data[0];
    let node_type = NodeType::from_type_byte(type_byte)?;
    let payload_bits = type_byte & 0x0F;

    match node_type {
        NodeType::PayloadOnly => Ok(NodeHeader {
            node_type,
            payload_bits,
            transitions: Vec::new(),
            pointers: Vec::new(),
            payload_position: 1,
        }),

        NodeType::SingleNopayload4 => {
            // 4-bit type + 4-bit pointer in byte 0, transition byte in byte 1
            if data.len() < 2 {
                return Err(Error::InvalidData(
                    "SingleNopayload4: need 2 bytes".to_string(),
                ));
            }
            let ptr = (type_byte & 0x0F) as u64;
            let transition = data[1];
            Ok(NodeHeader {
                node_type,
                payload_bits: 0, // no payload for this type
                transitions: vec![transition],
                pointers: vec![ptr],
                payload_position: 2,
            })
        }

        NodeType::Single8 => {
            // byte 0: type|pb, byte 1: transition, byte 2: 8-bit pointer
            if data.len() < 3 {
                return Err(Error::InvalidData("Single8: need 3 bytes".to_string()));
            }
            let transition = data[1];
            let ptr = data[2] as u64;
            Ok(NodeHeader {
                node_type,
                payload_bits,
                transitions: vec![transition],
                pointers: vec![ptr],
                payload_position: 3,
            })
        }

        NodeType::SingleNopayload12 => {
            // byte 0: type(4)|ptr_hi(4), byte 1: ptr_lo(8), byte 2: transition
            if data.len() < 3 {
                return Err(Error::InvalidData(
                    "SingleNopayload12: need 3 bytes".to_string(),
                ));
            }
            let ptr_hi = (type_byte & 0x0F) as u64;
            let ptr_lo = data[1] as u64;
            let ptr = (ptr_hi << 8) | ptr_lo;
            let transition = data[2];
            Ok(NodeHeader {
                node_type,
                payload_bits: 0,
                transitions: vec![transition],
                pointers: vec![ptr],
                payload_position: 3,
            })
        }

        NodeType::Single16 => {
            // byte 0: type|pb, byte 1: transition, bytes 2-3: 16-bit BE pointer
            if data.len() < 4 {
                return Err(Error::InvalidData("Single16: need 4 bytes".to_string()));
            }
            let transition = data[1];
            let ptr = read_unsigned_be(data, 2, 2)?;
            Ok(NodeHeader {
                node_type,
                payload_bits,
                transitions: vec![transition],
                pointers: vec![ptr],
                payload_position: 4,
            })
        }

        // Sparse types: byte 0 = type|pb, byte 1 = count, then count transition bytes,
        // then count pointers of varying sizes.
        NodeType::Sparse8 => read_sparse_header(data, node_type, payload_bits, 1),
        NodeType::Sparse12 => read_sparse12_header(data, node_type, payload_bits),
        NodeType::Sparse16 => read_sparse_header(data, node_type, payload_bits, 2),
        NodeType::Sparse24 => read_sparse_header(data, node_type, payload_bits, 3),
        NodeType::Sparse40 => read_sparse_header(data, node_type, payload_bits, 5),

        // Dense types: byte 0 = type|pb, byte 1 = start, byte 2 = length-1,
        // then (length) pointers of varying sizes.
        NodeType::Dense12 => read_dense12_header(data, node_type, payload_bits),
        NodeType::Dense16 => read_dense_header(data, node_type, payload_bits, 2),
        NodeType::Dense24 => read_dense_header(data, node_type, payload_bits, 3),
        NodeType::Dense32 => read_dense_header(data, node_type, payload_bits, 4),
        NodeType::Dense40 => read_dense_header(data, node_type, payload_bits, 5),
        NodeType::LongDense => read_dense_header(data, node_type, payload_bits, 8),
    }
}

/// Read a sparse node with fixed-width (non-12-bit) pointers.
fn read_sparse_header(
    data: &[u8],
    node_type: NodeType,
    payload_bits: u8,
    bytes_per_ptr: usize,
) -> Result<NodeHeader> {
    if data.len() < 2 {
        return Err(Error::InvalidData(
            "sparse node: need at least 2 bytes".to_string(),
        ));
    }
    let cc = data[1] as usize;
    let transitions_start = 2;
    let pointers_start = transitions_start + cc;
    let total = pointers_start + cc * bytes_per_ptr;
    if data.len() < total {
        return Err(Error::InvalidData(format!(
            "sparse node: need {total} bytes, have {}",
            data.len()
        )));
    }

    let transitions: Vec<u8> = data[transitions_start..transitions_start + cc].to_vec();
    let mut pointers = Vec::with_capacity(cc);
    for i in 0..cc {
        let ptr = read_unsigned_be(data, pointers_start + i * bytes_per_ptr, bytes_per_ptr)?;
        pointers.push(ptr);
    }

    Ok(NodeHeader {
        node_type,
        payload_bits,
        transitions,
        pointers,
        payload_position: total,
    })
}

/// Read a Sparse12 node with 12-bit packed pointers.
fn read_sparse12_header(data: &[u8], node_type: NodeType, payload_bits: u8) -> Result<NodeHeader> {
    if data.len() < 2 {
        return Err(Error::InvalidData(
            "Sparse12 node: need at least 2 bytes".to_string(),
        ));
    }
    let cc = data[1] as usize;
    let transitions_start = 2;
    let pointers_start = transitions_start + cc;
    let payload_pos = 2 + (cc * 5).div_ceil(2);

    if data.len() < payload_pos {
        return Err(Error::InvalidData(format!(
            "Sparse12 node: need {} bytes, have {}",
            payload_pos,
            data.len()
        )));
    }

    let transitions: Vec<u8> = data[transitions_start..transitions_start + cc].to_vec();
    let mut pointers = Vec::with_capacity(cc);
    for i in 0..cc {
        let ptr = read_12bits(data, pointers_start, i)? as u64;
        pointers.push(ptr);
    }

    Ok(NodeHeader {
        node_type,
        payload_bits,
        transitions,
        pointers,
        payload_position: payload_pos,
    })
}

/// Read a dense node with fixed-width (non-12-bit) pointers.
fn read_dense_header(
    data: &[u8],
    node_type: NodeType,
    payload_bits: u8,
    bytes_per_ptr: usize,
) -> Result<NodeHeader> {
    if data.len() < 3 {
        return Err(Error::InvalidData(
            "dense node: need at least 3 bytes".to_string(),
        ));
    }
    let start_byte = data[1];
    let len = (data[2] as usize) + 1; // stored as length-1
    let pointers_start = 3;
    let total = pointers_start + len * bytes_per_ptr;
    if data.len() < total {
        return Err(Error::InvalidData(format!(
            "dense node: need {total} bytes, have {}",
            data.len()
        )));
    }

    let mut transitions = Vec::with_capacity(len);
    let mut pointers = Vec::with_capacity(len);
    for i in 0..len {
        transitions.push(start_byte.wrapping_add(i as u8));
        let ptr = read_unsigned_be(data, pointers_start + i * bytes_per_ptr, bytes_per_ptr)?;
        pointers.push(ptr);
    }

    Ok(NodeHeader {
        node_type,
        payload_bits,
        transitions,
        pointers,
        payload_position: total,
    })
}

/// Read a Dense12 node with 12-bit packed pointers.
fn read_dense12_header(data: &[u8], node_type: NodeType, payload_bits: u8) -> Result<NodeHeader> {
    if data.len() < 3 {
        return Err(Error::InvalidData(
            "Dense12 node: need at least 3 bytes".to_string(),
        ));
    }
    let start_byte = data[1];
    let len = (data[2] as usize) + 1;
    let pointers_start = 3;
    let payload_pos = 3 + (len * 3).div_ceil(2);

    if data.len() < payload_pos {
        return Err(Error::InvalidData(format!(
            "Dense12 node: need {} bytes, have {}",
            payload_pos,
            data.len()
        )));
    }

    let mut transitions = Vec::with_capacity(len);
    let mut pointers = Vec::with_capacity(len);
    for i in 0..len {
        transitions.push(start_byte.wrapping_add(i as u8));
        let ptr = read_12bits(data, pointers_start, i)? as u64;
        pointers.push(ptr);
    }

    Ok(NodeHeader {
        node_type,
        payload_bits,
        transitions,
        pointers,
        payload_position: payload_pos,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // NodeType round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn node_type_round_trip() {
        for code in 0u8..=15 {
            let byte = code << 4;
            let nt = NodeType::from_type_byte(byte).unwrap();
            assert_eq!(nt as u8, code, "round-trip failed for code {code}");
        }
    }

    // -----------------------------------------------------------------------
    // node_size
    // -----------------------------------------------------------------------

    #[test]
    fn node_sizes() {
        assert_eq!(NodeType::PayloadOnly.node_size(0, 0), 1);
        assert_eq!(NodeType::SingleNopayload4.node_size(1, 0), 2);
        assert_eq!(NodeType::Single8.node_size(1, 0), 3);
        assert_eq!(NodeType::SingleNopayload12.node_size(1, 0), 3);
        assert_eq!(NodeType::Single16.node_size(1, 0), 4);

        // Sparse with cc=3
        assert_eq!(NodeType::Sparse8.node_size(3, 0), 2 + 3 * 2);
        assert_eq!(NodeType::Sparse16.node_size(3, 0), 2 + 3 * 3);
        assert_eq!(NodeType::Sparse24.node_size(3, 0), 2 + 3 * 4);
        assert_eq!(NodeType::Sparse40.node_size(3, 0), 2 + 3 * 6);

        // Dense with cs=10
        assert_eq!(NodeType::Dense16.node_size(0, 10), 3 + 10 * 2);
        assert_eq!(NodeType::Dense24.node_size(0, 10), 3 + 10 * 3);
        assert_eq!(NodeType::Dense32.node_size(0, 10), 3 + 10 * 4);
        assert_eq!(NodeType::Dense40.node_size(0, 10), 3 + 10 * 5);
        assert_eq!(NodeType::LongDense.node_size(0, 10), 3 + 10 * 8);
    }

    #[test]
    fn sparse12_size_integer_division() {
        // ceil(cc * 5 / 2) for various counts
        assert_eq!(NodeType::Sparse12.node_size(1, 0), 5); // 2 + 3
        assert_eq!(NodeType::Sparse12.node_size(2, 0), 7); // 2 + 5
        assert_eq!(NodeType::Sparse12.node_size(3, 0), 10); // 2 + 8
        assert_eq!(NodeType::Sparse12.node_size(4, 0), 12); // 2 + 10
    }

    // -----------------------------------------------------------------------
    // payload_size
    // -----------------------------------------------------------------------

    #[test]
    fn payload_size_values() {
        assert_eq!(payload_size(0), 0);
        assert_eq!(payload_size(1), 1);
        assert_eq!(payload_size(7), 7);
        assert_eq!(payload_size(8), 2); // 1 hash + 1 byte
        assert_eq!(payload_size(9), 3); // 1 hash + 2 bytes
        assert_eq!(payload_size(15), 9); // 1 hash + 8 bytes
    }

    // -----------------------------------------------------------------------
    // sign_extend
    // -----------------------------------------------------------------------

    #[test]
    fn sign_extend_positive() {
        assert_eq!(sign_extend(&[]), 0);
        assert_eq!(sign_extend(&[0x42]), 0x42);
        assert_eq!(sign_extend(&[0x01, 0x00]), 256);
        assert_eq!(sign_extend(&[0x7F]), 127);
    }

    #[test]
    fn sign_extend_negative() {
        assert_eq!(sign_extend(&[0xFF]), -1);
        assert_eq!(sign_extend(&[0x80]), -128);
        assert_eq!(sign_extend(&[0xFF, 0x00]), -256);
        assert_eq!(sign_extend(&[0xFE]), -2);
    }

    // -----------------------------------------------------------------------
    // encode_signed_bytes
    // -----------------------------------------------------------------------

    #[test]
    fn encode_signed_bytes_cases() {
        assert_eq!(encode_signed_bytes(0), vec![]);
        assert_eq!(encode_signed_bytes(1), vec![0x01]);
        assert_eq!(encode_signed_bytes(127), vec![0x7F]);
        assert_eq!(encode_signed_bytes(128), vec![0x00, 0x80]);
        assert_eq!(encode_signed_bytes(-1), vec![0xFF]);
        assert_eq!(encode_signed_bytes(-128), vec![0x80]);
        assert_eq!(encode_signed_bytes(-129), vec![0xFF, 0x7F]);
        assert_eq!(encode_signed_bytes(256), vec![0x01, 0x00]);
    }

    // -----------------------------------------------------------------------
    // decode_payload
    // -----------------------------------------------------------------------

    #[test]
    fn decode_payload_no_payload() {
        let (hash, idx) = decode_payload(0, &[]).unwrap();
        assert_eq!(hash, None);
        assert_eq!(idx, 0);
    }

    #[test]
    fn decode_payload_without_hash() {
        // pb=2, two bytes: 0x01 0x00 -> 256
        let (hash, idx) = decode_payload(2, &[0x01, 0x00]).unwrap();
        assert_eq!(hash, None);
        assert_eq!(idx, 256);
    }

    #[test]
    fn decode_payload_with_hash() {
        // pb=9 -> 1 hash byte + 2 idx bytes
        let (hash, idx) = decode_payload(9, &[0xAB, 0x01, 0x00]).unwrap();
        assert_eq!(hash, Some(0xAB));
        assert_eq!(idx, 256);
    }

    #[test]
    fn decode_payload_negative_idxpos() {
        // pb=8 -> 1 hash byte + 1 idx byte (0xFF = -1)
        let (hash, idx) = decode_payload(8, &[0x42, 0xFF]).unwrap();
        assert_eq!(hash, Some(0x42));
        assert_eq!(idx, -1);
    }

    // -----------------------------------------------------------------------
    // read_node_header — PayloadOnly
    // -----------------------------------------------------------------------

    #[test]
    fn read_payload_only() {
        let data = [0x00]; // type=0, pb=0
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::PayloadOnly);
        assert_eq!(hdr.payload_bits, 0);
        assert!(hdr.transitions.is_empty());
        assert!(hdr.pointers.is_empty());
        assert_eq!(hdr.payload_position, 1);
    }

    #[test]
    fn read_payload_only_with_payload() {
        let data = [0x02]; // type=0, pb=2
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::PayloadOnly);
        assert_eq!(hdr.payload_bits, 2);
        assert_eq!(hdr.payload_position, 1);
    }

    // -----------------------------------------------------------------------
    // read_node_header — Single types
    // -----------------------------------------------------------------------

    #[test]
    fn read_single_nopayload4() {
        // type=1, pointer=5, transition=0x41
        let data = [0x15, 0x41];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::SingleNopayload4);
        assert_eq!(hdr.payload_bits, 0);
        assert_eq!(hdr.transitions, vec![0x41]);
        assert_eq!(hdr.pointers, vec![5]);
        assert_eq!(hdr.payload_position, 2);
    }

    #[test]
    fn read_single_nopayload12() {
        // type=3, ptr_hi=0x1, ptr_lo=0x23, transition=0x42
        // byte 0: 0x31 (type=3, ptr_hi=1)
        // byte 1: 0x23 (ptr_lo)
        // byte 2: 0x42 (transition)
        let data = [0x31, 0x23, 0x42];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::SingleNopayload12);
        assert_eq!(hdr.payload_bits, 0);
        assert_eq!(hdr.transitions, vec![0x42]);
        assert_eq!(hdr.pointers, vec![0x123]);
        assert_eq!(hdr.payload_position, 3);
    }

    #[test]
    fn read_single8() {
        // type=2, pb=1, transition=0x61, pointer=0x0A
        let data = [0x21, 0x61, 0x0A];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Single8);
        assert_eq!(hdr.payload_bits, 1);
        assert_eq!(hdr.transitions, vec![0x61]);
        assert_eq!(hdr.pointers, vec![10]);
        assert_eq!(hdr.payload_position, 3);
    }

    #[test]
    fn read_single16() {
        // type=4, pb=2, transition=0x62, pointer=0x0102
        let data = [0x42, 0x62, 0x01, 0x02];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Single16);
        assert_eq!(hdr.payload_bits, 2);
        assert_eq!(hdr.transitions, vec![0x62]);
        assert_eq!(hdr.pointers, vec![0x0102]);
        assert_eq!(hdr.payload_position, 4);
    }

    // -----------------------------------------------------------------------
    // read_node_header — Sparse types
    // -----------------------------------------------------------------------

    #[test]
    fn read_sparse8() {
        // type=5, pb=0, cc=2, transitions=[0x41, 0x42], pointers=[0x05, 0x0A]
        let data = [0x50, 0x02, 0x41, 0x42, 0x05, 0x0A];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Sparse8);
        assert_eq!(hdr.payload_bits, 0);
        assert_eq!(hdr.transitions, vec![0x41, 0x42]);
        assert_eq!(hdr.pointers, vec![5, 10]);
        assert_eq!(hdr.payload_position, 6);
    }

    #[test]
    fn read_sparse16() {
        // type=7, pb=1, cc=2, transitions=[0x41, 0x42], pointers=[0x0100, 0x0200]
        let data = [0x71, 0x02, 0x41, 0x42, 0x01, 0x00, 0x02, 0x00];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Sparse16);
        assert_eq!(hdr.payload_bits, 1);
        assert_eq!(hdr.transitions, vec![0x41, 0x42]);
        assert_eq!(hdr.pointers, vec![0x0100, 0x0200]);
        assert_eq!(hdr.payload_position, 8);
    }

    #[test]
    fn read_sparse24() {
        // type=8, pb=0, cc=2, transitions=[0x41, 0x42], pointers=[0x010203, 0x040506]
        let data = [0x80, 0x02, 0x41, 0x42, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Sparse24);
        assert_eq!(hdr.payload_bits, 0);
        assert_eq!(hdr.transitions, vec![0x41, 0x42]);
        assert_eq!(hdr.pointers, vec![0x010203, 0x040506]);
        assert_eq!(hdr.payload_position, 10);
    }

    // -----------------------------------------------------------------------
    // read_node_header — Sparse12
    // -----------------------------------------------------------------------

    #[test]
    fn read_sparse12() {
        // type=6, pb=0, cc=2, transitions=[0x41, 0x42]
        // Two 12-bit pointers: 0x123 and 0x456
        // Packed: byte0 = 0x123 >> 4 = 0x12
        //         byte1 = (0x123 << 4) | (0x456 >> 8) = 0x34
        //         byte2 = 0x456 & 0xFF = 0x56
        let data = [0x60, 0x02, 0x41, 0x42, 0x12, 0x34, 0x56];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Sparse12);
        assert_eq!(hdr.payload_bits, 0);
        assert_eq!(hdr.transitions, vec![0x41, 0x42]);
        assert_eq!(hdr.pointers, vec![0x123, 0x456]);
        // payload_position = 2 + (2*5+1)/2 = 2 + 5 = 7
        assert_eq!(hdr.payload_position, 7);
    }

    #[test]
    fn read_sparse12_odd_count() {
        // type=6, pb=0, cc=3, transitions=[0x41, 0x42, 0x43]
        // Three 12-bit pointers: 0x100, 0x200, 0x300
        // Packed pairs:
        //   pair (0x100, 0x200): byte0=0x10, byte1=0x02, byte2=0x00
        //   odd 0x300: written as short (0x300 << 4) = 0x3000 -> bytes 0x30, 0x00
        let data = [0x60, 0x03, 0x41, 0x42, 0x43, 0x10, 0x02, 0x00, 0x30, 0x00];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Sparse12);
        assert_eq!(hdr.transitions, vec![0x41, 0x42, 0x43]);
        assert_eq!(hdr.pointers, vec![0x100, 0x200, 0x300]);
        // payload_position = 2 + (3*5+1)/2 = 2 + 8 = 10
        assert_eq!(hdr.payload_position, 10);
    }

    // -----------------------------------------------------------------------
    // read_node_header — Sparse40
    // -----------------------------------------------------------------------

    #[test]
    fn read_sparse40() {
        // type=9, pb=0, cc=1, transitions=[0x41], pointers=[0x0102030405]
        let data = [0x90, 0x01, 0x41, 0x01, 0x02, 0x03, 0x04, 0x05];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Sparse40);
        assert_eq!(hdr.payload_bits, 0);
        assert_eq!(hdr.transitions, vec![0x41]);
        assert_eq!(hdr.pointers, vec![0x01_02_03_04_05]);
        assert_eq!(hdr.payload_position, 8);
    }

    // -----------------------------------------------------------------------
    // read_node_header — Dense types
    // -----------------------------------------------------------------------

    #[test]
    fn read_dense16() {
        // type=11 (0xB), pb=0, start=0x41, length-1=1 (2 children: 0x41, 0x42)
        // pointers: [0x0100, 0x0200]
        let data = [0xB0, 0x41, 0x01, 0x01, 0x00, 0x02, 0x00];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Dense16);
        assert_eq!(hdr.payload_bits, 0);
        assert_eq!(hdr.transitions, vec![0x41, 0x42]);
        assert_eq!(hdr.pointers, vec![0x0100, 0x0200]);
        assert_eq!(hdr.payload_position, 7);
    }

    #[test]
    fn read_dense24() {
        // type=12 (0xC), pb=0, start=0x10, length-1=2 (3 children: 0x10, 0x11, 0x12)
        // pointers: [0x000100, 0x000000, 0x000300] (middle one = 0 means no child)
        let data = [
            0xC0, 0x10, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00,
        ];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Dense24);
        assert_eq!(hdr.transitions, vec![0x10, 0x11, 0x12]);
        assert_eq!(hdr.pointers, vec![0x000100, 0x000000, 0x000300]);
        assert_eq!(hdr.payload_position, 12);
    }

    #[test]
    fn read_dense12() {
        // type=10 (0xA), pb=0, start=0x41, length-1=1 (2 children)
        // Two 12-bit pointers: 0x100 and 0x200
        // Packed: byte0 = 0x10, byte1 = 0x02, byte2 = 0x00
        let data = [0xA0, 0x41, 0x01, 0x10, 0x02, 0x00];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Dense12);
        assert_eq!(hdr.transitions, vec![0x41, 0x42]);
        assert_eq!(hdr.pointers, vec![0x100, 0x200]);
        // payload_position = 3 + (2*3+1)/2 = 3 + 3 = 6
        assert_eq!(hdr.payload_position, 6);
    }

    // -----------------------------------------------------------------------
    // node_type_classification
    // -----------------------------------------------------------------------

    #[test]
    fn node_type_classification() {
        assert!(!NodeType::PayloadOnly.is_single());
        assert!(!NodeType::PayloadOnly.is_sparse());
        assert!(!NodeType::PayloadOnly.is_dense());

        assert!(NodeType::SingleNopayload4.is_single());
        assert!(NodeType::Single8.is_single());
        assert!(NodeType::SingleNopayload12.is_single());
        assert!(NodeType::Single16.is_single());

        assert!(NodeType::Sparse8.is_sparse());
        assert!(NodeType::Sparse12.is_sparse());
        assert!(NodeType::Sparse16.is_sparse());
        assert!(NodeType::Sparse24.is_sparse());
        assert!(NodeType::Sparse40.is_sparse());

        assert!(NodeType::Dense12.is_dense());
        assert!(NodeType::Dense16.is_dense());
        assert!(NodeType::Dense24.is_dense());
        assert!(NodeType::Dense32.is_dense());
        assert!(NodeType::Dense40.is_dense());
        assert!(NodeType::LongDense.is_dense());
    }

    // -----------------------------------------------------------------------
    // Dense32, Dense40, LongDense reading
    // -----------------------------------------------------------------------

    #[test]
    fn read_dense32() {
        // type=13 (0xD), pb=0, start=0x00, length-1=0 (1 child: 0x00)
        // pointer: 0x01020304
        let data = [0xD0, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Dense32);
        assert_eq!(hdr.transitions, vec![0x00]);
        assert_eq!(hdr.pointers, vec![0x01020304]);
        assert_eq!(hdr.payload_position, 7);
    }

    #[test]
    fn read_dense40() {
        // type=14 (0xE), pb=0, start=0x00, length-1=0 (1 child)
        // pointer: 0x0102030405
        let data = [0xE0, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::Dense40);
        assert_eq!(hdr.transitions, vec![0x00]);
        assert_eq!(hdr.pointers, vec![0x01_02_03_04_05]);
        assert_eq!(hdr.payload_position, 8);
    }

    #[test]
    fn read_long_dense() {
        // type=15 (0xF), pb=0, start=0x00, length-1=0 (1 child)
        // pointer: 8-byte BE value 0x00_00_00_00_00_00_01_00 = 256
        let data = [
            0xF0, 0x00, 0x00, // header: type, start, len-1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, // 8-byte pointer = 256
        ];
        let hdr = read_node_header(&data).unwrap();
        assert_eq!(hdr.node_type, NodeType::LongDense);
        assert_eq!(hdr.transitions, vec![0x00]);
        assert_eq!(hdr.pointers, vec![256]);
        assert_eq!(hdr.payload_position, 11);
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn read_empty_data() {
        let result = read_node_header(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn read_truncated_single8() {
        let data = [0x21, 0x61]; // missing pointer byte
        let result = read_node_header(&data);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // encode/decode round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn sign_extend_encode_round_trip() {
        for val in [
            0i64,
            1,
            -1,
            127,
            -128,
            128,
            -129,
            256,
            -256,
            32767,
            -32768,
            i64::MAX,
            i64::MIN,
        ] {
            let bytes = encode_signed_bytes(val);
            if val == 0 {
                assert!(bytes.is_empty());
                assert_eq!(sign_extend(&bytes), 0);
            } else {
                let decoded = sign_extend(&bytes);
                assert_eq!(decoded, val, "round-trip failed for {val}");
            }
        }
    }
}
