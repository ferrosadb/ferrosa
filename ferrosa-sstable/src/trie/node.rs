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
            NodeType::Sparse12 => 2 + (cc * 5).div_ceil(2),
            NodeType::Sparse16 => 2 + cc * 3,
            NodeType::Sparse24 => 2 + cc * 4,
            NodeType::Sparse40 => 2 + cc * 6,
            NodeType::Dense12 => 3 + (cs * 3).div_ceil(2),
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
///
/// Partition index payload:
/// - If `pb` == 0: no payload
/// - If `pb` < 8: `idxpos` is `pb`-byte sign-extended integer
/// - If `pb` >= 8: hash byte + `(pb-7)`-byte integer
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
pub(crate) fn sign_extend(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }
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
            let ptr = (data[0] & 0x0F) as u64;
            let trans = data[1];
            // pb bits are repurposed as pointer bits — no payload
            return Ok(NodeHeader {
                node_type,
                pb: 0,
                transitions: vec![trans],
                child_pointers: vec![ptr],
                payload_offset: 0,
                total_size: 2,
            });
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
            let ptr = (((data[0] & 0x0F) as u64) << 8) | data[1] as u64;
            let trans = data[2];
            // pb bits are repurposed as upper pointer bits
            return Ok(NodeHeader {
                node_type,
                pb: 0,
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

        NodeType::Sparse12 => {
            if data.len() < 2 {
                return Err(Error::InvalidData("truncated Sparse12".into()));
            }
            let cc = data[1] as usize;
            let ptr_bytes = (cc * 3).div_ceil(2);
            let needed = 2 + cc + ptr_bytes;
            if data.len() < needed {
                return Err(Error::InvalidData("truncated Sparse12 children".into()));
            }
            let trans: Vec<u8> = data[2..2 + cc].to_vec();
            let ptr_start = 2 + cc;
            let mut ptrs = Vec::with_capacity(cc);
            for i in 0..cc {
                // 12-bit pointers packed: pairs share a byte for upper nibbles
                let bit_offset = i * 12;
                let byte_offset = bit_offset / 8;
                let bit_within = bit_offset % 8;
                let p = if bit_within == 0 {
                    // Starts at byte boundary: upper 8 bits from byte, lower 4 from next
                    ((data[ptr_start + byte_offset] as u64) << 4)
                        | ((data[ptr_start + byte_offset + 1] as u64) >> 4)
                } else {
                    // Starts at nibble: lower 4 from byte, upper 8 from next
                    (((data[ptr_start + byte_offset] & 0x0F) as u64) << 8)
                        | (data[ptr_start + byte_offset + 1] as u64)
                };
                ptrs.push(p);
            }
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
                    ((data[off] as u64) << 16)
                        | ((data[off + 1] as u64) << 8)
                        | data[off + 2] as u64
                })
                .collect();
            (trans, ptrs, needed)
        }

        NodeType::Sparse40 => {
            if data.len() < 2 {
                return Err(Error::InvalidData("truncated Sparse40".into()));
            }
            let cc = data[1] as usize;
            let needed = 2 + cc * 6;
            if data.len() < needed {
                return Err(Error::InvalidData("truncated Sparse40 children".into()));
            }
            let trans: Vec<u8> = data[2..2 + cc].to_vec();
            let ptrs: Vec<u64> = (0..cc)
                .map(|i| {
                    let off = 2 + cc + i * 5;
                    ((data[off] as u64) << 32)
                        | ((data[off + 1] as u64) << 24)
                        | ((data[off + 2] as u64) << 16)
                        | ((data[off + 3] as u64) << 8)
                        | data[off + 4] as u64
                })
                .collect();
            (trans, ptrs, needed)
        }

        NodeType::Dense12
        | NodeType::Dense16
        | NodeType::Dense24
        | NodeType::Dense32
        | NodeType::Dense40
        | NodeType::LongDense => {
            if data.len() < 3 {
                return Err(Error::InvalidData("truncated Dense node".into()));
            }
            let min_trans = data[1];
            let max_trans = data[2];
            let cs = (max_trans as usize) - (min_trans as usize) + 1;

            let (ptr_width, needed) = match node_type {
                NodeType::Dense12 => (0, 3 + (cs * 3).div_ceil(2)), // 12-bit packed
                NodeType::Dense16 => (2, 3 + cs * 2),
                NodeType::Dense24 => (3, 3 + cs * 3),
                NodeType::Dense32 => (4, 3 + cs * 4),
                NodeType::Dense40 => (5, 3 + cs * 5),
                NodeType::LongDense => (8, 3 + cs * 8),
                _ => unreachable!(),
            };

            if data.len() < needed {
                return Err(Error::InvalidData("truncated Dense children".into()));
            }

            let transitions: Vec<u8> = (min_trans..=max_trans).collect();
            let ptr_start = 3;
            let mut ptrs = Vec::with_capacity(cs);

            if node_type == NodeType::Dense12 {
                // 12-bit packed pointers (same packing as Sparse12)
                for i in 0..cs {
                    let bit_offset = i * 12;
                    let byte_offset = bit_offset / 8;
                    let bit_within = bit_offset % 8;
                    let p = if bit_within == 0 {
                        ((data[ptr_start + byte_offset] as u64) << 4)
                            | ((data[ptr_start + byte_offset + 1] as u64) >> 4)
                    } else {
                        (((data[ptr_start + byte_offset] & 0x0F) as u64) << 8)
                            | (data[ptr_start + byte_offset + 1] as u64)
                    };
                    ptrs.push(p);
                }
            } else {
                for i in 0..cs {
                    let off = ptr_start + i * ptr_width;
                    let p = match ptr_width {
                        2 => u16::from_be_bytes([data[off], data[off + 1]]) as u64,
                        3 => {
                            ((data[off] as u64) << 16)
                                | ((data[off + 1] as u64) << 8)
                                | data[off + 2] as u64
                        }
                        4 => u32::from_be_bytes([
                            data[off],
                            data[off + 1],
                            data[off + 2],
                            data[off + 3],
                        ]) as u64,
                        5 => {
                            ((data[off] as u64) << 32)
                                | ((data[off + 1] as u64) << 24)
                                | ((data[off + 2] as u64) << 16)
                                | ((data[off + 3] as u64) << 8)
                                | data[off + 4] as u64
                        }
                        8 => u64::from_be_bytes([
                            data[off],
                            data[off + 1],
                            data[off + 2],
                            data[off + 3],
                            data[off + 4],
                            data[off + 5],
                            data[off + 6],
                            data[off + 7],
                        ]),
                        _ => unreachable!(),
                    };
                    ptrs.push(p);
                }
            }

            (transitions, ptrs, needed)
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
        assert_eq!(NodeType::Sparse12.node_size(1, 1), 2 + 5_usize.div_ceil(2));
        assert_eq!(NodeType::Sparse12.node_size(2, 2), 2 + 10_usize.div_ceil(2));
    }

    #[test]
    fn payload_size_values() {
        assert_eq!(payload_size(0), 0);
        assert_eq!(payload_size(1), 1);
        assert_eq!(payload_size(7), 7);
        assert_eq!(payload_size(8), 2); // 1 hash + 1 idxpos
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
        let (hash, idxpos) = decode_payload(2, &[0x01, 0x00]).unwrap();
        assert!(hash.is_none());
        assert_eq!(idxpos, 256);
    }

    #[test]
    fn decode_payload_with_hash() {
        let (hash, idxpos) = decode_payload(9, &[0xAB, 0x01, 0x00]).unwrap();
        assert_eq!(hash, Some(0xAB));
        assert_eq!(idxpos, 256);
    }

    #[test]
    fn decode_payload_negative_idxpos() {
        let (hash, idxpos) = decode_payload(8, &[0xAB, 0xFF]).unwrap();
        assert_eq!(hash, Some(0xAB));
        assert_eq!(idxpos, -1);
    }

    #[test]
    fn read_payload_only_node() {
        let data = [0x00];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::PayloadOnly);
        assert_eq!(header.pb, 0);
        assert!(header.transitions.is_empty());
        assert_eq!(header.total_size, 1);
    }

    #[test]
    fn read_payload_only_with_payload() {
        let data = [0x09, 0xAB, 0x01, 0x00];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::PayloadOnly);
        assert_eq!(header.pb, 9);
        assert_eq!(header.total_size, 4);
    }

    #[test]
    fn read_single8_node() {
        let data = [0x20, b'A', 0x05];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Single8);
        assert_eq!(header.transitions, vec![b'A']);
        assert_eq!(header.child_pointers, vec![5]);
        assert_eq!(header.total_size, 3);
    }

    #[test]
    fn read_single_nopayload12_node() {
        // Type 0x3, pb bits repurposed as upper pointer bits
        // ptr = 0x105 (261): upper nibble = 1 in type byte, lower byte = 0x05 at data[1]
        // trans = 'X' at data[2]
        let data = [0x31, 0x05, b'X']; // type=3, pb_bits=1, lower=0x05, trans='X'
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::SingleNopayload12);
        assert_eq!(header.pb, 0); // Nopayload
        assert_eq!(header.transitions, vec![b'X']);
        assert_eq!(header.child_pointers, vec![0x105]);
    }

    #[test]
    fn read_single16_node() {
        let data = [0x40, b'Z', 0x01, 0x00];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Single16);
        assert_eq!(header.transitions, vec![b'Z']);
        assert_eq!(header.child_pointers, vec![256]);
        assert_eq!(header.total_size, 4);
    }

    #[test]
    fn read_sparse8_node() {
        // Sparse8: type=5, pb=0, cc=2, trans=['a','b'], ptrs=[10, 20]
        let data = [0x50, 0x02, b'a', b'b', 10, 20];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Sparse8);
        assert_eq!(header.transitions, vec![b'a', b'b']);
        assert_eq!(header.child_pointers, vec![10, 20]);
        assert_eq!(header.total_size, 6);
    }

    #[test]
    fn read_sparse16_node() {
        // Sparse16: type=7, pb=0, cc=2, trans=['x','y'], ptrs=[0x0100, 0x0200]
        let data = [0x70, 0x02, b'x', b'y', 0x01, 0x00, 0x02, 0x00];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Sparse16);
        assert_eq!(header.transitions, vec![b'x', b'y']);
        assert_eq!(header.child_pointers, vec![256, 512]);
    }

    #[test]
    fn read_sparse24_node() {
        // Sparse24: type=8, pb=0, cc=1, trans=['a'], ptr=[0x010203]
        let data = [0x80, 0x01, b'a', 0x01, 0x02, 0x03];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Sparse24);
        assert_eq!(header.transitions, vec![b'a']);
        assert_eq!(header.child_pointers, vec![0x010203]);
    }

    #[test]
    fn read_sparse40_node() {
        // Sparse40: type=9, pb=0, cc=1, trans=['a'], ptr=[0x0102030405]
        let data = [0x90, 0x01, b'a', 0x01, 0x02, 0x03, 0x04, 0x05];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Sparse40);
        assert_eq!(header.transitions, vec![b'a']);
        assert_eq!(header.child_pointers, vec![0x01_02_03_04_05]);
    }

    #[test]
    fn read_dense16_node() {
        // Dense16: type=0xB, pb=0, min='a'(97), max='c'(99), 3 children
        // ptrs: [0x0010, 0x0020, 0x0030]
        let data = [0xB0, b'a', b'c', 0x00, 0x10, 0x00, 0x20, 0x00, 0x30];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Dense16);
        assert_eq!(header.transitions, vec![b'a', b'b', b'c']);
        assert_eq!(header.child_pointers, vec![0x10, 0x20, 0x30]);
    }

    #[test]
    fn read_dense24_node() {
        // Dense24: type=0xC, pb=0, min='x'(120), max='y'(121), 2 children
        let data = [0xC0, b'x', b'y', 0x01, 0x00, 0x00, 0x02, 0x00, 0x00];
        let header = read_node_header(&data).unwrap();
        assert_eq!(header.node_type, NodeType::Dense24);
        assert_eq!(header.transitions, vec![b'x', b'y']);
        assert_eq!(header.child_pointers, vec![0x010000, 0x020000]);
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
