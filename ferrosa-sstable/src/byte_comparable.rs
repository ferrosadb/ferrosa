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
//! assert_eq!(encoded.len(), 15);
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
        return Err(Error::InvalidFormat(
            "expected NEXT_COMPONENT at start".into(),
        ));
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
        return Err(Error::InvalidFormat(
            "expected NEXT_COMPONENT before key".into(),
        ));
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
                        return Err(Error::InvalidFormat(
                            "unterminated null-escape sequence".into(),
                        ));
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
        assert_eq!(
            &encoded[1..9],
            &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        );
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
        assert_eq!(
            decoded.key.as_bytes(),
            &[0x41, 0x00, 0x42, 0x00, 0x00, 0x43]
        );
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
