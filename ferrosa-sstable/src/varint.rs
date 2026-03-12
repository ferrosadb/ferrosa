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

    for &byte in &buf[1..total] {
        value = (value << 8) | byte as u64;
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
            0u64,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384, // 2-3 byte boundary
            2097151,
            2097152, // 3-4 byte boundary
            268435455,
            268435456, // 4-5 byte boundary
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
            (0, 0),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (i64::MAX, u64::MAX - 1),
            (i64::MIN, u64::MAX),
        ];
        for &(signed, expected_zigzag) in cases {
            let zigzag = ((signed << 1) ^ (signed >> 63)) as u64;
            assert_eq!(zigzag, expected_zigzag, "zigzag({signed})");
        }
    }

    #[test]
    fn signed_round_trip_boundary_values() {
        let values = [
            0i64,
            1,
            -1,
            63,
            -64,
            64,
            -65,
            i64::MAX,
            i64::MIN,
            i64::MIN + 1,
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
