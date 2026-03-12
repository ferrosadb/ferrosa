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

// Items are used by the segment module (Task 5) and reader (Task 7); suppress
// dead-code warnings until those modules exist.
#![allow(dead_code)]

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
        assert!(matches!(
            result.unwrap_err(),
            ferrosa_common::Error::ChecksumMismatch { .. }
        ));
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
