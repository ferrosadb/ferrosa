//! Bolt v5 version negotiation handshake.
//!
//! A Bolt connection begins with a 20-byte handshake from the client:
//! 4 magic bytes (`0x6060B017`) followed by four 4-byte version proposals
//! in big-endian order. The server selects the highest compatible version
//! and responds with 4 bytes. A zero response rejects all proposals.

/// Bolt magic preamble: `0x60 0x60 0xB0 0x17` ("GoGoBolt").
pub const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Bolt protocol version 5.0 as a 4-byte big-endian value.
///
/// The version is encoded as `[0x00, minor, 0x00, major]` in the Bolt
/// spec's wire format, but we store it as the raw u32 for comparison:
/// major in the lowest byte.
pub const BOLT_VERSION_5_0: u32 = 0x00_00_00_05;

/// Parse the client handshake and negotiate a version.
///
/// The handshake must be exactly 20 bytes: 4 magic bytes followed by
/// four 4-byte version proposals in big-endian order. The server
/// accepts any proposal with major version 5.
///
/// Returns the selected version, or `None` if no compatible version
/// was proposed.
pub fn negotiate_version(handshake: &[u8; 20]) -> Option<u32> {
    // Verify magic bytes
    if handshake[0..4] != BOLT_MAGIC {
        return None;
    }

    // Client proposes up to 4 versions (4 bytes each, big-endian).
    // Each version is encoded as: [range, minor, range, major].
    // We accept any proposal where the major version (lowest byte) is 5.
    for i in 0..4 {
        let offset = 4 + i * 4;
        let version = u32::from_be_bytes([
            handshake[offset],
            handshake[offset + 1],
            handshake[offset + 2],
            handshake[offset + 3],
        ]);
        let major = version & 0xFF;
        if major == 5 {
            return Some(version);
        }
    }

    None
}

/// Build the 4-byte server response for an accepted version.
pub fn version_response(version: u32) -> [u8; 4] {
    version.to_be_bytes()
}

/// Build the 4-byte rejection response (version 0 — no compatible version).
pub fn rejection_response() -> [u8; 4] {
    [0, 0, 0, 0]
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a 20-byte handshake with magic + 4 version slots.
    fn make_handshake(versions: [u32; 4]) -> [u8; 20] {
        let mut hs = [0u8; 20];
        hs[0..4].copy_from_slice(&BOLT_MAGIC);
        for (i, v) in versions.iter().enumerate() {
            let bytes = v.to_be_bytes();
            hs[4 + i * 4..4 + i * 4 + 4].copy_from_slice(&bytes);
        }
        hs
    }

    #[test]
    fn negotiate_version_5() {
        let hs = make_handshake([
            0x00_00_00_05, // 5.0
            0x00_00_00_04, // 4.0
            0x00_00_00_03, // 3.0
            0x00_00_00_00, // unused
        ]);
        let result = negotiate_version(&hs);
        assert_eq!(result, Some(0x00_00_00_05));
    }

    #[test]
    fn negotiate_version_5_with_minor() {
        // Propose 5.4 (minor = 4)
        let hs = make_handshake([
            0x00_04_00_05, // 5.4
            0x00_00_00_04, // 4.0
            0x00_00_00_00,
            0x00_00_00_00,
        ]);
        let result = negotiate_version(&hs);
        assert_eq!(result, Some(0x00_04_00_05));
    }

    #[test]
    fn negotiate_version_5_not_first() {
        let hs = make_handshake([
            0x00_00_00_04, // 4.0
            0x00_00_00_05, // 5.0 — second slot
            0x00_00_00_00,
            0x00_00_00_00,
        ]);
        let result = negotiate_version(&hs);
        assert_eq!(result, Some(0x00_00_00_05));
    }

    #[test]
    fn negotiate_version_mismatch() {
        let hs = make_handshake([
            0x00_00_00_04, // 4.0
            0x00_00_00_03, // 3.0
            0x00_00_00_02, // 2.0
            0x00_00_00_00,
        ]);
        let result = negotiate_version(&hs);
        assert_eq!(result, None);
    }

    #[test]
    fn negotiate_bad_magic() {
        let mut hs = make_handshake([0x00_00_00_05, 0, 0, 0]);
        hs[0] = 0xFF; // corrupt magic
        let result = negotiate_version(&hs);
        assert_eq!(result, None);
    }

    #[test]
    fn version_response_bytes() {
        let resp = version_response(BOLT_VERSION_5_0);
        assert_eq!(resp, [0x00, 0x00, 0x00, 0x05]);
    }

    #[test]
    fn rejection_response_is_zero() {
        assert_eq!(rejection_response(), [0, 0, 0, 0]);
    }
}
