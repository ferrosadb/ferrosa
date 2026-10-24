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

/// The Bolt major version this server speaks.
pub const BOLT_MAJOR_VERSION: u8 = 5;

/// The highest minor version whose **error** shape this server emits.
///
/// Bolt 5.7 added GQL-status metadata to `FAILURE` (`gql_status`,
/// `description`, `diagnostic_record`) alongside `code`/`message`, and 5.7+
/// drivers prefer it when hydrating an error. This server emits the 5.0–5.6
/// shape, so it must not negotiate a version that promises the rest: a driver
/// that believes it agreed on 5.8 takes its GQL-status path, finds no usable
/// status, and falls back to its own text (`An unknown error occurred` in the
/// Neo4j Python driver) instead of showing the engine's diagnostic.
pub const MAX_SUPPORTED_MINOR: u8 = 6;

/// Encode a concrete `(minor, major)` pair in this module's u32 form, which is
/// also the wire form: `[0x00, 0x00, minor, major]`.
const fn version(minor: u8, major: u8) -> u32 {
    ((minor as u32) << 8) | major as u32
}

/// Parse the client handshake and negotiate a version.
///
/// The handshake must be exactly 20 bytes: 4 magic bytes followed by
/// four 4-byte version proposals in big-endian order.
///
/// Each proposal is `[0x00, range, minor, major]`: it offers `minor` **and the
/// `range` minors below it**. The reply must name ONE version (its range byte
/// is zero) — echoing a proposal's range byte back makes the client read the
/// minor it hoped for and run a protocol version this server does not speak.
///
/// Returns the highest minor within an offer that this server implements, or
/// `None` if the client offered nothing in the supported 5.x range.
pub fn negotiate_version(handshake: &[u8; 20]) -> Option<u32> {
    // Verify magic bytes
    if handshake[0..4] != BOLT_MAGIC {
        return None;
    }

    // Client proposes up to 4 versions, in its own order of preference.
    for i in 0..4 {
        let offset = 4 + i * 4;
        let range = handshake[offset + 1];
        let minor = handshake[offset + 2];
        let major = handshake[offset + 3];
        if major != BOLT_MAJOR_VERSION {
            continue;
        }
        // This proposal covers [minor - range, minor]. Serve the highest minor
        // in that window that we implement.
        let highest = minor.min(MAX_SUPPORTED_MINOR);
        if highest >= minor.saturating_sub(range) {
            return Some(version(highest, major));
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
        // Propose 5.4 exactly: [0x00, range=0, minor=4, major=5].
        let hs = make_handshake([
            0x00_00_04_05, // 5.4
            0x00_00_00_04, // 4.0
            0x00_00_00_00,
            0x00_00_00_00,
        ]);
        let result = negotiate_version(&hs);
        assert_eq!(result, Some(0x00_00_04_05));
    }

    /// A proposal carrying a range (every real driver sends one) must be
    /// answered with ONE concrete version — its range byte zero — never with
    /// the proposal echoed back. Echoing `[0x00, 4, 8, 5]` ("5.4–5.8") tells a
    /// driver it agreed on 5.8, a version whose FAILURE shape this server does
    /// not emit, and the driver then replaces the engine's diagnostic with its
    /// own generic text.
    #[test]
    fn negotiate_version_answers_one_concrete_version_never_a_range() {
        // 5.8 down to 5.4.
        let hs = make_handshake([0x00_04_08_05, 0, 0, 0]);
        let result = negotiate_version(&hs).expect("5.4–5.8 includes a supported minor");
        assert_eq!(
            version_response(result),
            [0, 0, MAX_SUPPORTED_MINOR, BOLT_MAJOR_VERSION],
            "the reply must name one version at the highest minor we implement, \
             got {:?} (range byte must be 0)",
            version_response(result)
        );
        assert_eq!(
            version_response(result)[1],
            0,
            "a reply that reuses the request's range byte is a protocol lie"
        );

        // 5.8 down to 5.0 (the widest offer a driver makes).
        let hs = make_handshake([0x00_08_08_05, 0, 0, 0]);
        assert_eq!(
            negotiate_version(&hs).map(version_response),
            Some([0, 0, MAX_SUPPORTED_MINOR, BOLT_MAJOR_VERSION])
        );

        // An offer that starts *below* the cap keeps its own minor.
        let hs = make_handshake([0x00_02_04_05, 0, 0, 0]); // 5.2–5.4
        assert_eq!(
            negotiate_version(&hs).map(version_response),
            Some([0, 0, 4, 5]),
            "5.2–5.4 must answer 5.4, not the cap"
        );
    }

    /// An offer of only versions 5.7+ cannot be served: this server emits the
    /// 5.0–5.6 error shape, so agreeing to 5.8 would promise metadata it never
    /// sends. Rejecting the proposal is honest; the client then says so.
    #[test]
    fn negotiate_version_rejects_an_offer_above_the_emitted_error_shape() {
        let hs = make_handshake([0x00_00_08_05, 0x00_00_07_05, 0, 0]);
        assert_eq!(
            negotiate_version(&hs),
            None,
            "5.7/5.8-only offers must be refused, not silently downgraded in name only"
        );
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
