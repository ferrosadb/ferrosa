//! Lowercase hex encoding that fills one buffer instead of one `String` per byte.
//!
//! `format!("{byte:02x}")` allocates a fresh `String` per byte, so a hex encode
//! costs one allocation per byte. This helper reserves the whole output once
//! and pushes table entries straight into it.
//!
//! Lowercase only: the wire format on this path is the `0x…` JSON blob, which is
//! lowercase hex — `{:02X}` would be a different wire format.

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Appends the lowercase hex form of `bytes` to `out`, reserving once.
pub(crate) fn push_lower(out: &mut String, bytes: &[u8]) {
    out.reserve(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-side convenience: the whole encoding in one allocation.
    fn lower(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        push_lower(&mut out, bytes);
        out
    }

    /// The exact strings the previous per-byte `format!` produced.
    #[test]
    fn hex_matches_the_per_byte_format_output() {
        assert_eq!(lower(&[]), "");
        assert_eq!(lower(&[0x00]), "00");
        assert_eq!(lower(&[0x0f]), "0f");
        assert_eq!(lower(&[0xff]), "ff");
        assert_eq!(lower(b"\xff\xfe\x80\x7f "), "fffe807f20");
        let key: Vec<u8> = (0u8..32).collect();
        assert_eq!(
            lower(&key),
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
    }

    /// Differential oracle: the replaced expression, kept as the reference.
    #[test]
    fn hex_matches_the_old_expression_for_every_length() {
        let bytes: Vec<u8> = (0..=255u8).cycle().take(300).collect();
        for len in 0..=bytes.len() {
            let expected: String = bytes[..len]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(lower(&bytes[..len]), expected, "len={len}");
        }
    }

    /// One allocation for the whole string: the buffer is exactly big enough up
    /// front, so filling it cannot reallocate (one `format!` per byte would be
    /// one allocation per byte instead).
    #[test]
    fn hex_output_is_single_allocation() {
        for len in [0usize, 1, 32, 300] {
            let bytes = vec![0xab; len];
            let out = lower(&bytes);
            assert_eq!(out.len(), len * 2);
            assert_eq!(out.capacity(), len * 2);
        }
    }

    #[test]
    fn push_lower_appends_without_rewriting_the_prefix() {
        let mut out = String::from("0x");
        push_lower(&mut out, &[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(out, "0xdeadbeef");
    }

    /// I3: the named per-byte site encodes through this module rather than
    /// `format!`-ing each byte of the blob.
    #[test]
    fn blob_json_does_not_format_each_byte() {
        let src = include_str!("bridge.rs");
        let production = src.split("#[cfg(test)]").next().expect("production part");
        assert!(
            !production.contains("map(|byte| format!"),
            "blob JSON still builds one String per byte"
        );
        assert!(production.contains("crate::hex::push_lower"));
    }
}
