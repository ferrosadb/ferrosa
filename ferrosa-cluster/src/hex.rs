//! Lowercase hex encoding that fills one buffer instead of one `String` per byte.
//!
//! `format!("{byte:02x}")` allocates a fresh `String` per byte, so a hex encode
//! costs one allocation per byte. This helper reserves the whole output once
//! and pushes table entries straight into it.
//!
//! Lowercase only: every caller emits diagnostic text produced by `{:02x}`, and
//! `{:02X}` would be a different string.

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Appends the lowercase hex form of `bytes` to `out`, reserving once.
fn push_lower(out: &mut String, bytes: &[u8]) {
    out.reserve(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

/// Lowercase hex of the first `max` bytes of `bytes`, in a single allocation.
pub(crate) fn lower_prefix(bytes: &[u8], max: usize) -> String {
    let taken = &bytes[..bytes.len().min(max)];
    let mut out = String::with_capacity(taken.len() * 2);
    push_lower(&mut out, taken);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact strings the previous per-byte `format!` produced.
    #[test]
    fn hex_matches_the_per_byte_format_output() {
        assert_eq!(lower_prefix(&[], 16), "");
        assert_eq!(lower_prefix(&[0x00], 16), "00");
        assert_eq!(lower_prefix(&[0x0f], 16), "0f");
        assert_eq!(lower_prefix(&[0xff], 16), "ff");
        assert_eq!(lower_prefix(b"\xff\xfe\x80\x7f ", 16), "fffe807f20");
        let key: Vec<u8> = (0u8..32).collect();
        assert_eq!(
            lower_prefix(&key, 32),
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
    }

    /// `take(max).map(format!)` is the expression `lower_prefix` replaced: the
    /// oracle covers every length and every truncation point, including `max`
    /// beyond the input and `max == 0`.
    #[test]
    fn lower_prefix_matches_take_and_truncates_in_place() {
        let bytes: Vec<u8> = (0..=255u8).cycle().take(40).collect();
        for max in 0..=bytes.len() + 2 {
            let expected: String = bytes.iter().take(max).map(|b| format!("{b:02x}")).collect();
            assert_eq!(lower_prefix(&bytes, max), expected, "max={max}");
        }
        assert_eq!(lower_prefix(&[0xde, 0xad, 0xbe, 0xef], 2), "dead");
        assert_eq!(lower_prefix(&[0xde, 0xad], 9), "dead");
        assert_eq!(lower_prefix(b"", 4), "");
    }

    /// One allocation for the whole string: the buffer is exactly big enough up
    /// front, so filling it cannot reallocate (one `format!` per byte would be
    /// one allocation per byte instead).
    #[test]
    fn hex_output_is_single_allocation() {
        for len in [0usize, 1, 32, 300] {
            let out = lower_prefix(&vec![0xab; len], len);
            assert_eq!(out.len(), len * 2);
            assert_eq!(out.capacity(), len * 2);
        }
    }

    /// I3: both named per-byte sites encode through this module instead of
    /// `format!`-ing each byte.
    #[test]
    fn named_hex_sites_do_not_format_each_byte() {
        let write_path =
            function_source(include_str!("write_path.rs"), "fn resume_filtered_stream(");
        assert!(
            !write_path.contains("format!(\"{x:02x}\")"),
            "write_path still formats one String per byte"
        );
        assert!(write_path.contains("crate::hex::lower_prefix"));

        let log_store = function_source(include_str!("raft/log_store.rs"), "fn hex_preview(");
        assert!(
            !log_store.contains("format!(\"{b:02x}\")"),
            "log_store still formats one String per byte"
        );
        assert!(log_store.contains("crate::hex::lower_prefix"));
    }

    /// The source of one function, from its signature to its closing brace.
    fn function_source<'a>(src: &'a str, signature: &str) -> &'a str {
        let start = src
            .find(signature)
            .unwrap_or_else(|| panic!("missing {signature}"));
        let rest = &src[start..];
        let open = rest.find('{').expect("function body");
        let mut depth = 0usize;
        for (offset, ch) in rest[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &rest[..open + offset + 1];
                    }
                }
                _ => {}
            }
        }
        panic!("unbalanced body for {signature}");
    }
}
