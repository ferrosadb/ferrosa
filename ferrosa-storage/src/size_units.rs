//! Byte-size literals for table extensions: `"1048576"`, `"512KiB"`, `"1MiB"`.
//!
//! One parser, so a size means the same thing in `rrd.block_bytes` and in
//! `storage.pin_max_bytes`. Binary suffixes only (`KiB`/`MiB`/`GiB`/`TiB`); a
//! decimal suffix such as `MB` is rejected rather than guessed at, because
//! `1 MB` as 10^6 and as 2^20 differ by 5% on a value an operator chose to
//! bound disk with.

const BINARY_UNITS: [(&str, u64); 4] = [
    ("KiB", 1 << 10),
    ("MiB", 1 << 20),
    ("GiB", 1 << 30),
    ("TiB", 1 << 40),
];

/// Parse a byte size, with an optional binary suffix. A bare number is bytes.
pub fn parse_size(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("empty size string".to_string());
    }

    let (digits, multiplier) = match BINARY_UNITS
        .iter()
        .find(|(unit, _)| trimmed.ends_with(unit))
    {
        Some((unit, multiplier)) => (trimmed[..trimmed.len() - unit.len()].trim(), *multiplier),
        None => match trimmed.strip_suffix('B') {
            Some(digits) => (digits.trim(), 1),
            None => (trimmed, 1),
        },
    };

    let value: u64 = digits
        .parse()
        .map_err(|_| format!("invalid size: '{trimmed}'"))?;
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size out of range: '{trimmed}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_bytes_and_binary_suffixes() {
        assert_eq!(parse_size("0"), Ok(0));
        assert_eq!(parse_size("1048576"), Ok(1_048_576));
        assert_eq!(parse_size(" 512KiB "), Ok(512 * 1024));
        assert_eq!(parse_size("1MiB"), Ok(1024 * 1024));
        assert_eq!(parse_size("64KiB"), Ok(65_536));
        assert_eq!(parse_size("2GiB"), Ok(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1TiB"), Ok(1024 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("8B"), Ok(8));
    }

    #[test]
    fn rejects_decimal_suffixes_and_junk() {
        for raw in ["1MB", "1GB", "64 kilobytes", "MiB", "1.5MiB", "-1KiB", ""] {
            assert!(
                parse_size(raw).is_err(),
                "'{raw}' must fail loud rather than be guessed at"
            );
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_size("99999999999TiB").is_err());
    }
}
