//! TOC.txt reader and writer for BTI SSTables.
//!
//! Each SSTable has an associated TOC.txt file that lists one component
//! filename per line. This module parses and writes that file, and provides
//! helpers that return the standard component lists for compressed and
//! uncompressed BTI SSTables.

use ferrosa_common::{Error, Result};

// ---------------------------------------------------------------------------
// Component suffix constants
// ---------------------------------------------------------------------------

/// Data file suffix.
pub const DATA: &str = "Data.db";
/// Partition index (trie) suffix.
pub const PARTITIONS: &str = "Partitions.db";
/// Row index (trie) suffix.
pub const ROWS: &str = "Rows.db";
/// Bloom-filter suffix.
pub const FILTER: &str = "Filter.db";
/// Compression metadata suffix (present when compression is enabled).
pub const COMPRESSION_INFO: &str = "CompressionInfo.db";
/// Statistics / metadata suffix.
pub const STATISTICS: &str = "Statistics.db";
/// Table-of-contents suffix.
pub const TOC: &str = "TOC.txt";
/// CRC suffix (present when compression is disabled).
pub const CRC: &str = "CRC.db";

// ---------------------------------------------------------------------------
// Read / Write
// ---------------------------------------------------------------------------

/// Parse a TOC.txt file, returning one component filename per entry.
///
/// Empty lines are silently skipped. Returns an error if the data is not
/// valid UTF-8.
pub fn read_toc(data: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(data)
        .map_err(|e| Error::InvalidFormat(format!("TOC.txt is not valid UTF-8: {e}")))?;

    Ok(text
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect())
}

/// Write a TOC.txt file from a list of component filenames.
///
/// Each component is written on its own line terminated by `\n`.
pub fn write_toc(components: &[&str]) -> Vec<u8> {
    let mut buf = String::new();
    for c in components {
        buf.push_str(c);
        buf.push('\n');
    }
    buf.into_bytes()
}

// ---------------------------------------------------------------------------
// Standard component lists
// ---------------------------------------------------------------------------

/// All suffixes used in a BTI SSTable.
const COMMON_SUFFIXES: &[&str] = &[DATA, PARTITIONS, ROWS, FILTER, STATISTICS, TOC];

/// Return the 7 standard component filenames for a **compressed** BTI SSTable.
///
/// Includes `CompressionInfo.db`, excludes `CRC.db`.
pub fn standard_compressed_components(prefix: &str) -> Vec<String> {
    let mut out: Vec<String> = COMMON_SUFFIXES
        .iter()
        .map(|s| format!("{prefix}-{s}"))
        .collect();
    out.push(format!("{prefix}-{COMPRESSION_INFO}"));
    out.sort();
    out
}

/// Return the 7 standard component filenames for an **uncompressed** BTI SSTable.
///
/// Includes `CRC.db`, excludes `CompressionInfo.db`.
pub fn standard_uncompressed_components(prefix: &str) -> Vec<String> {
    let mut out: Vec<String> = COMMON_SUFFIXES
        .iter()
        .map(|s| format!("{prefix}-{s}"))
        .collect();
    out.push(format!("{prefix}-{CRC}"));
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_toc_basic() {
        let input = b"nb-1-big-Data.db\nnb-1-big-Filter.db\nnb-1-big-TOC.txt\n";
        let result = read_toc(input).unwrap();
        assert_eq!(
            result,
            vec!["nb-1-big-Data.db", "nb-1-big-Filter.db", "nb-1-big-TOC.txt",]
        );
    }

    #[test]
    fn read_toc_empty_lines() {
        let input = b"\nnb-1-big-Data.db\n\n\nnb-1-big-Filter.db\n\n";
        let result = read_toc(input).unwrap();
        assert_eq!(result, vec!["nb-1-big-Data.db", "nb-1-big-Filter.db"]);
    }

    #[test]
    fn write_toc_round_trip() {
        let components = vec!["nb-1-big-Data.db", "nb-1-big-Filter.db", "nb-1-big-TOC.txt"];
        let bytes = write_toc(&components);
        let parsed = read_toc(&bytes).unwrap();
        assert_eq!(parsed, components);
    }

    #[test]
    fn standard_components_compressed() {
        let comps = standard_compressed_components("nb-1-big");
        assert_eq!(comps.len(), 7);
        assert!(
            comps.iter().any(|c| c.ends_with("CompressionInfo.db")),
            "compressed components must include CompressionInfo.db"
        );
        assert!(
            !comps.iter().any(|c| c.ends_with("CRC.db")),
            "compressed components must not include CRC.db"
        );
    }

    #[test]
    fn standard_components_uncompressed() {
        let comps = standard_uncompressed_components("nb-1-big");
        assert_eq!(comps.len(), 7);
        assert!(
            comps.iter().any(|c| c.ends_with("CRC.db")),
            "uncompressed components must include CRC.db"
        );
        assert!(
            !comps.iter().any(|c| c.ends_with("CompressionInfo.db")),
            "uncompressed components must not include CompressionInfo.db"
        );
    }
}
