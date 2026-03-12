//! TOC.txt reader and writer.
//!
//! The TOC file lists all component filenames of an SSTable, one per line.
//! Used to enumerate files for deletion, upload, or verification.

use ferrosa_common::{Error, Result};

/// Known SSTable component suffixes.
pub const COMPONENT_DATA: &str = "Data.db";
pub const COMPONENT_PARTITIONS: &str = "Partitions.db";
pub const COMPONENT_ROWS: &str = "Rows.db";
pub const COMPONENT_FILTER: &str = "Filter.db";
pub const COMPONENT_COMPRESSION: &str = "CompressionInfo.db";
pub const COMPONENT_STATISTICS: &str = "Statistics.db";
pub const COMPONENT_TOC: &str = "TOC.txt";
pub const COMPONENT_CRC: &str = "CRC.db";

/// Parse a TOC file into component names.
pub fn read_toc(data: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(data)
        .map_err(|e| Error::InvalidFormat(format!("TOC not valid UTF-8: {e}")))?;
    Ok(text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .collect())
}

/// Write a TOC file from component names.
pub fn write_toc(components: &[&str]) -> Vec<u8> {
    let mut buf = String::new();
    for name in components {
        buf.push_str(name);
        buf.push('\n');
    }
    buf.into_bytes()
}

/// Return the standard component list for a compressed BTI SSTable.
pub fn standard_compressed_components(prefix: &str) -> Vec<String> {
    [
        COMPONENT_DATA,
        COMPONENT_PARTITIONS,
        COMPONENT_ROWS,
        COMPONENT_FILTER,
        COMPONENT_COMPRESSION,
        COMPONENT_STATISTICS,
        COMPONENT_TOC,
    ]
    .iter()
    .map(|suffix| format!("{prefix}-{suffix}"))
    .collect()
}

/// Return the standard component list for an uncompressed BTI SSTable.
pub fn standard_uncompressed_components(prefix: &str) -> Vec<String> {
    [
        COMPONENT_DATA,
        COMPONENT_PARTITIONS,
        COMPONENT_ROWS,
        COMPONENT_FILTER,
        COMPONENT_CRC,
        COMPONENT_STATISTICS,
        COMPONENT_TOC,
    ]
    .iter()
    .map(|suffix| format!("{prefix}-{suffix}"))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_toc_basic() {
        let data = b"na-1-big-Data.db\nna-1-big-Filter.db\n";
        let components = read_toc(data).unwrap();
        assert_eq!(components, vec!["na-1-big-Data.db", "na-1-big-Filter.db"]);
    }

    #[test]
    fn read_toc_empty_lines() {
        let data = b"na-1-big-Data.db\n\n\nna-1-big-Filter.db\n\n";
        let components = read_toc(data).unwrap();
        assert_eq!(components, vec!["na-1-big-Data.db", "na-1-big-Filter.db"]);
    }

    #[test]
    fn write_toc_round_trip() {
        let original = &["na-1-big-Data.db", "na-1-big-Filter.db", "na-1-big-TOC.txt"];
        let bytes = write_toc(original);
        let parsed = read_toc(&bytes).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn standard_components_compressed() {
        let components = standard_compressed_components("na-1-big");
        assert_eq!(components.len(), 7);
        assert!(components.iter().any(|c| c.ends_with("CompressionInfo.db")));
        assert!(!components.iter().any(|c| c.ends_with("CRC.db")));
    }

    #[test]
    fn standard_components_uncompressed() {
        let components = standard_uncompressed_components("na-1-big");
        assert_eq!(components.len(), 7);
        assert!(components.iter().any(|c| c.ends_with("CRC.db")));
        assert!(!components.iter().any(|c| c.ends_with("CompressionInfo.db")));
    }
}
