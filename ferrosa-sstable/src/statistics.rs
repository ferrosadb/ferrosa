//! Statistics.db reader and writer.
//!
//! Contains four metadata components, each CRC32-checksummed:
//! 0: ValidationMetadata (partitioner, bloom FP rate)
//! 1: CompactionMetadata (HyperLogLogPlus cardinality)
//! 2: StatsMetadata (timestamps, sizes, histograms)
//! 3: SerializationHeader (column definitions, delta-encoding minimums)
//!
//! # File Format
//!
//! ```text
//! [component_count: u32]
//! For each component:
//!   [ordinal: u32] [data_length: u32] [data: data_length bytes] [crc: u32]
//! ```
//!
//! Reference: `org.apache.cassandra.io.sstable.metadata.MetadataSerializer`

use ferrosa_common::{Error, Result};

use crate::varint;

/// Parsed Statistics.db file.
#[derive(Debug, Clone)]
pub struct Statistics {
    /// Validation metadata (partitioner + bloom filter settings).
    pub validation: ValidationMetadata,
    /// Opaque CompactionMetadata (HyperLogLogPlus cardinality estimator).
    pub compaction: Vec<u8>,
    /// Opaque StatsMetadata (timestamps, sizes, histograms).
    pub stats: Vec<u8>,
    /// Serialization header required for decoding Data.db.
    pub header: SerializationHeader,
}

/// Validation metadata (partitioner + bloom filter settings).
#[derive(Debug, Clone)]
pub struct ValidationMetadata {
    /// Fully-qualified partitioner class name.
    pub partitioner: String,
    /// Bloom filter false-positive chance.
    pub bloom_fp_chance: f64,
}

/// Serialization header from Statistics.db.
///
/// Required for decoding Data.db — provides the delta-encoding minimums
/// and column type definitions.
#[derive(Debug, Clone)]
pub struct SerializationHeader {
    /// Minimum timestamp (varint-encoded in file).
    pub min_timestamp: i64,
    /// Minimum local deletion time (varint-encoded in file).
    pub min_local_deletion_time: i32,
    /// Minimum TTL (varint-encoded in file).
    pub min_ttl: i32,
    /// Key type as a Cassandra type string (e.g. "org.apache.cassandra.db.marshal.UTF8Type").
    pub key_type: String,
    /// Clustering column types.
    pub clustering_types: Vec<String>,
    /// Static columns as (name_bytes, type_string) pairs.
    pub static_columns: Vec<(Vec<u8>, String)>,
    /// Regular columns as (name_bytes, type_string) pairs.
    pub regular_columns: Vec<(Vec<u8>, String)>,
}

/// Component ordinals in Statistics.db.
const VALIDATION_ORDINAL: u32 = 0;
const COMPACTION_ORDINAL: u32 = 1;
const STATS_ORDINAL: u32 = 2;
const HEADER_ORDINAL: u32 = 3;

// ---------------------------------------------------------------------------
// Top-level read/write
// ---------------------------------------------------------------------------

/// Read a Statistics.db file from raw bytes.
pub fn read_statistics(data: &[u8]) -> Result<Statistics> {
    let mut pos = 0;

    let component_count = read_u32(data, &mut pos)? as usize;
    if component_count != 4 {
        return Err(Error::InvalidFormat(format!(
            "expected 4 statistics components, got {component_count}"
        )));
    }

    let mut validation: Option<ValidationMetadata> = None;
    let mut compaction: Option<Vec<u8>> = None;
    let mut stats: Option<Vec<u8>> = None;
    let mut header: Option<SerializationHeader> = None;

    for _ in 0..component_count {
        let ordinal = read_u32(data, &mut pos)?;
        let data_length = read_u32(data, &mut pos)? as usize;

        if pos + data_length + 4 > data.len() {
            return Err(Error::InvalidData(format!(
                "component {ordinal}: need {} bytes at offset {pos}, only {} available",
                data_length + 4,
                data.len() - pos
            )));
        }

        let component_data = &data[pos..pos + data_length];
        pos += data_length;

        let stored_crc = read_u32(data, &mut pos)?;
        let computed_crc = crc32fast::hash(component_data);
        if stored_crc != computed_crc {
            return Err(Error::ChecksumMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        match ordinal {
            VALIDATION_ORDINAL => {
                validation = Some(read_validation_metadata(component_data)?);
            }
            COMPACTION_ORDINAL => {
                compaction = Some(component_data.to_vec());
            }
            STATS_ORDINAL => {
                stats = Some(component_data.to_vec());
            }
            HEADER_ORDINAL => {
                header = Some(read_serialization_header(component_data)?);
            }
            _ => {
                // Unknown component — skip silently for forward compatibility.
            }
        }
    }

    Ok(Statistics {
        validation: validation
            .ok_or_else(|| Error::InvalidFormat("missing ValidationMetadata".into()))?,
        compaction: compaction
            .ok_or_else(|| Error::InvalidFormat("missing CompactionMetadata".into()))?,
        stats: stats.ok_or_else(|| Error::InvalidFormat("missing StatsMetadata".into()))?,
        header: header.ok_or_else(|| Error::InvalidFormat("missing SerializationHeader".into()))?,
    })
}

/// Write a Statistics.db file to bytes.
pub fn write_statistics(stats: &Statistics) -> Vec<u8> {
    let mut out = Vec::new();

    // Component count
    out.extend_from_slice(&4u32.to_be_bytes());

    // Component 0: ValidationMetadata
    let validation_data = write_validation_metadata(&stats.validation);
    write_component(&mut out, VALIDATION_ORDINAL, &validation_data);

    // Component 1: CompactionMetadata (opaque)
    write_component(&mut out, COMPACTION_ORDINAL, &stats.compaction);

    // Component 2: StatsMetadata (opaque)
    write_component(&mut out, STATS_ORDINAL, &stats.stats);

    // Component 3: SerializationHeader
    let header_data = write_serialization_header(&stats.header);
    write_component(&mut out, HEADER_ORDINAL, &header_data);

    out
}

// ---------------------------------------------------------------------------
// Component framing helpers
// ---------------------------------------------------------------------------

/// Write a single component: ordinal, data length, data, CRC32.
fn write_component(out: &mut Vec<u8>, ordinal: u32, data: &[u8]) {
    out.extend_from_slice(&ordinal.to_be_bytes());
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(data);
    let crc = crc32fast::hash(data);
    out.extend_from_slice(&crc.to_be_bytes());
}

// ---------------------------------------------------------------------------
// ValidationMetadata
// ---------------------------------------------------------------------------

fn read_validation_metadata(data: &[u8]) -> Result<ValidationMetadata> {
    let mut pos = 0;
    let partitioner = read_u16_string(data, &mut pos)?;

    if pos + 8 > data.len() {
        return Err(Error::InvalidData(
            "ValidationMetadata: truncated bloom_fp_chance".into(),
        ));
    }
    let bloom_fp_chance =
        f64::from_be_bytes(data[pos..pos + 8].try_into().expect("8 bytes for f64"));
    // pos += 8; -- not needed, last field

    Ok(ValidationMetadata {
        partitioner,
        bloom_fp_chance,
    })
}

fn write_validation_metadata(v: &ValidationMetadata) -> Vec<u8> {
    let mut out = Vec::new();
    write_u16_string(&mut out, &v.partitioner);
    out.extend_from_slice(&v.bloom_fp_chance.to_be_bytes());
    out
}

// ---------------------------------------------------------------------------
// SerializationHeader
// ---------------------------------------------------------------------------

/// Read a SerializationHeader from raw component bytes.
pub fn read_serialization_header(data: &[u8]) -> Result<SerializationHeader> {
    let mut pos = 0;

    let min_timestamp = read_vint_i64(data, &mut pos)?;
    let min_local_deletion_time = read_vint_i32(data, &mut pos)?;
    let min_ttl = read_vint_i32(data, &mut pos)?;

    let key_type = read_u16_string(data, &mut pos)?;

    let clustering_count = read_u16(data, &mut pos)? as usize;
    let mut clustering_types = Vec::with_capacity(clustering_count);
    for _ in 0..clustering_count {
        clustering_types.push(read_u16_string(data, &mut pos)?);
    }

    let static_count = read_u16(data, &mut pos)? as usize;
    let mut static_columns = Vec::with_capacity(static_count);
    for _ in 0..static_count {
        let name = read_u16_bytes(data, &mut pos)?;
        let type_str = read_u16_string(data, &mut pos)?;
        static_columns.push((name, type_str));
    }

    let regular_count = read_u16(data, &mut pos)? as usize;
    let mut regular_columns = Vec::with_capacity(regular_count);
    for _ in 0..regular_count {
        let name = read_u16_bytes(data, &mut pos)?;
        let type_str = read_u16_string(data, &mut pos)?;
        regular_columns.push((name, type_str));
    }

    Ok(SerializationHeader {
        min_timestamp,
        min_local_deletion_time,
        min_ttl,
        key_type,
        clustering_types,
        static_columns,
        regular_columns,
    })
}

/// Write a SerializationHeader to bytes.
pub fn write_serialization_header(header: &SerializationHeader) -> Vec<u8> {
    let mut out = Vec::new();

    write_vint_i64(&mut out, header.min_timestamp);
    write_vint_i32(&mut out, header.min_local_deletion_time);
    write_vint_i32(&mut out, header.min_ttl);

    write_u16_string(&mut out, &header.key_type);

    out.extend_from_slice(&(header.clustering_types.len() as u16).to_be_bytes());
    for ct in &header.clustering_types {
        write_u16_string(&mut out, ct);
    }

    out.extend_from_slice(&(header.static_columns.len() as u16).to_be_bytes());
    for (name, type_str) in &header.static_columns {
        write_u16_bytes(&mut out, name);
        write_u16_string(&mut out, type_str);
    }

    out.extend_from_slice(&(header.regular_columns.len() as u16).to_be_bytes());
    for (name, type_str) in &header.regular_columns {
        write_u16_bytes(&mut out, name);
        write_u16_string(&mut out, type_str);
    }

    out
}

// ---------------------------------------------------------------------------
// Primitive read/write helpers
// ---------------------------------------------------------------------------

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > data.len() {
        return Err(Error::InvalidData(format!(
            "need 4 bytes at offset {}, only {} available",
            *pos,
            data.len() - *pos
        )));
    }
    let val = u32::from_be_bytes(data[*pos..*pos + 4].try_into().expect("4 bytes for u32"));
    *pos += 4;
    Ok(val)
}

fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16> {
    if *pos + 2 > data.len() {
        return Err(Error::InvalidData(format!(
            "need 2 bytes at offset {}, only {} available",
            *pos,
            data.len() - *pos
        )));
    }
    let val = u16::from_be_bytes(data[*pos..*pos + 2].try_into().expect("2 bytes for u16"));
    *pos += 2;
    Ok(val)
}

fn read_u16_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let len = read_u16(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(Error::InvalidData(format!(
            "string: need {len} bytes at offset {}, only {} available",
            *pos,
            data.len() - *pos
        )));
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| Error::InvalidData(format!("invalid UTF-8: {e}")))?;
    *pos += len;
    Ok(s.to_owned())
}

fn read_u16_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>> {
    let len = read_u16(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(Error::InvalidData(format!(
            "bytes: need {len} bytes at offset {}, only {} available",
            *pos,
            data.len() - *pos
        )));
    }
    let bytes = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(bytes)
}

fn write_u16_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn write_u16_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Read a varint-encoded i64 (signed zigzag).
fn read_vint_i64(data: &[u8], pos: &mut usize) -> Result<i64> {
    let (val, consumed) = varint::read_signed_vint(&data[*pos..])?;
    *pos += consumed;
    Ok(val)
}

/// Read a varint-encoded i32 (signed zigzag, read as i64 then narrowed).
fn read_vint_i32(data: &[u8], pos: &mut usize) -> Result<i32> {
    let val = read_vint_i64(data, pos)?;
    i32::try_from(val)
        .map_err(|_| Error::InvalidData(format!("varint value {val} does not fit in i32")))
}

/// Write a signed varint (zigzag) for an i64.
fn write_vint_i64(out: &mut Vec<u8>, value: i64) {
    let mut buf = [0u8; 9];
    let n = varint::write_signed_vint(&mut buf, value);
    out.extend_from_slice(&buf[..n]);
}

/// Write a signed varint (zigzag) for an i32 (widened to i64).
fn write_vint_i32(out: &mut Vec<u8>, value: i32) {
    write_vint_i64(out, value as i64);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_validation() -> ValidationMetadata {
        ValidationMetadata {
            partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
            bloom_fp_chance: 0.01,
        }
    }

    fn sample_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 1_700_000_000_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
            static_columns: vec![(
                b"s1".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            )],
            regular_columns: vec![
                (
                    b"value".to_vec(),
                    "org.apache.cassandra.db.marshal.BytesType".to_string(),
                ),
                (
                    b"ts".to_vec(),
                    "org.apache.cassandra.db.marshal.TimestampType".to_string(),
                ),
            ],
        }
    }

    fn sample_statistics() -> Statistics {
        Statistics {
            validation: sample_validation(),
            compaction: vec![0xCA, 0xFE, 0xBA, 0xBE],
            stats: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02],
            header: sample_header(),
        }
    }

    #[test]
    fn validation_metadata_round_trip() {
        let v = sample_validation();
        let encoded = write_validation_metadata(&v);
        let decoded = read_validation_metadata(&encoded).unwrap();

        assert_eq!(decoded.partitioner, v.partitioner);
        assert!((decoded.bloom_fp_chance - v.bloom_fp_chance).abs() < f64::EPSILON);
    }

    #[test]
    fn serialization_header_round_trip() {
        let h = sample_header();
        let encoded = write_serialization_header(&h);
        let decoded = read_serialization_header(&encoded).unwrap();

        assert_eq!(decoded.min_timestamp, h.min_timestamp);
        assert_eq!(decoded.min_local_deletion_time, h.min_local_deletion_time);
        assert_eq!(decoded.min_ttl, h.min_ttl);
        assert_eq!(decoded.key_type, h.key_type);
        assert_eq!(decoded.clustering_types, h.clustering_types);
        assert_eq!(decoded.static_columns, h.static_columns);
        assert_eq!(decoded.regular_columns, h.regular_columns);
    }

    #[test]
    fn full_statistics_round_trip() {
        let s = sample_statistics();
        let encoded = write_statistics(&s);
        let decoded = read_statistics(&encoded).unwrap();

        // ValidationMetadata
        assert_eq!(decoded.validation.partitioner, s.validation.partitioner);
        assert!(
            (decoded.validation.bloom_fp_chance - s.validation.bloom_fp_chance).abs()
                < f64::EPSILON
        );

        // Opaque components
        assert_eq!(decoded.compaction, s.compaction);
        assert_eq!(decoded.stats, s.stats);

        // SerializationHeader
        assert_eq!(decoded.header.min_timestamp, s.header.min_timestamp);
        assert_eq!(
            decoded.header.min_local_deletion_time,
            s.header.min_local_deletion_time
        );
        assert_eq!(decoded.header.min_ttl, s.header.min_ttl);
        assert_eq!(decoded.header.key_type, s.header.key_type);
        assert_eq!(decoded.header.clustering_types, s.header.clustering_types);
        assert_eq!(decoded.header.static_columns, s.header.static_columns);
        assert_eq!(decoded.header.regular_columns, s.header.regular_columns);
    }

    #[test]
    fn crc_checksum_validated() {
        let s = sample_statistics();
        let mut encoded = write_statistics(&s);

        // Corrupt a byte inside the first component's data.
        // Layout: 4 (count) + 4 (ordinal) + 4 (data_length) = 12, then data starts.
        let data_start = 12;
        assert!(data_start < encoded.len());
        encoded[data_start] ^= 0xFF;

        let result = read_statistics(&encoded);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ChecksumMismatch { .. } => {} // expected
            other => panic!("expected ChecksumMismatch, got: {other}"),
        }
    }

    #[test]
    fn header_with_no_columns() {
        let h = SerializationHeader {
            min_timestamp: 0,
            min_local_deletion_time: 0,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![],
        };
        let encoded = write_serialization_header(&h);
        let decoded = read_serialization_header(&encoded).unwrap();

        assert_eq!(decoded.min_timestamp, 0);
        assert_eq!(decoded.clustering_types.len(), 0);
        assert_eq!(decoded.static_columns.len(), 0);
        assert_eq!(decoded.regular_columns.len(), 0);
    }

    #[test]
    fn header_negative_varints() {
        let h = SerializationHeader {
            min_timestamp: -1_000_000,
            min_local_deletion_time: -1,
            min_ttl: -42,
            key_type: "k".to_string(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![],
        };
        let encoded = write_serialization_header(&h);
        let decoded = read_serialization_header(&encoded).unwrap();

        assert_eq!(decoded.min_timestamp, -1_000_000);
        assert_eq!(decoded.min_local_deletion_time, -1);
        assert_eq!(decoded.min_ttl, -42);
    }
}
