//! Statistics.db reader and writer for Cassandra BTI SSTables.
//!
//! The Statistics.db file contains four CRC32-checksummed components:
//!
//! | Ordinal | Name                   | Description                                     |
//! |---------|------------------------|-------------------------------------------------|
//! | 0       | ValidationMetadata     | Partitioner class name + Bloom filter FP chance  |
//! | 1       | CompactionMetadata     | HyperLogLogPlus cardinality estimate (opaque)    |
//! | 2       | StatsMetadata          | Timestamps, sizes, histograms (opaque for now)   |
//! | 3       | SerializationHeader    | Column definitions for Data.db delta decoding    |
//!
//! # File layout
//!
//! ```text
//! [i32 component_count]
//! [i32 ordinal, i32 offset] * component_count   // TOC
//! [component data]                                // concatenated
//! [u32 crc32] * component_count                   // one per component
//! ```
//!
//! The CRC32 checksum for each component covers that component's serialized bytes.

use ferrosa_common::{Error, Result};

use crate::varint;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// All four components of a Statistics.db file.
#[derive(Debug, Clone)]
pub struct Statistics {
    pub validation: ValidationMetadata,
    pub compaction: CompactionMetadata,
    pub stats: StatsMetadata,
    pub header: SerializationHeader,
}

/// Ordinal 0 — partitioner class name and Bloom filter false-positive chance.
#[derive(Debug, Clone)]
pub struct ValidationMetadata {
    /// Fully-qualified Java class name, e.g.
    /// `"org.apache.cassandra.dht.Murmur3Partitioner"`.
    pub partitioner_class: String,
    /// Bloom filter false-positive chance, typically 0.01.
    pub bloom_fp_chance: f64,
}

/// Ordinal 1 — HyperLogLogPlus cardinality estimate (stored as opaque bytes).
#[derive(Debug, Clone)]
pub struct CompactionMetadata {
    pub data: Vec<u8>,
}

/// Ordinal 2 — aggregate statistics (stored as opaque bytes for now).
#[derive(Debug, Clone)]
pub struct StatsMetadata {
    pub data: Vec<u8>,
}

/// Ordinal 3 — column definitions needed for Data.db delta decoding.
#[derive(Debug, Clone)]
pub struct SerializationHeader {
    pub min_timestamp: i64,
    pub min_local_deletion_time: i32,
    pub min_ttl: i32,
    /// CQL type of the partition key, e.g. `"org.apache.cassandra.db.marshal.UTF8Type"`.
    pub key_type: String,
    /// CQL types of the clustering columns.
    pub clustering_types: Vec<String>,
    /// Static columns: (name bytes, CQL type string).
    pub static_columns: Vec<(Vec<u8>, String)>,
    /// Regular columns: (name bytes, CQL type string).
    pub regular_columns: Vec<(Vec<u8>, String)>,
}

// ---------------------------------------------------------------------------
// Top-level read / write
// ---------------------------------------------------------------------------

/// Deserialize a complete Statistics.db file from `data`.
///
/// Cassandra's Statistics.db layout has inline CRC32 checksums:
///
/// ```text
/// [i32 component_count]
/// [i32 CRC of count]
/// [toc: (i32 ordinal, i32 offset) * component_count]
/// [i32 CRC of TOC]
/// [component_0 bytes] [i32 CRC]
/// [component_1 bytes] [i32 CRC]
/// ...
/// ```
///
/// Offsets in the TOC are relative to the first component byte (after the
/// TOC CRC). Each component's CRC follows its data inline.
pub fn read_statistics(data: &[u8]) -> Result<Statistics> {
    if data.len() < 8 {
        return Err(Error::InvalidFormat(
            "Statistics.db too short for component count + CRC".into(),
        ));
    }

    let component_count = i32_be(data, 0) as usize;
    if component_count != 4 {
        return Err(Error::InvalidFormat(format!(
            "expected 4 components, got {component_count}"
        )));
    }

    // Verify CRC of component count.
    let count_crc_expected = u32_be(data, 4);
    let count_crc_actual = crc32_of_i32(component_count as i32);
    if count_crc_expected != count_crc_actual {
        return Err(Error::ChecksumMismatch {
            expected: count_crc_expected,
            actual: count_crc_actual,
        });
    }

    // TOC: component_count * (ordinal i32, offset i32), starting at byte 8.
    let toc_size = component_count * 8;
    let toc_start = 8;
    let toc_end = toc_start + toc_size;
    if data.len() < toc_end + 4 {
        return Err(Error::InvalidFormat(
            "Statistics.db too short for TOC + CRC".into(),
        ));
    }

    // Verify CRC of TOC — Cassandra uses a cumulative CRC: the CRC32 object
    // is NOT reset after writing the count CRC, so the TOC CRC covers
    // count bytes + TOC bytes (skipping the count CRC itself).
    let toc_crc_expected = u32_be(data, toc_end);
    let toc_crc_actual = {
        let mut h = crc32fast::Hasher::new();
        h.update(&data[0..4]); // count bytes
        h.update(&data[toc_start..toc_end]); // TOC bytes
        h.finalize()
    };
    if toc_crc_expected != toc_crc_actual {
        return Err(Error::ChecksumMismatch {
            expected: toc_crc_expected,
            actual: toc_crc_actual,
        });
    }

    // Parse TOC entries and sort by ordinal.
    let mut toc: Vec<(i32, i32)> = Vec::with_capacity(component_count);
    for i in 0..component_count {
        let base = toc_start + i * 8;
        let ordinal = i32_be(data, base);
        let offset = i32_be(data, base + 4);
        toc.push((ordinal, offset));
    }
    toc.sort_by_key(|&(ord, _)| ord);

    // Offsets in the TOC are absolute positions within the file.
    // Each component's data ends 4 bytes before the next component starts
    // (the 4-byte inline CRC).
    let mut components: Vec<(i32, &[u8])> = Vec::with_capacity(component_count);
    for (idx, &(ordinal, offset)) in toc.iter().enumerate() {
        let comp_start = offset as usize;
        let comp_end = if idx + 1 < component_count {
            toc[idx + 1].1 as usize - 4
        } else {
            data.len() - 4
        };
        if comp_start > comp_end || comp_end > data.len() {
            return Err(Error::InvalidFormat(format!(
                "component {ordinal} has invalid range {comp_start}..{comp_end}"
            )));
        }
        let comp_data = &data[comp_start..comp_end];

        // CRC follows inline after the component data.
        let expected_crc = u32_be(data, comp_end);
        let actual_crc = crc32fast::hash(comp_data);
        if expected_crc != actual_crc {
            return Err(Error::ChecksumMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        components.push((ordinal, comp_data));
    }

    // Extract each component by ordinal.
    let find = |ord: i32| -> Result<&[u8]> {
        components
            .iter()
            .find(|&&(o, _)| o == ord)
            .map(|&(_, d)| d)
            .ok_or_else(|| Error::InvalidFormat(format!("missing component ordinal {ord}")))
    };

    let validation = read_validation_metadata(find(0)?)?;
    let compaction = CompactionMetadata {
        data: find(1)?.to_vec(),
    };
    let stats = StatsMetadata {
        data: find(2)?.to_vec(),
    };
    let header = read_serialization_header(find(3)?)?;

    Ok(Statistics {
        validation,
        compaction,
        stats,
        header,
    })
}

/// Serialize a `Statistics` into the Statistics.db binary format.
///
/// Produces the Cassandra-compatible layout with inline CRC32 checksums.
pub fn write_statistics(stats: &Statistics) -> Vec<u8> {
    // Serialize each component.
    let comp0 = write_validation_metadata(&stats.validation);
    let comp1 = stats.compaction.data.clone();
    let comp2 = stats.stats.data.clone();
    let comp3 = write_serialization_header(&stats.header);

    let components: [(i32, &[u8]); 4] = [(0, &comp0), (1, &comp1), (2, &comp2), (3, &comp3)];

    let component_count: i32 = 4;

    let mut out = Vec::new();

    // Component count + CRC.
    out.extend_from_slice(&component_count.to_be_bytes());
    out.extend_from_slice(&crc32_of_i32(component_count).to_be_bytes());

    // TOC: ordinal + offset pairs. Offsets are absolute file positions.
    // Header is: count(4) + count_crc(4) + toc(n*8) + toc_crc(4).
    let header_size = 4 + 4 + (component_count as usize * 8) + 4;
    let toc_start = out.len();
    let mut offset: i32 = header_size as i32;
    for &(ordinal, d) in &components {
        out.extend_from_slice(&ordinal.to_be_bytes());
        out.extend_from_slice(&offset.to_be_bytes());
        // Next offset = current offset + component data len + 4 bytes CRC
        offset += d.len() as i32 + 4;
    }
    let toc_end = out.len();

    // Cumulative CRC: count bytes + TOC bytes (Cassandra doesn't reset the CRC).
    let toc_crc = {
        let mut h = crc32fast::Hasher::new();
        h.update(&out[0..4]); // count bytes
        h.update(&out[toc_start..toc_end]); // TOC bytes
        h.finalize()
    };
    out.extend_from_slice(&toc_crc.to_be_bytes());

    // Component data, each followed by its inline CRC.
    for &(_, d) in &components {
        out.extend_from_slice(d);
        let crc = crc32fast::hash(d);
        out.extend_from_slice(&crc.to_be_bytes());
    }

    out
}

// ---------------------------------------------------------------------------
// ValidationMetadata (ordinal 0)
// ---------------------------------------------------------------------------

fn read_validation_metadata(data: &[u8]) -> Result<ValidationMetadata> {
    if data.len() < 2 {
        return Err(Error::InvalidFormat("ValidationMetadata too short".into()));
    }
    let (class, consumed) = read_utf8_string(data)?;
    let rest = &data[consumed..];
    if rest.len() < 8 {
        return Err(Error::InvalidFormat(
            "ValidationMetadata missing bloom_fp_chance".into(),
        ));
    }
    let bloom_fp_chance = f64::from_be_bytes(rest[..8].try_into().unwrap());
    Ok(ValidationMetadata {
        partitioner_class: class,
        bloom_fp_chance,
    })
}

fn write_validation_metadata(v: &ValidationMetadata) -> Vec<u8> {
    let mut out = Vec::new();
    write_utf8_string(&mut out, &v.partitioner_class);
    out.extend_from_slice(&v.bloom_fp_chance.to_be_bytes());
    out
}

// ---------------------------------------------------------------------------
// SerializationHeader (ordinal 3)
// ---------------------------------------------------------------------------

/// Epoch constants for delta-encoding in the SerializationHeader.
const TIMESTAMP_EPOCH: i64 = 1_442_880_000_000_000; // Sept 22, 2015 00:00 UTC, microseconds
const DELETION_TIME_EPOCH: i32 = 1_442_880_000; // Same date, seconds
const TTL_EPOCH: i32 = 0;

/// Decode a `SerializationHeader` from its binary representation.
///
/// All fields use unsigned varints. The min_timestamp, min_local_deletion_time,
/// and min_ttl are delta-encoded against epoch constants.
pub fn read_serialization_header(data: &[u8]) -> Result<SerializationHeader> {
    let mut pos = 0;

    let min_timestamp = read_uvint_u64(data, &mut pos)? as i64 + TIMESTAMP_EPOCH;
    let min_local_deletion_time = read_uvint_u32(data, &mut pos)? as i32 + DELETION_TIME_EPOCH;
    let min_ttl = read_uvint_u32(data, &mut pos)? as i32 + TTL_EPOCH;

    let key_type = read_vint_prefixed_string(data, &mut pos)?;

    let clustering_count = read_uvint_u32(data, &mut pos)? as usize;
    let mut clustering_types = Vec::with_capacity(clustering_count);
    for _ in 0..clustering_count {
        clustering_types.push(read_vint_prefixed_string(data, &mut pos)?);
    }

    let static_count = read_uvint_u32(data, &mut pos)? as usize;
    let mut static_columns = Vec::with_capacity(static_count);
    for _ in 0..static_count {
        let name = read_vint_prefixed_bytes(data, &mut pos)?;
        let typ = read_vint_prefixed_string(data, &mut pos)?;
        static_columns.push((name, typ));
    }

    let regular_count = read_uvint_u32(data, &mut pos)? as usize;
    let mut regular_columns = Vec::with_capacity(regular_count);
    for _ in 0..regular_count {
        let name = read_vint_prefixed_bytes(data, &mut pos)?;
        let typ = read_vint_prefixed_string(data, &mut pos)?;
        regular_columns.push((name, typ));
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

/// Encode a `SerializationHeader` into its binary representation.
///
/// All fields use unsigned varints. The min_timestamp, min_local_deletion_time,
/// and min_ttl are delta-encoded against epoch constants.
pub fn write_serialization_header(h: &SerializationHeader) -> Vec<u8> {
    let mut out = Vec::new();
    let mut vbuf = [0u8; 9];

    // min_timestamp delta (unsigned vint)
    let ts_delta = (h.min_timestamp - TIMESTAMP_EPOCH) as u64;
    let n = varint::write_unsigned_vint(&mut vbuf, ts_delta);
    out.extend_from_slice(&vbuf[..n]);

    // min_local_deletion_time delta (unsigned vint32)
    let ldt_delta = (h.min_local_deletion_time - DELETION_TIME_EPOCH) as u64;
    let n = varint::write_unsigned_vint(&mut vbuf, ldt_delta);
    out.extend_from_slice(&vbuf[..n]);

    // min_ttl delta (unsigned vint32)
    let ttl_delta = (h.min_ttl - TTL_EPOCH) as u64;
    let n = varint::write_unsigned_vint(&mut vbuf, ttl_delta);
    out.extend_from_slice(&vbuf[..n]);

    // key_type (unsigned vint32 length + UTF-8 bytes)
    write_vint_prefixed_string(&mut out, &h.key_type);

    // clustering_types
    let n = varint::write_unsigned_vint(&mut vbuf, h.clustering_types.len() as u64);
    out.extend_from_slice(&vbuf[..n]);
    for ct in &h.clustering_types {
        write_vint_prefixed_string(&mut out, ct);
    }

    // static_columns
    let n = varint::write_unsigned_vint(&mut vbuf, h.static_columns.len() as u64);
    out.extend_from_slice(&vbuf[..n]);
    for (name, typ) in &h.static_columns {
        write_vint_prefixed_bytes(&mut out, name);
        write_vint_prefixed_string(&mut out, typ);
    }

    // regular_columns
    let n = varint::write_unsigned_vint(&mut vbuf, h.regular_columns.len() as u64);
    out.extend_from_slice(&vbuf[..n]);
    for (name, typ) in &h.regular_columns {
        write_vint_prefixed_bytes(&mut out, name);
        write_vint_prefixed_string(&mut out, typ);
    }

    out
}

// ---------------------------------------------------------------------------
// Helpers — varint-prefixed reads (SerializationHeader)
// ---------------------------------------------------------------------------

/// Read an unsigned varint as `u64` from `data[pos..]`, advancing `pos`.
fn read_uvint_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    let (val, n) = varint::read_unsigned_vint(&data[*pos..])?;
    *pos += n;
    Ok(val)
}

/// Read an unsigned varint as `u32` from `data[pos..]`, advancing `pos`.
///
/// Returns an error if the decoded value exceeds `u32::MAX`.
fn read_uvint_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    let (val, n) = varint::read_unsigned_vint(&data[*pos..])?;
    *pos += n;
    if val > u32::MAX as u64 {
        return Err(Error::InvalidData(format!(
            "unsigned vint32 value {val} exceeds u32::MAX"
        )));
    }
    Ok(val as u32)
}

/// Read a vint-length-prefixed UTF-8 string from `data[pos..]`, advancing `pos`.
///
/// The length prefix is an unsigned varint (matching Cassandra's `writeUnsignedVInt32`
/// for string lengths in `SerializationHeader`).
fn read_vint_prefixed_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let len = read_uvint_u32(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(Error::InvalidData(format!(
            "vint-prefixed string length {len} exceeds available data {}",
            data.len() - *pos
        )));
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| Error::InvalidData(format!("invalid UTF-8: {e}")))?;
    *pos += len;
    Ok(s.to_owned())
}

/// Read vint-length-prefixed raw bytes from `data[pos..]`, advancing `pos`.
fn read_vint_prefixed_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>> {
    let len = read_uvint_u32(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(Error::InvalidData(format!(
            "vint-prefixed bytes length {len} exceeds available data {}",
            data.len() - *pos
        )));
    }
    let bytes = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(bytes)
}

/// Write a vint-length-prefixed UTF-8 string.
fn write_vint_prefixed_string(out: &mut Vec<u8>, s: &str) {
    let mut vbuf = [0u8; 9];
    let n = varint::write_unsigned_vint(&mut vbuf, s.len() as u64);
    out.extend_from_slice(&vbuf[..n]);
    out.extend_from_slice(s.as_bytes());
}

/// Write vint-length-prefixed raw bytes.
fn write_vint_prefixed_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let mut vbuf = [0u8; 9];
    let n = varint::write_unsigned_vint(&mut vbuf, bytes.len() as u64);
    out.extend_from_slice(&vbuf[..n]);
    out.extend_from_slice(bytes);
}

// ---------------------------------------------------------------------------
// Helpers — primitive encoding
// ---------------------------------------------------------------------------

/// Compute CRC32 of a single big-endian i32 value.
///
/// Cassandra's `updateChecksumInt` feeds each byte of the big-endian
/// representation individually to `java.util.zip.CRC32`.
fn crc32_of_i32(value: i32) -> u32 {
    crc32fast::hash(&value.to_be_bytes())
}

/// Read a big-endian i32 at the given byte offset.
fn i32_be(data: &[u8], off: usize) -> i32 {
    i32::from_be_bytes(data[off..off + 4].try_into().unwrap())
}

/// Read a big-endian u32 at the given byte offset.
fn u32_be(data: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(data[off..off + 4].try_into().unwrap())
}

/// Read a UTF-8 string prefixed by a big-endian u16 length.
/// Returns the string and total bytes consumed (2 + len).
fn read_utf8_string(data: &[u8]) -> Result<(String, usize)> {
    if data.len() < 2 {
        return Err(Error::InvalidData(
            "not enough bytes for string length prefix".into(),
        ));
    }
    let len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
    let end = 2 + len;
    if data.len() < end {
        return Err(Error::InvalidData(format!(
            "string length {len} exceeds available data {}",
            data.len() - 2
        )));
    }
    let s = std::str::from_utf8(&data[2..end])
        .map_err(|e| Error::InvalidData(format!("invalid UTF-8: {e}")))?;
    Ok((s.to_owned(), end))
}

/// Write a UTF-8 string with a big-endian u16 length prefix.
fn write_utf8_string(out: &mut Vec<u8>, s: &str) {
    let len = s.len() as u16;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a sample `SerializationHeader`.
    fn sample_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 1_700_000_000_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![(
                b"sc1".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
            regular_columns: vec![
                (
                    b"val".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"age".to_vec(),
                    "org.apache.cassandra.db.marshal.Int32Type".into(),
                ),
            ],
        }
    }

    /// Helper: build a sample `Statistics`.
    fn sample_statistics() -> Statistics {
        Statistics {
            validation: ValidationMetadata {
                partitioner_class: "org.apache.cassandra.dht.Murmur3Partitioner".into(),
                bloom_fp_chance: 0.01,
            },
            compaction: CompactionMetadata {
                data: vec![0xCA, 0xFE, 0xBA, 0xBE],
            },
            stats: StatsMetadata {
                data: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02],
            },
            header: sample_header(),
        }
    }

    // -- SerializationHeader round-trip --

    #[test]
    fn serialization_header_round_trip() {
        let original = sample_header();
        let encoded = write_serialization_header(&original);
        let decoded = read_serialization_header(&encoded).unwrap();

        assert_eq!(decoded.min_timestamp, original.min_timestamp);
        assert_eq!(
            decoded.min_local_deletion_time,
            original.min_local_deletion_time
        );
        assert_eq!(decoded.min_ttl, original.min_ttl);
        assert_eq!(decoded.key_type, original.key_type);
        assert_eq!(decoded.clustering_types, original.clustering_types);
        assert_eq!(decoded.static_columns, original.static_columns);
        assert_eq!(decoded.regular_columns, original.regular_columns);
    }

    #[test]
    fn serialization_header_empty_columns() {
        let header = SerializationHeader {
            min_timestamp: TIMESTAMP_EPOCH,
            min_local_deletion_time: DELETION_TIME_EPOCH,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.BytesType".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![],
        };
        let encoded = write_serialization_header(&header);
        let decoded = read_serialization_header(&encoded).unwrap();

        assert_eq!(decoded.min_timestamp, TIMESTAMP_EPOCH);
        assert_eq!(decoded.min_local_deletion_time, DELETION_TIME_EPOCH);
        assert!(decoded.clustering_types.is_empty());
        assert!(decoded.static_columns.is_empty());
        assert!(decoded.regular_columns.is_empty());
    }

    #[test]
    fn serialization_header_live_partition_values() {
        // i32::MAX for local_deletion_time means "live" (no deletion).
        let header = SerializationHeader {
            min_timestamp: TIMESTAMP_EPOCH + 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            key_type: "T".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![],
        };
        let encoded = write_serialization_header(&header);
        let decoded = read_serialization_header(&encoded).unwrap();

        assert_eq!(decoded.min_timestamp, TIMESTAMP_EPOCH + 1_000_000);
        assert_eq!(decoded.min_local_deletion_time, i32::MAX);
        assert_eq!(decoded.min_ttl, 0);
    }

    // -- Full Statistics round-trip --

    #[test]
    fn statistics_round_trip() {
        let original = sample_statistics();
        let encoded = write_statistics(&original);
        let decoded = read_statistics(&encoded).unwrap();

        // ValidationMetadata
        assert_eq!(
            decoded.validation.partitioner_class,
            original.validation.partitioner_class
        );
        assert!(
            (decoded.validation.bloom_fp_chance - original.validation.bloom_fp_chance).abs()
                < f64::EPSILON
        );

        // CompactionMetadata (opaque)
        assert_eq!(decoded.compaction.data, original.compaction.data);

        // StatsMetadata (opaque)
        assert_eq!(decoded.stats.data, original.stats.data);

        // SerializationHeader
        assert_eq!(decoded.header.min_timestamp, original.header.min_timestamp);
        assert_eq!(
            decoded.header.min_local_deletion_time,
            original.header.min_local_deletion_time
        );
        assert_eq!(decoded.header.min_ttl, original.header.min_ttl);
        assert_eq!(decoded.header.key_type, original.header.key_type);
        assert_eq!(
            decoded.header.clustering_types,
            original.header.clustering_types
        );
        assert_eq!(
            decoded.header.static_columns,
            original.header.static_columns
        );
        assert_eq!(
            decoded.header.regular_columns,
            original.header.regular_columns
        );
    }

    #[test]
    fn statistics_round_trip_empty_opaque() {
        let stats = Statistics {
            validation: ValidationMetadata {
                partitioner_class: "P".into(),
                bloom_fp_chance: 0.1,
            },
            compaction: CompactionMetadata { data: vec![] },
            stats: StatsMetadata { data: vec![] },
            header: SerializationHeader {
                min_timestamp: TIMESTAMP_EPOCH,
                min_local_deletion_time: DELETION_TIME_EPOCH,
                min_ttl: 0,
                key_type: "K".into(),
                clustering_types: vec![],
                static_columns: vec![],
                regular_columns: vec![],
            },
        };
        let encoded = write_statistics(&stats);
        let decoded = read_statistics(&encoded).unwrap();
        assert!(decoded.compaction.data.is_empty());
        assert!(decoded.stats.data.is_empty());
    }

    // -- CRC validation --

    #[test]
    fn statistics_crc_mismatch_detected() {
        let stats = sample_statistics();
        let mut encoded = write_statistics(&stats);

        // Corrupt one byte of component data (after TOC).
        let toc_end = 4 + 4 * 8; // 4 + 32 = 36
        if encoded.len() > toc_end {
            encoded[toc_end] ^= 0xFF;
        }

        let result = read_statistics(&encoded);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ChecksumMismatch { .. } => {} // expected
            other => panic!("expected ChecksumMismatch, got: {other}"),
        }
    }

    #[test]
    fn statistics_bad_component_count() {
        let mut data = vec![0u8; 40];
        // Set component count to 5 (invalid)
        data[..4].copy_from_slice(&5_i32.to_be_bytes());
        let result = read_statistics(&data);
        assert!(result.is_err());
    }

    // -- ValidationMetadata --

    #[test]
    fn validation_metadata_round_trip() {
        let v = ValidationMetadata {
            partitioner_class: "org.apache.cassandra.dht.Murmur3Partitioner".into(),
            bloom_fp_chance: 0.01,
        };
        let encoded = write_validation_metadata(&v);
        let decoded = read_validation_metadata(&encoded).unwrap();
        assert_eq!(decoded.partitioner_class, v.partitioner_class);
        assert!((decoded.bloom_fp_chance - v.bloom_fp_chance).abs() < f64::EPSILON);
    }

    // -- File structure verification --

    #[test]
    fn statistics_file_structure() {
        let stats = sample_statistics();
        let encoded = write_statistics(&stats);

        // First 4 bytes: component count = 4
        assert_eq!(i32_be(&encoded, 0), 4);

        // Bytes 4-7: CRC of count
        let count_crc = crc32_of_i32(4);
        assert_eq!(u32_be(&encoded, 4), count_crc);

        // TOC: 4 entries * 8 bytes each = 32 bytes starting at offset 8.
        // Ordinals should be 0, 1, 2, 3.
        for i in 0..4 {
            let ordinal = i32_be(&encoded, 8 + i * 8);
            assert_eq!(ordinal, i as i32);
        }

        // Byte 40-43: cumulative CRC of (count bytes + TOC bytes)
        let toc_crc = {
            let mut h = crc32fast::Hasher::new();
            h.update(&encoded[0..4]); // count
            h.update(&encoded[8..40]); // TOC
            h.finalize()
        };
        assert_eq!(u32_be(&encoded, 40), toc_crc);

        // Component 0 starts at offset stored in TOC[0] (absolute).
        let comp0_start = i32_be(&encoded, 8 + 4) as usize; // offset field of TOC[0]
        let comp1_start = i32_be(&encoded, 8 + 8 + 4) as usize; // offset field of TOC[1]
                                                                // Component 0 data is comp0_start..comp1_start-4 (minus inline CRC)
        let comp0_data = &encoded[comp0_start..comp1_start - 4];
        let comp0_crc = crc32fast::hash(comp0_data);
        assert_eq!(u32_be(&encoded, comp1_start - 4), comp0_crc);
    }
}
