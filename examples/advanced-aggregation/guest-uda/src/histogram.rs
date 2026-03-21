//! Histogram UDA: `histogram(double) -> map<text, bigint>`
//!
//! Buckets values into five ranges and returns counts as a CQL map:
//!
//!   "0-20"  -> count of values in (-inf, 20)
//!   "20-40" -> count of values in [20, 40)
//!   "40-60" -> count of values in [40, 60)
//!   "60-80" -> count of values in [60, 80)
//!   "80+"   -> count of values >= 80
//!
//! Serialized state is 40 bytes: 5 * i64 (little-endian), one per bucket.
//!
//! Return encoding: finalize() returns `collection-val(blob)` where the blob
//! encodes a `map<text, bigint>` as: [u32 entry_count] followed by entries of
//! [u16 key_len][key_bytes][i64 value_le]. The host decodes this back to the
//! CQL map<text, bigint> type.

use std::cell::RefCell;

use crate::CqlValue;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NUM_BUCKETS: usize = 5;
const BOUNDARIES: [f64; 4] = [20.0, 40.0, 60.0, 80.0];
const LABELS: [&str; NUM_BUCKETS] = ["0-20", "20-40", "40-60", "60-80", "80+"];

/// Serialized state length: 5 buckets * 8 bytes (i64) each.
pub const SERIAL_LEN: usize = NUM_BUCKETS * 8;

// ---------------------------------------------------------------------------
// Thread-local state
// ---------------------------------------------------------------------------

thread_local! {
    // counts[i] corresponds to LABELS[i]
    static COUNTS: RefCell<[i64; NUM_BUCKETS]> = RefCell::new([0i64; NUM_BUCKETS]);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map a value to its bucket index (0-based).
fn bucket_index(value: f64) -> usize {
    // Walk boundaries in order; the value falls into the first bucket
    // whose boundary it is less than.
    for (i, &boundary) in BOUNDARIES.iter().enumerate() {
        if value < boundary {
            return i;
        }
    }
    // Falls into the last bucket ("80+")
    NUM_BUCKETS - 1
}

/// Encode the bucket counts as a compact binary map blob.
///
/// Format: [u32 entry_count LE] then for each entry:
///         [u16 key_len LE][key_utf8_bytes][i64 value LE]
///
/// The host decodes this blob back to the CQL `map<text, bigint>` type.
fn encode_map_blob(counts: &[i64; NUM_BUCKETS]) -> Vec<u8> {
    // Precondition: counts length matches label count
    debug_assert_eq!(counts.len(), LABELS.len());

    let entry_count = NUM_BUCKETS as u32;
    // Capacity estimate: 4 (count) + 5 * (2 + 5 + 8) = 4 + 75 = 79 bytes worst case
    let mut buf = Vec::with_capacity(79);
    buf.extend_from_slice(&entry_count.to_le_bytes());

    for (i, &count) in counts.iter().enumerate() {
        let key = LABELS[i].as_bytes();
        let key_len = key.len() as u16;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&count.to_le_bytes());
    }

    buf
}

// ---------------------------------------------------------------------------
// Public interface (called from lib.rs dispatcher)
// ---------------------------------------------------------------------------

/// Reset all bucket counts to zero.
pub fn init(_init_cond: Option<CqlValue>) -> Result<(), String> {
    COUNTS.with(|c| {
        let mut counts = c.borrow_mut();
        for count in counts.iter_mut() {
            *count = 0;
        }
    });
    Ok(())
}

/// Process one row: args[0] = value (double).
pub fn accumulate(args: &[CqlValue]) -> Result<(), String> {
    // Precondition: exactly one argument
    if args.len() != 1 {
        return Err(format!(
            "histogram expects 1 arg, got {}",
            args.len()
        ));
    }

    let value = extract_double(&args[0]).ok_or("histogram: arg 0 is not a double")?;
    let idx = bucket_index(value);

    // Postcondition: idx is always within bounds
    debug_assert!(idx < NUM_BUCKETS, "bucket_index returned out-of-range index");

    COUNTS.with(|c| {
        let mut counts = c.borrow_mut();
        counts[idx] = counts[idx].saturating_add(1);
    });

    Ok(())
}

/// Merge a serialized partial state (40 bytes) from another replica.
///
/// NOTE: In production each UDA would be a separate component; this
/// merge path is shared here only because both UDAs live in one component.
pub fn merge(serialized_state: &[u8]) -> Result<(), String> {
    // Precondition: state buffer must be exactly SERIAL_LEN bytes
    if serialized_state.len() != SERIAL_LEN {
        return Err(format!(
            "histogram: merge expected {SERIAL_LEN} bytes, got {}",
            serialized_state.len()
        ));
    }

    COUNTS.with(|c| {
        let mut counts = c.borrow_mut();
        for (i, chunk) in serialized_state.chunks_exact(8).enumerate() {
            let partial = i64::from_le_bytes(chunk.try_into().unwrap());
            counts[i] = counts[i].saturating_add(partial);
        }
    });

    Ok(())
}

/// Serialize bucket counts to 40 bytes (5 * little-endian i64).
pub fn serialize_state() -> Vec<u8> {
    let mut buf = Vec::with_capacity(SERIAL_LEN);
    COUNTS.with(|c| {
        for count in c.borrow().iter() {
            buf.extend_from_slice(&count.to_le_bytes());
        }
    });
    // Postcondition: buffer is exactly SERIAL_LEN bytes
    debug_assert_eq!(buf.len(), SERIAL_LEN, "serialize_state produced wrong length");
    buf
}

/// Build the CQL map result encoded as a collection-val blob.
///
/// Returns `CqlValue::CollectionVal(blob)` where blob encodes
/// `map<text, bigint>` using the format described in `encode_map_blob`.
/// The host unwraps this into the final CQL map<text, bigint> response.
pub fn finalize() -> Result<CqlValue, String> {
    let blob = COUNTS.with(|c| encode_map_blob(&c.borrow()));

    // Postcondition: blob is non-empty (at minimum contains entry count)
    debug_assert!(!blob.is_empty());

    Ok(CqlValue::CollectionVal(blob))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_double(v: &CqlValue) -> Option<f64> {
    match v {
        CqlValue::DoubleVal(d) => Some(*d),
        CqlValue::FloatVal(f) => Some(f64::from(*f)),
        CqlValue::IntVal(i) => Some(f64::from(*i)),
        CqlValue::BigintVal(i) => Some(*i as f64),
        _ => None,
    }
}
