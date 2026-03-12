//! Cassandra-compatible Bloom filter using Murmur3 double-hashing.
//!
//! Uses the hash function family `h_i(key) = h1 + i * h2` where `(h1, h2)`
//! come from `murmur3::hash3_x64_128`. The number of hash functions `k` and
//! bit count `m` are computed from the target false-positive rate and expected
//! key count.
//!
//! # File Format (new format, Cassandra 5.x)
//!
//! ```text
//! [hash_count: i32] [word_count: i32] [bit_array: word_count * 8 bytes]
//! ```
//!
//! The bit array is raw bytes (NOT big-endian word-swapped).
//!
//! Reference: `org.apache.cassandra.utils.BloomFilter`,
//!            `org.apache.cassandra.utils.OffHeapBitSet`

use ferrosa_common::{Error, Result};

/// Cassandra-compatible Bloom filter.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Number of hash functions.
    hash_count: i32,
    /// Bit array stored as u64 words.
    bits: Vec<u64>,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for the expected key count and FP rate.
    ///
    /// ```
    /// use ferrosa_sstable::bloom::BloomFilter;
    ///
    /// let bf = BloomFilter::new(100, 0.01);
    /// assert!(!bf.is_present(42, 99)); // empty filter
    /// ```
    pub fn new(expected_keys: usize, fp_rate: f64) -> Self {
        let (num_bits, hash_count) = optimal_params(expected_keys, fp_rate);
        let word_count = num_bits.div_ceil(64);
        BloomFilter {
            hash_count: hash_count as i32,
            bits: vec![0u64; word_count],
        }
    }

    /// Add a key to the filter using its Murmur3 h1 and h2 values.
    ///
    /// Call with `DecoratedKey::filter_hash()` to get `(h1, h2)`.
    pub fn add(&mut self, h1: i64, h2: i64) {
        let num_bits = (self.bits.len() * 64) as i64;
        for i in 0..self.hash_count as i64 {
            let hash = h1.wrapping_add(i.wrapping_mul(h2));
            let bit_index = (hash % num_bits + num_bits) % num_bits;
            let word = bit_index as usize / 64;
            let bit = bit_index as usize % 64;
            self.bits[word] |= 1u64 << bit;
        }
    }

    /// Test if a key might be present (false positives possible).
    pub fn is_present(&self, h1: i64, h2: i64) -> bool {
        let num_bits = (self.bits.len() * 64) as i64;
        for i in 0..self.hash_count as i64 {
            let hash = h1.wrapping_add(i.wrapping_mul(h2));
            let bit_index = (hash % num_bits + num_bits) % num_bits;
            let word = bit_index as usize / 64;
            let bit = bit_index as usize % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Read a Bloom filter from its serialized form (new format).
    pub fn read(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::InvalidFormat("bloom filter too short".into()));
        }

        let hash_count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let word_count = i32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;

        let expected_len = 8 + word_count * 8;
        if data.len() < expected_len {
            return Err(Error::InvalidFormat(format!(
                "bloom filter: expected {} bytes, got {}",
                expected_len,
                data.len()
            )));
        }

        let mut bits = Vec::with_capacity(word_count);
        let mut pos = 8;
        for _ in 0..word_count {
            // Raw byte order (NOT big-endian swapped) — new format
            let word = u64::from_ne_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            bits.push(word);
            pos += 8;
        }

        Ok(BloomFilter { hash_count, bits })
    }

    /// Serialize the Bloom filter (new format).
    pub fn write(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.bits.len() * 8);
        buf.extend_from_slice(&self.hash_count.to_be_bytes());
        buf.extend_from_slice(&(self.bits.len() as i32).to_be_bytes());
        for &word in &self.bits {
            buf.extend_from_slice(&word.to_ne_bytes());
        }
        buf
    }
}

/// Compute optimal Bloom filter parameters.
/// Returns `(num_bits, hash_count)`.
fn optimal_params(expected_keys: usize, fp_rate: f64) -> (usize, usize) {
    let n = expected_keys.max(1) as f64;
    let m = -(n * fp_rate.ln()) / (2.0_f64.ln().powi(2));
    let num_bits = m.ceil() as usize;
    let k = (num_bits as f64 / n * 2.0_f64.ln()).round() as usize;
    let hash_count = k.max(1);
    (num_bits, hash_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_find() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(12345, 67890);
        assert!(bf.is_present(12345, 67890));
    }

    #[test]
    fn absent_key() {
        let bf = BloomFilter::new(100, 0.01);
        assert!(!bf.is_present(12345, 67890));
    }

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::new(1000, 0.01);
        let keys: Vec<(i64, i64)> = (0..1000)
            .map(|i| {
                let data = i64::to_le_bytes(i);
                ferrosa_common::murmur3::hash3_x64_128(&data, 0)
            })
            .collect();

        for &(h1, h2) in &keys {
            bf.add(h1, h2);
        }

        for &(h1, h2) in &keys {
            assert!(bf.is_present(h1, h2), "false negative!");
        }
    }

    #[test]
    fn false_positive_rate_reasonable() {
        let mut bf = BloomFilter::new(1000, 0.01);
        // Insert 1000 keys
        for i in 0..1000u64 {
            let data = i.to_le_bytes();
            let (h1, h2) = ferrosa_common::murmur3::hash3_x64_128(&data, 0);
            bf.add(h1, h2);
        }

        // Test 10000 non-inserted keys
        let mut false_positives = 0;
        for i in 1000..11000u64 {
            let data = i.to_le_bytes();
            let (h1, h2) = ferrosa_common::murmur3::hash3_x64_128(&data, 0);
            if bf.is_present(h1, h2) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / 10000.0;
        // Allow up to 5% (generous tolerance for probabilistic test)
        assert!(
            fp_rate < 0.05,
            "false positive rate {fp_rate:.4} exceeds 5%"
        );
    }

    #[test]
    fn read_write_round_trip() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.add(111, 222);
        bf.add(333, 444);

        let serialized = bf.write();
        let deserialized = BloomFilter::read(&serialized).unwrap();

        assert!(deserialized.is_present(111, 222));
        assert!(deserialized.is_present(333, 444));
        assert!(!deserialized.is_present(555, 666));
    }

    #[test]
    fn read_too_short() {
        assert!(BloomFilter::read(&[0; 4]).is_err());
    }

    #[test]
    fn optimal_params_sanity() {
        let (bits, k) = optimal_params(1000, 0.01);
        assert!(bits > 0);
        assert!(k > 0 && k < 20);
    }
}
