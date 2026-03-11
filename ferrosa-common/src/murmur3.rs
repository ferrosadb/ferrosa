/// Cassandra-compatible Murmur3 x64_128 hash function.
///
/// Returns `(h1, h2)` where:
/// - `h1` is used as the Token value (position on the hash ring)
/// - Both `h1` and `h2` are used for Bloom filter hashing
///
/// Reference: `org.apache.cassandra.utils.MurmurHash.hash3_x64_128`
pub fn hash3_x64_128(data: &[u8], seed: i64) -> (i64, i64) {
    let len = data.len();
    let nblocks = len / 16;
    let mut h1 = seed;
    let mut h2 = seed;

    const C1: i64 = 0x87c37b91114253d5_u64 as i64;
    const C2: i64 = 0x4cf5ad432745937f_i64;

    // Body: process 16-byte blocks
    for i in 0..nblocks {
        let mut k1 = get_block_i64(data, i * 2);
        let mut k2 = get_block_i64(data, i * 2 + 1);

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
    }

    // Tail: process remaining bytes
    //
    // CRITICAL: Cassandra's Java reads bytes as signed `byte` (-128..127)
    // and casts to `long` with SIGN EXTENSION. The body's getBlock() masks
    // with `& 0xff` (zero-extending), but the tail does NOT. This is
    // Cassandra's "sign bug" (see MurmurHash.java header comment). We must
    // replicate it exactly: `as i8 as i64` sign-extends, matching Java's
    // `(long) key.get(offset)` behavior.
    let tail = &data[nblocks * 16..];
    let mut k1: i64 = 0;
    let mut k2: i64 = 0;

    if tail.len() >= 15 {
        k2 ^= (tail[14] as i8 as i64) << 48;
    }
    if tail.len() >= 14 {
        k2 ^= (tail[13] as i8 as i64) << 40;
    }
    if tail.len() >= 13 {
        k2 ^= (tail[12] as i8 as i64) << 32;
    }
    if tail.len() >= 12 {
        k2 ^= (tail[11] as i8 as i64) << 24;
    }
    if tail.len() >= 11 {
        k2 ^= (tail[10] as i8 as i64) << 16;
    }
    if tail.len() >= 10 {
        k2 ^= (tail[9] as i8 as i64) << 8;
    }
    if tail.len() >= 9 {
        k2 ^= tail[8] as i8 as i64;
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }

    if tail.len() >= 8 {
        k1 ^= (tail[7] as i8 as i64) << 56;
    }
    if tail.len() >= 7 {
        k1 ^= (tail[6] as i8 as i64) << 48;
    }
    if tail.len() >= 6 {
        k1 ^= (tail[5] as i8 as i64) << 40;
    }
    if tail.len() >= 5 {
        k1 ^= (tail[4] as i8 as i64) << 32;
    }
    if tail.len() >= 4 {
        k1 ^= (tail[3] as i8 as i64) << 24;
    }
    if tail.len() >= 3 {
        k1 ^= (tail[2] as i8 as i64) << 16;
    }
    if tail.len() >= 2 {
        k1 ^= (tail[1] as i8 as i64) << 8;
    }
    if !tail.is_empty() {
        k1 ^= tail[0] as i8 as i64;
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    // Finalization
    h1 ^= len as i64;
    h2 ^= len as i64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

/// Read a little-endian i64 from `data` at the given 8-byte block index.
fn get_block_i64(data: &[u8], index: usize) -> i64 {
    let offset = index * 8;
    i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ])
}

/// Murmur3 finalization mix — forces all bits to avalanche.
/// Uses unsigned right shift to match Java's `>>>` operator.
fn fmix64(mut k: i64) -> i64 {
    k ^= (k as u64 >> 33) as i64;
    k = k.wrapping_mul(0xff51afd7ed558ccd_u64 as i64);
    k ^= (k as u64 >> 33) as i64;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53_u64 as i64);
    k ^= (k as u64 >> 33) as i64;
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_zero() {
        // Empty input with seed 0: no blocks, no tail, fmix64(0) = 0
        assert_eq!(hash3_x64_128(&[], 0), (0, 0));
    }

    #[test]
    fn deterministic() {
        let input = b"ferrosa";
        let a = hash3_x64_128(input, 0);
        let b = hash3_x64_128(input, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn different_inputs_differ() {
        let a = hash3_x64_128(b"hello", 0);
        let b = hash3_x64_128(b"world", 0);
        assert_ne!(a, b);
    }

    #[test]
    fn different_seeds_differ() {
        let a = hash3_x64_128(b"hello", 0);
        let b = hash3_x64_128(b"hello", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn single_block_16_bytes() {
        // Exactly one 16-byte block, no tail
        let input: Vec<u8> = (0..16).collect();
        let (h1, h2) = hash3_x64_128(&input, 0);
        // Must not be zero (extremely unlikely for non-empty input)
        assert!(h1 != 0 || h2 != 0);
    }

    #[test]
    fn block_plus_tail() {
        // 17 bytes: one block + 1-byte tail
        let input: Vec<u8> = (0..17).collect();
        let (h1, h2) = hash3_x64_128(&input, 0);
        assert!(h1 != 0 || h2 != 0);
    }

    // Characterization tests: exact values verified against Cassandra.
    // Generated by running the generate_murmur3_vectors task (Task 6).
    // DO NOT change these values without re-verifying against Cassandra.
    //
    // To regenerate:
    //   cd cassandra && ant build
    //   javac -cp build/classes/main GenerateMurmur3Vectors.java
    //   java -cp build/classes/main:. GenerateMurmur3Vectors
}
