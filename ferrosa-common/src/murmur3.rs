//! Cassandra-compatible Murmur3 x64_128 hash function.
//!
//! Produces identical output to Cassandra's
//! `org.apache.cassandra.utils.MurmurHash.hash3_x64_128`. Returns `(h1, h2)`:
//! - `h1` is the [`Token`](crate::Token) value (hash ring position)
//! - Both `h1` and `h2` are used for Bloom filter double-hashing
//!
//! # Cassandra Sign Bug
//!
//! The tail bytes use sign-extension (`as i8 as i64`) matching Java's signed
//! byte cast, while the body uses zero-extension (`& 0xff`). This asymmetry
//! is intentional and must be preserved for compatibility.
//!
//! # Testing
//!
//! Characterization vectors generated from Cassandra source ensure exact
//! compatibility. See the `tests` module and `tools/generate_murmur3_vectors.java`.

/// Cassandra-compatible Murmur3 x64_128 hash function.
///
/// Returns `(h1, h2)` where:
/// - `h1` is used as the Token value (position on the hash ring)
/// - Both `h1` and `h2` are used for Bloom filter hashing
///
/// Reference: `org.apache.cassandra.utils.MurmurHash.hash3_x64_128`
///
/// ```
/// use ferrosa_common::murmur3::hash3_x64_128;
///
/// let (h1, h2) = hash3_x64_128(b"hello", 0);
/// assert_eq!(h1, -3758069500696749310_i64);
/// assert_eq!(h2, 6565844092913065241_i64);
/// ```
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

    // Characterization tests: exact values verified against Cassandra's MurmurHash.
    // Generated by tools/generate_murmur3_vectors.java (extracted from Cassandra source).
    // DO NOT change these values without re-verifying against Cassandra.
    //
    // To regenerate:
    //   javac tools/generate_murmur3_vectors.java -d tools/
    //   java -cp tools generate_murmur3_vectors
    #[test]
    fn cassandra_characterization_vectors() {
        #[rustfmt::skip]
        let vectors: &[(&[u8], i64, i64, i64)] = &[
            (&[], 0, 0_i64, 0_i64),
            (&[0x00], 0, 5048724184180415669_i64, 5864299874987029891_i64),
            (&[0x01], 0, 8849112093580131862_i64, 8613248517421295493_i64),
            (&[0x00, 0x01, 0x02, 0x03], 0, -2178171369485783280_i64, -3182349777052172830_i64),
            // "hello"
            (&[0x68, 0x65, 0x6C, 0x6C, 0x6F], 0, -3758069500696749310_i64, 6565844092913065241_i64),
            // "cassandra"
            (&[0x63, 0x61, 0x73, 0x73, 0x61, 0x6E, 0x64, 0x72, 0x61], 0, 356242581507269238_i64, -3708818142985255407_i64),
            // "ferrosa"
            (&[0x66, 0x65, 0x72, 0x72, 0x6F, 0x73, 0x61], 0, -7911154581804264429_i64, -5083183550992889052_i64),
            // 16 bytes: full block, no tail
            (&[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F], 0, 4920504430128807728_i64, -6084252774064723899_i64),
            // 17 bytes: one block + 1-byte tail
            (&[0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10], 0, 6662781046685680142_i64, -4512885640352061404_i64),
            // Tail lengths 1-9
            (&[0x2A], 0, -2387438309745315495_i64, -1061927915756559090_i64),
            (&[0x2A, 0x2B], 0, -3269077344954651576_i64, 2386793656744013895_i64),
            (&[0x2A, 0x2B, 0x2C], 0, 933789257985841999_i64, 9220622680705777568_i64),
            (&[0x2A, 0x2B, 0x2C, 0x2D], 0, -7266121506941928210_i64, -9171090338516060687_i64),
            (&[0x2A, 0x2B, 0x2C, 0x2D, 0x2E], 0, 1760022473563919461_i64, 6297924262328599677_i64),
            (&[0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F], 0, 3827067501025323880_i64, -3411035616850265486_i64),
            (&[0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30], 0, 3965318260031892433_i64, -6347482478139178268_i64),
            (&[0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31], 0, 4414120156332809765_i64, -397294961073588220_i64),
            (&[0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32], 0, -46930170645283670_i64, -1407208099063096482_i64),
            // High-byte values: critical for sign-extension behavior
            (&[0xFF], 0, -4442228696663692417_i64, -6049531771631615289_i64),
            (&[0x80], 0, -5284281814142962636_i64, 7980414882014114757_i64),
            (&[0xFF, 0xFE, 0xFD], 0, 4778542740094909933_i64, -8472617770952608660_i64),
            (&[0x00, 0x80, 0xFF, 0x01, 0x81, 0xFE, 0x02, 0x82], 0, 5298875845197940848_i64, -3170415259881363727_i64),
            // 16 high bytes: full block (body path uses & 0xff masking)
            (&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 0, -2824192546314762522_i64, -3410324854106180046_i64),
            // 17 high bytes: full block + 1-byte tail (tail uses sign-extension, NOT masking)
            (&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 0, -4128212798341382003_i64, 3989950299795791994_i64),
            // Non-zero seed
            (&[0x68, 0x65, 0x6C, 0x6C, 0x6F], 42, -4271466569069007096_i64, 2536855305735617658_i64),
        ];
        for (input, seed, expected_h1, expected_h2) in vectors {
            let (h1, h2) = hash3_x64_128(input, *seed);
            assert_eq!(
                (h1, h2),
                (*expected_h1, *expected_h2),
                "mismatch for input {:?} seed {}",
                input,
                seed
            );
        }
    }
}
