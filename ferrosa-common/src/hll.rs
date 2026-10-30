//! HyperLogLog estimator for `COUNT(ESTIMATE_DISTINCT)`.
//!
//! `COUNT(DISTINCT v)` is answered exactly by a slot table that spills to disk;
//! `COUNT(ESTIMATE_DISTINCT v)` is the other job. Its answer must survive
//! per-partition evaluation and be combined afterwards, which rules out a Bloom
//! filter: a Bloom count is a function of probe history, so OR-ing two filters
//! yields membership, not a count. A register sketch does combine — its merge is
//! register-wise maximum, which is commutative, associative and idempotent, so a
//! retried merge cannot inflate the estimate and shard order does not matter.
//!
//! The shape mirrors `specs/lean4/Ferrosa/Commons/Hll.lean`:
//!
//! - `2^p` registers (`m`), `m` a power of two, so the bucket index is a bit mask;
//! - each register holds the rank of the longest run of zero bits seen in its
//!   bucket, saturating at [`REGISTER_MAX`] (63, so it fits six bits);
//! - relative standard error [`Hll::standard_error`] is `1.04 / sqrt(m)`;
//! - small counts use linear counting below `2.5 * m`, the raw estimate above.
//!
//! # Aeneas-friendliness
//!
//! The core paths are written to be translatable to Lean and then proved against
//! the specification: registers are a `Vec<u8>` (never `HashMap`/`HashSet`), every
//! loop is a `while` with an explicit index, no iterator chain or closure appears
//! in [`Hll::insert_hash`], [`Hll::merge`] or [`Hll::estimate`], and `f64` appears
//! only in the final estimate conversion — never while accumulating registers.
//!
//! Last revised: 2026-10-29
//! Last changed: Added the estimator: register-wise max merge, `1.04/sqrt(m)`
//! standard error, 6-bit saturating registers and the small/large range split.

use std::fmt;

/// Largest value a register may hold: six bits, so `0..=63`.
///
/// Saturation is a storage invariant. Scanning the 64-bit hash from the bucket
/// boundary upward, the largest reachable rank is `64 - p + 1`, which for
/// `p >= 2` is already at most 63; the clamps in `rank_for_hash` and
/// `raise_register` keep every register inside six bits regardless.
pub const REGISTER_MAX: u8 = 63;

/// Smallest accepted precision, `m = 16` registers.
pub const MIN_PRECISION: u8 = 4;
/// Largest accepted precision, `m = 65536` registers (64 KiB of registers).
pub const MAX_PRECISION: u8 = 16;

/// Why a sketch could not be built or combined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HllError {
    /// The requested register count is not a power of two, so no bit-mask index.
    RegisterCountNotPowerOfTwo {
        /// The count the caller asked for.
        requested: usize,
    },
    /// The requested register count is a power of two but outside the accepted
    /// range for [`MIN_PRECISION`]..=[`MAX_PRECISION`].
    RegisterCountOutOfRange {
        /// The count the caller asked for.
        requested: usize,
        /// Smallest accepted count.
        min: usize,
        /// Largest accepted count.
        max: usize,
    },
    /// The requested precision is outside [`MIN_PRECISION`]..=[`MAX_PRECISION`].
    PrecisionOutOfRange {
        /// The precision the caller asked for.
        requested: u8,
    },
    /// Two sketches with different register counts cannot be merged: register
    /// `i` of one would correspond to an unrelated bucket of the other.
    MismatchedRegisterCount {
        /// Register count of the left sketch.
        left: usize,
        /// Register count of the right sketch.
        right: usize,
    },
}

impl fmt::Display for HllError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HllError::RegisterCountNotPowerOfTwo { requested } => {
                write!(f, "register count {requested} is not a power of two")
            }
            HllError::RegisterCountOutOfRange { requested, min, max } => write!(
                f,
                "register count {requested} is outside the accepted range {min}..={max}"
            ),
            HllError::PrecisionOutOfRange { requested } => write!(
                f,
                "precision {requested} is outside the accepted range {MIN_PRECISION}..={MAX_PRECISION}"
            ),
            HllError::MismatchedRegisterCount { left, right } => {
                write!(f, "cannot merge sketches with {left} and {right} registers")
            }
        }
    }
}

impl std::error::Error for HllError {}

/// FNV-1a over the bytes, then the splitmix64 finalizer.
///
/// The finalizer is what makes the low bits usable as a bucket index: FNV alone
/// leaves sequential inputs correlated in the high bits, and the rank reads
/// those. Written as two `while` loops over explicit indices so the Lean model
/// can mirror it step for step.
pub fn hash64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = FNV_OFFSET_BASIS;
    let mut i = 0usize;
    while i < bytes.len() {
        h = (h ^ bytes[i] as u64).wrapping_mul(FNV_PRIME);
        i += 1;
    }
    // splitmix64 finalizer.
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^= h >> 31;
    h
}

/// Register index for a hash: the low `p` bits.
pub fn bucket_for_hash(hash: u64, p: u8) -> usize {
    let mask = (1u64 << p) - 1;
    (hash & mask) as usize
}

/// Rank for a hash: one plus the number of zero bits from bit `p` upward,
/// saturating at [`REGISTER_MAX`].
pub fn rank_for_hash(hash: u64, p: u8) -> u8 {
    let mut rank: u8 = 1;
    let mut bit = 1u64 << p;
    while rank < REGISTER_MAX && bit != 0 && (hash & bit) == 0 {
        rank += 1;
        bit <<= 1;
    }
    rank
}

/// Raise register `index` to `rank`, leaving it alone if it is already larger.
///
/// This is the only write path to a register, and the only place saturation is
/// applied, so `registers[i] <= REGISTER_MAX` holds by construction.
fn raise_register(registers: &mut [u8], index: usize, rank: u8) {
    let saturated = if rank > REGISTER_MAX {
        REGISTER_MAX
    } else {
        rank
    };
    if saturated > registers[index] {
        registers[index] = saturated;
    }
}

/// A HyperLogLog sketch over `m = 2^p` six-bit registers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hll {
    p: u8,
    m: usize,
    registers: Vec<u8>,
}

impl Hll {
    /// Build an empty sketch with `m` registers.
    ///
    /// `m` must be a power of two in `16..=65536`; anything else is rejected
    /// rather than silently rounded, because a non-power-of-two `m` would make
    /// the bucket index a modulo instead of a mask and the Lean model would no
    /// longer match the code.
    pub fn new(m: usize) -> Result<Self, HllError> {
        // A power of two has exactly one bit set; `m == 0` has none.
        if m == 0 || (m & (m - 1)) != 0 {
            return Err(HllError::RegisterCountNotPowerOfTwo { requested: m });
        }
        let min = 1usize << MIN_PRECISION;
        let max = 1usize << MAX_PRECISION;
        if m < min || m > max {
            return Err(HllError::RegisterCountOutOfRange {
                requested: m,
                min,
                max,
            });
        }
        let mut p: u8 = 0;
        while (1usize << p) < m {
            p += 1;
        }
        let mut registers: Vec<u8> = Vec::with_capacity(m);
        let mut i = 0usize;
        while i < m {
            registers.push(0u8);
            i += 1;
        }
        Ok(Hll { p, m, registers })
    }

    /// Build an empty sketch with precision `p`, i.e. `2^p` registers.
    pub fn with_precision(p: u8) -> Result<Self, HllError> {
        if !(MIN_PRECISION..=MAX_PRECISION).contains(&p) {
            return Err(HllError::PrecisionOutOfRange { requested: p });
        }
        Hll::new(1usize << p)
    }

    /// Register count, a power of two.
    pub fn m(&self) -> usize {
        self.m
    }

    /// Precision, i.e. `log2(m)`.
    pub fn precision(&self) -> u8 {
        self.p
    }

    /// The registers, for merge-equality tests and for serialisation.
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    /// Whether no value has been inserted.
    pub fn is_empty(&self) -> bool {
        let mut i = 0usize;
        while i < self.m {
            if self.registers[i] != 0 {
                return false;
            }
            i += 1;
        }
        true
    }

    /// Relative standard error of the estimate, `1.04 / sqrt(m)`.
    ///
    /// This is the number the Lean ledger carries as a red: it is the error the
    /// estimator is designed for, not a per-query guarantee.
    pub fn standard_error(&self) -> f64 {
        1.04f64 / (self.m as f64).sqrt()
    }

    /// Insert one value.
    pub fn insert(&mut self, value: &[u8]) {
        let hash = hash64(value);
        self.insert_hash(hash);
    }

    /// Insert a pre-hashed value.
    pub fn insert_hash(&mut self, hash: u64) {
        let index = bucket_for_hash(hash, self.p);
        let rank = rank_for_hash(hash, self.p);
        raise_register(&mut self.registers, index, rank);
    }

    /// Merge `other` into `self` by exact register-wise maximum.
    ///
    /// Commutative, associative and idempotent: this is the whole reason the
    /// sketch works across shards, so it is never approximated.
    pub fn merge(&mut self, other: &Hll) -> Result<(), HllError> {
        if self.m != other.m {
            return Err(HllError::MismatchedRegisterCount {
                left: self.m,
                right: other.m,
            });
        }
        let mut i = 0usize;
        while i < self.m {
            let theirs = other.registers[i];
            if theirs > self.registers[i] {
                self.registers[i] = theirs;
            }
            i += 1;
        }
        Ok(())
    }

    /// Merge without mutating either side.
    pub fn merged(&self, other: &Hll) -> Result<Hll, HllError> {
        if self.m != other.m {
            return Err(HllError::MismatchedRegisterCount {
                left: self.m,
                right: other.m,
            });
        }
        let mut out = Hll {
            p: self.p,
            m: self.m,
            registers: Vec::with_capacity(self.m),
        };
        let mut i = 0usize;
        while i < self.m {
            let a = self.registers[i];
            let b = other.registers[i];
            if a > b {
                out.registers.push(a);
            } else {
                out.registers.push(b);
            }
            i += 1;
        }
        Ok(out)
    }

    /// The estimated number of distinct values.
    ///
    /// Linear counting below `2.5 * m` (where the raw estimate is biased high and
    /// the register histogram still carries the information), the raw estimate
    /// above, with the `2^64` correction for counts that approach the hash space.
    pub fn estimate(&self) -> f64 {
        let m = self.m;
        let mf = m as f64;
        let mut sum = 0.0f64;
        let mut zeros = 0usize;
        let mut i = 0usize;
        while i < m {
            let r = self.registers[i];
            if r == 0 {
                zeros += 1;
                sum += 1.0;
            } else {
                // 2^-r built by halving: no libm, and the Lean model can follow
                // the same loop.
                let mut term = 1.0f64;
                let mut k = 0u8;
                while k < r {
                    term *= 0.5;
                    k += 1;
                }
                sum += term;
            }
            i += 1;
        }
        let alpha = 0.7213f64 / (1.0f64 + 1.079f64 / mf);
        let raw = alpha * mf * mf / sum;
        if raw <= 2.5f64 * mf && zeros > 0 {
            let zf = zeros as f64;
            return mf * (mf / zf).ln();
        }
        // Counts that approach the 64-bit hash space: invert the saturation.
        let hash_space = 18446744073709551616.0f64;
        if raw > hash_space / 30.0f64 {
            let ratio = raw / hash_space;
            if ratio < 1.0f64 {
                return -hash_space * (1.0f64 - ratio).ln();
            }
        }
        raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch(p: u8) -> Hll {
        Hll::with_precision(p).expect("valid precision")
    }

    fn sketch_of(p: u8, lo: usize, hi: usize) -> Hll {
        let mut h = sketch(p);
        let mut i = lo;
        while i < hi {
            h.insert(&(i as u64).to_le_bytes());
            i += 1;
        }
        h
    }

    /// The hash is deterministic and a fixed function of the bytes, so the Lean
    /// side has something concrete to model.
    #[test]
    fn hash_is_deterministic_and_pinned() {
        assert_eq!(hash64(b"ferrosa"), 0x2019_e92f_8b94_c179);
        assert_eq!(hash64(b""), 0xf52a_15e9_a9b5_e89b);
        assert_ne!(hash64(b"a"), hash64(b"b"));
        assert_eq!(hash64(b"ferrosa"), hash64(b"ferrosa"));
    }

    #[test]
    fn empty_sketch_estimates_zero() {
        assert_eq!(sketch(16).estimate(), 0.0);
    }

    #[test]
    fn one_insertion_estimates_one_within_ten_percent() {
        let mut h = sketch(16);
        h.insert(b"ferrosa");
        let est = h.estimate();
        assert!(
            (est - 1.0).abs() <= 0.10,
            "estimate {est} is not within 10% of 1"
        );
    }

    #[test]
    fn estimates_lie_within_three_standard_errors() {
        let p = 16u8;
        let cases = [10usize, 100, 1000, 10000];
        let mut c = 0usize;
        while c < cases.len() {
            let n = cases[c];
            let h = sketch_of(p, 0, n);
            let est = h.estimate();
            let bound = 3.0 * h.standard_error() * n as f64;
            eprintln!(
                "hll accuracy p={p} m={} n={n} estimate={est:.2} rel_error={:.4}% bound={bound:.3}",
                h.m(),
                (est - n as f64).abs() / n as f64 * 100.0
            );
            assert!(
                (est - n as f64).abs() <= bound,
                "n={n} estimate={est} error={} exceeds 3 standard errors ({bound})",
                (est - n as f64).abs()
            );
            c += 1;
        }
    }

    /// Above `2.5 * m` the raw estimate is used; check it lands inside the same
    /// three-standard-error band well away from the transition region.
    #[test]
    fn large_range_estimates_within_three_standard_errors() {
        let p = 16u8;
        let n = 1_000_000usize;
        let h = sketch_of(p, 0, n);
        let est = h.estimate();
        let bound = 3.0 * h.standard_error() * n as f64;
        eprintln!(
            "hll accuracy p={p} m={} n={n} estimate={est:.1} rel_error={:.4}% bound={bound:.1}",
            h.m(),
            (est - n as f64).abs() / n as f64 * 100.0
        );
        assert!(
            (est - n as f64).abs() <= bound,
            "n={n} estimate={est} exceeds 3 standard errors ({bound})"
        );
    }

    /// Below `2.5 * m` the raw estimate reads ~10.9 for one occupied register of
    /// sixteen; linear counting must return ~1.03 instead.
    #[test]
    fn small_range_uses_linear_counting() {
        let mut h = sketch(4);
        h.insert_hash(0);
        let est = h.estimate();
        assert!(
            (est - 1.03).abs() < 0.05,
            "expected linear counting near 1.03, got {est}"
        );
    }

    /// Above `2.5 * m` the raw estimate is used: sixteen registers all at rank 6
    /// give `alpha * m * 2^6`.
    #[test]
    fn large_range_uses_the_raw_estimate() {
        let mut h = sketch(4);
        let m = h.m();
        let mut i = 0usize;
        while i < m {
            h.registers[i] = 6;
            i += 1;
        }
        let alpha = 0.7213f64 / (1.0f64 + 1.079f64 / m as f64);
        let expected = alpha * m as f64 * 64.0;
        let est = h.estimate();
        assert!(
            (est - expected).abs() < 1.0,
            "expected raw estimate {expected}, got {est}"
        );
    }

    #[test]
    fn merging_disjoint_sketches_estimates_the_sum() {
        let a = sketch_of(16, 0, 5000);
        let b = sketch_of(16, 5000, 10000);
        let est_a = a.estimate();
        let est_b = b.estimate();
        let union = a.merged(&b).expect("same register count");
        let est = union.estimate();
        let se = union.standard_error();
        assert!((est - 10_000.0).abs() <= 3.0 * se * 10_000.0,
            "disjoint merge estimate {est} (parts {est_a}, {est_b}) is not within 3 standard errors of 10000");
    }

    #[test]
    fn merge_is_commutative_associative_and_idempotent() {
        let a = sketch_of(14, 0, 700);
        let b = sketch_of(14, 700, 1400);
        let c = sketch_of(14, 1400, 2100);

        let ab = a.merged(&b).expect("same register count");
        let ba = b.merged(&a).expect("same register count");
        assert_eq!(ab.registers(), ba.registers(), "merge is not commutative");

        let left = ab.merged(&c).expect("same register count");
        let bc = b.merged(&c).expect("same register count");
        let right = a.merged(&bc).expect("same register count");
        assert_eq!(
            left.registers(),
            right.registers(),
            "merge is not associative"
        );

        let aa = a.merged(&a).expect("same register count");
        assert_eq!(aa.registers(), a.registers(), "merge is not idempotent");
        assert_eq!(aa.estimate(), a.estimate());
    }

    #[test]
    fn union_with_the_empty_sketch_is_unchanged() {
        let a = sketch_of(14, 0, 500);
        let empty = sketch(14);
        let left = a.merged(&empty).expect("same register count");
        let right = empty.merged(&a).expect("same register count");
        assert_eq!(left.registers(), a.registers());
        assert_eq!(right.registers(), a.registers());
        assert_eq!(left.estimate(), a.estimate());
    }

    #[test]
    fn registers_saturate_at_sixty_three() {
        // A hash of all zeros has 64 low zero bits: the longest possible run.
        let mut h = sketch(4);
        h.insert_hash(0);
        assert_eq!(h.registers()[0], 61, "rank for an all-zero hash");
        let mut i = 0usize;
        while i < h.registers().len() {
            assert!(h.registers()[i] <= REGISTER_MAX, "register {i} exceeds 63");
            i += 1;
        }

        // A long run of low zeros past the bucket bits: bucket 0, rank 37.
        let mut g = sketch(4);
        g.insert_hash(1u64 << 40);
        assert_eq!(g.registers()[0], 37);

        // The write path clamps, so six bits are never overrun.
        let mut registers = vec![0u8; 4];
        raise_register(&mut registers, 1, 200);
        assert_eq!(registers[1], REGISTER_MAX);
        raise_register(&mut registers, 1, 5);
        assert_eq!(registers[1], REGISTER_MAX, "rank must never decrease");

        let big = sketch_of(14, 0, 10_000);
        let mut i = 0usize;
        while i < big.registers().len() {
            assert!(big.registers()[i] <= REGISTER_MAX);
            i += 1;
        }
    }

    #[test]
    fn non_power_of_two_register_counts_are_rejected() {
        assert_eq!(
            Hll::new(1000),
            Err(HllError::RegisterCountNotPowerOfTwo { requested: 1000 })
        );
        assert_eq!(
            Hll::new(0),
            Err(HllError::RegisterCountNotPowerOfTwo { requested: 0 })
        );
        assert!(Hll::new(1024).is_ok());
        assert!(Hll::new(8).is_err(), "m below 16 is rejected");
        assert!(Hll::new(1 << 17).is_err(), "m above 65536 is rejected");
        assert_eq!(
            Hll::with_precision(3),
            Err(HllError::PrecisionOutOfRange { requested: 3 })
        );
        assert!(Hll::with_precision(17).is_err());
    }

    #[test]
    fn merging_sketches_of_different_sizes_is_rejected() {
        let a = sketch(14);
        let b = sketch(12);
        assert_eq!(
            a.merged(&b),
            Err(HllError::MismatchedRegisterCount {
                left: 1 << 14,
                right: 1 << 12
            })
        );
    }
}
