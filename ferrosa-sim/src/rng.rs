//! Deterministic, dependency-free PRNG.
//!
//! Sprint 5 simulator must be reproducible from a single `u64` seed.
//! Using `rand` would pull in transitive crates whose internal state
//! (thread-local pools, OsRng fallbacks) could leak non-determinism.
//! A single splitmix64 step is sufficient for our needs: we only use
//! it to pick election timeouts and pick which message in a tie
//! delivers first.

/// Tiny splitmix64-based PRNG.
///
/// Same seed = same stream.  No global state, no allocation.
#[derive(Clone, Debug)]
pub struct SeededRng {
    state: u64,
}

impl SeededRng {
    /// Construct a generator anchored at `seed`.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next raw 64 bits.
    pub fn next_u64(&mut self) -> u64 {
        // splitmix64 (Vigna 2014, public domain).
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform integer in `0..bound`.  Caller must ensure `bound > 0`.
    pub fn gen_range(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "bound must be positive");
        self.next_u64() % bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_same_sequence() {
        let mut a = SeededRng::new(42);
        let mut b = SeededRng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge_quickly() {
        let mut a = SeededRng::new(0);
        let mut b = SeededRng::new(1);
        // First call must differ — splitmix64 mixes hard.
        assert_ne!(a.next_u64(), b.next_u64());
    }
}
