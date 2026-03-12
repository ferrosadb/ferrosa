//! Property-based tests for ferrosa-common types.

use ferrosa_common::murmur3::hash3_x64_128;
use ferrosa_common::{DecoratedKey, PartitionKey, Token};
use proptest::prelude::*;

proptest! {
    /// Murmur3 is deterministic: same input always produces same output.
    #[test]
    fn murmur3_deterministic(data: Vec<u8>, seed: i64) {
        let a = hash3_x64_128(&data, seed);
        let b = hash3_x64_128(&data, seed);
        prop_assert_eq!(a, b);
    }

    /// Token::from_key is deterministic.
    #[test]
    fn token_from_key_deterministic(key: Vec<u8>) {
        let a = Token::from_key(&key);
        let b = Token::from_key(&key);
        prop_assert_eq!(a, b);
    }

    /// DecoratedKey ordering is consistent: token-first, then key bytes.
    #[test]
    fn decorated_key_ordering_consistent(
        key_a: Vec<u8>,
        key_b: Vec<u8>,
    ) {
        let dk_a = DecoratedKey::new(PartitionKey::new(key_a));
        let dk_b = DecoratedKey::new(PartitionKey::new(key_b));

        // Ordering must be total and consistent
        let cmp_ab = dk_a.cmp(&dk_b);
        let cmp_ba = dk_b.cmp(&dk_a);
        prop_assert_eq!(cmp_ab, cmp_ba.reverse());

        // Token comparison drives the primary order
        if dk_a.token != dk_b.token {
            prop_assert_eq!(cmp_ab, dk_a.token.cmp(&dk_b.token));
        }
    }

    /// DecoratedKey::filter_hash h1 always equals the token value.
    #[test]
    fn filter_hash_h1_equals_token(key: Vec<u8>) {
        let dk = DecoratedKey::new(PartitionKey::new(key));
        let (h1, _h2) = dk.filter_hash();
        prop_assert_eq!(h1, dk.token.0);
    }
}
