/// Position on the Murmur3 hash ring.
///
/// Wraps an `i64` produced by `murmur3::hash3_x64_128`. The full range
/// `i64::MIN..=i64::MAX` is used, matching Cassandra's `LongToken`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(pub i64);

impl Token {
    pub const MIN: Token = Token(i64::MIN);
    pub const MAX: Token = Token(i64::MAX);

    /// Compute the token for a raw partition key by hashing with Murmur3.
    pub fn from_key(key: &[u8]) -> Token {
        let (h1, _) = crate::murmur3::hash3_x64_128(key, 0);
        Token(h1)
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_and_max() {
        assert_eq!(Token::MIN.0, i64::MIN);
        assert_eq!(Token::MAX.0, i64::MAX);
        assert!(Token::MIN < Token::MAX);
    }

    #[test]
    fn from_key_deterministic() {
        let a = Token::from_key(b"hello");
        let b = Token::from_key(b"hello");
        assert_eq!(a, b);
    }

    #[test]
    fn from_key_different_keys_differ() {
        let a = Token::from_key(b"hello");
        let b = Token::from_key(b"world");
        assert_ne!(a, b);
    }

    #[test]
    fn from_empty_key() {
        let t = Token::from_key(b"");
        assert_eq!(t, Token(0));
    }

    #[test]
    fn ordering_matches_i64() {
        let a = Token(-100);
        let b = Token(0);
        let c = Token(100);
        assert!(a < b);
        assert!(b < c);
    }
}
