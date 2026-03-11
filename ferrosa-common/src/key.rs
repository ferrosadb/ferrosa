use std::cmp::Ordering;

use crate::Token;

/// Raw partition key bytes.
///
/// For simple partition keys, this is the CQL-serialized value of the
/// partition column. For composite keys, it's the length-prefixed
/// concatenation of component values (serialization handled by ferrosa-cql).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey(Vec<u8>);

impl PartitionKey {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Ord for PartitionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for PartitionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Vec<u8>> for PartitionKey {
    fn from(data: Vec<u8>) -> Self {
        Self(data)
    }
}

impl From<&[u8]> for PartitionKey {
    fn from(data: &[u8]) -> Self {
        Self(data.to_vec())
    }
}

/// A partition key decorated with its token (position on the hash ring).
///
/// The token is computed once via Murmur3 and cached. DecoratedKey is the
/// primary key type used throughout the storage engine — it determines
/// both which node owns the partition and where it lives within SSTables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecoratedKey {
    pub token: Token,
    pub key: PartitionKey,
}

impl DecoratedKey {
    /// Create a DecoratedKey by hashing the partition key with Murmur3.
    pub fn new(key: PartitionKey) -> Self {
        let token = Token::from_key(key.as_bytes());
        Self { token, key }
    }

    /// Returns both Murmur3 hash values `(h1, h2)`.
    /// `h1` is the token. Both are needed for Bloom filter hashing.
    pub fn filter_hash(&self) -> (i64, i64) {
        crate::murmur3::hash3_x64_128(self.key.as_bytes(), 0)
    }
}

impl Ord for DecoratedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.token
            .cmp(&other.token)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for DecoratedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_from_bytes() {
        let pk = PartitionKey::from(b"hello".as_slice());
        assert_eq!(pk.as_bytes(), b"hello");
        assert_eq!(pk.len(), 5);
        assert!(!pk.is_empty());
    }

    #[test]
    fn partition_key_empty() {
        let pk = PartitionKey::new(vec![]);
        assert!(pk.is_empty());
        assert_eq!(pk.len(), 0);
    }

    #[test]
    fn decorated_key_computes_token() {
        let dk = DecoratedKey::new(PartitionKey::from(b"hello".as_slice()));
        let expected_token = Token::from_key(b"hello");
        assert_eq!(dk.token, expected_token);
    }

    #[test]
    fn decorated_key_ordering_by_token() {
        let a = DecoratedKey::new(PartitionKey::from(b"aaa".as_slice()));
        let b = DecoratedKey::new(PartitionKey::from(b"bbb".as_slice()));
        // Ordering is by token, not by key bytes
        if a.token < b.token {
            assert!(a < b);
        } else {
            assert!(a > b);
        }
    }

    #[test]
    fn decorated_key_filter_hash_matches_token() {
        let dk = DecoratedKey::new(PartitionKey::from(b"test".as_slice()));
        let (h1, _h2) = dk.filter_hash();
        assert_eq!(h1, dk.token.0);
    }

    #[test]
    fn decorated_key_same_token_breaks_tie_by_key() {
        // Construct two keys with the same token (unlikely but must handle)
        let dk1 = DecoratedKey {
            token: Token(42),
            key: PartitionKey::from(b"aaa".as_slice()),
        };
        let dk2 = DecoratedKey {
            token: Token(42),
            key: PartitionKey::from(b"bbb".as_slice()),
        };
        assert!(dk1 < dk2); // tie-broken by key bytes
    }
}
