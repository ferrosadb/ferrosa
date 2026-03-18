//! Serde helper modules for types that require custom serialization.

/// Serialize/deserialize `HashMap<K, V>` where `K` is not a string
/// by converting to/from `Vec<(K, V)>`.
///
/// JSON requires object keys to be strings. When the key is a tuple
/// (or any non-string type), use this module with `#[serde(with = "...")]`.
pub mod hash_map_as_vec {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;
    use std::hash::Hash;

    pub fn serialize<S, K, V>(map: &HashMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: Serialize,
        V: Serialize,
    {
        let pairs: Vec<(&K, &V)> = map.iter().collect();
        pairs.serialize(serializer)
    }

    pub fn deserialize<'de, D, K, V>(deserializer: D) -> Result<HashMap<K, V>, D::Error>
    where
        D: Deserializer<'de>,
        K: Deserialize<'de> + Eq + Hash,
        V: Deserialize<'de>,
    {
        let pairs: Vec<(K, V)> = Vec::deserialize(deserializer)?;
        Ok(pairs.into_iter().collect())
    }
}
