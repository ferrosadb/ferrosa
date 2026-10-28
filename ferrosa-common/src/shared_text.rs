//! Immutable UTF-8 text that can retain shared storage bytes without copying.
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use bytes::Bytes;
use bytestring::ByteString;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::cell_bytes::CellBytes;

/// Validated text plus retained backing capacity for ownership accounting.
/// Equality, ordering, hashing and serde depend only on the text bytes.
#[derive(Clone, Default)]
pub struct SharedText {
    value: ByteString,
    retained_capacity: usize,
}

impl SharedText {
    /// Validate once and retain the original immutable owner, including siblings.
    pub fn from_cell(bytes: CellBytes) -> Result<Self, std::str::Utf8Error> {
        let retained_capacity = bytes.retained_capacity();
        ByteString::try_from(Bytes::from_owner(bytes)).map(|value| Self {
            value,
            retained_capacity,
        })
    }
    pub fn as_str(&self) -> &str {
        self.value.as_ref()
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
    pub fn retained_capacity(&self) -> usize {
        self.retained_capacity
    }
}
impl Deref for SharedText {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl AsRef<str> for SharedText {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl Borrow<str> for SharedText {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
impl<T: AsRef<str> + ?Sized> PartialEq<T> for SharedText {
    fn eq(&self, other: &T) -> bool {
        self.as_str() == other.as_ref()
    }
}
impl Eq for SharedText {}
impl PartialOrd for SharedText {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SharedText {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
impl Hash for SharedText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}
impl PartialEq<SharedText> for String {
    fn eq(&self, other: &SharedText) -> bool {
        self.as_str() == other.as_str()
    }
}
impl From<String> for SharedText {
    fn from(value: String) -> Self {
        let retained_capacity = value.capacity();
        Self {
            value: value.into(),
            retained_capacity,
        }
    }
}
impl From<&str> for SharedText {
    fn from(value: &str) -> Self {
        value.to_owned().into()
    }
}
impl From<SharedText> for String {
    fn from(value: SharedText) -> Self {
        value.value.into()
    }
}
impl fmt::Display for SharedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}
impl fmt::Debug for SharedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}
impl Serialize for SharedText {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for SharedText {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CqlValue;
    use std::hash::{Hash, Hasher};

    #[test]
    fn shared_text_retains_cell_and_clone_allocations() {
        let bytes = CellBytes::from("héllo".repeat(1000).into_bytes());
        let original = bytes.as_ref().as_ptr();
        let sibling = bytes.clone();
        let text = SharedText::from_cell(bytes).unwrap();
        let clone = text.clone();
        assert_eq!(text.as_bytes().as_ptr(), original);
        assert_eq!(clone.as_bytes().as_ptr(), original);
        assert_eq!(text.as_str(), "héllo".repeat(1000));
        drop(text);
        assert_eq!(clone.as_bytes(), sibling.as_ref());
    }

    #[test]
    fn shared_text_validates_utf8_and_keeps_string_hash_semantics() {
        assert!(SharedText::from_cell(CellBytes::from(vec![0xff])).is_err());
        let text: SharedText = "abc".into();
        let mut shared_hash = std::collections::hash_map::DefaultHasher::new();
        let mut string_hash = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut shared_hash);
        String::from("abc").hash(&mut string_hash);
        assert_eq!(shared_hash.finish(), string_hash.finish());
        let greater = SharedText::from("abd");
        assert!(text < greater);
        assert_eq!(text, "abc");
    }

    #[test]
    fn retained_capacity_never_changes_value_or_borrowed_lookup_semantics() {
        let small = SharedText::from("abc");
        let mut reserved = String::with_capacity(4096);
        reserved.push_str("abc");
        let large = SharedText::from(reserved);
        assert_ne!(small.retained_capacity(), large.retained_capacity());
        assert_eq!(small, large);
        assert_eq!(small.cmp(&large), std::cmp::Ordering::Equal);
        let hash = |value: &SharedText| {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut state);
            state.finish()
        };
        assert_eq!(hash(&small), hash(&large));
        assert_eq!(
            serde_json::to_string(&small).unwrap(),
            serde_json::to_string(&large).unwrap()
        );
        let values = std::collections::HashMap::from([(large, 42)]);
        assert_eq!(values.get("abc"), Some(&42));
    }

    #[test]
    fn immutable_cql_values_preserve_legacy_json() {
        for json in [
            r#"{"Text":"hé\n\""}"#,
            r#"{"Ascii":"abc"}"#,
            r#"{"Blob":[0,128,255]}"#,
        ] {
            let value: CqlValue = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&value).unwrap(), json);
        }
    }
}
