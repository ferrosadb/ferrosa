//! Encapsulate cell payload ownership independently from row merge metadata.
//! Correctness: bytes, serialization, aliases and owned-buffer identity are stable.
//! Last revised: 2026-10-24
//! Last changed: Inline small shared payloads without a separate Vec allocation.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Covers decoded scalar cells and short strings without growing the public handle.
// Adopted vectors always retain their existing allocation, independently of size.
const INLINE_CAPACITY: usize = 32;

enum Backing {
    Inline {
        len: usize,
        bytes: [u8; INLINE_CAPACITY],
    },
    Owned(Vec<u8>),
}

impl Backing {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { len, bytes } => &bytes[..*len],
            Self::Owned(bytes) => bytes.as_slice(),
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Inline { len, bytes } => &mut bytes[..*len],
            Self::Owned(bytes) => bytes.as_mut_slice(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Inline { .. } => INLINE_CAPACITY,
            Self::Owned(bytes) => bytes.capacity(),
        }
    }

    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Inline { len, bytes } => bytes[..len].to_vec(),
            Self::Owned(bytes) => bytes,
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Inline { len, .. } => *len = 0,
            Self::Owned(bytes) => bytes.clear(),
        }
    }

    /// Grow past the fixed inline bound once; retain a Vec's capacity thereafter.
    fn resize(&mut self, new_len: usize, value: u8) {
        match self {
            Self::Inline { len, bytes } if new_len <= INLINE_CAPACITY => {
                if new_len > *len {
                    bytes[*len..new_len].fill(value);
                }
                *len = new_len;
            }
            Self::Inline { len, bytes } => {
                let mut owned = Vec::with_capacity(new_len);
                owned.extend_from_slice(&bytes[..*len]);
                owned.resize(new_len, value);
                *self = Self::Owned(owned);
            }
            Self::Owned(bytes) => bytes.resize(new_len, value),
        }
    }
}

/// Immutable cell bytes. Cloning shares the payload; merge metadata stays owned.
///
/// Capacity is read from the owner for accounting. Equality, hashing and
/// ordering depend exclusively on bytes, preserving cell tie-break semantics.
#[derive(Clone)]
pub struct CellBytes {
    bytes: Arc<Backing>,
}

impl Eq for CellBytes {}

impl Default for CellBytes {
    fn default() -> Self {
        static EMPTY: OnceLock<Arc<Backing>> = OnceLock::new();
        Self {
            bytes: EMPTY
                .get_or_init(|| Arc::new(Backing::Owned(Vec::new())))
                .clone(),
        }
    }
}

impl Ord for CellBytes {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl PartialOrd for CellBytes {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for CellBytes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl CellBytes {
    /// Borrow the payload without copying it.
    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Convert to an owned vector at an API that requires one.
    ///
    /// A unique owned vector transfers its allocation. Inline or shared bytes
    /// copy here; hot read/serialization paths should borrow instead.
    pub fn into_vec(self) -> Vec<u8> {
        match Arc::try_unwrap(self.bytes) {
            Ok(unique) => unique.into_vec(),
            Err(shared) => shared.as_slice().to_vec(),
        }
    }

    /// Recover unique I/O scratch, discarding its logical contents.
    ///
    /// Shared bytes are never copied just to overwrite them: use fresh empty
    /// scratch when a previous consumer still retains the payload.
    pub fn into_scratch(mut self) -> CellBytesMut {
        if let Some(unique) = Arc::get_mut(&mut self.bytes) {
            unique.clear();
            CellBytesMut(Some(self.bytes))
        } else {
            CellBytesMut::default()
        }
    }

    /// Payload backing capacity retained by this handle, even when shared.
    pub fn retained_capacity(&self) -> usize {
        self.bytes.capacity()
    }

    /// Heap charge including the full backing and Arc's strong/weak counters.
    /// Shared allocations may be charged more than once; this is a conservative
    /// per-handle estimate, not physical RSS or allocator size-class accounting.
    pub fn retained_heap_bytes(&self) -> usize {
        let capacity = self.bytes.capacity();
        if capacity == 0 {
            return 0;
        }
        let owner = std::mem::size_of::<Backing>() + 2 * std::mem::size_of::<usize>();
        match self.bytes.as_ref() {
            Backing::Inline { .. } => owner,
            Backing::Owned(_) => capacity.saturating_add(owner),
        }
    }
}

/// Caller-owned decoding scratch; freezing publishes an immutable payload.
///
/// Keeping this type separate from `CellBytes` lets a decoder reuse unique
/// storage without ever mutating bytes retained by an earlier consumer.
#[derive(Default)]
pub struct CellBytesMut(Option<Arc<Backing>>);

impl CellBytesMut {
    /// Resize owned scratch before an initialized read.
    pub fn resize(&mut self, len: usize, value: u8) {
        if len == 0 && self.0.is_none() {
            return;
        }
        let bytes = self.0.get_or_insert_with(|| {
            Arc::new(Backing::Inline {
                len: 0,
                bytes: [0; INLINE_CAPACITY],
            })
        });
        Arc::get_mut(bytes)
            .expect("scratch is uniquely owned")
            .resize(len, value);
    }

    /// Clear the logical contents while retaining unique capacity.
    pub fn clear(&mut self) {
        if let Some(bytes) = self.0.as_mut() {
            Arc::get_mut(bytes)
                .expect("scratch is uniquely owned")
                .clear();
        }
    }

    /// The scratch capacity available without growing its backing allocation.
    pub fn capacity(&self) -> usize {
        self.0.as_ref().map_or(0, |bytes| bytes.capacity())
    }

    /// Publish the initialized contents without copying their bytes.
    pub fn freeze(self) -> CellBytes {
        self.0
            .map_or_else(CellBytes::default, |bytes| CellBytes { bytes })
    }
}

impl AsMut<[u8]> for CellBytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut().map_or(&mut [], |bytes| {
            Arc::get_mut(bytes)
                .expect("scratch is uniquely owned")
                .as_mut_slice()
        })
    }
}

/// Initialized storage owned by an I/O caller, resized before reading bytes.
/// This is implemented for legacy vectors and typed payload scratch so readers
/// do not require a particular mutable backing representation.
pub trait CellReadBuffer: AsMut<[u8]> {
    /// Initialize exactly `len` bytes for a subsequent read.
    fn resize_for_read(&mut self, len: usize);
}

impl CellReadBuffer for Vec<u8> {
    fn resize_for_read(&mut self, len: usize) {
        self.resize(len, 0);
    }
}

impl CellReadBuffer for CellBytesMut {
    fn resize_for_read(&mut self, len: usize) {
        self.resize(len, 0);
    }
}

impl Deref for CellBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for CellBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for CellBytes {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for CellBytes {
    fn from(bytes: Vec<u8>) -> Self {
        if bytes.capacity() == 0 {
            Self::default()
        } else {
            Self {
                bytes: Arc::new(Backing::Owned(bytes)),
            }
        }
    }
}

impl From<CellBytes> for Vec<u8> {
    fn from(bytes: CellBytes) -> Self {
        bytes.into_vec()
    }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for CellBytes {
    fn eq(&self, other: &T) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl fmt::Debug for CellBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(formatter)
    }
}

// Serialize a sequence like Vec<u8>, rather than selecting a serializer's
// distinct byte-string representation. This contract survives the backing swap.
impl Serialize for CellBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CellBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<u8>::deserialize(deserializer).map(Self::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adopting_and_returning_a_unique_vector_keeps_its_allocation() {
        let input = vec![3, 1, 4, 1, 5];
        let address = input.as_ptr();
        let payload = CellBytes::from(input);
        assert_eq!(payload.as_ptr(), address);
        assert_eq!(payload.as_slice(), [3, 1, 4, 1, 5]);
        let returned = payload.into_vec();
        assert_eq!(returned.as_ptr(), address);
    }

    #[test]
    fn adopting_a_small_reserved_vector_retains_pointer_and_spare_capacity() {
        let mut input = Vec::with_capacity(16);
        input.extend_from_slice(b"tiny");
        let address = input.as_ptr();
        let capacity = input.capacity();
        let payload = CellBytes::from(input);
        assert_eq!(payload.as_ptr(), address);
        assert_eq!(payload.retained_capacity(), capacity);
        let mut scratch = payload.into_scratch();
        assert_eq!(scratch.capacity(), capacity);
        scratch.resize(8, 9);
        let returned = scratch.freeze().into_vec();
        assert_eq!(returned.as_ptr(), address);
        assert_eq!(returned.capacity(), capacity);
        assert_eq!(returned, [9; 8]);
    }

    #[test]
    fn adopting_and_returning_a_unique_large_vector_keeps_its_allocation() {
        let input: Vec<u8> = (0..128).collect();
        let address = input.as_ptr();
        let payload = CellBytes::from(input);
        assert_eq!(payload.as_ptr(), address);
        assert_eq!(payload.as_slice(), (0..128).collect::<Vec<u8>>());
        let returned = payload.into_vec();
        assert_eq!(returned.as_ptr(), address);
    }

    #[test]
    fn ordering_debug_and_serialization_match_owned_bytes() {
        let a = CellBytes::from(vec![0, 128, 255]);
        let b = CellBytes::from(vec![1, 0]);
        assert!(a < b);
        assert_eq!(format!("{a:?}"), "[0, 128, 255]");
        let encoded = serde_json::to_string(&a).unwrap();
        assert_eq!(
            encoded,
            serde_json::to_string(&vec![0u8, 128, 255]).unwrap()
        );
        assert_eq!(serde_json::from_str::<CellBytes>(&encoded).unwrap(), a);
    }

    #[test]
    fn shared_payload_keeps_bytes_and_capacity_out_of_value_ordering() {
        let mut spare = Vec::with_capacity(4096);
        spare.extend_from_slice(b"payload");
        let a = CellBytes::from(spare);
        let b = CellBytes::from(b"payload".to_vec());
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), std::cmp::Ordering::Equal);
        use std::hash::{Hash, Hasher};
        let hash = |value: &CellBytes| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
        assert!(a.retained_capacity() >= 4096);
        let retained = a.clone();
        assert_eq!(
            a.as_ptr(),
            retained.as_ptr(),
            "clone copied immutable payload"
        );
        let mut scratch = a.into_scratch();
        scratch.clear();
        scratch.resize(7, b'x');
        assert_eq!(scratch.freeze().as_slice(), b"xxxxxxx");
        assert_eq!(retained.as_slice(), b"payload");
    }

    #[test]
    fn decoder_scratch_reuses_unique_capacity_across_freezes() {
        let mut input = Vec::with_capacity(4096);
        input.extend_from_slice(b"payload");
        let address = input.as_ptr();
        let mut frozen = CellBytes::from(input);
        for _ in 0..64 {
            let mut scratch = frozen.into_scratch();
            assert!(scratch.capacity() >= 4096);
            scratch.clear();
            scratch.resize(7, b'x');
            frozen = scratch.freeze();
            assert_eq!(frozen.as_ptr(), address);
            assert_eq!(frozen.as_slice(), b"xxxxxxx");
        }
    }

    #[test]
    fn small_scratch_reuses_its_shared_owner_without_a_vector_buffer() {
        let mut scratch = CellBytesMut::default();
        scratch.resize(16, 7);
        let mut frozen = scratch.freeze();
        let address = frozen.as_ptr();
        assert_eq!(frozen.retained_capacity(), INLINE_CAPACITY);
        for _ in 0..64 {
            let mut scratch = frozen.into_scratch();
            scratch.resize(16, 9);
            frozen = scratch.freeze();
            assert_eq!(frozen.as_ptr(), address);
            assert_eq!(frozen.as_slice(), [9; 16]);
        }
        let retained = frozen.clone();
        assert_eq!(retained.as_ptr(), address, "small clones share their owner");
        let mut scratch = frozen.into_scratch();
        assert_eq!(
            scratch.capacity(),
            0,
            "shared bytes are not copied for scratch"
        );
        scratch.resize(16, 3);
        assert_eq!(scratch.freeze().as_slice(), [3; 16]);
        assert_eq!(
            retained.as_slice(),
            [9; 16],
            "scratch cannot mutate an alias"
        );
    }

    #[test]
    fn scratch_growth_and_regrowth_initialize_bytes_without_losing_contents() {
        let mut scratch = CellBytesMut::default();
        scratch.resize(4, 1);
        scratch.as_mut()[1] = 2;
        scratch.resize(2, 0);
        scratch.resize(4, 3);
        assert_eq!(scratch.as_mut(), [1, 2, 3, 3]);
        scratch.resize(INLINE_CAPACITY + 1, 5);
        assert_eq!(&scratch.as_mut()[..4], [1, 2, 3, 3]);
        assert!(scratch.as_mut()[4..].iter().all(|byte| *byte == 5));
        let frozen = scratch.freeze();
        let address = frozen.as_ptr();
        let capacity = frozen.retained_capacity();
        let mut scratch = frozen.into_scratch();
        scratch.resize(3, 8);
        let frozen = scratch.freeze();
        assert_eq!(
            frozen.as_ptr(),
            address,
            "promoted Vec is retained after shrinking"
        );
        assert_eq!(frozen.retained_capacity(), capacity);
        assert_eq!(frozen.as_slice(), [8; 3]);
    }

    #[test]
    fn owned_capacity_is_preserved_and_charged_even_for_an_empty_payload() {
        let bytes = Vec::with_capacity(4096);
        let address = bytes.as_ptr();
        let payload = CellBytes::from(bytes);
        assert!(payload.is_empty());
        assert_eq!(payload.as_ptr(), address);
        assert!(payload.retained_capacity() >= 4096);
        assert!(payload.retained_heap_bytes() >= 4096 + std::mem::size_of::<Backing>());
        let returned = payload.into_vec();
        assert_eq!(returned.as_ptr(), address);
        assert_eq!(CellBytes::default().retained_heap_bytes(), 0);
    }

    #[test]
    fn serialization_and_owned_conversion_match_across_the_inline_boundary() {
        for len in [
            0,
            1,
            INLINE_CAPACITY - 1,
            INLINE_CAPACITY,
            INLINE_CAPACITY + 1,
            1024,
        ] {
            let expected: Vec<u8> = (0..len).map(|idx| idx as u8).collect();
            let mut scratch = CellBytesMut::default();
            scratch.resize(len, 0);
            scratch.as_mut().copy_from_slice(&expected);
            let payload = scratch.freeze();
            assert_eq!(payload.as_slice(), expected);
            let retained = payload.clone();
            assert_eq!(retained.as_ptr(), payload.as_ptr());
            let encoded = serde_json::to_vec(&payload).unwrap();
            assert_eq!(encoded, serde_json::to_vec(&expected).unwrap());
            let decoded: CellBytes = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded, payload);
            assert_eq!(payload.into_vec(), expected);
            assert_eq!(retained.as_slice(), expected);
            if len > 0 {
                assert!(retained.retained_heap_bytes() >= retained.retained_capacity());
            }
        }
    }
}
