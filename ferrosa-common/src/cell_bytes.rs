//! Encapsulate cell payload ownership independently from row merge metadata.
//! Correctness: byte order, serialization and owned-buffer identity are stable.
//! Last revised: 2026-10-24
//! Last changed: Share payloads while keeping mutable I/O scratch uniquely owned.

use bytes::{Bytes, BytesMut};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Immutable cell bytes. Cloning shares the payload; merge metadata stays owned.
///
/// The retained capacity is accounting metadata only. Equality, hashing and
/// ordering depend exclusively on bytes, preserving cell tie-break semantics.
#[derive(Clone, Default, Eq)]
pub struct CellBytes {
    bytes: Bytes,
    retained_capacity: usize,
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
        &self.bytes
    }

    /// Convert to an owned vector at an API that requires one.
    ///
    /// Ownership is transferred when possible. A shared representation may
    /// need to copy here; hot read/serialization paths should borrow instead.
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes.into()
    }

    /// Recover unique I/O scratch, discarding its logical contents.
    ///
    /// Shared bytes are never copied just to overwrite them: use fresh empty
    /// scratch when a previous consumer still retains the payload.
    pub fn into_scratch(self) -> CellBytesMut {
        let mut buffer = self.bytes.try_into_mut().unwrap_or_default();
        buffer.clear();
        CellBytesMut(buffer)
    }

    /// Payload backing capacity retained by this handle, even when shared.
    pub fn retained_capacity(&self) -> usize {
        self.retained_capacity
    }

    /// Conservative heap charge, including a possible shared-owner allocation.
    ///
    /// bytes 1.11.1's largest owner (BytesMut::Shared) contains a Vec and two
    /// usize-sized fields. Charge it for every nonempty allocation, including
    /// unique buffers that have not yet allocated that owner. Shared allocations
    /// may be charged more than once; this is a bound, not physical RSS.
    pub fn retained_heap_bytes(&self) -> usize {
        if self.retained_capacity == 0 {
            return 0;
        }
        self.retained_capacity
            .saturating_add(std::mem::size_of::<Vec<u8>>() + 2 * std::mem::size_of::<usize>())
    }
}

/// Caller-owned decoding scratch; freezing publishes an immutable payload.
///
/// Keeping this type separate from `CellBytes` lets a decoder reuse unique
/// storage without ever mutating bytes retained by an earlier consumer.
#[derive(Default)]
pub struct CellBytesMut(BytesMut);

impl CellBytesMut {
    /// Resize owned scratch before an initialized read.
    pub fn resize(&mut self, len: usize, value: u8) {
        self.0.resize(len, value);
    }

    /// Clear the logical contents while retaining unique capacity.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// The scratch capacity available without growing its backing allocation.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Publish the initialized contents without copying their bytes.
    pub fn freeze(self) -> CellBytes {
        let retained_capacity = self.0.capacity();
        CellBytes {
            bytes: self.0.freeze(),
            retained_capacity,
        }
    }
}

impl AsMut<[u8]> for CellBytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
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
        let retained_capacity = bytes.capacity();
        Self {
            bytes: Bytes::from(bytes),
            retained_capacity,
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
}
