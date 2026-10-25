//! Encapsulate cell payload ownership independently from row merge metadata.
//! Correctness: byte order, serialization and owned-buffer identity are stable.
//! Last revised: 2026-10-24
//! Last changed: Added the immutable payload seam before changing cell storage.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Immutable cell payload bytes, with an explicit owned-vector boundary.
///
/// Callers read slices; they do not depend on the backing allocation type. This
/// first, additive step retains the owned representation. Sharing can then be a
/// local representation change without exposing mutation through `DerefMut`.
#[derive(Default, Eq, Hash, Ord, PartialOrd)]
pub struct CellBytes(Vec<u8>);

impl Clone for CellBytes {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }

    fn clone_from(&mut self, source: &Self) {
        self.0.clone_from(&source.0);
    }
}

impl CellBytes {
    /// Borrow the payload without copying it.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Convert to an owned vector at an API that requires one.
    ///
    /// Ownership is transferred when possible. A shared representation may
    /// need to copy here; hot read/serialization paths should borrow instead.
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    /// Recover caller-owned decoding scratch without exposing mutable payloads.
    pub fn into_mut(self) -> CellBytesMut {
        CellBytesMut(self.0)
    }

    /// Backing capacity retained by this payload, for spill/memory accounting.
    pub fn retained_capacity(&self) -> usize {
        self.0.capacity()
    }
}

/// Caller-owned decoding scratch; freezing publishes an immutable payload.
///
/// Keeping this type separate from `CellBytes` lets a decoder reuse unique
/// storage without ever mutating bytes retained by an earlier consumer.
#[derive(Default)]
pub struct CellBytesMut(Vec<u8>);

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
        CellBytes(self.0)
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
        Self(bytes)
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
    fn decoder_scratch_reuses_unique_capacity_across_freezes() {
        let mut input = Vec::with_capacity(4096);
        input.extend_from_slice(b"payload");
        let address = input.as_ptr();
        let mut frozen = CellBytes::from(input);
        for _ in 0..64 {
            let mut scratch = frozen.into_mut();
            assert!(scratch.capacity() >= 4096);
            scratch.clear();
            scratch.resize(7, b'x');
            frozen = scratch.freeze();
            assert_eq!(frozen.as_ptr(), address);
            assert_eq!(frozen.as_slice(), b"xxxxxxx");
        }
    }
}
