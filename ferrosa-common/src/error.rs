//! Error types shared across all Ferrosa crates.
//!
//! [`Error`] is `#[non_exhaustive]` so new variants can be added without
//! breaking downstream crates. The [`Result`] type alias is re-exported
//! from the crate root for convenience.
//!
//! Key variants for SSTable operations:
//! - [`Error::InvalidFormat`] — file doesn't match expected structure
//! - [`Error::ChecksumMismatch`] — data corruption detected
//! - [`Error::UnsupportedCompression`] — algorithm not yet implemented

use std::fmt;

/// Errors that can occur across Ferrosa crates.
///
/// `#[non_exhaustive]` allows adding variants without semver breakage.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// I/O error (disk, network, S3).
    Io(std::io::Error),
    /// Data does not conform to expected format.
    InvalidData(String),
    /// File or structure format not recognized.
    InvalidFormat(String),
    /// Checksum verification failed.
    ChecksumMismatch { expected: u32, actual: u32 },
    /// SSTable or protocol version not supported.
    UnsupportedVersion(String),
    /// Compression algorithm not supported.
    UnsupportedCompression(String),
    /// A read could not be resolved because a snapshotted SSTable was
    /// genuinely corrupt (the view-retry bound was exhausted with the file
    /// still failing) and no healthy local source held the key.
    ///
    /// This is a *typed* signal — never string-matched — so the read
    /// coordinator can (a) treat the local replica as failed and fail over to
    /// another replica, and (b) target anti-entropy repair at the corrupt
    /// SSTable's covered token range. The corrupt SSTable has already been
    /// quarantined by the storage layer when this error is raised.
    CorruptSstable {
        /// Stable generation ID of the corrupt SSTable (file-name / pool key).
        gen: String,
        /// Smallest partition token the corrupt SSTable covered — the lower
        /// bound of the range repair must refill from a healthy replica.
        min_token: i64,
        /// Largest partition token the corrupt SSTable covered.
        max_token: i64,
    },
}

impl Error {
    /// Returns true when this error represents deliberate server-side
    /// backpressure rather than a malformed request or unexpected failure.
    pub fn is_backpressure(&self) -> bool {
        match self {
            Error::InvalidData(msg) => {
                msg.contains("local disk free space below write reserve")
                    || msg.starts_with("overloaded:")
                    || msg.contains(": overloaded:")
            }
            _ => false,
        }
    }

    /// Construct a [`Error::CorruptSstable`] from the corrupt SSTable's
    /// generation and covered token range.
    pub fn corrupt_sstable(gen: impl Into<String>, min_token: i64, max_token: i64) -> Self {
        Error::CorruptSstable {
            gen: gen.into(),
            min_token,
            max_token,
        }
    }

    /// When this error is a [`Error::CorruptSstable`], return the corrupt
    /// SSTable's covered token range `(min_token, max_token)` so the caller can
    /// target anti-entropy repair at exactly that range. `None` for every other
    /// error variant — the typed alternative to matching on a message string.
    pub fn corrupt_sstable_range(&self) -> Option<(i64, i64)> {
        match self {
            Error::CorruptSstable {
                min_token,
                max_token,
                ..
            } => Some((*min_token, *max_token)),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Error::InvalidFormat(msg) => write!(f, "invalid format: {msg}"),
            Error::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "checksum mismatch: expected {expected:#x}, got {actual:#x}"
                )
            }
            Error::UnsupportedVersion(v) => write!(f, "unsupported version: {v}"),
            Error::UnsupportedCompression(c) => write!(f, "unsupported compression: {c}"),
            Error::CorruptSstable {
                gen,
                min_token,
                max_token,
            } => write!(
                f,
                "corrupt SSTable made the read unresolvable [gen={gen} \
                 tokens=[{min_token},{max_token}]]; quarantined and scheduled \
                 for anti-entropy repair"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Result type alias used throughout Ferrosa.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("gone"));
    }

    #[test]
    fn checksum_mismatch_display() {
        let err = Error::ChecksumMismatch {
            expected: 0xDEAD,
            actual: 0xBEEF,
        };
        let s = err.to_string();
        assert!(s.contains("0xdead"));
        assert!(s.contains("0xbeef"));
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Error>();
        assert_sync::<Error>();
    }
}
