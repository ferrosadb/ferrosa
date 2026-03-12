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
