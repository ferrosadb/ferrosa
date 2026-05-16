//! Positional I/O traits and file-system implementations.
//!
//! [`ReadAt`] and [`WriteAt`] decouple SSTable logic from the backing store.
//! This module provides file-system implementations ([`FileReadAt`] and
//! [`FileWriteAt`]) using `pread`/`pwrite` on Unix. The S3 implementation
//! lives in `ferrosa-storage`.
//!
//! # Design
//!
//! Positional I/O avoids shared file offset state, making it safe to read
//! from multiple threads without external synchronization (each call specifies
//! its offset). This matches SSTable access patterns where multiple index
//! lookups happen concurrently.

use ferrosa_common::Result;

/// Positional read — read bytes at an offset without seeking.
///
/// ```no_run
/// use ferrosa_sstable::io::ReadAt;
/// use ferrosa_common::Result;
///
/// fn read_header(reader: &impl ReadAt) -> Result<[u8; 4]> {
///     let mut buf = [0u8; 4];
///     reader.read_at(&mut buf, 0)?;
///     Ok(buf)
/// }
/// ```
pub trait ReadAt {
    /// Read bytes into `buf` starting at `offset`.
    /// Returns the number of bytes read (may be less than `buf.len()` at EOF).
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Returns the total length of the underlying data.
    fn len(&self) -> Result<u64>;

    /// Returns true if the underlying data is empty.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Read exactly `buf.len()` bytes at `offset`, or return an error.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let n = self.read_at(buf, offset)?;
        if n != buf.len() {
            return Err(ferrosa_common::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("read_exact_at: wanted {} bytes, got {}", buf.len(), n),
            )));
        }
        Ok(())
    }
}

/// Positional write — write bytes at an offset.
pub trait WriteAt {
    /// Write `buf` starting at `offset`.
    /// Returns the number of bytes written.
    fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<usize>;

    /// Flush any buffered data to the underlying store.
    fn flush(&mut self) -> Result<()>;

    /// Write all bytes in `buf` at `offset`, or return an error.
    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        let n = self.write_at(buf, offset)?;
        if n != buf.len() {
            return Err(ferrosa_common::Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("write_all_at: wanted {} bytes, wrote {}", buf.len(), n),
            )));
        }
        Ok(())
    }
}

/// File-system implementation of [`ReadAt`] using `pread` on Unix.
pub struct FileReadAt {
    path: std::path::PathBuf,
}

impl FileReadAt {
    /// Open a file for positional reading.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        // Validate that the file exists and is readable, but don't retain the
        // descriptor. SSTable readers live for as long as an SSTable is present
        // in a table store; pinning Data.db/Partitions.db/Rows.db handles for
        // every idle SSTable exhausts RLIMIT_NOFILE during startup/compaction
        // backlogs and in parallel engine tests. Reads reopen briefly per call.
        std::fs::File::open(&path)?;
        Ok(Self { path })
    }
}

impl ReadAt for FileReadAt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let file = std::fs::File::open(&self.path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            Ok(file.read_at(buf, offset)?)
        }
        #[cfg(not(unix))]
        {
            // Fallback: seek + read (not thread-safe, but functional)
            use std::io::{Read, Seek, SeekFrom};
            let mut file = file;
            file.seek(SeekFrom::Start(offset))?;
            Ok(file.read(buf)?)
        }
    }

    fn len(&self) -> Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }
}

/// File-system implementation of [`WriteAt`] using `pwrite` on Unix.
pub struct FileWriteAt {
    file: std::fs::File,
}

impl FileWriteAt {
    /// Create a new file for positional writing.
    pub fn create(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self { file })
    }
}

impl WriteAt for FileWriteAt {
    fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            Ok(self.file.write_at(buf, offset)?)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            self.file.seek(SeekFrom::Start(offset))?;
            Ok(self.file.write(buf)?)
        }
    }

    fn flush(&mut self) -> Result<()> {
        use std::io::Write;
        Ok(self.file.flush()?)
    }
}

/// In-memory implementation of [`ReadAt`] for testing.
impl ReadAt for &[u8] {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset = offset as usize;
        let slice_len = <[u8]>::len(self);
        if offset >= slice_len {
            return Ok(0);
        }
        let available = &self[offset..];
        let n = buf.len().min(available.len());
        buf[..n].copy_from_slice(&available[..n]);
        Ok(n)
    }

    fn len(&self) -> Result<u64> {
        Ok(<[u8]>::len(self) as u64)
    }
}

/// In-memory implementation of [`ReadAt`] for testing.
impl ReadAt for Vec<u8> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.as_slice().read_at(buf, offset)
    }

    fn len(&self) -> Result<u64> {
        Ok(Vec::len(self) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_read_at_basic() {
        let data: &[u8] = b"hello world";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn slice_read_at_offset() {
        let data: &[u8] = b"hello world";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 6).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn slice_read_at_past_eof() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 100).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn slice_read_at_partial() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let n = data.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
    }

    #[test]
    fn slice_len() {
        let data: &[u8] = b"hello";
        assert_eq!(ReadAt::len(&data).unwrap(), 5);
    }

    #[test]
    fn slice_is_empty() {
        let empty: &[u8] = b"";
        assert!(ReadAt::is_empty(&empty).unwrap());
        let nonempty: &[u8] = b"x";
        assert!(!ReadAt::is_empty(&nonempty).unwrap());
    }

    #[test]
    fn read_exact_at_success() {
        let data: &[u8] = b"hello";
        let mut buf = [0u8; 5];
        data.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn read_exact_at_eof_error() {
        let data: &[u8] = b"hi";
        let mut buf = [0u8; 5];
        let err = data.read_exact_at(&mut buf, 0).unwrap_err();
        assert!(err.to_string().contains("wanted 5 bytes, got 2"));
    }

    #[test]
    fn file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let data = b"hello world from ferrosa";
        {
            let mut writer = FileWriteAt::create(&path).unwrap();
            writer.write_all_at(data, 0).unwrap();
            writer.flush().unwrap();
        }

        let reader = FileReadAt::open(&path).unwrap();
        assert_eq!(reader.len().unwrap(), data.len() as u64);

        let mut buf = vec![0u8; data.len()];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, data);

        // Partial read at offset
        let mut buf2 = [0u8; 5];
        reader.read_exact_at(&mut buf2, 6).unwrap();
        assert_eq!(&buf2, b"world");
    }

    #[test]
    fn file_write_at_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("offset.dat");

        let mut writer = FileWriteAt::create(&path).unwrap();
        // Write "hello" at offset 0
        writer.write_all_at(b"hello", 0).unwrap();
        // Write "world" at offset 10
        writer.write_all_at(b"world", 10).unwrap();
        writer.flush().unwrap();

        let reader = FileReadAt::open(&path).unwrap();
        assert_eq!(reader.len().unwrap(), 15);

        let mut buf = [0u8; 5];
        reader.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");

        reader.read_exact_at(&mut buf, 10).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn many_file_readers_do_not_pin_idle_file_descriptors() {
        fn open_fd_count() -> usize {
            std::fs::read_dir("/proc/self/fd").unwrap().count()
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sstable-component.db");
        std::fs::write(&path, b"ferrosa component bytes").unwrap();

        let baseline = open_fd_count();
        let readers: Vec<_> = (0..256).map(|_| FileReadAt::open(&path).unwrap()).collect();
        let after_open = open_fd_count();

        assert!(
            after_open <= baseline + 8,
            "idle FileReadAt instances must not pin one fd each: baseline={baseline}, after_open={after_open}"
        );

        for reader in &readers {
            let mut buf = [0u8; 7];
            reader.read_exact_at(&mut buf, 0).unwrap();
            assert_eq!(&buf, b"ferrosa");
        }

        let after_reads = open_fd_count();
        assert!(
            after_reads <= baseline + 8,
            "read_at must close transient descriptors promptly: baseline={baseline}, after_reads={after_reads}"
        );
    }
}
