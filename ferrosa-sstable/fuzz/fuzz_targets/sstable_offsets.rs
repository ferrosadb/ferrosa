#![no_main]

use std::sync::OnceLock;

use ferrosa_common::Result;
use ferrosa_sstable::compression::CompressionInfo;
use ferrosa_sstable::io::ReadAt;
use ferrosa_sstable::reader::fuzz_read_compressed_chunk;
use ferrosa_sstable::Compression;
use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 128 * 1024;
const MAX_CHUNK_BYTES: usize = 64 * 1024;
const GOLDEN_INFO: &[u8] =
    include_bytes!("../../tests/golden/random_lz4_chunk4096_file_backed_4/CompressionInfo.db");
const GOLDEN_DATA: &[u8] =
    include_bytes!("../../tests/golden/random_lz4_chunk4096_file_backed_4/Data.db");

struct MemoryReadAt<'a>(&'a [u8]);

impl ReadAt for MemoryReadAt<'_> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(0);
        };
        let Some(remaining) = self.0.get(offset..) else {
            return Ok(0);
        };
        let count = buf.len().min(remaining.len());
        buf[..count].copy_from_slice(&remaining[..count]);
        Ok(count)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.0.len() as u64)
    }
}

fn golden_chunks() -> &'static [Vec<u8>] {
    static CHUNKS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    CHUNKS.get_or_init(|| {
        let info = CompressionInfo::read(GOLDEN_INFO).expect("checked-in golden metadata parses");
        let data = MemoryReadAt(GOLDEN_DATA);
        (0..info.chunk_offsets.len())
            .map(|index| {
                fuzz_read_compressed_chunk(&data, &info, GOLDEN_DATA.len() as u64, index)
                    .expect("checked-in golden chunk decodes")
            })
            .collect()
    })
}

fuzz_target!(|input: &[u8]| {
    // Input layout is a little-endian u32 CompressionInfo length, followed by
    // CompressionInfo.db and Data.db. The fixed-size cap keeps even malformed
    // corpus entries bounded before parsing or decoding.
    if input.len() > MAX_INPUT_BYTES || input.len() < 4 {
        return;
    }
    let info_len = u32::from_le_bytes([input[0], input[1], input[2], input[3]]) as usize;
    let Some(data_start) = info_len.checked_add(4) else {
        return;
    };
    if data_start > input.len() {
        return;
    }
    let info_bytes = &input[4..data_start];
    let data_bytes = &input[data_start..];
    let Ok(info) = CompressionInfo::read(info_bytes) else {
        return;
    };

    // The seed is LZ4. Keep the fuzz run bounded if arbitrary metadata tries
    // to request a huge chunk or an unbounded number of chunks.
    if info.compression != Compression::Lz4
        || info.chunk_length > MAX_CHUNK_BYTES
        || info.chunk_offsets.len() > golden_chunks().len()
    {
        return;
    }

    let data = MemoryReadAt(data_bytes);
    for (index, expected) in golden_chunks()
        .iter()
        .take(info.chunk_offsets.len())
        .enumerate()
    {
        match fuzz_read_compressed_chunk(&data, &info, data_bytes.len() as u64, index) {
            Ok(chunk) => assert_eq!(chunk, *expected, "accepted offsets returned wrong bytes"),
            Err(_) => return,
        }
    }
});
