#![no_main]

use ferrosa_sstable::checksum::{parse_digest, ChunkCrcTable};
use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 128 * 1024;

fuzz_target!(|input: &[u8]| {
    // Input layout is a little-endian u32 CRC.db length, CRC.db bytes, then
    // Digest.crc32 bytes. Both parsers consume only the bounded slices.
    if input.len() > MAX_INPUT_BYTES || input.len() < 4 {
        return;
    }
    let crc_len = u32::from_le_bytes([input[0], input[1], input[2], input[3]]) as usize;
    let Some(digest_start) = crc_len.checked_add(4) else {
        return;
    };
    if digest_start > input.len() {
        return;
    }
    let _ = ChunkCrcTable::parse(&input[4..digest_start]);
    let _ = parse_digest(&input[digest_start..]);
});
