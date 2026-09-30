//! Fuzz `parse_text` (T-103, FM-01, JB-D5): no panic on any bytes, and an
//! accepted input parses to identical bytes every time.
#![no_main]

use ferrosa_jsonb::{parse_text, Limits, LimitsConfig};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let limits = Limits::from_config(&LimitsConfig::default(), 64 * 1024 * 1024)
        .expect("default limits are valid");
    if let Ok(first) = parse_text(data, &limits) {
        let second = parse_text(data, &limits).expect("accepted input must re-parse");
        assert_eq!(first.bytes, second.bytes);
    }
});
