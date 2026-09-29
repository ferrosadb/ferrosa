---
crate: ferrosa-jsonb-conformance
doc: fmea
last_updated: 2026-09-28
---

# ferrosa-jsonb-conformance — FMEA

RPN = Severity x Occurrence x Detection.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| JB-CONF-1 | The encoder misreads a Variant spec field and its own tests agree (the T-102 bit-5 bug) | Stored cells unreadable by every other Variant reader | 9 | 3 | 2 | 54 | Direction A decodes every corpus cell with parquet-variant; `header_layout.rs` pins the bits. Red evidence: bit-5 shift fails 6 tests. |
| JB-CONF-2 | The validator accepts bytes upstream considers invalid, or misreads a header | Corrupt cell served as data | 9 | 2 | 3 | 54 | Direction B feeds upstream-built cells; `validator_reads_offset_width_from_bits_7_6` sweeps widths 1-4. |
| JB-CONF-3 | The oracle silently checks nothing (an empty corpus, a swallowed skip) | False assurance | 8 | 2 | 3 | 48 | Tests assert at least 200 golden cases ran; every skipped case is proven to hold a bigdecimal. |
| JB-CONF-4 | The expected value is derived from ferrosa itself | The oracle repeats the misreading | 9 | 2 | 4 | 72 | Model built from source lexemes by an independent parser; splitter and kind chooser written from the spec. Review rule: no expectation may call `ferrosa_encode` unless it is the byte-identity check. |
| JB-CONF-5 | Someone loosens the validator to pass a non-canonical row | Two encodings per value; equality and dedup by bytes break | 8 | 3 | 2 | 48 | Rows are pinned to typed faults in `non_canonical.rs`; the README states the rows are intended refusals. |
| JB-CONF-6 | A parquet-variant upgrade changes behaviour unnoticed | Spurious or missing failures | 5 | 3 | 2 | 30 | Exact `=60.0.0` pin. |
| JB-CONF-7 | arrow-rs leaks into a production crate's graph | Violates D5a, bloats the leaf crate | 8 | 2 | 2 | 32 | Dev-dependency of a `publish = false` crate with no dependents; `guard-arrow-free.sh` in CI. |
| JB-CONF-8 | Depth beyond 128 is untested against upstream | Deep values unverified | 4 | 3 | 3 | 36 | Accepted: upstream refuses them; the divergence is pinned by a test. |
