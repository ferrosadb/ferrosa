# ferrosa-jsonb-conformance

Dev-only differential conformance test of [`ferrosa-jsonb`](../ferrosa-jsonb/README.md)
against arrow-rs [`parquet-variant`](https://crates.io/crates/parquet-variant) `=60.0.0`
(spec: [VariantEncoding.md](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)).
Task `t_0b127826`.

## Why it exists

T-104 found that T-102's encoder wrote the metadata header's `offset_size_minus_one`
at bit 5 instead of bits 7-6, and that T-102's hand-derived byte tests shared the
misreading, so they passed. A self-consistency test cannot catch a spec misreading;
an independent implementation can.

## Where arrow lives (D5a)

`publish = false`, no `[lib]`, no dependents. `parquet-variant` is a
dev-dependency here and nowhere else, so `ferrosa-jsonb` and `ferrosa-common` stay
arrow-free. Check with `bash scripts/ci/guard-arrow-free.sh ferrosa-common ferrosa-jsonb`.

**This crate is a workspace of its own**, listed under `exclude` in the root
`Cargo.toml`, with its own `Cargo.lock`. It cannot be a root workspace member:
parquet-variant 60 needs `chrono >= 0.4.40`, and with that chrono the root lock's
`arrow-arith` 53 (pulled by `ferrosa-flight`) fails to compile (`d.quarter()` is
ambiguous between chrono's `Datelike` and arrow's `ChronoDateExt`; 53.4.0 and 53.4.1
both). Adding it as a member breaks `cargo build --workspace`. Fold it back into the
workspace once `ferrosa-flight` moves off arrow 53.

## What is tested (`tests/`)

| File | Direction | Assertion |
|------|-----------|-----------|
| `ours_to_theirs.rs` | A: ferrosa -> upstream | Strip the `0xF1` envelope, split `metadata | value` with the spec's own layout, decode with `parquet-variant` (full validation), require value equality with the source model; dictionary is exactly the sorted used keys. Golden corpus (213 of 237 cases qualify), 10 width-boundary cases in both key orders, nesting to 128, 768 generated values. |
| `theirs_to_ours.rs` | B: upstream -> ferrosa | Build with `VariantBuilder` (sorted dictionary, smallest kinds), wrap in the envelope, require `JsonbRef::validate` to accept, read back equal, and be byte-identical to ferrosa's own encoding. Golden corpus (213), nesting to 128, 384 generated values. |
| `non_canonical.rs` | B: valid-but-refused | 29 byte strings upstream accepts and ferrosa refuses, each with its typed fault (table below). |
| `header_layout.rs` | pin | `offset_size_minus_one` at bits 7-6 (bit 5 reserved, bit 4 `sorted_strings`, bits 3-0 version), judged by upstream's builder bytes and parser; container header bits compared with upstream's bytes. Reintroducing the bit-5 bug fails 6 tests across A, B and this file. |

The corpus is extension-free with depth at most 128 (`parquet-variant`'s
`MAX_NESTING_DEPTH`), no bigdecimal (primitive 63) and no decimal wider than 38
digits. The 24 golden cases skipped hold such a number, and the test proves each
skipped case really contains a bigdecimal. Golden cases come from
`ferrosa-jsonb/tests/common`, the same list the T-102 byte pins use.

## Intended refusals (canonical storage)

ferrosa stores exactly one encoding per value. `parquet-variant` accepts several
others as valid. ferrosa must keep refusing these; none may be "fixed" by accepting
them.

| Upstream-valid input | ferrosa fault |
|----------------------|---------------|
| Empty dictionary (upstream writes header `0x01`, `sorted_strings` clear) | `BadMetadataHeader` |
| Dictionary built in non-byte order (`sorted_strings` clear) | `BadMetadataHeader` |
| Dictionary key no value uses | `NonCanonical("C3")` |
| Metadata with wider-than-minimal offsets (2 bytes for a 1-key dictionary) | `NonCanonical("C4")` |
| Metadata header reserved bit 5 set (upstream ignores it) | `NonCanonical("C2")` |
| Object with 2-byte value offsets, 2-byte field ids, or `is_large` for few fields | `NonCanonical("C5")` |
| Array with 2-byte offsets, or `is_large` for few elements | `NonCanonical("C6")` |
| `String` (primitive 16) holding 63 bytes or fewer | `NonCanonical("C7")` |
| Int16/32/64 or Decimal that fits a narrower kind, or a decimal with scale 0 | `NonCanonical("C9")` |
| Double, Date, Timestamp (micros, ntz, nanos, ntz nanos), Float, Binary, Time, Uuid | `ExcludedPrimitive(id)` (C10) |

The empty-dictionary row is the one systematic difference for key-less documents
(scalars, arrays of scalars). `theirs_to_ours.rs` proves that setting the
`sorted_strings` bit is the only change needed to make such a cell valid.

One divergence is not a fault: `parquet-variant` refuses nesting deeper than 128,
ferrosa allows up to `HARD_MAX_DEPTH`. `upstream_rejects_depth_129_where_ferrosa_accepts_it`
pins that they differ; values deeper than 128 are outside the oracle.

## Run

```bash
frg run --tee -- cargo test --manifest-path ferrosa-jsonb-conformance/Cargo.toml
frg run --tee -- cargo clippy --manifest-path ferrosa-jsonb-conformance/Cargo.toml --all-targets -- -D warnings
```

Specs: [overview](specs/overview.md) · [fmea](specs/fmea.md) · [roadmap](specs/roadmap.md).
