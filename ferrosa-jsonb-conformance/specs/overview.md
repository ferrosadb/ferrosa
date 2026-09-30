---
crate: ferrosa-jsonb-conformance
doc: overview
last_updated: 2026-09-28
---

# ferrosa-jsonb-conformance — overview

A test-only crate (`publish = false`, no `[lib]`, no dependents). It exists so that
`ferrosa-jsonb`'s Variant encoding is checked against a second, independent
implementation (arrow-rs `parquet-variant` `=60.0.0`) instead of against itself.

## Shape

- `tests/support/mod.rs`: the value `Model`; a metadata/value splitter written from
  the spec's header layout; a canonical-kind chooser; readers of ferrosa and
  upstream cells back into the `Model`; proptest generators. It never derives an
  expectation from ferrosa's encoder or decoder.
- `tests/ours_to_theirs.rs`: direction A (ferrosa writes, upstream reads).
- `tests/theirs_to_ours.rs`: direction B (upstream writes, ferrosa validates and reads).
- `tests/non_canonical.rs`: bytes upstream accepts and ferrosa must refuse, by typed fault.
- `tests/header_layout.rs`: pins the metadata and container header bit layout.

## Boundaries

- Corpus: extension-free, depth at most 128, no primitive 63, no decimal wider than 38
  digits. Extensions are ferrosa-only and have no upstream reading.
- Workspace: standalone (root `exclude`, own lock) because parquet-variant 60's chrono
  breaks arrow-arith 53 in the root lock; see the README.
- Dependencies: arrow-rs appears only here (D5a). `scripts/ci/guard-arrow-free.sh
  ferrosa-common ferrosa-jsonb` must keep passing.
- Non-canonical upstream encodings are refused on purpose (canonical storage, JB-T1);
  see the README table. The oracle never justifies loosening the validator.
- Golden cases are shared with `ferrosa-jsonb/tests/common` by `#[path]`, so a new golden
  case is exercised by the oracle automatically.

## Upgrading parquet-variant

The version is pinned with `=`. Moving it is a deliberate act: re-run the suite,
read upstream's changelog for header, decimal and empty-dictionary behaviour, and
update the README table if the non-canonical set changes.
