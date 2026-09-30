---
crate: ferrosa-jsonb-conformance
doc: roadmap
last_updated: 2026-09-28
---

# ferrosa-jsonb-conformance — roadmap

## Done (t_0b127826)

- Direction A and B differential tests over the golden corpus and generated values.
- Typed non-canonical table and the header bit-layout pin.

## Open

- Run this crate in CI (it is outside the root workspace, so `cargo test --workspace`
  skips it): `cargo test` and `cargo clippy -D warnings` with `--manifest-path`.
- Fold into the root workspace once ferrosa-flight leaves arrow 53.
- Extend the golden corpus when later packets add value shapes; the oracle picks
  them up through the shared `tests/common` module.
- When the Flight packets land, add a check that the Arrow Variant column that
  ferrosa-flight emits reads back through parquet-variant.
- Revisit when parquet-variant leaves its "work in progress" status: unpin, or move
  to a supported version.
