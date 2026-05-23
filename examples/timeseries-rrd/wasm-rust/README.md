# Rust WASM RRD Aggregates

These examples show Rust-authored streaming aggregate functions for time-series
rollups. Each crate keeps bounded scalar state, consumes one sensor value per
`update(value: f64)` call, and returns one `f64` from `finalize()`.

Build-check the core WASM examples:

```bash
rustup target add wasm32-unknown-unknown
cargo check --target wasm32-unknown-unknown --manifest-path stddev/Cargo.toml
cargo check --target wasm32-unknown-unknown --manifest-path rms/Cargo.toml
```

Compile a release artifact:

```bash
cargo build --release --target wasm32-unknown-unknown --manifest-path stddev/Cargo.toml
cargo build --release --target wasm32-unknown-unknown --manifest-path rms/Cargo.toml
```

Ferrosa stores and executes Component Model WASM. These crates intentionally
show the allocation-free Rust aggregate logic as core WASM exports; package the
compiled core module as a component before loading it. In production, prefer a
`cargo-component`/`wit-bindgen` build that exports a world with `init`,
`update`, and `finalize`. For simple core modules, the packaging step starts
with:

```bash
wasm-tools component new \
  stddev/target/wasm32-unknown-unknown/release/ferrosa_rrd_stddev.wasm \
  -o stddev.wasm
```

Then load the vetted artifact through the admin-only CQL file form:

```sql
CREATE OR REPLACE FUNCTION plant.stddev(value double)
  CALLED ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS FILE '/secure/udf/stddev.wasm';
```

Use `stddev` for vibration volatility and `rms` for vibration energy. RMS is a
good fit for rotating-equipment sensors because it tracks sustained oscillation
amplitude better than a single peak sample.
