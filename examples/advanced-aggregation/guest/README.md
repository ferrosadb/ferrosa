# Ferrosa Scalar UDF Guest Crate

Standalone Rust crate that compiles to a WebAssembly Component Model module
implementing two scalar UDFs for Ferrosa:

| UDF | Signature | Logic |
|-----|-----------|-------|
| `to_celsius` | `(double) -> double` | `(f - 32.0) * 5.0 / 9.0` |
| `coalesce` | `(int, int) -> int` | First non-null argument, or null |

The exported `invoke` function (wired to the WIT `udf` world) implements
`to_celsius`. The `coalesce_invoke` function is included as a reference
implementation; to deploy it, create a second crate that delegates its
`impl Guest` block to `coalesce_invoke`.

## Prerequisites

```
rustup target add wasm32-wasip2
cargo install cargo-component
```

> `wasm32-wasip2` is the Component Model WASM target. It replaces the older
> `wasm32-wasi` target for Component Model modules.

## Build

```bash
# Using cargo-component (produces a validated WASM component)
cargo component build --release

# Using plain cargo (produces a raw WASM module without component wrapping)
cargo build --target wasm32-wasip2 --release
```

The output is written to:

```
target/wasm32-wasip2/release/ferrosa_udf_guest.wasm
```

## WIT Contract

This crate targets the `udf` world defined in `wit/ferrosa-udf.wit`.

The WIT file bundled here is a **scalar-only** simplification of the canonical
`ferrosa-udf/src/wit/ferrosa-udf.wit`. The full WIT defines recursive
collection types (`list-val`, `set-val`, `map-val`, `tuple-val`, `udt-val`)
that the WebAssembly Component Model type system cannot express in generated
bindings (wit-bindgen reports "type depends on itself"). The Ferrosa host
handles this via the dynamic `Val` API. For scalar UDFs, which only receive
and return primitive CQL types, the simplified WIT is functionally equivalent.

The `unsupported` variant case is a catch-all that the host never sends to a
scalar UDF; it exists so exhaustive match arms compile without a wildcard.

## Loading into Ferrosa

```cql
-- Register the WASM binary
INSERT INTO system.wasm_binaries (keyspace_name, function_name, body)
VALUES ('mykeyspace', 'to_celsius', 0x<hex-encoded WASM bytes>);

-- Register the UDF
CREATE FUNCTION mykeyspace.to_celsius(temp double)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE wasm
AS '$$to_celsius$$';

-- Use it
SELECT to_celsius(temperature) FROM sensor_readings;
```

## Alternative: inline AssemblyScript

For simple scalar functions you can skip the Rust build and hex-encoding entirely
and write the source inline. The server compiles it at definition time
(`LANGUAGE assemblyscript`, requires the `asc-udf` feature):

```cql
CREATE FUNCTION mykeyspace.to_celsius(temp double)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE assemblyscript
AS 'export function to_celsius(temp: f64): f64 { return (temp - 32.0) * 5.0 / 9.0; }';
```

The exported function name must match the UDF name. Supported argument/return
types: numeric (`int`, `bigint`, `float`, `double`, `smallint`, `tinyint`),
`text`/`ascii` (AS `string`), and `blob` (AS `Uint8Array`). Collection and
temporal types still require the precompiled `LANGUAGE wasm` form above.
