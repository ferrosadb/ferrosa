#!/usr/bin/env bash
# Smoke-test the timeseries RRD example contract without requiring a cluster.

set -euo pipefail

cd "$(dirname "$0")"

require_text() {
  local file="$1"
  local pattern="$2"
  if ! grep -Fq "$pattern" "$file"; then
    echo "FAIL: ${file} is missing: ${pattern}" >&2
    return 1
  fi
}

require_regex() {
  local file="$1"
  local pattern="$2"
  if ! grep -Eq "$pattern" "$file"; then
    echo "FAIL: ${file} does not match: ${pattern}" >&2
    return 1
  fi
}

require_text schema.cql "CREATE KEYSPACE IF NOT EXISTS plant"
require_text schema.cql "CREATE TABLE sensor_readings_1s"
require_text schema.cql "CREATE TABLE sensor_readings_5m"
require_text schema.cql "'consolidation.functions': 'min,max,avg,stddev'"
require_text schema.cql "'consolidation.columns': 'vibration_mm_s,temperature_c'"
if grep -Fq "'consolidation.cascade': 'true'" schema.cql; then
  echo "FAIL: schema.cql must not enable cascade until multi-tier rollup state is implemented" >&2
  exit 1
fi
if grep -Fq "wasm:plant.stddev" schema.cql; then
  echo "FAIL: schema.cql must use only streaming built-ins for the runnable demo" >&2
  exit 1
fi

require_text data.cql "INSERT INTO sensor_readings_1s"
require_text data.cql "vibration_mm_s, temperature_c"

require_text queries.cql "vibration_mm_s_min"
require_text queries.cql "vibration_mm_s_max"
require_text queries.cql "vibration_mm_s_avg"
require_text queries.cql "vibration_mm_s_stddev"
require_text queries.cql "system_observability.rrd_runtime_settings"
require_text queries.cql "ring_memory_budget_bytes"
require_text queries.cql "ring_thrash_warn_evictions"
require_text queries.cql "FERROSA_RRD_RING_MEMORY_BUDGET_BYTES"

require_text custom-wasm-udf.cql "CREATE OR REPLACE FUNCTION plant.stddev(value double)"
require_text custom-wasm-udf.cql "streaming aggregate"
require_text custom-wasm-udf.cql "ferrosa:streaming-aggregate:v1"
require_text custom-wasm-udf.cql "export fn update(value: f64)"
require_text custom-wasm-udf.cql "export fn finalize() -> f64"
require_text custom-wasm-udf.cql "wasm-rust/stddev"
require_text custom-wasm-udf.cql "wasm-rust/rms"
require_text median.cql "rejects median at DDL time"
require_regex custom-wasm-udf.cql "AS '0x[0-9a-fA-F]+';"
require_text custom-wasm-udf.cql "AS FILE '/secure/udf/stddev.wasm';"
require_text custom-wasm-udf.cql "AS URL 'https://artifacts.example/ferrosa/stddev.wasm'"
require_regex custom-wasm-udf.cql "WITH SHA256 = '[0-9a-f]{64}';"

require_text wasm-rust/README.md "cargo check --target wasm32-unknown-unknown"
require_text wasm-rust/README.md "wasm-tools component new"
require_text wasm-rust/README.md "AS FILE '/secure/udf/stddev.wasm'"
require_text wasm-rust/stddev/Cargo.toml "crate-type = [\"cdylib\"]"
require_text wasm-rust/stddev/src/lib.rs "ferrosa:streaming-aggregate:v1"
require_text wasm-rust/stddev/src/lib.rs "pub extern \"C\" fn update(value: f64)"
require_text wasm-rust/stddev/src/lib.rs "pub extern \"C\" fn finalize() -> f64"
require_text wasm-rust/stddev/src/lib.rs "M2"
require_text wasm-rust/rms/Cargo.toml "crate-type = [\"cdylib\"]"
require_text wasm-rust/rms/src/lib.rs "ferrosa:streaming-aggregate:v1"
require_text wasm-rust/rms/src/lib.rs "SUM_SQUARES"
require_text wasm-rust/rms/src/lib.rs "pub extern \"C\" fn update(value: f64)"
require_text wasm-rust/rms/src/lib.rs "pub extern \"C\" fn finalize() -> f64"
cargo build --release --manifest-path wasm-rust/stddev/Cargo.toml --target wasm32-unknown-unknown
cargo build --release --manifest-path wasm-rust/rms/Cargo.toml --target wasm32-unknown-unknown

require_text timeseries-rrd.adoc "vibration_mm_s_min"
require_text timeseries-rrd.adoc "vibration_mm_s_stddev"
require_text timeseries-rrd.adoc "include::custom-wasm-udf.cql"
require_text timeseries-rrd.adoc "rrd_runtime_settings"
require_text timeseries-rrd.adoc "FERROSA_RRD_RING_MEMORY_BUDGET_BYTES"
require_text timeseries-rrd.adoc "wasm-rust/stddev"
require_text timeseries-rrd.adoc "wasm-rust/rms"

echo "PASS: timeseries-rrd smoke contract"
