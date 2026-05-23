-- RRD Cascading Time-Series Aggregation: WASM Stddev UDF
--
-- Use a WASM UDF when rollup math needs a domain-specific implementation or
-- when teams want to version their own statistical routines. This example uses
-- a stddev UDF so min/max/avg come from built-ins while volatility comes from
-- a deployed WASM component.
--
-- Status: WASM function DDL, artifact loading, and streaming aggregate
-- execution are implemented. The runnable sensor demo still uses the built-in
-- streaming stddev function so it runs without an external WASM artifact.
-- Rust-authored examples live under wasm-rust/stddev and wasm-rust/rms.

USE plant;

-- Inline hex loading form. Replace these tiny placeholder bytes with a
-- compiled Component Model module that carries the
-- ferrosa:streaming-aggregate:v1 marker and exports init/update/finalize.
CREATE OR REPLACE FUNCTION plant.stddev(value double)
  CALLED ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS '0x0061736d0d000100';

-- Admin-only file loading form for pre-approved local artifacts.
CREATE OR REPLACE FUNCTION plant.stddev(value double)
  CALLED ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS FILE '/secure/udf/stddev.wasm';

-- Admin-only URL loading form. Ferrosa verifies the fetched bytes against the
-- required SHA-256 digest before storing the function metadata.
CREATE OR REPLACE FUNCTION plant.stddev(value double)
  CALLED ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS URL 'https://artifacts.example/ferrosa/stddev.wasm'
  WITH SHA256 = '3dbe4f6fb3f44dfb11ad93f6c07f81a4b34d217f8dcf7dd06d80670d88e6b7c1';

-- RRD table syntax references the UDF through the wasm:keyspace.function_name
-- consolidation function syntax. The RRD worker streams window rows through
-- init/update/finalize.
CREATE TABLE vibration_with_wasm_stddev (
    sensor_id uuid,
    ts timestamp,
    vibration_mm_s double,
    PRIMARY KEY (sensor_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'min,max,avg,wasm:plant.stddev',
    'consolidation.target': 'vibration_with_wasm_stddev_5m',
    'consolidation.columns': 'vibration_mm_s'
  };

-- Output table vibration_with_wasm_stddev_5m will have columns:
--   vibration_mm_s_min       double   (built-in min)
--   vibration_mm_s_max       double   (built-in max)
--   vibration_mm_s_avg       double   (built-in avg)
--   vibration_mm_s_stddev    double   (from WASM UDF)

-- The complete Rust examples live in wasm-rust/stddev and wasm-rust/rms.
-- Sketch of the stddev guest logic exported through the Component Model:
--
--   struct Stddev { count: f64, mean: f64, m2: f64 }
--
--   export fn init() {
--       STATE = Stddev { count: 0.0, mean: 0.0, m2: 0.0 };
--   }
--
--   export fn update(value: f64) {
--       STATE.count += 1.0;
--       let delta = value - STATE.mean;
--       STATE.mean += delta / STATE.count;
--       let delta2 = value - STATE.mean;
--       STATE.m2 += delta * delta2;
--   }
--
--   export fn finalize() -> f64 {
--       (STATE.m2 / STATE.count).sqrt()
--   }
