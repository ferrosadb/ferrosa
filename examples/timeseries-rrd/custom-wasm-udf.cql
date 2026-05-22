-- RRD Cascading Time-Series Aggregation: WASM Stddev UDF
--
-- Use a WASM UDF when rollup math needs a domain-specific implementation or
-- when teams want to version their own statistical routines. This example uses
-- a stddev UDF so min/max/avg come from built-ins while volatility comes from
-- a deployed WASM component.
--
-- Status: WASM function DDL and artifact loading are implemented, but WASM
-- aggregate execution is not enabled for live materialization yet. The runnable
-- sensor demo uses the built-in streaming stddev function until the streaming
-- WASM aggregate ABI lands.

USE plant;

-- Inline hex loading form. Replace these tiny placeholder bytes with the
-- compiled Component Model module that exports stddev(values: list<double>).
CREATE OR REPLACE FUNCTION plant.stddev(values list<double>)
  CALLED ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS '0x0061736d0d000100';

-- Admin-only file loading form for pre-approved local artifacts.
CREATE OR REPLACE FUNCTION plant.stddev(values list<double>)
  CALLED ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS FILE '/secure/udf/stddev.wasm';

-- Admin-only URL loading form. Ferrosa verifies the fetched bytes against the
-- required SHA-256 digest before storing the function metadata.
CREATE OR REPLACE FUNCTION plant.stddev(values list<double>)
  CALLED ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS URL 'https://artifacts.example/ferrosa/stddev.wasm'
  WITH SHA256 = '3dbe4f6fb3f44dfb11ad93f6c07f81a4b34d217f8dcf7dd06d80670d88e6b7c1';

-- Future table syntax will reference the UDF through the
-- wasm:keyspace.function_name consolidation function syntax. Current releases
-- reject this CREATE TABLE with a clear "streaming WASM aggregate ABI" error
-- because live RRD materialization cannot execute list-style WASM aggregates
-- without materializing the whole window.
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

-- Once the streaming ABI exists, output table vibration_with_wasm_stddev_5m
-- will have columns:
--   vibration_mm_s_min       double   (built-in min)
--   vibration_mm_s_max       double   (built-in max)
--   vibration_mm_s_avg       double   (built-in avg)
--   vibration_mm_s_stddev    double   (from WASM UDF)

-- Sketch of the exported function:
--
--   export fn stddev(values: list<float64>) -> float64 {
--       // Population standard deviation over the values in a rollup window.
--   }
