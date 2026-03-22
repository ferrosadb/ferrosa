-- RRD Cascading Time-Series Aggregation: Custom WASM UDF
--
-- Use a WASM UDF as a consolidation function for domain-specific logic
-- that cannot be expressed with the built-in functions.
--
-- WASM UDFs are executed after the single-pass accumulator loop, receiving
-- the full batch of values for the window.

USE sensors;

-- Step 1: Register a WASM UDF that computes the 95th percentile.
-- The WASM binary is uploaded as a blob and referenced by name.
CREATE FUNCTION sensors.percentile_95 (values list<double>)
  RETURNS NULL ON NULL INPUT
  RETURNS double
  LANGUAGE wasm
  AS 'percentile_95.wasm';

-- Step 2: Create a table with the WASM UDF as consolidation function.
-- The 'wasm:keyspace.function_name' syntax invokes the WASM UDF.
CREATE TABLE api_latency (
    endpoint text,
    ts timestamp,
    latency_ms double,
    PRIMARY KEY (endpoint, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'avg,median,wasm:sensors.percentile_95',
    'consolidation.target': 'api_latency_5m',
    'consolidation.columns': 'latency_ms'
  };

-- Output table api_latency_5m will have columns:
--   latency_ms_avg              double   (built-in avg)
--   latency_ms_median           double   (built-in median)
--   latency_ms_percentile_95    double   (from WASM UDF)

-- Sample data: mixed latency distribution.
INSERT INTO api_latency (endpoint, ts, latency_ms)
  VALUES ('/api/users', '2026-03-20 00:00:01', 12.0);
INSERT INTO api_latency (endpoint, ts, latency_ms)
  VALUES ('/api/users', '2026-03-20 00:00:02', 15.0);
INSERT INTO api_latency (endpoint, ts, latency_ms)
  VALUES ('/api/users', '2026-03-20 00:00:03', 11.0);
INSERT INTO api_latency (endpoint, ts, latency_ms)
  VALUES ('/api/users', '2026-03-20 00:00:04', 200.0);
INSERT INTO api_latency (endpoint, ts, latency_ms)
  VALUES ('/api/users', '2026-03-20 00:00:05', 14.0);

-- Query the consolidated results.
SELECT endpoint, ts,
       latency_ms_avg,
       latency_ms_median,
       latency_ms_percentile_95
  FROM api_latency_5m
  WHERE endpoint = '/api/users'
  LIMIT 10;

-- Example WASM UDF source (Rust, compiled to wasm32-wasi):
--
--   #[no_mangle]
--   pub extern "C" fn percentile_95(values: &[f64]) -> f64 {
--       let mut sorted = values.to_vec();
--       sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
--       let idx = ((sorted.len() as f64) * 0.95).ceil() as usize - 1;
--       sorted[idx.min(sorted.len() - 1)]
--   }
