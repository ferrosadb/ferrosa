CREATE KEYSPACE IF NOT EXISTS sensor_analytics WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': 1
};

USE sensor_analytics;

CREATE TABLE IF NOT EXISTS sensor_readings (
    sensor_id        text,
    ts               timestamp,
    temperature      double,
    confidence       double,
    priority         int,
    default_priority int,
    PRIMARY KEY (sensor_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- -----------------------------------------------------------------------
-- User-Defined Functions (UDFs)
--
-- These functions are implemented as WebAssembly guest crates under
-- examples/advanced-aggregation/. Build each crate with cargo-component,
-- hex-encode the resulting .wasm binary, then substitute the placeholder
-- below.
-- -----------------------------------------------------------------------

-- Scalar UDF: convert a Fahrenheit value to Celsius.
--
-- CREATE OR REPLACE FUNCTION sensor_analytics.to_celsius(f double)
--     CALLED ON NULL INPUT
--     RETURNS double
--     LANGUAGE wasm
--     AS '0x<hex-encoded wasm bytes for to_celsius>';

-- Scalar UDF: return the first non-null argument.
-- Useful for substituting a per-sensor default when priority is NULL.
--
-- CREATE OR REPLACE FUNCTION sensor_analytics.coalesce(val int, fallback int)
--     CALLED ON NULL INPUT
--     RETURNS int
--     LANGUAGE wasm
--     AS '0x<hex-encoded wasm bytes for coalesce>';

-- -----------------------------------------------------------------------
-- User-Defined Aggregates (UDAs)
--
-- Each aggregate needs a state function (sfunc) that folds one row into
-- the running state, and a final function (finalfunc) that produces the
-- result from the accumulated state.  Both are implemented in the same
-- guest crates as the scalar UDFs above.
-- -----------------------------------------------------------------------

-- Aggregate UDA: confidence-weighted average of temperature.
--
-- CREATE OR REPLACE AGGREGATE sensor_analytics.weighted_avg(double, double)
--     SFUNC weighted_avg_state
--     STYPE tuple<double, double>
--     FINALFUNC weighted_avg_final
--     INITCOND (0.0, 0.0)
--     LANGUAGE wasm
--     AS '0x<hex-encoded wasm bytes for weighted_avg>';

-- Aggregate UDA: bucket temperature readings into a histogram.
-- Returns a map<text, int> where keys are 10-degree Fahrenheit bands
-- (e.g. '40-50', '50-60') and values are counts.
--
-- CREATE OR REPLACE AGGREGATE sensor_analytics.histogram(double)
--     SFUNC histogram_state
--     STYPE map<text, int>
--     FINALFUNC histogram_final
--     INITCOND {}
--     LANGUAGE wasm
--     AS '0x<hex-encoded wasm bytes for histogram>';
