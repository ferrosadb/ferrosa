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
-- Scalar UDFs (to_celsius, coalesce) are implemented as WASM Component
-- Model crates under guest/ and guest-coalesce/. The UDF query-time
-- execution pipeline is validated by 74 unit tests in ferrosa-udf.
--
-- Embedding hex-encoded WASM in CQL is deferred until the storage engine
-- supports large cell values (>64KB) without SSTable corruption.
-- -----------------------------------------------------------------------
