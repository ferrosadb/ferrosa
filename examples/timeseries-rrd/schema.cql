-- RRD Time-Series Aggregation: Sensor Schema
--
-- Creates a runnable single-step rollup for factory sensor readings:
-- 1s raw readings -> 5m rollups.
-- Min/max/avg/stddev use the built-in streaming consolidation path.

CREATE KEYSPACE IF NOT EXISTS plant
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE plant;

-- Tier 1: materialized 5-minute aggregates.
CREATE TABLE sensor_readings_5m (
    sensor_id uuid,
    ts timestamp,
    vibration_mm_s_min double,
    vibration_mm_s_max double,
    vibration_mm_s_avg double,
    vibration_mm_s_stddev double,
    temperature_c_min double,
    temperature_c_max double,
    temperature_c_avg double,
    temperature_c_stddev double,
    PRIMARY KEY (sensor_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Tier 0: raw 1-second vibration and temperature readings.
CREATE TABLE sensor_readings_1s (
    sensor_id uuid,
    ts timestamp,
    vibration_mm_s double,
    temperature_c double,
    PRIMARY KEY (sensor_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND extensions = {
    'consolidation.interval': '5m',
    'consolidation.functions': 'min,max,avg,stddev',
    'consolidation.target': 'sensor_readings_5m',
    'consolidation.late_window': '15m',
    'consolidation.columns': 'vibration_mm_s,temperature_c'
  };

-- Multi-tier cascade auto-creation is intentionally not used in this runnable
-- contract. It is tracked separately because correct avg/stddev cascades need
-- additional rollup state such as count, sum, and sum-of-squares.
