-- RRD Cascading Time-Series Aggregation: Sensor Schema
--
-- Creates a 4-tier cascade for factory sensor readings:
-- 1s raw readings -> 5m rollups -> 15m rollups -> 1h rollups.
-- Min/max/avg/stddev use the built-in streaming consolidation path.

CREATE KEYSPACE IF NOT EXISTS plant
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE plant;

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
    'consolidation.cascade': 'true',
    'consolidation.late_window': '15m',
    'consolidation.columns': 'vibration_mm_s,temperature_c'
  };

-- Tiers 1-3 are auto-created by cascade. Shown here for reference:
--
-- sensor_readings_5m:
--   5-minute aggregates with vibration_mm_s_min, vibration_mm_s_max,
--   vibration_mm_s_avg, vibration_mm_s_stddev, temperature_c_min,
--   temperature_c_max, temperature_c_avg, temperature_c_stddev.
-- sensor_readings_15m:
--   15-minute aggregates produced from 5-minute rollups.
-- sensor_readings_1h:
--   1-hour aggregates produced from 15-minute rollups.
