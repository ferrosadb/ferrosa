-- RRD Cascading Time-Series Aggregation: Queries
--
-- Query each tier to see progressively coarser aggregates.

USE sensors;

-- Raw 1-second data (most recent 10 readings).
SELECT sensor_id, ts, value
  FROM sensor_1s
  WHERE sensor_id = 550e8400-e29b-41d4-a716-446655440001
  LIMIT 10;

-- 5-minute aggregates.
SELECT sensor_id, ts, value_min, value_max, value_avg, value_stddev
  FROM sensor_5m
  WHERE sensor_id = 550e8400-e29b-41d4-a716-446655440001
  LIMIT 10;

-- 15-minute aggregates.
SELECT sensor_id, ts, value_min, value_max, value_avg, value_stddev
  FROM sensor_15m
  WHERE sensor_id = 550e8400-e29b-41d4-a716-446655440001
  LIMIT 10;

-- 1-hour aggregates.
SELECT sensor_id, ts, value_min, value_max, value_avg, value_stddev
  FROM sensor_1h
  WHERE sensor_id = 550e8400-e29b-41d4-a716-446655440001
  LIMIT 10;

-- Time-range query across a specific day at 5-minute resolution.
SELECT sensor_id, ts, value_avg
  FROM sensor_5m
  WHERE sensor_id = 550e8400-e29b-41d4-a716-446655440001
    AND ts >= '2026-03-20 00:00:00'
    AND ts <  '2026-03-21 00:00:00';

-- Cross-sensor comparison at 1-hour resolution.
SELECT sensor_id, ts, value_avg
  FROM sensor_1h
  WHERE sensor_id IN (
    550e8400-e29b-41d4-a716-446655440001,
    550e8400-e29b-41d4-a716-446655440002
  )
  LIMIT 48;

-- Observability: check consolidation status.
SELECT * FROM system_observability.consolidation_status;
