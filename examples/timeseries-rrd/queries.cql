-- RRD Time-Series Aggregation: Sensor Queries
--
-- Query raw readings and the materialized 5-minute aggregate tier.

USE plant;

-- Raw 1-second data for one pump.
SELECT sensor_id, ts, vibration_mm_s, temperature_c
  FROM sensor_readings_1s
  WHERE sensor_id = 2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34
  LIMIT 10;

-- 5-minute aggregates show excursions, trend, and volatility.
SELECT sensor_id, ts,
       vibration_mm_s_min,
       vibration_mm_s_max,
       vibration_mm_s_avg,
       vibration_mm_s_stddev,
       temperature_c_min,
       temperature_c_max,
       temperature_c_avg,
       temperature_c_stddev
  FROM sensor_readings_5m
  WHERE sensor_id = 2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34
  LIMIT 10;

-- Time-range query across a shift at 5-minute resolution.
SELECT sensor_id, ts, vibration_mm_s_min, vibration_mm_s_max,
       vibration_mm_s_avg, vibration_mm_s_stddev
  FROM sensor_readings_5m
  WHERE sensor_id = 2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34
    AND ts >= '2026-05-20 12:00:00'
    AND ts <  '2026-05-20 20:00:00';

-- Cross-sensor comparison at 5-minute resolution.
SELECT sensor_id, ts, vibration_mm_s_avg, temperature_c_avg
  FROM sensor_readings_5m
  WHERE sensor_id IN (
    2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34,
    7c07fa8d-f630-4a32-a0d8-df48235ed6f2
  )
  LIMIT 48;

-- Observability: check consolidation status, materialization queue health,
-- and runtime RRD ring memory controls.
SELECT * FROM system_observability.consolidation_status;
SELECT keyspace_name, table_name, queue_depth, oldest_task_age_ms, alerting
  FROM system_observability.materialization_queues
  WHERE alerting = true;
SELECT setting_name, setting_value, source
  FROM system_observability.rrd_runtime_settings;

-- Admin-only runtime tuning. The default ring memory budget is derived from
-- detected memory and can be overridden at process start with
-- FERROSA_RRD_RING_MEMORY_BUDGET_BYTES.
UPDATE system_observability.rrd_runtime_settings
  SET setting_value = 134217728
  WHERE setting_name = 'ring_memory_budget_bytes';
UPDATE system_observability.rrd_runtime_settings
  SET setting_value = 50
  WHERE setting_name = 'ring_thrash_warn_evictions';
