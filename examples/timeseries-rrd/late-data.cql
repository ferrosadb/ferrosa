-- RRD Time-Series Aggregation: Late-Arriving Data
--
-- Demonstrates re-aggregation when late data arrives after a window
-- has already been consolidated.

USE plant;

-- Assume sensor_readings_1s has been running and has consolidated 5m windows.
-- The window [12:00:00, 12:05:00) has already been consolidated into
-- sensor_readings_5m with the values from data.cql.

-- Now a late data point arrives (e.g., from a device that was offline):
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:01:30', 4.9, 68.8);

-- If '2026-05-20 12:01:30' falls in an already-consolidated 5m window:
-- 1. The aggregator detects late data (ts < boundary_ts)
-- 2. Sends LateData task to async worker
-- 3. Worker reads full window from disk
-- 4. Re-runs consolidation with the late point included
-- 5. Upserts corrected aggregate into sensor_readings_5m

-- Late data is only accepted within the configured late_window (15m).
-- Data older than boundary_ts - late_window is dropped and counted in
-- materialization observability.

-- Verify the corrected aggregates:
SELECT * FROM sensor_readings_5m
  WHERE sensor_id = 2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34
  AND ts = '2026-05-20 12:00:00';

-- Multiple late arrivals for the same window are debounced.
-- The LateDataDebouncer coalesces re-aggregation requests within
-- a configurable debounce interval (default 1 second) so that a burst
-- of late data only triggers one re-aggregation per window.

-- Example: three late points arriving in rapid succession.
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:02:15', 5.2, 69.1);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:02:45', 5.3, 69.2);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:03:15', 5.6, 69.7);

-- Only one re-aggregation will be triggered for the [12:00:00, 12:05:00)
-- window, incorporating all three late points.
