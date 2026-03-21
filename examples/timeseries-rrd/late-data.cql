-- RRD Cascading Time-Series Aggregation: Late-Arriving Data
--
-- Demonstrates re-aggregation when late data arrives after a window
-- has already been consolidated.

USE sensors;

-- Assume sensor_1s has been running and has consolidated 5m windows.
-- The window [00:00:00, 00:05:00) has already been consolidated into
-- sensor_5m with the values from data.cql.

-- Now a late data point arrives (e.g., from a device that was offline):
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:01:30', 23.5);

-- If '2026-03-20 00:01:30' falls in an already-consolidated 5m window:
-- 1. The aggregator detects late data (ts < boundary_ts)
-- 2. Sends LateData task to async worker
-- 3. Worker reads full window from disk
-- 4. Re-runs consolidation with the late point included
-- 5. Upserts corrected aggregate into sensor_5m
-- 6. The upsert triggers sensor_5m's observer -> cascade continues

-- Late data is only accepted within the configured late_window (10m).
-- Data older than boundary_ts - late_window is silently dropped.

-- Verify the corrected aggregates:
SELECT * FROM sensor_5m
  WHERE sensor_id = 550e8400-e29b-41d4-a716-446655440001
  AND ts = '2026-03-20 00:00:00';

-- Multiple late arrivals for the same window are debounced.
-- The LateDataDebouncer coalesces re-aggregation requests within
-- a configurable debounce interval (default 1 second) so that a burst
-- of late data only triggers one re-aggregation per window.

-- Example: three late points arriving in rapid succession.
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:02:15', 22.9);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:02:45', 23.0);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:03:15', 22.7);

-- Only one re-aggregation will be triggered for the [00:00:00, 00:05:00)
-- window, incorporating all three late points.
