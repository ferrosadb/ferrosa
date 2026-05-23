-- RRD Time-Series Aggregation: Sensor Data
--
-- Insert factory equipment readings. Consolidation triggers automatically
-- when data timestamps cross 5-minute boundaries.

USE plant;

-- Pump A: rising vibration during the first 5-minute window, then recovery.
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:00:01', 4.2, 68.1);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:00:30', 4.4, 68.3);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:01:00', 4.1, 68.0);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:01:30', 4.7, 68.6);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:02:00', 5.0, 69.0);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:02:30', 5.4, 69.5);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:03:00', 5.9, 70.0);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:03:30', 6.2, 70.4);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:04:00', 5.8, 70.1);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:04:30', 5.1, 69.4);

-- Expected first 5-minute rollup for Pump A:
--   vibration_mm_s_min    = 4.1
--   vibration_mm_s_max    = 6.2
--   vibration_mm_s_avg    = 5.08
--   vibration_mm_s_stddev = computed by the built-in streaming stddev
--   temperature_c_min     = 68.0
--   temperature_c_max     = 70.4
--   temperature_c_avg     = 69.14
--   temperature_c_stddev  = computed by the built-in streaming stddev

-- Second 5-minute window: the same pump settles after inspection.
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:05:00', 4.8, 69.0);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:06:00', 4.6, 68.7);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:07:00', 4.5, 68.6);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:08:00', 4.3, 68.4);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (2f4d6a9e-6a9a-4f75-a6c4-cd8d210e7e34, '2026-05-20 12:09:00', 4.2, 68.2);

-- Pump B: baseline readings for cross-sensor comparisons.
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (7c07fa8d-f630-4a32-a0d8-df48235ed6f2, '2026-05-20 12:00:01', 2.1, 64.2);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (7c07fa8d-f630-4a32-a0d8-df48235ed6f2, '2026-05-20 12:01:00', 2.3, 64.4);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (7c07fa8d-f630-4a32-a0d8-df48235ed6f2, '2026-05-20 12:02:00', 2.2, 64.3);
INSERT INTO sensor_readings_1s (sensor_id, ts, vibration_mm_s, temperature_c)
  VALUES (7c07fa8d-f630-4a32-a0d8-df48235ed6f2, '2026-05-20 12:05:00', 2.4, 64.5);
