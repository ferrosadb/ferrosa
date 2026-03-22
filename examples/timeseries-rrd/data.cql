-- RRD Cascading Time-Series Aggregation: Sample Data
--
-- Insert 1-second sensor readings. Consolidation triggers automatically
-- when data timestamps cross 5-minute boundaries.

USE sensors;

-- Sensor 1: temperature readings every second for 10 minutes.
-- (In production, these would come from the application layer.)
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:01', 22.1);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:02', 22.3);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:03', 22.0);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:00:30', 22.5);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:01:00', 23.1);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:01:30', 23.4);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:02:00', 22.8);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:02:30', 22.6);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:03:00', 22.2);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:03:30', 22.0);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:04:00', 21.8);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:04:30', 21.5);

-- After 300 rows (5 minutes), the first consolidation triggers automatically:
-- sensor_1s observer detects timestamp crossing the 5m boundary,
-- runs min/max/avg/stddev over the window, writes result to sensor_5m.

-- Second 5-minute window: 00:05:00 - 00:09:59
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:05:00', 23.0);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:05:30', 23.2);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:06:00', 23.5);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:06:30', 24.0);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:07:00', 24.2);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:07:30', 23.8);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:08:00', 23.5);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:08:30', 23.1);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:09:00', 22.9);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440001, '2026-03-20 00:09:30', 22.7);

-- Sensor 2: pressure readings (different sensor, same cascade).
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440002, '2026-03-20 00:00:01', 1013.2);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440002, '2026-03-20 00:00:02', 1013.3);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440002, '2026-03-20 00:00:03', 1013.1);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440002, '2026-03-20 00:05:00', 1013.5);
INSERT INTO sensor_1s (sensor_id, ts, value)
  VALUES (550e8400-e29b-41d4-a716-446655440002, '2026-03-20 00:05:30', 1013.7);
