USE sensor_analytics;

-- sensor-001: outdoor temperature probe, high-confidence readings
-- priority column is always populated for this sensor

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-001', '2026-03-20 06:00:00', 42.1, 0.97, 2, 3);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-001', '2026-03-20 07:00:00', 44.6, 0.98, 2, 3);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-001', '2026-03-20 08:00:00', 51.3, 0.99, 1, 3);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-001', '2026-03-20 09:00:00', 57.8, 0.96, 1, 3);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-001', '2026-03-20 10:00:00', 63.2, 0.95, 2, 3);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-001', '2026-03-20 11:00:00', 68.4, 0.97, 2, 3);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-001', '2026-03-20 12:00:00', 72.9, 0.98, 3, 3);

-- sensor-002: indoor HVAC probe, some readings have null priority
-- to demonstrate the coalesce() UDF falling back to default_priority

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-002', '2026-03-20 06:00:00', 68.0, 0.91, null, 2);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-002', '2026-03-20 07:00:00', 69.5, 0.89, 2, 2);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-002', '2026-03-20 08:00:00', 71.1, 0.92, null, 2);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-002', '2026-03-20 09:00:00', 73.4, 0.88, 2, 2);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-002', '2026-03-20 10:00:00', 75.0, 0.87, null, 2);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-002', '2026-03-20 11:00:00', 76.3, 0.90, 1, 2);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-002', '2026-03-20 12:00:00', 77.8, 0.93, null, 2);

-- sensor-003: rooftop weather station, lower confidence due to exposure
-- mix of null and populated priority values

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-003', '2026-03-20 06:00:00', 40.5, 0.75, null, 4);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-003', '2026-03-20 07:00:00', 45.8, 0.72, 4, 4);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-003', '2026-03-20 08:00:00', 53.3, 0.78, null, 4);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-003', '2026-03-20 09:00:00', 62.7, 0.70, 3, 4);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-003', '2026-03-20 10:00:00', 74.1, 0.73, null, 4);

INSERT INTO sensor_readings (sensor_id, ts, temperature, confidence, priority, default_priority)
VALUES ('sensor-003', '2026-03-20 12:00:00', 89.9, 0.74, null, 4);
