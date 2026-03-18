USE iot;

-- Get the 5 most recent readings from a device
SELECT * FROM sensor_readings
WHERE device_id = 'temp-sensor-001'
LIMIT 5;

-- Query a time range
SELECT device_id, reading_time, value, unit
FROM sensor_readings
WHERE device_id = 'temp-sensor-001'
  AND reading_time >= '2024-01-15 08:00:00'
  AND reading_time <= '2024-01-15 12:00:00';

-- Dashboard overview: latest reading per device
SELECT * FROM latest_reading;

-- Count readings for a device
SELECT COUNT(*) FROM sensor_readings
WHERE device_id = 'temp-sensor-001';

-- Find readings in a time range (for threshold analysis in application)
SELECT reading_time, value
FROM sensor_readings
WHERE device_id = 'humidity-sensor-002'
  AND reading_time >= '2024-01-15 06:00:00'
  AND reading_time <= '2024-01-15 12:00:00';

-- Query bucketed table for a single day
SELECT * FROM sensor_readings_bucketed
WHERE device_id = 'temp-sensor-001'
  AND reading_date = '2024-01-15'
LIMIT 10;
