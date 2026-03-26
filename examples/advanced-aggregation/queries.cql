USE sensor_analytics;

-- Per-sensor temperature readings (raw data).
SELECT sensor_id, ts, temperature, confidence
FROM sensor_readings
WHERE sensor_id = 'sensor-001';

-- Per-sensor priority with default fallback.
-- (UDF coalesce() requires WASM registration; using raw column for now.)
SELECT sensor_id, priority, default_priority
FROM sensor_readings
WHERE sensor_id = 'sensor-001';

-- Built-in aggregation: average temperature per sensor.
SELECT avg(temperature)
FROM sensor_readings
WHERE sensor_id = 'sensor-001';

-- Built-in aggregation: temperature range statistics across all sensors.
SELECT min(temperature), max(temperature), avg(temperature), count(temperature)
FROM sensor_readings;
