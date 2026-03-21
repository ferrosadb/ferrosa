USE sensor_analytics;

-- Scalar UDF: convert temperature from Fahrenheit to Celsius per row.
-- to_celsius() is called once for every row returned by the SELECT.
SELECT sensor_id, temperature, to_celsius(temperature)
FROM sensor_readings
WHERE sensor_id = 'sensor-001';

-- Scalar UDF: substitute a sensor-level default when priority is NULL.
-- coalesce(priority, default_priority) returns priority when it is not null,
-- or falls back to default_priority for rows where priority was not recorded.
SELECT sensor_id, coalesce(priority, default_priority)
FROM sensor_readings
WHERE sensor_id = 'sensor-001';

-- Built-in aggregation: average temperature per sensor.
-- (UDA weighted_avg requires WIT collection type support — deferred to
-- a future sprint when the WIT interface supports tuple/map state types.)
SELECT avg(temperature)
FROM sensor_readings
WHERE sensor_id = 'sensor-001';

-- Built-in aggregation: temperature range statistics across all sensors.
-- (UDA histogram requires map state type in WIT — deferred.)
SELECT min(temperature), max(temperature), avg(temperature), count(temperature)
FROM sensor_readings;
