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

-- Aggregate UDA: compute a confidence-weighted average of temperature.
-- Each reading's contribution is scaled by its confidence score, so a
-- reading with confidence 0.99 counts nearly twice as much as one with 0.50.
SELECT weighted_avg(temperature, confidence)
FROM sensor_readings
WHERE sensor_id = 'sensor-001';

-- Aggregate UDA: produce a temperature histogram across all sensors.
-- Returns a map<text, int> with 10-degree Fahrenheit bands as keys
-- (e.g. '40-50', '50-60') and row counts as values.
-- NOTE: cross-partition aggregation; add ALLOW FILTERING if required.
SELECT histogram(temperature)
FROM sensor_readings;
