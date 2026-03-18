USE iot;

-- Temperature sensor in the main lobby
INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 06:00:00', 'temperature', 18.2, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 07:00:00', 'temperature', 19.1, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 08:00:00', 'temperature', 20.5, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 09:00:00', 'temperature', 21.3, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 10:00:00', 'temperature', 22.0, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 11:00:00', 'temperature', 22.8, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 12:00:00', 'temperature', 23.1, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 13:00:00', 'temperature', 23.4, 'celsius', 'building-a/floor-1/lobby');

-- Humidity sensor in the server room
INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 06:00:00', 'humidity', 42.1, 'percent', 'building-a/floor-2/server-room');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 07:00:00', 'humidity', 43.5, 'percent', 'building-a/floor-2/server-room');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 08:00:00', 'humidity', 44.0, 'percent', 'building-a/floor-2/server-room');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 09:00:00', 'humidity', 45.2, 'percent', 'building-a/floor-2/server-room');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 10:00:00', 'humidity', 46.8, 'percent', 'building-a/floor-2/server-room');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 11:00:00', 'humidity', 47.3, 'percent', 'building-a/floor-2/server-room');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 12:00:00', 'humidity', 46.1, 'percent', 'building-a/floor-2/server-room');

-- Pressure sensor on the roof
INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 06:00:00', 'pressure', 1013.2, 'hPa', 'building-a/roof/weather-station');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 07:00:00', 'pressure', 1013.0, 'hPa', 'building-a/roof/weather-station');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 08:00:00', 'pressure', 1012.8, 'hPa', 'building-a/roof/weather-station');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 09:00:00', 'pressure', 1012.5, 'hPa', 'building-a/roof/weather-station');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 10:00:00', 'pressure', 1011.9, 'hPa', 'building-a/roof/weather-station');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 11:00:00', 'pressure', 1011.5, 'hPa', 'building-a/roof/weather-station');

INSERT INTO sensor_readings (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 12:00:00', 'pressure', 1011.1, 'hPa', 'building-a/roof/weather-station');

-- Latest readings (one per device)
INSERT INTO latest_reading (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('temp-sensor-001', '2024-01-15 13:00:00', 'temperature', 23.4, 'celsius', 'building-a/floor-1/lobby');

INSERT INTO latest_reading (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('humidity-sensor-002', '2024-01-15 12:00:00', 'humidity', 46.1, 'percent', 'building-a/floor-2/server-room');

INSERT INTO latest_reading (device_id, reading_time, sensor_type, value, unit, location)
VALUES ('pressure-sensor-003', '2024-01-15 12:00:00', 'pressure', 1011.1, 'hPa', 'building-a/roof/weather-station');
