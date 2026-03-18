CREATE KEYSPACE IF NOT EXISTS iot WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': 3
};

USE iot;

CREATE TABLE IF NOT EXISTS sensor_readings (
    device_id    text,
    reading_time timestamp,
    sensor_type  text,
    value        double,
    unit         text,
    location     text,
    PRIMARY KEY (device_id, reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC);

CREATE TABLE IF NOT EXISTS latest_reading (
    device_id    text PRIMARY KEY,
    reading_time timestamp,
    sensor_type  text,
    value        double,
    unit         text,
    location     text
);

CREATE TABLE IF NOT EXISTS sensor_readings_bucketed (
    device_id    text,
    reading_date date,
    reading_time timestamp,
    sensor_type  text,
    value        double,
    unit         text,
    location     text,
    PRIMARY KEY ((device_id, reading_date), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC);
