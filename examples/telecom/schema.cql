-- Telecommunications Platform Schema
-- Run: cqlsh -f schema.cql

CREATE KEYSPACE IF NOT EXISTS telecom
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
  };

USE telecom;

CREATE TABLE IF NOT EXISTS call_records (
    subscriber_id    text,
    call_date        date,
    call_time        timestamp,
    call_id          uuid,
    call_type        text,       -- voice, sms, data
    duration_seconds int,
    data_bytes       bigint,
    destination      text,
    cell_tower       text,
    cost             decimal,
    PRIMARY KEY ((subscriber_id, call_date), call_time, call_id)
) WITH CLUSTERING ORDER BY (call_time DESC, call_id ASC);

CREATE TABLE IF NOT EXISTS subscribers (
    subscriber_id        text PRIMARY KEY,
    name                 text,
    plan_type            text,
    monthly_limit_minutes int,
    monthly_limit_data_gb int,
    balance              decimal,
    activated_at         timestamp
);

CREATE TABLE IF NOT EXISTS billing_summary (
    subscriber_id       text,
    billing_month       text,
    total_voice_minutes int,
    total_sms           int,
    total_data_mb       bigint,
    total_cost          decimal,
    PRIMARY KEY (subscriber_id, billing_month)
) WITH CLUSTERING ORDER BY (billing_month DESC);

CREATE TABLE IF NOT EXISTS network_events (
    cell_tower   text,
    event_date   date,
    event_time   timestamp,
    event_type   text,
    severity     text,
    description  text,
    PRIMARY KEY ((cell_tower, event_date), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
