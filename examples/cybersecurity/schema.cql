-- Cybersecurity Monitoring Schema
-- Run: cqlsh -f schema.cql

CREATE KEYSPACE IF NOT EXISTS security
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE security;

CREATE TABLE IF NOT EXISTS events (
    event_date   date,
    event_time   timestamp,
    event_id     uuid,
    source_ip    text,
    dest_ip      text,
    source_port  int,
    dest_port    int,
    protocol     text,
    event_type   text,
    severity     text,
    rule_id      text,
    description  text,
    raw_payload  blob,
    PRIMARY KEY ((event_date), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS events_by_source (
    source_ip   text,
    event_date  date,
    event_time  timestamp,
    event_id    uuid,
    event_type  text,
    severity    text,
    dest_ip     text,
    dest_port   int,
    PRIMARY KEY ((source_ip, event_date), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS threat_intel (
    indicator_type  text,
    indicator_value text,
    threat_name     text,
    severity        text,
    source          text,
    first_seen      timestamp,
    last_seen       timestamp,
    tags            set<text>,
    PRIMARY KEY (indicator_type, indicator_value)
);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id    uuid PRIMARY KEY,
    title          text,
    severity       text,
    status         text,
    created_at     timestamp,
    updated_at     timestamp,
    assigned_to    text,
    related_events list<uuid>,
    notes          text
);
