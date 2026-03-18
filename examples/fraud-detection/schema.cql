-- Fraud Detection Schema
-- Run: cqlsh -f schema.cql

CREATE KEYSPACE IF NOT EXISTS fraud
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE fraud;

CREATE TABLE IF NOT EXISTS transactions (
    account_id        text,
    transaction_time  timestamp,
    transaction_id    uuid,
    amount            decimal,
    currency          text,
    merchant          text,
    merchant_category text,
    location          text,
    country           text,
    status            text,
    risk_score        int,
    PRIMARY KEY (account_id, transaction_time, transaction_id)
) WITH CLUSTERING ORDER BY (transaction_time DESC);

CREATE TABLE IF NOT EXISTS account_profile (
    account_id           text PRIMARY KEY,
    customer_name        text,
    home_country         text,
    avg_transaction      decimal,
    max_transaction      decimal,
    last_transaction_time timestamp,
    flagged              boolean,
    risk_level           text
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_date     date,
    alert_time     timestamp,
    alert_id       uuid,
    account_id     text,
    transaction_id uuid,
    rule_triggered text,
    severity       text,
    details        text,
    resolved       boolean,
    PRIMARY KEY (alert_date, alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC);
