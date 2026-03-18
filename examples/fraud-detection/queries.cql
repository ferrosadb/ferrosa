-- Fraud Detection Queries
-- Run: cqlsh -f queries.cql (after schema.cql and data.cql)

USE fraud;

-- View recent transactions for an account
SELECT transaction_time, amount, currency, merchant, country, risk_score, status
FROM transactions
WHERE account_id = 'ACCT-1001'
LIMIT 10;

-- Find transactions for an account in a specific date range
SELECT transaction_time, amount, merchant, country, risk_score
FROM transactions
WHERE account_id = 'ACCT-1001'
  AND transaction_time >= '2026-03-17 00:00:00+0000'
  AND transaction_time <= '2026-03-17 23:59:59+0000';

-- Review today's alerts
SELECT alert_time, account_id, rule_triggered, severity, details, resolved
FROM alerts
WHERE alert_date = '2026-03-17';

-- Find unresolved alerts only
SELECT alert_time, account_id, rule_triggered, severity, details
FROM alerts
WHERE alert_date = '2026-03-17'
  AND resolved = false
ALLOW FILTERING;

-- Check an account's risk profile
SELECT customer_name, home_country, avg_transaction, max_transaction, flagged, risk_level
FROM account_profile
WHERE account_id = 'ACCT-3003';

-- Detect unusual country patterns
SELECT transaction_time, amount, currency, merchant, country, risk_score
FROM transactions
WHERE account_id = 'ACCT-3003';

-- Review transactions in a specific time window
SELECT transaction_time, amount, merchant, status, risk_score
FROM transactions
WHERE account_id = 'ACCT-1001'
  AND transaction_time >= '2026-03-17 18:00:00+0000'
  AND transaction_time <= '2026-03-17 19:00:00+0000';

-- Compare a transaction against the account baseline
SELECT avg_transaction, max_transaction
FROM account_profile
WHERE account_id = 'ACCT-1001';

-- Update an account's risk level
UPDATE account_profile
SET flagged = true, risk_level = 'high'
WHERE account_id = 'ACCT-1001';

-- Verify the update
SELECT customer_name, flagged, risk_level
FROM account_profile
WHERE account_id = 'ACCT-1001';
