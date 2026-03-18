-- Telecommunications Platform Queries
-- Run: cqlsh -f queries.cql (after schema.cql and data.cql)

USE telecom;

-- Alice's calls on March 17
SELECT call_time, call_type, duration_seconds, destination, cost
FROM call_records
WHERE subscriber_id = 'SUB-1001'
  AND call_date = '2026-03-17';

-- Ben's data usage on March 17
SELECT call_time, data_bytes, destination
FROM call_records
WHERE subscriber_id = 'SUB-1002'
  AND call_date = '2026-03-17'
  AND call_type = 'data'
ALLOW FILTERING;

-- Carmen's billing history
SELECT billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost
FROM billing_summary
WHERE subscriber_id = 'SUB-1003';

-- All events for Chicago tower on March 17
SELECT event_time, event_type, severity, description
FROM network_events
WHERE cell_tower = 'TWR-CHI-008'
  AND event_date = '2026-03-17';

-- Check Ben's balance
SELECT name, plan_type, balance, monthly_limit_minutes, monthly_limit_data_gb
FROM subscribers
WHERE subscriber_id = 'SUB-1002';

-- Ben's calls that cost more than $1.00
SELECT call_time, call_type, destination, duration_seconds, cost
FROM call_records
WHERE subscriber_id = 'SUB-1002'
  AND call_date = '2026-03-17'
  AND cost > 1.00
ALLOW FILTERING;

-- Ben tops up his prepaid balance by $20
UPDATE subscribers
SET balance = 42.50
WHERE subscriber_id = 'SUB-1002';

-- Carmen's voice calls on March 17
SELECT call_time, destination, duration_seconds, cost
FROM call_records
WHERE subscriber_id = 'SUB-1003'
  AND call_date = '2026-03-17'
  AND call_type = 'voice'
ALLOW FILTERING;

-- Alice's billing for February and March
SELECT billing_month, total_voice_minutes, total_sms, total_data_mb, total_cost
FROM billing_summary
WHERE subscriber_id = 'SUB-1001'
  AND billing_month >= '2026-02'
  AND billing_month <= '2026-03';
