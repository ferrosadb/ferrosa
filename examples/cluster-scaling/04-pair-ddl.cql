-- Phase 2: Pair mode — DDL replication test
-- Create a counter table and populate it with daily summaries

USE app;

CREATE TABLE IF NOT EXISTS daily_summary (
    tenant_id  text,
    event_date date,
    event_type text,
    event_count counter,
    PRIMARY KEY ((tenant_id, event_date), event_type)
);

-- acme summaries for 2026-03-18
UPDATE daily_summary SET event_count = event_count + 3
    WHERE tenant_id = 'acme' AND event_date = '2026-03-18' AND event_type = 'page_view';
UPDATE daily_summary SET event_count = event_count + 2
    WHERE tenant_id = 'acme' AND event_date = '2026-03-18' AND event_type = 'click';
UPDATE daily_summary SET event_count = event_count + 1
    WHERE tenant_id = 'acme' AND event_date = '2026-03-18' AND event_type = 'signup';
UPDATE daily_summary SET event_count = event_count + 2
    WHERE tenant_id = 'acme' AND event_date = '2026-03-18' AND event_type = 'purchase';

-- globex summaries for 2026-03-18
UPDATE daily_summary SET event_count = event_count + 3
    WHERE tenant_id = 'globex' AND event_date = '2026-03-18' AND event_type = 'page_view';
UPDATE daily_summary SET event_count = event_count + 2
    WHERE tenant_id = 'globex' AND event_date = '2026-03-18' AND event_type = 'click';
UPDATE daily_summary SET event_count = event_count + 1
    WHERE tenant_id = 'globex' AND event_date = '2026-03-18' AND event_type = 'signup';
UPDATE daily_summary SET event_count = event_count + 1
    WHERE tenant_id = 'globex' AND event_date = '2026-03-18' AND event_type = 'purchase';

-- Verify counter table
SELECT * FROM daily_summary WHERE tenant_id = 'acme' AND event_date = '2026-03-18';
SELECT * FROM daily_summary WHERE tenant_id = 'globex' AND event_date = '2026-03-18';
