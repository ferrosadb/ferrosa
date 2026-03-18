-- Phase 2: Pair mode — additional writes replicated to second node
-- 3 more for acme (total 8), 2 more for globex (total 7)

USE app;

-- acme events
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', now(), 'click', '{"button": "export_csv"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', now(), 'page_view', '{"url": "/reports"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', now(), 'purchase', '{"item": "addon_analytics", "amount": 14.99}');

-- globex events
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', now(), 'page_view', '{"url": "/settings"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', now(), 'click', '{"button": "invite_team"}');

-- Verify counts after pair writes
SELECT COUNT(*) FROM events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';
-- Expected: 8
SELECT COUNT(*) FROM events WHERE tenant_id = 'globex' AND event_date = '2026-03-18';
-- Expected: 7
