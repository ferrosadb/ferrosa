-- Act 2: Pair mode — writes replicated to second node
-- 3 more for acme (total 8), 2 more for globex (total 7)

USE app;

-- acme events (IDs 6-8)
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 6, 'click', '{"button": "export_csv"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 7, 'page_view', '{"url": "/reports"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('acme', '2026-03-18', 8, 'purchase', '{"item": "addon_analytics", "amount": 14.99}');

-- globex events (IDs 6-7)
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', 6, 'page_view', '{"url": "/settings"}');
INSERT INTO events (tenant_id, event_date, event_id, event_type, payload)
    VALUES ('globex', '2026-03-18', 7, 'click', '{"button": "invite_team"}');

-- Verify counts after pair writes
SELECT COUNT(*) FROM events WHERE tenant_id = 'acme' AND event_date = '2026-03-18';
-- Expected: 8
SELECT COUNT(*) FROM events WHERE tenant_id = 'globex' AND event_date = '2026-03-18';
-- Expected: 7
