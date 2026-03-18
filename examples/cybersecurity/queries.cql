-- Cybersecurity Monitoring Queries
-- Run: cqlsh -f queries.cql (after schema.cql and data.cql)

USE security;

-- View events in a time window
SELECT event_time, source_ip, dest_ip, event_type, severity, description
FROM events
WHERE event_date = '2026-03-17'
  AND event_time >= '2026-03-17 13:00:00+0000'
  AND event_time <= '2026-03-17 14:00:00+0000';

-- Investigate the SSH brute force source IP
SELECT event_time, event_type, severity, dest_ip, dest_port
FROM events_by_source
WHERE source_ip = '203.0.113.42'
  AND event_date = '2026-03-17';

-- Find high-severity events (pull today's events, filter in application)
SELECT event_time, source_ip, dest_ip, event_type, severity, description
FROM events
WHERE event_date = '2026-03-17'
LIMIT 50;

-- Check all activity from the compromised host
SELECT event_time, event_type, severity, dest_ip, dest_port
FROM events_by_source
WHERE source_ip = '10.0.1.200'
  AND event_date = '2026-03-17';

-- Look up threat intelligence for the C2 server
SELECT threat_name, severity, source, first_seen, last_seen, tags
FROM threat_intel
WHERE indicator_type = 'ip'
  AND indicator_value = '45.33.32.156';

-- Review the incident
SELECT title, severity, status, assigned_to, related_events, notes
FROM incidents
WHERE incident_id = f5a10000-0000-0000-0000-000000000001;

-- Look up threat intel for the SSH brute force IP
SELECT threat_name, severity, tags
FROM threat_intel
WHERE indicator_type = 'ip'
  AND indicator_value = '203.0.113.42';

-- Search all IP-based threat intel
SELECT indicator_value, threat_name, severity, tags
FROM threat_intel
WHERE indicator_type = 'ip';

-- Look up domain-based threats
SELECT indicator_value, threat_name, severity, source, tags
FROM threat_intel
WHERE indicator_type = 'domain';

-- Update incident with correlation findings
UPDATE incidents
SET notes = 'Workstation 10.0.1.200 is communicating with known Cobalt Strike C2 at 45.33.32.156 on a 5-minute interval. Lateral movement to app server and DB server observed. 450MB data exfiltration detected. Immediate containment required. CORRELATED: SSH brute force from 203.0.113.42 also targeted 10.0.1.5 -- possible initial access vector.',
    updated_at = '2026-03-17 15:00:00+0000',
    status = 'containment'
WHERE incident_id = f5a10000-0000-0000-0000-000000000001;
