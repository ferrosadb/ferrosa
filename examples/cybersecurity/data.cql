-- Cybersecurity Monitoring Sample Data
-- Run: cqlsh -f data.cql (after schema.cql)

USE security;

-- ── Normal Network Traffic ──

-- Normal web traffic
INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 08:01:00+0000', e1000000-0000-0000-0000-000000000001, '10.0.1.50', '93.184.216.34', 52431, 443, 'TCP', 'connection', 'info', 'NET-001', 'HTTPS connection to example.com');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 08:01:05+0000', e1000000-0000-0000-0000-000000000002, '10.0.1.50', '10.0.0.2', 49152, 53, 'UDP', 'dns_query', 'info', 'DNS-001', 'DNS lookup for internal.corp.local');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 08:05:00+0000', e1000000-0000-0000-0000-000000000003, '10.0.2.10', '10.0.3.20', 55000, 5432, 'TCP', 'connection', 'info', 'NET-001', 'Internal database connection from app server');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 08:10:00+0000', e1000000-0000-0000-0000-000000000004, '10.0.1.100', '10.0.0.1', 60123, 389, 'TCP', 'authentication', 'info', 'AUTH-001', 'Successful LDAP authentication for user jdoe');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 09:00:00+0000', e1000000-0000-0000-0000-000000000005, '10.0.1.75', '172.217.14.206', 51888, 443, 'TCP', 'connection', 'info', 'NET-001', 'HTTPS connection to google.com');

-- ── SSH Brute Force Attack ──

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 10:15:01+0000', e2000000-0000-0000-0000-000000000001, '203.0.113.42', '10.0.1.5', 44001, 22, 'TCP', 'auth_failure', 'medium', 'SSH-001', 'Failed SSH login attempt: user root');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 10:15:03+0000', e2000000-0000-0000-0000-000000000002, '203.0.113.42', '10.0.1.5', 44002, 22, 'TCP', 'auth_failure', 'medium', 'SSH-001', 'Failed SSH login attempt: user admin');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 10:15:05+0000', e2000000-0000-0000-0000-000000000003, '203.0.113.42', '10.0.1.5', 44003, 22, 'TCP', 'auth_failure', 'medium', 'SSH-001', 'Failed SSH login attempt: user ubuntu');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 10:15:07+0000', e2000000-0000-0000-0000-000000000004, '203.0.113.42', '10.0.1.5', 44004, 22, 'TCP', 'auth_failure', 'high', 'SSH-002', 'SSH brute force detected: 4 failures in 6 seconds from 203.0.113.42');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 10:15:08+0000', e2000000-0000-0000-0000-000000000005, '203.0.113.42', '10.0.1.5', 44005, 22, 'TCP', 'auth_failure', 'medium', 'SSH-001', 'Failed SSH login attempt: user deploy');

-- ── Port Scan Activity ──

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 11:30:00+0000', e3000000-0000-0000-0000-000000000001, '198.51.100.77', '10.0.1.5', 33000, 21, 'TCP', 'port_scan', 'medium', 'SCAN-001', 'SYN scan detected on port 21 (FTP)');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 11:30:01+0000', e3000000-0000-0000-0000-000000000002, '198.51.100.77', '10.0.1.5', 33001, 23, 'TCP', 'port_scan', 'medium', 'SCAN-001', 'SYN scan detected on port 23 (Telnet)');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 11:30:02+0000', e3000000-0000-0000-0000-000000000003, '198.51.100.77', '10.0.1.5', 33002, 80, 'TCP', 'port_scan', 'low', 'SCAN-001', 'SYN scan detected on port 80 (HTTP)');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 11:30:03+0000', e3000000-0000-0000-0000-000000000004, '198.51.100.77', '10.0.1.5', 33003, 3389, 'TCP', 'port_scan', 'high', 'SCAN-002', 'SYN scan on port 3389 (RDP) -- high-value target port');

-- ── Malware Beacon and Lateral Movement ──

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 13:00:00+0000', e4000000-0000-0000-0000-000000000001, '10.0.1.200', '45.33.32.156', 49200, 443, 'TCP', 'c2_beacon', 'critical', 'MAL-001', 'Outbound connection to known C2 server (Cobalt Strike)');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 13:05:00+0000', e4000000-0000-0000-0000-000000000002, '10.0.1.200', '45.33.32.156', 49201, 443, 'TCP', 'c2_beacon', 'critical', 'MAL-001', 'Repeated beacon to C2 server -- 5 minute interval detected');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 13:10:00+0000', e4000000-0000-0000-0000-000000000003, '10.0.1.200', '45.33.32.156', 49202, 443, 'TCP', 'c2_beacon', 'critical', 'MAL-001', 'Third C2 beacon in 10 minutes -- confirmed periodic callback');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 13:12:00+0000', e4000000-0000-0000-0000-000000000004, '10.0.1.200', '10.0.2.10', 49300, 445, 'TCP', 'lateral_movement', 'critical', 'LAT-001', 'SMB connection from compromised host to internal app server');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 13:14:00+0000', e4000000-0000-0000-0000-000000000005, '10.0.1.200', '10.0.3.20', 49301, 445, 'TCP', 'lateral_movement', 'critical', 'LAT-001', 'SMB connection from compromised host to database server');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 14:00:00+0000', e4000000-0000-0000-0000-000000000006, '10.0.1.200', '45.33.32.156', 49203, 443, 'TCP', 'data_exfiltration', 'critical', 'EXFIL-001', 'Large outbound data transfer (450MB) to C2 server');

-- ── Events by Source (denormalized for IP investigation) ──

-- SSH brute force attacker
INSERT INTO events_by_source (source_ip, event_date, event_time, event_id, event_type, severity, dest_ip, dest_port)
VALUES ('203.0.113.42', '2026-03-17', '2026-03-17 10:15:01+0000', e2000000-0000-0000-0000-000000000001, 'auth_failure', 'medium', '10.0.1.5', 22);

INSERT INTO events_by_source (source_ip, event_date, event_time, event_id, event_type, severity, dest_ip, dest_port)
VALUES ('203.0.113.42', '2026-03-17', '2026-03-17 10:15:03+0000', e2000000-0000-0000-0000-000000000002, 'auth_failure', 'medium', '10.0.1.5', 22);

INSERT INTO events_by_source (source_ip, event_date, event_time, event_id, event_type, severity, dest_ip, dest_port)
VALUES ('203.0.113.42', '2026-03-17', '2026-03-17 10:15:05+0000', e2000000-0000-0000-0000-000000000003, 'auth_failure', 'medium', '10.0.1.5', 22);

INSERT INTO events_by_source (source_ip, event_date, event_time, event_id, event_type, severity, dest_ip, dest_port)
VALUES ('203.0.113.42', '2026-03-17', '2026-03-17 10:15:07+0000', e2000000-0000-0000-0000-000000000004, 'auth_failure', 'high', '10.0.1.5', 22);

-- Compromised internal host
INSERT INTO events_by_source (source_ip, event_date, event_time, event_id, event_type, severity, dest_ip, dest_port)
VALUES ('10.0.1.200', '2026-03-17', '2026-03-17 13:00:00+0000', e4000000-0000-0000-0000-000000000001, 'c2_beacon', 'critical', '45.33.32.156', 443);

INSERT INTO events_by_source (source_ip, event_date, event_time, event_id, event_type, severity, dest_ip, dest_port)
VALUES ('10.0.1.200', '2026-03-17', '2026-03-17 13:12:00+0000', e4000000-0000-0000-0000-000000000004, 'lateral_movement', 'critical', '10.0.2.10', 445);

INSERT INTO events_by_source (source_ip, event_date, event_time, event_id, event_type, severity, dest_ip, dest_port)
VALUES ('10.0.1.200', '2026-03-17', '2026-03-17 14:00:00+0000', e4000000-0000-0000-0000-000000000006, 'data_exfiltration', 'critical', '45.33.32.156', 443);

-- ── Threat Intelligence ──

INSERT INTO threat_intel (indicator_type, indicator_value, threat_name, severity, source, first_seen, last_seen, tags)
VALUES ('ip', '45.33.32.156', 'Cobalt Strike C2', 'critical', 'AlienVault OTX',
    '2026-01-15 00:00:00+0000', '2026-03-17 13:00:00+0000',
    {'cobalt-strike', 'c2', 'apt', 'beacon'});

INSERT INTO threat_intel (indicator_type, indicator_value, threat_name, severity, source, first_seen, last_seen, tags)
VALUES ('ip', '203.0.113.42', 'SSH Brute Force Botnet', 'medium', 'AbuseIPDB',
    '2026-02-20 00:00:00+0000', '2026-03-17 10:15:00+0000',
    {'brute-force', 'ssh', 'botnet'});

INSERT INTO threat_intel (indicator_type, indicator_value, threat_name, severity, source, first_seen, last_seen, tags)
VALUES ('ip', '198.51.100.77', 'Reconnaissance Scanner', 'low', 'Shodan',
    '2026-03-10 00:00:00+0000', '2026-03-17 11:30:00+0000',
    {'scanner', 'recon', 'port-scan'});

INSERT INTO threat_intel (indicator_type, indicator_value, threat_name, severity, source, first_seen, last_seen, tags)
VALUES ('domain', 'evil-payload.example.net', 'Malware Distribution', 'high', 'VirusTotal',
    '2026-03-01 00:00:00+0000', '2026-03-16 00:00:00+0000',
    {'malware', 'dropper', 'phishing'});

-- ── Incidents ──

INSERT INTO incidents (incident_id, title, severity, status, created_at, updated_at, assigned_to, related_events, notes)
VALUES (
    f5a10000-0000-0000-0000-000000000001,
    'Active C2 Communication from Workstation 10.0.1.200',
    'critical',
    'investigating',
    '2026-03-17 13:05:30+0000',
    '2026-03-17 14:10:00+0000',
    'analyst-rivera',
    [e4000000-0000-0000-0000-000000000001,
     e4000000-0000-0000-0000-000000000002,
     e4000000-0000-0000-0000-000000000003,
     e4000000-0000-0000-0000-000000000004,
     e4000000-0000-0000-0000-000000000005,
     e4000000-0000-0000-0000-000000000006],
    'Workstation 10.0.1.200 is communicating with known Cobalt Strike C2 at 45.33.32.156 on a 5-minute interval. Lateral movement to app server and DB server observed. 450MB data exfiltration detected. Immediate containment required.'
);

-- ── Afternoon Normal Traffic ──

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 14:30:00+0000', e1000000-0000-0000-0000-000000000006, '10.0.1.50', '10.0.0.2', 49200, 53, 'UDP', 'dns_query', 'info', 'DNS-001', 'DNS lookup for api.corp.local');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 15:00:00+0000', e1000000-0000-0000-0000-000000000007, '10.0.1.100', '10.0.0.1', 60200, 389, 'TCP', 'authentication', 'info', 'AUTH-001', 'Successful LDAP authentication for user asmith');

INSERT INTO events (event_date, event_time, event_id, source_ip, dest_ip, source_port, dest_port, protocol, event_type, severity, rule_id, description)
VALUES ('2026-03-17', '2026-03-17 16:00:00+0000', e1000000-0000-0000-0000-000000000008, '10.0.2.10', '10.0.3.20', 55100, 5432, 'TCP', 'connection', 'info', 'NET-001', 'Routine database connection from app server');
