# Threat Model — Observability Subsystem

> **Date:** 2026-04-02
> **Scope:** Attack surfaces introduced by the observability architecture (self-hosted telemetry, OTLP export, debug endpoints, billing data)
> **Methodology:** STRIDE per element
> **Design spec:** `specs/observability-architecture.md`

## Assets

| # | Asset | Type | Impact if Compromised |
|---|-------|------|----------------------|
| AO1 | `system_observability.slow_queries` (query text) | Confidentiality | Sensitive data in WHERE clauses leaked |
| AO2 | `system_observability.query_fingerprints` (parameterized queries) | Confidentiality | Schema structure and access patterns exposed |
| AO3 | `system_observability.client_usage` (billing counters) | Integrity | Revenue loss or fraudulent billing |
| AO4 | `system_observability.spans` (trace attributes) | Confidentiality | Sensitive key/value pairs in attributes map |
| AO5 | `/api/debug/flamechart` endpoint | Availability | CPU/memory exhaustion during profiling |
| AO6 | OTLP export channel | Confidentiality | Internal telemetry exfiltrated to external endpoint |
| AO7 | Self-hosted telemetry write path | Availability | Feedback loop or amplification |
| AO8 | Internode trace context (32-byte header) | Integrity | Forged trace correlation across nodes |

## Threat Inventory

### 1. Debug Endpoint

#### OBS-T1: Flamechart DoS — CPU Exhaustion (Risk 12 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Threat** | Attacker repeatedly requests `/api/debug/flamechart?seconds=300` with maximum duration. FlameLayer captures every span close — at high request rates, the BufWriter grows unbounded and CPU is consumed rendering the SVG via `inferno`. |
| **Likelihood** | 3 |
| **Impact** | 4 |
| **Risk** | **12** |
| **Mitigation** | (1) Require admin auth token (not just superuser role) — dedicated token separate from CQL credentials. (2) Optional IP whitelist via `FERROSA_DEBUG_IP_WHITELIST` env var for faster, more secure access control. (3) Hard cap `seconds` parameter to 60. (4) Mutex ensures only one concurrent profile — reject with 429 if in progress. (5) Memory-bound the BufWriter (cap at 64 MB, drop spans beyond). (6) Rate-limit: max 1 profile per 5 minutes per node. All mitigations implemented together to prevent admin foot-gun scenarios. |
| **Status** | **APPROVED** — implement all |

#### OBS-T2: Flamechart Information Disclosure (Risk 8 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | Flame chart SVG contains full span hierarchy including table names, keyspace names, partition key hashes, and timing side-channels. If endpoint auth is bypassed, internal architecture is fully exposed. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | (1) Auth-gate behind superuser role (not just env var). (2) Strip sensitive span attributes from flame output — include only span names, not attribute maps. (3) Bind endpoint to localhost by default; require explicit config to expose externally. |
| **Status** | **APPROVED** — implement all |

### 2. Slow Query Log

#### OBS-T3: Sensitive Data in Query Text (Risk 12 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | `slow_queries.query_text` stores the full CQL statement including literal values in WHERE clauses (e.g., `WHERE ssn = '123-45-6789'`). Any user with SELECT on `system_observability` reads PII/secrets from other tenants' queries. |
| **Likelihood** | 4 |
| **Impact** | 3 |
| **Risk** | **12** |
| **Mitigation** | (1) Store ONLY `?`-masked (parameterized) queries in `query_text` — never raw literals in CQL tables. (2) Store raw text only at DEBUG level in local logs, never in CQL tables. (3) Grant SELECT on `system_observability.slow_queries` only to superuser role. (4) TTL slow query rows — default 7 days, configurable via `FERROSA_SLOW_QUERY_TTL_DAYS` env var. |
| **Status** | **APPROVED** |

#### OBS-T4: Slow Query Threshold Manipulation (Risk 4 — Medium)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering |
| **Threat** | Operator sets `FERROSA_SLOW_QUERY_THRESHOLD_MS=0`, capturing all queries as "slow." This floods `slow_queries` with every query including its text, creating a full query audit log that was not intended. |
| **Likelihood** | 2 |
| **Impact** | 2 |
| **Risk** | **4** |
| **Mitigation** | (1) Enforce minimum threshold (100ms floor) for fingerprint capture. (2) Rate-limit slow query writes (max 100/sec per node). (3) Log warning when threshold is below 500ms. (4) Maintain an all-query counter (total request count by opcode) even for queries below the fingerprint threshold — this counter does not store query text, only aggregate counts. |
| **Status** | **APPROVED** — with all-query opcode counter addition |

### 3. Query Fingerprints

#### OBS-T5: Schema Reconnaissance via Fingerprints (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | `query_fingerprints.parameterized_text` reveals all table names, column names, and access patterns. An attacker with read access to `system_observability` can reconstruct the full schema and identify high-value tables without needing access to `system_schema`. |
| **Likelihood** | 3 |
| **Impact** | 2 |
| **Risk** | **6** |
| **Mitigation** | (1) Restrict SELECT on `system_observability.query_fingerprints` to DB superuser only. (2) Consider hashing table/column names in fingerprints (trade-off: reduces advisor utility). (3) Document that only DB superusers can read billing and fingerprint data. |
| **Status** | **APPROVED** |

### 4. Billing Data

#### OBS-T6: Billing Counter Tampering (Risk 10 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Tampering, Repudiation |
| **Threat** | `client_usage` counters (`reads`, `writes`, `bytes_read`, `bytes_written`, `compute_us`) are written by the node itself. A compromised node (or operator with CQL access) can reset or decrement counters, reducing billable usage. No audit trail for counter modifications. |
| **Likelihood** | 2 |
| **Impact** | 5 |
| **Risk** | **10** |
| **Mitigation** | (1) Make `system_observability.client_usage` a virtual table (read-only from CQL — no INSERT/UPDATE/DELETE). (2) Append-only design: each bucket is a new row, never updated. (3) `ferrosa-dbaas` aggregator cross-checks node-reported totals against coordinator-level counters. (4) Billing data is SIGNED for non-repudiation — the DBaaS layer creates the signing key. Even a DB admin cannot modify signed billing records. This provides a strong integrity guarantee for the billing pipeline. |
| **Status** | **APPROVED** — add signing |

#### OBS-T7: Billing Data Loss During Restart (Risk 6 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Repudiation |
| **Threat** | In-memory billing counters reset to zero on node restart. Usage between last flush and crash is lost. Customer is undercharged (revenue loss) or the gap is attributed incorrectly. |
| **Likelihood** | 3 |
| **Impact** | 2 |
| **Risk** | **6** |
| **Mitigation** | (1) Flush billing counters to commit log every 10 seconds (acceptable loss window). (2) On restart, replay commit log to recover partial-bucket usage. (3) `ferrosa-dbaas` flags nodes with billing gaps (missing expected buckets). |
| **Status** | **APPROVED** |

### 5. Self-Hosted Telemetry Write Path

#### OBS-T8: Feedback Loop Amplification (Risk 9 — Critical)

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Threat** | `FerrosaTelemetryLayer` writes spans to ferrosa via CQL. Those CQL writes are themselves instrumented, generating new spans, which are written, generating more spans — exponential amplification. A single user query generates infinite telemetry. |
| **Likelihood** | 3 |
| **Impact** | 3 |
| **Risk** | **9** |
| **Mitigation** | (1) **Architectural fix:** System table writes go directly to the storage layer, NOT through CQL parsing. CQL is the READ path only for observability data. This eliminates the feedback loop architecturally — observability writes never enter the CQL instrumentation path. (2) Tag telemetry-origin spans and filter them in the layer as defense-in-depth. (3) Bounded channel with backpressure: if channel is full, drop spans (never block data path). (4) Circuit breaker: if telemetry write latency exceeds 500ms, pause writes for 30s. |
| **Status** | **APPROVED** — architectural fix (bypass CQL write path) |

#### OBS-T9: Telemetry Write Backpressure Stalls Data Path (Risk 8 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Denial of Service |
| **Threat** | Bounded channel in `FerrosaTelemetryLayer` is full. If the layer blocks on `send()` instead of dropping, every instrumented span becomes a blocking point. Data path latency spikes proportional to telemetry backlog. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | (1) Use `try_send()` (non-blocking) — drop OLDEST span on full channel (not newest — preserves recent data). (2) Counter for dropped spans, exposed in `system_observability.backpressure`. (3) Alert when drop rate exceeds threshold — monitor with alerting when drops occur. (4) Design review: telemetry layer must NEVER call `.await` in the `on_close` path. (5) MUST be cancel-safe — bad/excessive monitoring must never crash the system. |
| **Status** | **APPROVED** |

### 6. Enterprise OTLP Export

#### OBS-T10: Data Exfiltration via OTLP Endpoint (Risk 8 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | `FERROSA_OTEL_ENDPOINT` points to an attacker-controlled gRPC endpoint. All spans (including attributes with table names, key hashes, timing data, error messages) are streamed externally. Operator sets this once; exfiltration is continuous and silent. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | (1) Log a WARN on startup when OTLP export is enabled, including the target endpoint. (2) Use `?` for ALL data values in any externally exported spans — do NOT ship query text or data values to external endpoints. (3) External OTEL data limited to ONLY metrics that do not compromise data (span names, durations, status codes). (4) Allow-list of exportable span attributes in config. (5) mTLS for OTLP connection (verify server cert). (6) Query analysis (full text, fingerprints) available only to admins on the system — never exported externally. |
| **Status** | **APPROVED** — data-safe export only |

#### OBS-T11: OTLP Export as Covert Channel (Risk 4 — Medium)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | Malicious code injects data into span attributes (e.g., `attributes["exfil"] = secret_value`). OTLP layer exports this to the external endpoint. Span attributes become a covert exfiltration channel. |
| **Likelihood** | 1 |
| **Impact** | 4 |
| **Risk** | **4** |
| **Mitigation** | (1) Allow-list of permitted attribute keys for OTLP export — modifiable ONLY by admin. (2) Cap attribute value length (256 bytes). (3) Code review: no user-controlled data in span attributes without sanitization. Matches OBS-T10 restriction: only data-safe attributes are exportable. |
| **Status** | **APPROVED** |

### 7. Trace Data

#### OBS-T12: Sensitive Data in Span Attributes (Risk 8 — High)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | `spans.attributes` map stores arbitrary key-value pairs from `#[instrument]` fields. If a function is instrumented with `#[instrument(fields(password = %password))]`, the password is stored in `system_observability.spans` in plaintext. |
| **Likelihood** | 2 |
| **Impact** | 4 |
| **Risk** | **8** |
| **Mitigation** | (1) Coding standard: NEVER instrument functions with sensitive parameters. CI lint rule to flag `#[instrument]` on auth/credential functions. (2) Attribute scrub list in `FerrosaTelemetryLayer` — drop keys matching `password`, `token`, `secret`, `key`, `credential`. (3) TTL on spans table (7 days default). Belt-and-suspenders: implement all three layers of defense. |
| **Status** | **APPROVED** — all mitigations |

### 8. Internode Trace Context

#### OBS-T13: Trace Context Spoofing (Risk 4 — Medium)

| Field | Value |
|-------|-------|
| **STRIDE** | Spoofing, Tampering |
| **Threat** | Attacker on a compromised node injects a forged `trace_context` header (32 bytes) into internode frames. This stitches the attacker's operations into a legitimate trace, polluting trace data and potentially hiding malicious activity within benign trace timelines. |
| **Likelihood** | 2 |
| **Impact** | 2 |
| **Risk** | **4** |
| **Mitigation** | (1) Trace context is informational, not authorization — no security decisions based on trace_id. (2) Validate trace_id format (UUID v4 structure). (3) Rate-limit: if a peer sends >10k unique trace_ids/sec, flag as anomalous. (4) mTLS already authenticates the peer — spoofing requires a compromised node. (5) Log WARNING when invalid/spoofed trace context is detected, identifying the potentially malicious node. (6) HIGH SECURITY MODE (`FERROSA_HIGH_SECURITY_MODE=true`): eject the suspicious node from the cluster entirely when spoofed trace context is detected. |
| **Status** | **APPROVED** — with node ejection option |

#### OBS-T14: Trace Context as Side Channel (Risk 3 — Low)

| Field | Value |
|-------|-------|
| **STRIDE** | Information Disclosure |
| **Threat** | The 8-byte flags field in the trace context header could be used to encode covert information between compromised nodes, bypassing application-level monitoring. |
| **Likelihood** | 1 |
| **Impact** | 3 |
| **Risk** | **3** |
| **Mitigation** | (1) Validate flags field — only defined bits should be set; reject or zero unknown bits. (2) Log anomalous flag patterns with WARNING identifying the source node. (3) Rate-limit: flag nodes sending anomalous patterns above threshold. (4) HIGH SECURITY MODE (`FERROSA_HIGH_SECURITY_MODE=true`): eject the suspicious node from the cluster (same mechanism as OBS-T13). |
| **Status** | **APPROVED** — with node ejection option |

## Risk Summary (sorted by risk score)

| ID | Threat | STRIDE | L | I | Risk | Status |
|----|--------|--------|---|---|------|--------|
| OBS-T1 | Flamechart DoS | DoS | 3 | 4 | **12** | APPROVED — implement all |
| OBS-T3 | Sensitive data in slow query text | Info Disclosure | 4 | 3 | **12** | APPROVED |
| OBS-T6 | Billing counter tampering | Tampering | 2 | 5 | **10** | APPROVED — add signing |
| OBS-T8 | Feedback loop amplification | DoS | 3 | 3 | **9** | APPROVED — architectural fix |
| OBS-T2 | Flamechart info disclosure | Info Disclosure | 2 | 4 | **8** | APPROVED |
| OBS-T9 | Telemetry backpressure stalls data path | DoS | 2 | 4 | **8** | APPROVED |
| OBS-T10 | OTLP data exfiltration | Info Disclosure | 2 | 4 | **8** | APPROVED — data-safe export only |
| OBS-T12 | Sensitive data in span attributes | Info Disclosure | 2 | 4 | **8** | APPROVED — all mitigations |
| OBS-T5 | Schema recon via fingerprints | Info Disclosure | 3 | 2 | **6** | APPROVED |
| OBS-T7 | Billing data loss on restart | Repudiation | 3 | 2 | **6** | APPROVED |
| OBS-T4 | Slow query threshold manipulation | Tampering | 2 | 2 | **4** | APPROVED — with addition |
| OBS-T11 | OTLP covert channel | Info Disclosure | 1 | 4 | **4** | APPROVED |
| OBS-T13 | Trace context spoofing | Spoofing | 2 | 2 | **4** | APPROVED — with node ejection option |
| OBS-T14 | Trace context side channel | Info Disclosure | 1 | 3 | **3** | APPROVED — with node ejection option |

## Key Findings

1. **Highest-risk items** are the flamechart DoS (OBS-T1) and sensitive data in slow queries (OBS-T3). Both score 12 and have no mitigation in place.
2. **The feedback loop (OBS-T8)** is an architectural risk that must be addressed before the telemetry layer ships — a single missing guard causes cascading failure.
3. **Information disclosure is the dominant category** (8 of 14 threats). The observability subsystem is inherently a data aggregation layer — every table is an attack surface for data leakage.
4. **Billing integrity (OBS-T6)** is business-critical. Virtual tables provide read-only access by design, but cross-node validation and non-repudiation signing are needed for production billing.
5. **OTLP export (OBS-T10)** sends internal data to an operator-configured external endpoint with no attribute scrubbing — this needs an allow-list before enterprise GA.
