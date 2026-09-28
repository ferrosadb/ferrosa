---
crate: ferrosa-jsonb
doc: fmea
last_updated: 2026-09-28
---

# ferrosa-jsonb — FMEA

RPN = Severity x Occurrence x Detection. Ids use the packet form (`JB-T100-n`) so
parallel packets cannot collide.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| JB-T100-1 | A tunable above its ceiling is accepted (FM-108) | Ingest admits values the read path refuses, so stored cells become unreadable (FM-17) | 9 | 3 | 2 | 54 | `Limits::from_config` refuses at startup naming the tunable; test `jsonb_limits_tunable_above_hard_ceiling_refuses_startup`. |
| JB-T100-2 | A zero tunable is accepted | Every value rejected, or a check that never fires | 6 | 3 | 2 | 36 | Refused at startup; `jsonb_limits_zero_refuses_startup`. |
| JB-T100-3 | Ingest limit above the commit-log segment size (D14d) | An admitted value cannot be held by the write path | 8 | 3 | 3 | 72 | `write_path_max` argument; `jsonb_limits_above_commitlog_segment_refuses_startup`. **Open:** the `ferrosa` binary does not yet pass the segment size (see roadmap). |
| JB-T100-4 | Env override is unparseable and treated as unset | An operator's limit silently ignored | 6 | 3 | 3 | 54 | `InvalidEnv` error; non-unicode values are surfaced through lossy conversion so they fail to parse. |
| JB-T100-5 | TOML and env precedence inverted | Operator config overridden by a stale env var | 5 | 2 | 3 | 30 | `jsonb_limits_toml_wins_over_env`. |
| JB-T100-6 | Read path uses tunables instead of `HardCeilings` | Lowering a limit makes stored data unreadable | 9 | 2 | 4 | 72 | Separate types; only `HardCeilings` is documented for reads. Enforced by T-102 onward. |
