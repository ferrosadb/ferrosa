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
| JB-T101-1 | Digit caps checked after BigInt conversion (JB-D2) | Quadratic conversion of a hostile lexeme burns CPU | 8 | 3 | 3 | 72 | Caps run on the lexeme; integer parts of 147456+ digits refused unscanned; `jsonb_number_digit_caps_at_boundary` (10 MiB in under 50 ms). |
| JB-T101-2 | Huge exponent overflows or is stored as an integer (`1e2147483648`) | Wrong value or memory blow-up | 9 | 2 | 3 | 54 | Exponent saturates in `i64`, digits computed before any allocation; `jsonb_number_boundaries_round_trip`. |
| JB-T101-3 | Scale lost (`1.0` becomes `1`) (FM-49) | PG prints `1`, Bolt returns Integer | 7 | 3 | 3 | 63 | Scale stored and printed; `jsonb_number_scale_follows_pg_numeric`, `jsonb_number_lexeme_round_trip_preserves_scale`. |
| JB-T101-4 | Eq and Hash disagree | Set/DISTINCT/hash-key corruption | 8 | 2 | 3 | 48 | Both use one normalized form; `jsonb_number_eq_hash_agree`. |
| JB-T101-5 | NaN/Inf silently become a number (FM-48) | Invalid stored value | 7 | 2 | 2 | 28 | `NonFiniteNumber`; `jsonb_number_f64_shortest_edges`. |
| JB-T101-6 | Implicit f64 rounding in a number path | Silent precision loss | 9 | 2 | 3 | 54 | No f64 in parse or compare; f64 only in `from_f64_shortest` / `to_f64_if_shortest_round_trips`, which verify the round trip. |
| JB-T100-6 | Read path uses tunables instead of `HardCeilings` | Lowering a limit makes stored data unreadable | 9 | 2 | 4 | 72 | Separate types; only `HardCeilings` is documented for reads. Enforced by T-102 onward. |
