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
| JB-T102-1 | Encoding drifts (dictionary order, width, numeric kind) so equal values get different bytes (FM-06) | Repair digests, Accord agreed_row and LWW ties diverge | 9 | 3 | 2 | 54 | Golden corpus of 237 values pins exact bytes (`jsonb_canonical_bytes_golden_v1`) plus hand-derived spec byte tests (`tests/hand_bytes.rs`); a change needs a new envelope. |
| JB-T102-2 | Primitive id 63 gets assigned upstream (FM-08) | Exports read our bigdecimal as another type | 8 | 2 | 2 | 32 | `variant_primitive_table_does_not_assign_63` pins the published table (Variant v1, ids 0-20). |
| JB-T102-3 | Builder misuse (unbalanced, key without value, second root) yields a silent partial value | A corrupt cell is stored | 8 | 3 | 2 | 48 | Typed `BuilderMisuse`; any error poisons the builder so `finish` fails; `jsonb_builder_rejects_unbalanced_and_oversize`. |
| JB-T102-4 | Oversize document buffered before refusal (D14) | Memory blow-up on hostile input | 7 | 3 | 3 | 63 | Running lower bound checked per attach; exact size checked before the output is allocated; `jsonb_builder_oversize_is_refused_while_building`. |
| JB-T102-5 | Deep nesting overflows the stack (M6) | Worker crash | 9 | 2 | 2 | 36 | Arena plus explicit stacks in build, size and write; depth 1000 passes on a 256 KiB thread (`jsonb_builder_depth_limit_and_no_recursion_at_1000`). |
| JB-T102-6 | Duplicate key resolved silently or non-deterministically (D6b) | Wrong value kept, invisible | 6 | 3 | 2 | 36 | Stable sort then last wins; `duplicate_keys_dropped` returned for the metric; `Error` policy is typed. **Open:** the metric and edge log line belong to the parse layer (T-104). |
| JB-T103-1 | Depth bomb overflows the stack or builds before refusal (FM-03, D14) | Worker crash, memory spike | 9 | 2 | 2 | 36 | `prescan_depth` refuses before building; parser is iterative; 256 KiB thread test at 999/1000/1001 (`jsonb_depth_prescan_ignores_brackets_in_strings`). |
| JB-T103-2 | Brackets in strings miscounted as depth | Valid document refused | 5 | 3 | 2 | 30 | Pre-scan tracks string and escape state; 2000 brackets in a string parse. |
| JB-T103-3 | Invalid UTF-8, lone surrogate, BOM, NaN, trailing garbage accepted or repaired (FM-01) | Corrupt value stored | 8 | 3 | 2 | 48 | Each is a typed error with a byte offset (`jsonb_parse_rejects_lone_surrogate_bom_nan_trailing`, 37 n-cases). |
| JB-T103-4 | Huge number lexeme builds a big integer before the cap (JB-D2) | CPU/memory DoS | 7 | 3 | 2 | 42 | Lexeme goes to `Number::parse_lexeme`, which checks digit caps first; test with 131073 digits. |
| JB-T103-5 | Duplicate key dropped invisibly (D6b, FM-04) | Silent data change | 6 | 3 | 2 | 36 | Count returned and reported to `DuplicateKeyObserver` per edge; strict mode errors with the path. **Open:** adapter must wire the metric and log line. |
| JB-T103-6 | Working set unbounded or reparsed (FM-16, JB-D5) | OOM under concurrent large parses | 8 | 2 | 4 | 64 | Single pass, arena bounded by 32x input, `InflightBudget` for adapters. **Open:** no counting-allocator test (crate forbids `unsafe`); only a 10 MiB time/size proportionality test. |
| JB-T102-7 | Size bound is conservative before an object closes (counts a duplicate until dedupe) | A value that dedupes under the limit is refused if its pre-dedupe form is over | 3 | 2 | 3 | 18 | Documented; input is bounded by `max_input_bytes`; the bound is returned at `end_object`. |
