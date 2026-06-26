---
crate: ferrosa-row-bridge
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-row-bridge — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This crate is small but on the critical data path, so
severities are high.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| RB-1 | Encoder divergence — a front-end encodes a row differently than the shared codec | Rows written by one front-end read back wrong/invisible via the other (silent corruption) | 10 | 2 | 6 | 120 | **Structural**: both front-ends call this crate; no second encoder exists (D10). Reinforced by the Postgres differential oracle (PG write → CQL read agree). |
| RB-2 | Cells emitted out of storage-column-index order | SSTable reader misreads cells → parse drift → row data loss | 10 | 1 | 7 | 70 | `build_row` sorts cells by index before returning; documented invariant. |
| RB-3 | NULL written as a live empty cell instead of a tombstone | Reads return `""`/`0` instead of NULL | 7 | 2 | 4 | 56 | `build_row`/`build_delete_row` emit tombstones for `Null`; covered by the PG NULL differential case. |
| RB-4 | In-crate test coverage gap — canonical codec/row tests live in `ferrosa-cql`, not here | A change to this crate can pass `cargo test -p ferrosa-row-bridge` while breaking the real encoding | 8 | 4 | 7 | 224 | **Open gap.** Move/duplicate the codec + row builder unit tests into this crate. See roadmap. |
| RB-5 | Composite partition-key encoding mismatch vs the engine's key format | Wrong partition routing / unreadable keys | 9 | 1 | 6 | 54 | `build_decorated_key` uses the documented `[2-byte len][bytes][0x00]` composite format; exercised by CQL + PG round-trips. |
| RB-6 | Lossy/unsupported CQL types decoded as NULL silently | Data appears as NULL rather than erroring | 5 | 3 | 5 | 75 | Documented known gap (Duration, collections, UDT, tuple, vector decode to NULL in some paths). Track which types are in scope per front-end. |

## Top risks to act on

1. **RB-4 (RPN 224)** — the highest risk is *test placement*: this crate's own
   test suite does not exercise its core functions, so its green build is not a
   real safety signal. Move the bridge codec/row unit tests in-crate.
2. **RB-1 (RPN 120)** — encoder divergence is severe but well-mitigated
   structurally + by the differential oracle; keep the "no second encoder" rule.

## Detection assets

- Postgres differential oracle (`ferrosa-postgres/tests/differential_oracle.rs`)
  — PG-written rows must read identically over CQL.
- `ferrosa-cql` bridge unit tests (build_decorated_key/build_row/encode_clustering).
