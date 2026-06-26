---
crate: ferrosa-worker
doc: fmea
last_updated: 2026-06-19
---

# ferrosa-worker — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). This crate is **nascent**, so the dominant risks are
*absent functionality* and *unwired dependencies*, not subtle data-path bugs.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| WK-1 | `IndexBuild` is a stub — the worker's sole task type does no work | Any caller that spawns the worker to build an index gets `success:false, "not yet implemented"`; the feature is non-functional | 8 | 10 | 2 | 160 | **Open, by design.** Clearly stubbed in code and logged; detection is trivial (immediate failure). Implement the real S3-read + `ferrosa-index` build path. |
| WK-2 | Declared deps (`ferrosa-common`, `ferrosa-index`, `ferrosa-sstable`) are unused | Cargo carries three heavy path deps that contribute nothing; misleads readers into thinking the work is wired | 3 | 10 | 3 | 90 | **Open.** Either implement the path that uses them (preferred) or drop them until needed. Flagged here so the gap is visible. |
| WK-3 | No `lib` target — types are private to the binary | Any in-process caller wanting to construct a `TaskDescriptor` or assert on `TaskResult` must duplicate the structs; the descriptor schema can silently drift between producer and worker | 6 | 6 | 5 | 180 | **Open.** Extract `TaskDescriptor`/`TaskResult` into a `lib` (or a shared protocol crate) so producer and worker share one definition. |
| WK-4 | Promised stdin / S3 task sources and S3 result sink are undocumented-as-absent | The module doc advertises stdin/S3 I/O that does not exist; an integrator wires against a contract that isn't there | 5 | 7 | 4 | 140 | **Open.** Doc comment over-promises vs. code (argv-in/stdout-out only). Either implement the I/O modes or correct the doc comment. |
| WK-5 | `.unwrap()` on `serde_json::to_string(&result)` in every output path | If result serialization ever fails, the worker panics instead of emitting a structured error | 4 | 1 | 6 | 24 | Low risk: `TaskResult` is trivially serializable today. Still violates fail-loud-with-context; prefer an explicit error path. |
| WK-6 | No idempotency / partial-output cleanup design for the (future) real build | A retried IndexBuild could leave or collide with half-written S3 sidecars | 7 | 1 | 7 | 49 | **Not yet applicable** (work is stubbed). Must be designed before WK-1 is implemented: deterministic output paths + overwrite/cleanup semantics. |
| WK-7 | `tokio = { features=["full"] }` declared but `main` is synchronous | Dead async runtime in the dependency tree; future async work has no `#[tokio::main]` wiring | 2 | 8 | 4 | 64 | Cosmetic today. Add `#[tokio::main]` (or trim the dep) when S3 I/O lands. |

## Top risks to act on

1. **WK-3 (RPN 180)** — *no shared type definition.* The descriptor/result
   schema lives only inside the binary, so any producer must hand-copy it and can
   drift. Extracting a `lib`/protocol crate is the cheapest high-value fix and
   unblocks in-process testing of the contract.
2. **WK-1 (RPN 160)** — the core feature is a stub; the crate does not yet do its
   job. High severity/occurrence, but detection is trivial (it fails loudly), so
   it is a *missing-work* risk, not a *silent-corruption* risk.
3. **WK-4 (RPN 140)** — doc/code mismatch on input/output modes; fix the doc or
   the code so integrators aren't misled.

## Detection assets

- `tests/cli_test.rs` — confirms the envelope (usage error, invalid-JSON
  rejection, IndexBuild-stub-parses-and-fails).
- `src/main.rs` unit tests — `TaskDescriptor`/`TaskResult` serde round-trips.
- **Gap:** no test exercises a *successful* task, because none exists yet; the
  green build is a signal about the envelope only, not about real index work.
