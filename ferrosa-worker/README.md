# ferrosa-worker

> A standalone, stateless task-executor **binary** — input task descriptor →
> compute → output result. Currently nascent: the one task type (`IndexBuild`)
> is a parsed-but-stubbed placeholder.

## What this crate is

`ferrosa-worker` is a small command-line **binary** (no library target) intended
to offload heavy background work — secondary-index builds, and eventually
compaction — from the main engine into a short-lived external process. Its
design contract is deliberate and narrow, per the module doc comment:

> No cluster membership, no Raft, no persistent state. Pure function:
> input → compute → output.

The worker reads a JSON **`TaskDescriptor`** from `argv[1]`, executes it, and
prints a JSON **`TaskResult`** to stdout (logs go to stderr via `tracing`). The
exit code is non-zero on any failure.

## What's implemented (the honest picture)

This crate is **early/nascent**. Today it is a ~160-line `main.rs` with three
unit tests plus three CLI integration tests. Concretely, what works:

- **CLI envelope** — argument parsing, a usage error when no task is given, and
  a structured-JSON error (still printed as a `TaskResult`) on malformed input.
- **`TaskDescriptor` enum** — internally-tagged (`#[serde(tag = "type")]`) with a
  single variant, `IndexBuild`, carrying S3 paths, keyspace/table/index names,
  and JSON-encoded index + table-schema blobs.
- **`TaskResult` struct** — `{ success, output_paths, error }`, serialized to
  stdout in every code path.
- **`tracing` setup** — env-filtered subscriber writing to stderr.

What is **not** implemented (explicitly stubbed in code):

- The actual `IndexBuild` execution. The match arm logs
  `"IndexBuild task received (stub -- not yet implemented)"` and returns
  `success: false, error: "IndexBuild not yet implemented"`. The comment reads:
  *"actual S3 read + index build will be wired in a follow-up."*
- Reading a task from **stdin or an S3 path**, and writing results to **S3** —
  promised in the module doc comment but not coded; only `argv[1]` in / stdout
  out exist today.
- **Compaction offloading** — named in the package description, no code yet.
- Any **registry, scheduling, or background task management** — there is none;
  the worker runs exactly one task and exits.

## How it works

```
$ ferrosa-worker '{"type":"IndexBuild", ...}'
  → parse argv[1] as TaskDescriptor (serde_json)
  → match on variant → execute (IndexBuild: stub)
  → print TaskResult JSON to stdout
  → exit(0) on success, exit(1) otherwise
```

A single module, `src/main.rs`. There is no `src/lib.rs`, so the types are not
importable by other crates — they are private to the binary.

## Public API (binary types)

| Type | Shape |
|------|-------|
| `TaskDescriptor` | `enum` tagged on `type`; variant `IndexBuild { sstable_s3_paths, keyspace, table, index_name, index_metadata_json, table_schema_json, output_s3_prefix }` |
| `TaskResult` | `struct { success: bool, output_paths: Vec<String>, error: Option<String> }` |

> These are `pub` inside the binary crate but, absent a `lib` target, are not
> consumable as a library API.

## Dependencies

**Calls** (ferrosa crates declared as path dependencies):

- **`ferrosa-common`**, **`ferrosa-index`**, **`ferrosa-sstable`** — declared in
  `Cargo.toml` in anticipation of the real index-build path.

> **Reality check:** none of these three crates is referenced anywhere in
> `src/` or `tests/` yet — the code that would use them (S3 read + index build)
> is stubbed. They are present so the wiring compiles when the follow-up lands.

External: `serde`/`serde_json` (descriptor codec), `tokio` (`features=["full"]`,
declared but the current `main` is synchronous), `tracing` /
`tracing-subscriber`.

**Called by** (crates that depend on this):

- **NONE.** No ferrosa crate lists `ferrosa-worker` as a path dependency. Its
  only appearance outside its own manifest is the workspace `members` array in
  the root `Cargo.toml`. It may be invoked **out-of-process** (the engine's
  `FERROSA_INDEX_BACKEND=remote`/external-builder mode shells out to a worker
  binary), or it may simply be nascent and not yet wired. Documenting the truth
  from the dependency matrix: no compile-time reverse dependency exists.

## Tests

- `src/main.rs` unit tests (3): `TaskDescriptor`/`TaskResult` serde round-trips.
- `tests/cli_test.rs` (3): no-args usage error, invalid-JSON rejection, and the
  `IndexBuild` stub (parses correctly, returns the not-implemented failure).

All six tests exercise the **envelope**, not real index work — appropriate,
since the work itself is a stub. See [specs/fmea.md](specs/fmea.md).

## Specs

- [Architecture overview](specs/overview.md) — module map, contract, data flow
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
