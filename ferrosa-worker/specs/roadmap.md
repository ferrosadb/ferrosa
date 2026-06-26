---
crate: ferrosa-worker
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-worker — Roadmap

Sourced from the in-code stub comments, the FMEA gaps ([fmea.md](fmea.md)), and
the dependency/usage review. This crate is **nascent**; the roadmap is mostly
"build the thing it promises to be."

## Now (highest value)

- **Implement the real `IndexBuild` path** (FMEA WK-1). Wire the stubbed match
  arm to: read the SSTables at `sstable_s3_paths`, deserialize
  `index_metadata_json` + `table_schema_json`, build the index via
  `ferrosa-index` over `ferrosa-sstable` partitions, write sidecars under
  `output_s3_prefix`, and return real `output_paths`. This is also what justifies
  the three currently-unused path dependencies (FMEA WK-2).
- **Extract `TaskDescriptor` / `TaskResult` into a shared `lib` or protocol
  crate** (FMEA WK-3). Today they are private to the binary, so any in-process
  producer must hand-copy them and can silently drift. One definition, shared by
  producer and worker.

## Next

- **Reconcile I/O modes with the doc comment** (FMEA WK-4). The module doc
  advertises stdin and S3 as task sources and S3 as a result sink; only
  argv-in / stdout-out exist. Either implement stdin/S3 I/O or correct the doc.
- **Add async wiring when S3 lands** (FMEA WK-7). The `tokio` "full" dependency
  is dead until `main` becomes `#[tokio::main]`; add it with the S3 work or trim
  it meanwhile.
- **Replace output `.unwrap()`s with an explicit error path** (FMEA WK-5) so a
  serialization failure fails loud with context instead of panicking.

## Later

- **Idempotency + partial-output semantics for index builds** (FMEA WK-6).
  Deterministic output paths and overwrite/cleanup rules so retried tasks don't
  collide or strand half-written sidecars. Must precede production use.
- **Compaction-offload task type.** The package description promises "compaction
  offloading"; add a second `TaskDescriptor` variant once the index path proves
  the model.
- **Wire a real caller.** No ferrosa crate currently spawns or depends on the
  worker. Connect it to the engine's external/remote index-build backend
  (`FERROSA_INDEX_BACKEND`) so the binary is actually exercised end-to-end, and
  add an integration test that drives a *successful* task.

## Non-goals

- Cluster membership, Raft, or persistent state — the worker is, by contract, a
  stateless `input → compute → output` process. Orchestration, retries, and
  scheduling belong to whatever spawns it, not here.
