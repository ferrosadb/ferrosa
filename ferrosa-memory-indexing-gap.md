# ferrosa-memory Indexing Gap Report

## Problem

When starting work on the 7 correctness gaps, I attempted to use `ferrosa-memory` 
(via `recursive_explore`) to find relevant code entities — function signatures, 
module locations, and bug patterns — instead of reading files directly.

The query:
```
read_one_replica CL=ONE read fallback coordinator read.rs hints on failed writes 
write.rs streaming receiver row decoding ClusterInvite handler force_promote 
promotion epoch anti-entropy repair SSTable streaming
```

**Result:** 0 results returned across all strategies (ANN + phonetic). The system 
reported `85100 derived_facts_count` but zero matches, with the hint: 
"No results found. Try smart_ingest to add entities first."

## Why It Failed

The session startup banner said "6543 entities ingested (433 code, 312 docs, 5778 sections, 20 bugs open)" — but the 433 code entities apparently don't include the specific functions and modules I needed:

1. **Function-level entities missing**: `read_one_replica`, `force_promote`, `coordinate_write` — these are the actual functions containing the bugs, but they weren't indexed as entities.

2. **File-path entities missing**: Searching for `coordinator/read.rs` or `streaming/receiver.rs` returned nothing. The code entities that were ingested seem to be at a higher granularity (crate or module level?) rather than file/function level.

3. **Concept-level entities missing**: Terms like "CL=ONE read fallback", "hints on failed writes", "promotion epoch" — these are design concepts documented in specs and memory notes but not indexed as searchable entities.

## What I Needed

For each correctness gap, I needed:
- The **file path** containing the bug
- The **function name** and **line number** 
- The **current implementation** (to understand what to change)
- The **surrounding code** (imports, types, test patterns)

## What Would Have Helped

1. **Index Rust functions as entities**: Parse `fn function_name` declarations and index them with their file path, line number, and module path. A query for `read_one_replica` should return the entity with path `ferrosa-cluster/src/coordinator/read.rs:552`.

2. **Index file paths as entities**: Each source file should be an entity so that a query for `coordinator/read.rs` returns it.

3. **Cross-reference specs to code**: The project memory notes reference specific files and line numbers (e.g., "File: ferrosa-cluster/src/coordinator/read.rs:552"). These spec-to-code links should be indexed as edges so searching for "CL=ONE read fallback" finds both the spec description AND the code location.

4. **Index TODO/FIXME comments**: The streaming receiver has `// TODO: decode mutation.row as a ferrosa_sstable::types::Row` — these are exactly the kind of actionable items that should be findable via semantic search.

## Workaround Used

Fell back to direct file reads (`Read` tool) and `Grep` for pattern matching — which worked fine but doesn't benefit from the memory system's semantic capabilities or cross-session persistence.

## Suggestion

The `smart_ingest` or `batch_ingest` should run a Rust-aware parser (e.g., `tree-sitter-rust`) during code ingestion to extract:
- Function definitions with signatures
- Struct/enum/trait definitions
- Module hierarchy
- TODO/FIXME/BUG comments
- Test function names (for finding test coverage)

These would make the memory system the fastest way to navigate the codebase rather than a fallback-to-grep situation.
