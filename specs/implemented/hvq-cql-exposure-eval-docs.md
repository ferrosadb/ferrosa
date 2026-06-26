# HVQ Quantized Vector Index — CQL Exposure, Evaluation, and Docs

Status: In progress (design approved 2026-05-30)
Builds on: `specs/in-process/hvq-cspann-implementation-blueprint.md`,
`specs/plans/hvq-cspann-implementation-plan.md`, commit `6b0558f` (#68).

## Problem

Commit #68 added the hybrid vector quantization (HVQ) index path — `.qvec`
container, Q8/Q4/Q2/Q1 codecs, deterministic quantized-IVF builder, staged
page-budgeted reader with exact f32 rerank, S3 range-read page cache, and
storage dispatch (`VectorIndexMethod::QuantizedIvf`). It sits alongside the
original HNSW and IVFFlat full-precision sidecars.

Three gaps remain for it to be usable and discoverable:

1. **Not reachable from CQL.** `resolve_index_type()` maps `USING 'vector'` to a
   single `IndexType::Vector`; `WITH OPTIONS` is parsed but never consumed. The
   quantized method is selectable only from the Rust storage API
   (`store.rs:2523/2534`). A CQL user always gets HNSW.
2. **No head-to-head evaluation.** `tests/baseline.rs` measures HNSW only; the
   quantized side has unit tests; the benchmark-evidence packet is pending.
3. **No user-facing documentation** for vector indexes at all.

## Goals

- Let CQL select the quantized index via `WITH OPTIONS = {'method': 'hvq'}`.
- Produce real measured recall / bytes-read / latency / size numbers comparing
  HNSW, IVFFlat, and QuantizedIvf on one shared corpus.
- Ship a user-facing docs page (CQL reference + evaluation), linked from the
  docs home page.
- Add a runnable example covering every CQL-reachable index strategy that
  executes in CI and renders to generated HTML docs like the existing examples.

Non-goals: changing the on-disk `.qvec` format; product quantization / RaBitQ;
exposing every structural knob in v1.

## Workstream 1 — CQL method selection (TDD)

CQL contract:

```cql
CREATE INDEX idx_embed ON ks.docs (embedding) USING 'vector'
  WITH OPTIONS = {'method': 'hvq', 'metric': 'cosine'};
```

- `method`: `hnsw` (default when absent) | `hvq` (the quantized-IVF path).
- **Unknown `method` value → loud `CqlError::Invalid`.** No silent fallback to
  HNSW (fail-loud rule).
- `metric` threaded through where `VectorIndexConfig` supports it. Structural
  knobs (`lists`, `tiers`, `rerank`) accepted with documented defaults in v1.

Seam: `route_create_index` (router.rs ~5402) currently calls
`engine.add_vector_index(table_id, index_name, pos, dimension)`. Thread a
resolved `VectorIndexMethod` through that call to
`store.add_vector_index_with_method` / `add_quantized_vector_index`.

RED tests (router unit tests, same module as
`ann_prefix_create_vector_index_wires_scoped_storage_index`):

1. `{'method':'hvq'}` → storage index created with method `QuantizedIvf`.
2. Absent method or `{'method':'hnsw'}` → method HNSW.
3. `{'method':'bogus'}` → `CqlError::Invalid`, no index created.

GREEN: resolve the option, thread it down; keep HNSW as default.

## Workstream 2 — Evaluation harness (real numbers)

New `ferrosa-index/tests/eval_comparison.rs`, reusing the `baseline.rs`
clustered-corpus generator. On one identical corpus, build HNSW, IVFFlat, and
QuantizedIvf; measure against exact f32 brute-force truth:

- recall@10 and recall@100
- bytes read per query (HNSW/IVFFlat = full sidecar; QuantizedIvf = page-store
  range-read metrics)
- p50 / p95 query latency
- on-disk index size

Emit a table to stdout. Run natively with a clean `CARGO_TARGET_DIR` to avoid
the root-owned files under `target/` left by old docker builds. Transcribe the
numbers into the docs page with corpus parameters and commit SHA noted for
reproducibility. Frame results against the spec's locked gates: recall ≥ 0.95,
≥ 5× fewer bytes read, ≥ 2× lower p95.

## Workstream 3 — Reference + evaluation page

New `docs/database/vector-indexes.html`, cloning the existing site design system
(see `docs/database/cql-compatibility.html`). This is the detail page the home
page links to. Sections:

- Concepts: HNSW vs IVFFlat vs HVQ/quantized-IVF; Q8/Q4/Q2/Q1 + staged rerank;
  the storage/latency-vs-recall tradeoff.
- CQL reference: `vector<float, N>` column type, `CREATE INDEX … USING 'vector'
  WITH OPTIONS={…}` including the new `method`, and `ORDER BY … ANN OF … LIMIT`.
- Evaluation: the measured comparison table from Workstream 2.
- Link to the runnable example (Workstream 5).

This page is hand-authored HTML (static reference + eval numbers). The runnable,
executable example lives in Workstream 5.

## Workstream 4 — Home page

Add a "Vector Indexes" card to `docs/index.html` with a brief results summary
(one table or three bullets) linking to the reference page (WS3) and the
runnable example (WS5). Keep `docs/index.md` in sync.

## Workstream 5 — CI-run example → generated docs

Add a new example to the existing generated-examples pipeline so it executes in
CI and renders to HTML like the others. Pipeline:
`examples/<name>/*.adoc + *.cql` → `make -C examples html` (asciidoctor) →
`docs/database/examples/<name>.html`; CI `docs-examples.yml` regenerates HTML and
`ci.yml`'s `examples` job runs the `.cql` against a live single node via cqlsh.

New `examples/vector-indexes/`:

- `schema.cql` — a table with a `vector<float, N>` column; create one BTree
  index (scalar filter), one default HNSW vector index, and one HVQ vector index
  (`WITH OPTIONS={'method':'hvq'}`) — demonstrating every CQL-reachable index
  strategy in one place ("all of the indexes").
- `data.cql` — inserts with clustered embeddings whose nearest neighbour is known.
- `queries.cql` — scalar lookups plus `ORDER BY … ANN OF … LIMIT` against both
  the HNSW and HVQ indexes.
- `smoke-test.sh` — asserts the ANN query returns the known nearest neighbour
  (fail-loud: non-zero exit if the expected row is absent), so CI validates
  correctness, not just "cqlsh didn't error".
- `vector-indexes.adoc` — tutorial that `include::`s the `.cql` files.
- Register the example in `examples/index.adoc` (new "Machine Learning & AI"
  section or under an existing one).

Because `ci.yml`'s examples job runs `queries.cql` against a real node, the HVQ
CQL path from Workstream 1 must be fully wired end-to-end (DDL → storage → ANN
query) for this example to pass — verified locally before pushing.

## Sequencing

WS1 (working CQL, TDD) → WS2 (eval numbers) → WS5 (runnable example, depends on
WS1) → WS3 + WS4 (docs cite real syntax, real numbers, and link the example).
All on branch `docs/hvq-vector-indexes`; no commits to main.

## Verification

- `cargo test -p ferrosa-cql` (router method-selection tests green).
- `cargo test -p ferrosa-index eval_comparison -- --nocapture` produces the table.
- The `examples/vector-indexes` `.cql` + `smoke-test.sh` run green against a live
  single node (the same path CI exercises); HVQ ANN query returns the known
  nearest neighbour.
- `make -C examples html` regenerates `docs/database/examples/vector-indexes.html`.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` clean for
  touched crates.
- Reference + home pages render and all links resolve.
