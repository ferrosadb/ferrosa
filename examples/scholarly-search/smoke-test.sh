#!/usr/bin/env bash
# Smoke-test the scholarly-search example contract without requiring a cluster.
# The live path (schema.cql -> data.cql -> queries.cql against a running node)
# is exercised by the CI examples job; this script enforces that the example
# keeps demonstrating all five lenses over one corpus.

set -euo pipefail
cd "$(dirname "$0")"

require_text() {
  grep -Fq "$2" "$1" || { echo "FAIL: $1 is missing: $2" >&2; exit 1; }
}
require_regex() {
  grep -Eq "$2" "$1" || { echo "FAIL: $1 does not match: $2" >&2; exit 1; }
}

# Lens 1 -- keyword (substring LIKE).
require_text queries.cql "LIKE '%"

# Lens 2 -- vector ANN over HVQ-quantized real embeddings.
require_regex schema.cql "USING 'vector'"
require_text schema.cql "WITH OPTIONS = {'method': 'hvq'}"
require_text schema.cql "vector<float, 768>"
require_text queries.cql "ANN OF"

# Lens 3 -- phonetic author search.
require_text schema.cql "USING 'phonetic'"
require_text queries.cql "SOUNDS LIKE"

# Lens 4 -- RRD consolidation (trend signal).
require_text schema.cql "'consolidation.interval': '168h'"
require_text schema.cql "'consolidation.target': 'paper_citations_weekly'"

# Lens 5 -- property graph over the same tables.
require_text schema.cql "'graph.type': 'vertex'"
require_text schema.cql "'graph.type': 'edge'"
[ -x cypher-queries.sh ] || { echo "FAIL: cypher-queries.sh must be executable" >&2; exit 1; }

# Embeddings are real (generated), not placeholders.
require_text data.cql "nomic-embed-text-v2-moe"
require_regex data.cql "INSERT INTO paper .*embedding.* VALUES .*\[0?-?[0-9.]+, "
[ "$(grep -c '@@Q:' queries.cql || true)" -eq 0 ] || { echo "FAIL: unfilled query-vector placeholder" >&2; exit 1; }

echo "scholarly-search: all five lenses present; embeddings baked in. OK"
