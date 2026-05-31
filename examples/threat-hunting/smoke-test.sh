#!/usr/bin/env bash
# Smoke-test the threat-hunting example contract without requiring a cluster.
# The live path (schema -> data -> queries against a running node) is exercised
# by the CI examples job; this enforces that the example keeps demonstrating all
# five lenses over one indicator corpus.

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
# Lens 3 -- phonetic actor search.
require_text schema.cql "USING 'phonetic'"
require_text queries.cql "SOUNDS LIKE"
# Lens 4 -- RRD consolidation (spike signal).
require_text schema.cql "'consolidation.interval': '1h'"
require_text schema.cql "'consolidation.target': 'indicator_activity_hourly'"
# Lens 5 -- property graph over the same tables.
require_text schema.cql "'graph.type': 'vertex'"
require_text schema.cql "'graph.type': 'edge'"
[ -x cypher-queries.sh ] || { echo "FAIL: cypher-queries.sh must be executable" >&2; exit 1; }

# Embeddings are real (generated), not placeholders.
require_text data.cql "nomic-embed-text-v2-moe"
require_regex data.cql "INSERT INTO indicator .*embedding.* VALUES "
[ "$(grep -c '@@Q:' queries.cql || true)" -eq 0 ] || { echo "FAIL: unfilled query-vector placeholder" >&2; exit 1; }

echo "threat-hunting: all five lenses present; embeddings baked in. OK"
