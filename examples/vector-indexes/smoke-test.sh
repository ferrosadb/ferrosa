#!/usr/bin/env bash
# Smoke-test the vector-index example contract without requiring a cluster.
# The live execution path (schema.cql -> data.cql -> queries.cql against a
# running node) is exercised by the CI examples job; this script enforces that
# the example keeps demonstrating all three index strategies.

set -euo pipefail

cd "$(dirname "$0")"

require_text() {
  local file="$1"
  local pattern="$2"
  if ! grep -Fq "$pattern" "$file"; then
    echo "FAIL: ${file} is missing: ${pattern}" >&2
    exit 1
  fi
}

require_regex() {
  local file="$1"
  local pattern="$2"
  if ! grep -Eq "$pattern" "$file"; then
    echo "FAIL: ${file} does not match: ${pattern}" >&2
    exit 1
  fi
}

# 1) BTree secondary index on a scalar column.
require_regex schema.cql "CREATE INDEX articles_category_idx ON articles_hnsw \(category\)"

# 2) Default HNSW vector index.
require_regex schema.cql "CREATE INDEX articles_hnsw_ann ON articles_hnsw \(embedding\) USING 'vector'"

# 3) Quantized HVQ vector index selected via the method option.
require_text schema.cql "WITH OPTIONS = {'method': 'hvq'}"

# Data is loaded into both the HNSW and HVQ tables as list literals.
require_text data.cql "INSERT INTO articles_hnsw"
require_text data.cql "INSERT INTO articles_hvq"

# Queries exercise the BTree lookup plus ANN against both vector indexes.
require_text queries.cql "WHERE category = 'science'"
require_text queries.cql "FROM articles_hnsw"
require_text queries.cql "FROM articles_hvq"
require_text queries.cql "ORDER BY embedding ANN OF"
require_text queries.cql "ANN OF [0.90, 0.10, 0.00, 0.00]"

echo "PASS: vector-indexes example contract holds (BTree + HNSW + HVQ)."
