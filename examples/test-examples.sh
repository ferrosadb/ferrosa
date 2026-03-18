#!/usr/bin/env bash
# test-examples.sh — Run all Ferrosa example CQL/Cypher scripts against a Docker cluster.
# Exit 0 if all pass, 1 if any fail.

set -euo pipefail

FERROSA_HOST="${FERROSA_HOST:-localhost}"
FERROSA_CQL_PORT="${FERROSA_CQL_PORT:-9042}"
COMPOSE_FILE="cluster-setup/docker-compose.yml"
TIMEOUT=120

passed=0
failed=0
failed_dirs=""

# ── Cleanup trap ──
cleanup() {
  echo ""
  echo "Tearing down cluster..."
  docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}
trap cleanup EXIT

# ── Build the Ferrosa image ──
echo "=== Building Ferrosa image ==="
docker compose -f "$COMPOSE_FILE" build node1

# ── Start the cluster ──
echo "=== Starting 3-node cluster ==="
docker compose -f "$COMPOSE_FILE" up -d

# ── Wait for CQL to become available ──
echo "=== Waiting for CQL on ${FERROSA_HOST}:${FERROSA_CQL_PORT} (timeout: ${TIMEOUT}s) ==="
elapsed=0
while ! nc -z "$FERROSA_HOST" "$FERROSA_CQL_PORT" 2>/dev/null; do
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "FAIL: CQL port ${FERROSA_CQL_PORT} not available after ${TIMEOUT}s"
    echo ""
    echo "=== Node logs ==="
    docker compose -f "$COMPOSE_FILE" logs --tail=50 node1 node2 node3
    exit 1
  fi
  sleep 2
  elapsed=$((elapsed + 2))
  if [ $((elapsed % 10)) -eq 0 ]; then
    echo "  ...${elapsed}s elapsed"
  fi
done
echo "CQL is ready (${elapsed}s)"

# ── Run cluster-setup verification first ──
echo ""
echo "=== Running cluster-setup/setup.cql ==="
if cqlsh "$FERROSA_HOST" "$FERROSA_CQL_PORT" -f cluster-setup/setup.cql; then
  echo "PASS: cluster-setup"
  passed=$((passed + 1))
else
  echo "FAIL: cluster-setup"
  failed=$((failed + 1))
  failed_dirs="cluster-setup ${failed_dirs}"
fi

# ── Run each example directory ──
for dir in $(find . -mindepth 1 -maxdepth 1 -type d ! -name theme ! -name cluster-setup | sort); do
  dir="${dir#./}"
  echo ""
  echo "=== Running example: ${dir} ==="
  dir_ok=true

  # schema.cql
  if [ -f "${dir}/schema.cql" ]; then
    echo "  -> ${dir}/schema.cql"
    if ! cqlsh "$FERROSA_HOST" "$FERROSA_CQL_PORT" -f "${dir}/schema.cql"; then
      echo "  FAIL: ${dir}/schema.cql"
      dir_ok=false
    fi
  fi

  # data.cql
  if [ -f "${dir}/data.cql" ]; then
    echo "  -> ${dir}/data.cql"
    if ! cqlsh "$FERROSA_HOST" "$FERROSA_CQL_PORT" -f "${dir}/data.cql"; then
      echo "  FAIL: ${dir}/data.cql"
      dir_ok=false
    fi
  fi

  # queries.cql
  if [ -f "${dir}/queries.cql" ]; then
    echo "  -> ${dir}/queries.cql"
    if ! cqlsh "$FERROSA_HOST" "$FERROSA_CQL_PORT" -f "${dir}/queries.cql"; then
      echo "  FAIL: ${dir}/queries.cql"
      dir_ok=false
    fi
  fi

  # cypher-queries.sh
  if [ -x "${dir}/cypher-queries.sh" ]; then
    echo "  -> ${dir}/cypher-queries.sh"
    if ! bash "${dir}/cypher-queries.sh"; then
      echo "  FAIL: ${dir}/cypher-queries.sh"
      dir_ok=false
    fi
  fi

  if [ "$dir_ok" = true ]; then
    # Only count directories that had at least one script to run
    if [ -f "${dir}/schema.cql" ] || [ -f "${dir}/data.cql" ] || \
       [ -f "${dir}/queries.cql" ] || [ -x "${dir}/cypher-queries.sh" ]; then
      echo "  PASS: ${dir}"
      passed=$((passed + 1))
    else
      echo "  SKIP: ${dir} (no CQL or Cypher scripts found)"
    fi
  else
    echo "  FAIL: ${dir}"
    failed=$((failed + 1))
    failed_dirs="${dir} ${failed_dirs}"
  fi
done

# ── Summary ──
echo ""
echo "========================================"
echo "  Examples test summary"
echo "========================================"
echo "  Passed: ${passed}"
echo "  Failed: ${failed}"
if [ -n "$failed_dirs" ]; then
  echo "  Failed: ${failed_dirs}"
fi
echo "========================================"

if [ "$failed" -gt 0 ]; then
  exit 1
fi
exit 0
