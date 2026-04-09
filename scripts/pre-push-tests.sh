#!/usr/bin/env bash
# pre-push-tests.sh — Smart pre-push validation.
#
# 1. Runs cargo test only for changed crates + their dependents
# 2. Always runs example CQL tests against a live Ferrosa cluster
#
# Usage: called by pre-commit hook (pre-push stage)

set -euo pipefail

# ── Reverse dependency map ──────────────────────────────────────────────
# If crate X changes, also test everything that depends on X.
# Generated from: cargo metadata --no-deps
declare -A RDEPS
RDEPS[ferrosa-common]="ferrosa-index ferrosa-schema ferrosa-sstable ferrosa-storage ferrosa-cql ferrosa-cluster ferrosa-graph ferrosa-udf ferrosa-ctl ferrosa"
RDEPS[ferrosa-index]="ferrosa-schema ferrosa-storage ferrosa-cql ferrosa-cluster ferrosa"
RDEPS[ferrosa-net]="ferrosa-cluster ferrosa"
RDEPS[ferrosa-schema]="ferrosa-storage ferrosa-cql ferrosa-cluster ferrosa-graph ferrosa-ctl ferrosa"
RDEPS[ferrosa-sstable]="ferrosa-storage ferrosa-cql ferrosa-cluster ferrosa-graph ferrosa"
RDEPS[ferrosa-storage]="ferrosa-cql ferrosa-cluster ferrosa-graph ferrosa-ctl ferrosa"
RDEPS[ferrosa-udf]="ferrosa-cql ferrosa-ctl ferrosa"
RDEPS[ferrosa-cql]="ferrosa-ctl ferrosa"
RDEPS[ferrosa-cluster]="ferrosa-cql ferrosa-ctl ferrosa"
RDEPS[ferrosa-graph]="ferrosa"
RDEPS[ferrosa-net]="ferrosa-cluster ferrosa"
RDEPS[ferrosa]=""
RDEPS[ferrosa-ctl]=""

# ── Detect changed crates ──────────────────────────────────────────────
echo "=== Detecting changed crates ==="

# Get the remote ref we're pushing to
REMOTE_REF=$(git rev-parse @{push} 2>/dev/null || git rev-parse origin/main 2>/dev/null || echo "HEAD~1")
CHANGED_FILES=$(git diff --name-only "$REMOTE_REF"..HEAD 2>/dev/null || git diff --name-only HEAD~1..HEAD)

# Map changed files to crate names
declare -A CRATES_TO_TEST=()
for file in $CHANGED_FILES; do
  # Match ferrosa-*/... or ferrosa/...
  crate=$(echo "$file" | grep -oP '^(ferrosa(-[a-z]+)*)/' | tr -d '/' || true)
  if [ -n "$crate" ]; then
    CRATES_TO_TEST[$crate]=1
    # Add reverse dependencies
    if [ -n "${RDEPS[$crate]+x}" ]; then
      for dep in ${RDEPS[$crate]}; do
        CRATES_TO_TEST[$dep]=1
      done
    fi
  fi
done

# ── Run cargo test for affected crates ─────────────────────────────────
if [ ${#CRATES_TO_TEST[@]} -eq 0 ]; then
  echo "No Rust crate changes detected — skipping cargo test"
else
  CRATE_LIST=$(echo "${!CRATES_TO_TEST[@]}" | tr ' ' '\n' | sort | tr '\n' ' ')
  echo "Testing crates: $CRATE_LIST"

  CARGO_ARGS=""
  for crate in ${!CRATES_TO_TEST[@]}; do
    CARGO_ARGS="$CARGO_ARGS -p $crate"
  done

  # Don't use --all-features: it enables infrastructure-dependent features
  # (telemetry instrumentation, skiplist-memtable) whose tests require
  # running clusters, containers, or FERROSA_TEST_* env vars and panic
  # without them.  CI runs the full feature matrix with infrastructure.
  #
  # Exclude main.rs binary entry points from coverage — startup code
  # only exercisable via full integration tests (covered in CI).
  echo ""
  if command -v cargo-llvm-cov &> /dev/null; then
    echo "=== Running cargo llvm-cov$CARGO_ARGS ==="
    # Skip: S3/container-gated tests, flaky tracing subscriber tests.
    # Capture exit code so we always echo output before failing.
    set +e
    COV_OUTPUT=$(cargo llvm-cov $CARGO_ARGS --lib --summary-only \
      --ignore-filename-regex '(^|/)main\.rs$' \
      -- --skip cassandra_reads_compacted --skip compaction_end_to_end \
         --skip accord_coordinator_creates_spans \
         --skip commitlog_write_span_is_created 2>&1)
    COV_RC=$?
    set -e
    echo "$COV_OUTPUT"
    if [ "$COV_RC" -ne 0 ]; then
      echo "FAIL: cargo llvm-cov exited with code $COV_RC"
      exit 1
    fi
    # Check 80% coverage threshold (matches CI)
    COVERAGE=$(echo "$COV_OUTPUT" | grep 'TOTAL' | awk '{print $10}' | tr -d '%')
    if [ -n "$COVERAGE" ]; then
      THRESHOLD=80
      if [ "$(echo "$COVERAGE < $THRESHOLD" | bc -l 2>/dev/null || echo 0)" -eq 1 ]; then
        echo "FAIL: Line coverage ${COVERAGE}% is below threshold ${THRESHOLD}%"
        exit 1
      fi
      echo "Coverage ${COVERAGE}% meets threshold ${THRESHOLD}%"
    fi
  else
    echo "=== Running cargo test$CARGO_ARGS ==="
    echo "(install cargo-llvm-cov for coverage: cargo install cargo-llvm-cov)"
    cargo test $CARGO_ARGS --lib -- --skip cassandra_reads_compacted --skip compaction_end_to_end \
      --skip accord_coordinator_creates_spans \
      --skip commitlog_write_span_is_created
  fi
fi

# ── Always run example tests ───────────────────────────────────────────
echo ""
echo "=== Running example CQL tests ==="

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/examples/cluster-setup/docker-compose.yml"
EXAMPLES_DIR="$PROJECT_DIR/examples"

FERROSA_HOST="${FERROSA_HOST:-localhost}"
FERROSA_CQL_PORT="${FERROSA_CQL_PORT:-9042}"

# Check if cqlsh is available
if ! command -v cqlsh &>/dev/null; then
  echo "WARNING: cqlsh not found — skipping example tests"
  echo "Install with: pip install cqlsh"
  exit 0
fi

# Check if docker is available
if ! command -v docker &>/dev/null; then
  echo "WARNING: docker not found — skipping example tests"
  exit 0
fi

# Cleanup on exit
cleanup() {
  echo ""
  echo "=== Tearing down example cluster ==="
  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# Build and start
echo "Building Ferrosa Docker image..."
docker compose -f "$COMPOSE_FILE" build node1 --quiet 2>/dev/null || docker compose -f "$COMPOSE_FILE" build node1

echo "Starting 3-node cluster..."
docker compose -f "$COMPOSE_FILE" up -d

# Wait for CQL
echo "Waiting for CQL port ($FERROSA_CQL_PORT)..."
for i in $(seq 1 120); do
  if docker compose -f "$COMPOSE_FILE" exec -T node1 bash -c '</dev/tcp/127.0.0.1/9042' 2>/dev/null; then
    echo "Ferrosa ready after ${i}s"
    break
  fi
  if [ "$i" -eq 120 ]; then
    echo "FAIL: Ferrosa did not become ready within 120s"
    docker compose -f "$COMPOSE_FILE" logs node1 | tail -30
    exit 1
  fi
  sleep 1
done

# Run each example
pass=0
fail=0
failed_examples=()

for dir in $(find "$EXAMPLES_DIR" -mindepth 1 -maxdepth 1 -type d | sort); do
  name=$(basename "$dir")
  [ "$name" = "theme" ] && continue
  [ "$name" = "cluster-scaling" ] && continue  # self-managed lifecycle

  ok=true
  for f in schema.cql data.cql queries.cql; do
    if [ -f "$dir/$f" ]; then
      output=$(cqlsh "$FERROSA_HOST" "$FERROSA_CQL_PORT" -f "$dir/$f" 2>&1) && rc=0 || rc=$?
      if [ "$rc" -ne 0 ]; then
        # Exit code 2 = warnings only; fail only on real errors
        if [ "$rc" -eq 2 ] && ! echo "$output" | grep -qiE "Error from server|SyntaxException|InvalidRequest|NoHostAvailable|struct\.error|Connection refused"; then
          continue  # warning only, not a real error
        fi
        echo "  FAIL: $name/$f"
        ok=false
        break
      fi
    fi
  done

  if [ -x "$dir/cypher-queries.sh" ]; then
    if ! bash "$dir/cypher-queries.sh" 2>/dev/null; then
      echo "  FAIL: $name/cypher-queries.sh"
      ok=false
    fi
  fi

  if $ok; then
    echo "  PASS  $name"
    pass=$((pass + 1))
  else
    echo "  FAIL  $name"
    fail=$((fail + 1))
    failed_examples+=("$name")
  fi
done

echo ""
echo "Examples: $pass passed, $fail failed"
if [ ${#failed_examples[@]} -gt 0 ]; then
  echo "Failed: ${failed_examples[*]}"
  exit 1
fi
