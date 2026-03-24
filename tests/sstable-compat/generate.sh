#!/usr/bin/env bash
# Generate Cassandra SSTable test fixtures using the real Cassandra writer.
#
# The generated fixtures are placed in ferrosa-sstable/tests/fixtures/cassandra_generated/
# and are used by cassandra_compat.rs integration tests to verify ferrosa can
# read SSTables produced by Cassandra.
#
# Prerequisites: Docker
#
# Usage:
#   ./tests/sstable-compat/generate.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="$REPO_ROOT/ferrosa-sstable/tests/fixtures/cassandra_generated"

echo "Building Cassandra SSTable fixture generator..."
docker build -t ferrosa-sstable-gen \
    -f "$SCRIPT_DIR/Dockerfile" \
    "$REPO_ROOT" 2>&1 | tail -5

echo "Generating fixtures..."
mkdir -p "$OUTPUT_DIR"
docker run --rm -v "$OUTPUT_DIR:/output" ferrosa-sstable-gen

echo ""
echo "Fixtures generated in: $OUTPUT_DIR"
find "$OUTPUT_DIR" -name "*.db" | wc -l
echo " SSTable component files created"
