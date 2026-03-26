#!/usr/bin/env bash
# musl-build.sh — Build, verify, and strip static musl binaries.
# Matches CI's "Static Build (musl)" stage exactly.
set -euo pipefail

TARGET="x86_64-unknown-linux-musl"
RELEASE_DIR="target/$TARGET/release"

echo "=== Building musl static binaries ==="
cargo build --release --target "$TARGET" -p ferrosa -p ferrosa-ctl

echo "=== Verifying statically linked ==="
file "$RELEASE_DIR/ferrosa"
file "$RELEASE_DIR/ferrosa" | grep -qE "statically linked|static-pie linked|static"

echo "=== Stripping ==="
strip "$RELEASE_DIR/ferrosa" "$RELEASE_DIR/ferrosa-ctl"
echo "ferrosa:     $(du -h "$RELEASE_DIR/ferrosa" | cut -f1)"
echo "ferrosa-ctl: $(du -h "$RELEASE_DIR/ferrosa-ctl" | cut -f1)"
