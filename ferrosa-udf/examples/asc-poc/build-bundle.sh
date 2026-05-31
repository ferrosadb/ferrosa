#!/usr/bin/env bash
# Build the asc + binaryen JS bundle the PoC loads into rquickjs.
# Output: $1 (default /tmp/asc-host/asc-bundle.mjs)
set -euo pipefail
OUT="${1:-/tmp/asc-host/asc-bundle.mjs}"
DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"; cp "$DIR/host-driver.mjs" "$WORK/"; cd "$WORK"
npm init -y >/dev/null
npm install --no-audit --no-fund assemblyscript esbuild
EXT="--external:fs --external:path --external:url --external:module --external:os --external:crypto --external:tty --external:util --external:process --external:worker_threads --external:perf_hooks --external:fs/promises --external:node:fs --external:node:path --external:node:url --external:node:module --external:node:os --external:node:process --external:node:crypto"
mkdir -p "$(dirname "$OUT")"
npx esbuild host-driver.mjs --bundle --format=esm --platform=neutral $EXT --outfile="$OUT"
echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"
