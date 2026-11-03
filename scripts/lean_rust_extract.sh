#!/usr/bin/env bash
# Extract a Rust crate (or one call graph of it) into Lean, and record where each Lean item came
# from. Charon reads MIR and writes LLBC; Aeneas turns LLBC into Lean whose docstrings carry the
# `Source: '<file>', lines A:B` span of the item. That span table is what lets a red claim point at
# a line of implementation without anybody writing the line number down.
#
# MEMORY. Full-crate translation of ferrosa-storage was OOM-killed: charon-driver translates every
# item reachable from the crate root, and this tree reaches tokio, object_store and rustls. Always
# pass `--start-from` for the module you care about, and keep the address-space cap — without it
# the machine starts swapping and takes the desktop with it.
#
# TIME. `lake build` of the Aeneas Lean backend pulls Mathlib: hours and many gigabytes. That is
# deliberately NOT done here. The extract and its span table are the deliverable; type-checking the
# extract against Aeneas is a separate, watched run.
set -euo pipefail

CRATE=${1:?usage: lean_rust_extract.sh <crate-dir> <start-from-pattern>}
START=${2:?usage: lean_rust_extract.sh <crate-dir> <start-from-pattern>}
MEM=${MEM:-12G}

export PATH="$HOME/.local/aeneas:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.local/share/cargo-target}"

cd "$(dirname "$0")/.."
OUT=specs/lean4/rust-extract
mkdir -p "$OUT"

echo "charon: $CRATE from $START (address space capped at $MEM)"
( ulimit -v $(( ${MEM%G} * 1024 * 1024 )) && cd "$CRATE" && charon cargo --preset=aeneas \
    --start-from "$START" -- --lib ) 2>&1 | tail -5

LLBC=$(ls -t ./*.llbc | head -1)
echo "aeneas: $LLBC"
( cd "$OUT" && aeneas -backend lean "../../../$(basename "$LLBC")" ) 2>&1 | tail -3

python3 scripts/rust_spans.py "$OUT"/*.lean
echo "spans: specs/lean4/docs/rust_spans.json"
