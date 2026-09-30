#!/bin/bash
# Tests for scripts/ci/guard-serde-json-features.sh and scripts/ci/guard-arrow-free.sh
# (T-025: ci_serde_json_feature_guard, ci_arrow_free_guard). Each case builds a
# throwaway one-crate workspace and runs the real guard against real `cargo tree`,
# so a guard that silently reads nothing fails here. Needs the crates in the local
# cargo registry (offline); CI has them after any prior build.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SJ="$ROOT/scripts/ci/guard-serde-json-features.sh"
AF="$ROOT/scripts/ci/guard-arrow-free.sh"
export CARGO_NET_OFFLINE=true
fail=0

# ws <deps-toml> -> prints a temp workspace dir with crate `victim`
ws() {
  local d; d=$(mktemp -d)
  mkdir -p "$d/src"
  : > "$d/src/lib.rs"
  printf '[package]\nname = "victim"\nversion = "0.0.0"\nedition = "2021"\n[dependencies]\n%s\n' "$1" > "$d/Cargo.toml"
  # Reuse the real lockfile so offline resolution picks already-downloaded versions.
  cp "$ROOT/Cargo.lock" "$d/Cargo.lock"
  echo "$d"
}
expect() { # name want-exit guard-and-args... (GUARD_MANIFEST_PATH from $MP)
  local name=$1 want=$2; shift 2
  local out rc
  out=$(GUARD_MANIFEST_PATH="$MP" "$@" 2>&1); rc=$?
  if [ "$rc" = "$want" ]; then echo "ok   $name"; else echo "FAIL $name: exit $rc, want $want"; echo "$out" | tail -4 | sed 's/^/     /'; fail=1; fi
}

D=$(ws 'serde_json = "=1.0.149"'); MP="$D/Cargo.toml"
expect "serde_json default features pass" 0 bash "$SJ"
rm -rf "$D"
D=$(ws 'serde_json = { version = "=1.0.149", features = ["arbitrary_precision"] }'); MP="$D/Cargo.toml"
expect "arbitrary_precision is refused" 1 bash "$SJ"
rm -rf "$D"
D=$(ws 'serde_json = { version = "=1.0.149", features = ["preserve_order"] }'); MP="$D/Cargo.toml"
expect "preserve_order is refused" 1 bash "$SJ"
rm -rf "$D"
D=$(ws 'serde_json = { version = "=1.0.149", features = ["unbounded_depth"] }'); MP="$D/Cargo.toml"
expect "unbounded_depth is refused" 1 bash "$SJ"
rm -rf "$D"
D=$(ws ''); MP="$D/Cargo.toml"
expect "no serde_json in the graph is an input error, not a pass" 2 bash "$SJ"
expect "arrow guard: no args is a usage error" 2 bash "$AF"
expect "arrow guard: unknown crate is an input error" 2 bash "$AF" nonexistent
expect "arrow guard: arrow-free crate passes" 0 bash "$AF" victim
rm -rf "$D"
D=$(ws 'arrow-array = "=53.4.1"'); MP="$D/Cargo.toml"
expect "arrow guard: seeded arrow-array is refused" 1 bash "$AF" victim
rm -rf "$D"
exit $fail
