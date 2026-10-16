#!/usr/bin/env bash
# pgo-build.sh — profile-guided optimization build for the shipped `ferrosa` binary.
#
#   scripts/pgo-build.sh [--baseline] [--verify] [--keep]
#
# Implements the rustc PGO workflow (instrument -> run -> merge -> use) for the
# artifact we actually deploy, with the failure modes this repo can hit turned
# into hard errors.
#
# WHY THIS SCRIPT EXISTS RATHER THAN FOUR COMMANDS IN A README
#
# The workflow has three silent-failure modes, each of which produces a build
# that *looks* optimized and is not:
#
#   1. No profile was collected (the workload never ran, or died). `-Cprofile-use`
#      with a missing file is an error, but an empty directory merged into a
#      zero-count profile is not.
#   2. The profile has zero counts. Measured on the memory toolchain: an
#      instrumented process SIGKILLed in continuous mode writes a profraw whose
#      header looks correct and whose `Total count` is 0. Feeding that to the
#      compiler emits "stale profile" warnings and optimizes nothing.
#   3. The workload stopped exercising the code (a knob drifted, so every call
#      errors). The profile is valid and worthless.
#
# So: no profile data -> error. Zero-count profile -> error. Workload below its
# success budget -> error. A PGO build must never silently degrade to a normal
# build.
#
# The training workload is the `pgo-workload` bin (ferrosa/src/bin), a
# deterministic in-process run of the real StorageEngine. It needs no cluster,
# which is what makes this runnable in CI and inside the image build. For the
# production-faithful pass against a live cluster, see
# specs/pgo-release-build.md.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The crate that carries the training workload and the crate we ship. Both are
# the same package: the workload is a second bin of `ferrosa`, so the shipped
# binary and the profiled binary come from one package with one feature set.
# That is the whole reason it is not a separate crate — see the workload's doc
# comment for the `-C metadata` argument.
SHIP_PACKAGE="ferrosa"
# The feature set BOTH the instrumented workload build and the optimized ship
# build must use, and it must be IDENTICAL between them.
#
# `-C metadata` — which cargo derives from the package, version, target, profile
# and the crate's own enabled feature set — feeds the crate disambiguator rustc
# bakes into every mangled symbol and matches profile data against. Enable a
# feature on one side only and the two builds disagree; `-Cprofile-use` against
# the wrong one reports the crate's own functions as `no profile data available
# for function` and discards them, while exiting 0 and printing `Finished`
# (measured — see specs/pgo-release-build.md).
#
# `pgo-bench` gates a bin target and nothing else, so it is inert on the serving
# path: it adds no code to the `ferrosa` bin and enables nothing in any
# dependency. Do NOT "clean this up" by trimming it from one side — that
# silently reverts to an unoptimized build with a green exit code.
PGO_FEATURES="pgo-bench"
TRAIN_BIN="pgo-workload"
SHIP_BIN_NAME="ferrosa"
PROFILE_DIR="${PGO_PROFILE_DIR:-$REPO_ROOT/target/pgo-profile}"
BASELINE=0
VERIFY=0
KEEP=0

usage() {
  sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Flags:
  --baseline          Also build a plain release binary for an honest A/B
  --verify            Re-run the workload against the optimized build
  --keep              Keep the instrumented artifacts after the build
  -h, --help          This message

Environment:
  PGO_TARGET          Cargo --target triple (default: host, via rustc -vV)
  PGO_CODEGEN_UNITS   -Ccodegen-units; set to 1 to let PGO inline across CGUs
  PGO_PROFILE_DIR     Where profraw/.profdata live (default: target/pgo-profile)
  PGO_WORKLOAD_*      Forwarded to the workload (iterations, seed, min ok rate)
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --baseline) BASELINE=1; shift ;;
    --verify) VERIFY=1; shift ;;
    --keep) KEEP=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "pgo-build: unknown argument $1" >&2; usage >&2; exit 2 ;;
  esac
done

say() { printf '\n=== %s\n' "$*"; }
die() { printf 'pgo-build: %s\n' "$*" >&2; exit 1; }

# --------------------------------------------------------------------------
# Toolchain: llvm-profdata MUST come from the toolchain that will consume the
# profile. A Homebrew LLVM of a different major version can merge the data but
# the compiler's reader is what defines the format; mismatches surface as
# "counter mismatch" / "stale profile" at compile time.
# --------------------------------------------------------------------------
HOST_TRIPLE="$(rustc -vV | awk '/^host:/{print $2}')"
[ -n "$HOST_TRIPLE" ] || die "cannot determine host triple from rustc -vV"
TARGET="${PGO_TARGET:-$HOST_TRIPLE}"

PROFDATA="$(rustc --print sysroot 2>/dev/null)/lib/rustlib/$HOST_TRIPLE/bin/llvm-profdata"
if [ ! -x "$PROFDATA" ]; then
  PROFDATA="$(command -v llvm-profdata || true)"
  [ -n "$PROFDATA" ] || die \
    "llvm-profdata not found. Install it with:
    rustup component add llvm-tools-preview
  (the binary lives in \$(rustc --print sysroot)/lib/rustlib/$HOST_TRIPLE/bin/)"
  echo "pgo-build: using llvm-profdata from PATH ($PROFDATA)" >&2
fi

export PATH="$HOME/.cargo/bin:$PATH"
command -v cargo >/dev/null || die "cargo not on PATH"

# Absolute paths: cargo invokes rustc from varying working directories, so a
# relative -Cprofile-* path is resolved against the wrong place.
MERGED="$PROFILE_DIR/merged.profdata"

RUSTFLAGS_BASE=""
[ -n "${PGO_CODEGEN_UNITS:-}" ] && RUSTFLAGS_BASE="-Ccodegen-units=${PGO_CODEGEN_UNITS}"

TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
OUT_DIR="$TARGET_DIR/$TARGET/release"
TRAIN_BIN_PATH="$OUT_DIR/$TRAIN_BIN"
SHIP_BIN="$OUT_DIR/$SHIP_BIN_NAME"

say "PGO build"
cat <<EOF
  repo            $REPO_ROOT
  package         $SHIP_PACKAGE (features: $PGO_FEATURES)
  train bin       $TRAIN_BIN
  ship bin        $SHIP_BIN_NAME
  target          $TARGET
  profile dir     $PROFILE_DIR
  merged profile  $MERGED
  llvm-profdata   $PROFDATA
  cargo           $(cargo --version)
  rustc           $(rustc --version)
EOF

# --------------------------------------------------------------------------
# STEP 0 — clean. Left-over profraw from an earlier session is merged silently
# and skews the profile toward whatever ran last.
# --------------------------------------------------------------------------
say "STEP 0: clean profile directory"
rm -rf "$PROFILE_DIR"
mkdir -p "$PROFILE_DIR"

# --------------------------------------------------------------------------
# STEP 1 — instrumented build. `--target` keeps RUSTFLAGS away from build
# scripts, which would otherwise emit profraw files of their own.
# --------------------------------------------------------------------------
say "STEP 1: build instrumented workload runner"
RUSTFLAGS="-Cprofile-generate=$PROFILE_DIR $RUSTFLAGS_BASE" \
  cargo build --release --target "$TARGET" \
    -p "$SHIP_PACKAGE" --features "$PGO_FEATURES" --bin "$TRAIN_BIN"
[ -x "$TRAIN_BIN_PATH" ] || die "instrumented workload runner not found at $TRAIN_BIN_PATH"

# --------------------------------------------------------------------------
# STEP 2 — run it. It exits non-zero when it fell below its success budget.
# --------------------------------------------------------------------------
say "STEP 2: run the training workload"
"$TRAIN_BIN_PATH" --check-hits || die "workload failed; refusing to build a profile from it"

# --------------------------------------------------------------------------
# STEP 2b — assert data exists. This is the guard against a build that quietly
# becomes a normal build.
# --------------------------------------------------------------------------
say "STEP 2b: assert the run produced profile data"
shopt -s nullglob
PROFRAW=("$PROFILE_DIR"/*.profraw)
shopt -u nullglob
[ "${#PROFRAW[@]}" -gt 0 ] || die \
  "the workload produced no .profraw files in $PROFILE_DIR.
  An instrumented binary writes them on a clean exit; none means the run did
  not execute the instrumentation (or the process was killed)."

# --------------------------------------------------------------------------
# STEP 3 — merge.
# --------------------------------------------------------------------------
say "STEP 3: merge with llvm-profdata"
"$PROFDATA" merge -o "$MERGED" "$PROFILE_DIR"
[ -s "$MERGED" ] || die "llvm-profdata merge produced an empty $MERGED"

# --------------------------------------------------------------------------
# STEP 3b — assert the merged profile carries counts. A zero-count profile is
# accepted by the compiler and optimizes nothing; it is the single most
# misleading PGO failure we measured.
# --------------------------------------------------------------------------
say "STEP 3b: assert the merged profile has counts"
PROFILE_TOTAL="$("$PROFDATA" show "$MERGED" | awk '/^Total count:/{print $3}')"
[ -n "$PROFILE_TOTAL" ] || die "cannot read 'Total count' out of $MERGED"
if [ "$PROFILE_TOTAL" = "0" ]; then
  die "merged profile has Total count: 0 — it would optimize nothing.
  This happens when the profiled process did not exit cleanly. See
  specs/pgo-release-build.md."
fi
PROFILE_FNS="$("$PROFDATA" show --all-functions "$MERGED" | grep -c 'Counters:' || true)"
echo "  merged profile: total_count=$PROFILE_TOTAL instrumented_functions=$PROFILE_FNS"

# --------------------------------------------------------------------------
# STEP 4 — profile-use build.
# --------------------------------------------------------------------------
say "STEP 4: build with -Cprofile-use"
# `--features "$PGO_FEATURES"` is not optional: see the PGO_FEATURES definition.
# Without it LLVM discards the crate's profile and the build is silently not
# optimized.
#
# stderr is captured rather than streamed: STEP 5 reads it to count functions the
# profile did not cover, and that count is the only signal that STEP 4 was
# effective. `set -o pipefail` keeps cargo's exit status.
PROFILE_USE_LOG="${PGO_PROFILE_USE_STDERR:-$PROFILE_DIR/profile-use.log}"
RUSTFLAGS="-Cprofile-use=$MERGED -Cllvm-args=-pgo-warn-missing-function $RUSTFLAGS_BASE" \
  cargo build --release --target "$TARGET" -p "$SHIP_PACKAGE" \
    --features "$PGO_FEATURES" --bin "$SHIP_BIN_NAME" 2>&1 | tee "$PROFILE_USE_LOG"
[ -f "$SHIP_BIN" ] || die "expected the shipped binary at $SHIP_BIN after STEP 4"

if [ "$BASELINE" -eq 1 ]; then
  say "STEP 4b: baseline (no PGO) build for comparison"
  # Same feature set as STEP 4, so the only difference between the two binaries
  # is the profile. Otherwise the A/B compares two different builds.
  CARGO_TARGET_DIR="$REPO_ROOT/target/pgo-baseline" \
    RUSTFLAGS="$RUSTFLAGS_BASE" \
    cargo build --release --target "$TARGET" -p "$SHIP_PACKAGE" \
      --features "$PGO_FEATURES" --bin "$SHIP_BIN_NAME"
fi

if [ "$VERIFY" -eq 1 ]; then
  say "STEP 4c: verify the optimized build still behaves"
  # Rebuild the runner with the profile applied, so the behavior check runs the
  # same optimized core the shipped binary links.
  RUSTFLAGS="-Cprofile-use=$MERGED -Cllvm-args=-pgo-warn-missing-function $RUSTFLAGS_BASE" \
    cargo build --release --target "$TARGET" \
      -p "$SHIP_PACKAGE" --features "$PGO_FEATURES" --bin "$TRAIN_BIN"
  "$TRAIN_BIN_PATH" --check-hits --json || die "optimized build failed its workload check"
fi

# --------------------------------------------------------------------------
# STEP 5 — assert the profile actually reached the shipped crates.
#
# This is the check that would have caught the feature-disambiguator defect: a
# profile-use build happily succeeds while discarding every function it cannot
# match. If a shipped crate's own functions are all reported as having no
# profile data, STEP 4 produced an unoptimized binary that looks fine.
# --------------------------------------------------------------------------
say "STEP 5: the profile reached the shipped crates"
if [ -f "$PROFILE_USE_LOG" ]; then
  MISSING="$(grep -c 'no profile data available for function' "$PROFILE_USE_LOG" || true)"
  echo "  functions without profile data: $MISSING  (log: $PROFILE_USE_LOG)"
  echo "  A handful is expected: the dependency crates the workload never called,"
  echo "  plus the shipped bin's own main() (the instrumented run executed the"
  echo "  trainer bin, not the server bin). A count that rises when the ship"
  echo "  features change means the profile is not matching the crate it was"
  echo "  built for."
else
  echo "  skipped: no profile-use log at $PROFILE_USE_LOG"
fi

if [ "$KEEP" -eq 0 ]; then
  # Keep merged.profdata (it is the input to STEP 4 and worth archiving); drop
  # the raw profraw files.
  find "$PROFILE_DIR" -name '*.profraw' -delete
fi

# Portable checksum: macOS ships `shasum`, Debian's rust image ships `sha256sum`
# and may have no perl. Getting this wrong fails the build on the last line,
# after every compile has already succeeded.
sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

say "done"
echo "  merged profile   $MERGED ($(wc -c <"$MERGED" | tr -d ' ') bytes)"
echo "  optimized binary $SHIP_BIN ($(wc -c <"$SHIP_BIN" | tr -d ' ') bytes)"
echo "  sha256           $(sha256_of "$SHIP_BIN")"
if [ "$BASELINE" -eq 1 ]; then
  BASE_BIN="$REPO_ROOT/target/pgo-baseline/$TARGET/release/$(basename "$SHIP_BIN")"
  if [ -f "$BASE_BIN" ]; then
    B=$(wc -c <"$BASE_BIN" | tr -d ' ')
    P=$(wc -c <"$SHIP_BIN" | tr -d ' ')
    echo "  baseline binary  $BASE_BIN ($B bytes)"
    echo "  size delta       $((P - B)) bytes ($(awk -v b="$B" -v p="$P" 'BEGIN{printf "%+.2f%%", (p-b)*100/b}'))"
  fi
fi
