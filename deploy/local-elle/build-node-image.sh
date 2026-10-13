#!/usr/bin/env bash
# Build a node image for the LOCAL Elle certification rig from THIS worktree.
#
# Why this exists: the Elle certification needs a 3-node RF=3 cluster running
# the code under test. In CI that image comes from `build-node-image` (a
# release build of `-p ferrosa -p ferrosa-ctl` on the runner, packaged into a
# thin runtime image). The prebuilt CI artifact exists only for amd64, and
# qemu-user emulation of that binary segfaults on an Apple-silicon podman VM
# (`qemu: uncaught target signal 11`) — so on this host the image must be
# built natively, from the same source, with the same runtime recipe.
#
# The two repo-owned pieces this reuses UNCHANGED:
#   * the builder base image the repo's own Dockerfile builds from
#     (`rust:1.94`, plus capnproto) — same toolchain, same crate;
#   * `tests/Dockerfile.nightly-bin`, the repo's "binary + thin runtime"
#     image recipe, which already installs `curl` (the elle compose
#     healthcheck probes it) alongside ca-certificates/gdb/procps.
#
# Nothing here defines a cluster. The cluster is
# `ferrosa-jepsen/tests/docker/elle-cluster-rf3.yml`, driven by certify-cql.sh.
#
# Usage:
#   deploy/local-elle/build-node-image.sh [worktree] [image-tag]
# Env:
#   BUILDER_IMAGE   rust builder base (default docker.io/library/rust:1.94)
#   JOBS            cargo build parallelism (default 3; rustc/LLVM release
#                   builds are memory-heavy and the podman VM is small)
#
# Idempotent: re-running rebuilds the same tag; the builder container is
# removed on every exit path.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKTREE="${1:-$ROOT_DIR}"
IMAGE="${2:-localhost/ferrosa-elle-current:latest}"
BUILDER_IMAGE="${BUILDER_IMAGE:-docker.io/library/rust:1.94}"
JOBS="${JOBS:-3}"
STAGE_DIR="$WORKTREE/tests"
BIN_SRC="/build/target/release"

log() { printf '\n[build-node-image] %s\n' "$*" >&2; }
die() { printf '\n[build-node-image][FATAL] %s\n' "$*" >&2; exit 1; }

command -v podman >/dev/null || die "podman not on PATH"
[ -f "$WORKTREE/Cargo.toml" ] || die "no Cargo.toml in $WORKTREE"
[ -f "$STAGE_DIR/Dockerfile.nightly-bin" ] || die "missing $STAGE_DIR/Dockerfile.nightly-bin"

# A target dir on the HOST (through the podman machine's /Users mount) rather
# than inside the VM: a release build of this workspace does not fit in the
# VM's remaining disk, and a failed build must not take the VM down with it.
TARGET_DIR="${CARGO_TARGET_DIR_HOST:-$(dirname "$WORKTREE")/.target-elle-linux}"
mkdir -p "$TARGET_DIR"

cleanup() { podman rm -f elle-node-image-build >/dev/null 2>&1 || true; }
trap cleanup EXIT

log "building -p ferrosa -p ferrosa-ctl for $(podman info --format '{{.Host.Arch}}' 2>/dev/null || echo "$(uname -m)")/linux"
log "  source:   $WORKTREE"
log "  builder:  $BUILDER_IMAGE"
log "  target:   $TARGET_DIR"
cleanup
podman run --name elle-node-image-build --user 0 \
  -v "$WORKTREE":/build/repo \
  -v "$TARGET_DIR":/build/target \
  -w /build/repo \
  -e CARGO_TARGET_DIR=/build/target \
  -e CARGO_BUILD_JOBS="$JOBS" \
  -e CARGO_INCREMENTAL=0 \
  "$BUILDER_IMAGE" \
  bash -euo pipefail -c '
    export DEBIAN_FRONTEND=noninteractive
    # capnproto is a build-time dependency of this workspace (see the repo
    # Dockerfile). Installed here so the builder base can stay the plain
    # rust image the repo pins.
    command -v capnp >/dev/null || { apt-get update -qq && apt-get install -y -qq --no-install-recommends capnproto >/dev/null; }
    cargo build --release -p ferrosa -p ferrosa-ctl
  '

# Stage the binaries where tests/Dockerfile.nightly-bin expects them.
# NOTE: this writes two build products into the worktree; they are removed
# again below, and both names are covered by tests/.gitignore via the
# `ferrosa*` pattern — verify with `git status` if that ever changes.
log "staging binaries into $STAGE_DIR/"
install -m 0755 "$TARGET_DIR/release/ferrosa" "$STAGE_DIR/ferrosa"
install -m 0755 "$TARGET_DIR/release/ferrosa-ctl" "$STAGE_DIR/ferrosa-ctl"

log "building thin runtime image $IMAGE (repo recipe: tests/Dockerfile.nightly-bin)"
podman build -f "$STAGE_DIR/Dockerfile.nightly-bin" -t "$IMAGE" "$STAGE_DIR"

rm -f "$STAGE_DIR/ferrosa" "$STAGE_DIR/ferrosa-ctl"

log "image ready:"
podman image inspect "$IMAGE" --format '  {{.Id}} {{.Architecture}}/{{.Os}} created={{.Created}}'
