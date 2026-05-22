#!/usr/bin/env bash
# ferrosa-memory fast setup — installs prebuilt binaries via the LATEST file,
# downloads ONBOARDING.md, optionally clones source repos, optionally pulls
# the Nomic embedding model, and hands off to a selected LLM harness.
#
# Reads https://ferrosadb.com/LATEST (a plain-text version tag like "v0.12.0")
# and uses it for both ferrosa and ferrosa-memory release artifacts (the two
# projects ship synchronized tags). No source compile.
#
# Usage:
#   curl -fsSL https://ferrosadb.com/setup-memory.sh | bash
#   curl -fsSL https://ferrosadb.com/setup-memory.sh | bash -s -- --version v0.12.0 --no-clone
#
# Env overrides (mostly for testing):
#   FERROSA_LATEST_URL    — version pointer (default https://ferrosadb.com/LATEST)
#   FERROSA_RELEASE_HOST  — ferrosa releases root
#   MEMORY_RELEASE_HOST   — ferrosa-memory releases root
#   ONBOARDING_URL        — ONBOARDING.md source (default github raw on main)
#   FERROSA_SUITE_DIR     — where to put cloned repos (default $HOME/src/ferrosa-suite)
#   FERROSA_INSTALL_ROOT  — binary install prefix (default $HOME/.ferrosa)
#   NOMIC_MODEL           — embedding model name (default nomic-embed-text-v2-moe)
set -euo pipefail

FERROSA_REPO="ferrosadb/ferrosa"
MEMORY_REPO="ferrosadb/ferrosa-memory"
LATEST_URL="${FERROSA_LATEST_URL:-https://ferrosadb.com/LATEST}"
FERROSA_RELEASE_HOST="${FERROSA_RELEASE_HOST:-https://github.com/${FERROSA_REPO}/releases}"
MEMORY_RELEASE_HOST="${MEMORY_RELEASE_HOST:-https://github.com/${MEMORY_REPO}/releases}"
ONBOARDING_URL="${ONBOARDING_URL:-https://raw.githubusercontent.com/${MEMORY_REPO}/main/ONBOARDING.md}"
FERROSA_SUITE_DIR="${FERROSA_SUITE_DIR:-$HOME/src/ferrosa-suite}"
INSTALL_ROOT="${FERROSA_INSTALL_ROOT:-${HOME}/.ferrosa}"
BIN_DIR="${INSTALL_ROOT}/bin"
CONFIG_DIR="${INSTALL_ROOT}/config"
DATA_DIR="${INSTALL_ROOT}/data"
LOG_DIR="${INSTALL_ROOT}/logs"
NOMIC_MODEL="${NOMIC_MODEL:-nomic-embed-text-v2-moe}"

VERSION=""
WANT_CLONE=""    # ask|yes|no
WANT_NOMIC=""    # ask|yes|no
WANT_HERMES=""   # ask|yes|no

while [ $# -gt 0 ]; do
  case "$1" in
    --version)    VERSION="$2"; shift 2 ;;
    --clone)      WANT_CLONE="yes"; shift ;;
    --no-clone)   WANT_CLONE="no"; shift ;;
    --nomic)      WANT_NOMIC="yes"; shift ;;
    --no-nomic)   WANT_NOMIC="no"; shift ;;
    --hermes)     WANT_HERMES="yes"; shift ;;
    --no-hermes)  WANT_HERMES="no"; shift ;;
    -h|--help)
      cat <<EOF
ferrosa-memory fast setup
  --version <tag>          install a specific tag (default: read $LATEST_URL)
  --clone / --no-clone     clone or update source repos under \$FERROSA_SUITE_DIR
  --nomic / --no-nomic     pull the Nomic embedding model via ollama
  --hermes / --no-hermes   exec hermes "onboard me ..." when done
EOF
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

say() { printf ':: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

detect_target() {
  local os arch
  os=$(uname -s); arch=$(uname -m)
  case "$os/$arch" in
    Darwin/arm64)              echo "aarch64-apple-darwin" ;;
    Darwin/x86_64)
      die "Intel macOS is not supported in v0.x. Build from source: https://github.com/${MEMORY_REPO}#building" ;;
    Linux/x86_64)              echo "x86_64-unknown-linux-musl" ;;
    Linux/aarch64|Linux/arm64) echo "aarch64-unknown-linux-musl" ;;
    *) die "unsupported platform: $os/$arch" ;;
  esac
}
TARGET=$(detect_target)

if [ -z "$VERSION" ]; then
  say "resolving latest version from $LATEST_URL"
  VERSION=$(curl -fsSL "$LATEST_URL" | tr -d '[:space:]')
fi
[ -n "$VERSION" ] || die "no version resolved from $LATEST_URL"
case "$VERSION" in
  v*) : ;;
  *)  VERSION="v${VERSION}" ;;
esac

prompt_yes() {
  local q="$1" a
  if [ ! -t 0 ] && [ ! -r /dev/tty ]; then
    return 1
  fi
  read -r -p "$q [y/N] " a < /dev/tty
  case "${a:-N}" in y|Y|yes|Yes|YES) return 0 ;; *) return 1 ;; esac
}

# ── Stage 1: install binaries ───────────────────────────────────────────────
install_tarball() {
  local label="$1" host="$2" tarball="$3"
  local url="${host}/download/${VERSION}/${tarball}"
  local sums_url="${host}/download/${VERSION}/SHA256SUMS"
  local tmp; tmp=$(mktemp -d)
  say "downloading ${label} ${tarball}"
  curl -fsSL --output "$tmp/$tarball" "$url" >&2
  curl -fsSL --output "$tmp/SHA256SUMS" "$sums_url" >&2
  ( cd "$tmp" && grep "$tarball" SHA256SUMS | shasum -a 256 -c - >&2 ) \
    || die "${label}: checksum verification FAILED"
  tar -xzf "$tmp/$tarball" -C "$tmp" >&2
  printf '%s\n' "$tmp"
}

mkdir -p "$BIN_DIR" "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR"

FERROSA_TARBALL="ferrosa-${VERSION}-${TARGET}.tar.gz"
MEMORY_TARBALL="ferrosa-memory-${VERSION}-${TARGET}.tar.gz"

# ferrosa binary
F_TMP=$(install_tarball "ferrosa" "$FERROSA_RELEASE_HOST" "$FERROSA_TARBALL")
cp "$F_TMP/ferrosa"     "$BIN_DIR/"
cp "$F_TMP/ferrosa-ctl" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa" "$BIN_DIR/ferrosa-ctl"
if [ ! -f "$CONFIG_DIR/ferrosa.toml" ]; then
  cp "$F_TMP/config/ferrosa.example.toml" "$CONFIG_DIR/ferrosa.toml"
fi
rm -rf "$F_TMP"

# ferrosa-memory binary
M_TMP=$(install_tarball "ferrosa-memory" "$MEMORY_RELEASE_HOST" "$MEMORY_TARBALL")
cp "$M_TMP/ferrosa-memory-mcp" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa-memory-mcp"
if [ ! -f "$CONFIG_DIR/ferrosa-memory.toml" ]; then
  cp "$M_TMP/config/ferrosa-memory.example.toml" "$CONFIG_DIR/ferrosa-memory.toml"
fi
rm -rf "$M_TMP"

# ── Stage 2: optional source clone ──────────────────────────────────────────
clone_or_update() {
  local url="$1" dir="$2"
  if [ -d "$dir/.git" ]; then
    say "updating $dir"
    git -C "$dir" fetch --all --prune
  else
    say "cloning $url -> $dir"
    git clone "$url" "$dir"
  fi
}

do_clone() {
  mkdir -p "$FERROSA_SUITE_DIR"
  clone_or_update "https://github.com/${FERROSA_REPO}.git" "$FERROSA_SUITE_DIR/ferrosa"
  clone_or_update "https://github.com/${MEMORY_REPO}.git" "$FERROSA_SUITE_DIR/ferrosa-memory"
}

case "$WANT_CLONE" in
  yes) do_clone ;;
  no)  : ;;
  "")  prompt_yes "Clone or update source repos at $FERROSA_SUITE_DIR?" && do_clone ;;
esac

# ── Stage 3: ONBOARDING.md ──────────────────────────────────────────────────
ONBOARDING_DIR="$FERROSA_SUITE_DIR/ferrosa-memory"
ONBOARDING_PATH="$ONBOARDING_DIR/ONBOARDING.md"
mkdir -p "$ONBOARDING_DIR"
if [ ! -f "$ONBOARDING_PATH" ]; then
  say "downloading ONBOARDING.md from $ONBOARDING_URL"
  curl -fsSL "$ONBOARDING_URL" -o "$ONBOARDING_PATH" \
    || say "failed to fetch ONBOARDING.md (continuing; you can re-download later)"
fi

# ── Stage 4: optional Nomic embedding model ─────────────────────────────────
pull_nomic() {
  if command -v ollama >/dev/null 2>&1; then
    say "pulling $NOMIC_MODEL via ollama"
    ollama pull "$NOMIC_MODEL"
  else
    say "ollama not found — skipping. Install ollama and run: ollama pull $NOMIC_MODEL"
  fi
}

case "$WANT_NOMIC" in
  yes) pull_nomic ;;
  no)  : ;;
  "")  if command -v ollama >/dev/null 2>&1; then
         prompt_yes "Pull Nomic embedding model ($NOMIC_MODEL) for semantic search?" \
           && pull_nomic
       else
         say "ollama not found — skipping embedding model. Semantic/vector search will be degraded."
       fi ;;
esac

# ── Stage 5: hand off to LLM harness ────────────────────────────────────────
cat <<EOF >&2

ferrosa-memory $VERSION installed.

  binaries: $BIN_DIR
  config:   $CONFIG_DIR/ferrosa-memory.toml
  onboard:  $ONBOARDING_PATH

EOF

case "$WANT_HERMES" in
  yes) command -v hermes >/dev/null 2>&1 && exec hermes "onboard me using $ONBOARDING_PATH" ;;
  no)  : ;;
  "")  if command -v hermes >/dev/null 2>&1 \
         && prompt_yes "Launch Hermes with the onboard-me prompt now?"; then
         exec hermes "onboard me using $ONBOARDING_PATH"
       fi ;;
esac

cat <<EOF >&2
Next: run your preferred LLM harness with the onboard-me prompt.

Hermes:
  hermes "onboard me using $ONBOARDING_PATH"

Claude Code / Codex / another harness — paste at the prompt:
  onboard me using $ONBOARDING_PATH

The onboarding prompt walks through native vs Compose runtime, skills, hooks,
credentials, and ports.
EOF
