#!/usr/bin/env bash
# ferrosa installer — fetches a release tarball, installs to ~/.ferrosa,
# offers system-service registration and admin-password setup.
#
# Usage:
#   curl -fsSL https://ferrosadb.com/install.sh | bash
#   curl -fsSL https://ferrosadb.com/install.sh | bash -s -- --version v0.12.0 --no-service
set -euo pipefail

REPO="ferrosadb/ferrosa"
RELEASE_HOST="https://github.com/${REPO}/releases"
INSTALL_ROOT="${HOME}/.ferrosa"
BIN_DIR="${INSTALL_ROOT}/bin"
CONFIG_DIR="${INSTALL_ROOT}/config"
DATA_DIR="${INSTALL_ROOT}/data"
LOG_DIR="${INSTALL_ROOT}/logs"

VERSION=""
WANT_SERVICE=""    # ask|yes|no
WANT_PASSWORD=""   # ask|yes|no

# ---------- arg parsing ----------
while [ $# -gt 0 ]; do
  case "$1" in
    --version)      VERSION="$2"; shift 2 ;;
    --no-service)   WANT_SERVICE="no"; shift ;;
    --service)      WANT_SERVICE="yes"; shift ;;
    --no-password)  WANT_PASSWORD="no"; shift ;;
    --password)     WANT_PASSWORD="yes"; shift ;;
    -h|--help)
      cat <<EOF
ferrosa installer
  --version <tag>           install a specific tag (default: latest)
  --service / --no-service  enable or skip system-service install
  --password / --no-password prompt for or skip admin-password setup
EOF
      exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

say() { printf ':: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# ---------- platform detect ----------
detect_target() {
  local os arch
  os=$(uname -s); arch=$(uname -m)
  case "$os/$arch" in
    Darwin/arm64)              echo "aarch64-apple-darwin" ;;
    Darwin/x86_64)
      die "Intel macOS is not supported in v0.x. Please build from source: https://github.com/ferrosadb/ferrosa#building" ;;
    Linux/x86_64)              echo "x86_64-unknown-linux-musl" ;;
    Linux/aarch64|Linux/arm64) echo "aarch64-unknown-linux-musl" ;;
    *) die "unsupported platform: $os/$arch" ;;
  esac
}
TARGET=$(detect_target)

# ---------- fetch tag ----------
if [ -z "$VERSION" ]; then
  VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
              | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)
fi
[ -n "$VERSION" ] || die "no release found at https://github.com/${REPO}/releases"

TARBALL="ferrosa-${VERSION}-${TARGET}.tar.gz"
URL="${RELEASE_HOST}/download/${VERSION}/${TARBALL}"
SUMS_URL="${RELEASE_HOST}/download/${VERSION}/SHA256SUMS"

# ---------- download + verify ----------
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
say "downloading $TARBALL"
curl -fsSL --output "$TMP/$TARBALL" "$URL"
curl -fsSL --output "$TMP/SHA256SUMS" "$SUMS_URL"

say "verifying SHA256"
( cd "$TMP" && grep "$TARBALL" SHA256SUMS | shasum -a 256 -c - ) \
  || die "checksum verification FAILED"

# ---------- install layout ----------
say "installing to $INSTALL_ROOT"
mkdir -p "$BIN_DIR" "$CONFIG_DIR" "$DATA_DIR" "$LOG_DIR"
tar -xzf "$TMP/$TARBALL" -C "$TMP"

cp "$TMP/ferrosa"     "$BIN_DIR/"
cp "$TMP/ferrosa-ctl" "$BIN_DIR/"
chmod +x "$BIN_DIR/ferrosa" "$BIN_DIR/ferrosa-ctl"

if [ ! -f "$CONFIG_DIR/ferrosa.toml" ]; then
  cp "$TMP/config/ferrosa.example.toml" "$CONFIG_DIR/ferrosa.toml"
  say "wrote default config to $CONFIG_DIR/ferrosa.toml"
else
  say "kept existing $CONFIG_DIR/ferrosa.toml"
fi

# ---------- service registration ----------
prompt_yes() {
  local q="$1" a
  read -r -p "$q [y/N] " a < /dev/tty
  case "${a:-N}" in y|Y|yes|Yes|YES) return 0 ;; *) return 1 ;; esac
}

register_macos() {
  local plist="$HOME/Library/LaunchAgents/com.ferrosadb.ferrosa.plist"
  mkdir -p "$(dirname "$plist")"
  sed "s|__HOME__|$HOME|g" "$TMP/launchd/com.ferrosadb.ferrosa.plist" > "$plist"
  launchctl bootout "gui/$(id -u)" "$plist" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$plist"
  say "launchd: com.ferrosadb.ferrosa loaded; will start now and on every login"
}

register_linux() {
  local unit="$HOME/.config/systemd/user/ferrosa.service"
  mkdir -p "$(dirname "$unit")"
  cp "$TMP/systemd/ferrosa.service" "$unit"
  systemctl --user daemon-reload
  systemctl --user enable --now ferrosa.service
  if command -v loginctl >/dev/null; then
    loginctl enable-linger "$USER" 2>/dev/null \
      && say "systemd: lingering enabled (boot-time start without login)"
  fi
  say "systemd: ferrosa.service enabled and started"
}

do_service() {
  case "$(uname -s)" in
    Darwin) register_macos ;;
    Linux)  register_linux ;;
  esac
}

case "$WANT_SERVICE" in
  yes) do_service ;;
  no)  : ;;
  "")  prompt_yes "Register ferrosa as a user service (autostart on login)?" \
         && do_service ;;
esac

# ---------- wait for port 9042 ----------
wait_for_cql() {
  local host="${1:-127.0.0.1}" port="${2:-9042}"
  for _ in $(seq 1 30); do
    if (echo > "/dev/tcp/$host/$port") 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

# ---------- password setup ----------
do_password() {
  if ! wait_for_cql 127.0.0.1 9042; then
    say "ferrosa not reachable on 127.0.0.1:9042 yet — skipping password setup"
    say "run later: $BIN_DIR/ferrosa-ctl auth set-password"
    return
  fi
  say "set ferrosa_admin password (current default: ferrosa_admin)"
  # Exact flag shape pinned to the auth set-password CLI from PR (Task #8 re-brief)
  "$BIN_DIR/ferrosa-ctl" auth set-password --user ferrosa_admin
}

case "$WANT_PASSWORD" in
  yes) do_password ;;
  no)  : ;;
  "")  prompt_yes "Set the ferrosa_admin password now?" && do_password ;;
esac

# ---------- finish ----------
cat <<EOF >&2

ferrosa $VERSION installed.

  binaries: $BIN_DIR
  config:   $CONFIG_DIR/ferrosa.toml
  data:     $DATA_DIR
  logs:     $LOG_DIR

Add to your shell profile:
  export PATH="\$HOME/.ferrosa/bin:\$PATH"

If you didn't register a service, run manually:
  FERROSA_CONFIG="$CONFIG_DIR/ferrosa.toml" "$BIN_DIR/ferrosa"

Docs: https://ferrosadb.com/database/getting-started.html
EOF
