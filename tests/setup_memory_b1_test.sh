#!/usr/bin/env bash
# Focused decision-flow coverage for the hosted memory quick start's DB gate.
set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT="$REPO/install/setup-memory.sh"
WORK=$(mktemp -d)
FAKE_BIN="$WORK/fake-bin"
DB_PID_FILE="$WORK/db.pid"

cleanup() {
  stop_db
  rm -rf "$WORK"
}

stop_db() {
  if [ -f "$DB_PID_FILE" ]; then
    kill "$(cat "$DB_PID_FILE")" 2>/dev/null || true
    rm -f "$DB_PID_FILE"
  fi
}
trap cleanup EXIT

fail() {
  printf 'not ok: %s\n' "$*" >&2
  exit 1
}

ok() {
  printf 'ok: %s\n' "$*" >&2
}

assert_outcome() { # home database-status mcp-status
  python3 - "$1/.ferrosa/install-outcome.json" "$2" "$3" <<'PY'
import json
import sys

path, expected_db, expected_mcp = sys.argv[1:]
with open(path, encoding="utf-8") as receipt:
    outcome = json.load(receipt)

assert outcome["schema_version"] == 1, outcome
assert outcome["database"]["status"] == expected_db, outcome
assert outcome["mcp"]["status"] == expected_mcp, outcome
PY
}

free_port() {
  python3 - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

make_tarballs() {
  local db_stage="$WORK/db-stage" memory_stage="$WORK/memory-stage"
  mkdir -p "$db_stage/config" "$db_stage/launchd" "$db_stage/systemd"
  mkdir -p "$memory_stage/config" "$memory_stage/launchd"

  printf '#!/usr/bin/env bash\nexit 0\n' > "$db_stage/ferrosa"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$db_stage/ferrosa-ctl"
  chmod +x "$db_stage/ferrosa" "$db_stage/ferrosa-ctl"
  printf '[cql]\nbind = "127.0.0.1:9042"\n' > "$db_stage/config/ferrosa.example.toml"
  printf '<plist>__HOME__</plist>\n' > "$db_stage/launchd/com.ferrosadb.ferrosa.plist"
  printf '[Service]\nExecStart=ferrosa\n' > "$db_stage/systemd/ferrosa.service"
  tar -czf "$WORK/ferrosa.tar.gz" -C "$db_stage" .

  cat > "$memory_stage/ferrosa-memory-mcp" <<'EOF'
#!/usr/bin/env bash
: "${MCP_STARTED_FILE:?}"
printf started > "$MCP_STARTED_FILE"
EOF
  chmod +x "$memory_stage/ferrosa-memory-mcp"
  printf '[server]\ntransport = "stdio"\n' > "$memory_stage/config/ferrosa-memory.example.toml"
  printf '<plist>ferrosa-memory-mcp __BINARY_PATH__</plist>\n' > "$memory_stage/launchd/com.ferrosa-memory.mcp.plist"
  tar -czf "$WORK/ferrosa-memory.tar.gz" -C "$memory_stage" .
}

make_fake_commands() {
  mkdir -p "$FAKE_BIN"

  cat > "$FAKE_BIN/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

out=""
url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output|-o) out="$2"; shift 2 ;;
    *) url="$1"; shift ;;
  esac
done

case "$url" in
  *SHA256SUMS)
    for candidate in "$(dirname "$out")"/*.tar.gz; do
      [ -f "$candidate" ] || continue
      printf '%s  %s\n' "$(shasum -a 256 "$candidate" | awk '{print $1}')" "$(basename "$candidate")"
    done > "$out" ;;
  *ferrosa-memory-*.tar.gz) cp "$TEST_MEMORY_TARBALL" "$out" ;;
  *ferrosa-*.tar.gz) cp "$TEST_DB_TARBALL" "$out" ;;
  *ONBOARDING.md) printf '# onboarding\n' > "$out" ;;
  *) printf 'unexpected curl URL: %s\n' "$url" >&2; exit 1 ;;
esac
EOF

  cat > "$FAKE_BIN/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$FAKE_SYSTEMCTL_LOG"
case "${FAKE_SYSTEMCTL_MODE:?}" in
  ready)
    case " $* " in
      *" enable --now ferrosa.service "*)
        python3 - "$FERROSA_SETUP_CQL_PORT" <<'PY' >/dev/null 2>&1 &
import socket
import sys

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(sys.argv[1])))
listener.listen()
while True:
    connection, _ = listener.accept()
    connection.close()
PY
        printf '%s\n' "$!" > "$FAKE_DB_PID_FILE" ;;
    esac ;;
  fail) exit 1 ;;
  *) printf 'unexpected systemctl mode: %s\n' "$FAKE_SYSTEMCTL_MODE" >&2; exit 2 ;;
esac
EOF

  cat > "$FAKE_BIN/uname" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  -s) echo "${FAKE_OS:?}" ;;
  -m) case "${FAKE_OS:?}" in Darwin) echo arm64 ;; Linux) echo x86_64 ;; esac ;;
  *) /usr/bin/uname "$@" ;;
esac
EOF

  cat > "$FAKE_BIN/launchctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$FAKE_LAUNCHCTL_LOG"
case "${1:-}" in
  bootstrap)
    plist="${3:?}"
    if grep -F "ferrosa-memory-mcp" "$plist" >/dev/null; then
      : "${MCP_STARTED_FILE:?}"
      printf started > "$MCP_STARTED_FILE"
    else
      python3 - "$FERROSA_SETUP_CQL_PORT" <<'PY' >/dev/null 2>&1 &
import socket
import sys

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(sys.argv[1])))
listener.listen()
while True:
    connection, _ = listener.accept()
    connection.close()
PY
      printf '%s\n' "$!" > "$FAKE_DB_PID_FILE"
    fi ;;
esac
EOF

  chmod +x "$FAKE_BIN/curl" "$FAKE_BIN/systemctl" "$FAKE_BIN/uname" "$FAKE_BIN/launchctl"
}

run_setup() {
  local home="$1" log="$2" port="$3" mode="$4" os="$5" start="$6"
  local -a args=(--version v0.0.0-test --no-clone --no-hooks --no-nomic --no-hermes)
  [ "$start" = yes ] || args+=(--no-start)
  env \
    HOME="$home" \
    PATH="$FAKE_BIN:$PATH" \
    TEST_DB_TARBALL="$WORK/ferrosa.tar.gz" \
    TEST_MEMORY_TARBALL="$WORK/ferrosa-memory.tar.gz" \
    FAKE_SYSTEMCTL_LOG="$WORK/systemctl-$mode.log" \
    FAKE_SYSTEMCTL_MODE="$mode" \
    FAKE_LAUNCHCTL_LOG="$WORK/launchctl-$mode.log" \
    FAKE_OS="$os" \
    FAKE_DB_PID_FILE="$DB_PID_FILE" \
    FERROSA_SETUP_CQL_PORT="$port" \
    FERROSA_SUITE_DIR="$home/suite" \
    MCP_STARTED_FILE="$WORK/mcp-$mode.started" \
    bash "$SCRIPT" "${args[@]}" \
      > "$log" 2>&1
}

make_tarballs
make_fake_commands
bash -n "$SCRIPT"

ready_port=$(free_port)
ready_home="$WORK/home-ready"
ready_log="$WORK/ready.log"
if ! run_setup "$ready_home" "$ready_log" "$ready_port" ready Linux no; then
  cat "$ready_log" >&2
  fail "quick start should succeed after systemd starts the DB"
fi
grep -F "enable --now ferrosa.service" "$WORK/systemctl-ready.log" >/dev/null \
  || fail "quick start did not use the canonical systemd DB start path"
grep -F "database ready on 127.0.0.1:$ready_port" "$ready_log" >/dev/null \
  || fail "quick start did not wait for the DB readiness probe"
assert_outcome "$ready_home" ready skipped \
  || fail "quick start did not record its skipped MCP start outcome"
[ ! -e "$WORK/mcp-ready.started" ] \
  || fail "--no-start must not launch MCP after the DB gate"
ok "supported Linux service path starts and verifies the database before MCP handling"
stop_db

mac_port=$(free_port)
mac_home="$WORK/home-macos"
mac_log="$WORK/macos.log"
if ! run_setup "$mac_home" "$mac_log" "$mac_port" macos Darwin yes; then
  cat "$mac_log" >&2
  fail "quick start should succeed after launchd starts the DB"
fi
grep -F "database ready on 127.0.0.1:$mac_port" "$mac_log" >/dev/null \
  || fail "quick start did not wait for database readiness before MCP launchd setup"
[ -e "$WORK/mcp-macos.started" ] \
  || fail "MCP LaunchAgent was not started after the database gate"
assert_outcome "$mac_home" ready started \
  || fail "quick start did not record its successful MCP start outcome"
db_bootstrap=$(grep -n '/com.ferrosadb.ferrosa.plist' "$WORK/launchctl-macos.log" | head -1 | cut -d: -f1)
mcp_bootstrap=$(grep -n '/com.ferrosa-memory.mcp.plist' "$WORK/launchctl-macos.log" | head -1 | cut -d: -f1)
[ -n "$db_bootstrap" ] && [ -n "$mcp_bootstrap" ] && [ "$db_bootstrap" -lt "$mcp_bootstrap" ] \
  || fail "MCP LaunchAgent was not registered after the DB LaunchAgent"
ok "supported macOS service path verifies the database before launching MCP"
stop_db

manual_port=$(free_port)
manual_home="$WORK/home-manual"
manual_log="$WORK/manual.log"
if run_setup "$manual_home" "$manual_log" "$manual_port" fail Linux no; then
  cat "$manual_log" >&2
  fail "quick start must fail loudly when no user service can start the DB"
fi
manual_command="FERROSA_CONFIG=\"$manual_home/.ferrosa/config/ferrosa.toml\" \"$manual_home/.ferrosa/bin/ferrosa\""
grep -F "manual_action_required: local Ferrosa is configured but not running" "$manual_log" >/dev/null \
  || fail "missing explicit manual-action state"
grep -F "$manual_command" "$manual_log" >/dev/null \
  || fail "manual-action state did not include the exact DB start command"
assert_outcome "$manual_home" manual_action_required not_attempted \
  || fail "quick start did not record the failed DB service outcome"
[ ! -e "$WORK/mcp-fail.started" ] \
  || fail "MCP must not launch when the database start requires manual action"
ok "unsupported service path exits with the exact manual DB command before MCP handling"
