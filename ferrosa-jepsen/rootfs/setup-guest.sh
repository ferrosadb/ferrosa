#!/usr/bin/env bash
set -euo pipefail

# Guest-side setup script for Firecracker VMs.
#
# Called after the VM boots to finalize configuration and start ferrosa.
#
# Usage: setup-guest.sh [--seed-nodes <ip1,ip2,...>] [--listen-addr <addr>]
#
# Environment:
#   FERROSA_SEED_NODES  - Comma-separated list of seed node addresses.
#   FERROSA_LISTEN_ADDR - Address to bind ferrosa (default: 0.0.0.0:9042).

SEED_NODES="${FERROSA_SEED_NODES:-}"
LISTEN_ADDR="${FERROSA_LISTEN_ADDR:-0.0.0.0:9042}"

# ── Parse arguments ─────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --seed-nodes)
            SEED_NODES="$2"
            shift 2
            ;;
        --listen-addr)
            LISTEN_ADDR="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# ── Ensure pseudo-filesystems are mounted ───────────────────────────────

mount -t proc proc /proc 2>/dev/null || true
mount -t sysfs sysfs /sys 2>/dev/null || true
mount -t devtmpfs devtmpfs /dev 2>/dev/null || true

# ── Networking ──────────────────────────────────────────────────────────

# The primary interface is configured via kernel boot args (ip=...).
# Bring up loopback explicitly.
ip link set lo up 2>/dev/null || true

echo "Network configuration:"
ip addr show

# ── Start SSH daemon ────────────────────────────────────────────────────

if ! pgrep -x sshd > /dev/null; then
    echo "Starting sshd..."
    /usr/sbin/sshd
    echo "sshd started on port 22"
else
    echo "sshd already running"
fi

# ── Start ferrosa ───────────────────────────────────────────────────────

FERROSA_BIN="/usr/local/bin/ferrosa"

if [[ ! -x "${FERROSA_BIN}" ]]; then
    echo "WARNING: ferrosa binary not found at ${FERROSA_BIN}" >&2
    echo "         The guest will run without ferrosa." >&2
    exit 0
fi

FERROSA_ARGS=("--listen-addr" "${LISTEN_ADDR}")

if [[ -n "${SEED_NODES}" ]]; then
    FERROSA_ARGS+=("--seed-nodes" "${SEED_NODES}")
fi

echo "Starting ferrosa: ${FERROSA_BIN} ${FERROSA_ARGS[*]}"

# Run ferrosa in the background, logging to a file.
${FERROSA_BIN} "${FERROSA_ARGS[@]}" \
    > /var/log/ferrosa.log 2>&1 &

FERROSA_PID=$!
echo "ferrosa started with PID ${FERROSA_PID}"

# Write PID file for management scripts.
echo "${FERROSA_PID}" > /var/run/ferrosa.pid
