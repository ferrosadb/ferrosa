#!/usr/bin/env bash
# lima-fc-assets.sh — Download a Firecracker kernel and build a rootfs image
# inside the Lima VM for use by ferrosa-jepsen infrastructure tests.
#
# Run this ONCE before running any Firecracker-based tests.
#
# Prerequisites:
#   limactl start mvm       — Lima VM named "mvm" must be running
#   cargo build --release   — Ferrosa binary must be built first
#
# After this script completes:
#   scripts/lima-fc-setup.sh          — single-VM SSH tests
#   scripts/lima-fc-cluster-up.sh     — 3-node cluster tests
#
# Environment:
#   LIMA_INSTANCE   — Lima VM name (default: mvm)
#   FC_KERNEL_URL   — Override kernel download URL
#   SKIP_KERNEL     — Set to 1 to skip kernel download (already have one)
#   SKIP_ROOTFS     — Set to 1 to skip rootfs build (already have one)

set -euo pipefail

LIMA="${LIMA_INSTANCE:-mvm}"

# aarch64 kernel from Firecracker's CI asset bucket.
# Firecracker v1.15 was tested with kernel 6.1; this URL is pinned to that
# version.  Check https://github.com/firecracker-microvm/firecracker/releases
# for updated kernel URLs if this download fails.
KERNEL_URL="${FC_KERNEL_URL:-https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.11/aarch64/vmlinux-6.1.bin}"
KERNEL_NAME="vmlinux-6.1.bin"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FERROSA_BIN="$REPO_ROOT/target/release/ferrosa"

echo "=== Firecracker Asset Setup ==="
echo "  Lima instance : $LIMA"
echo "  Kernel URL    : $KERNEL_URL"
echo ""

# ── Preflight ──────────────────────────────────────────────────────────────
if ! limactl list --format "{{.Name}}" 2>/dev/null | grep -q "^${LIMA}$"; then
    echo "ERROR: Lima instance '$LIMA' is not running."
    echo "  Run: limactl start mvm"
    exit 1
fi

if [[ ! -f "$FERROSA_BIN" ]]; then
    echo "ERROR: ferrosa release binary not found at $FERROSA_BIN"
    echo "  Run: cargo build --release"
    exit 1
fi

# ── 1. Download kernel (inside Lima) ──────────────────────────────────────
if [[ "${SKIP_KERNEL:-0}" == "1" ]]; then
    echo "--- Skipping kernel download (SKIP_KERNEL=1) ---"
else
    echo "--- Downloading Firecracker kernel ---"
    limactl shell "$LIMA" -- bash -c "
        set -euo pipefail
        mkdir -p \"\$HOME/firecracker-assets\"
        KERNEL=\"\$HOME/firecracker-assets/$KERNEL_NAME\"
        if [ -f \"\$KERNEL\" ]; then
            echo \"Kernel already exists: \$KERNEL\"
        else
            echo \"Downloading $KERNEL_URL ...\"
            curl -fsSL -o \"\$KERNEL\" '$KERNEL_URL'
            echo \"Kernel saved: \$KERNEL\"
        fi
        ls -lh \"\$KERNEL\"
    "
fi

# ── 2. Build rootfs inside Lima ───────────────────────────────────────────
if [[ "${SKIP_ROOTFS:-0}" == "1" ]]; then
    echo "--- Skipping rootfs build (SKIP_ROOTFS=1) ---"
else
    echo ""
    echo "--- Building rootfs (requires sudo inside Lima) ---"
    echo "    This takes ~2-3 minutes (Alpine package install + chroot setup)"

    # Lima auto-mounts the macOS home directory, so the ferrosa repo is
    # accessible inside Lima at the same absolute path.
    BUILD_SH="$REPO_ROOT/ferrosa-jepsen/rootfs/build.sh"

    limactl shell "$LIMA" -- sudo bash "$BUILD_SH"

    # Move the built image to the assets directory.
    BUILT_IMAGE="$REPO_ROOT/ferrosa-jepsen/rootfs/rootfs.ext4"
    limactl shell "$LIMA" -- bash -c "
        mkdir -p \"\$HOME/firecracker-assets\"
        if [ -f '$BUILT_IMAGE' ]; then
            cp '$BUILT_IMAGE' \"\$HOME/firecracker-assets/rootfs.ext4\"
            echo \"Rootfs copied to ~/firecracker-assets/rootfs.ext4\"
        else
            echo 'ERROR: build.sh did not produce $BUILT_IMAGE'
            exit 1
        fi
    "
fi

# ── 3. Verify assets ───────────────────────────────────────────────────────
echo ""
echo "--- Verifying assets ---"
limactl shell "$LIMA" -- bash -c "
    ls -lh \"\$HOME/firecracker-assets/\"
    echo ''
    echo 'Required files:'
    for f in vmlinux-6.1.bin rootfs.ext4; do
        if [ -f \"\$HOME/firecracker-assets/\$f\" ]; then
            echo \"  ✓ \$f\"
        else
            echo \"  ✗ \$f  <-- MISSING\"
        fi
    done
"

echo ""
echo "========================================"
echo "Assets ready. Next steps:"
echo "========================================"
echo ""
echo "Single-VM SSH tests:"
echo "  scripts/lima-fc-setup.sh"
echo "  FERROSA_TEST_FIRECRACKER=1 cargo test -p ferrosa-jepsen"
echo ""
echo "3-node cluster tests:"
echo "  scripts/lima-fc-cluster-up.sh"
echo "  FERROSA_TEST_CLUSTER_NODES=127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044 \\"
echo "  FERROSA_TEST_VM_KEY=$(pwd)/rootfs/test_key \\"
echo "  cargo test -p ferrosa-jepsen --test nemesis_correctness"
