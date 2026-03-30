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
# Check https://github.com/firecracker-microvm/firecracker/releases for updated URLs.
KERNEL_URL="${FC_KERNEL_URL:-https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.11/aarch64/vmlinux-6.1.bin}"
KERNEL_NAME="vmlinux-6.1.bin"
ALPINE_MIRROR="https://dl-cdn.alpinelinux.org/alpine/v3.19"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FERROSA_BIN="$REPO_ROOT/target/release/ferrosa"
SETUP_GUEST="$REPO_ROOT/ferrosa-jepsen/rootfs/setup-guest.sh"
# SSH key lives at the workspace root so both lima-fc-setup.sh and the tests
# can find it at a stable path.
KEY_PATH="$REPO_ROOT/rootfs/test_key"

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

# ── 0. Generate SSH key on macOS if missing ────────────────────────────────
# Generate on macOS (not in Lima) because Lima mounts the repo read-only.
mkdir -p "$REPO_ROOT/rootfs"
if [[ ! -f "${KEY_PATH}" ]]; then
    echo "--- Generating SSH test key pair ---"
    ssh-keygen -t ed25519 -f "$KEY_PATH" -N "" -C "ferrosa-jepsen-test"
    echo "Generated: $KEY_PATH"
else
    echo "--- SSH key already exists: $KEY_PATH ---"
fi
chmod 600 "$KEY_PATH"
PUB_KEY="$(cat "${KEY_PATH}.pub")"

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
    echo "--- Building rootfs in Lima ---"
    echo "    Writing to /tmp/fc-build/ (Lima's writable space)"
    echo "    This takes ~2-3 minutes (Alpine package bootstrap)"

    # Lima mounts the macOS repo read-only, so build.sh cannot write there.
    # We pass all paths explicitly and run fully in Lima's writable /tmp.
    # The ferrosa binary and setup-guest.sh are read via the ro mount.

    limactl shell "$LIMA" -- sudo bash -s "$FERROSA_BIN" "$SETUP_GUEST" "$PUB_KEY" << 'LIMA_BUILD'
set -euo pipefail
FERROSA_BIN="$1"
SETUP_GUEST="$2"
PUB_KEY="$3"

ALPINE_MIRROR="https://dl-cdn.alpinelinux.org/alpine/v3.19"
BUILD_DIR="/tmp/fc-build"
IMAGE="$BUILD_DIR/rootfs.ext4"
MOUNT_DIR="$BUILD_DIR/mnt"

rm -rf "$BUILD_DIR"
mkdir -p "$MOUNT_DIR"

echo "Creating 1GB ext4 image at $IMAGE..."
dd if=/dev/zero of="$IMAGE" bs=1M count=1024 status=progress
mkfs.ext4 -F "$IMAGE"

mount -o loop "$IMAGE" "$MOUNT_DIR"
cleanup() { umount "$MOUNT_DIR" 2>/dev/null || true; }
trap cleanup EXIT

echo "Bootstrapping Alpine Linux..."
if ! command -v apk &>/dev/null; then
    APK_TOOLS_URL="${ALPINE_MIRROR}/main/aarch64"
    APK_PKG=$(curl -s "${APK_TOOLS_URL}/" | grep -oP 'apk-tools-static-[0-9][^"]*\.apk' | head -1)
    curl -sL "${APK_TOOLS_URL}/${APK_PKG}" -o /tmp/apk-tools-static.apk
    tar -xzf /tmp/apk-tools-static.apk -C /tmp sbin/apk.static
    APK_CMD="/tmp/sbin/apk.static"
else
    APK_CMD="apk"
fi

$APK_CMD add --root "$MOUNT_DIR" --initdb \
    --repository "${ALPINE_MIRROR}/main" \
    --repository "${ALPINE_MIRROR}/community" \
    --no-cache --allow-untrusted \
    alpine-base openssh-server bash iproute2 iptables libfaketime curl

echo "Configuring SSH..."
mkdir -p "$MOUNT_DIR/root/.ssh"
chmod 700 "$MOUNT_DIR/root/.ssh"
echo "$PUB_KEY" > "$MOUNT_DIR/root/.ssh/authorized_keys"
chmod 600 "$MOUNT_DIR/root/.ssh/authorized_keys"
ssh-keygen -A -f "$MOUNT_DIR"

cat > "$MOUNT_DIR/etc/ssh/sshd_config" <<'SSHD'
Port 22
PermitRootLogin prohibit-password
PubkeyAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys
PasswordAuthentication no
ChallengeResponseAuthentication no
Subsystem sftp /usr/lib/ssh/sftp-server
SSHD

echo "Writing init script..."
# Named rcS so lima-fc-setup.sh's boot_args (init=/etc/init.d/rcS) work.
# IP is hardcoded to 172.16.0.2/gateway 172.16.0.1; the cluster-up script
# overwrites this script in per-node rootfs copies with the correct IPs.
mkdir -p "$MOUNT_DIR/etc/init.d"
cat > "$MOUNT_DIR/etc/init.d/rcS" <<'INIT'
#!/bin/bash
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev 2>/dev/null || true
ip link set lo up
ip link set eth0 up
ip addr add 172.16.0.2/24 dev eth0
ip route add default via 172.16.0.1
/usr/sbin/sshd -D &
exec /bin/bash
INIT
chmod +x "$MOUNT_DIR/etc/init.d/rcS"

echo "Copying ferrosa binary..."
cp "$FERROSA_BIN" "$MOUNT_DIR/usr/local/bin/ferrosa"
chmod +x "$MOUNT_DIR/usr/local/bin/ferrosa"

echo "Copying setup-guest.sh..."
cp "$SETUP_GUEST" "$MOUNT_DIR/setup-guest.sh"
chmod +x "$MOUNT_DIR/setup-guest.sh"

echo "Image size: $(du -sh "$IMAGE" | cut -f1)"
echo "Done."
LIMA_BUILD

    # Copy the image to the assets directory.
    limactl shell "$LIMA" -- bash -c "
        mkdir -p \"\$HOME/firecracker-assets\"
        cp /tmp/fc-build/rootfs.ext4 \"\$HOME/firecracker-assets/rootfs.ext4\"
        echo \"Rootfs copied to ~/firecracker-assets/rootfs.ext4\"
        ls -lh \"\$HOME/firecracker-assets/rootfs.ext4\"
    "
fi

# ── 3. Verify assets ───────────────────────────────────────────────────────
echo ""
echo "--- Verifying assets ---"
limactl shell "$LIMA" -- bash -c "
    echo 'Assets in ~/firecracker-assets/:'
    ls -lh \"\$HOME/firecracker-assets/\" 2>/dev/null || echo '  (empty)'
    echo ''
    for f in '$KERNEL_NAME' rootfs.ext4; do
        if [ -f \"\$HOME/firecracker-assets/\$f\" ]; then
            echo \"  OK  \$f\"
        else
            echo \"  MISSING  \$f\"
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
