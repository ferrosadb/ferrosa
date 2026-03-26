#!/usr/bin/env bash
set -euo pipefail

# Build a minimal Alpine Linux root filesystem image for Firecracker VMs.
#
# Prerequisites:
#   - Must be run as root (mount, chroot, losetup)
#   - apk (Alpine package manager) available on host, or debootstrap-style fetch
#   - SSH test key pair generated (rootfs/test_key, rootfs/test_key.pub)
#
# Output: rootfs/rootfs.ext4 (1 GB ext4 image)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${SCRIPT_DIR}/rootfs.ext4"
MOUNT_DIR="${SCRIPT_DIR}/mnt"
IMAGE_SIZE_MB=1024
ALPINE_MIRROR="https://dl-cdn.alpinelinux.org/alpine/v3.19"
FERROSA_BIN="${SCRIPT_DIR}/../../target/release/ferrosa"

# ── Preflight checks ───────────────────────────────────────────────────

if [[ "$(id -u)" -ne 0 ]]; then
    echo "ERROR: this script must be run as root" >&2
    exit 1
fi

if [[ ! -f "${SCRIPT_DIR}/test_key.pub" ]]; then
    echo "Generating SSH test key pair..."
    ssh-keygen -t ed25519 -f "${SCRIPT_DIR}/test_key" -N "" -C "ferrosa-jepsen-test"
fi

# ── Create image ────────────────────────────────────────────────────────

echo "Creating ${IMAGE_SIZE_MB}MB ext4 image at ${IMAGE}..."
dd if=/dev/zero of="${IMAGE}" bs=1M count=${IMAGE_SIZE_MB} status=progress
mkfs.ext4 -F "${IMAGE}"

# ── Mount and populate ──────────────────────────────────────────────────

mkdir -p "${MOUNT_DIR}"
mount -o loop "${IMAGE}" "${MOUNT_DIR}"

cleanup() {
    echo "Cleaning up..."
    umount "${MOUNT_DIR}" 2>/dev/null || true
    rmdir "${MOUNT_DIR}" 2>/dev/null || true
}
trap cleanup EXIT

echo "Installing Alpine Linux minimal..."

# Bootstrap Alpine using apk --root (static apk required).
# If apk is not available on the host, download the static binary.
if ! command -v apk &>/dev/null; then
    echo "Downloading static apk-tools..."
    APK_TOOLS_URL="${ALPINE_MIRROR}/main/x86_64"
    APK_TOOLS_PKG=$(curl -s "${APK_TOOLS_URL}/" \
        | grep -oP 'apk-tools-static-[0-9][^"]*\.apk' \
        | head -1)
    curl -sL "${APK_TOOLS_URL}/${APK_TOOLS_PKG}" -o /tmp/apk-tools-static.apk
    tar -xzf /tmp/apk-tools-static.apk -C /tmp sbin/apk.static
    APK_CMD="/tmp/sbin/apk.static"
else
    APK_CMD="apk"
fi

# Initialize the Alpine root.
${APK_CMD} add --root "${MOUNT_DIR}" --initdb \
    --repository "${ALPINE_MIRROR}/main" \
    --repository "${ALPINE_MIRROR}/community" \
    --no-cache --allow-untrusted \
    alpine-base \
    openssh-server \
    bash \
    iproute2 \
    iptables \
    libfaketime \
    curl

# ── Configure SSH ───────────────────────────────────────────────────────

echo "Configuring SSH..."
mkdir -p "${MOUNT_DIR}/root/.ssh"
chmod 700 "${MOUNT_DIR}/root/.ssh"
cp "${SCRIPT_DIR}/test_key.pub" "${MOUNT_DIR}/root/.ssh/authorized_keys"
chmod 600 "${MOUNT_DIR}/root/.ssh/authorized_keys"

# Generate SSH host keys inside the image.
ssh-keygen -A -f "${MOUNT_DIR}"

# Harden sshd_config for key-only auth.
cat > "${MOUNT_DIR}/etc/ssh/sshd_config" <<'SSHD_EOF'
Port 22
PermitRootLogin prohibit-password
PubkeyAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys
PasswordAuthentication no
ChallengeResponseAuthentication no
UsePAM no
Subsystem sftp /usr/lib/ssh/sftp-server
SSHD_EOF

# ── Configure networking and init ───────────────────────────────────────

echo "Configuring init scripts..."

# Simple init: mount pseudo-filesystems, bring up networking, start sshd.
cat > "${MOUNT_DIR}/etc/init.d/ferrosa-init" <<'INIT_EOF'
#!/bin/bash
# Minimal init for Firecracker guest.
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev

# Networking is configured via kernel boot args (ip=).
# Bring up lo for localhost.
ip link set lo up

# Start SSH daemon.
/usr/sbin/sshd -D &

# Keep init alive.
exec /bin/bash
INIT_EOF
chmod +x "${MOUNT_DIR}/etc/init.d/ferrosa-init"

# Point /sbin/init at our script.
ln -sf /etc/init.d/ferrosa-init "${MOUNT_DIR}/sbin/init"

# ── Copy ferrosa binary ────────────────────────────────────────────────

if [[ -f "${FERROSA_BIN}" ]]; then
    echo "Copying ferrosa binary..."
    cp "${FERROSA_BIN}" "${MOUNT_DIR}/usr/local/bin/ferrosa"
    chmod +x "${MOUNT_DIR}/usr/local/bin/ferrosa"
else
    echo "WARNING: ferrosa binary not found at ${FERROSA_BIN}"
    echo "         Build with: cargo build --release -p ferrosa"
    echo "         The rootfs will be created without it."
fi

# ── Copy guest setup script ────────────────────────────────────────────

cp "${SCRIPT_DIR}/setup-guest.sh" "${MOUNT_DIR}/usr/local/bin/setup-guest.sh"
chmod +x "${MOUNT_DIR}/usr/local/bin/setup-guest.sh"

# ── Finalize ────────────────────────────────────────────────────────────

echo "Image size: $(du -sh "${IMAGE}" | cut -f1)"
echo "Done. Rootfs image created at ${IMAGE}"
