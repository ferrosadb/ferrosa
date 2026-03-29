#!/usr/bin/env bash
# lima-fc-setup.sh — Boot a Firecracker VM with SSH inside Lima and forward
# its SSH port to localhost:2022 on macOS. Used by ferrosa-jepsen SSH tests.
#
# Prerequisites:
#   limactl start mvm     — Lima VM named "mvm" must be running
#   Firecracker 1.15.0+   — installed in Lima VM (run: limactl shell mvm -- which firecracker)
#   ~/fc-assets/ in Lima  — vmlinux.bin + rootfs.ext4 (run once: this script creates them)
#
# After running this script:
#   cargo test -p ferrosa-jepsen -- --include-ignored ssh_execute_command ssh_upload_file
#
# Environment:
#   LIMA_INSTANCE  — Lima VM name (default: mvm)
#   FC_ROOTFS      — Path to rootfs inside Lima (default: ~/firecracker-assets/rootfs-ssh.ext4)
#   FC_KERNEL      — Path to kernel inside Lima (default: ~/firecracker-assets/vmlinux.bin)
#   FC_TAP         — TAP device to use (default: tap0)
#   FC_GUEST_IP    — Guest IP (default: 172.16.0.2)
#   FC_GATEWAY     — Gateway IP (default: 172.16.0.1)
#   FC_SSH_PORT    — macOS localhost port to forward SSH to (default: 2022)

set -euo pipefail

LIMA="${LIMA_INSTANCE:-mvm}"
FC_KERNEL="${FC_KERNEL:-/home/bkearns.guest/firecracker-assets/vmlinux-6.1.bin}"
FC_ROOTFS="${FC_ROOTFS:-/home/bkearns.guest/firecracker-assets/rootfs-ssh.ext4}"
FC_TAP="${FC_TAP:-tap0}"
FC_GUEST_IP="${FC_GUEST_IP:-172.16.0.2}"
FC_GATEWAY="${FC_GATEWAY:-172.16.0.1}"
FC_SSH_PORT="${FC_SSH_PORT:-2022}"
FC_SOCK="/tmp/ferrosa-jepsen-ssh-test.sock"
KEY_DIR="$(cd "$(dirname "$0")/../rootfs" && pwd)"
KEY_PATH="$KEY_DIR/test_key"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Lima Firecracker SSH Test Setup ==="
echo "  Lima instance : $LIMA"
echo "  Guest IP      : $FC_GUEST_IP"
echo "  macOS port    : 127.0.0.1:$FC_SSH_PORT → $FC_GUEST_IP:22"
echo "  SSH key       : $KEY_PATH"
echo ""

# ── 1. Generate test SSH key if missing ────────────────────────────────────
mkdir -p "$KEY_DIR"
if [ ! -f "$KEY_PATH" ]; then
    echo "--- Generating Ed25519 test key ---"
    ssh-keygen -t ed25519 -f "$KEY_PATH" -N "" -C "ferrosa-jepsen-test"
    echo "Generated: $KEY_PATH"
else
    echo "--- SSH key already exists: $KEY_PATH ---"
fi
chmod 600 "$KEY_PATH"
PUB_KEY="$(cat "${KEY_PATH}.pub")"

# ── 2. Build SSH-enabled rootfs in Lima if missing ────────────────────────
echo ""
echo "--- Building SSH-enabled rootfs in Lima ---"
limactl shell "$LIMA" -- bash -s <<SHELL
set -euo pipefail

ROOTFS_SSH="$FC_ROOTFS"
sudo mkdir -p /mnt/fc-rootfs

if [ ! -f "\$ROOTFS_SSH" ]; then
    echo "Creating SSH rootfs from base..."
    cp ~/firecracker-assets/rootfs.ext4 "\$ROOTFS_SSH"
    sudo mount "\$ROOTFS_SSH" /mnt/fc-rootfs

    # Bind-mount proc/sys/dev + resolv.conf so apk can reach the network.
    sudo mount -t proc  proc /mnt/fc-rootfs/proc
    sudo mount --bind   /sys /mnt/fc-rootfs/sys
    sudo mount --bind   /dev /mnt/fc-rootfs/dev
    sudo cp /etc/resolv.conf /mnt/fc-rootfs/etc/resolv.conf

    # Install dropbear (openssh 9.x segfaults on kernel 4.14).
    sudo chroot /mnt/fc-rootfs apk add --no-cache dropbear

    # Pre-generate all key types: dropbear 2022.83 is fatal if any are missing.
    sudo mkdir -p /mnt/fc-rootfs/etc/dropbear
    sudo chroot /mnt/fc-rootfs dropbearkey -t rsa     -f /etc/dropbear/dropbear_rsa_host_key
    sudo chroot /mnt/fc-rootfs dropbearkey -t dss     -f /etc/dropbear/dropbear_dss_host_key
    sudo chroot /mnt/fc-rootfs dropbearkey -t ecdsa   -f /etc/dropbear/dropbear_ecdsa_host_key
    sudo chroot /mnt/fc-rootfs dropbearkey -t ed25519 -f /etc/dropbear/dropbear_ed25519_host_key

    sudo umount /mnt/fc-rootfs/proc
    sudo umount /mnt/fc-rootfs/sys
    sudo umount /mnt/fc-rootfs/dev

    # Write init script (IPs baked in; init=/etc/init.d/rcS in boot_args).
    # Must not exit as PID 1; exec getty at end keeps serial console alive.
    sudo mount "\$ROOTFS_SSH" /mnt/fc-rootfs
    printf '%s\n' '#!/bin/sh' \
        'mount -t proc proc /proc' \
        'mount -t sysfs sysfs /sys' \
        'mount -t devtmpfs devtmpfs /dev 2>/dev/null || true' \
        'mkdir -p /var/run' \
        'ip link set lo up' \
        'ip link set eth0 up' \
        'ip addr add ${FC_GUEST_IP}/24 dev eth0' \
        'ip route add default via ${FC_GATEWAY}' \
        '/usr/sbin/dropbear' \
        "echo 'VM init complete'" \
        'exec /sbin/getty -L ttyS0 115200 vt100' \
        | sudo tee /mnt/fc-rootfs/etc/init.d/rcS > /dev/null
    sudo chmod +x /mnt/fc-rootfs/etc/init.d/rcS
    sudo umount /mnt/fc-rootfs
    echo "SSH rootfs created: \$ROOTFS_SSH"
else
    echo "Using existing SSH rootfs: \$ROOTFS_SSH"
fi

# Always update authorized_keys with the current public key.
sudo mount "\$ROOTFS_SSH" /mnt/fc-rootfs
sudo mkdir -p /mnt/fc-rootfs/root/.ssh
sudo chmod 700 /mnt/fc-rootfs/root/.ssh
echo "${PUB_KEY}" | sudo tee /mnt/fc-rootfs/root/.ssh/authorized_keys > /dev/null
sudo chmod 600 /mnt/fc-rootfs/root/.ssh/authorized_keys
sudo umount /mnt/fc-rootfs
echo "SSH rootfs ready: \$ROOTFS_SSH"
SHELL

# ── 3. Kill any existing test Firecracker processes ───────────────────────
echo ""
echo "--- Stopping any existing Firecracker test VM ---"
limactl shell "$LIMA" -- bash -c "pkill -f 'firecracker.*ferrosa-jepsen-ssh' 2>/dev/null || true; rm -f $FC_SOCK; sleep 1" 2>/dev/null || true

# ── 4. Reset TAP device ───────────────────────────────────────────────────
echo "--- Setting up $FC_TAP ---"
limactl shell "$LIMA" -- bash -c "
    # Remove all stale tap interfaces that share our subnet to avoid routing conflicts.
    for iface in \$(ip link show | awk -F': ' '/^[0-9]+: tap/{print \$2}'); do
        if [ \"\$iface\" != \"$FC_TAP\" ]; then
            sudo ip link delete \"\$iface\" 2>/dev/null || true
        fi
    done
    sudo ip link delete $FC_TAP 2>/dev/null || true
    sudo ip tuntap add $FC_TAP mode tap
    sudo ip addr add $FC_GATEWAY/24 dev $FC_TAP 2>/dev/null || true
    sudo ip link set $FC_TAP up
    sudo sysctl -w net.ipv4.ip_forward=1 > /dev/null 2>&1 || true
" 2>&1

# ── 5. Boot the Firecracker VM (background) ───────────────────────────────
echo ""
echo "--- Booting Firecracker VM ---"
limactl shell "$LIMA" -- bash -c "
    nohup firecracker --api-sock $FC_SOCK > /tmp/fc-ssh-vm.log 2>&1 &
    echo \$! > /tmp/fc-ssh-vm.pid
    sleep 2

    curl -s -X PUT --unix-socket $FC_SOCK http://localhost/boot-source \\
        -H 'Content-Type: application/json' \\
        -d '{\"kernel_image_path\":\"$FC_KERNEL\",\"boot_args\":\"console=ttyS0 reboot=k panic=1 pci=off rw init=/etc/init.d/rcS\"}' > /dev/null

    curl -s -X PUT --unix-socket $FC_SOCK http://localhost/drives/rootfs \\
        -H 'Content-Type: application/json' \\
        -d '{\"drive_id\":\"rootfs\",\"path_on_host\":\"$FC_ROOTFS\",\"is_root_device\":true,\"is_read_only\":false}' > /dev/null

    curl -s -X PUT --unix-socket $FC_SOCK http://localhost/network-interfaces/eth0 \\
        -H 'Content-Type: application/json' \\
        -d '{\"iface_id\":\"eth0\",\"guest_mac\":\"AA:FC:00:00:00:01\",\"host_dev_name\":\"$FC_TAP\"}' > /dev/null

    curl -s -X PUT --unix-socket $FC_SOCK http://localhost/actions \\
        -H 'Content-Type: application/json' \\
        -d '{\"action_type\":\"InstanceStart\"}' > /dev/null

    echo 'Firecracker VM started'
" 2>&1

# ── 6. Wait for SSH to be ready inside the VM ─────────────────────────────
echo ""
echo "--- Waiting for SSH on $FC_GUEST_IP:22 (inside Lima) ---"
MAX_WAIT=30
for i in $(seq 1 $MAX_WAIT); do
    if limactl shell "$LIMA" -- bash -c "nc -z -w1 $FC_GUEST_IP 22 2>/dev/null" 2>/dev/null; then
        echo "SSH ready after ${i}s"
        break
    fi
    if [ "$i" -eq "$MAX_WAIT" ]; then
        echo "FAIL: SSH on $FC_GUEST_IP:22 not ready after ${MAX_WAIT}s"
        limactl shell "$LIMA" -- cat /tmp/fc-ssh-vm.log 2>/dev/null | tail -20
        exit 1
    fi
    sleep 1
done

# ── 7. Forward the SSH port from Lima to macOS localhost ──────────────────
echo ""
echo "--- Forwarding $FC_GUEST_IP:22 → 127.0.0.1:$FC_SSH_PORT ---"

# Kill any existing forward on this port
lsof -ti tcp:$FC_SSH_PORT | xargs kill 2>/dev/null || true

# SSH tunnel: macOS → Lima VM → Firecracker VM
# Lima SSH key is at the standard location
LIMA_SSH_PORT=$(limactl show-ssh "$LIMA" 2>/dev/null | grep -o 'Port=[0-9]*' | head -1 | cut -d= -f2)
LIMA_SSH_PORT="${LIMA_SSH_PORT:-59766}"

# Lima stores its user key at _config/user (shared across all instances).
LIMA_KEY="$HOME/.lima/_config/user"

ssh -fN \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ExitOnForwardFailure=yes \
    -i "$LIMA_KEY" \
    -p "$LIMA_SSH_PORT" \
    -L "127.0.0.1:${FC_SSH_PORT}:${FC_GUEST_IP}:22" \
    "$(whoami)@127.0.0.1" 2>/dev/null &

sleep 1

# Verify the forward works
if nc -z -w2 127.0.0.1 "$FC_SSH_PORT" 2>/dev/null; then
    echo "Port forward active: 127.0.0.1:$FC_SSH_PORT → $FC_GUEST_IP:22"
else
    echo "WARNING: Port forward may not be active. Check Lima SSH settings."
fi

# ── 8. Print usage ────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "Lima Firecracker VM ready for SSH tests"
echo "========================================"
echo ""
echo "Run SSH tests:"
echo "  FERROSA_TEST_VM_HOST=127.0.0.1 \\"
echo "  FERROSA_TEST_VM_PORT=$FC_SSH_PORT \\"
echo "  FERROSA_TEST_VM_KEY=$KEY_PATH \\"
echo "  cargo test -p ferrosa-jepsen -- --include-ignored ssh_execute_command ssh_upload_file"
echo ""
echo "Teardown:"
echo "  limactl shell mvm -- bash -c 'pkill -f firecracker; sudo ip link delete $FC_TAP'"
echo "  lsof -ti tcp:$FC_SSH_PORT | xargs kill"
