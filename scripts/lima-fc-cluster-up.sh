#!/usr/bin/env bash
# lima-fc-cluster-up.sh — Boot a 3-node Ferrosa cluster in Firecracker VMs
# inside Lima and forward CQL + SSH ports to macOS localhost.
#
# Prerequisites:
#   scripts/lima-fc-assets.sh   — kernel + rootfs must be built first
#   limactl start mvm           — Lima VM must be running
#
# After running this script:
#   FERROSA_TEST_CLUSTER_NODES=127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044 \
#   FERROSA_TEST_VM_KEY=rootfs/test_key \
#   FERROSA_TEST_FIRECRACKER=1 \
#   cargo test -p ferrosa-jepsen --test nemesis_correctness
#
# To tear down:
#   scripts/lima-fc-cluster-down.sh
#
# Architecture:
#   macOS
#    └── Lima VM (mvm)
#         ├── tap0 ← Firecracker VM node1 (172.16.0.2) — seed
#         ├── tap1 ← Firecracker VM node2 (172.16.0.3) — joins via seed
#         └── tap2 ← Firecracker VM node3 (172.16.0.4) — joins via seed
#
# Port forwards (macOS localhost → Lima → Firecracker VM):
#   127.0.0.1:9042 → 172.16.0.2:9042  (node1 CQL)
#   127.0.0.1:9043 → 172.16.0.3:9042  (node2 CQL)
#   127.0.0.1:9044 → 172.16.0.4:9042  (node3 CQL)
#   127.0.0.1:2022 → 172.16.0.2:22    (node1 SSH — for nemesis injection)
#   127.0.0.1:2023 → 172.16.0.3:22    (node2 SSH)
#   127.0.0.1:2024 → 172.16.0.4:22    (node3 SSH)
#
# NOTE: nemesis tests inject failures via SSH directly to node IPs.
# Since 172.16.0.x is inside Lima's network, macOS cannot reach these IPs
# directly.  After running this script, add a macOS static route so that
# 172.16.0.0/24 is routed through the Lima VM:
#
#   sudo route -n add -net 172.16.0.0/24 $(limactl shell mvm -- ip route | \
#     awk '/default/{print $3}')
#
# That one sudo command lets the test runner SSH directly to 172.16.0.x
# for nemesis operations (iptables, tc netem, etc.).

set -euo pipefail

LIMA="${LIMA_INSTANCE:-mvm}"
FC_KERNEL="${FC_KERNEL:-/home/$(limactl shell "$LIMA" -- whoami 2>/dev/null)/firecracker-assets/vmlinux-6.1.bin}"
FC_ROOTFS_BASE="${FC_ROOTFS:-/home/$(limactl shell "$LIMA" -- whoami 2>/dev/null)/firecracker-assets/rootfs.ext4}"
GATEWAY="172.16.0.1"
NODE_IPS=("172.16.0.2" "172.16.0.3" "172.16.0.4")
NODE_NAMES=("node1" "node2" "node3")
CQL_PORTS=(9042 9043 9044)
SSH_PORTS=(2022 2023 2024)
SEED_IP="172.16.0.2"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KEY_PATH="$REPO_ROOT/rootfs/test_key"

echo "=== Ferrosa 3-Node Firecracker Cluster ==="
echo "  Lima instance : $LIMA"
echo "  Nodes         : ${NODE_IPS[*]}"
echo "  Seed node     : $SEED_IP"
echo ""

# ── Preflight ──────────────────────────────────────────────────────────────
if ! limactl list --format "{{.Name}}" 2>/dev/null | grep -q "^${LIMA}$"; then
    echo "ERROR: Lima instance '$LIMA' is not running."
    echo "  Run: limactl start mvm"
    exit 1
fi

limactl shell "$LIMA" -- bash -c "
    [ -f '$FC_KERNEL' ] || { echo 'ERROR: kernel missing: $FC_KERNEL'; echo 'Run: scripts/lima-fc-assets.sh'; exit 1; }
    [ -f '$FC_ROOTFS_BASE' ] || { echo 'ERROR: rootfs missing: $FC_ROOTFS_BASE'; echo 'Run: scripts/lima-fc-assets.sh'; exit 1; }
"

if [[ ! -f "$KEY_PATH" ]]; then
    echo "ERROR: SSH key not found at $KEY_PATH"
    echo "  Run: scripts/lima-fc-assets.sh (it generates the key)"
    exit 1
fi

# ── Tear down any existing cluster ────────────────────────────────────────
echo "--- Stopping any existing cluster ---"
for i in 0 1 2; do
    SOCK="/tmp/ferrosa-jepsen-cluster-vm${i}.sock"
    limactl shell "$LIMA" -- bash -c "
        pkill -f 'firecracker.*${SOCK}' 2>/dev/null || true
        rm -f '${SOCK}'
    " 2>/dev/null || true
done

# Kill any existing port forwards on our ports.
for port in 9042 9043 9044 2022 2023 2024; do
    lsof -ti tcp:"$port" | xargs kill 2>/dev/null || true
done
sleep 1

# ── Set up TAP devices in Lima ─────────────────────────────────────────────
echo "--- Setting up TAP devices ---"
limactl shell "$LIMA" -- sudo bash -c "
    set -euo pipefail
    for i in 0 1 2; do
        TAP=\"tap\$i\"
        ip link delete \"\$TAP\" 2>/dev/null || true
        ip tuntap add \"\$TAP\" mode tap
        ip link set \"\$TAP\" up
    done
    # Gateway IP on tap0 bridges all VMs to Lima.
    ip addr add $GATEWAY/24 dev tap0 2>/dev/null || true
    sysctl -w net.ipv4.ip_forward=1 >/dev/null
    echo 'TAP devices ready: tap0 tap1 tap2'
"

# ── Create per-node rootfs copies (with different IPs baked into init) ────
echo ""
echo "--- Creating per-node rootfs images ---"
limactl shell "$LIMA" -- sudo bash -c "
    set -euo pipefail
    BASE='$FC_ROOTFS_BASE'
    MNT='/mnt/fc-cluster-node'
    PUB_KEY=\$(cat '$KEY_PATH.pub')

    for i in 0 1 2; do
        NODE_IP=\$(echo '${NODE_IPS[*]}' | tr ' ' '\n' | sed -n \"\$((i+1))p\")
        DEST=\"/tmp/ferrosa-cluster-rootfs-\${i}.ext4\"

        echo \"  node\$((i+1)): \$NODE_IP → \$DEST\"
        cp \"\$BASE\" \"\$DEST\"

        mkdir -p \"\$MNT\"
        mount \"\$DEST\" \"\$MNT\"

        # Update init script with this node's IP.
        printf '%s\n' \
            '#!/bin/sh' \
            'mount -t proc proc /proc' \
            'mount -t sysfs sysfs /sys' \
            'mount -t devtmpfs devtmpfs /dev 2>/dev/null || true' \
            'mkdir -p /var/run' \
            'ip link set lo up' \
            'ip link set eth0 up' \
            \"ip addr add \${NODE_IP}/24 dev eth0\" \
            \"ip route add default via $GATEWAY\" \
            '/usr/sbin/dropbear 2>/dev/null || /usr/sbin/sshd -D &' \
            \"exec /sbin/getty -L ttyS0 115200 vt100\" \
            | tee \"\$MNT/etc/init.d/rcS\" > /dev/null
        chmod +x \"\$MNT/etc/init.d/rcS\"

        # Inject SSH authorized key.
        mkdir -p \"\$MNT/root/.ssh\"
        chmod 700 \"\$MNT/root/.ssh\"
        echo \"\$PUB_KEY\" > \"\$MNT/root/.ssh/authorized_keys\"
        chmod 600 \"\$MNT/root/.ssh/authorized_keys\"

        umount \"\$MNT\"
    done
    echo 'Per-node rootfs images ready.'
"

# ── Boot 3 Firecracker VMs ─────────────────────────────────────────────────
echo ""
echo "--- Booting Firecracker VMs ---"
limactl shell "$LIMA" -- bash -c "
    set -euo pipefail
    for i in 0 1 2; do
        NODE_IP=\$(echo '${NODE_IPS[*]}' | tr ' ' '\n' | sed -n \"\$((i+1))p\")
        SOCK=\"/tmp/ferrosa-jepsen-cluster-vm\${i}.sock\"
        ROOTFS=\"/tmp/ferrosa-cluster-rootfs-\${i}.ext4\"
        TAP=\"tap\${i}\"

        nohup firecracker --api-sock \"\$SOCK\" > \"/tmp/fc-cluster-node\${i}.log\" 2>&1 &
        echo \$! > \"/tmp/fc-cluster-node\${i}.pid\"
        sleep 1

        curl -s -X PUT --unix-socket \"\$SOCK\" http://localhost/boot-source \
            -H 'Content-Type: application/json' \
            -d '{\"kernel_image_path\":\"$FC_KERNEL\",\"boot_args\":\"console=ttyS0 reboot=k panic=1 pci=off rw init=/etc/init.d/rcS\"}' >/dev/null

        curl -s -X PUT --unix-socket \"\$SOCK\" http://localhost/drives/rootfs \
            -H 'Content-Type: application/json' \
            -d \"{\\\"drive_id\\\":\\\"rootfs\\\",\\\"path_on_host\\\":\\\"\$ROOTFS\\\",\\\"is_root_device\\\":true,\\\"is_read_only\\\":false}\" >/dev/null

        curl -s -X PUT --unix-socket \"\$SOCK\" \"/http://localhost/network-interfaces/eth0\" \
            -H 'Content-Type: application/json' \
            -d \"{\\\"iface_id\\\":\\\"eth0\\\",\\\"guest_mac\\\":\\\"AA:FC:00:00:00:0\$((i+2))\\\",\\\"host_dev_name\\\":\\\"\$TAP\\\"}\" >/dev/null

        curl -s -X PUT --unix-socket \"\$SOCK\" http://localhost/machine-config \
            -H 'Content-Type: application/json' \
            -d '{\"vcpu_count\":1,\"mem_size_mib\":512}' >/dev/null

        curl -s -X PUT --unix-socket \"\$SOCK\" http://localhost/actions \
            -H 'Content-Type: application/json' \
            -d '{\"action_type\":\"InstanceStart\"}' >/dev/null

        echo \"  node\$((i+1)) (\$NODE_IP) started\"
    done
"

# ── Wait for SSH on all nodes ──────────────────────────────────────────────
echo ""
echo "--- Waiting for SSH on all nodes ---"
limactl shell "$LIMA" -- bash -c "
    set -euo pipefail
    for NODE_IP in ${NODE_IPS[*]}; do
        echo -n \"  \$NODE_IP: \"
        for i in \$(seq 1 45); do
            if nc -z -w1 \"\$NODE_IP\" 22 2>/dev/null; then
                echo \"SSH ready after \${i}s\"
                break
            fi
            if [ \"\$i\" -eq 45 ]; then
                echo 'TIMEOUT after 45s'
                exit 1
            fi
            sleep 1
        done
    done
"

# ── Start ferrosa on each node via SSH ────────────────────────────────────
echo ""
echo "--- Starting ferrosa on each node ---"

SEED_NODES="${NODE_IPS[0]}:9042,${NODE_IPS[1]}:9042,${NODE_IPS[2]}:9042"

limactl shell "$LIMA" -- bash -c "
    set -euo pipefail
    KEY='$KEY_PATH'
    SSH_OPTS='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5'
    SEED_NODES='$SEED_NODES'

    for NODE_IP in ${NODE_IPS[*]}; do
        echo \"  ferrosa on \$NODE_IP (seeds: \$SEED_NODES)\"
        ssh \$SSH_OPTS -i \"\$KEY\" root@\"\$NODE_IP\" \
            \"nohup /setup-guest.sh --seed-nodes '\$SEED_NODES' --listen-addr '\$NODE_IP:9042' \
             > /var/log/ferrosa.log 2>&1 &\"
    done
"

# ── Wait for CQL on all nodes ─────────────────────────────────────────────
echo ""
echo "--- Waiting for CQL readiness (up to 60s per node) ---"
limactl shell "$LIMA" -- bash -c "
    set -euo pipefail
    for NODE_IP in ${NODE_IPS[*]}; do
        echo -n \"  \$NODE_IP:9042: \"
        for i in \$(seq 1 60); do
            if nc -z -w1 \"\$NODE_IP\" 9042 2>/dev/null; then
                echo \"CQL ready after \${i}s\"
                break
            fi
            if [ \"\$i\" -eq 60 ]; then
                echo 'TIMEOUT'
                echo '  Check logs: limactl shell mvm -- cat /var/log/fc-cluster-node0.log'
                exit 1
            fi
            sleep 1
        done
    done
"

# ── Forward ports to macOS ─────────────────────────────────────────────────
echo ""
echo "--- Forwarding ports to macOS ---"

LIMA_SSH_PORT=$(limactl show-ssh "$LIMA" 2>/dev/null | grep -o 'Port=[0-9]*' | head -1 | cut -d= -f2 || echo "59766")
LIMA_KEY="$HOME/.lima/_config/user"

# CQL port forwards: localhost:9042-9044 → Lima → 172.16.0.2-4:9042
for i in 0 1 2; do
    LOCAL_PORT="${CQL_PORTS[$i]}"
    REMOTE_IP="${NODE_IPS[$i]}"
    ssh -fN \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ExitOnForwardFailure=yes \
        -i "$LIMA_KEY" \
        -p "$LIMA_SSH_PORT" \
        -L "127.0.0.1:${LOCAL_PORT}:${REMOTE_IP}:9042" \
        "$(whoami)@127.0.0.1" 2>/dev/null &
    echo "  127.0.0.1:${LOCAL_PORT} → ${REMOTE_IP}:9042"
done

# SSH port forwards: localhost:2022-2024 → Lima → 172.16.0.2-4:22
for i in 0 1 2; do
    LOCAL_PORT="${SSH_PORTS[$i]}"
    REMOTE_IP="${NODE_IPS[$i]}"
    ssh -fN \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ExitOnForwardFailure=yes \
        -i "$LIMA_KEY" \
        -p "$LIMA_SSH_PORT" \
        -L "127.0.0.1:${LOCAL_PORT}:${REMOTE_IP}:22" \
        "$(whoami)@127.0.0.1" 2>/dev/null &
    echo "  127.0.0.1:${LOCAL_PORT} → ${REMOTE_IP}:22"
done

sleep 2

# Verify CQL forwards.
echo ""
echo "--- Verifying CQL port forwards ---"
for i in 0 1 2; do
    PORT="${CQL_PORTS[$i]}"
    if nc -z -w2 127.0.0.1 "$PORT" 2>/dev/null; then
        echo "  ✓ 127.0.0.1:${PORT}"
    else
        echo "  ✗ 127.0.0.1:${PORT}  <-- forward may not be active"
    fi
done

# ── macOS static route (optional, needed for nemesis SSH injection) ────────
LIMA_HOST_IP=$(limactl shell "$LIMA" -- ip route 2>/dev/null | awk '/default/{print $3}' | head -1 || true)

echo ""
echo "========================================"
echo "Cluster is up!"
echo "========================================"
echo ""
echo "Run cluster tests:"
echo "  FERROSA_TEST_CLUSTER_NODES=127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044 \\"
echo "  FERROSA_TEST_VM_KEY=$KEY_PATH \\"
echo "  cargo test -p ferrosa-jepsen --test nemesis_correctness -- --nocapture"
echo ""
echo "--- Optional: enable nemesis SSH injection from macOS ---"
echo "Nemesis tests inject failures via SSH to 172.16.0.x directly."
echo "The SSH port-forwards above work for CQL but the nemesis NemesisContext"
echo "sends iptables/tc commands to the node IPs, not to localhost ports."
echo ""
echo "To route 172.16.0.0/24 through the Lima VM (one-time, macOS sudo required):"
if [[ -n "$LIMA_HOST_IP" ]]; then
    echo "  sudo route -n add -net 172.16.0.0/24 $LIMA_HOST_IP"
    echo ""
    echo "With that route in place, use:"
    echo "  FERROSA_TEST_CLUSTER_NODES=172.16.0.2:9042,172.16.0.3:9042,172.16.0.4:9042 \\"
    echo "  FERROSA_TEST_VM_KEY=$KEY_PATH \\"
    echo "  FERROSA_TEST_FIRECRACKER=1 \\"
    echo "  cargo test -p ferrosa-jepsen --test nemesis_correctness -- --nocapture"
else
    echo "  sudo route -n add -net 172.16.0.0/24 <lima-host-ip>"
    echo "  (Run: limactl shell mvm -- ip route | awk '/default/{print \$3}')"
fi
echo ""
echo "Teardown:"
echo "  limactl shell mvm -- bash -c 'pkill -f firecracker; for i in 0 1 2; do sudo ip link delete tap\$i 2>/dev/null || true; done'"
echo "  for p in 9042 9043 9044 2022 2023 2024; do lsof -ti tcp:\$p | xargs kill 2>/dev/null || true; done"
