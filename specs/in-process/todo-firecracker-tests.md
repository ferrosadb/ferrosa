---
type: todo
priority: P2
status: deferred
created: 2026-05-10
---

# TODO: Firecracker-gated Jepsen tests

Three tests in `ferrosa-jepsen` panic with `FERROSA_TEST_FIRECRACKER not set`:

- `firecracker::tests::provision_single_vm`
- `ssh::tests::ssh_upload_file`
- `ssh::tests::ssh_execute_command`

Per CLAUDE.md test policy these are not `#[ignore]`d — they panic with setup
instructions. They exercise `ferrosa-jepsen/src/firecracker.rs` and
`ssh.rs`, orthogonal to Raft correctness.

## Status as of 2026-05-10

A first attempt at running them in this environment caused machine
instability (probable OOM under concurrent docker-build + apk-add +
docker-export). The setup was abandoned mid-stream. The Docker-gated
tests (5 of 8) ran successfully on the 4xxxx port range; the 3
Firecracker-gated tests are skipped pending a calmer setup window.

## Setup state already done

- Firecracker v1.15.1 binary installed at `~/bin/firecracker` and
  `~/.local/bin/firecracker`.
- `tap0` network device created with the test user as owner
  (`172.16.0.1/24`).
- User in `kvm` group (operator-applied via `sudo usermod -aG kvm`),
  pending shell re-login or `newgrp kvm`.
- SSH test keypair generated at `ferrosa-jepsen/rootfs/test_key{,.pub}`
  (per-machine, gitignored).

## Setup state still required

1. **Kernel image.** Download from
   `https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.15/x86_64/vmlinux-6.1.155`
   into `ferrosa-jepsen/rootfs/vmlinux` (~30 MiB).

2. **Root filesystem.** The existing `ferrosa-jepsen/rootfs/build.sh`
   needs root for losetup + mount. Two approaches:

   a) **Operator runs `sudo bash ferrosa-jepsen/rootfs/build.sh`** — this
      builds the canonical Alpine rootfs with sshd configured and the
      test_key.pub in `/root/.ssh/authorized_keys`. ~1 GiB image.

   b) **Sudo-free path via `mkfs.ext4 -d`** — builds rootfs in a Docker
      container, exports via `docker export | tar -x`, then
      `mkfs.ext4 -d <tree> <image>`. Avoids losetup/mount but requires
      careful sequencing (do NOT run concurrent with `docker compose
      build`; that combination caused the 2026-05-10 crash). Workflow
      sketch:

      ```sh
      mkdir -p /tmp/fc-rootfs
      CONT=$(docker run -d alpine:latest sh -c '
        apk add --no-cache openssh-server openrc bash iproute2 ca-certificates
        rc-update add sshd default
        ssh-keygen -A
        passwd -d root
        mkdir -p /root/.ssh && chmod 700 /root/.ssh
        cat >/etc/network/interfaces <<EOF
      auto eth0
      iface eth0 inet static
          address 172.16.0.2/24
          gateway 172.16.0.1
      EOF
      ')
      docker wait "$CONT"
      docker export "$CONT" | tar -x -C /tmp/fc-rootfs
      docker rm "$CONT"
      cp ferrosa-jepsen/rootfs/test_key.pub /tmp/fc-rootfs/root/.ssh/authorized_keys
      mkfs.ext4 -d /tmp/fc-rootfs -F ferrosa-jepsen/rootfs/rootfs.ext4 1G
      ```

3. **Override env vars for tests** (the existing scripts assume Lima on
   macOS forwarding port 2022):

   ```sh
   export FERROSA_TEST_FIRECRACKER=1
   export FERROSA_TEST_VM_HOST=172.16.0.2   # guest IP, not Lima localhost
   export FERROSA_TEST_VM_PORT=22           # native sshd, not Lima-forwarded
   export FERROSA_TEST_VM_KEY=ferrosa-jepsen/rootfs/test_key
   sg kvm -c 'cargo test -p ferrosa-jepsen --lib ssh provision_single_vm'
   ```

   `sg kvm` is needed unless the operator has re-logged in since `usermod`.

## Acceptance

- All 3 tests pass without panic.
- A `tier-firecracker-smoke` Jepsen tier runs in nightly CI gated on
  `FERROSA_TEST_FIRECRACKER=1`.

## Related

- `specs/in-process/sprint-02-progress.md` — the Sprint 2 work that left
  the 5 Docker tests passing and these 3 deferred.
- `scripts/lima-fc-setup.sh`, `scripts/lima-fc-assets.sh` — macOS-via-Lima
  variants of the same setup. Linux-native rewrite is in scope here.
