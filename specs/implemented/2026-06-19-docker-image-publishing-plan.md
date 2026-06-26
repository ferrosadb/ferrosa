# Docker Image Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish a minimal, full-featured, config-injectable `ferrosa` container image to GHCR (multi-arch amd64+arm64) for both the nightly and stable release channels.

**Architecture:** Add a curated `full` cargo feature, flip all release/CI binary builds to it (so `.deb`, tarballs, and image share one all-features musl static binary), then add a COPY-only Alpine `Dockerfile.release` and a `docker-image` job in `release.yml` that reuses the prebuilt per-arch binaries to push a multi-arch manifest. A musl static-link validation gate (Phase 0) runs first.

**Tech Stack:** Rust (musl static, `x86_64`/`aarch64-unknown-linux-musl`), GitHub Actions, Docker buildx (multi-arch), Alpine, GHCR.

## Global Constraints

- **Spec:** `specs/proposed/2026-06-19-docker-image-publishing-design.md` — read it first.
- **Curated feature:** `full = ["otel", "flight", "ferrosa-cql/asc-udf"]`. Excludes `live-infra-tests` and `ferrosa-cluster/sprint-03-engine-transfer`. Never use raw `--all-features`.
- **Build invocation everywhere:** `cargo build --release --target <triple> -p ferrosa -p ferrosa-ctl --features ferrosa/full` (package-qualified so one invocation covers both binaries; `cross` substitutes for `cargo` on aarch64).
- **Registry / image:** `ghcr.io/${{ github.repository_owner }}/ferrosa` (= `ghcr.io/ferrosadb/ferrosa`).
- **Tags:** stable (`inputs.prerelease == 'false'`): `:latest`, `:v{VERSION}`, `:{MAJOR}.{MINOR}`, `:{MAJOR}`. nightly (else): `:nightly`, `:{GITHUB_REF_NAME}` (the CalVer tag, e.g. `v2026.06.19.0017`).
- **Prerelease detection:** mirror the existing `release` job exactly — `PRERELEASE="${{ github.event.inputs.prerelease }}"`; `false` → stable, anything else → nightly.
- **Ports:** CQL 9042, internode 17000, web/Prometheus 9090, graph HTTP 7474, Bolt 7687, Arrow Flight gRPC 8815.
- **Config injection:** no active config baked in; `FERROSA_*` env vars and/or a mounted `/etc/ferrosa/ferrosa.toml`. Ship `config/ferrosa.example.toml` only as `/etc/ferrosa/ferrosa.example.toml`.
- **Hardening:** non-root user `ferrosa` (UID 10001); read-only-rootfs compatible (only `/var/lib/ferrosa` written).
- **Actions MUST be SHA-pinned** (`actions-pin-guard.yml` enforces this). Reuse these already-pinned refs verbatim; prefer raw CLI over adding new actions:
  - `actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2`
  - `actions/download-artifact@fa0a91b85d4f404e444e00e005971372dc801d16 # v4.1.8`
  - `actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4.6.2`
  - `dtolnay/rust-toolchain@29eef336d9b2848a0b548edc03f92a220660cdb8 # stable`
  - `Swatinem/rust-cache@c19371144df3bb44fab255c43d04cbc2ab54d1c4 # v2.9.1`
- **GHCR login (no new action):** `echo "${{ secrets.GITHUB_TOKEN }}" | docker login ghcr.io -u "${{ github.actor }}" --password-stdin` (same pattern as `ci.yml`).
- **Conventions:** Conventional Commits; never hand-edit `[workspace.package] version`; feature branch only (`feat/docker-image-publishing`); no specs under `docs/`.
- **Branch/worktree:** all work in worktree `/home/bkearns/src/ferrosa-suite/worktrees/docker-image-publishing` on `feat/docker-image-publishing`.

## File Structure

- **Modify** `ferrosa/Cargo.toml` — add the `full` feature.
- **Create** `Dockerfile.release` — Alpine, COPY-only, multi-arch via `TARGETARCH`.
- **Modify** `.github/workflows/release.yml` — flip 3 build jobs to `--features ferrosa/full`; add `docker-image` job.
- **Modify** `.github/workflows/ci.yml` — flip the musl build to `--features ferrosa/full`.
- **Modify** `specs/release-process.md` — document the image artifact, tags, config contract, ports.
- **Reference (no change):** `.github/scripts/stage-release-tarball.sh` (tarball top-level layout: `ferrosa`, `ferrosa-ctl`, `config/ferrosa.example.toml`).

---

### Task 1: Add the curated `full` cargo feature

**Files:**
- Modify: `ferrosa/Cargo.toml` (the `[features]` table)

**Interfaces:**
- Produces: cargo feature `ferrosa/full` enabling `otel`, `flight`, and `ferrosa-cql/asc-udf`. Every later build step consumes it via `--features ferrosa/full`.

- [ ] **Step 1: Verify the current feature table**

Run: `sed -n '/^\[features\]/,/^$/p' ferrosa/Cargo.toml`
Expected: shows `default = []`, `flight = ["dep:ferrosa-flight"]`, and the `otel = [...]` list. No `full` present.

- [ ] **Step 2: Add the `full` feature**

In `ferrosa/Cargo.toml`, the `[features]` table becomes:

```toml
[features]
default = []
# Arrow Flight (gRPC) query endpoint on port 8815 (see ferrosa-flight).
flight = ["dep:ferrosa-flight"]
otel = [
    "tracing-opentelemetry",
    "opentelemetry",
    "opentelemetry-otlp",
    "opentelemetry_sdk",
]
# All shippable optional features. Curated, NOT --all-features: excludes
# `live-infra-tests` (test-only) and `ferrosa-cluster/sprint-03-engine-transfer`
# (in-progress). Used by release artifacts and the published Docker image.
full = ["otel", "flight", "ferrosa-cql/asc-udf"]
```

- [ ] **Step 3: Verify the feature resolves (host build, glibc)**

Run: `cargo metadata --no-deps --format-version 1 >/dev/null && cargo check -p ferrosa --features ferrosa/full 2>&1 | tail -20`
Expected: `Finished` with no error. (This proves the feature graph is valid before the musl gate. If `ferrosa-cql` does not expose `asc-udf`, this fails fast — see Task 2 fallback.)

- [ ] **Step 4: Commit**

```bash
git add ferrosa/Cargo.toml
git commit -m "feat(build): add curated full feature (otel+flight+asc-udf)"
```

---

### Task 2: Phase 0 — musl static-link validation gate

**Files:** none (validation only; produces a go/no-go decision)

**Interfaces:**
- Consumes: `ferrosa/full` from Task 1.
- Produces: confirmation that `--features ferrosa/full` builds a **statically linked** binary for both `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`. **Gates Tasks 3–7.**

This runs in a container matching CI (musl tools + capnproto), so the host toolchain is irrelevant.

- [ ] **Step 1: Build x86_64 musl with full features**

Run:
```bash
docker run --rm \
  -v "$PWD":/work \
  -v ferrosa-cargo-registry:/usr/local/cargo/registry \
  -v ferrosa-cargo-git:/usr/local/cargo/git \
  -e CARGO_TARGET_DIR=/work/target-musl-fullcheck \
  -w /work rust:latest bash -c '
    set -e
    apt-get update -qq && apt-get install -y -qq musl-tools capnproto cmake clang
    rustup target add x86_64-unknown-linux-musl
    cargo build --release --target x86_64-unknown-linux-musl -p ferrosa --features ferrosa/full
    file target-musl-fullcheck/x86_64-unknown-linux-musl/release/ferrosa'
```
Expected: build `Finished`; `file` output contains `statically linked`. **If it fails to compile or reports `dynamically linked`, STOP** — do not proceed. Record the exact error and go to Step 4 (fallback reporting).

- [ ] **Step 2: Build aarch64 musl with full features (via cross)**

Run:
```bash
docker run --rm \
  -v "$PWD":/work \
  -v ferrosa-cargo-registry:/usr/local/cargo/registry \
  -v ferrosa-cargo-git:/usr/local/cargo/git \
  -e CARGO_TARGET_DIR=/work/target-musl-fullcheck \
  -w /work rust:latest bash -c '
    set -e
    apt-get update -qq && apt-get install -y -qq capnproto cmake clang
    cargo install cross --git https://github.com/cross-rs/cross --tag v0.2.5 2>/dev/null || cargo install cross --version 0.2.5
    cross build --release --target aarch64-unknown-linux-musl -p ferrosa --features ferrosa/full
    file target-musl-fullcheck/aarch64-unknown-linux-musl/release/ferrosa || true'
```
Expected: build `Finished`. (`file` may not classify a foreign-arch binary precisely; the pass criterion here is a clean cross build. `Cross.toml` already installs capnproto in the cross image.) **If the cross build fails, STOP** and go to Step 4.

- [ ] **Step 3: Record success and proceed**

If both builds succeeded with a statically linked x86_64 binary, note it in the task tracker / PR description and continue to Task 3.

- [ ] **Step 4: Fallback reporting (only if Step 1 or 2 failed)**

Do NOT silently drop features. Stop and report to the user with the captured error and these options (from the spec §3):
  1. glibc-based image for the full feature set (drops "tiny musl static" for "all features");
  2. a `full` glibc/debian-slim image **plus** a separate `full-static` musl image without `asc-udf`;
  3. drop `asc-udf` from `full` (keep `otel`+`flight`, musl static).
Wait for the user's decision before changing the plan.

---

### Task 3: Create `Dockerfile.release` (Alpine, COPY-only, multi-arch)

**Files:**
- Create: `Dockerfile.release`

**Interfaces:**
- Consumes (build context): `dist/${TARGETARCH}/ferrosa`, `dist/${TARGETARCH}/ferrosa-ctl` (laid down by Task 5's job), and `config/ferrosa.example.toml` (in the repo checkout). `TARGETARCH` is `amd64` or `arm64`, set automatically by buildx per `--platform`.
- Produces: a runnable image whose `ENTRYPOINT` is `ferrosa`, config-injectable via env/mount.

- [ ] **Step 1: Write `Dockerfile.release`**

```dockerfile
# Minimal runtime image for the ferrosa engine.
# COPY-only: the musl static binaries are built upstream (release.yml) and
# downloaded into dist/<arch>/ before this build. No compilation happens here,
# so the multi-arch build needs no QEMU emulation.
FROM alpine:3.20

# ca-certificates: required for TLS to S3. tzdata: correct timestamps/logs.
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S ferrosa \
    && adduser -S -G ferrosa -u 10001 ferrosa \
    && mkdir -p /etc/ferrosa /var/lib/ferrosa \
    && chown -R ferrosa:ferrosa /var/lib/ferrosa

# buildx sets TARGETARCH automatically per --platform entry (amd64 | arm64).
ARG TARGETARCH
COPY dist/${TARGETARCH}/ferrosa     /usr/local/bin/ferrosa
COPY dist/${TARGETARCH}/ferrosa-ctl /usr/local/bin/ferrosa-ctl
# Reference only — NOT the active config. Operators inject via env or a mount.
COPY config/ferrosa.example.toml    /etc/ferrosa/ferrosa.example.toml

ENV FERROSA_CONFIG=/etc/ferrosa/ferrosa.toml \
    FERROSA_DATA_DIR=/var/lib/ferrosa

# CQL, internode RPC, web/Prometheus, graph HTTP, Bolt, Arrow Flight gRPC.
EXPOSE 9042 17000 9090 7474 7687 8815
VOLUME /var/lib/ferrosa
USER ferrosa
ENTRYPOINT ["ferrosa"]
```

- [ ] **Step 2: Lint the Dockerfile**

Run: `docker run --rm -i hadolint/hadolint < Dockerfile.release || true`
Expected: no errors (info/style warnings acceptable). If `hadolint` is unavailable, skip; the real test is Step 3 of Task 6.

- [ ] **Step 3: Commit**

```bash
git add Dockerfile.release
git commit -m "feat(docker): add minimal alpine Dockerfile.release for engine image"
```

---

### Task 4: Flip release + CI binary builds to `--features ferrosa/full`

**Files:**
- Modify: `.github/workflows/release.yml:44` (x86_64), `:170` (aarch64 `cross`), `:216` (macOS)
- Modify: `.github/workflows/ci.yml:102` (musl build)

**Interfaces:**
- Consumes: `ferrosa/full` (Task 1).
- Produces: all release artifacts (`.deb`, tarballs) and the PR-CI musl build use the all-features binary.

- [ ] **Step 1: Edit `release.yml` x86_64 build (line ~44)**

Change:
```
          cargo build --release --target x86_64-unknown-linux-musl -p ferrosa -p ferrosa-ctl
```
to:
```
          cargo build --release --target x86_64-unknown-linux-musl -p ferrosa -p ferrosa-ctl --features ferrosa/full
```

- [ ] **Step 2: Edit `release.yml` aarch64 build (line ~170)**

Change:
```
          cross build --release --target aarch64-unknown-linux-musl -p ferrosa -p ferrosa-ctl
```
to:
```
          cross build --release --target aarch64-unknown-linux-musl -p ferrosa -p ferrosa-ctl --features ferrosa/full
```

- [ ] **Step 3: Edit `release.yml` macOS build (line ~216)**

Change:
```
          cargo build --release --target aarch64-apple-darwin -p ferrosa -p ferrosa-ctl
```
to:
```
          cargo build --release --target aarch64-apple-darwin -p ferrosa -p ferrosa-ctl --features ferrosa/full
```

- [ ] **Step 4: Edit `ci.yml` musl build (line ~102)**

Change:
```
        run: cargo build --release --target x86_64-unknown-linux-musl -p ferrosa -p ferrosa-ctl
```
to:
```
        run: cargo build --release --target x86_64-unknown-linux-musl -p ferrosa -p ferrosa-ctl --features ferrosa/full
```

- [ ] **Step 5: Validate workflow YAML**

Run: `for f in .github/workflows/release.yml .github/workflows/ci.yml; do python3 -c "import yaml,sys; yaml.safe_load(open('$f')); print('$f OK')"; done`
Expected: both print `OK`. If `actionlint` is installed, also run `actionlint .github/workflows/release.yml .github/workflows/ci.yml` and expect no errors.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/release.yml .github/workflows/ci.yml
git commit -m "build(ci): build release + CI musl binaries with full features"
```

---

### Task 5: Add the `docker-image` job to `release.yml`

**Files:**
- Modify: `.github/workflows/release.yml` (add a new top-level job under `jobs:`)

**Interfaces:**
- Consumes: tarball artifacts `tarball-x86_64-unknown-linux-musl` and `tarball-aarch64-unknown-linux-musl` (each contains top-level `ferrosa`, `ferrosa-ctl`); `Dockerfile.release` (Task 3); `inputs.prerelease`; `GITHUB_REF_NAME`.
- Produces: multi-arch image pushed to `ghcr.io/<owner>/ferrosa` with the channel-appropriate tags.

- [ ] **Step 1: Add the job**

Append this job to the `jobs:` map in `.github/workflows/release.yml` (sibling of `build-linux-x86_64`, `release`, etc.). Note the job-level `permissions` grants `packages: write`:

```yaml
  # ── Multi-arch container image (GHCR) ────────────────────────────────
  docker-image:
    name: Build & push container image (GHCR)
    needs:
      - build-linux-x86_64
      - build-linux-aarch64
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2

      - name: Download Linux musl tarball artifacts
        uses: actions/download-artifact@fa0a91b85d4f404e444e00e005971372dc801d16 # v4.1.8
        with:
          path: artifacts

      - name: Extract per-arch binaries into dist/<arch>/
        run: |
          set -euo pipefail
          mkdir -p dist/amd64 dist/arm64
          x86_tar=$(find artifacts -name 'ferrosa-*-x86_64-unknown-linux-musl.tar.gz' | head -n1)
          arm_tar=$(find artifacts -name 'ferrosa-*-aarch64-unknown-linux-musl.tar.gz' | head -n1)
          test -n "$x86_tar" || { echo "ERROR: x86_64 tarball not found" >&2; exit 1; }
          test -n "$arm_tar" || { echo "ERROR: aarch64 tarball not found" >&2; exit 1; }
          tar -xzf "$x86_tar" -C dist/amd64 ferrosa ferrosa-ctl
          tar -xzf "$arm_tar" -C dist/arm64 ferrosa ferrosa-ctl
          chmod 0755 dist/amd64/ferrosa dist/amd64/ferrosa-ctl dist/arm64/ferrosa dist/arm64/ferrosa-ctl
          ls -l dist/amd64 dist/arm64

      - name: Compute image tags
        id: tags
        run: |
          set -euo pipefail
          IMAGE="ghcr.io/${{ github.repository_owner }}/ferrosa"
          REF="${GITHUB_REF_NAME}"          # e.g. v0.13.0 or v2026.06.19.0017
          VERSION="${REF#v}"
          PRERELEASE="${{ github.event.inputs.prerelease }}"
          TAGS=""
          if [ "${PRERELEASE}" = "false" ]; then
            # Stable: latest + full + rolling major.minor + major.
            MAJOR="${VERSION%%.*}"
            MINOR="${VERSION#*.}"; MINOR="${MINOR%%.*}"
            TAGS="${IMAGE}:latest ${IMAGE}:v${VERSION} ${IMAGE}:${MAJOR}.${MINOR} ${IMAGE}:${MAJOR}"
          else
            # Nightly: moving :nightly + the exact CalVer ref.
            TAGS="${IMAGE}:nightly ${IMAGE}:${REF}"
          fi
          echo "Computed tags: ${TAGS}"
          # Emit as space-separated for the build step.
          echo "tags=${TAGS}" >> "$GITHUB_OUTPUT"

      - name: Log in to GHCR
        run: echo "${{ secrets.GITHUB_TOKEN }}" | docker login ghcr.io -u "${{ github.actor }}" --password-stdin

      - name: Set up buildx (multi-arch)
        run: |
          docker buildx create --name ferrosa-builder --use
          docker buildx inspect --bootstrap

      - name: Build and push multi-arch image
        run: |
          set -euo pipefail
          tag_args=""
          for t in ${{ steps.tags.outputs.tags }}; do
            tag_args="${tag_args} -t ${t}"
          done
          docker buildx build \
            --platform linux/amd64,linux/arm64 \
            -f Dockerfile.release \
            ${tag_args} \
            --push \
            .

      - name: Inspect published manifest
        run: |
          first_tag=$(echo "${{ steps.tags.outputs.tags }}" | awk '{print $1}')
          docker buildx imagetools inspect "${first_tag}"
```

- [ ] **Step 2: Validate workflow YAML**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('release.yml OK')"`
Expected: `release.yml OK`. If `actionlint` is available: `actionlint .github/workflows/release.yml` → no errors.

- [ ] **Step 3: Sanity-check the tag computation logic locally**

Run:
```bash
bash -c '
for REF in v0.13.0 v2026.06.19.0017; do
 for PRERELEASE in false true; do
  IMAGE=ghcr.io/ferrosadb/ferrosa; VERSION="${REF#v}"
  if [ "$PRERELEASE" = false ]; then
    MAJOR="${VERSION%%.*}"; MINOR="${VERSION#*.}"; MINOR="${MINOR%%.*}"
    echo "REF=$REF pre=$PRERELEASE -> $IMAGE:latest $IMAGE:v$VERSION $IMAGE:$MAJOR.$MINOR $IMAGE:$MAJOR"
  else
    echo "REF=$REF pre=$PRERELEASE -> $IMAGE:nightly $IMAGE:$REF"
  fi
 done
done'
```
Expected (the meaningful rows): stable `v0.13.0` → `:latest :v0.13.0 :0.13 :0`; nightly `v2026.06.19.0017` → `:nightly :v2026.06.19.0017`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "feat(ci): publish multi-arch ferrosa image to GHCR on release"
```

---

### Task 6: Local end-to-end smoke (amd64 image)

**Files:** none (verification only)

**Interfaces:**
- Consumes: `Dockerfile.release`, the `full` feature. Proves the image boots and is config-injectable before relying on CI.

- [ ] **Step 1: Build the amd64 musl full binary locally into the build context**

Run:
```bash
docker run --rm \
  -v "$PWD":/work \
  -v ferrosa-cargo-registry:/usr/local/cargo/registry \
  -v ferrosa-cargo-git:/usr/local/cargo/git \
  -e CARGO_TARGET_DIR=/work/target-musl-fullcheck \
  -w /work rust:latest bash -c '
    set -e
    apt-get update -qq && apt-get install -y -qq musl-tools capnproto cmake clang
    rustup target add x86_64-unknown-linux-musl
    cargo build --release --target x86_64-unknown-linux-musl -p ferrosa -p ferrosa-ctl --features ferrosa/full'
mkdir -p dist/amd64
cp target-musl-fullcheck/x86_64-unknown-linux-musl/release/ferrosa     dist/amd64/
cp target-musl-fullcheck/x86_64-unknown-linux-musl/release/ferrosa-ctl dist/amd64/
```
Expected: `dist/amd64/ferrosa` and `ferrosa-ctl` exist.

- [ ] **Step 2: Build the image for amd64 only (load locally)**

Run: `docker buildx build --platform linux/amd64 -f Dockerfile.release -t ferrosa-image-smoke:local --load .`
Expected: build succeeds; `docker images ferrosa-image-smoke` shows the image.

- [ ] **Step 3: Record the image size**

Run: `docker images ferrosa-image-smoke:local --format '{{.Size}}'`
Expected: a value materially smaller than the glibc node image (~485MB). Record it in the PR description.

- [ ] **Step 4: Run with env-only config (no mounted file) and check readiness**

Run:
```bash
docker rm -f ferrosa-smoke 2>/dev/null || true
docker run -d --name ferrosa-smoke \
  -e FERROSA_MODE=development \
  -e FERROSA_CQL_BIND=0.0.0.0:9042 \
  -e FERROSA_WEB_BIND=0.0.0.0:9090 \
  -p 19142:9042 -p 19190:9090 \
  ferrosa-image-smoke:local
# give it a few seconds to bind, then probe the web/health endpoint
sleep 8
docker logs ferrosa-smoke 2>&1 | tail -30
curl -fsS http://127.0.0.1:19190/healthz/ready || curl -fsS http://127.0.0.1:19190/healthz/live || echo "PROBE_FAILED"
```
Expected: logs show the engine booting and binding 9042/9090 (config came purely from env vars — proves injection). A health endpoint responds. If the exact health path differs, confirm against `ferrosa/src` and use the correct one. `PROBE_FAILED` with a clean boot log + bound ports is acceptable for single-node (readiness may require cluster quorum); the key assertion is **boot + env-driven bind**, not full readiness.

- [ ] **Step 5: Confirm non-root + binary is the full build**

Run:
```bash
docker exec ferrosa-smoke id -u                 # expect 10001 (non-root)
docker exec ferrosa-smoke ferrosa --version     # expect a version string, X.Y.Z[-nightly]
docker rm -f ferrosa-smoke
```
Expected: UID `10001`; a version prints (confirms the static binary runs on Alpine).

- [ ] **Step 6: Clean up build context (do not commit binaries)**

Run: `rm -rf dist target-musl-fullcheck && git status --porcelain`
Expected: no `dist/` or `target-musl-fullcheck/` shown. (Confirm `.gitignore` already ignores `dist/` and `target*/`; if not, add them in this step and commit that `.gitignore` change only.)

---

### Task 7: Document the image in `release-process.md`

**Files:**
- Modify: `specs/release-process.md`

**Interfaces:**
- Consumes: the finalized tag matrix, ports, and config contract.

- [ ] **Step 1: Add a "Container image" section**

Add a section to `specs/release-process.md` documenting:

```markdown
## Container image

Every release publishes a multi-arch (`linux/amd64`, `linux/arm64`) image to
`ghcr.io/ferrosadb/ferrosa`, built by the `docker-image` job in `release.yml`.

- **Base:** `alpine:3.20` + `ca-certificates` + `tzdata`. Built by COPYing the
  prebuilt musl static binary — no compilation in Docker, no QEMU.
- **Features:** built with `--features ferrosa/full` (otel + flight + asc-udf),
  identical to the `.deb`/tarball binaries.
- **Tags:**
  - Stable (`prerelease=false`): `:latest`, `:vX.Y.Z`, `:X.Y`, `:X`.
  - Nightly (`prerelease=true`): `:nightly`, `:<CalVer>` (e.g. `:v2026.06.19.0017`).
- **Runs as** non-root UID 10001; `--read-only` rootfs compatible (only the
  `/var/lib/ferrosa` volume is written).
- **Exposed ports:** 9042 (CQL), 17000 (internode), 9090 (web/Prometheus),
  7474 (graph HTTP), 7687 (Bolt), 8815 (Arrow Flight gRPC).

### Configuration injection

No active config is baked in. Configure via either:

- `FERROSA_*` environment variables (e.g. `FERROSA_CQL_BIND`, `FERROSA_S3_*`,
  `FERROSA_INTERNODE_BROADCAST`, `FERROSA_DATA_DIR`) — no file required; a missing
  `FERROSA_CONFIG` file is tolerated, or
- mounting a TOML file at `/etc/ferrosa/ferrosa.toml` (or set `FERROSA_CONFIG`).

A reference config ships at `/etc/ferrosa/ferrosa.example.toml`.

Example:

\`\`\`bash
docker run -d --name ferrosa \
  -e FERROSA_MODE=production \
  -e FERROSA_CQL_BIND=0.0.0.0:9042 \
  -e FERROSA_INTERNODE_BROADCAST=10.0.0.5:17000 \
  -e FERROSA_S3_ENDPOINT=https://s3.example.com \
  -e FERROSA_S3_BUCKET=ferrosa \
  -v ferrosa-data:/var/lib/ferrosa \
  -p 9042:9042 \
  ghcr.io/ferrosadb/ferrosa:latest
\`\`\`
```

- [ ] **Step 2: Verify the doc renders / no broken markdown**

Run: `sed -n '/## Container image/,/^## [A-Z]/p' specs/release-process.md | head -60`
Expected: the new section prints intact.

- [ ] **Step 3: Commit**

```bash
git add specs/release-process.md
git commit -m "docs(release): document the published container image and config injection"
```

---

## Self-Review

**Spec coverage:**
- Tiny base + musl static → Tasks 3, 6 (Alpine, COPY-only, size recorded). ✓
- Full features incl. asc-udf → Tasks 1, 2 (feature + gate), 4 (artifact consistency). ✓
- Config injectable → Task 3 (env/mount, no baked config), 6 Step 4 (env-only boot proof), 7 (documented). ✓
- Nightly + release coverage → Task 5 (single job keyed on `inputs.prerelease`, served via dispatch/tag-push). ✓
- Multi-arch GHCR → Task 5. ✓
- Tag matrix → Task 5 Steps 1+3. ✓
- Phase 0 gate with fallback → Task 2. ✓
- Docs/ exclusion + specs location → plan + spec under `specs/proposed/`. ✓
- Ports incl. flight 8815 → Tasks 3, 7. ✓

**Placeholder scan:** No TBD/TODO; every code/YAML/command step has concrete content. ✓

**Type/name consistency:** `ferrosa/full` feature name, `dist/${TARGETARCH}/` layout, artifact names `tarball-{x86_64,aarch64}-unknown-linux-musl`, image `ghcr.io/${{ github.repository_owner }}/ferrosa`, and prerelease condition `inputs.prerelease == 'false'` are used identically across Tasks 1–7. ✓

**Open verification item for execution:** the exact health endpoint path (`/healthz/ready` vs `/healthz/live`) — Task 6 Step 4 instructs confirming against source; non-blocking for the boot+bind assertion.
