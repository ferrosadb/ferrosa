# Design: Publish minimal Docker images for nightly & release

- **Status:** Proposed
- **Date:** 2026-06-19
- **Branch:** `feat/docker-image-publishing`
- **Owner:** release/build

## Goal

Publish an official `ferrosa` container image from CI for both the **nightly** and
**stable release** channels. The image must be:

1. **As small as practical** — a tiny base on top of the existing **musl static** binary,
   so no glibc/runtime dependency sprawl.
2. **Full-featured** — built with all shippable optional features enabled (including
   `asc-udf` inline AssemblyScript/WASM UDF compilation), consistent with the other
   release artifacts.
3. **Config-injectable at runtime** — no configuration baked into the image; everything
   is driven by `FERROSA_*` environment variables and/or a mounted config file, so the
   container orchestrator owns configuration.

## Background (current state)

- **`release.yml`** is the real artifact builder: it builds `x86_64-unknown-linux-musl`
  (native) and `aarch64-unknown-linux-musl` (via `cross`) static binaries plus a macOS
  `aarch64-apple-darwin` build, packages `.deb` + tarballs, generates `SHA256SUMS`, and
  creates the GitHub Release. Triggered on tags `v*` and via `workflow_dispatch` with a
  `prerelease` boolean input.
- **`nightly-release.yml`** computes the next SemVer from Conventional Commits, bumps the
  workspace version to `X.Y.Z-nightly`, creates a CalVer tag `vYYYY.MM.DD.HHMM`, and
  **dispatches `release.yml` with `prerelease=true`**. Stable promotion
  (`promote-release.yml`) dispatches `release.yml` with `prerelease=false`.
- The musl static build is proven: `.cargo/config.toml` sets the musl linker +
  `-C target-feature=+crt-static`; `Cross.toml` installs `capnproto` as a pre-build step
  for the aarch64 cross build.
- **No `ferrosa` binary image is published today.** `ci.yml` pushes a glibc
  `ferrosa-test-node` image to GHCR for internal test reuse only.
- The `ferrosa` binary's only optional feature is `otel`. `asc-udf` lives in
  `ferrosa-cql`/`ferrosa-udf`, is **not** wired into the `ferrosa` binary, and pulls
  `rquickjs` (bundled QuickJS C source) + `wasmtime`.
- Configuration is **already fully env-var injectable**: `FERROSA_CONFIG` selects the
  config file (default `/etc/ferrosa/ferrosa.toml`; a missing file is tolerated), and
  every setting has a `FERROSA_*` override (CQL bind, internode bind/broadcast, S3
  credentials, auth, graph, web, data dir, etc.).

## Decisions (locked)

| Decision | Choice |
|----------|--------|
| Feature scope | **Full**, including `asc-udf` — gated on a musl-static build validation (below). |
| Base image | **Alpine** (pinned `alpine:3.20`) — ships a shell for `docker exec` debugging; `ca-certificates` + `tzdata` added via `apk`. |
| Artifact consistency | **All artifacts** (`.deb`, tarballs, image) built from the **same** all-features binary. |
| Registry / arch | **GHCR**, multi-arch manifest (`linux/amd64` + `linux/arm64`). |
| Tags | stable: `:latest`, `:vX.Y.Z`, `:X.Y`, `:X`; nightly: `:nightly`, `:<CalVer>`. |
| Hardening | non-root UID, read-only-rootfs compatible (only the data volume is writable). |

## Architecture

### 1. The `full` cargo feature

Add a curated `full` feature to `ferrosa/Cargo.toml`:

```toml
[features]
default = []
otel = [ "tracing-opentelemetry", "opentelemetry", "opentelemetry-otlp", "opentelemetry_sdk" ]
full = ["otel", "ferrosa-cql/asc-udf"]
```

`ferrosa-cql/asc-udf` already forwards to `ferrosa-udf/asc-udf`. The feature is **curated,
not `--all-features`**: it deliberately **excludes**

- `live-infra-tests` (test-only opt-in across several crates), and
- `ferrosa-cluster/sprint-03-engine-transfer` (in-progress, not shippable).

so "all features" means "all *shippable* features", not the test/in-progress gates.

### 2. Build-time changes (consistency)

Change the binary build invocation in the release jobs (and PR CI) from the default-feature
build to:

```
cargo build --release --target <triple> -p ferrosa -p ferrosa-ctl --features ferrosa/full
```

- `release.yml` jobs: `build-linux-x86_64`, `build-linux-aarch64` (`cross`), `build-macos-aarch64`.
- `ci.yml` musl build job: same `--features ferrosa/full`, so a full-features musl break is
  caught in **PR CI**, not first discovered at release time (fail loud, early).

`ferrosa/full` (package-qualified) is used so a single invocation can build both `ferrosa`
and `ferrosa-ctl` without requiring `ferrosa-ctl` to define its own `full` feature.

### 3. Phase 0 — musl static-link validation (gating)

`asc-udf` pulls `rquickjs` (compiles bundled QuickJS C via the `cc` crate) and `wasmtime`.
Before any workflow change is finalized, prove that

```
cargo build --release --target x86_64-unknown-linux-musl  -p ferrosa --features ferrosa/full
cargo build --release --target aarch64-unknown-linux-musl -p ferrosa --features ferrosa/full   # via cross
```

both succeed **and** produce a binary that `file` reports as *statically linked*.

- **If both succeed:** proceed with the full design as written.
- **If they fail** (e.g. wasmtime/QuickJS cannot fully static-link against musl): **stop and
  report** with options — e.g. (a) ship a glibc-based image for the full feature set, (b) a
  `full` image plus a separate `full-static` musl image without `asc-udf`, or (c) drop
  `asc-udf` from the image. Do **not** silently ship a partial/broken image.

This gate is run first during implementation; the workflow edits are conditional on it.

### 4. `Dockerfile.release` (alpine, COPY-only, multi-arch)

A new `Dockerfile.release` that **copies the prebuilt musl binary** — it never compiles
inside Docker, so the multi-arch build needs **no QEMU emulation** (only `COPY` runs
per-arch, and `COPY` is architecture-agnostic):

```dockerfile
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S ferrosa && adduser -S -G ferrosa -u 10001 ferrosa \
    && mkdir -p /etc/ferrosa /var/lib/ferrosa \
    && chown -R ferrosa:ferrosa /var/lib/ferrosa

# buildx sets TARGETARCH automatically per --platform entry (amd64 | arm64).
ARG TARGETARCH
COPY dist/${TARGETARCH}/ferrosa     /usr/local/bin/ferrosa
COPY dist/${TARGETARCH}/ferrosa-ctl /usr/local/bin/ferrosa-ctl
COPY config/ferrosa.example.toml    /etc/ferrosa/ferrosa.example.toml

ENV FERROSA_CONFIG=/etc/ferrosa/ferrosa.toml \
    FERROSA_DATA_DIR=/var/lib/ferrosa

# CQL, internode RPC, web/Prometheus, graph HTTP, Bolt
EXPOSE 9042 17000 9090 7474 7687
VOLUME /var/lib/ferrosa
USER ferrosa
ENTRYPOINT ["ferrosa"]
```

**Config injection contract (the core requirement):**

- No active config is baked in. The example config is shipped only as
  `/etc/ferrosa/ferrosa.example.toml` for reference.
- Operators configure the container by either:
  - setting `FERROSA_*` env vars (fully supported; no file needed — a missing
    `FERROSA_CONFIG` file is tolerated), and/or
  - mounting a config file at `/etc/ferrosa/ferrosa.toml` (or pointing `FERROSA_CONFIG`
    elsewhere).
- `EXPOSE` corrects the stale `7000` in the existing Dockerfiles to the real internode
  port `17000`, and adds graph HTTP `7474` and Bolt `7687`.
- Runs as non-root `ferrosa` (UID 10001); only `/var/lib/ferrosa` is written, so the
  container is `--read-only` rootfs compatible when that volume is mounted.

### 5. New `docker-image` job in `release.yml`

```
docker-image:
  needs: [build-linux-x86_64, build-linux-aarch64]
  permissions: { contents: read, packages: write }
```

Steps:

1. Download the `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` binary
   artifacts; lay them out as `dist/amd64/{ferrosa,ferrosa-ctl}` and
   `dist/arm64/{ferrosa,ferrosa-ctl}`.
2. `docker/login-action` → GHCR using `GITHUB_TOKEN` (same pattern `ci.yml` already uses).
3. `docker/setup-buildx-action`, then
   `docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile.release`
   → `ghcr.io/ferrosadb/ferrosa`, producing a multi-arch manifest.
4. **Tagging keyed off the `prerelease` flag** (computed via `docker/metadata-action` or an
   explicit tag list):
   - `prerelease=false` (stable): `:latest`, `:vX.Y.Z`, `:X.Y`, `:X`.
   - `prerelease=true` (nightly): `:nightly`, `:<CalVer tag>` (e.g. `:v2026.06.19.0017`).

Because `nightly-release.yml` and `promote-release.yml` both dispatch `release.yml`, this
single job services **both** channels — the nightly path gets images via the existing
dispatch, with no separate job needed in `nightly-release.yml`.

The job **does not** modify `[workspace.package] version` — version ownership stays with the
nightly automation, per `specs/release-process.md`.

### 6. Documentation

Update `specs/release-process.md` to document:

- the new image artifact and its full tag matrix,
- the env-var / mounted-file config-injection contract,
- the non-root UID + read-only-rootfs expectations,
- the exposed ports.

## Testing / verification

- **Phase 0 gate:** both musl targets build statically with `--features ferrosa/full`
  (`file` confirms "statically linked").
- **Image smoke:** after build, run the amd64 image with only env vars (no mounted config),
  hit `/healthz/ready`, and confirm the engine boots and binds the expected ports — reusing
  the existing smoke approach (`tests/docker-smoke.sh` pattern).
- **Manifest check:** `docker buildx imagetools inspect ghcr.io/ferrosadb/ferrosa:<tag>`
  shows both `linux/amd64` and `linux/arm64`.
- **Size check:** record the final compressed image size in the PR (expectation:
  alpine base + static binary, materially smaller than the current glibc node image).

## Risks & mitigations

| Risk | Mitigation |
|------|-----------|
| `wasmtime`/QuickJS can't fully static-link against musl | Phase 0 gate stops and reports before shipping; explicit fallbacks listed. |
| Full-features build regresses musl only at release time | `ci.yml` musl build also flips to `--features ferrosa/full` to catch it in PR CI. |
| Image larger than expected | Alpine + static binary; `apk --no-cache`; record size in PR; revisit base if needed. |
| Stale `EXPOSE 7000` misleads operators | Corrected to `17000` + graph/Bolt ports. |
| Multi-arch build slowness from emulation | COPY-only Dockerfile; binaries are prebuilt per-arch, so no QEMU compile. |

## Out of scope (deferred)

- SBOM / build provenance attestation (buildx defaults for now; can enable later).
- Publishing to Docker Hub (GHCR only for this change).
- Cosign image signing.
- A `distroless`/`scratch` ultra-minimal variant (alpine chosen for debuggability).

## Conventions respected

- Feature branch off `origin/main`; never commit to `main`; Conventional Commits.
- No hand-edit of `[workspace.package] version`.
- No specs under `docs/` (public marketing site) — this proposal lives in `specs/proposed/`.
