# Release Process & Channels

How ferrosa versions, tags, builds, and ships releases — and how the installer
consumes them.

## TL;DR

- **`main`'s version is bumped after every merge** by the `ferrosa-release-bot`
  App (`.github/workflows/semver.yml` runs `scripts/semver-bump --release`,
  commits `chore(release): vX.Y.Z` to `main` as a ruleset bypass actor and tags
  it `semver-v<version>`). PRs carry no bump; their Conventional title sets it.
  See "Versioning" in `CONTRIBUTING.md`. Never hand-edit it.
- **Main leads.** Releases never compute a version of their own: nightly and
  stable both read `main`'s `[workspace.package] version`
  (`.github/scripts/release-version-from-main.sh`). The script refuses loudly
  when `main` is below the latest stable `vX.Y.Z` tag, when a change reached
  `main` without a bump, or when promotion would re-release an existing `vX.Y.Z`.
- **Releases cut on merge.** Every merged PR cuts the next release
  automatically, from the bot's `chore(release)` commit that follows it (the
  merge push itself still carries the previous version, so it is skipped). A **nightly cron** runs
  as a safety-net for anything the merge path missed. Both paths share one
  workflow (`nightly-release.yml`) and the same tag-only mechanics. Doc/spec-only
  merges are excluded (`paths-ignore`) so prose changes don't trigger a full
  multi-platform build.
- **Two channels, two tag schemes:**
  - **nightly** — every automatically cut release. Tagged **CalVer**
    `vYYYY.MM.DD.HHMM` (UTC) and marked a GitHub *prerelease*. The binary
    reports `<main's version>-nightly` (e.g. `0.22.1-nightly`).
  - **stable** — a nightly a maintainer has *promoted*. Tagged **SemVer**
    `vX.Y.Z` where `X.Y.Z` is `main`'s version at the nightly's commit, marked
    *latest*, not prerelease. Promotion **recuts** from the nightly's commit (it
    is no longer a flag flip — the two are separate tags), so stable artifacts
    carry a plain `X.Y.Z`.
  - The CalVer/SemVer split makes the channel obvious in the releases list.
    Only 3-segment `vX.Y.Z` tags count as the stable base; CalVer nightlies and
    `semver-v*` merge tags never do.
- **Releases are tag-only.** The `-nightly` / clean-version commit lives on the
  tag, never directly on `main` (the `main` ruleset forbids direct pushes — see
  below). The released artifact always carries the correct version.
- **Promotion never writes `main`.** `main` already carries the version, so there
  is no "sync main version" PR any more (it used to set `main` *below* its
  bumps).
- **`ferrosa-docs` `docs/LATEST` + the marketing-site version are release steps.** `ferrosa-docs` `docs/LATEST`
  (the plain-text tag the install scripts fetch from `ferrosadb.com/LATEST`) and the
  *descriptive* version strings on the site (`docs/index.html`, `docs/database/index.html`,
  `docs/index.md`) are updated when a release is promoted. **`ferrosa-docs` `docs/LATEST` must only be
  bumped once the release tarball exists** — pointing it at a tag with no built artifact
  breaks `install.sh`. (Not yet automated; tracked separately.)
- **Current promoted stable releases:** Ferrosa Database `v0.16.0`; Ferrosa
  Memory `v0.22.0`. Use GitHub's latest-release endpoint as the source of truth
  for installers and docs that need the current stable tag.

## Why tag-only (the bug this fixed)

`main` has a repository **ruleset** (`pull_request` + merge queue; its only
bypass actor is the `ferrosa-release-bot` App, for the version bump) that
rejects any other direct branch push:

```
remote: error: GH013: Repository rule violations found for refs/heads/main.
remote: - Changes must be made through a pull request.
```

The old `nightly-release.yml` did `git push origin HEAD:main` and failed every
night (computing the version, committing, and tagging all succeeded — only the
branch push was rejected). The ruleset targets **branches only**, so pushing the
**tag** is allowed. The fix: keep the bump commit local, create the tag on it,
and push only the tag.

## Pipeline

```
nightly-release.yml  (on: push→main [merge], cron 08:17 UTC, or manual)
  └─ release-version-from-main.sh nightly  # main's version; refuses if <= last vX.Y.Z with commits since
  └─ should_release? (commits since last stable tag) — else skip
  └─ set Cargo.toml to <main's version>-nightly + commit (local only)
  └─ git tag vYYYY.MM.DD.HHMM  →  git push origin <calver tag>   # CalVer, tag only, never main
  └─ gh workflow run release.yml -f prerelease=true # explicit: GITHUB_TOKEN tag
                                                    # pushes don't trigger on:push

promote-release.yml  (manual: input = nightly CalVer tag)
  └─ git checkout <nightly tag>     # detach at the nightly's commit
  └─ release-version-from-main.sh promote  # main's X.Y.Z there; refuses if below last vX.Y.Z or already tagged
  └─ set Cargo.toml to X.Y.Z (drop -nightly) + commit (local only)
  └─ git tag vX.Y.Z  →  git push origin vX.Y.Z       # SemVer, tag only
  └─ gh workflow run release.yml -f prerelease=false # rebuild as stable (--latest)

release.yml  (per built tag — CalVer or SemVer)
  └─ build musl x86_64/aarch64 + macOS aarch64 tarballs, .deb, Homebrew bottle
  └─ SHA256SUMS
  └─ gh release create … (--prerelease for nightly, --latest for stable)
```

The bump itself happens on `main` after each merge, not here:
`semver.yml`'s `bump and tag main` job maps each merged PR's Conventional title
to major/minor/patch (see "Versioning" in `CONTRIBUTING.md`), and the PR check
refuses a non-Conventional title.

## Promoting nightly → stable

When a nightly build is validated, promote it by its **CalVer** tag:

- **UI:** Actions → *Promote Release to Stable* → enter the nightly tag
  (e.g. `v2026.06.16.0817`).

Promotion checks out that nightly's commit, reads `main`'s version there,
tags `vX.Y.Z`, and dispatches `release.yml` with
`prerelease=false` so it rebuilds and publishes as `--latest`. The **stable**
channel (`/releases/latest`) then resolves to the new SemVer release.

> Promotion is a **recut**, not a flag flip: nightly (CalVer) and stable (SemVer)
> are separate tags/releases. The stable binaries are rebuilt so they report a
> clean `X.Y.Z` instead of `X.Y.Z-nightly`.

## Installer (`docs/install.sh`, served at ferrosadb.com/install.sh)

Idempotent; re-run to upgrade.

| Invocation                                   | Installs                              |
|----------------------------------------------|---------------------------------------|
| `… \| bash`                                   | stable (latest promoted release)      |
| `… \| bash -s -- --channel nightly`           | newest published release (incl. pre)  |
| `… \| bash -s -- --version v0.13.0`           | that exact tag                        |
| `… \| bash -s -- --force`                     | reinstall even if up to date          |

It records the installed tag in `~/.ferrosa/.version`; a re-run that resolves to
the same tag is a no-op (`already up to date`). On a differing version it prints
`upgrading X -> Y`, replaces the binaries, restarts an already-registered
service, and does **not** re-prompt for service/password.

Channel resolution:
- **stable** → `GET /repos/ferrosadb/ferrosa/releases/latest` (non-prerelease only)
- **nightly** → `GET /repos/ferrosadb/ferrosa/releases?per_page=1` (newest, incl. prerelease)

## Container image

Every release publishes multi-arch (`linux/amd64`, `linux/arm64`) images to
`ghcr.io/ferrosadb/ferrosa` in two libc variants. Where we choose the libc, we
choose glibc: it is 15-20% faster than musl, so it is the default image.

- **Default (glibc), `docker-image-glibc`:** Debian trixie (glibc 2.41), built
  by COPYing the `build-linux-gnu` binaries, whose glibc requirement is asserted
  against that base at build time.
- **musl, `docker-image`:** `alpine:3.20` + `ca-certificates` + `tzdata`,
  COPYing the static musl binary.
- Neither compiles in Docker or uses QEMU.
- **Features:** both are built with `--features ferrosa/full` (otel + flight +
  asc-udf), identical to the `.deb`/tarball binaries.
- **Tags:**
  - Stable (`prerelease=false`): glibc `:latest`, `:vX.Y.Z`, `:X.Y`, `:X` (also
    `-glibc` aliases); musl the same set suffixed `-musl`.
  - Nightly (`prerelease=true`): glibc `:nightly`, `:<CalVer>` (e.g.
    `:v2026.06.19.0017`, also `-glibc`); musl `:nightly-musl`, `:<CalVer>-musl`.
  - Profiling: `-glibc-profiling` (unstripped, full DWARF).
- **Tarballs:** musl tarballs remain what `install/install.sh` installs on
  Linux, since a host may lack a usable glibc. The glibc tarballs
  (`*-unknown-linux-gnu.tar.gz`, glibc 2.39+) ship alongside them.
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

```bash
docker run -d --name ferrosa \
  -e FERROSA_MODE=production \
  -e FERROSA_CQL_BIND=0.0.0.0:9042 \
  -e FERROSA_INTERNODE_BROADCAST=10.0.0.5:17000 \
  -e FERROSA_S3_ENDPOINT=https://s3.example.com \
  -e FERROSA_S3_BUCKET=ferrosa \
  -v ferrosa-data:/var/lib/ferrosa \
  -p 9042:9042 \
  ghcr.io/ferrosadb/ferrosa:latest
```

## Runbook — the nightly release failed

1. `gh run list --workflow=nightly-release.yml` → open the failed run.
2. `GH013 … refs/heads/main` → the ruleset rejected a branch push. The workflow
   must push **tags only**; confirm no `git push origin HEAD:…main` remains.
3. `main's version … is BELOW the latest stable tag` → `main`'s Cargo version
   was lowered or never caught up. Fix it on `main` with a PR; never release
   backwards.
4. `equals the latest stable tag … not bumped` → a change reached `main` and the
   release bot did not bump it. Open the latest `Semver` / `bump and tag main`
   run (`gh run list --workflow=semver.yml --branch main`); a missing App
   variable/secret is reported there as an `::error::`.
5. Promotion: `vX.Y.Z already exists` → that version is already stable; promote
   a nightly cut after a later bump on `main`.
