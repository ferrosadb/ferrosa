# Release Process & Channels

How ferrosa versions, tags, builds, and ships releases — and how the installer
consumes them.

## TL;DR

- **Versioning is automatic.** The release job derives the next SemVer from
  Conventional Commit history. **Do not hand-edit `[workspace.package] version`
  in `Cargo.toml` in a PR** — it is owned by the release automation and is
  overwritten.
- **Releases cut on merge.** Every push to `main` (a merged PR) that carries a
  releasable commit cuts the next release automatically. A **nightly cron** runs
  as a safety-net for anything the merge path missed. Both paths share one
  workflow (`nightly-release.yml`) and the same tag-only mechanics. Doc/spec-only
  merges are excluded (`paths-ignore`) so prose changes don't trigger a full
  multi-platform build.
- **Two channels, two tag schemes:**
  - **nightly** — every automatically cut release. Tagged **CalVer**
    `vYYYY.MM.DD.HHMM` (UTC) and marked a GitHub *prerelease*. The binary
    reports `<next-semver>-nightly` (e.g. `0.22.0-nightly`).
  - **stable** — a nightly a maintainer has *promoted*. Tagged **SemVer**
    `vX.Y.Z`, marked *latest*, not prerelease. Promotion **recuts** a clean
    SemVer from the nightly's commit (it is no longer a flag flip — the two are
    separate tags), so stable artifacts carry a plain `X.Y.Z`.
  - The CalVer/SemVer split makes the channel obvious in the releases list.
    `next-release-version.sh` only treats 3-segment `vX.Y.Z` tags as the stable
    base, so CalVer nightlies never pollute the version math.
- **Releases are tag-only.** The version-bump commit lives on the tag, never on
  `main`. `main`'s `Cargo.toml` version is therefore decorative between
  releases; the released artifact always carries the correct version.

## Why tag-only (the bug this fixed)

`main` has a repository **ruleset** (`pull_request` + `required_linear_history`,
no bypass actors) that rejects any direct branch push:

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
  └─ next-release-version.sh        # next SemVer from Conventional Commits since last vX.Y.Z tag
  └─ should_release? (commits since last stable tag) — else skip
  └─ bump Cargo.toml to <next>-nightly + commit (local only)
  └─ git tag vYYYY.MM.DD.HHMM  →  git push origin <calver tag>   # CalVer, tag only, never main
  └─ gh workflow run release.yml -f prerelease=true # explicit: GITHUB_TOKEN tag
                                                    # pushes don't trigger on:push

promote-release.yml  (manual: input = nightly CalVer tag)
  └─ git checkout <nightly tag>     # detach at the nightly's commit
  └─ next-release-version.sh        # clean next SemVer relative to that commit
  └─ set Cargo.toml to X.Y.Z + commit (local only)
  └─ git tag vX.Y.Z  →  git push origin vX.Y.Z       # SemVer, tag only
  └─ gh workflow run release.yml -f prerelease=false # rebuild as stable (--latest)

release.yml  (per built tag — CalVer or SemVer)
  └─ build musl x86_64/aarch64 + macOS aarch64 tarballs, .deb, Homebrew bottle
  └─ SHA256SUMS
  └─ gh release create … (--prerelease for nightly, --latest for stable)
```

Conventional Commit → bump mapping (see `.github/scripts/next-release-version.sh`):

| Commit                                   | Bump  |
|------------------------------------------|-------|
| `feat!:` / `BREAKING CHANGE:` in body    | major |
| `feat:` / `feat(scope):`                 | minor |
| anything else                            | patch |

A PR with non-Conventional commit subjects silently degrades to a **patch**
bump. Keep commit subjects conventional so the auto-bump is correct.

## Promoting nightly → stable

When a nightly build is validated, promote it by its **CalVer** tag:

- **UI:** Actions → *Promote Release to Stable* → enter the nightly tag
  (e.g. `v2026.06.16.0817`).

Promotion checks out that nightly's commit, computes the next stable SemVer from
Conventional Commit history, tags `vX.Y.Z`, and dispatches `release.yml` with
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
3. `computed tag … already exists` → a tag for the next version already exists
   (e.g. a partial prior run). Delete the stray tag or bump past it.
4. Version came out as `patch` when a `feat` landed → a PR used non-Conventional
   commit subjects. Fix history hygiene; promotion/bump is commit-driven.
