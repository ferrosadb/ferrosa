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
- **Two channels:**
  - **nightly** — every automatically cut `vX.Y.Z` release. Marked as a GitHub
    *prerelease*.
  - **stable** — a nightly release a maintainer has *promoted*. Marked
    *latest*, not prerelease.
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
  └─ next-release-version.sh        # SemVer from Conventional Commits since last vX.Y.Z tag
  └─ should_release? (commits since last tag) — else skip
  └─ bump Cargo.toml + commit (local only)
  └─ git tag vX.Y.Z  →  git push origin vX.Y.Z      # tag only, never main
  └─ gh workflow run release.yml -f prerelease=true # explicit: GITHUB_TOKEN tag
                                                    # pushes don't trigger on:push
release.yml  (per built tag)
  └─ build musl x86_64/aarch64 + macOS aarch64 tarballs, .deb, Homebrew bottle
  └─ SHA256SUMS
  └─ gh release create … --prerelease   # nightly channel by default
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

When a nightly build is validated, promote its tag:

- **UI:** Actions → *Promote Release to Stable* → enter the tag (e.g. `v0.14.0`).
- **CLI:** `gh release edit v0.14.0 --repo ferrosadb/ferrosa --prerelease=false --latest`

This marks it `latest` and clears the prerelease flag, so the **stable** channel
now resolves to it.

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

## Runbook — the nightly release failed

1. `gh run list --workflow=nightly-release.yml` → open the failed run.
2. `GH013 … refs/heads/main` → the ruleset rejected a branch push. The workflow
   must push **tags only**; confirm no `git push origin HEAD:…main` remains.
3. `computed tag … already exists` → a tag for the next version already exists
   (e.g. a partial prior run). Delete the stray tag or bump past it.
4. Version came out as `patch` when a `feat` landed → a PR used non-Conventional
   commit subjects. Fix history hygiene; promotion/bump is commit-driven.
