# Contributing to Ferrosa

Thanks for your interest in Ferrosa. We welcome contributions of all sizes — bug
reports, documentation fixes, performance improvements, and new features.

## Code of Conduct

This project follows the [Contributor Covenant 2.1](CODE_OF_CONDUCT.md).
Participation is conditioned on adherence to its terms.

## Getting Started

### Prerequisites

- Rust stable (current: see `rust-toolchain.toml` if present, otherwise latest stable)
- A POSIX-ish environment (Linux, macOS). Windows is not currently supported for the
  database; client tools may work via WSL.
- Optional: Podman or Docker for the cluster integration test suite.

### Building

```bash
git clone https://github.com/ferrosadb/ferrosa.git
cd ferrosa
cargo build --workspace
```

### Running the Test Suite

```bash
cargo test --workspace        # All crates
cargo test -p ferrosa-storage # A single crate
```

Some integration tests require infrastructure and are gated behind environment
variables:

| Variable                       | Enables                              |
| ------------------------------ | ------------------------------------ |
| `FERROSA_TEST_CONTAINERS=1`    | Docker/Podman compose tests          |
| `FERROSA_TEST_FIRECRACKER=1`   | Firecracker VM tests                 |
| `FERROSA_TEST_CLUSTER_NODES=…` | Tests against a pre-provisioned cluster |

If you don't set these, the gated tests will fail loudly with setup instructions —
that's by design. The codebase does not silently skip tests.

### Hygiene Checklist (Before Every Commit)

1. `cargo fmt` — format
2. `cargo clippy --all-targets -- -D warnings` — lint, zero warnings
3. `cargo test -p <touched-crate>` — affected tests pass
4. `cargo build --all-targets` — full workspace compiles clean

CI runs all of the above on every pull request. If you can run them locally first,
you'll save a CI round-trip.

## Development Workflow

### Branching

- Fork the repository and create a topic branch off `main`
- Branch naming: `feat/short-description`, `fix/issue-summary`, `docs/what-changed`
- Never commit directly to `main`

### Commit Messages

Follow conventional-commit style:

- `feat(crate): summary` — new feature
- `fix(crate): summary` — bug fix
- `refactor(crate): summary` — restructure without behavior change
- `test(crate): summary` — test-only change
- `docs: summary` — documentation
- `chore: summary` — tooling, deps, build

Body should explain *why*, not just *what*. The diff already shows the *what*.

### Versioning

Every change to `main` bumps `[workspace.package] version` in `Cargo.toml`, by
the amount it calls for. **You do not bump it in your PR.** After a merge, the
`ferrosa-release-bot` GitHub App bumps `main` itself
(`.github/workflows/semver.yml`, job `bump and tag main`), so the bump happens
no matter who opened the PR. What you provide is a **Conventional Commit PR
title**: the merge queue records it as the merge commit's message, and it sets
the bump:

| PR title                                       | Bump                       |
| ---------------------------------------------- | -------------------------- |
| `feat!:` / `fix(x)!:` (or a `BREAKING CHANGE:` footer) | major (minor while `0.x`) |
| `feat:` / `feat(scope):`                       | minor                      |
| anything else (`fix`, `perf`, `docs`, `ci`, …) | patch                      |

Checks:

- **PR title** — the `semver bump present` job runs
  `scripts/semver-bump --check-title` on every PR (re-run when you retitle).
  In the merge queue it runs only the tool's unit tests.
- **commit-msg hook** — refuses a commit whose header is not a Conventional
  Commit (installed by `pre-commit install`). It keeps titles easy; it does not
  set the version.

Never edit the version by hand, and never add a `chore(release): …` commit to a
PR.

**Reading the release run.** Each push to `main` runs `bump and tag main`.
`scripts/semver-bump --release` reads `main`'s first-parent history since the
newest `semver-v<version>` tag (a merge contributes its PR title), and prints
`version=`, `released=` and `bootstrap=`:

- `released=true` — it committed `chore(release): vX.Y.Z`; the job tags
  `semver-vX.Y.Z` and pushes the commit and tag atomically.
- `bootstrap=true` — no `semver-v*` tag existed yet; it tags the current version
  only.
- both `false` — `main` already declares a high enough version (for example the
  run triggered by the bot's own release commit); nothing is pushed.
- `::warning::main moved during the release` — another merge landed while it
  ran; the run for that newer push releases both. Not an error.
- A non-Conventional title is counted as a patch with a `::warning::`; the
  release never stalls on wording.

`semver-v*` tags never trigger `release.yml`, which only builds `v*` tags.

#### One-time setup

The release job fails with an `::error::` until all of these exist:

1. A GitHub App named `ferrosa-release-bot`, installed on this repository with
   **Contents: read and write** (and Metadata: read).
2. Org variable `FERROSA_RELEASE_BOT_APP_ID` — the App's ID.
3. Org secret `FERROSA_RELEASE_BOT_PRIVATE_KEY` — a private key generated for
   the App.
4. The App as a **bypass actor** on `main`'s ruleset, so its release commit can
   reach `main` without a PR or the merge queue.

See [GitHub: authenticating with a GitHub App in Actions](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/making-authenticated-api-requests-with-a-github-app-in-a-github-actions-workflow)
and [ruleset bypass](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository#granting-bypass-permissions-for-your-ruleset).

### Test-Driven Development

For new features and bug fixes, write the failing test first, then make it pass, then
refactor. This is enforced by review for non-trivial changes.

For bug fixes specifically: include a regression test that fails on the unfixed code
and passes on the fix.

### Pull Requests

- Keep PRs focused. One bug fix or one feature per PR.
- Include test coverage. PRs without tests for new behavior will be requested to add
  them before review.
- Link the issue number if one exists: "Closes #123".
- Mark drafts as drafts. Mark ready-for-review when CI is green.

PRs are squash-merged by default. Your commit-by-commit history during development
doesn't need to be perfect — the squashed commit message will be the canonical record.

## Areas Where Help Is Welcome

- **Driver compatibility** — Try a Cassandra driver in your language and report issues
- **Documentation** — README clarifications, examples, architecture notes
- **Performance** — Profiling reports, benchmark improvements
- **Test coverage** — Property tests, fuzz inputs, integration scenarios
- **Tooling** — Better error messages, improved `ferrosa-ctl` ergonomics

## What We Don't Accept

- Code generated by AI tools without human review and understanding. We're happy for
  contributors to use AI as an assistant, but the contributor must understand and
  stand behind every line they submit.
- Drive-by formatting changes that touch unrelated files.
- Dependency bumps without a stated reason (security CVE, required feature, etc.).
- Changes that disable or skip existing tests without replacing them.

## Reporting Bugs

Open a GitHub issue with:

- Ferrosa version (commit hash or release tag)
- Reproduction steps, ideally a minimal self-contained example
- Expected behavior, actual behavior
- Relevant logs (sanitize secrets first)
- Environment: OS, Rust version, S3 backend (real S3 / MinIO / etc.)

Security issues — please do **not** open a public issue. See [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache
License, Version 2.0. See [LICENSE](LICENSE).

You retain copyright on your contribution. The Apache 2.0 license includes an explicit
patent grant covering contributions.
