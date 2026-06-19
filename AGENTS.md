# AGENTS.md

Guidance for AI agents working in this repository. This complements
[`CLAUDE.md`](CLAUDE.md) — read that for the full project overview, workspace
layout, design decisions, and test policy. This file highlights the rules an
agent most often gets wrong.

## Per-crate docs are part of the definition of done

Every crate in the workspace carries its own documentation:

- **`<crate>/README.md`** — what's implemented, how the crate works, and its
  dependencies (the crates it calls) and dependents (the crates that call it).
- **`<crate>/specs/`** — `overview.md` (architecture), `fmea.md` (failure modes /
  known issues), `roadmap.md` (Now / Next / Later), and `data-flow.md` for the
  heavier crates.

**Rule:** changing a crate's behavior, public API, dependency set, or
known-issue/roadmap status is **not finished until that crate's `README.md` +
`specs/` are updated to match.** A crate's docs must reflect its *current*
implementation and dependency set. Treat stale crate docs the same as a failing
test — do not open a PR with code changes whose crate docs are out of date.

The crate-centric map (all crates + the runtime dependency graph) lives at
[`specs/crates.md`](specs/crates.md). Cross-cutting topic specs live under
[`specs/reference/`](specs/reference/).

## Other rules agents must follow

- **TDD** — every feature/fix follows red-green-refactor (`/tdd`). No code lands
  without a test exercising it.
- **Hygiene before pushing** — `cargo fmt --check`, `cargo clippy --all-targets`
  (zero warnings), `cargo test`, and `cargo doc` with `-D warnings`, across the
  full workspace, must pass locally. Do not push and hope. (CI also requires
  these — the `main` merge queue gates on them.)
- **Never commit to `main`** — feature branch + PR. Tag-only releases; the
  version is owned by nightly automation (never hand-edit `[workspace.package]
  version`).
- **Conventional Commits** — the release bump level is computed from commit
  subjects (`feat:`/`fix:`/`feat!:`).
- **Fail loud, never fake** — prefer a clear error over a silent fallback that
  returns wrong/empty results. (This is also what the FMEAs in each crate's
  `specs/` exist to surface.)

## Mermaid in docs

GitHub's Mermaid renderer treats `<Type>` in diagram text as an HTML tag and
breaks the diagram. In any Mermaid block, escape generic/placeholder angle
brackets as `&lt;`/`&gt;` (e.g. `Vec&lt;Row&gt;`). `<br/>` and `<<choice>>` are
valid and fine.
