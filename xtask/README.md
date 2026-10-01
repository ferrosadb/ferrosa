# xtask

Repo automation for the ferrosa workspace. One binary, one subcommand.

## `p0-oom-audit`

The static AST audit (Layer 1) of the P0 OOM guard — see
[`specs/p0-oom-guard/blueprint.md`](../specs/p0-oom-guard/blueprint.md). It parses
every audited crate's `src/` with `syn` and reports read-path materialization
shapes (`Vec<Partition>`/`Vec<Row>` returns, `.collect::<Vec<..>>()` over a scan,
`read_range(None, None, ..)`, row-data clones, hardcoded result caps, …). It is
**AST-based on purpose**: a source-grep test is what certified the original OOM.
See the module header of [`src/oom_audit.rs`](src/oom_audit.rs) and decision D2
in the blueprint.

```bash
# Advisory run against this checkout (prints findings, always exits 0).
cargo run -p xtask --all-features -- p0-oom-audit

# The enforced gate, as CI runs it. An expired allow entry fails this.
cargo run -p xtask --all-features -- p0-oom-audit --enforce --today "$(date -u +%F)"
```

| Flag | Meaning |
|---|---|
| `--enforce` | Exit 1 when any non-whitelisted finding remains. Without it the audit is advisory and always exits 0. |
| `--today YYYY-MM-DD` | The date the expiry check is evaluated against. The CLI passes the run date; lib code never reads the clock, so results are reproducible. |
| `--warn-within DAYS` | Warn-ahead window, default `21`. See below. |
| `--root PATH` | Audit another checkout (e.g. a feature-branch worktree). |

### What the audit reports

* **Findings** (`rule::*`) — decide the verdict. `--enforce` exits 1 on any of them.
* **Warnings** (`warning::*`) — advisory only, never affect the exit code:
  * `expiring-allow-entry` — an allowlist entry whose `expires` is inside the
    warn-ahead window (or is today). The message names the owner, rule, path,
    expiry and days remaining. This exists because an allow entry is silently
    fine until the instant it blocks every PR: on 2026-10-01 nine entries dated
    2026-09-30 expired at once and turned `main` red on the next push. A warning
    is the advance notice that keeps that from being a surprise.
  * `unparseable-allow-expiry` — an `expires` that is not a valid `YYYY-MM-DD`
    date. Such an entry can neither warn ahead nor (reliably) expire, so it is
    reported rather than silently skipped.

An already-expired entry is a **finding**, never also a warning. The two halves
of the contract are separate and are covered by
[`tests/oom_audit_cli.rs`](tests/oom_audit_cli.rs) (the process-boundary
invariants) and the unit tests in `src/oom_audit.rs` (including a check that the
lexicographic expiry compare agrees with real calendar arithmetic on every
shipped entry).

### Where it runs

* `ci.yml` — the `clippy` job's audit step, per PR / push to main / merge queue.
* `.github/workflows/oom-audit-daily.yml` — **daily**, so a date-triggered
  failure is seen on `main` first instead of ambushing whichever PR runs next.
  Pinned by `tests/ci/test_oom_audit_daily_workflow.py`.
* `.pre-commit-config.yaml` — the `p0-oom-audit-prepush` hook, same command.

## Layout

```
xtask/
  Cargo.toml      # publish = false; the only consumer is CI + the hook
  src/main.rs     # CLI: arg parsing, the verdict, output
  src/oom_audit.rs# the AST rules, the allowlist, the expiry/warn-ahead logic
  tests/          # CLI-level acceptance for the process boundary
```

## Dependencies and dependents

* **Depends on**: `syn` (parsing + `visit`), `proc-macro2` (`span-locations`, so
  `Span::start().line` resolves outside a proc-macro build), `quote`, `walkdir`,
  `toml`, `serde`, `anyhow`. No workspace crate — the audit reads source files,
  it does not link the product.
* **Depended on by**: nothing at compile time. `ci.yml`, the daily workflow and
  the pre-push hook invoke the binary; `specs/p0-oom-guard/` documents it.
