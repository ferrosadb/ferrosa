# xtask — roadmap

## Now

* `p0-oom-audit` static AST audit — enforced in `ci.yml`'s Clippy job, in the
  `p0-oom-audit-prepush` pre-push hook, and daily against `main`
  (`.github/workflows/oom-audit-daily.yml`).
* Warn-ahead for allowlist expiries (default 21 days, `--warn-within DAYS`), so
  an approaching expiry is visible before it fails the gate; advisory only.
* `expiring-allow-entry` / `unparseable-allow-expiry` warnings — the latter
  closes the "a malformed date is immortal" hole.
* Exact Gregorian day arithmetic for `days_remaining`; the historical
  lexicographic expiry compare is pinned by a test to agree with it on every
  shipped entry.
* Workflow-contract tests: `tests/ci/test_oom_audit_daily_workflow.py` pins the
  daily job's schedule, command, pins, permissions and credential surface.

## Next

* **Renew or retire the 2026-10-01 batch.** 58 allow entries carry
  `expires = "2026-10-01"`; the warn-ahead window reports them from mid-September
  onward. Each needs a real decision (land the fix, or renew with a reason),
  tracked under the materialization epic `t_110dd8a5` and the t_a49d88c3 triage.
  The audit will fail the gate the day they lapse — this is deliberate.
* **Harden `expired_allow_findings` to the calendar compare** when the pinned
  equivalence test is no longer enough (e.g. if a non-ISO or non-UTC date ever
  enters the pipeline). Doing it now would change the verdict path of a safety
  control for no behavioural gain; the FMEA records the trade-off.
* **Triage the remaining not-individually-triaged entries** — many are marked
  "TRIAGED" in `oom-audit-allow.toml`; the rest is `t_a49d88c3`.

## Later

* **Merge/replace the retired source-grep guard.** `scripts/check-unbounded-reads.py`
  still runs beside the AST audit; folding its remaining checks into `xtask`
  leaves one gate instead of two that can disagree.
* **Per-rule documentation.** Each rule in `src/oom_audit.rs` carries its
  rationale in a doc comment; lifting them into a reference next to
  `specs/p0-oom-guard/blueprint.md` would make the rule set reviewable without
  reading the visitor.
* **A `--format json` output** so the daily job and a future dashboard can
  consume findings/warnings structurally instead of grepping text.
