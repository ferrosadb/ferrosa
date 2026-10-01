# xtask — failure modes

The audit is a **safety control**: its failure mode of interest is *letting a
materializing read path through*, not crashing. Weakening it is worse than never
having had it. The warn-ahead addition is therefore deliberately one-directional
— it adds a signal and changes no verdict.

| # | Failure mode | Effect | S | O | D | RPN | Mitigation |
|---|---|---|--:|--:|--:|--:|---|
| X-1 | Warn-ahead output changes the exit code | A build goes green on a tree the gate rejects, or red on a warning | 9 | 2 | 2 | 36 | Warnings are computed after the verdict and never reach it. Pinned by `invariant_warnings_do_not_change_the_exit_code_under_enforce` (CLI level, over `--warn-within` 0/21/3650) and `invariant_warnings_never_suppress_or_excuse_a_finding` (unit). |
| X-2 | Warn-ahead is implemented by *extending* the expired-entry rule | An expiring entry becomes a finding; the gate red-flags a tree that is still within its exemption | 9 | 2 | 2 | 36 | The two halves are separate functions with disjoint date ranges: expired is `< today`, warn is `0..=within_days`, and an expired entry yields no warning. Pinned by `already_expired_entry_is_not_also_a_warning` and `invariant_entry_dated_yesterday_still_fails_and_is_not_a_warning`. |
| X-3 | A malformed `expires` makes an entry immortal and silent | An exemption never lapses; the gate's date logic is bypassed | 8 | 2 | 6 | 96 | `unparseable-allow-expiry` warning plus `invariant_every_shipped_allow_expiry_is_a_real_iso_date`, which fails on any non-ISO date in the real allowlist. |
| X-4 | `--warn-within` is silently ignored or accepts a bad value | A typo (`--warn-within soon`, a stale flag name) reads as "no warnings" instead of an error | 4 | 3 | 4 | 48 | The flag fails loud on a missing or non-numeric value; the CLI test asserts the error names the flag. |
| X-5 | The daily workflow is deleted or its schedule is dropped | Date-triggered failures go back to ambushing an unrelated PR | 7 | 3 | 4 | 84 | `tests/ci/test_oom_audit_daily_workflow.py` pins the cron, the `workflow_dispatch`, the enforced command, the SHA pins, `contents: read`, and that no secret is used; the test itself is pinned into `ci.yml`'s `fmt` job. Verified by six negative controls (remove schedule, weaken command, add a test run, unpin an action, add a secret, soften with `continue-on-error` — each makes it fail). |
| X-6 | The daily job drifts from the per-PR command | The nightly reports green on a tree the PR gate rejects (or vice versa) | 8 | 2 | 3 | 48 | The pin test asserts the daily `run:` string is byte-identical to `ci.yml`'s Clippy-job audit step, and that the Clippy job keeps its own step (the nightly is not a reason to drop the per-PR gate). |
| X-7 | The dedicated workflow grows into a second full nightly | Runner cost and a nightly nobody can afford to keep | 2 | 3 | 3 | 18 | The pin test asserts exactly one cargo invocation, no `cargo test`/`build`/`clippy`/`doc`, and no Docker on code lines. |
| X-8 | A red nightly is not noticed | The signal exists but nobody reads it (the 2026-09-30 case) | 7 | 5 | 5 | 175 | `nightly-fuzz.yml`'s `fuzz` job files a tracking issue on failure, after the unchanged red verdict. Pinned by `test_the_nightly_fuzz_workflow_surfaces_a_red_run_as_an_issue`. **Honest limit:** the issue is only visible if the repo is watched; this is a same-day signal, not a pager. |
| X-9 | A new rule false-positives and blocks unrelated PRs | The audit is disabled out of frustration | 5 | 3 | 4 | 60 | An allow entry with reason/owner/expiry is the escape hatch; `--warn-within` keeps the renewal visible. Warn-mode (`--today` without `--enforce`) is the default, so a diagnosis never needs a red build. |
| X-10 | The allowlist grows an entry nobody owns | The gate is quietly waived | 6 | 3 | 5 | 90 | `owner` is required by the schema and named in every warning; `invariant_expired_entry_detection_is_unchanged` proves every entry still expires. |

## Notes

* The `expires` compare in `expired_allow_findings` is **lexicographic**
  (`e.expires.as_str() < today`) and has been left as-is: it is correct for
  zero-padded ISO dates, which X-3 now guarantees, and changing it would alter
  the gate's verdict path for no current gain.
  `invariant_lexicographic_expiry_compare_agrees_with_calendar_compare` proves
  the string compare and calendar arithmetic agree on every shipped entry across
  a cross-product of probe dates, so the invariant is *known* rather than
  assumed — if that test ever goes red, the hardening has become necessary.
* `parse_iso_date`/`days_since_epoch` are exact-day Gregorian arithmetic
  (including the 4/100/400 leap rule), not a lexicographic or month-approximation
  shortcut, so `days_remaining` is right across month, year and leap-day
  boundaries.
