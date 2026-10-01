"""Contract for the daily scheduled P0 OOM-guard audit.

An allow entry is silently fine until the instant it blocks every PR. Nine
entries dated 2026-09-30 expired together and turned `main` red on the next
push, ambushing an unrelated pull request: nothing on `main` noticed on the day,
because `ci.yml` triggers the audit only on push-to-main / pull_request /
merge_group. `.github/workflows/oom-audit-daily.yml` exists so `main` is the
FIRST to notice a date-triggered failure.

These tests are the guard on the guard. The audit is a safety control, so the
things that matter — that it still runs daily, still enforces, and still runs
the SAME command as the per-PR gate — are pinned here rather than trusted to
review:

* the schedule must exist (a date-triggered failure returns to ambushing a PR);
* the enforced audit command must be byte-identical to ci.yml's Clippy-job step
  (a weaker nightly would report green on a tree the PR gate rejects);
* the job must run the audit ONLY — it must not grow into a second nightly that
  re-runs the expensive suite (that is why this is not a `schedule:` on ci.yml);
* the third-party actions must stay SHA-pinned;
* no credential may be introduced: the job needs no token.
"""

import re
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
DAILY = WORKFLOWS / "oom-audit-daily.yml"
CI = WORKFLOWS / "ci.yml"

# The one audit invocation. `--today "$(date -u +%F)"` is the run date: it is
# what makes an expired entry fail THE DAY, on whichever ref runs it.
AUDIT_COMMAND = (
    'cargo run --quiet -p xtask --all-features -- p0-oom-audit --enforce '
    '--today "$(date -u +%F)"'
)


def text(path):
    return path.read_text(encoding="utf-8")


def workflow(path):
    loaded = yaml.safe_load(text(path))
    assert isinstance(loaded, dict), f"{path.name} must be a YAML mapping"
    return loaded


def jobs(doc):
    assert "jobs" in doc, "workflow has no jobs"
    return doc["jobs"]


def on_triggers(doc):
    """The `on:` mapping.

    YAML 1.1 parses the bare key `on` as the boolean True, and `yaml.safe_load`
    follows that, so the mapping lands under `True`.
    """
    for key in ("on", True):
        if key in doc:
            triggers = doc[key]
            assert isinstance(
                triggers, dict
            ), "the workflow must declare `on:` as a mapping"
            return triggers
    raise AssertionError("workflow has no `on:` trigger block")


def run_scripts(job):
    """Every `run:` string in the job, in order."""
    scripts = []
    for step in job.get("steps", []):
        if "run" in step:
            scripts.append(str(step["run"]))
    return scripts


def cargo_invocations(job):
    """Every line that actually INVOKES cargo.

    Matched at line start, so prose that merely mentions a command (an echoed
    hint in a summary step) is not mistaken for a second compile.
    """
    found = []
    for script in run_scripts(job):
        for line in script.splitlines():
            if re.match(r"^\s*cargo\s+(run|test|build|clippy|doc|check)\b", line):
                found.append(line.strip())
    return found


def uses_lines(path):
    return re.findall(r"^\s*-?\s*uses:\s*(\S+)", text(path), flags=re.M)


def code_lines(path):
    """Non-comment lines, so prose ABOUT a thing is not mistaken for the thing."""
    return [l for l in text(path).splitlines() if not l.lstrip().startswith("#")]


class OomAuditDailyWorkflowTests(unittest.TestCase):
    def test_workflow_is_valid_yaml(self):
        doc = workflow(DAILY)
        self.assertEqual(doc.get("name"), "OOM Audit (daily)")

    def test_main_is_the_first_to_notice_a_date_triggered_failure(self):
        # `schedule:` is the entire point: without it the audit on main is only
        # ever triggered by a push, so a date-triggered failure is first seen by
        # whichever unrelated PR happens to push a branch.
        on = on_triggers(workflow(DAILY))
        schedule = on.get("schedule")
        self.assertIsInstance(schedule, list, "the workflow must have a daily schedule")
        crons = [entry["cron"] for entry in schedule]
        self.assertEqual(
            crons,
            ["0 8 * * *"],
            "the audit must run daily; changing the slot changes when a "
            "date-triggered failure is first seen",
        )
        # A human must be able to run it on demand too (e.g. to re-check after
        # renewing an entry) without waiting for the next slot.
        self.assertIn("workflow_dispatch", on, "a human must be able to trigger it")

    def test_runs_exactly_the_same_audit_as_the_ci_clippy_job(self):
        daily = jobs(workflow(DAILY))
        self.assertEqual(list(daily), ["audit"], f"unexpected jobs: {list(daily)}")
        daily_runs = run_scripts(daily["audit"])

        clippy_runs = run_scripts(jobs(workflow(CI))["clippy"])
        # The per-PR gate's step, located by its command, not by a step index.
        gate = [r for r in clippy_runs if "p0-oom-audit" in r]
        self.assertEqual(len(gate), 1, "ci.yml must have exactly one audit step")

        self.assertIn(
            AUDIT_COMMAND,
            daily_runs,
            "the nightly must run the same enforced audit as ci.yml",
        )
        self.assertIn(
            gate[0].strip(),
            daily_runs,
            "the daily job's audit command must be byte-identical to ci.yml's "
            "Clippy-job step — a weaker nightly reports green on a tree the PR "
            f"gate rejects; ci.yml has: {gate[0]!r}",
        )

    def test_runs_the_audit_only_and_not_the_expensive_suite(self):
        # Why this is a dedicated workflow rather than a `schedule:` on ci.yml:
        # `schedule:` there would re-run the whole expensive suite nightly.
        job = jobs(workflow(DAILY))["audit"]
        invocations = cargo_invocations(job)
        self.assertEqual(
            len(invocations),
            1,
            f"the job must invoke cargo exactly once; found {invocations}",
        )
        self.assertIn("p0-oom-audit", invocations[0])
        for line in invocations:
            for forbidden in ("test", "build", "clippy", "doc", "check"):
                self.assertNotRegex(
                    line,
                    rf"^\s*cargo\s+{forbidden}\b",
                    f"the daily audit must not run `cargo {forbidden}` — that is ci.yml's job",
                )
        # No Docker, no cluster, no image build: the audit is static analysis.
        # Checked on CODE lines, so the comment explaining that this workflow
        # deliberately does not touch Docker is not read as touching Docker.
        code = "\n".join(code_lines(DAILY)).lower()
        for forbidden in ("docker", "compose", "cargo test", "cargo build"):
            self.assertNotIn(
                forbidden, code, f"the daily audit must not {forbidden}"
            )

    def test_fails_loud_and_carries_no_secret(self):
        job = jobs(workflow(DAILY))["audit"]
        # The enforced command must not be softened: continue-on-error or an
        # `if:`-gated audit would hide exactly the failure this job exists for.
        audit_step = next(
            s for s in job["steps"] if "p0-oom-audit" in str(s.get("run", ""))
        )
        self.assertNotIn(
            "continue-on-error", audit_step, "the audit step must fail the job"
        )
        self.assertNotIn("if", audit_step, "the audit step must always run")
        # Default-deny the credential surface: this job reads a repo.
        permissions = workflow(DAILY).get("permissions")
        self.assertEqual(
            permissions,
            {"contents": "read"},
            "the daily audit needs no credential beyond reading the repo",
        )
        raw = "\n".join(code_lines(DAILY))
        for leak in ("secrets.", "secrets.GITHUB_TOKEN", "token:", "password"):
            self.assertNotIn(leak, raw, f"the daily audit must not use `{leak}`")

    def test_every_action_is_sha_pinned(self):
        pins = uses_lines(DAILY)
        self.assertGreaterEqual(len(pins), 3, f"expected the standard pins: {pins}")
        for pin in pins:
            ref = pin.split("@")[-1]
            self.assertRegex(
                ref,
                r"^[a-f0-9]{40}$",
                f"`{pin}` must be pinned to a 40-char commit SHA",
            )
        # The same pins the rest of the repo uses, so a version bump is a
        # repo-wide change rather than a per-file drift.
        self.assertIn("actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd", pins)
        self.assertIn(
            "dtolnay/rust-toolchain@29eef336d9b2848a0b548edc03f92a220660cdb8", pins
        )
        self.assertIn(
            "Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6", pins
        )

    def test_rust_cache_restores_everywhere_and_saves_only_from_main(self):
        # The repo-wide cache policy (tests/ci/test_ci_cache_workflow.py).
        job = jobs(workflow(DAILY))["audit"]
        cache = [s for s in job["steps"] if "Swatinem/rust-cache@" in str(s.get("uses", ""))]
        self.assertEqual(len(cache), 1, "expected exactly one rust-cache step")
        save_if = str(cache[0].get("with", {}).get("save-if", ""))
        self.assertIn("refs/heads/main", save_if)
        self.assertIn("github-hosted", save_if)
        self.assertIn(
            'CARGO_INCREMENTAL: "0"',
            text(DAILY),
            "incremental state is never reused from a fresh checkout",
        )

    def test_a_failed_audit_explains_the_fix_in_the_run_summary(self):
        # The cheapest honest escalation: the failure reason and the two
        # remedies (land the fix, or renew the exemption) are visible on the run
        # itself, with no token and no new service.
        job = jobs(workflow(DAILY))["audit"]
        notes = [
            s
            for s in run_scripts(job)
            if "GITHUB_STEP_SUMMARY" in s and "expired-allow-entry" in s
        ]
        self.assertEqual(
            len(notes),
            1,
            "exactly one step must write an escalation note to the run summary",
        )
        step = next(
            s
            for s in job["steps"]
            if "GITHUB_STEP_SUMMARY" in str(s.get("run", ""))
        )
        self.assertEqual(
            step.get("if"),
            "failure()",
            "the note belongs on the failure path only",
        )
        for remedy in ("Land the fix", "Renew the exemption"):
            self.assertIn(remedy, notes[0])

    def test_the_contract_tests_run_in_ci(self):
        # This file is the guard on the guard; a guard nobody runs is not a
        # guard. ci.yml's `fmt` job hosts the workflow-contract tests.
        ci = text(CI)
        self.assertIn(
            "python3 -m unittest tests.ci.test_oom_audit_daily_workflow",
            ci,
            "ci.yml must run this workflow contract test",
        )
        fmt_job = jobs(workflow(CI))["fmt"]
        contract_runs = [
            str(s.get("run", ""))
            for s in fmt_job["steps"]
            if "unittest" in str(s.get("run", ""))
        ]
        self.assertIn(
            "python3 -m unittest tests.ci.test_oom_audit_daily_workflow",
            contract_runs,
            f"the check belongs in the fmt job's contract-test run group: {contract_runs}",
        )

    def test_the_nightly_fuzz_workflow_surfaces_a_red_run_as_an_issue(self):
        # DEFECT 3: nightly-fuzz went red on 2026-09-30 and nobody noticed for a
        # day. Same mechanism the other nightlies already use (nightly-sim,
        # elle): a failed run files a tracking issue. No new secret — the repo
        # token, and `issues: write` as the entire added permission surface.
        fuzz = workflow(WORKFLOWS / "nightly-fuzz.yml")
        fuzz_job = jobs(fuzz)["fuzz"]
        steps = fuzz_job["steps"]

        filers = [
            (i, s)
            for i, s in enumerate(steps)
            if "gh issue create" in str(s.get("run", ""))
            or "issues.create" in str(s.get("run", ""))
            or "github-script" in str(s.get("uses", ""))
        ]
        self.assertEqual(
            len(filers),
            1,
            "nightly-fuzz must file exactly one tracking issue when it goes red",
        )
        filer_index, filer = filers[0]
        self.assertEqual(filer.get("if"), "failure()", "only on failure")
        self.assertIn("GITHUB_TOKEN", str(filer.get("env", {})))
        # The red verdict must still be what fails the run, and it must come
        # first: the issue is a signal, never a replacement for a failing job.
        # The fuzz steps also write `cargo-test-status` but end in `exit 0`; the
        # verdict is the one that propagates the status with `exit "$status"`.
        verdicts = [
            (i, s)
            for i, s in enumerate(steps)
            if "cargo-test-status" in str(s.get("run", ""))
            and 'exit "$status"' in str(s.get("run", ""))
        ]
        self.assertEqual(
            len(verdicts), 1, f"the fail-if-tests-failed verdict must remain: {steps}"
        )
        verdict_index, verdict = verdicts[0]
        self.assertLess(
            verdict_index,
            filer_index,
            "the verdict must run before the issue filer, so the run is red first",
        )
        self.assertNotIn(
            "continue-on-error",
            verdict,
            "the verdict must not be softened",
        )
        # The permission surface: exactly the pre-existing two plus issues:write.
        self.assertEqual(
            fuzz.get("permissions"),
            {"contents": "write", "pull-requests": "write", "issues": "write"},
            "filing an issue needs issues:write and nothing more",
        )
        # No new credential: the repo's own token, nothing else.
        fuzz_code = "\n".join(
            l for l in text(WORKFLOWS / "nightly-fuzz.yml").splitlines()
            if not l.lstrip().startswith("#")
        )
        secrets = set(re.findall(r"secrets\.([A-Za-z_][A-Za-z0-9_]*)", fuzz_code))
        self.assertEqual(
            secrets,
            {"GITHUB_TOKEN"},
            f"the nightly must not introduce a new secret: {secrets}",
        )

    def test_the_pr_gate_keeps_its_own_audit_step(self):
        # The nightly ADDS a signal; it must not be used as a reason to drop the
        # per-PR gate.
        clippy = jobs(workflow(CI))["clippy"]
        audit_steps = [
            s for s in clippy["steps"] if "p0-oom-audit" in str(s.get("run", ""))
        ]
        self.assertEqual(
            len(audit_steps),
            1,
            "ci.yml's Clippy job must keep its enforced audit step",
        )


if __name__ == "__main__":
    unittest.main()
