from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "driver-tests.yml"


def read_workflow() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_driver_smoke_is_fail_loud():
    # C8 (Full CQL Driver Compatibility) is complete: the driver smoke step must
    # be fail-loud (no continue-on-error) so a driver regression fails the build.
    # See specs/coverage/driver-compat-c8.md.
    workflow = read_workflow()
    run_all_step = workflow.split("- name: Run all language driver smoke tests", 1)[1]
    run_all_step = run_all_step.split("\n\n", 1)[0]

    assert "bash tests/drivers/run-all.sh" in run_all_step
    assert "continue-on-error: true" not in run_all_step


def test_driver_smoke_harness_output_is_teed_to_uploaded_artifact():
    workflow = read_workflow()
    run_all_step = workflow.split("- name: Run all language driver smoke tests", 1)[1]
    run_all_step = run_all_step.split("\n\n", 1)[0]

    assert "mkdir -p /tmp/driver-logs" in run_all_step
    assert "bash tests/drivers/run-all.sh 2>&1 | tee /tmp/driver-logs/run-all.log" in run_all_step
    assert "exit ${PIPESTATUS[0]}" in run_all_step


def test_driver_smoke_failure_logs_are_uploaded_when_harness_fails():
    workflow = read_workflow()

    assert "- name: Collect driver test logs on failure" in workflow
    assert "- name: Upload driver test logs on failure" in workflow
    assert "if: failure()" in workflow
    assert "path: /tmp/driver-logs/" in workflow
