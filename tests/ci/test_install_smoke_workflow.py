import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "install-smoke.yml"


def jobs_routed_to_self_hosted(workflow: str) -> list[str]:
    """Names of the jobs in this workflow whose matrix routes to self-hosted runners."""
    names = []
    for block in re.split(r"\n  (?=[a-z][a-z0-9_-]*:\n)", workflow):
        header = block.split(":", 1)[0].strip()
        if "self-hosted" in block and header:
            names.append(header)
    return names


class InstallSmokeWorkflowTest(unittest.TestCase):
    # These jobs are labelled `ubuntu-latest`/`macos-14` by their matrix `os:`
    # key, but that is a DISPLAY NAME. The `runner:` key routes them to
    # self-hosted machines, and a self-hosted machine is not a fresh hosted
    # image: it may have no passwordless sudo, and it already carries the
    # toolchain from the last build.
    #
    # Running the hosted-image install line blindly failed the Linux tarball
    # build in 10 seconds, before any compilation, on every run after builds
    # were routed here — while the identical line kept passing in the jobs that
    # really do use `runs-on: ubuntu-latest`. The `os:` label is exactly what
    # makes that easy to misread.
    #
    # Test list:
    # - [x] A dependency install never assumes passwordless sudo is available.
    # - [x] It is skipped when the tool is already present.
    # - [x] A runner that has neither says which machine and what to install.

    def test_dependency_install_does_not_assume_passwordless_sudo(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertTrue(
            jobs_routed_to_self_hosted(workflow),
            "this test is meaningless if nothing here routes to a self-hosted "
            "runner any more — delete it, or re-point it at wherever they went",
        )

        bare_sudo = re.findall(r"^\s*run: sudo apt-get .*$", workflow, re.MULTILINE)
        self.assertEqual(
            bare_sudo,
            [],
            "a one-line `run: sudo apt-get ...` assumes a hosted image. On a "
            "self-hosted runner without passwordless sudo it fails before any "
            "build step, and the only diagnostic is apt's. Guard it with "
            "`sudo -n true` and fall back to a message naming the machine. "
            f"Found: {bare_sudo}",
        )

    def test_dependency_install_is_skipped_when_already_present(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")

        installs = workflow.count("- name: Install capnproto")
        self.assertGreater(installs, 0, "expected at least one capnproto step")
        self.assertEqual(
            workflow.count("if command -v capnp >/dev/null 2>&1; then"),
            installs,
            "every capnproto step must short-circuit when capnp is already on "
            "the box. A self-hosted runner keeps what the last build installed, "
            "so re-installing it is pure latency and one more thing to fail.",
        )

    def test_a_runner_that_cannot_install_says_which_machine_and_what_to_do(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn(
            "RUNNER_NAME",
            workflow,
            "the failure must name the runner. 'apt-get failed' on a fleet of "
            "self-hosted machines does not say which one to go fix.",
        )
        self.assertIn(
            "sudo apt-get install -y capnproto",
            workflow,
            "the failure must state the command that fixes it, so the person "
            "reading the log does not have to work it out",
        )


if __name__ == "__main__":
    unittest.main()
