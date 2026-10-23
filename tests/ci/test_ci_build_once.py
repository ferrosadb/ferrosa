"""PR CI builds the shipped binary once and every job runs that build.

12-factor build/release/run: one build stage produces the artifact, and every
later stage runs that artifact instead of compiling its own. ci.yml used to
compile the server's release binary three times per run, each in its own
~8-minute job:

* `musl`: x86_64 musl, `ferrosa/full`. The binary release.yml ships, and the
  one inside the published Docker image.
* `build-node-image`: glibc, default features, packaged into the test node
  image. It never shipped. Its reason, "musl's allocator is slow", stopped
  holding when jemalloc became the global allocator on every non-MSVC target
  (ferrosa/src/main.rs).
* `build-flight-binary`: glibc again with `flight`, used only by secure-compose.

Now `build-node-image` compiles the release.yml musl `ferrosa/full` build once.
The test node image, the examples job, the secure-compose overlay, the
integration cluster and the PostgreSQL Jepsen cluster all run that binary.
Whether Arrow Flight listens is runtime config (`FERROSA_FLIGHT_ENABLED`), not
a second build: the test node image ships it off so the paired repositories
that pull the image see no new listener, and the production overlay turns it
on.
"""

import re
import unittest
from pathlib import Path

from tests.ci.test_ci_cache_workflow import cargo_commands, jobs

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
CI = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
RELEASE = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")
SECURE_OVERLAY = (REPO_ROOT / "docker-compose.secure.yml").read_text(encoding="utf-8")

SHIPPED_BUILD = (
    "cargo build --release --target x86_64-unknown-linux-musl "
    "-p ferrosa -p ferrosa-ctl --features ferrosa/full"
)
BUILD_JOB = "build-node-image"
# Jobs that run the server binary. None of them may compile it.
CONSUMERS = ("examples", "secure-compose", "integration", "postgres-jepsen-fault")


def release_builds(job):
    return [c for c in cargo_commands(job) if re.search(r"cargo build\b.*--release", c)]


class CiBuildsTheShippedBinaryOnce(unittest.TestCase):
    def setUp(self):
        self.jobs = jobs(CI)

    def test_exactly_one_job_compiles_a_release_binary(self):
        builders = {name for name, job in self.jobs.items() if release_builds(job)}
        self.assertEqual(builders, {BUILD_JOB})
        self.assertEqual(len(release_builds(self.jobs[BUILD_JOB])), 1)

    def test_the_one_build_is_the_build_release_yml_ships(self):
        self.assertEqual(release_builds(self.jobs[BUILD_JOB])[0].strip(), SHIPPED_BUILD)
        release_x86 = jobs(RELEASE)["build-linux-x86_64"]
        self.assertIn(SHIPPED_BUILD, [c.strip() for c in release_builds(release_x86)])

    def test_the_build_job_reports_the_required_musl_check(self):
        # The main ruleset requires "Static Build (musl)" (gh api
        # repos/ferrosadb/ferrosa/rules/branches/main). Losing the name blocks
        # every merge until the ruleset is edited.
        self.assertRegex(self.jobs[BUILD_JOB], r"(?m)^    name: Static Build \(musl\)$")

    def test_the_duplicate_build_jobs_are_gone(self):
        self.assertNotIn("musl", self.jobs)
        self.assertNotIn("build-flight-binary", self.jobs)

    def test_the_node_image_packages_the_musl_binary_with_flight_off(self):
        job = self.jobs[BUILD_JOB]
        self.assertIn("cp target/x86_64-unknown-linux-musl/release/ferrosa ", job)
        self.assertIn("ENV FERROSA_FLIGHT_ENABLED=false", job)

    def test_every_consumer_runs_the_built_image_and_compiles_no_server(self):
        for name in CONSUMERS:
            job = self.jobs[name]
            self.assertRegex(job, rf"(?m)^    needs: .*\b{BUILD_JOB}\b", name)
            self.assertIn("name: ferrosa-node-image", job, name)
            self.assertEqual(release_builds(job), [], name)

    def test_the_production_overlay_turns_flight_on_by_config(self):
        self.assertRegex(SECURE_OVERLAY, r'FERROSA_FLIGHT_ENABLED: "true"')

    def test_the_pr_install_smoke_installs_the_shipped_linux_tarball(self):
        # install/install.sh installs the x86_64 MUSL tarball on Linux. The PR
        # install smoke used to compile and install a glibc default-features
        # build instead: a third Linux release compile, of a build no user gets.
        job = self.jobs["install-smoke"]
        self.assertRegex(job, rf"(?m)^    needs: .*\b{BUILD_JOB}\b")
        self.assertIn("name: ferrosa-musl-static", job)
        self.assertIn("stage-release-tarball.sh x86_64-unknown-linux-musl", job)
        self.assertIn("tests/install_smoke.sh --ferrosa-tarball", job)
        self.assertEqual(cargo_commands(job), [])

    def test_the_isolated_harness_run_reuses_the_main_test_build(self):
        # `-p ferrosa` resolves features differently from the main run's
        # `--workspace` selection, so cargo rebuilt the workspace: 2m55s of
        # compile to run 24s of tests (CI run 40017365568). The same package
        # selection, narrowed by a filterset, builds nothing new.
        job = self.jobs["test"]
        main = [c for c in cargo_commands(job) if "--skip ::slow::" in c]
        harness = [c for c in cargo_commands(job) if "commit_guarantee_replica_process" in c and c not in main]
        self.assertEqual(len(main), 1)
        self.assertEqual(len(harness), 1)
        selection = re.compile(r"--all-features|--workspace|--lib|--tests|--exclude \S+")
        self.assertEqual(sorted(selection.findall(harness[0])), sorted(selection.findall(main[0])))
        excluded = set(re.findall(r"not binary\((\w+_process)\)", main[0]))
        selected = set(re.findall(r"binary\((\w+)\)", harness[0]))
        self.assertEqual(len(excluded), 6)
        self.assertEqual(selected, excluded)
        self.assertIn("package(ferrosa) and (", harness[0])

    def test_the_from_source_install_smokes_do_not_run_per_pr(self):
        # install-smoke.yml compiles ferrosa on Linux and macOS and ferrosa-memory
        # from source. It gates releases (workflow_call) and runs on main and
        # nightly; per PR, the job above covers the Linux install.
        smoke = (WORKFLOWS / "install-smoke.yml").read_text(encoding="utf-8")
        triggers = smoke.split("\njobs:\n", 1)[0]
        self.assertNotIn("pull_request_target:", triggers)
        self.assertNotIn("pull_request:", triggers)
        self.assertIn("workflow_call:", triggers)
        self.assertIn("schedule:", triggers)


if __name__ == "__main__":
    unittest.main()
