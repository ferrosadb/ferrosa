"""PR CI builds each shipped binary once, and every job runs one of them.

12-factor build/release/run: one build stage produces each artifact, and every
later stage runs an artifact instead of compiling its own. ci.yml used to
compile the server's release binary five times per run: musl `ferrosa/full`,
a glibc default-features node image, a glibc `flight` binary, and the glibc
install-smoke tarball on Linux and macOS.

Two binaries ship on Linux, and CI builds each of them exactly once, with the
command release.yml ships it with:

* glibc `ferrosa/full` (`build-node-image`): 15-20% faster than musl, and the
  build to ship wherever the host has a new enough glibc. The test node image,
  examples, the secure-compose overlay, the integration cluster, the
  PostgreSQL Jepsen cluster and the paired repositories all run it.
* musl `ferrosa/full` (`musl`): the static build for hosts without a usable
  glibc. The main ruleset requires its check, "Static Build (musl)".

The PR install smoke installs both tarballs, staged from those two builds.
Whether Arrow Flight listens is runtime config (`FERROSA_FLIGHT_ENABLED`), not
a third build: the test node image ships it off so the paired repositories
that pull the image see no new listener, and the jobs that need it turn it on.
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

GLIBC_TARGET = "x86_64-unknown-linux-gnu"
MUSL_TARGET = "x86_64-unknown-linux-musl"
GLIBC_BUILD = (
    f"cargo build --locked --release --target {GLIBC_TARGET} "
    "-p ferrosa -p ferrosa-ctl --features ferrosa/full"
)
MUSL_BUILD = (
    f"cargo build --release --target {MUSL_TARGET} "
    "-p ferrosa -p ferrosa-ctl --features ferrosa/full"
)
BUILD_JOB = "build-node-image"
MUSL_JOB = "musl"
# Jobs that run the server binary. None of them may compile it.
CONSUMERS = ("examples", "secure-compose", "integration", "postgres-jepsen-fault")


def release_builds(job):
    # Whitespace-normalized: a joined line continuation leaves a double space.
    return [" ".join(c.split()) for c in cargo_commands(job) if re.search(r"cargo build\b.*--release", c)]


class CiBuildsEachShippedBinaryOnce(unittest.TestCase):
    def setUp(self):
        self.jobs = jobs(CI)

    def test_exactly_two_jobs_compile_a_release_binary_once_each(self):
        builders = {name for name, job in self.jobs.items() if release_builds(job)}
        self.assertEqual(builders, {BUILD_JOB, MUSL_JOB})
        self.assertEqual(release_builds(self.jobs[BUILD_JOB]), [GLIBC_BUILD])
        self.assertEqual(release_builds(self.jobs[MUSL_JOB]), [MUSL_BUILD])

    def test_each_build_is_the_command_release_yml_ships(self):
        release = jobs(RELEASE)
        gnu = [c.replace("${{ matrix.target }}", GLIBC_TARGET) for c in release_builds(release["build-linux-gnu"])]
        self.assertIn(GLIBC_BUILD, gnu)
        self.assertIn(MUSL_BUILD, release_builds(release["build-linux-x86_64"]))

    def test_the_musl_job_reports_the_required_check(self):
        # The main ruleset requires "Static Build (musl)" (gh api
        # repos/ferrosadb/ferrosa/rules/branches/main). Losing the name blocks
        # every merge until the ruleset is edited.
        self.assertRegex(self.jobs[MUSL_JOB], r"(?m)^    name: Static Build \(musl\)$")

    def test_the_duplicate_build_jobs_are_gone(self):
        self.assertNotIn("build-flight-binary", self.jobs)

    def test_the_node_image_packages_the_glibc_binary_with_flight_off(self):
        job = self.jobs[BUILD_JOB]
        self.assertIn(f"cp target/{GLIBC_TARGET}/release/ferrosa ", job)
        self.assertIn("ENV FERROSA_FLIGHT_ENABLED=false", job)
        self.assertIn("name: ferrosa-glibc", job)

    def test_every_consumer_runs_the_built_image_and_compiles_no_server(self):
        for name in CONSUMERS:
            job = self.jobs[name]
            self.assertRegex(job, rf"(?m)^    needs: .*\b{BUILD_JOB}\b", name)
            self.assertIn("name: ferrosa-node-image", job, name)
            self.assertEqual(release_builds(job), [], name)

    def test_the_production_overlay_turns_flight_on_by_config(self):
        self.assertRegex(SECURE_OVERLAY, r'FERROSA_FLIGHT_ENABLED: "true"')

    def test_the_pr_install_smoke_installs_both_shipped_linux_tarballs(self):
        # The PR install smoke used to compile and install a glibc
        # default-features build: a Linux release compile of a build no user
        # gets. It now stages each shipped tarball from its one build.
        job = self.jobs["install-smoke"]
        self.assertRegex(job, r"(?m)^    needs: \[build-node-image, musl\]$")
        for artifact, target in (("ferrosa-glibc", GLIBC_TARGET), ("ferrosa-musl-static", MUSL_TARGET)):
            self.assertIn(artifact, job)
            self.assertIn(target, job)
        self.assertIn("stage-release-tarball.sh", job)
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
