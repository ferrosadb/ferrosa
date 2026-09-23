import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


class ReleaseWorkflowTest(unittest.TestCase):
    def test_release_builds_do_not_run_cache_post_jobs(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("Swatinem/rust-cache@", workflow)
        self.assertNotIn("actions/cache/", workflow)

    def test_release_downloads_artifacts_without_node_actions(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("actions/download-artifact@", workflow)
        self.assertNotIn("gh run download", workflow)
        self.assertEqual(
            2,
            workflow.count(
                'bash .github/scripts/download-run-artifacts.sh \\\n            "${GITHUB_RUN_ID}"'
            ),
        )
        self.assertEqual(2, workflow.count("actions: read"))
        docker_job = workflow.split("  docker-image:\n", 1)[1].split("\n  release:\n", 1)[0]
        release_job = workflow.split("  release:\n", 1)[1]
        self.assertIn(
            "    permissions:\n      actions: read\n      contents: read\n      packages: write",
            docker_job,
        )
        self.assertIn(
            "    permissions:\n      actions: read\n      contents: write",
            release_job,
        )
        self.assertNotIn("\npermissions:\n  actions: read", workflow)

    def test_linux_aarch64_builds_natively_with_unchanged_artifacts(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        job = workflow.split("  build-linux-aarch64:\n", 1)[1].split("\n  build-macos-aarch64:\n", 1)[0]
        self.assertIn("    runs-on: ubuntu-24.04-arm\n", job)
        self.assertNotRegex(job, r"(?m)^\s+(?:run: )?cross |cargo install cross|aarch64-linux-gnu-strip")
        self.assertIn(
            "cargo build --release --target aarch64-unknown-linux-musl -p ferrosa -p ferrosa-ctl --features ferrosa/full",
            job,
        )
        self.assertIn("musl-tools", job)
        self.assertIn("CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER: musl-gcc", job)
        # Without it jemalloc's configure finds no atomics under Ubuntu's
        # musl-gcc on arm64 (libgcc's outline-atomics init needs glibc).
        self.assertIn("CFLAGS_aarch64_unknown_linux_musl: -mno-outline-atomics", job)
        # Consumers (docker-image, release) download these names.
        self.assertIn("name: tarball-aarch64-unknown-linux-musl", job)
        self.assertIn("path: dist/ferrosa-*-aarch64-unknown-linux-musl.tar.gz", job)

    def test_release_checkout_is_quiet_and_manifest_inspection_fails_loud(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "env:\n"
            "  CARGO_TERM_COLOR: always\n"
            "  GIT_CONFIG_COUNT: 1\n"
            "  GIT_CONFIG_KEY_0: init.defaultBranch\n"
            "  GIT_CONFIG_VALUE_0: main",
            workflow,
        )
        self.assertNotIn("manifest inspect failed", workflow)
        self.assertNotIn("docker buildx imagetools inspect \"${first_tag}\" ||", workflow)


if __name__ == "__main__":
    unittest.main()
