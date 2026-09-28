import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


class ReleaseWorkflowTest(unittest.TestCase):
    def test_release_workflow_contract_tests_run_in_ci(self):
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("python3 -m unittest -q tests/ci/test_release_workflow.py", workflow)

    def test_manual_branch_release_uses_a_debian_compatible_version(self):
        version_script = ROOT / ".github" / "scripts" / "release-version.sh"
        version = subprocess.run(
            ["bash", str(version_script), "branch", "main", "42"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertEqual("0.0.0-main.42", version)

        tagged_version = subprocess.run(
            ["bash", str(version_script), "tag", "v0.13.2", "42"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertEqual("0.13.2", tagged_version)

        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        debian_job = workflow.split("      - name: Build Debian package\n", 1)[1].split("\n      - name:", 1)[0]
        self.assertIn(
            'VERSION="$(bash .github/scripts/release-version.sh '
            '"$GITHUB_REF_TYPE" "$GITHUB_REF_NAME" "$GITHUB_RUN_NUMBER")"',
            debian_job,
        )

    def test_manual_branch_release_uses_the_same_version_in_all_tarballs(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(
            3,
            workflow.count(
                'FERROSA_RELEASE_TAG="v${VERSION}" bash .github/scripts/stage-release-tarball.sh'
            ),
        )

    def test_tarball_stager_accepts_normalized_version_and_legacy_ref_name(self):
        stage_script = ROOT / ".github" / "scripts" / "stage-release-tarball.sh"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binaries = root / "bin"
            binaries.mkdir()
            for name in ("ferrosa", "ferrosa-ctl"):
                binary = binaries / name
                binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
                binary.chmod(0o755)

            env = os.environ.copy()
            env["GITHUB_REF_NAME"] = "main"
            env["FERROSA_RELEASE_TAG"] = "v0.0.0-main.42"
            subprocess.run(
                ["bash", str(stage_script), "x86_64-unknown-linux-musl", str(binaries)],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertTrue(
                (root / "dist/ferrosa-v0.0.0-main.42-x86_64-unknown-linux-musl.tar.gz").is_file()
            )

            env.pop("FERROSA_RELEASE_TAG")
            env["GITHUB_REF_NAME"] = "v0.0.0-smoke"
            subprocess.run(
                ["bash", str(stage_script), "x86_64-unknown-linux-musl", str(binaries)],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertTrue(
                (root / "dist/ferrosa-v0.0.0-smoke-x86_64-unknown-linux-musl.tar.gz").is_file()
            )

    def test_release_builds_do_not_run_cache_post_jobs(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("Swatinem/rust-cache@", workflow)
        self.assertNotIn("actions/cache/", workflow)

    def test_release_downloads_artifacts_without_node_actions(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("actions/download-artifact@", workflow)
        self.assertNotIn("gh run download", workflow)
        self.assertEqual(
            3,
            workflow.count(
                'bash .github/scripts/download-run-artifacts.sh \\\n            "${GITHUB_RUN_ID}"'
            ),
        )
        self.assertEqual(3, workflow.count("actions: read"))
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

    def test_release_publishes_production_and_profiling_oci_downloads(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("  publish-oci-downloads:\n", workflow)
        job = workflow.split("  publish-oci-downloads:\n", 1)[1].split("\n  release:\n", 1)[0]
        self.assertIn("ferrosa-minimal", job)
        self.assertIn("ferrosa-profiling", job)
        self.assertIn("--profile profiling", job)
        self.assertIn("ferrosa/full,ferrosa/profiling", job)
        self.assertIn("images/${{ matrix.arch }}/${{ steps.identity.outputs.channel }}", job)
        self.assertIn("scripts/publish-images.sh", job)
        self.assertIn("R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}", job)
        self.assertIn("R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}", job)
        self.assertRegex(job, r"repository: ferrosadb/ferrosa-installer\n\s+ref: [0-9a-f]{40}")

    def test_profiling_build_probe_uses_jemalloc_runtime_configuration(self):
        workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('MALLOC_CONF="prof:true,prof_active:true,prof_final:true,lg_prof_sample:0,prof_prefix:$RUNNER_TEMP/ferrosa-profile"', workflow)
        self.assertNotIn("_RJEM_MALLOC_CONF=", workflow)
        self.assertIn("find \"$RUNNER_TEMP\" -maxdepth 1 -name 'ferrosa-profile.*.heap' -print -quit | grep -q .", workflow)


if __name__ == "__main__":
    unittest.main()
