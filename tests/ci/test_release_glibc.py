"""Releases ship glibc alongside musl, and glibc wherever we choose the libc.

The glibc build is 15-20% faster than musl. Both ship:

* musl stays exactly as it was: the static tarballs install/install.sh picks on
  Linux, because a user's host may have no usable glibc, and the .deb.
* glibc is additive: build-linux-gnu already compiled it for the -glibc
  images; its tarballs are now release assets too, for hosts that have glibc.
* Where WE choose the libc -- the container image -- the default tags
  (latest, vX, X.Y, X, nightly) are the glibc image. The musl image keeps
  shipping under -musl tags, and -glibc stays as an alias so existing pins
  keep resolving.

build-linux-gnu's own image bundle used to be named ferrosa-gnu-<arch>.tar.gz,
which matches the release job's `ferrosa-*.tar.gz` asset glob. release did not
wait for build-linux-gnu, so whether that bundle (with its profiling binary)
was published as a release asset depended on which job finished first.
"""

import re
import unittest
from pathlib import Path

from tests.ci.test_ci_cache_workflow import jobs

REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASE = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
RELEASE_ASSET_GLOB = re.compile(r"^ferrosa-.*\.tar\.gz$")
# Ubuntu 22.04 (glibc 2.35), pinned by digest. Not yet served from
# downloads.ferrosa.ai like the other build images: forge t_df6a29de.
UBUNTU_2204 = "docker.io/library/ubuntu@sha256:0a9887931b0e38af2e3487ed39e29436b0535c4f3f52e57745baaf79d1898956"


def code(job):
    return "\n".join(l for l in job.split("\n") if not l.lstrip().startswith("#"))


class ReleaseShipsGlibcAlongsideMusl(unittest.TestCase):
    def setUp(self):
        self.jobs = jobs(RELEASE)

    def test_the_glibc_build_stages_standard_release_tarballs(self):
        gnu = code(self.jobs["build-linux-gnu"])
        self.assertIn('bash .github/scripts/stage-release-tarball.sh "${{ matrix.target }}"', gnu)
        self.assertIn("name: tarball-${{ matrix.target }}", gnu)
        self.assertIn("path: dist/ferrosa-*-${{ matrix.target }}.tar.gz", gnu)

    def test_the_image_bundle_cannot_become_a_release_asset(self):
        bundles = re.findall(r'tar -czf "dist/([^"]+)"', code(self.jobs["build-linux-gnu"]))
        self.assertTrue(bundles)
        for name in bundles:
            sample = name.replace("${{ matrix.arch }}", "amd64")
            self.assertIsNone(RELEASE_ASSET_GLOB.match(sample), name)
        self.assertIn("image-bundle-gnu-${arch}.tar.gz", code(self.jobs["docker-image-glibc"]))

    def test_the_release_waits_for_the_glibc_tarballs(self):
        self.assertIn("- build-linux-gnu", self.jobs["release"])

    def test_the_release_notes_list_the_glibc_tarballs(self):
        notes = self.jobs["release"]
        self.assertIn("x86_64-unknown-linux-gnu.tar.gz", notes)
        self.assertIn("aarch64-unknown-linux-gnu.tar.gz", notes)

    def test_the_glibc_build_runs_on_ubuntu_22_04_and_asserts_its_floor(self):
        # Built against glibc 2.35 so the tarball, image and .deb all run on
        # Ubuntu 22.04 and newer. Linking an older glibc costs no speed: the
        # binary uses the host's glibc at run time.
        gnu = self.jobs["build-linux-gnu"]
        self.assertIn(f"image: {UBUNTU_2204}", gnu)
        self.assertIn('GLIBC_FLOOR: "2.35"', gnu)
        self.assertIn('"$GLIBC_FLOOR"', code(gnu))

    def test_the_deb_is_built_from_the_glibc_binaries(self):
        gnu = code(self.jobs["build-linux-gnu"])
        self.assertIn(
            'bash .github/scripts/build-deb.sh "${{ matrix.arch }}" "target/${{ matrix.target }}/release" "$VERSION" "$GLIBC_FLOOR" dist',
            gnu,
        )
        self.assertIn("dist/ferrosa_*_${{ matrix.arch }}.deb", gnu)
        musl = code(self.jobs["build-linux-x86_64"])
        self.assertNotIn("dpkg-deb", musl)
        self.assertNotIn(".deb", musl)

    def test_the_release_notes_describe_the_glibc_deb(self):
        notes = self.jobs["release"]
        self.assertIn("`ferrosa_VERSION_amd64.deb`", notes)
        self.assertIn("`ferrosa_VERSION_arm64.deb`", notes)
        self.assertIn("Ubuntu 22.04", notes)

    def test_the_default_image_tags_are_glibc(self):
        glibc = code(self.jobs["docker-image-glibc"])
        for tag in ("${IMAGE}:latest ", "${IMAGE}:v${VERSION} ", "${IMAGE}:nightly "):
            self.assertIn(tag, glibc, tag)
        for alias in ("${IMAGE}:latest-glibc", "${IMAGE}:nightly-glibc"):
            self.assertIn(alias, glibc, alias)

    def test_the_musl_image_keeps_shipping_under_musl_tags(self):
        musl = code(self.jobs["docker-image"])
        tags = re.findall(r"\$\{IMAGE\}:[^\s\"]+", musl)
        self.assertTrue(tags)
        for tag in tags:
            self.assertTrue(tag.endswith("-musl"), tag)


if __name__ == "__main__":
    unittest.main()
