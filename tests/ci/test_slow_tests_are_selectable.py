"""Every `slow-tests` test is named `...::slow::...`, so both filters see it.

PR CI skips slow tests with `--skip ::slow::` and nightly-slow-tests.yml selects
them with `::slow::`. Both are substring filters on the test's path. A `mod slow`
at the ROOT of an integration-test file produces paths that START with
`slow::`, with no leading `::`, so they match neither: the test runs on every
PR and never in the nightly that exists for it.

ferrosa-graph/tests/adjacency_deploy_heal.rs had exactly that.
`slow::deploy_heal_at_memory_cluster_scale` ran on every PR for 99 s (CI run
40017365568). Nest the module one level down instead.
"""

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ROOT_SLOW_MOD = re.compile(r"(?m)^(?:pub )?mod slow\b")


def integration_test_roots():
    return sorted(
        p for p in REPO_ROOT.glob("*/tests/*.rs") if p.parts[len(REPO_ROOT.parts)] != "cassandra"
    )


class SlowTestsAreSelectable(unittest.TestCase):
    def test_no_integration_test_root_declares_a_top_level_slow_module(self):
        roots = integration_test_roots()
        self.assertGreater(len(roots), 100, "expected the workspace's integration tests")
        offenders = [
            str(p.relative_to(REPO_ROOT))
            for p in roots
            if ROOT_SLOW_MOD.search(p.read_text(encoding="utf-8"))
        ]
        self.assertEqual(offenders, [], "nest `mod slow` inside another module")


if __name__ == "__main__":
    unittest.main()
