"""The macOS LaunchAgent template must restart a node that exits on failure.

ferrosa fails loud: an unrecoverable startup error exits with status 1. With
KeepAlive {Crashed: true} launchd restarts only on a signal, so such a node
stayed down until someone noticed. node3 of a local cluster stayed down for
35 hours on 2026-10-06 after a commit-log stall during startup replay.
"""

import plistlib
import unittest
from pathlib import Path

TEMPLATE = Path(__file__).resolve().parents[2] / "launchd" / "com.ferrosadb.ferrosa.plist"


def load_template() -> dict:
    return plistlib.loads(TEMPLATE.read_bytes().replace(b"__HOME__", b"/Users/test"))


class LaunchdTemplateTest(unittest.TestCase):
    def test_a_node_that_exits_non_zero_is_restarted(self):
        keep_alive = load_template()["KeepAlive"]
        self.assertIsInstance(keep_alive, dict, "KeepAlive must be conditional, not always-on")
        # SuccessfulExit = false: restart on any non-zero exit or signal, never
        # on exit 0. `launchctl bootout` removes the job whatever KeepAlive says.
        self.assertIs(keep_alive.get("SuccessfulExit"), False, keep_alive)

    def test_restarts_are_throttled(self):
        # A node that fails at every start must not spin: launchd waits at
        # least this long between launches.
        self.assertGreaterEqual(load_template().get("ThrottleInterval", 0), 10)


if __name__ == "__main__":
    unittest.main()
