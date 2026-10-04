import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tests" / "install_smoke.sh"


class InstallSmokeScriptTest(unittest.TestCase):
    """Guards the ferrosa-memory readiness wait in tests/install_smoke.sh.

    That wait timed out on a cold, loaded CI runner while ferrosa-memory was
    HEALTHY but still migrating the agent_memory schema (dozens of tables,
    ~200-550 ms each), and reported it as "never became ready ... could not
    connect to ferrosa?" — a fixed 90x1 s tick count with a diagnostic that
    named the wrong cause. A readiness wait must be sized on the wall clock and
    must say WHY it gave up. This pins all of that, so it cannot be silently
    restored to a tick count or an unhelpful message.
    """

    def _start_memory(self) -> str:
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("start_memory() {", script)
        body = script.split("start_memory() {", 1)[1]
        return body.split("\nstop_memory() {", 1)[0]

    def test_readiness_wait_is_a_wall_clock_deadline_not_a_tick_count(self):
        start_memory = self._start_memory()

        self.assertNotIn(
            'while [ "$i" -lt 90 ]',
            start_memory,
            "a fixed 90x1 s tick count cannot absorb a slow cold schema "
            "migration; wait on the wall clock with headroom instead",
        )
        self.assertIn(
            "deadline=$(( SECONDS + MEM_READY_TIMEOUT ))",
            start_memory,
            "the readiness wait must size itself on $SECONDS against a named "
            "timeout, so a slow-but-healthy server is waited out, not failed",
        )
        self.assertIn("while (( SECONDS < deadline )); do", start_memory)

    def test_readiness_timeout_is_named_generous_and_overridable(self):
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn(
            'MEM_READY_TIMEOUT="${MEM_READY_TIMEOUT:-300}"',
            script,
            "the timeout must be a named default (overridable for local "
            "iteration) with real headroom over the observed cold-migration time",
        )
        self.assertIn(
            "-m 2",
            self._start_memory(),
            "each readiness probe must be bounded so one hung connect cannot "
            "consume the whole wall-clock budget",
        )

    def test_timeout_reports_why_instead_of_blaming_ferrosa(self):
        start_memory = self._start_memory()

        self.assertIn(
            "--- last /healthz/ready body ---",
            start_memory,
            "the timeout must surface the last readiness body: it names the "
            "failing migration, which a bare 'could not connect to ferrosa?' hides",
        )
        self.assertIn('tail -40 "$LOGDIR/memory.log"', start_memory)
        self.assertNotIn(
            "could not connect to ferrosa?",
            start_memory,
            "the old message named a cause it never checked; a healthy server "
            "still migrating the schema is not a connection failure",
        )


if __name__ == "__main__":
    unittest.main()
