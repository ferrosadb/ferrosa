import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOCKER_SMOKE = ROOT / "tests" / "docker-smoke.sh"
DOCKER_COMPOSE = ROOT / "docker-compose.yml"


class NightlyDockerSmokeTest(unittest.TestCase):
    def test_published_pair_services_bind_to_the_container_network(self):
        compose = DOCKER_COMPOSE.read_text(encoding="utf-8")

        for service, next_service in (
            ("node1", "node2"),
            ("node2", "node3"),
            ("node3", "volumes"),
        ):
            block = compose.split(f"  {service}:\n", 1)[1]
            block = block.split(f"\n  {next_service}:\n", 1)[0]
            self.assertIn(
                'FERROSA_CQL_BIND: "0.0.0.0:9042"',
                block,
                f"{service} publishes CQL to the host and must not inherit the loopback-only default",
            )
            self.assertIn(
                'FERROSA_WEB_BIND: "0.0.0.0:9090"',
                block,
                f"{service} publishes HTTP to the host and must not inherit the loopback-only default",
            )

    def test_pair_cql_readiness_has_a_wall_clock_deadline_and_bounded_probes(self):
        smoke = DOCKER_SMOKE.read_text(encoding="utf-8")
        wait_cql = smoke.split("wait_cql() {", 1)[1]
        wait_cql = wait_cql.split("# Helper: check cluster status", 1)[0]

        self.assertIn("deadline=$((SECONDS + timeout))", wait_cql)
        self.assertIn("while (( SECONDS < deadline )); do", wait_cql)
        self.assertIn('cql_ready "$port"', wait_cql)
        self.assertIn("--connect-timeout=2", smoke)
        self.assertIn("--request-timeout=2", smoke)
        self.assertNotIn('seq 1 "$timeout"', wait_cql)

    def test_pair_visibility_checks_retry_with_a_bounded_deadline(self):
        smoke = DOCKER_SMOKE.read_text(encoding="utf-8")

        self.assertIn("wait_for_cql_value() {", smoke)
        helper = smoke.split("wait_for_cql_value() {", 1)[1]
        helper = helper.split("# Helper: check cluster status", 1)[0]
        self.assertIn("deadline=$((SECONDS + timeout))", helper)
        self.assertIn("while (( SECONDS < deadline )); do", helper)
        self.assertIn("--connect-timeout=2", helper)
        self.assertIn("--request-timeout=2", helper)
        self.assertIn('fail "$description did not become visible in ${timeout}s', helper)
        self.assertGreaterEqual(smoke.count("wait_for_cql_value 9042"), 2)
        self.assertIn("SELECT v FROM smoke_test.kv WHERE k = 'key1';", smoke)
        self.assertIn('"from_node1" "node1 key1"', smoke)

    def test_rejoining_secondary_waits_for_role_instead_of_cql(self):
        smoke = DOCKER_SMOKE.read_text(encoding="utf-8")
        phase4 = smoke.split("# Phase 4: Rejoin and catch-up", 1)[1]
        phase4 = phase4.split("# Phase 5: Switchover", 1)[0]

        self.assertIn("wait_for_cluster_role() {", smoke)
        role_helper = smoke.split("wait_for_cluster_role() {", 1)[1]
        role_helper = role_helper.split("# Helper: check cluster status", 1)[0]
        self.assertIn("deadline=$((SECONDS + timeout))", role_helper)
        self.assertIn("while (( SECONDS < deadline )); do", role_helper)
        self.assertIn('wait_for_cluster_role 9090 "node1" "secondary"', phase4)
        self.assertNotIn('wait_cql 9042 "node1"', phase4)


if __name__ == "__main__":
    unittest.main()
