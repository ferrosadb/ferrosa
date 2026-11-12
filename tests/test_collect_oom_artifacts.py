#!/usr/bin/env python3
"""Tests for the non-invasive OOM artifact collector."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "collect-oom-artifacts.py"


def load_module():
    spec = importlib.util.spec_from_file_location("collect_oom_artifacts", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CollectOomArtifactsTests(unittest.TestCase):
    def test_memory_sample_header_contains_phase_correlation_fields(self) -> None:
        module = load_module()

        self.assertEqual(
            module.memory_sample_header(),
            [
                "timestamp_utc",
                "container",
                "status",
                "health",
                "oom_killed",
                "exit_code",
                "restart_count",
                "pid",
                "rss_kb",
                "hwm_kb",
                "vmsize_kb",
                "threads",
                "cgroup_current_bytes",
                "docker_mem_usage",
                "docker_mem_percent",
                "docker_pids",
            ],
        )

    def test_secret_redaction_masks_common_credentials(self) -> None:
        module = load_module()

        text = "AWS_SECRET_ACCESS_KEY=abc123\npassword: hunter2\napi_token = xyz\nnormal=value"

        self.assertEqual(
            module.redact_secrets(text),
            "AWS_SECRET_ACCESS_KEY=[REDACTED]\npassword: [REDACTED]\napi_token = [REDACTED]\nnormal=value",
        )

    def test_secret_redaction_masks_docker_inspect_env_entries(self) -> None:
        module = load_module()

        text = (
            '{\n'
            '  "Config": {\n'
            '    "Env": [\n'
            '      "AWS_SECRET_ACCESS_KEY=abc123",\n'
            '      "TOKEN=plain-token",\n'
            '      "DATABASE_URL=postgres://app:dbpass@example.invalid/db",\n'
            '      "NORMAL=value"\n'
            '    ]\n'
            '  }\n'
            '}\n'
        )

        redacted = module.redact_secrets(text)

        self.assertNotIn("abc123", redacted)
        self.assertNotIn("plain-token", redacted)
        self.assertNotIn("dbpass", redacted)
        self.assertIn('"AWS_SECRET_ACCESS_KEY=[REDACTED]"', redacted)
        self.assertIn('"TOKEN=[REDACTED]"', redacted)
        self.assertIn('postgres://app:[REDACTED]@example.invalid/db', redacted)
        self.assertIn('"NORMAL=value"', redacted)
        self.assertEqual(
            json.loads(redacted)["Config"]["Env"],
            [
                "AWS_SECRET_ACCESS_KEY=[REDACTED]",
                "TOKEN=[REDACTED]",
                "DATABASE_URL=postgres://app:[REDACTED]@example.invalid/db",
                "NORMAL=value",
            ],
        )

    def test_default_containers_include_ferrosa_nodes_and_mcp(self) -> None:
        module = load_module()

        self.assertEqual(
            module.DEFAULT_CONTAINERS,
            (
                "ferrosa-memory-node1-1",
                "ferrosa-memory-node2-1",
                "ferrosa-memory-node3-1",
                "ferrosa-memory-ferrosa-memory-mcp-1",
            ),
        )


if __name__ == "__main__":
    unittest.main()
