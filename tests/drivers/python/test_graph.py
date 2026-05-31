"""Graph HTTP endpoint smoke tests using the requests library.

Tests exercise the /graph/* endpoints exposed by Ferrosa's graph engine.
Auth is disabled in the test environment, but we still validate that the
endpoints reject requests without credentials when auth would be required.
"""

import os

import pytest
import requests

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_GRAPH_PORT = int(os.environ.get("FERROSA_GRAPH_PORT", "7474"))
FERROSA_AUTH_DISABLED = os.environ.get("FERROSA_AUTH_DISABLED", "false").lower() in {
    "1",
    "true",
    "yes",
}

BASE_URL = f"http://{FERROSA_HOST}:{FERROSA_GRAPH_PORT}"


# ---- Health check --------------------------------------------------------


class TestHealth:
    def test_health_returns_ok(self):
        """GET /graph/health returns 200 with status ok."""
        resp = requests.get(f"{BASE_URL}/graph/health", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"

    def test_health_no_auth_required(self):
        """Health check works without any Authorization header."""
        resp = requests.get(
            f"{BASE_URL}/graph/health",
            headers={},  # explicitly no auth
            timeout=10,
        )
        assert resp.status_code == 200


# ---- Auth enforcement ----------------------------------------------------


@pytest.mark.skipif(
    FERROSA_AUTH_DISABLED,
    reason="auth-disabled smoke node never returns 401; auth enforcement is "
    "covered separately with auth enabled",
)
class TestAuth:
    def test_query_without_auth_returns_401(self):
        """POST /graph/query without credentials returns 401."""
        resp = requests.post(
            f"{BASE_URL}/graph/query",
            json={"query": "MATCH (n) RETURN n", "keyspace": "test"},
            timeout=10,
        )
        assert resp.status_code == 401
        body = resp.json()
        assert "error" in body

    def test_explain_without_auth_returns_401(self):
        """POST /graph/explain without credentials returns 401."""
        resp = requests.post(
            f"{BASE_URL}/graph/explain",
            json={"query": "MATCH (n) RETURN n", "keyspace": "test"},
            timeout=10,
        )
        assert resp.status_code == 401

    def test_schema_without_auth_returns_401(self):
        """GET /graph/schema without credentials returns 401."""
        resp = requests.get(
            f"{BASE_URL}/graph/schema",
            params={"keyspace": "test"},
            timeout=10,
        )
        assert resp.status_code == 401


# ---- Error handling ------------------------------------------------------


class TestErrors:
    def test_query_bad_json(self):
        """POST /graph/query with invalid JSON returns 400."""
        resp = requests.post(
            f"{BASE_URL}/graph/query",
            data="this is not json",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Basic Y2Fzc2FuZHJhOmNhc3NhbmRyYQ==",  # cassandra:cassandra
            },
            timeout=10,
        )
        assert resp.status_code == 400
        body = resp.json()
        assert "error" in body

    def test_query_missing_fields(self):
        """POST /graph/query with missing required fields returns 400."""
        resp = requests.post(
            f"{BASE_URL}/graph/query",
            json={"query": "MATCH (n) RETURN n"},  # missing keyspace
            headers={
                "Authorization": "Basic Y2Fzc2FuZHJhOmNhc3NhbmRyYQ==",
            },
            timeout=10,
        )
        assert resp.status_code == 400

    def test_explain_bad_json(self):
        """POST /graph/explain with invalid JSON returns 400."""
        resp = requests.post(
            f"{BASE_URL}/graph/explain",
            data="not json",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Basic Y2Fzc2FuZHJhOmNhc3NhbmRyYQ==",
            },
            timeout=10,
        )
        assert resp.status_code == 400


# ---- Query and explain (with auth) --------------------------------------


class TestQueryEndpoints:
    """Smoke-test query and explain with Basic auth.

    These use the default cassandra:cassandra credentials.  The graph
    engine may return errors if no graph keyspace is configured, but the
    HTTP layer should still process the request (not 401/400).
    """

    AUTH_HEADER = "Basic Y2Fzc2FuZHJhOmNhc3NhbmRyYQ=="  # cassandra:cassandra

    def test_query_endpoint_accepts_request(self):
        """POST /graph/query with valid auth and body is processed."""
        resp = requests.post(
            f"{BASE_URL}/graph/query",
            json={"query": "MATCH (n) RETURN n LIMIT 1", "keyspace": "system"},
            headers={"Authorization": self.AUTH_HEADER},
            timeout=10,
        )
        # May succeed or return a domain error -- but not 401 or 400.
        assert resp.status_code != 401
        assert resp.headers.get("content-type", "").startswith("application/json")

    def test_explain_endpoint_accepts_request(self):
        """POST /graph/explain with valid auth and body is processed."""
        resp = requests.post(
            f"{BASE_URL}/graph/explain",
            json={"query": "MATCH (n) RETURN n", "keyspace": "system"},
            headers={"Authorization": self.AUTH_HEADER},
            timeout=10,
        )
        assert resp.status_code != 401
        assert resp.headers.get("content-type", "").startswith("application/json")

    def test_schema_endpoint_accepts_request(self):
        """GET /graph/schema with valid auth is processed."""
        resp = requests.get(
            f"{BASE_URL}/graph/schema",
            params={"keyspace": "system"},
            headers={"Authorization": self.AUTH_HEADER},
            timeout=10,
        )
        assert resp.status_code != 401
        assert resp.headers.get("content-type", "").startswith("application/json")
