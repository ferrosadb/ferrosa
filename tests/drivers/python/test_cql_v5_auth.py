"""Authenticated native-protocol v5 regression using the real Python driver."""

import os

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def test_seed_admin_connects_over_v5():
    """AUTH_RESPONSE and AUTH_SUCCESS switch to checksummed v5 framing."""
    cluster = Cluster(
        contact_points=[os.environ.get("FERROSA_HOST", "127.0.0.1")],
        port=int(os.environ.get("FERROSA_CQL_PORT", "9042")),
        protocol_version=5,
        auth_provider=PlainTextAuthProvider(
            username="ferrosa_admin",
            password="ferrosa_admin",
        ),
        compression=False,
        schema_metadata_enabled=False,
        token_metadata_enabled=False,
    )
    session = None
    try:
        session = cluster.connect()
        row = session.execute(
            "SELECT cluster_name FROM system.local"
        ).one()
        assert row is not None
        assert row.cluster_name
    finally:
        if session is not None:
            session.shutdown()
        cluster.shutdown()
