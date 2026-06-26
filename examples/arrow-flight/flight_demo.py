#!/usr/bin/env python3
"""Runnable Arrow Flight client for ferrosa — handshake, GetFlightInfo, DoGet.

Run `schema.cql` over CQL first to create flight_demo.sensor_readings.

    pip install pyarrow
    python flight_demo.py            # defaults below
    python flight_demo.py --flight grpc://127.0.0.1:18815  # mapped test port

ferrosa's Flight handshake is intentionally simple and NOT pyarrow's basic-token
flow: the client sends one HandshakeRequest whose payload is `username\\0password`
and reads the signed token from the response payload, then sends
`authorization: Bearer <token>` on every subsequent RPC.
"""
import argparse
import sys

import pyarrow as pa
import pyarrow.flight as flight


class _Handshake(flight.ClientAuthHandler):
    """Sends `username\\0password`; captures the token from the response payload."""

    def __init__(self, user: str, password: str):
        self._creds = user.encode() + b"\x00" + password.encode()
        self.token = b""

    def authenticate(self, outgoing, incoming):
        outgoing.write(self._creds)
        self.token = incoming.read()

    def get_token(self):
        return self.token


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--flight", default="grpc://127.0.0.1:8815")
    ap.add_argument("--user", default="cassandra")
    ap.add_argument("--password", default="cassandra")
    ap.add_argument("--keyspace", default="flight_demo")
    ap.add_argument("--table", default="sensor_readings")
    args = ap.parse_args()

    client = flight.FlightClient(args.flight)

    # 1. Handshake -> bearer token, then attach it to every call.
    hs = _Handshake(args.user, args.password)
    client.authenticate(hs)
    opts = flight.FlightCallOptions(headers=[(b"authorization", b"Bearer " + hs.token)])
    print(f"[flight] authenticated, token {len(hs.token)} bytes")

    # 2. GetFlightInfo for a SELECT, then DoGet each endpoint as Arrow.
    cmd = f"SELECT sensor_id, ts, temperature, humidity FROM {args.keyspace}.{args.table}".encode()
    info = client.get_flight_info(flight.FlightDescriptor.for_command(cmd), opts)
    print(f"[flight] GetFlightInfo: {len(info.endpoints)} endpoint(s), columns {info.schema.names}")
    total = 0
    for endpoint in info.endpoints:
        table = client.do_get(endpoint.ticket, opts).read_all()
        total += table.num_rows
        for row in table.to_pylist():
            print(f"[flight] row: {row}")
    print(f"[flight] read {total} rows over Arrow Flight")
    return 0


if __name__ == "__main__":
    sys.exit(main())
