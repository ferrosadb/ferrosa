"""Exercise the benchmark's SPARQL HTTP contract through a real socket.

Correctness: requests use the selected endpoint, preserve RDF values, reuse
connections, and reject HTTP failures or ambiguous records.
Last revised: 2026-10-26
Last changed: Add wire-level contracts for the Ferrosa/Oxigraph comparison.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest


SOURCE = (
    Path(__file__).resolve().parents[2]
    / "deploy/fly-bench/pipeline/validated_multimetric_bench.py"
)
SPEC = importlib.util.spec_from_file_location("sparql_benchmark", SOURCE)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


def binding(value):
    return {"type": "literal", "value": str(value)}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"])).decode()
        self.server.requests.append(
            (self.path, self.headers["Content-Type"], body, self.client_address[1])
        )
        update = self.headers["Content-Type"] == "application/sparql-update"
        code = self.server.code or (204 if update else 200)
        data = (
            b""
            if code == 204
            else json.dumps({"results": {"bindings": self.server.rows}}).encode()
        )
        self.send_response(code)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args):
        pass


class SparqlBenchmarkAdapter(unittest.TestCase):
    def setUp(self):
        adapter_type = BENCH.SparqlAdapter
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.requests = []
        self.server.rows = [
            {"pk": binding(7), "bucket": binding(7), "payload": binding("payload")}
        ]
        self.server.code = None
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.args = SimpleNamespace(
            host="127.0.0.1",
            port=self.server.server_port,
            sparql_backend="ferrosa",
            user=None,
            password=None,
        )
        self.adapter = adapter_type(self.args)

    def tearDown(self):
        self.adapter.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_point_read_validates_the_record_and_uses_the_query_endpoint(self):
        self.assertEqual(self.adapter.point_read(7), (7, 7, "payload"))
        path, content_type, body, _port = self.server.requests[-1]
        self.assertEqual(path, "/sparql")
        self.assertEqual(content_type, "application/sparql-query")
        self.assertIn("urn:ferrosa:benchmark:row:7", body)

    def test_oxigraph_uses_its_update_endpoint(self):
        self.adapter.close()
        self.args.sparql_backend = "oxigraph"
        self.adapter = BENCH.SparqlAdapter(self.args)
        self.adapter.ensure_schema()
        self.assertEqual(self.server.requests[-1][0], "/update")
        self.assertIn("CLEAR SILENT GRAPH", self.server.requests[-1][2])
        self.adapter.point_read(7)
        self.assertEqual(self.server.requests[-1][0], "/query")

    def test_http_errors_abort_the_run(self):
        self.server.code = 400
        with self.assertRaisesRegex(RuntimeError, "400"):
            self.adapter.point_read(7)

    def test_multivalued_records_cannot_pass_as_point_reads(self):
        self.server.rows *= 2
        with self.assertRaises(ValueError):
            self.adapter.point_read(7)

    def test_payloads_are_escaped_and_loaded_into_the_isolated_graph(self):
        self.adapter.load_range(0, 1, ['quote" slash\\ newline\n'])
        path, content_type, body, _port = self.server.requests[-1]
        self.assertEqual(path, "/sparql/update")
        self.assertEqual(content_type, "application/sparql-update")
        self.assertIn(json.dumps('quote" slash\\ newline\n'), body)
        self.assertIn("GRAPH <urn:ferrosa:benchmark>", body)

    def test_repeated_reads_reuse_the_connection(self):
        self.adapter.point_read(7)
        self.adapter.point_read(7)
        self.assertEqual(len({request[3] for request in self.server.requests}), 1)

    def test_count_is_a_scalar_over_the_logical_records(self):
        self.server.rows = [{"c": binding(20)}]
        self.assertEqual(self.adapter.count(), 20)
        self.assertIn("COUNT(*)", self.server.requests[-1][2])


if __name__ == "__main__":
    unittest.main()
