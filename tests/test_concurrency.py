"""Exercise the real Dagster/gql transport against a local GraphQL endpoint."""

import json
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Barrier, BrokenBarrierError, Lock, Thread

import main as gateway


def test_simultaneous_requests_use_independent_graphql_transports(
    client, auth_headers, monkeypatch
):
    both_requests_arrived = Barrier(2)
    seen_run_ids = []
    lock = Lock()

    class GraphQLHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with lock:
                seen_run_ids.append(body["variables"]["runId"])
            # With the previous shared client, the second request fails before
            # reaching this server. The timeout keeps that regression bounded.
            try:
                both_requests_arrived.wait(timeout=3)
            except BrokenBarrierError:
                pass
            data = json.dumps(
                {
                    "data": {
                        "pipelineRunOrError": {"__typename": "Run", "status": "SUCCESS"}
                    }
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), GraphQLHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(gateway, "DAGSTER_HOST", "127.0.0.1")
    monkeypatch.setattr(gateway, "DAGSTER_PORT", server.server_port)
    monkeypatch.setattr(gateway, "DAGSTER_TIMEOUT", 10)

    def request_status(run_id):
        return client.get(
            "/api/v1/get_run_status", params={"run_id": run_id}, headers=auth_headers
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(request_status, ["first-run", "second-run"]))
        assert [response.status_code for response in responses] == [200, 200]
        assert {response.json()["run_id"] for response in responses} == {
            "first-run",
            "second-run",
        }
        assert all(response.json()["status"] == "SUCCESS" for response in responses)
        assert sorted(seen_run_ids) == ["first-run", "second-run"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
