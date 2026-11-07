"""Exercise the in-network HTTP transport without requiring a Docker daemon."""

import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock
from urllib.error import URLError

import pytest

from integration import http_probe
from integration.check import Environment


@pytest.fixture
def fixture_http_server(monkeypatch):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            if self.path == "/redoc":
                content, status, kind = b"<html>ReDoc</html>", 200, "text/html"
            else:
                content = json.dumps({"path": self.path}).encode()
                status = 403 if self.path == "/denied" else 200
                kind = "application/json"
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.end_headers()
            self.wfile.write(content)

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {
                        "payload": json.loads(body),
                        "authorization": self.headers["Authorization"],
                    }
                ).encode()
            )

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        monkeypatch.setitem(
            http_probe.SERVICE_URLS, "gateway", f"http://127.0.0.1:{server.server_port}"
        )
        yield server
        server.shutdown()
        thread.join(timeout=2)


def test_probe_preserves_http_errors_json_and_document_html(fixture_http_server):
    assert http_probe.perform({"service": "gateway", "path": "/denied"}) == {
        "status": 403,
        "body": {"path": "/denied"},
    }
    assert http_probe.perform({"service": "gateway", "path": "/redoc"}) == {
        "status": 200,
        "body": "<html>ReDoc</html>",
    }
    assert http_probe.perform(
        {
            "service": "gateway",
            "path": "/api/v1/runs",
            "method": "POST",
            "payload": {"job_id": "fixture"},
            "headers": {"Authorization": "Bearer fixture-token"},
        }
    ) == {
        "status": 201,
        "body": {
            "payload": {"job_id": "fixture"},
            "authorization": "Bearer fixture-token",
        },
    }


def test_probe_returns_transport_errors_and_rejects_other_services(fixture_http_server):
    # Release the fixture socket, then connect to its now-closed local port.
    fixture_http_server.shutdown()
    fixture_http_server.server_close()
    assert "transport_error" in http_probe.perform(
        {"service": "gateway", "path": "/ready"}
    )
    with pytest.raises(ValueError, match="Unknown acceptance service"):
        http_probe.perform({"service": "business-server", "path": "/ready"})


def test_docker_requests_use_proxy_exec_and_stdin_without_host_published_ports(
    tmp_path,
):
    environment = Environment("docker", "python", "fixture:built", tmp_path)
    environment.compose = Mock(
        return_value=subprocess.CompletedProcess(
            [],
            0,
            stdout=json.dumps({"status": 200, "body": {"items": []}}),
        )
    )
    assert environment.request(
        environment.gateway_url + "/api/v1/runs?limit=1",
        headers={"Authorization": "Bearer fixture-token"},
    ) == (200, {"items": []})
    args, kwargs = environment.compose.call_args
    assert args == ("exec", "-T", "proxy", "python", "/fixture/http_probe.py")
    assert json.loads(kwargs["input"]) == {
        "service": "gateway",
        "path": "/api/v1/runs?limit=1",
        "method": "GET",
        "payload": None,
        "headers": {"Authorization": "Bearer fixture-token"},
    }
    assert kwargs["capture_output"] is True
    environment.compose.return_value.stdout = json.dumps(
        {"transport_error": "connection refused"}
    )
    with pytest.raises(URLError):
        environment.request(environment.proxy_url + "/__acceptance/state")
    environment.compose.side_effect = subprocess.CalledProcessError(
        1, ["docker", "compose"]
    )
    with pytest.raises(URLError):
        environment.request(environment.gateway_url + "/ready")
