"""Acceptance-only proxy: drop a launch response after Dagster has accepted it.

Never package this control surface in the application image. Its only upstream
is the disposable fixture process/container created by integration/check.py.
"""

import argparse
import json
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener


def serve(host: str, port: int, upstream: str):
    lock = threading.Lock()
    state = {"drop_next_submit": False, "submissions": 0, "dropped": 0}
    opener = build_opener(ProxyHandler({}))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def reply(self, status, value):
            data = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/__acceptance/state":
                with lock:
                    self.reply(200, dict(state))
            else:
                self.reply(404, {"error": "fixture route not found"})

        def do_POST(self):
            if self.path == "/__acceptance/drop-next-submit":
                with lock:
                    state["drop_next_submit"] = True
                self.reply(200, {"armed": True})
                return
            if self.path != "/graphql":
                self.reply(404, {"error": "fixture route not found"})
                return
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            payload = json.loads(body)
            is_submission = "mutation GatewaySubmit" in payload.get("query", "")
            with lock:
                if is_submission:
                    state["submissions"] += 1
                drop = is_submission and state["drop_next_submit"]
                if drop:
                    state["drop_next_submit"] = False
            request = Request(
                upstream + "/graphql", body, {"Content-Type": "application/json"}
            )
            try:
                with opener.open(request, timeout=60) as response:
                    result, status = response.read(), response.status
            except HTTPError as exc:
                result, status = exc.read(), exc.code
            except URLError as exc:
                self.reply(502, {"error": str(exc.reason)})
                return
            if is_submission and json.loads(result).get("errors"):
                print(
                    "Fixture GraphQL rejected submission:",
                    result.decode(),
                    file=sys.stderr,
                    flush=True,
                )
            # The real launch has completed before losing the response. Assert
            # success first so a fixture/config failure cannot simulate success.
            accepted = (
                (
                    (json.loads(result).get("data") or {})
                    .get("launchRun", {})
                    .get("__typename")
                    == "LaunchRunSuccess"
                )
                if drop
                else False
            )
            if drop and accepted:
                with lock:
                    state["dropped"] += 1
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(result)))
            self.end_headers()
            self.wfile.write(result)

    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    args = parser.parse_args()
    serve(args.host, args.port, args.upstream)
