"""Perform one acceptance HTTP request inside the isolated Docker network.

Input and output use JSON over stdin/stdout. Only the two fixture services can
be selected; there is no arbitrary upstream URL or Docker socket in this image.
"""

import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

SERVICE_URLS = {"gateway": "http://gateway:8080", "proxy": "http://127.0.0.1:3001"}


def perform(message):
    service = message["service"]
    if service not in SERVICE_URLS:
        raise ValueError("Unknown acceptance service")
    path = message["path"]
    if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
        raise ValueError("An absolute fixture path is required")
    payload = message.get("payload")
    body = None if payload is None else json.dumps(payload).encode()
    req = Request(
        SERVICE_URLS[service] + path,
        body,
        {"Content-Type": "application/json", **message.get("headers", {})},
        method=message.get("method", "GET"),
    )
    opener = build_opener(ProxyHandler({}))
    try:
        try:
            response = opener.open(req, timeout=30)
        except HTTPError as exc:
            response = exc
        with response:
            data = response.read().decode()
            if "application/json" in response.headers.get("Content-Type", ""):
                data = json.loads(data)
            return {"status": response.status, "body": data}
    except (URLError, ConnectionError, TimeoutError) as exc:
        return {"transport_error": str(exc)}


if __name__ == "__main__":
    print(json.dumps(perform(json.load(sys.stdin))))
