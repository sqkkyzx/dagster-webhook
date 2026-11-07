"""Validated connection settings for one Dagster deployment.

Constructing a client does not make a request. Each request must own its client
because the synchronous gql transport carries mutable connection state.
"""

import ipaddress
import json
import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from dagster_graphql import DagsterGraphQLClient
from gql.transport.requests import RequestsHTTPTransport

_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def _positive_number(value: str | float, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return number


def _port(value: str | int, name: str) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer between 1 and 65535") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} must be an integer between 1 and 65535")
    return port


@dataclass(frozen=True)
class ConnectionSettings:
    host: str = "webserver"
    port: int | None = 3000
    use_https: bool = False
    path_prefix: str = ""
    timeout: float = 120
    readiness_timeout: float = 3
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    ca_bundle: str | None = None

    @classmethod
    def from_env(
        cls,
        *,
        fallback_host: str = "webserver",
        fallback_port: int = 3000,
        fallback_timeout: float = 120,
    ) -> "ConnectionSettings":
        """Read settings at call time; an explicit URL supersedes host and port.

        A final ``/graphql`` in DAGSTER_URL is accepted and normalized to the
        deployment base URL. Userinfo, query parameters and fragments are never
        accepted; upstream credentials belong in DAGSTER_HEADERS_JSON.
        """
        timeout = _positive_number(
            os.getenv("DAGSTER_TIMEOUT", str(fallback_timeout)), "DAGSTER_TIMEOUT"
        )
        readiness_timeout = _positive_number(
            os.getenv("DAGSTER_READINESS_TIMEOUT", "3"),
            "DAGSTER_READINESS_TIMEOUT",
        )
        configured_url = os.getenv("DAGSTER_URL", "").strip()
        if configured_url:
            try:
                parsed = urlsplit(configured_url)
                port = parsed.port
            except ValueError as exc:
                raise ValueError(
                    "DAGSTER_URL is not a valid HTTP(S) deployment URL"
                ) from exc
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
                or any(char.isspace() for char in configured_url)
                or "\\" in configured_url
            ):
                raise ValueError(
                    "DAGSTER_URL must be an HTTP(S) URL without credentials, query or fragment"
                )
            if port is not None:
                port = _port(port, "DAGSTER_URL port")
            host = parsed.hostname
            use_https = parsed.scheme == "https"
            prefix = parsed.path.rstrip("/")
            prefix = prefix.removesuffix("/graphql")
        else:
            host = os.getenv("DAGSTER_HOST", fallback_host).strip()
            if (
                not host
                or any(char in host for char in "/@?#\\")
                or any(char.isspace() for char in host)
            ):
                raise ValueError(
                    "DAGSTER_HOST must be a hostname or IP without a URL path"
                )
            # Accept bracketed IPv6 hosts for compatibility with SDK URL building.
            host = host.removeprefix("[").removesuffix("]")
            if not host:
                raise ValueError("DAGSTER_HOST must be a non-empty hostname or IP")
            if ":" in host:
                try:
                    ipaddress.IPv6Address(host)
                except ValueError as exc:
                    raise ValueError(
                        "DAGSTER_HOST must not include a port; use DAGSTER_PORT"
                    ) from exc
            port = _port(os.getenv("DAGSTER_PORT", str(fallback_port)), "DAGSTER_PORT")
            use_https = False
            prefix = ""

        try:
            headers = json.loads(os.getenv("DAGSTER_HEADERS_JSON", "{}"))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "DAGSTER_HEADERS_JSON must be a JSON object of string headers"
            ) from exc
        if not isinstance(headers, dict) or any(
            not isinstance(name, str)
            or not _HEADER_NAME.fullmatch(name)
            or not isinstance(value, str)
            or "\r" in value
            or "\n" in value
            for name, value in headers.items()
        ):
            raise ValueError(
                "DAGSTER_HEADERS_JSON must contain valid string HTTP headers"
            )

        ca_bundle = os.getenv("DAGSTER_CA_BUNDLE", "").strip() or None
        if ca_bundle:
            certificate = Path(ca_bundle).expanduser()
            if not certificate.is_file():
                raise ValueError(
                    "DAGSTER_CA_BUNDLE must point to an existing CA bundle file"
                )
            ca_bundle = str(certificate)
        return cls(
            host=host,
            port=port,
            use_https=use_https,
            path_prefix=prefix,
            timeout=timeout,
            readiness_timeout=readiness_timeout,
            headers=headers,
            ca_bundle=ca_bundle,
        )

    @property
    def graphql_url(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        authority = f"{host}:{self.port}" if self.port is not None else host
        return f"{'https' if self.use_https else 'http'}://{authority}{self.path_prefix}/graphql"

    def create_client(
        self, *, timeout_override: float | None = None
    ) -> DagsterGraphQLClient:
        """Build an isolated SDK client with TLS verification enabled."""
        timeout = (
            self.timeout
            if timeout_override is None
            else _positive_number(timeout_override, "timeout_override")
        )
        transport = RequestsHTTPTransport(
            url=self.graphql_url,
            use_json=True,
            timeout=timeout,
            headers=self.headers,
            verify=self.ca_bundle or True,
        )
        host = f"[{self.host}]" if ":" in self.host else self.host
        return DagsterGraphQLClient(
            host,
            port_number=self.port,
            transport=transport,
            use_https=self.use_https,
            timeout=timeout,
            path_prefix=self.path_prefix,
        )
