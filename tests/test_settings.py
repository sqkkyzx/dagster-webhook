"""Connection tests construct transports but never contact Dagster."""

from unittest.mock import Mock

import pytest

from gateway.settings import ConnectionSettings


@pytest.fixture(autouse=True)
def clean_connection_environment(monkeypatch):
    for name in (
        "DAGSTER_URL",
        "DAGSTER_HOST",
        "DAGSTER_PORT",
        "DAGSTER_TIMEOUT",
        "DAGSTER_READINESS_TIMEOUT",
        "DAGSTER_HEADERS_JSON",
        "DAGSTER_CA_BUNDLE",
    ):
        monkeypatch.delenv(name, raising=False)


def test_legacy_host_settings_are_read_at_call_time(monkeypatch):
    assert ConnectionSettings.from_env().graphql_url == "http://webserver:3000/graphql"
    first = ConnectionSettings.from_env(
        fallback_host="patched-host", fallback_port=4000
    )
    assert first.graphql_url == "http://patched-host:4000/graphql"
    monkeypatch.setenv("DAGSTER_HOST", "new-host")
    monkeypatch.setenv("DAGSTER_PORT", "3100")
    monkeypatch.setenv("DAGSTER_TIMEOUT", "5.5")
    second = ConnectionSettings.from_env()
    assert second.graphql_url == "http://new-host:3100/graphql"
    assert second.timeout == 5.5


@pytest.mark.parametrize("host", ["localhost:3000", "[]", "https://host"])
def test_legacy_host_does_not_silently_accept_a_url_or_port(monkeypatch, host):
    monkeypatch.setenv("DAGSTER_HOST", host)
    with pytest.raises(ValueError, match="DAGSTER_HOST"):
        ConnectionSettings.from_env()


def test_legacy_ipv6_host_is_valid(monkeypatch):
    monkeypatch.setenv("DAGSTER_HOST", "[::1]")
    assert ConnectionSettings.from_env().graphql_url == "http://[::1]:3000/graphql"


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://dagster.example.test", "https://dagster.example.test/graphql"),
        (
            "https://dagster.example.test/dagster/",
            "https://dagster.example.test/dagster/graphql",
        ),
        (
            "https://dagster.example.test/dagster/graphql",
            "https://dagster.example.test/dagster/graphql",
        ),
        ("http://localhost:3100/graphql/", "http://localhost:3100/graphql"),
        ("https://[::1]:3443/dagster", "https://[::1]:3443/dagster/graphql"),
    ],
)
def test_url_and_path_prefix_are_normalized(monkeypatch, url, expected):
    monkeypatch.setenv("DAGSTER_URL", url)
    monkeypatch.setenv("DAGSTER_HOST", "ignored")
    monkeypatch.setenv("DAGSTER_PORT", "invalid-but-ignored")
    assert ConnectionSettings.from_env().graphql_url == expected


@pytest.mark.parametrize(
    "url",
    [
        "ftp://host/path",
        "https:///missing-host",
        "https://user:password@host",
        "https://host/path?token=hidden",
        "https://host/#fragment",
        "https://host:0",
        "https://host:65536",
        "https://host:invalid",
        "http://host name",
        "https://host\\path",
    ],
)
def test_invalid_upstream_url_is_rejected(monkeypatch, url):
    monkeypatch.setenv("DAGSTER_URL", url)
    with pytest.raises(ValueError, match="DAGSTER_URL"):
        ConnectionSettings.from_env()


@pytest.mark.parametrize(
    "raw",
    [
        "[]",
        '{"Authorization": 7}',
        '{"Bad Header":"value"}',
        '{"X-Test":"bad\\nvalue"}',
        "not-json",
    ],
)
def test_invalid_header_config_is_rejected_without_echoing_secrets(monkeypatch, raw):
    monkeypatch.setenv("DAGSTER_HEADERS_JSON", raw)
    with pytest.raises(ValueError, match="DAGSTER_HEADERS_JSON") as failure:
        ConnectionSettings.from_env()
    assert raw not in str(failure.value)


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "invalid"])
@pytest.mark.parametrize("variable", ["DAGSTER_TIMEOUT", "DAGSTER_READINESS_TIMEOUT"])
def test_timeout_settings_require_positive_finite_numbers(monkeypatch, variable, value):
    monkeypatch.setenv(variable, value)
    with pytest.raises(ValueError, match=variable):
        ConnectionSettings.from_env()


def test_ca_bundle_must_exist(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_CA_BUNDLE", str(tmp_path / "missing.pem"))
    with pytest.raises(ValueError, match="DAGSTER_CA_BUNDLE"):
        ConnectionSettings.from_env()


def test_client_uses_https_auth_ca_and_short_readiness_timeout(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("test CA data; no network use")
    monkeypatch.setenv("DAGSTER_URL", "https://dagster.example.test/prefix")
    monkeypatch.setenv(
        "DAGSTER_HEADERS_JSON", '{"Authorization":"Bearer test-upstream-token"}'
    )
    monkeypatch.setenv("DAGSTER_CA_BUNDLE", str(ca))
    settings = ConnectionSettings.from_env()
    constructor = Mock()
    monkeypatch.setattr("gateway.settings.DagsterGraphQLClient", constructor)
    settings.create_client(timeout_override=settings.readiness_timeout)
    kwargs = constructor.call_args.kwargs
    transport = kwargs["transport"]
    assert transport.url == "https://dagster.example.test/prefix/graphql"
    assert transport.default_timeout == 3
    assert transport.headers == {"Authorization": "Bearer test-upstream-token"}
    assert transport.verify == str(ca)
    assert kwargs["use_https"] is True
    assert kwargs["path_prefix"] == "/prefix"


def test_default_tls_verification_remains_enabled(monkeypatch):
    constructor = Mock()
    monkeypatch.setattr("gateway.settings.DagsterGraphQLClient", constructor)
    ConnectionSettings.from_env().create_client()
    assert constructor.call_args.kwargs["transport"].verify is True
