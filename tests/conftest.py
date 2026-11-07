"""API tests use a fixed local token and never contact a deployed Dagster."""

import os
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("APP_API_TOKEN", "test-api-token")

import main as gateway


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(gateway, "API_TOKEN", "test-api-token")
    previous_overrides = gateway.app.dependency_overrides.copy()
    gateway.app.dependency_overrides.clear()
    yield gateway.app
    gateway.app.dependency_overrides.clear()
    gateway.app.dependency_overrides.update(previous_overrides)


@pytest.fixture
def client(app):
    with TestClient(app, raise_server_exceptions=False) as api:
        yield api


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer test-api-token"}


@pytest.fixture
def sdk_client(app):
    sdk = Mock(spec=gateway.DagsterGraphQLClient)
    sdk._execute.return_value = {
        "launchRun": {
            "__typename": "LaunchRunSuccess",
            "run": {"runId": "mocked-run", "jobName": "daily_job", "status": "QUEUED"},
        }
    }
    app.dependency_overrides[gateway.get_dagster_client] = lambda: sdk
    return sdk
