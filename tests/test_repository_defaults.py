import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import main


def discovery_result(*repositories):
    return {
        "repositoriesOrError": {
            "__typename": "RepositoryConnection",
            "nodes": [
                {
                    "name": repo,
                    "location": {"name": location},
                    "jobs": [{"name": job, "tags": []}],
                }
                for location, repo, job in repositories
            ],
        }
    }


@pytest.mark.parametrize(
    "repository,expected",
    [
        ("omitted", "__repository__"),
        (None, "__repository__"),
        ("custom_repository", "custom_repository"),
        ("  custom_repository  ", "custom_repository"),
    ],
)
def test_rpc_submission_resolves_repository_without_contacting_dagster(
    client, sdk_client, auth_headers, repository, expected
):
    payload = {"job_name": "daily_job", "repository_location_name": "my_code"}
    if repository != "omitted":
        payload["repository_name"] = repository
    response = client.post("/api/v1/submit_job", headers=auth_headers, json=payload)
    assert response.status_code == 200
    selector = sdk_client._execute.call_args.args[1]["params"]["selector"]
    assert selector["repositoryName"] == expected
    assert selector["repositoryLocationName"] == "my_code"


def test_blank_repository_is_rejected(client, sdk_client, auth_headers):
    response = client.post(
        "/api/v1/submit_job",
        headers=auth_headers,
        json={"job_name": "daily_job", "repository_name": "   "},
    )
    assert response.status_code == 422
    sdk_client.submit_job_execution.assert_not_called()


@pytest.mark.parametrize("repository", [None, "requested_repository"])
def test_missing_location_never_falls_back_to_another_repository(
    client, sdk_client, auth_headers, repository
):
    sdk_client._execute.return_value = discovery_result(
        ("other_location", "other_repository", "daily_job")
    )
    payload = {"job_name": "daily_job"}
    if repository is not None:
        payload["repository_name"] = repository
    response = client.post("/api/v1/submit_job", headers=auth_headers, json=payload)
    assert response.status_code == 404
    sdk_client.submit_job_execution.assert_not_called()


def test_location_is_resolved_within_requested_repository(
    client, sdk_client, auth_headers
):
    discovery = discovery_result(
        ("wrong", "other_repository", "daily_job"),
        ("right", "requested_repository", "daily_job"),
    )
    sdk_client._execute.side_effect = [discovery, sdk_client._execute.return_value]
    response = client.post(
        "/api/v1/submit_job",
        headers=auth_headers,
        json={"job_name": "daily_job", "repository_name": "requested_repository"},
    )
    assert response.status_code == 200
    data = sdk_client._execute.call_args.args[1]["params"]["selector"]
    assert data["repositoryLocationName"] == "right"
    assert data["repositoryName"] == "requested_repository"


def test_multiple_locations_require_explicit_choice(client, sdk_client, auth_headers):
    sdk_client._execute.return_value = discovery_result(
        ("first", "__repository__", "daily_job"),
        ("second", "__repository__", "daily_job"),
    )
    response = client.post(
        "/api/v1/submit_job", headers=auth_headers, json={"job_name": "daily_job"}
    )
    assert response.status_code == 409
    assert response.json()["code"] == "JOB_SELECTION_AMBIGUOUS"
    assert [row["repository_location_name"] for row in response.json()["details"]] == [
        "first",
        "second",
    ]
    sdk_client.submit_job_execution.assert_not_called()


def test_rpc_rejects_reserved_source_tag_before_contacting_upstream(
    client, sdk_client, auth_headers
):
    response = client.post(
        "/api/v1/submit_job",
        headers=auth_headers,
        json={
            "job_name": "daily_job",
            "tags": {".dagster/repository": "wrong@location"},
        },
    )
    assert response.status_code == 422
    assert response.json()["details"][0]["path"] == ["body", "tags"]
    sdk_client._execute.assert_not_called()
    sdk_client.submit_job_execution.assert_not_called()


def test_repository_default_is_documented(client):
    schema = client.get("/openapi.json").json()
    field = schema["components"]["schemas"]["SubmitJobRequest"]["properties"][
        "repository_name"
    ]
    assert field["default"] == main.DEFAULT_REPOSITORY_NAME == "__repository__"
    assert "DAGSTER_REPOSITORY_NAME" in field["description"]
    assert (
        "repository_name"
        not in schema["components"]["schemas"]["SubmitJobRequest"]["required"]
    )


def test_configured_default_is_reflected_in_validation_and_openapi():
    env = {
        **os.environ,
        "DAGSTER_REPOSITORY_NAME": "custom_repository",
        "APP_API_TOKEN": "unit-test-token",
    }
    env.pop("APP_API_KEYS_JSON", None)
    script = """
import json
import main
payload = main.SubmitJobRequest(job_name='daily_job')
explicit_null = main.SubmitJobRequest(job_name='daily_job', repository_name=None)
field = main.app.openapi()['components']['schemas']['SubmitJobRequest']['properties']['repository_name']
print(json.dumps([payload.repository_name, explicit_null.repository_name, field['default']]))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout) == ["custom_repository"] * 3
