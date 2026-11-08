"""Named Job selection discovers only missing parts of the exact identity."""

from unittest.mock import Mock

import pytest

from gateway.dagster_adapter import encode_job_id
from gateway.errors import GatewayHTTPError
from gateway.job_resolution import (
    get_default_repository_name,
    resolve_job_location,
    resolve_job_reference,
)


def job_reference(location="location", repository="custom", name="daily_job"):
    return {
        "job_id": encode_job_id(location, repository, name),
        "job_name": name,
        "repository_location_name": location,
        "repository_name": repository,
    }


def test_repository_default_is_read_on_each_call(monkeypatch):
    monkeypatch.delenv("DAGSTER_REPOSITORY_NAME", raising=False)
    assert get_default_repository_name() == "__repository__"
    monkeypatch.setenv("DAGSTER_REPOSITORY_NAME", "  custom_repository  ")
    assert get_default_repository_name() == "custom_repository"
    monkeypatch.setenv("DAGSTER_REPOSITORY_NAME", "  ")
    with pytest.raises(ValueError, match="DAGSTER_REPOSITORY_NAME"):
        get_default_repository_name()


def test_complete_job_reference_needs_no_discovery():
    adapter = Mock()
    assert (
        resolve_job_reference(adapter, " daily_job ", " location ", " custom ")
        == job_reference()
    )
    adapter.list_jobs.assert_not_called()


@pytest.mark.parametrize("configured_default", [None, "other", " "])
def test_unique_job_infers_its_actual_repository_and_location(
    monkeypatch, configured_default
):
    if configured_default is None:
        monkeypatch.delenv("DAGSTER_REPOSITORY_NAME", raising=False)
    else:
        monkeypatch.setenv("DAGSTER_REPOSITORY_NAME", configured_default)
    adapter = Mock()
    adapter.list_jobs.return_value = {"items": [job_reference()]}
    assert resolve_job_reference(adapter, "daily_job") == job_reference()
    adapter.list_jobs.assert_called_once_with(location_name=None, repository_name=None)


@pytest.mark.parametrize(
    "conditions,expected",
    [
        ({"repository_location_name": "chosen"}, job_reference("chosen", "custom")),
        ({"repository_name": "chosen"}, job_reference("remote", "chosen")),
    ],
)
def test_partial_reference_respects_every_supplied_condition(conditions, expected):
    adapter = Mock()
    adapter.list_jobs.return_value = {
        "items": [
            job_reference("other", "other"),
            job_reference("chosen", "custom", "another_job"),
            expected,
        ]
    }
    assert resolve_job_reference(adapter, "daily_job", **conditions) == expected
    adapter.list_jobs.assert_called_once_with(
        location_name=conditions.get("repository_location_name"),
        repository_name=conditions.get("repository_name"),
    )


@pytest.mark.parametrize(
    "conditions",
    [{}, {"repository_location_name": "location"}, {"repository_name": "custom"}],
)
def test_ambiguous_reference_returns_readable_candidates_without_internal_ids(
    conditions,
):
    adapter = Mock()
    candidates = (
        [job_reference("location", "custom"), job_reference("location", "other")]
        if "repository_location_name" in conditions
        else [job_reference("first", "custom"), job_reference("second", "custom")]
    )
    adapter.list_jobs.return_value = {"items": candidates}
    with pytest.raises(GatewayHTTPError) as error:
        resolve_job_reference(adapter, "daily_job", **conditions)
    assert error.value.status_code == 409
    assert error.value.code == "JOB_SELECTION_AMBIGUOUS"
    assert error.value.details == [
        {key: value for key, value in candidate.items() if key != "job_id"}
        for candidate in candidates
    ]


@pytest.mark.parametrize(
    "conditions",
    [{}, {"repository_location_name": "missing"}, {"repository_name": "missing"}],
)
def test_unmatched_reference_is_not_found(conditions):
    adapter = Mock()
    adapter.list_jobs.return_value = {"items": [job_reference(name="another_job")]}
    with pytest.raises(GatewayHTTPError) as error:
        resolve_job_reference(adapter, "daily_job", **conditions)
    assert error.value.status_code == 404
    assert error.value.code == "RESOURCE_NOT_FOUND"
    assert "job_id" not in error.value.details[0]


@pytest.mark.parametrize(
    "field", ["job_name", "repository_location_name", "repository_name"]
)
@pytest.mark.parametrize("value", ["", "  ", 7])
def test_explicit_invalid_reference_names_fail_before_discovery(field, value):
    adapter = Mock()
    arguments = {"job_name": "daily_job", field: value}
    with pytest.raises(GatewayHTTPError) as error:
        resolve_job_reference(adapter, **arguments)
    assert error.value.status_code == 422
    adapter.list_jobs.assert_not_called()


def test_legacy_location_resolution_remains_scoped_to_requested_repository():
    adapter = Mock()
    adapter.list_jobs.return_value = {
        "items": [job_reference("wrong", "other"), job_reference("right", "custom")]
    }
    assert resolve_job_location(adapter, "daily_job", "custom") == "right"
    adapter.list_jobs.assert_called_once_with(repository_name="custom")


def test_legacy_ambiguity_also_exposes_only_names():
    adapter = Mock()
    adapter.list_jobs.return_value = {
        "items": [job_reference("first"), job_reference("second")]
    }
    with pytest.raises(GatewayHTTPError) as error:
        resolve_job_location(adapter, "daily_job", "custom")
    assert error.value.status_code == 409
    assert all("job_id" not in candidate for candidate in error.value.details)
