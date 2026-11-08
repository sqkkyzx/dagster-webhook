"""Resolve named Job references using every explicitly supplied condition."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from gateway.errors import GatewayHTTPError

if TYPE_CHECKING:
    from gateway.dagster_adapter import DagsterAdapter


def get_default_repository_name() -> str:
    """Read the configured Repository default without capturing startup state."""
    repository = os.getenv("DAGSTER_REPOSITORY_NAME", "__repository__").strip()
    if not repository:
        raise ValueError("DAGSTER_REPOSITORY_NAME must be a non-empty Repository name")
    return repository


def _reference_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GatewayHTTPError(
            422,
            "REQUEST_VALIDATION_FAILED",
            f"{field} must be a non-empty name",
        )
    return value.strip()


def resolve_job_reference(
    adapter: DagsterAdapter,
    job_name: str,
    repository_location_name: str | None = None,
    repository_name: str | None = None,
) -> dict[str, str]:
    """Return the exact internal identity for an externally named Job."""
    # The adapter imports name validation for run filters. Importing this
    # encoder at call time keeps both modules independently importable.
    from gateway.dagster_adapter import encode_job_id

    job = _reference_name(job_name, "job_name")
    repository = (
        _reference_name(repository_name, "repository_name")
        if repository_name is not None
        else None
    )
    location = (
        _reference_name(repository_location_name, "repository_location_name")
        if repository_location_name is not None
        else None
    )
    if location is None or repository is None:
        candidates = [
            candidate
            for candidate in adapter.list_jobs(
                location_name=location, repository_name=repository
            )["items"]
            if candidate["job_name"] == job
            and (location is None or candidate["repository_location_name"] == location)
            and (repository is None or candidate["repository_name"] == repository)
        ]
        if not candidates:
            raise GatewayHTTPError(
                404,
                "RESOURCE_NOT_FOUND",
                "Job does not exist for the requested names",
                details=[
                    {
                        "job_name": job,
                        "repository_location_name": location,
                        "repository_name": repository,
                    }
                ],
            )
        if len(candidates) != 1:
            raise GatewayHTTPError(
                409,
                "JOB_SELECTION_AMBIGUOUS",
                "Narrow the selection with repository_location_name or repository_name",
                details=[
                    {
                        "job_name": candidate["job_name"],
                        "repository_location_name": candidate[
                            "repository_location_name"
                        ],
                        "repository_name": candidate["repository_name"],
                    }
                    for candidate in candidates
                ],
            )
        location = candidates[0]["repository_location_name"]
        repository = candidates[0]["repository_name"]
    return {
        "job_id": encode_job_id(location, repository, job),
        "job_name": job,
        "repository_location_name": location,
        "repository_name": repository,
    }


def resolve_job_location(
    adapter: DagsterAdapter, job_name: str, repository_name: str
) -> str:
    candidates = [
        job
        for job in adapter.list_jobs(repository_name=repository_name)["items"]
        if job["job_name"] == job_name and job["repository_name"] == repository_name
    ]
    if not candidates:
        raise GatewayHTTPError(
            404,
            "RESOURCE_NOT_FOUND",
            "Job does not exist in the requested repository",
            details=[{"job_name": job_name, "repository_name": repository_name}],
        )
    if len(candidates) != 1:
        raise GatewayHTTPError(
            409,
            "JOB_SELECTION_AMBIGUOUS",
            "Specify repository_location_name to select one of the matching jobs",
            details=[
                {
                    "job_name": job["job_name"],
                    "repository_name": job["repository_name"],
                    "repository_location_name": job["repository_location_name"],
                }
                for job in candidates
            ],
        )
    return candidates[0]["repository_location_name"]
