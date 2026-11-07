"""Resolve optional RPC location without weakening the requested repository."""

from gateway.dagster_adapter import DagsterAdapter
from gateway.errors import GatewayHTTPError


def resolve_job_location(
    adapter: DagsterAdapter, job_name: str, repository_name: str
) -> str:
    candidates = [
        job
        for job in adapter.list_jobs(repository_name=repository_name)["items"]
        if job["job_name"] == job_name
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
                    "job_id": job["job_id"],
                    "job_name": job["job_name"],
                    "repository_name": job["repository_name"],
                    "repository_location_name": job["repository_location_name"],
                }
                for job in candidates
            ],
        )
    return candidates[0]["repository_location_name"]
