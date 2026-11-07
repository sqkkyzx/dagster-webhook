"""Version-bound GraphQL documents; this is the only SDK private API boundary."""

import base64
import datetime
import json

from dagster import AssetKey
from dagster._core.storage.tags import REPOSITORY_LABEL_TAG
from dagster._utils.tags import normalize_tags
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError
from fastapi import HTTPException

from gateway.errors import GatewayHTTPError
from gateway.run_outputs import (
    OUTPUTS_EVENTS_QUERY,
    OUTPUTS_RUN_QUERY,
    collect_run_outputs,
)
from gateway.validation import SUBMISSION_TAG, validate_submission_tags

RUN_FIELDS = """
  runId jobName status creationTime startTime endTime parentRunId rootRunId
  tags { key value }
  repositoryOrigin { repositoryLocationName repositoryName }
  stepStats { stepKey status }
"""
CONFIG_FIELDS = """
fragment ConfigTypeInfo on ConfigType {
  __typename key description typeParamKeys isSelector
  ... on RegularConfigType { givenName }
  ... on CompositeConfigType {
    fields { name description configTypeKey isRequired isSecret defaultValueAsJson }
  }
  ... on EnumConfigType { givenName values { value description } }
  ... on ArrayConfigType { ofType { key } }
  ... on NullableConfigType { ofType { key } }
  ... on MapConfigType { keyType { key } valueType { key } keyLabelName }
  ... on ScalarUnionConfigType { scalarTypeKey nonScalarTypeKey }
}
"""
CONFIG_ERROR_FIELDS = """
  message reason
  stack {
    entries {
      __typename
      ... on EvaluationStackPathEntry { fieldName }
      ... on EvaluationStackListItemEntry { listIndex }
      ... on EvaluationStackMapKeyEntry { mapKey }
      ... on EvaluationStackMapValueEntry { mapKey }
    }
  }
  ... on MissingFieldConfigError { field { name } }
  ... on MissingFieldsConfigError { fields { name } }
  ... on FieldNotDefinedConfigError { fieldName }
  ... on FieldsNotDefinedConfigError { fieldNames }
"""
LOCATIONS_QUERY = """
query GatewayLocations {
  workspaceOrError {
    __typename ... on Error { message }
    ... on Workspace {
      locationEntries {
        name loadStatus updatedTimestamp
        locationOrLoadError {
          __typename ... on Error { message }
          ... on RepositoryLocation { name isReloadSupported repositories { name } }
        }
      }
    }
  }
}
"""
JOBS_QUERY = """
query GatewayJobs {
  repositoriesOrError {
    __typename ... on Error { message }
    ... on RepositoryConnection {
      nodes { name location { name } jobs { name description isAssetJob tags { key value } } }
    }
  }
}
"""
JOB_QUERY = """
query GatewayJob($selector: PipelineSelector!) {
  pipelineOrError(params: $selector) {
    __typename ... on Error { message }
    ... on Pipeline { name description isAssetJob nodeNames tags { key value } }
  }
}
"""
CONFIG_QUERY = (
    """
query GatewayConfig($selector: PipelineSelector!) {
  runConfigSchemaOrError(selector: $selector) {
    __typename ... on Error { message }
    ... on RunConfigSchema {
      rootConfigType { key } rootDefaultYaml allConfigTypes { ...ConfigTypeInfo }
    }
  }
}
"""
    + CONFIG_FIELDS
)
VALIDATE_QUERY = (
    """
query GatewayValidate($selector: PipelineSelector!, $config: RunConfigData) {
  isPipelineConfigValid(pipeline: $selector, mode: "default", runConfigData: $config) {
    __typename ... on Error { message }
    ... on PipelineConfigValidationValid { pipelineName }
    ... on RunConfigValidationInvalid { errors { """
    + CONFIG_ERROR_FIELDS
    + """ } }
  }
}
"""
)
RUNS_QUERY = (
    """
query GatewayRuns($filter: RunsFilter, $cursor: String, $limit: Int) {
  runsOrError(filter: $filter, cursor: $cursor, limit: $limit) {
    __typename ... on Error { message }
    ... on Runs { results { """
    + RUN_FIELDS
    + """ } }
  }
}
"""
)
RUN_QUERY = (
    """
query GatewayRun($runId: ID!) {
  runOrError(runId: $runId) {
    __typename ... on Error { message }
    ... on Run { """
    + RUN_FIELDS
    + """ }
  }
}
"""
)
EVENTS_QUERY = """
query GatewayEvents($runId: ID!, $cursor: String, $limit: Int) {
  logsForRun(runId: $runId, afterCursor: $cursor, limit: $limit) {
    __typename ... on Error { message }
    ... on EventConnection {
      cursor hasMore events {
        __typename
        ... on MessageEvent { runId message timestamp level stepKey eventType }
        ... on RunFailureEvent { error { message className } }
        ... on ExecutionStepFailureEvent { error { message className } }
      }
    }
  }
}
"""
SUBMIT_MUTATION = (
    """
mutation GatewaySubmit($params: ExecutionParams!) {
  launchRun(executionParams: $params) {
    __typename ... on Error { message }
    ... on LaunchRunSuccess { run { """
    + RUN_FIELDS
    + """ } }
    ... on RunConfigValidationInvalid { errors { """
    + CONFIG_ERROR_FIELDS
    + """ } }
  }
}
"""
)
TERMINATE_MUTATION = """
mutation GatewayTerminate($runIds: [String!]!) {
  terminateRuns(runIds: $runIds) {
    __typename ... on Error { message }
    ... on TerminateRunsResult {
      terminateRunResults {
        __typename ... on Error { message }
        ... on TerminateRunSuccess { run { runId } }
        ... on TerminateRunFailure { run { runId } }
        ... on RunNotFoundError { runId }
      }
    }
  }
}
"""
GRAPHQL_DOCUMENTS = (
    LOCATIONS_QUERY,
    JOBS_QUERY,
    JOB_QUERY,
    CONFIG_QUERY,
    VALIDATE_QUERY,
    RUNS_QUERY,
    RUN_QUERY,
    EVENTS_QUERY,
    SUBMIT_MUTATION,
    TERMINATE_MUTATION,
    OUTPUTS_RUN_QUERY,
    OUTPUTS_EVENTS_QUERY,
)


def encode_job_id(location_name: str, repository_name: str, job_name: str) -> str:
    """Encode identity, not authority: clients still need an API token."""
    value = json.dumps(
        [location_name, repository_name, job_name],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def decode_job_id(job_id: str) -> dict[str, str]:
    try:
        if not job_id or len(job_id) > 4096:
            raise ValueError
        raw = base64.b64decode(
            job_id + "=" * (-len(job_id) % 4), altchars=b"-_", validate=True
        )
        parts = json.loads(raw)
        if (
            not isinstance(parts, list)
            or len(parts) != 3
            or not all(isinstance(p, str) and p.strip() for p in parts)
        ):
            raise ValueError
        if encode_job_id(*parts) != job_id:
            raise ValueError
    except (ValueError, UnicodeError, TypeError) as exc:
        raise HTTPException(
            422, detail="Invalid job_id; use an ID returned by GET /api/v1/jobs"
        ) from exc
    return dict(
        zip(("repositoryLocationName", "repositoryName", "pipelineName"), parts)
    )


def _result(data: dict, field: str, expected: str) -> dict:
    result = data[field]
    if result["__typename"] != expected:
        detail = result.get("message") or (
            _config_errors(result["errors"])
            if result.get("errors") is not None
            else "Unexpected Dagster result"
        )
        raise DagsterGraphQLClientError(result["__typename"], detail)
    return result


def _config_errors(errors: list[dict]) -> list[dict]:
    """Build paths from native evaluation stacks; Dagster's old path is empty.

    Missing/unknown-field errors point at their enclosing object in the stack.
    Expand those errors per affected field so every issue locates that field.
    Selector errors retain the object path because the selection itself is invalid.
    """
    normalized = []
    for error in errors:
        path = []
        for entry in error.get("stack", {}).get("entries", []):
            if "fieldName" in entry:
                path.append(entry["fieldName"])
            elif "listIndex" in entry:
                path.append(entry["listIndex"])
            elif "mapKey" in entry:
                key = entry["mapKey"]
                # JSON object keys are strings, including numeric/boolean keys.
                path.append(key if isinstance(key, str) else json.dumps(key))
        if error.get("field") is not None:
            names = [error["field"]["name"]]
        elif error.get("fields") is not None:
            names = [field["name"] for field in error["fields"]]
        elif error.get("fieldName") is not None:
            names = [error["fieldName"]]
        else:
            names = error.get("fieldNames", [])
        for issue_path in [path + [name] for name in names] if names else [path]:
            normalized.append(
                {
                    "message": error["message"],
                    "path": issue_path,
                    "reason": error["reason"],
                }
            )
    return normalized


def _time(value: float | None) -> str | None:
    return (
        datetime.datetime.fromtimestamp(value, datetime.UTC).isoformat()
        if value is not None
        else None
    )


def _run(record: dict) -> dict:
    origin = record.get("repositoryOrigin") or {}
    location = origin.get("repositoryLocationName")
    repository = origin.get("repositoryName")
    job_name = record.get("jobName", "")
    failed = [
        step["stepKey"]
        for step in record.get("stepStats", [])
        if step.get("status") == "FAILURE"
    ]
    return {
        "run_id": record["runId"],
        "job_name": record.get("jobName", ""),
        "job_id": encode_job_id(location, repository, job_name)
        if location and repository and job_name
        else None,
        "repository_location_name": location,
        "repository_name": repository,
        "status": record["status"],
        "created_at": _time(record.get("creationTime")),
        "started_at": _time(record.get("startTime")),
        "ended_at": _time(record.get("endTime")),
        "parent_run_id": record.get("parentRunId"),
        "root_run_id": record.get("rootRunId"),
        "tags": {tag["key"]: tag["value"] for tag in record.get("tags", [])},
        "failed_steps": failed,
        "failure_summary": ("Failed steps: " + ", ".join(failed))
        if failed
        else (
            "Run failed; inspect events for diagnostics"
            if record["status"] == "FAILURE"
            else None
        ),
    }


def _selector(job_id: str, op_selection=None, asset_selection=None) -> dict:
    selector = decode_job_id(job_id)
    if op_selection is not None:
        selector["solidSelection"] = op_selection
    if asset_selection is not None:
        selector["assetSelection"] = [
            AssetKey.from_coercible(key).to_graphql_input() for key in asset_selection
        ]
    return selector


def build_runs_filter(statuses=None, job_name=None, tags=None, job_id=None) -> dict:
    """Use the same repository-label filter as Dagster's own Pipeline.runs."""
    filters = {}
    resolved_tags = dict(tags or {})
    if job_id is not None:
        selector = decode_job_id(job_id)
        selected_name = selector["pipelineName"]
        if job_name is not None and job_name != selected_name:
            raise GatewayHTTPError(
                422, "REQUEST_VALIDATION_FAILED", "job_name conflicts with job_id"
            )
        label = f"{selector['repositoryName']}@{selector['repositoryLocationName']}"
        if (
            REPOSITORY_LABEL_TAG in resolved_tags
            and resolved_tags[REPOSITORY_LABEL_TAG] != label
        ):
            raise GatewayHTTPError(
                422, "REQUEST_VALIDATION_FAILED", "Repository tag conflicts with job_id"
            )
        resolved_tags[REPOSITORY_LABEL_TAG] = label
        job_name = selected_name
    if statuses:
        filters["statuses"] = statuses
    if job_name:
        filters["pipelineName"] = job_name
    if resolved_tags:
        filters["tags"] = [
            {"key": key, "value": value} for key, value in resolved_tags.items()
        ]
    return filters


class DagsterAdapter:
    def __init__(self, client: DagsterGraphQLClient):
        self.client = client

    def _execute(self, document: str, variables: dict | None = None) -> dict:
        return self.client._execute(document, variables)

    def list_locations(self) -> dict:
        result = _result(
            self._execute(LOCATIONS_QUERY), "workspaceOrError", "Workspace"
        )
        items = []
        for entry in result["locationEntries"]:
            location = entry.get("locationOrLoadError") or {}
            loaded = location.get("__typename") == "RepositoryLocation"
            items.append(
                {
                    "location_name": entry["name"],
                    "load_status": entry["loadStatus"],
                    "updated_at": _time(entry.get("updatedTimestamp")),
                    "loaded": loaded,
                    "reload_supported": location.get("isReloadSupported", False),
                    "repositories": [
                        repo["name"] for repo in location.get("repositories", [])
                    ],
                    "error": None if loaded else location.get("message"),
                }
            )
        return {"items": items}

    def list_jobs(self, location_name=None, repository_name=None) -> dict:
        result = _result(
            self._execute(JOBS_QUERY), "repositoriesOrError", "RepositoryConnection"
        )
        items = []
        for repo in result["nodes"]:
            location = repo["location"]["name"]
            if location_name is not None and location != location_name:
                continue
            if repository_name is not None and repo["name"] != repository_name:
                continue
            for job in repo["jobs"]:
                items.append(
                    {
                        "job_id": encode_job_id(location, repo["name"], job["name"]),
                        "job_name": job["name"],
                        "repository_location_name": location,
                        "repository_name": repo["name"],
                        "description": job.get("description"),
                        "is_asset_job": job.get("isAssetJob", False),
                        "tags": {t["key"]: t["value"] for t in job.get("tags", [])},
                    }
                )
        return {"items": items}

    def get_job(self, job_id: str) -> dict:
        selector = decode_job_id(job_id)
        result = _result(
            self._execute(JOB_QUERY, {"selector": selector}),
            "pipelineOrError",
            "Pipeline",
        )
        return {
            "job_id": job_id,
            "job_name": result["name"],
            "repository_location_name": selector["repositoryLocationName"],
            "repository_name": selector["repositoryName"],
            "description": result.get("description"),
            "is_asset_job": result.get("isAssetJob", False),
            "op_names": result.get("nodeNames", []),
            "tags": {t["key"]: t["value"] for t in result.get("tags", [])},
        }

    def get_config_schema(self, job_id: str) -> dict:
        result = _result(
            self._execute(CONFIG_QUERY, {"selector": decode_job_id(job_id)}),
            "runConfigSchemaOrError",
            "RunConfigSchema",
        )
        # Dagster exposes native config types, not JSON Schema.
        return {
            "job_id": job_id,
            "format": "dagster",
            "root_type_key": result["rootConfigType"]["key"],
            "default_yaml": result.get("rootDefaultYaml", ""),
            "types": result["allConfigTypes"],
        }

    def validate_job(
        self, job_id: str, run_config: dict, op_selection=None, asset_selection=None
    ) -> dict:
        data = self._execute(
            VALIDATE_QUERY,
            {
                "selector": _selector(job_id, op_selection, asset_selection),
                "config": run_config,
            },
        )
        result = data["isPipelineConfigValid"]
        if result["__typename"] == "PipelineConfigValidationValid":
            return {"job_id": job_id, "valid": True, "errors": []}
        if result["__typename"] == "RunConfigValidationInvalid":
            return {
                "job_id": job_id,
                "valid": False,
                "errors": _config_errors(result["errors"]),
            }
        raise DagsterGraphQLClientError(
            result["__typename"], result.get("message", "Config validation failed")
        )

    def list_runs(
        self,
        statuses=None,
        job_name=None,
        tags=None,
        cursor=None,
        limit=50,
        job_id=None,
    ) -> dict:
        filters = build_runs_filter(statuses, job_name, tags, job_id)
        result = _result(
            self._execute(
                RUNS_QUERY, {"filter": filters, "cursor": cursor, "limit": limit + 1}
            ),
            "runsOrError",
            "Runs",
        )
        records = result["results"]
        has_more = len(records) > limit
        items = [_run(record) for record in records[:limit]]
        return {
            "items": items,
            "next_cursor": items[-1]["run_id"] if has_more and items else None,
            "has_more": has_more,
        }

    def get_run(self, run_id: str) -> dict:
        return _run(
            _result(self._execute(RUN_QUERY, {"runId": run_id}), "runOrError", "Run")
        )

    def find_submission_run(self, submission_id: str) -> dict | None:
        """Resolve a gateway marker; this read never authorizes another launch."""
        result = _result(
            self._execute(
                RUNS_QUERY,
                {
                    "filter": {
                        "tags": [{"key": SUBMISSION_TAG, "value": submission_id}]
                    },
                    "cursor": None,
                    "limit": 2,
                },
            ),
            "runsOrError",
            "Runs",
        )
        records = result["results"]
        if not records:
            return None
        if len(records) != 1:
            raise DagsterGraphQLClientError(
                "SubmissionCorrelationConflict",
                "Multiple runs match this submission marker; the outcome remains unknown",
            )
        marker_values = [
            tag["value"]
            for tag in records[0].get("tags", [])
            if tag["key"] == SUBMISSION_TAG
        ]
        if marker_values != [submission_id]:
            raise DagsterGraphQLClientError(
                "SubmissionCorrelationConflict",
                "Dagster returned a run without the exact submission marker",
            )
        return _run(records[0])

    def get_events(self, run_id: str, cursor=None, limit=100) -> dict:
        result = _result(
            self._execute(
                EVENTS_QUERY, {"runId": run_id, "cursor": cursor, "limit": limit}
            ),
            "logsForRun",
            "EventConnection",
        )
        items = []
        for event in result["events"]:
            items.append(
                {
                    "event_type": event.get("eventType") or event["__typename"],
                    "message": event.get("message", ""),
                    "timestamp": event.get("timestamp"),
                    "level": event.get("level"),
                    "step_key": event.get("stepKey"),
                    "error": event.get("error"),
                }
            )
        # The tail cursor is useful for a later poll even when has_more is false.
        return {
            "run_id": run_id,
            "items": items,
            "next_cursor": result["cursor"],
            "has_more": result["hasMore"],
        }

    def get_run_outputs(
        self,
        run_id: str,
        *,
        cursor=None,
        max_events=10000,
        value_metadata_key="api_value",
    ) -> dict:
        return collect_run_outputs(
            self._execute,
            run_id,
            cursor=cursor,
            max_events=max_events,
            value_metadata_key=value_metadata_key,
        )

    def submit_legacy_job(self, payload: dict) -> str:
        """Submit an already resolved legacy selector without an idempotent Run ID."""
        selected = {
            **payload,
            "job_id": encode_job_id(
                payload["repository_location_name"],
                payload["repository_name"],
                payload["job_name"],
            ),
        }
        return self._launch_run(selected, submission_id=None)["run_id"]

    def submit_run(self, payload: dict, submission_id: str) -> dict:
        return self._launch_run(payload, submission_id)

    def submit_direct_run(self, payload: dict) -> dict:
        """Create a fresh run without claiming or tagging an idempotent submission."""
        return self._launch_run(payload, submission_id=None)

    def _launch_run(self, payload: dict, submission_id: str | None) -> dict:
        validate_submission_tags(payload.get("tags"))
        tags = dict(normalize_tags(payload.get("tags")))
        if submission_id is not None:
            tags[SUBMISSION_TAG] = submission_id
        params = {
            "selector": _selector(
                payload["job_id"],
                payload.get("op_selection"),
                payload.get("asset_selection"),
            ),
            "runConfigData": payload.get("run_config") or {},
            "mode": "default",
            "executionMetadata": {
                "tags": [{"key": key, "value": value} for key, value in tags.items()],
            },
        }
        result = _result(
            self._execute(SUBMIT_MUTATION, {"params": params}),
            "launchRun",
            "LaunchRunSuccess",
        )
        return _run(result["run"])

    def terminate_runs(self, run_ids: list[str]) -> dict:
        result = _result(
            self._execute(TERMINATE_MUTATION, {"runIds": run_ids}),
            "terminateRuns",
            "TerminateRunsResult",
        )
        returned = result["terminateRunResults"]
        if len(returned) != len(run_ids):
            raise DagsterGraphQLClientError(
                "PythonError", "Dagster returned an incomplete batch termination result"
            )
        items = []
        for run_id, row in zip(run_ids, returned):
            actual = row.get("run", {}).get("runId") or row.get("runId")
            if actual is not None and actual != run_id:
                raise DagsterGraphQLClientError(
                    "PythonError", "Dagster returned mismatched run IDs"
                )
            accepted = row["__typename"] == "TerminateRunSuccess"
            items.append(
                {
                    "run_id": run_id,
                    "status": "accepted" if accepted else "failed",
                    "error_type": None if accepted else row["__typename"],
                    "detail": None
                    if accepted
                    else row.get("message", "Termination failed"),
                }
            )
        succeeded = sum(row["status"] == "accepted" for row in items)
        return {
            "results": items,
            "succeeded": succeeded,
            "failed": len(items) - succeeded,
        }
