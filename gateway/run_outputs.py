"""Read execution/output evidence without loading objects from an IO manager.

Only a metadata entry explicitly selected by the caller is an exported value.
Paths and other metadata are reported as metadata, never opened or evaluated.
"""

import datetime
import json
import math
from typing import Any

from dagster_graphql import DagsterGraphQLClientError

MAX_VALUE_BYTES = 64 * 1024
VALUE_BUDGET_BYTES = 1024 * 1024
METADATA_PREVIEW_BYTES = 4 * 1024
METADATA_BUDGET_BYTES = 1024 * 1024
MAX_JSON_DEPTH = 64

OUTPUTS_RUN_QUERY = """
query GatewayRunOutputSteps($runId: ID!) {
  runOrError(runId: $runId) {
    __typename ... on Error { message }
    ... on Run {
      runId status
      executionPlan { steps { key solidHandleID kind outputs { name } } }
      stepStats { stepKey status startTime endTime attempts { startTime endTime } }
    }
  }
}
"""
OUTPUT_METADATA_FIELDS = """
  __typename label description
  ... on JsonMetadataEntry { jsonString }
  ... on TextMetadataEntry { text }
  ... on IntMetadataEntry { intValue intRepr }
  ... on FloatMetadataEntry { floatValue floatRepr }
  ... on BoolMetadataEntry { boolValue }
  ... on PathMetadataEntry { path }
  ... on NotebookMetadataEntry { path }
  ... on UrlMetadataEntry { url }
  ... on MarkdownMetadataEntry { mdStr }
  ... on TimestampMetadataEntry { timestamp }
  ... on AssetMetadataEntry { assetKey { path } }
  ... on PipelineRunMetadataEntry { runId }
  ... on JobMetadataEntry { jobName repositoryName locationName }
  ... on PythonArtifactMetadataEntry { module name }
"""
OUTPUTS_EVENTS_QUERY = (
    """
query GatewayRunOutputEvents($runId: ID!, $cursor: String, $limit: Int) {
  logsForRun(runId: $runId, afterCursor: $cursor, limit: $limit) {
    __typename ... on Error { message }
    ... on EventConnection {
      cursor hasMore events {
        __typename
        ... on MessageEvent { stepKey solidHandleID timestamp }
        ... on ExecutionStepOutputEvent {
          outputName typeCheck { success }
          metadataEntries { """
    + OUTPUT_METADATA_FIELDS
    + """ }
        }
        ... on HandledOutputEvent {
          outputName managerKey
          metadataEntries { """
    + OUTPUT_METADATA_FIELDS
    + """ }
        }
      }
    }
  }
}
"""
)


def _result(data, field, expected):
    result = data.get(field)
    if not isinstance(result, dict):
        raise DagsterGraphQLClientError(
            "InvalidOutputResponse", "Missing Dagster result"
        )
    if result.get("__typename") != expected:
        raise DagsterGraphQLClientError(
            result.get("__typename", "InvalidOutputResponse"),
            result.get("message", "Unexpected Dagster result"),
        )
    return result


def _time(value, *, milliseconds=False):
    if value is None:
        return None
    try:
        seconds = float(value) / (1000 if milliseconds else 1)
        if not math.isfinite(seconds):
            raise ValueError
        return datetime.datetime.fromtimestamp(seconds, datetime.UTC).isoformat()
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise DagsterGraphQLClientError(
            "InvalidOutputResponse", "Dagster returned an invalid timestamp"
        ) from exc


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _size(value):
    return len(_json(value).encode("utf-8"))


class _TooLarge(ValueError):
    pass


def _invalid_constant(value):
    raise ValueError(f"Non-finite JSON constant {value}")


def _check_json_depth(value):
    pending = [(value, 0)]
    while pending:
        current, depth = pending.pop()
        if isinstance(current, (dict, list)):
            if depth >= MAX_JSON_DEPTH:
                raise ValueError("Exported JSON value is too deeply nested")
            children = current.values() if isinstance(current, dict) else current
            pending.extend((child, depth + 1) for child in children)


def _scalar_metadata(entry, *, maximum=None):
    """Decode only documented export types; no path, table or object loading."""
    kind = entry.get("__typename")
    if kind == "NullMetadataEntry":
        return None
    if kind == "JsonMetadataEntry":
        raw = entry["jsonString"]
        if not isinstance(raw, str):
            raise ValueError("Invalid JSON metadata")
        value = json.loads(raw, parse_constant=_invalid_constant)
        _check_json_depth(value)
    elif kind == "TextMetadataEntry":
        value = entry["text"]
        if not isinstance(value, str):
            raise ValueError("Invalid text metadata")
    elif kind == "IntMetadataEntry":
        raw = entry.get("intRepr")
        if raw is not None:
            if not isinstance(raw, str):
                raise ValueError("Invalid integer metadata")
            if maximum is not None and len(raw.encode("utf-8")) > maximum:
                raise _TooLarge
            value = int(raw)
        else:
            value = entry["intValue"]
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("Invalid integer metadata")
    elif kind == "FloatMetadataEntry":
        raw = entry.get("floatValue")
        if raw is None:
            raw = entry.get("floatRepr")
        if isinstance(raw, bool) or raw is None:
            raise ValueError("Invalid float metadata")
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError("Non-finite float metadata")
    elif kind == "BoolMetadataEntry":
        value = entry["boolValue"]
        if not isinstance(value, bool):
            raise ValueError("Invalid boolean metadata")
    else:
        raise ValueError("Metadata type is not an exported JSON value")
    if maximum is not None and _size(value) > maximum:
        raise _TooLarge
    return value


def _preview_value(entry):
    kind = entry.get("__typename")
    if kind in {"PathMetadataEntry", "NotebookMetadataEntry"}:
        return entry["path"]
    if kind == "UrlMetadataEntry":
        return entry["url"]
    if kind == "MarkdownMetadataEntry":
        return entry["mdStr"]
    if kind == "TimestampMetadataEntry":
        return entry["timestamp"]
    if kind == "AssetMetadataEntry":
        return entry["assetKey"]
    if kind == "PipelineRunMetadataEntry":
        return entry["runId"]
    if kind == "JobMetadataEntry":
        return {
            key: entry.get(key) for key in ("jobName", "repositoryName", "locationName")
        }
    if kind == "PythonArtifactMetadataEntry":
        return {key: entry.get(key) for key in ("module", "name")}
    return _scalar_metadata(entry, maximum=METADATA_PREVIEW_BYTES)


def _text_preview(value, maximum):
    """Fit a readable string preview into a JSON-encoded UTF-8 byte budget."""
    if maximum < 2:
        return None
    low, high = 0, min(len(value), maximum)
    while low < high:
        middle = (low + high + 1) // 2
        if _size(value[:middle]) <= maximum:
            low = middle
        else:
            high = middle - 1
    return value[:low]


class _Budget:
    def __init__(self, maximum):
        self.remaining = maximum

    def take(self, size):
        if size > self.remaining:
            return False
        self.remaining -= size
        return True


def _metadata_preview(entry, budget):
    result = {
        "label": entry.get("label", ""),
        "type": entry.get("__typename", "UnknownMetadataEntry"),
        "description": entry.get("description"),
        "value": None,
        "value_available": False,
        "truncated": False,
    }
    maximum = min(METADATA_PREVIEW_BYTES, budget.remaining)
    if maximum < 4:
        result["truncated"] = True
        return result
    try:
        value = _preview_value(entry)
        size = _size(value)
        if size <= maximum:
            budget.take(size)
            result.update(value=value, value_available=True)
            return result
        raw = _json(value)
    except _TooLarge:
        raw = entry.get("jsonString", entry.get("intRepr", entry.get("text", "")))
    except (KeyError, TypeError, ValueError, RecursionError):
        return result
    preview = _text_preview(raw, maximum)
    if preview is not None:
        budget.take(_size(preview))
    result.update(value=preview, truncated=True)
    return result


def _exported_value(entries, label, budget):
    base = {"value_available": False, "value": None, "value_source": None}
    matching = [entry for entry in entries if entry.get("label") == label]
    if not matching:
        return {**base, "value_unavailable_reason": "NOT_EXPOSED"}
    base["value_source"] = label
    if len(matching) != 1:
        return {**base, "value_unavailable_reason": "INVALID_VALUE"}
    try:
        value = _scalar_metadata(matching[0], maximum=MAX_VALUE_BYTES)
        size = _size(value)
    except _TooLarge:
        return {**base, "value_unavailable_reason": "VALUE_TOO_LARGE"}
    except (KeyError, TypeError, ValueError, RecursionError, OverflowError):
        return {**base, "value_unavailable_reason": "INVALID_VALUE"}
    if not budget.take(size):
        return {**base, "value_unavailable_reason": "RESPONSE_LIMIT"}
    return {
        "value_available": True,
        "value": value,
        "value_source": label,
        "value_unavailable_reason": None,
    }


def _new_step(key, op_handle=None, kind=None, declared=None):
    return {
        "step_key": key,
        "op_handle": op_handle,
        "kind": kind,
        "status": "NOT_STARTED",
        "started_at": None,
        "ended_at": None,
        "attempts": [],
        "declared_outputs": declared or [],
        "outputs": [],
    }


def collect_run_outputs(
    execute, run_id, *, cursor=None, max_events=10000, value_metadata_key="api_value"
):
    """Combine fresh step snapshots with a bounded, cursor-scoped output history."""
    if (
        not isinstance(max_events, int)
        or isinstance(max_events, bool)
        or not 1 <= max_events <= 20000
    ):
        raise ValueError("max_events must be between 1 and 20000")
    if (
        not isinstance(value_metadata_key, str)
        or not value_metadata_key
        or len(value_metadata_key) > 200
    ):
        raise ValueError("value_metadata_key must contain 1 to 200 characters")
    run = _result(execute(OUTPUTS_RUN_QUERY, {"runId": run_id}), "runOrError", "Run")
    steps: dict[str, dict[str, Any]] = {}
    plan = run.get("executionPlan")
    for step in (plan or {}).get("steps", []):
        steps[step["key"]] = _new_step(
            step["key"],
            step.get("solidHandleID"),
            step.get("kind"),
            [output["name"] for output in step.get("outputs", [])],
        )
    stats_keys = set()
    for stats in run.get("stepStats", []):
        stats_keys.add(stats["stepKey"])
        step = steps.setdefault(stats["stepKey"], _new_step(stats["stepKey"]))
        step.update(
            status=stats.get("status") or "UNKNOWN",
            started_at=_time(stats.get("startTime")),
            ended_at=_time(stats.get("endTime")),
            attempts=[
                {
                    "started_at": _time(attempt.get("startTime")),
                    "ended_at": _time(attempt.get("endTime")),
                }
                for attempt in stats.get("attempts", [])
            ],
        )
    scanned = 0
    next_cursor = cursor
    has_more = True
    value_budget = _Budget(VALUE_BUDGET_BYTES)
    metadata_budget = _Budget(METADATA_BUDGET_BYTES)
    while scanned < max_events and has_more:
        limit = min(1000, max_events - scanned)
        connection = _result(
            execute(
                OUTPUTS_EVENTS_QUERY,
                {"runId": run_id, "cursor": next_cursor, "limit": limit},
            ),
            "logsForRun",
            "EventConnection",
        )
        events = connection["events"]
        if len(events) > limit:
            raise DagsterGraphQLClientError(
                "InvalidOutputResponse", "Dagster ignored the event page limit"
            )
        has_more = connection["hasMore"]
        tail = connection["cursor"]
        if has_more and (not events or tail == next_cursor):
            raise DagsterGraphQLClientError(
                "InvalidOutputResponse", "Dagster event cursor did not advance"
            )
        for event in events:
            kind = event.get("__typename")
            key = event.get("stepKey")
            if key:
                if key not in steps:
                    steps[key] = _new_step(key)
                step = steps[key]
                if key not in stats_keys:
                    # The event history is later than the initial snapshot.
                    # Execution was observed but its latest status is unknown.
                    step["status"] = "UNKNOWN"
                if not step["op_handle"]:
                    step["op_handle"] = event.get("solidHandleID")
            if kind not in {"ExecutionStepOutputEvent", "HandledOutputEvent"}:
                continue
            if not key:
                raise DagsterGraphQLClientError(
                    "InvalidOutputResponse", "Output event has no step key"
                )
            step = steps[key]
            entries = event.get("metadataEntries", [])
            step["outputs"].append(
                {
                    "event_type": "STEP_OUTPUT"
                    if kind == "ExecutionStepOutputEvent"
                    else "HANDLED_OUTPUT",
                    "output_name": event["outputName"],
                    "timestamp": _time(event.get("timestamp"), milliseconds=True),
                    "type_check_success": (event.get("typeCheck") or {}).get("success"),
                    "manager_key": event.get("managerKey"),
                    "metadata": [
                        _metadata_preview(entry, metadata_budget) for entry in entries
                    ],
                    **_exported_value(entries, value_metadata_key, value_budget),
                }
            )
        scanned += len(events)
        next_cursor = tail
    return {
        "run_id": run["runId"],
        "run_status": run["status"],
        "execution_plan_available": plan is not None,
        "items": list(steps.values()),
        "scanned_events": scanned,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "complete": cursor is None and not has_more,
    }
