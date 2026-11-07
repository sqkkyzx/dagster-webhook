"""Read IO-manager output files from the configured, mounted storage tree."""

import json

from dagster_graphql import DagsterGraphQLClientError

from gateway.output_files import read_output_file

MAX_VALUE_BYTES = 16 * 1024 * 1024
VALUE_BUDGET_BYTES = 32 * 1024 * 1024

OUTPUTS_RUN_QUERY = """
query GatewayRunOutputSteps($runId: ID!) {
  runOrError(runId: $runId) {
    __typename ... on Error { message }
    ... on Run {
      runId status
      executionPlan { steps { key solidHandleID kind outputs { name } } }
      stepStats { stepKey status }
    }
  }
}
"""
OUTPUTS_EVENTS_QUERY = """
query GatewayRunOutputEvents($runId: ID!, $cursor: String, $limit: Int) {
  logsForRun(runId: $runId, afterCursor: $cursor, limit: $limit) {
    __typename ... on Error { message }
    ... on EventConnection {
      cursor hasMore events {
        __typename
        ... on MessageEvent { stepKey solidHandleID }
        ... on HandledOutputEvent {
          outputName managerKey
          metadataEntries {
            __typename label
            ... on PathMetadataEntry { path }
            ... on TextMetadataEntry { text }
          }
        }
      }
    }
  }
}
"""


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


def _step(key, handle=None, kind=None, declared=None):
    return {
        "step_key": key,
        "status": "NOT_STARTED",
        "handle": handle,
        "kind": kind,
        "declared": declared or [],
        "handled": {},
    }


def _path(event):
    entries = [
        entry
        for entry in event.get("metadataEntries", [])
        if entry.get("label") == "path"
    ]
    if len(entries) != 1:
        return None
    entry = entries[0]
    field = {"PathMetadataEntry": "path", "TextMetadataEntry": "text"}.get(
        entry.get("__typename")
    )
    value = entry.get(field) if field else None
    return value if isinstance(value, str) and value else None


def collect_run_outputs(execute, run_id, *, cursor=None, max_events=10000):
    """Keep statuses and return only output names and current stored values."""
    if (
        not isinstance(max_events, int)
        or isinstance(max_events, bool)
        or not 1 <= max_events <= 20000
    ):
        raise ValueError("max_events must be between 1 and 20000")
    run = _result(execute(OUTPUTS_RUN_QUERY, {"runId": run_id}), "runOrError", "Run")
    steps, declarations = {}, {}
    for plan_step in (run.get("executionPlan") or {}).get("steps", []):
        names = [output["name"] for output in plan_step.get("outputs", [])]
        handle = plan_step.get("solidHandleID")
        steps[plan_step["key"]] = _step(
            plan_step["key"], handle, plan_step.get("kind"), names
        )
        if handle:
            declarations[handle] = names
    stats_keys = set()
    for stats in run.get("stepStats", []):
        key = stats["stepKey"]
        stats_keys.add(key)
        steps.setdefault(key, _step(key))["status"] = stats.get("status") or "UNKNOWN"
    scanned, tail, has_more = 0, cursor, True
    while scanned < max_events and has_more:
        limit = min(1000, max_events - scanned)
        connection = _result(
            execute(
                OUTPUTS_EVENTS_QUERY, {"runId": run_id, "cursor": tail, "limit": limit}
            ),
            "logsForRun",
            "EventConnection",
        )
        events, new_tail = connection["events"], connection["cursor"]
        if len(events) > limit:
            raise DagsterGraphQLClientError(
                "InvalidOutputResponse", "Dagster ignored the event page limit"
            )
        has_more = connection["hasMore"]
        if has_more and (not events or new_tail == tail):
            raise DagsterGraphQLClientError(
                "InvalidOutputResponse", "Dagster event cursor did not advance"
            )
        for event in events:
            key = event.get("stepKey")
            if key:
                step = steps.setdefault(key, _step(key))
                if key not in stats_keys:
                    step["status"] = "UNKNOWN"
                if not step["handle"]:
                    step["handle"] = event.get("solidHandleID")
                if not step["declared"]:
                    step["declared"] = declarations.get(step["handle"], [])
            if event.get("__typename") != "HandledOutputEvent":
                continue
            if not key or not event.get("outputName"):
                raise DagsterGraphQLClientError(
                    "InvalidOutputResponse", "Handled output has no step or output name"
                )
            path = _path(event)
            identity = (event["outputName"], path)
            # One current value per file; dynamic outputs with different paths
            # remain separate even when their names are the same.
            step["handled"].pop(identity, None)
            step["handled"][identity] = event.get("managerKey")
        scanned += len(events)
        tail = new_tail
    complete = cursor is None and not has_more
    observed_handles = {
        step["handle"]
        for key, step in steps.items()
        if step["handle"] and (key in stats_keys or step["handled"])
    }
    items, remaining = [], VALUE_BUDGET_BYTES
    for key, step in steps.items():
        if (
            step["kind"] == "UNRESOLVED_MAPPED"
            and key not in stats_keys
            and step["handle"] in observed_handles
        ):
            continue
        outputs, names = [], set()
        for (name, path), manager in step["handled"].items():
            value = None
            if manager == "io_manager" and path and remaining > 0:
                value = read_output_file(
                    path, max_value_bytes=min(MAX_VALUE_BYTES, remaining)
                )
                size = len(
                    json.dumps(
                        value,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                )
                if size <= remaining:
                    remaining -= size
                else:
                    value = None
            names.add(name)
            outputs.append({"output_name": name, "output_value": value})
        if complete:
            outputs.extend(
                {"output_name": name, "output_value": None}
                for name in step["declared"]
                if name not in names
            )
        items.append({"step_key": key, "status": step["status"], "outputs": outputs})
    return {
        "run_id": run["runId"],
        "run_status": run["status"],
        "items": items,
        "next_cursor": tail,
        "has_more": has_more,
        "complete": complete,
    }
