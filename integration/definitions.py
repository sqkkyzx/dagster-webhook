"""Jobs used exclusively by the isolated acceptance environment."""

import time

from dagster import Definitions, Field, Out, Output, job, op


@op(config_schema={"count": int, "wait_seconds": Field(int, default_value=0)})
def acceptance_step(context):
    seconds = context.op_config["wait_seconds"]
    if not 0 <= seconds <= 120:
        raise ValueError("Acceptance fixture wait must be between 0 and 120 seconds")
    context.log.info("Isolated acceptance fixture started")
    time.sleep(seconds)
    return context.op_config["count"]


@job
def acceptance_job():
    acceptance_step()


@op(
    out={
        "payload": Out(dict),
        "null_value": Out(),
        "zero": Out(int),
        "false_value": Out(bool),
        "text": Out(str),
    }
)
def exported_outputs():
    payload = {"numbers": [1, 2], "ok": True}
    yield Output(payload, "payload")
    yield Output(None, "null_value")
    yield Output(0, "zero")
    yield Output(False, "false_value")
    yield Output("mounted output", "text")


@op
def unexported_output(payload: dict) -> int:
    return len(payload["numbers"])


@job
def acceptance_outputs_job():
    outputs = exported_outputs()
    unexported_output(outputs.payload)


defs = Definitions(jobs=[acceptance_job, acceptance_outputs_job])
