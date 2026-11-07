"""Jobs used exclusively by the isolated acceptance environment."""

import time

from dagster import Definitions, Field, MetadataValue, Out, Output, job, op


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
    }
)
def exported_outputs():
    payload = {"numbers": [1, 2], "ok": True}
    yield Output(
        payload,
        "payload",
        metadata={
            "api_value": MetadataValue.json(payload),
            "custom_value": MetadataValue.json(payload),
        },
    )
    yield Output(None, "null_value", metadata={"api_value": MetadataValue.json(None)})
    yield Output(0, "zero", metadata={"api_value": MetadataValue.int(0)})
    yield Output(
        False, "false_value", metadata={"api_value": MetadataValue.bool(False)}
    )


@op
def unexported_output(payload: dict) -> int:
    return len(payload["numbers"])


@job
def acceptance_outputs_job():
    outputs = exported_outputs()
    unexported_output(outputs.payload)


defs = Definitions(jobs=[acceptance_job, acceptance_outputs_job])
