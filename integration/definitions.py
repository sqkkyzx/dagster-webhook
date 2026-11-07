"""Jobs used exclusively by the isolated acceptance environment."""

import time

from dagster import Definitions, Field, job, op


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


defs = Definitions(jobs=[acceptance_job])
