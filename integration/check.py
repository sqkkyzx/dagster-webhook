"""Launch disposable services and verify real Dagster HTTP workflows.

No remote URL is accepted: both backends exclusively create their own fixture.
Docker mode tests an already-built gateway image and retains its data volume
across restart. Native mode tests the source tree using the selected Python.
"""

import argparse
import concurrent.futures
import contextlib
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "integration"
OPENER = build_opener(ProxyHandler({}))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(url, method="GET", payload=None, headers=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = Request(
        url,
        body,
        {"Content-Type": "application/json", **(headers or {})},
        method=method,
    )
    try:
        response = OPENER.open(req, timeout=30)
    except HTTPError as exc:
        response = exc
    with response:
        data = response.read()
        if "application/json" in response.headers.get("Content-Type", ""):
            data = json.loads(data)
        return response.status, data


def wait_until(check, label, timeout=120):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (URLError, ConnectionError, TimeoutError) as exc:
            last_error = exc
        time.sleep(0.3)
    raise AssertionError(f"Timed out waiting for {label}: {last_error}")


class Environment:
    def __init__(self, mode, python, image, directory):
        self.mode, self.python, self.image = mode, python, image
        self.directory = Path(directory)
        self.gateway_port, self.proxy_port, self.dagster_port = (
            free_port(),
            free_port(),
            free_port(),
        )
        self.gateway_url = f"http://127.0.0.1:{self.gateway_port}"
        self.proxy_url = f"http://127.0.0.1:{self.proxy_port}"
        if mode == "docker":
            self.gateway_url = "http://gateway:8080"
            self.proxy_url = "http://proxy:3001"
        self.token = secrets.token_hex(24)
        self.project = "dagster-acceptance-" + secrets.token_hex(6)
        self.processes, self.logs = {}, {}
        # The caller's business URL, tokens and storage paths never reach fixtures.
        self.env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("DAGSTER_", "APP_", "IDEMPOTENCY_", "RUN_OUTPUT_"))
        }
        self.env.update(
            DAGSTER_ACCEPTANCE_IMAGE=image or "unused-native-image",
            DAGSTER_ACCEPTANCE_TOKEN=self.token,
            NO_PROXY="127.0.0.1,localhost",
            no_proxy="127.0.0.1,localhost",
        )

    def compose(self, *args, **kwargs):
        return subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(FIXTURE / "compose.yaml"),
                "--project-name",
                self.project,
                *args,
            ],
            env=self.env,
            check=True,
            text=True,
            **kwargs,
        )

    def request(self, url, method="GET", payload=None, headers=None):
        if self.mode != "docker":
            return request(url, method, payload, headers)
        parsed = urlsplit(url)
        if parsed.hostname not in {"gateway", "proxy"}:
            raise ValueError("Docker acceptance requests must target fixture services")
        message = {
            "service": parsed.hostname,
            "path": parsed.path + ("?" + parsed.query if parsed.query else ""),
            "method": method,
            "payload": payload,
            "headers": headers or {},
        }
        try:
            result = self.compose(
                "exec",
                "-T",
                "proxy",
                "python",
                "/fixture/http_probe.py",
                input=json.dumps(message),
                capture_output=True,
                timeout=35,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            diagnostic = exc.stderr or ""
            if isinstance(diagnostic, bytes):
                diagnostic = diagnostic.decode(errors="replace")
            raise URLError(
                "Acceptance HTTP probe could not run in the proxy container: "
                + diagnostic[-1500:]
            ) from exc
        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Acceptance HTTP probe returned invalid JSON") from exc
        if "transport_error" in response:
            raise URLError(response["transport_error"])
        return response["status"], response["body"]

    def spawn(self, name, args, extra_env):
        # The log remains open for this subprocess and is closed by stop_process.
        log = open(self.directory / f"{name}.log", "ab")  # noqa: SIM115
        self.logs[name] = log
        self.processes[name] = subprocess.Popen(
            [self.python, *args],
            cwd=ROOT,
            env={**self.env, **extra_env},
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def start_gateway(self):
        self.spawn(
            "gateway",
            [str(ROOT / "main.py")],
            {
                "APP_HOST": "127.0.0.1",
                "APP_PORT": str(self.gateway_port),
                "APP_API_TOKEN": self.token,
                "DAGSTER_URL": self.proxy_url,
                "DAGSTER_TIMEOUT": "20",
                "DAGSTER_READINESS_TIMEOUT": "1",
                "IDEMPOTENCY_DB_PATH": str(self.directory / "idempotency.sqlite3"),
                "IDEMPOTENCY_LEASE_SECONDS": "2",
                "RUN_OUTPUT_STORAGE_ROOT": str(
                    self.directory / "dagster-home" / "storage"
                ),
            },
        )

    def start(self):
        if self.mode == "docker":
            self.compose("up", "--build", "--detach")
        else:
            home = self.directory / "dagster-home"
            home.mkdir()
            (home / "storage").mkdir()
            shutil.copy(FIXTURE / "dagster.yaml", home / "dagster.yaml")
            self.spawn(
                "dagster",
                [
                    "-m",
                    "dagster_webserver",
                    "-f",
                    str(FIXTURE / "definitions.py"),
                    "-h",
                    "127.0.0.1",
                    "-p",
                    str(self.dagster_port),
                ],
                {"DAGSTER_HOME": str(home)},
            )
            self.spawn(
                "proxy",
                [
                    str(FIXTURE / "upstream_proxy.py"),
                    "--port",
                    str(self.proxy_port),
                    "--upstream",
                    f"http://127.0.0.1:{self.dagster_port}",
                ],
                {},
            )
            self.start_gateway()
        wait_until(
            lambda: self.request(self.gateway_url + "/ready")[0] == 200,
            "isolated gateway and Dagster",
        )

    def stop_process(self, name):
        process = self.processes.pop(name, None)
        if process is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        log = self.logs.pop(name, None)
        if log:
            log.close()

    def restart_gateway(self):
        if self.mode == "docker":
            self.compose("restart", "gateway")
        else:
            self.stop_process("gateway")
            self.start_gateway()
        wait_until(
            lambda: self.request(self.gateway_url + "/ready")[0] == 200,
            "gateway restart with retained idempotency storage",
        )

    def remove_output_file(self, run_id, step_key, output_name):
        """Delete only one artifact created in this disposable fixture."""
        relative = Path(run_id) / step_key / output_name
        if self.mode == "docker":
            path = "/opt/dagster/home/storage/" + str(relative)
            self.compose(
                "exec",
                "-T",
                "dagster",
                "python",
                "-c",
                "from pathlib import Path; Path(" + repr(path) + ").unlink()",
            )
        else:
            (self.directory / "dagster-home" / "storage" / relative).unlink()

    def dump_logs(self):
        if self.mode == "docker":
            self.compose("ps", "--all")
            subprocess.run(
                [
                    "docker",
                    "network",
                    "inspect",
                    self.project + "_acceptance",
                    "--format",
                    "internal={{.Internal}} containers={{json .Containers}}",
                ],
                env=self.env,
                check=False,
                text=True,
            )
            self.compose("logs", "--no-color", "--tail", "100")
        else:
            for name in ("gateway", "dagster", "proxy"):
                path = self.directory / f"{name}.log"
                if path.exists():
                    print(
                        f"\n{name} fixture log:\n{path.read_text(errors='replace')[-12000:]}",
                        file=sys.stderr,
                    )

    def close(self):
        if self.mode == "docker":
            self.compose("down", "--volumes", "--remove-orphans", "--timeout", "15")
        else:
            for name in ("gateway", "proxy", "dagster"):
                self.stop_process(name)


def check(environment):
    headers = {"Authorization": "Bearer " + environment.token}

    def api(path, method="GET", payload=None, key=None):
        return environment.request(
            environment.gateway_url + path,
            method,
            payload,
            {**headers, **({"Idempotency-Key": key} if key else {})},
        )

    def count_submissions():
        return environment.request(environment.proxy_url + "/__acceptance/state")[1][
            "submissions"
        ]

    def submit(payload, key):
        status, body = api("/api/v1/runs", "POST", payload, key)
        assert status in {200, 201}, (status, body)
        return body

    def run_status(run_id):
        status, body = api("/api/v1/runs/" + run_id)
        assert status == 200, (status, body)
        return body["status"]

    def replay(payload, key):
        status, body = api("/api/v1/runs", "POST", payload, key)
        if status == 409:
            assert body["code"] in {
                "IDEMPOTENCY_IN_PROGRESS",
                "SUBMISSION_OUTCOME_UNKNOWN",
            }, body
            return None
        assert status == 200, (status, body)
        assert body["replayed"] is True, body
        return body

    expected_version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"][
        "version"
    ]
    status, health = api("/health")
    assert status == 200 and health["version"] == expected_version, health
    assert api("/redoc")[0] == 200
    status, schema = api("/openapi.json")
    assert status == 200 and schema["info"]["version"] == expected_version

    def loaded_fixture():
        status, locations = api("/api/v1/locations")
        if (
            status == 200
            and locations["items"]
            and all(location["loaded"] for location in locations["items"])
        ):
            return locations
        return None

    locations = wait_until(loaded_fixture, "isolated code location loading")
    assert len(locations["items"]) == 1, locations
    status, jobs = api("/api/v1/jobs")
    assert status == 200, jobs
    job = next(job for job in jobs["items"] if job["job_name"] == "acceptance_job")
    job_url = "/api/v1/jobs/" + job["job_id"]
    assert api(job_url)[0] == api(job_url + "/config-schema")[0] == 200
    config = {"ops": {"acceptance_step": {"config": {"count": 1}}}}
    payload = {"job_id": job["job_id"], "run_config": config}
    status, valid = api(job_url + "/validate", "POST", {"run_config": config})
    assert status == 200 and valid["valid"] is True, valid
    bad_config = {"ops": {"acceptance_step": {"config": {"count": "bad"}}}}
    status, invalid = api(job_url + "/validate", "POST", {"run_config": bad_config})
    assert status == 200 and invalid["valid"] is False, invalid
    assert any(
        error["path"] == ["ops", "acceptance_step", "config", "count"]
        for error in invalid["errors"]
    ), invalid
    status, wrong_repository = api(
        "/api/v1/submit_job",
        "POST",
        {"job_name": "acceptance_job", "repository_name": "absent_repository"},
    )
    assert status == 404, wrong_repository

    direct_runs = []
    direct_before = count_submissions()
    for _ in range(2):
        status, direct = api("/api/v1/runs/submit", "POST", payload)
        assert status == 201, (status, direct)
        assert direct["job_id"] == job["job_id"], direct
        assert direct["status_url"] == "/api/v1/runs/" + direct["run_id"], direct
        assert "submission_id" not in direct and "replayed" not in direct, direct
        direct_runs.append(direct)
    assert direct_runs[0]["run_id"] != direct_runs[1]["run_id"], direct_runs
    assert count_submissions() == direct_before + 2
    for direct in direct_runs:
        wait_until(
            lambda run_id=direct["run_id"]: run_status(run_id) == "SUCCESS",
            "non-idempotent fixture completion",
        )
        status, detail = api(direct["status_url"])
        assert (
            status == 200 and "dagster-webhook/submission_id" not in detail["tags"]
        ), detail
    print(
        "PASS non-idempotent submissions create two distinct runs without recovery markers",
        flush=True,
    )

    optional_key_before = count_submissions()
    body_payload = {**payload, "idempotency_key": "body-header-replay"}
    status, body_created = api("/api/v1/runs", "POST", body_payload)
    assert status == 201 and body_created["replayed"] is False, (status, body_created)
    header_replayed = wait_until(
        lambda: replay(payload, "body-header-replay"),
        "header replay of body-key submission",
    )
    assert header_replayed["run_id"] == body_created["run_id"], header_replayed
    status, both_replayed = api(
        "/api/v1/runs", "POST", body_payload, "body-header-replay"
    )
    assert status == 200 and both_replayed["run_id"] == body_created["run_id"], (
        both_replayed
    )
    assert count_submissions() == optional_key_before + 1
    for invalid_payload, invalid_key in (
        (payload, None),
        ({**payload, "idempotency_key": "body-key"}, "different-header-key"),
    ):
        status, error = api("/api/v1/runs", "POST", invalid_payload, invalid_key)
        assert status == 422 and count_submissions() == optional_key_before + 1, error
    print(
        "PASS body/header key alternatives, cross-source replay and conflicting/missing key rejection",
        flush=True,
    )

    output_job = next(
        job for job in jobs["items"] if job["job_name"] == "acceptance_outputs_job"
    )
    status, output_run = api(
        "/api/v1/runs/submit", "POST", {"job_id": output_job["job_id"]}
    )
    assert status == 201, (status, output_run)
    wait_until(
        lambda: run_status(output_run["run_id"]) == "SUCCESS",
        "output fixture completion",
    )
    outputs_url = "/api/v1/runs/" + output_run["run_id"] + "/outputs"
    status, outputs = api(outputs_url)
    assert status == 200 and outputs["run_status"] == "SUCCESS", (status, outputs)
    assert outputs["complete"] is True and outputs["has_more"] is False, outputs
    by_step = {step["step_key"]: step for step in outputs["items"]}
    assert set(by_step) == {"exported_outputs", "unexported_output"}, by_step
    assert all(step["status"] == "SUCCESS" for step in by_step.values()), by_step
    exported = by_step["exported_outputs"]
    values = {output["output_name"]: output for output in exported["outputs"]}
    assert len(exported["outputs"]) == 5, exported
    assert set(values) == {"payload", "null_value", "zero", "false_value", "text"}, (
        values
    )
    assert all(
        set(output) == {"output_name", "output_value"} for output in values.values()
    ), values
    assert values["payload"]["output_value"] == {"numbers": [1, 2], "ok": True}, values
    assert (
        values["null_value"]["output_value"] is None
        and values["zero"]["output_value"] == 0
        and values["false_value"]["output_value"] is False
        and values["text"]["output_value"] == "mounted output"
    ), values
    unexported = by_step["unexported_output"]["outputs"]
    assert unexported == [{"output_name": "result", "output_value": 2}], unexported
    assert set(outputs) == {
        "run_id",
        "run_status",
        "items",
        "next_cursor",
        "has_more",
        "complete",
    }, outputs
    assert all(
        set(step) == {"step_key", "status", "outputs"} for step in outputs["items"]
    ), outputs

    def output_signatures(body):
        return [
            (
                step["step_key"],
                output["output_name"],
                json.dumps(output["output_value"], ensure_ascii=False, sort_keys=True),
            )
            for step in body["items"]
            for output in step["outputs"]
        ]

    expected_outputs = output_signatures(outputs)
    paged_outputs, cursor, page_count = [], None, 0
    for _ in range(100):
        query = {"max_events": 5, **({"cursor": cursor} if cursor else {})}
        status, page = api(outputs_url + "?" + urlencode(query))
        assert status == 200, (status, page)
        assert all(step["status"] == "SUCCESS" for step in page["items"]), page
        paged_outputs.extend(output_signatures(page))
        page_count += 1
        if cursor is not None:
            assert page["complete"] is False, page
        if not page["has_more"]:
            break
        assert page["complete"] is False and page["next_cursor"] != cursor, page
        cursor = page["next_cursor"]
    else:
        raise AssertionError("Output event pagination did not terminate")
    assert page_count > 1, page_count
    # Planned outputs may appear as null before their HANDLED_OUTPUT is in a
    # page. Every readable full-snapshot value must still be observed.
    assert set(expected_outputs) <= set(paged_outputs), (
        paged_outputs,
        expected_outputs,
    )
    environment.remove_output_file(output_run["run_id"], "exported_outputs", "text")
    status, missing_file = api(outputs_url)
    assert status == 200, missing_file
    missing_text = next(
        output
        for step in missing_file["items"]
        if step["step_key"] == "exported_outputs"
        for output in step["outputs"]
        if output["output_name"] == "text"
    )
    assert missing_text["output_value"] is None, missing_text
    assert api("/api/v1/runs/missing-output-acceptance-run/outputs")[0] == 404
    print(
        "PASS per-op status, mounted IO output values, missing-file null and complete event pagination",
        flush=True,
    )

    before = count_submissions()
    for reserved in (".dagster/repository", "dagster-webhook/submission_id"):
        status, error = api(
            "/api/v1/runs",
            "POST",
            {**payload, "tags": {reserved: "spoofed"}},
            "reserved",
        )
        assert status == 422 and count_submissions() == before, error
    print(
        "PASS health, ReDoc, discovery, real config error paths and reserved tags",
        flush=True,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = list(
            executor.map(
                lambda _: api("/api/v1/runs", "POST", payload, "concurrent"), range(4)
            )
        )
    assert all(status in {200, 201, 409} for status, _ in results), results
    assert sum(status == 201 for status, _ in results) == 1, results
    first = wait_until(lambda: replay(payload, "concurrent"), "concurrent replay")
    assert count_submissions() == before + 1
    wait_until(lambda: run_status(first["run_id"]) == "SUCCESS", "fixture completion")
    status, runs = api("/api/v1/runs?job_id=" + job["job_id"])
    assert status == 200 and first["run_id"] in {
        run["run_id"] for run in runs["items"]
    }, runs
    status, events = api("/api/v1/runs/" + first["run_id"] + "/events?limit=2")
    assert status == 200 and events["items"] and events["next_cursor"], events
    environment.restart_gateway()
    after_restart = wait_until(
        lambda: replay(payload, "concurrent"), "persisted replay"
    )
    assert (
        after_restart["run_id"] == first["run_id"] and count_submissions() == before + 1
    )
    print(
        "PASS concurrent submit, run/events queries and restart persistence", flush=True
    )

    environment.request(
        environment.proxy_url + "/__acceptance/drop-next-submit", "POST", {}
    )
    status, uncertain = api("/api/v1/runs", "POST", payload, "lost-response")
    assert status == 409 and uncertain["code"] == "SUBMISSION_OUTCOME_UNKNOWN", (
        uncertain
    )
    identity = uncertain["details"][0]
    submission_url = "/api/v1/submissions/" + identity["submission_id"]
    status, record = api(submission_url)
    assert status == 200 and record["run_id"] is None, record
    environment.restart_gateway()

    def reconcile():
        status, record = api(submission_url + "/reconcile", "POST", {})
        if status == 409:
            return None
        assert status == 200, (status, record)
        return record if record["state"] == "ACCEPTED" else None

    reconciled = wait_until(reconcile, "explicit reconciliation after a lost response")
    recovered = wait_until(lambda: replay(payload, "lost-response"), "recovered replay")
    assert (
        recovered["run_id"] == reconciled["run_id"]
        and count_submissions() == before + 2
    )
    status, matching = api(
        "/api/v1/runs?tag=dagster-webhook%2Fsubmission_id%3D"
        + identity["submission_id"]
    )
    assert status == 200 and len(matching["items"]) == 1, matching
    assert matching["items"][0]["run_id"] == recovered["run_id"], matching
    proxy_state = environment.request(environment.proxy_url + "/__acceptance/state")[1]
    assert proxy_state["dropped"] == 1, proxy_state
    print(
        "PASS accepted-but-lost upstream response, reconciliation and no duplicate launch",
        flush=True,
    )

    long_config = {
        "ops": {"acceptance_step": {"config": {"count": 1, "wait_seconds": 90}}}
    }
    long_run = submit({**payload, "run_config": long_config}, "terminate")
    wait_until(lambda: run_status(long_run["run_id"]) == "STARTED", "long fixture run")
    status, batch = api(
        "/api/v1/runs/terminate-batch",
        "POST",
        {"run_ids": [long_run["run_id"], "missing-acceptance-run"]},
    )
    assert status == 200 and batch["succeeded"] == batch["failed"] == 1, batch
    assert [row["status"] for row in batch["results"]] == ["accepted", "failed"], batch
    wait_until(
        lambda: run_status(long_run["run_id"]) == "CANCELED", "fixture cancellation"
    )
    print("PASS real mixed-result batch termination", flush=True)
    if environment.mode == "docker":
        environment.compose("stop", "dagster")
    else:
        environment.stop_process("dagster")
    wait_until(lambda: api("/ready")[0] == 503, "unavailable fixture readiness")
    assert api("/health")[0] == 200
    status, available_record = api(submission_url)
    assert status == 200 and available_record["state"] == "ACCEPTED", available_record
    print("PASS health stays live while upstream readiness fails", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("native", "docker"), required=True)
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Native fixture Python with dagster-webserver installed",
    )
    parser.add_argument(
        "--image", help="Already-built local gateway image for Docker mode"
    )
    args = parser.parse_args()
    if args.mode == "docker" and not args.image:
        parser.error(
            "Docker mode requires --image; this harness never builds the gateway"
        )
    with tempfile.TemporaryDirectory(prefix="dagster-gateway-acceptance-") as temporary:
        environment = Environment(args.mode, args.python, args.image, temporary)
        try:
            environment.start()
            check(environment)
            print(f"PASS all isolated acceptance checks ({args.mode})", flush=True)
        except BaseException:
            environment.dump_logs()
            raise
        finally:
            environment.close()


if __name__ == "__main__":
    main()
