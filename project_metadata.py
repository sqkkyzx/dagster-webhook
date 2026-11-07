"""Shared application and release versions, read without importing the gateway."""

import re
import tomllib
from pathlib import Path

PROJECT_FILE = Path(__file__).with_name("pyproject.toml")
_NUMBER = r"(?:0|[1-9][0-9]*)"
_DAGSTER_PIN = re.compile(
    rf"dagster\s*==\s*({_NUMBER}\.{_NUMBER}\.{_NUMBER})", re.IGNORECASE
)


def read_versions(project_file: Path = PROJECT_FILE) -> tuple[str, str]:
    """Return (gateway version, Dagster version), rejecting incompatible releases.

    The first three version components follow the exact Dagster dependency pin;
    the fourth is this gateway's private revision for that Dagster release.
    """
    with project_file.open("rb") as stream:
        project = tomllib.load(stream)["project"]

    pins = [
        match.group(1)
        for dependency in project["dependencies"]
        if (match := _DAGSTER_PIN.fullmatch(dependency.strip()))
    ]
    if len(pins) != 1:
        raise ValueError("Exactly one dagster==X.Y.Z dependency is required")

    dagster_version = pins[0]
    version = project["version"]
    if not re.fullmatch(rf"{re.escape(dagster_version)}\.{_NUMBER}", version):
        raise ValueError(
            f"Project version must be {dagster_version}.N, where N is a non-negative "
            "private revision without leading zeroes"
        )
    return version, dagster_version


VERSION, DAGSTER_VERSION = read_versions()
