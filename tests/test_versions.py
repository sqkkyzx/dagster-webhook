import json

import pytest

from project_metadata import read_versions


def project_file(tmp_path, gateway_version, dependencies):
    path = tmp_path / "pyproject.toml"
    path.write_text(
        "[project]\n"
        f"version = {json.dumps(gateway_version)}\n"
        f"dependencies = {json.dumps(dependencies)}\n"
    )
    return path


@pytest.mark.parametrize(
    "gateway_version,dagster_version",
    [("1.13.4.0", "1.13.4"), ("1.13.4.1", "1.13.4"), ("1.13.26.12", "1.13.26")],
)
def test_accepts_private_revisions_for_the_pinned_dagster_release(
    tmp_path, gateway_version, dagster_version
):
    path = project_file(
        tmp_path, gateway_version, ["fastapi", f"dagster=={dagster_version}"]
    )
    assert read_versions(path) == (gateway_version, dagster_version)


@pytest.mark.parametrize(
    "gateway_version,dependencies",
    [
        ("1.13.26.1", ["dagster==1.13.4"]),
        ("1.13.4", ["dagster==1.13.4"]),
        ("1.13.4.01", ["dagster==1.13.4"]),
        ("1.13.4.x", ["dagster==1.13.4"]),
        ("1.13.4.1", ["dagster>=1.13.4"]),
        ("1.13.4.1", ["dagster-graphql", "fastapi"]),
        ("1.13.4.1", ["dagster==1.13.4", "dagster==1.13.26"]),
    ],
)
def test_rejects_ambiguous_or_incompatible_release_versions(
    tmp_path, gateway_version, dependencies
):
    path = project_file(tmp_path, gateway_version, dependencies)
    with pytest.raises(ValueError):
        read_versions(path)
