"""Mounted output loading never imports or executes persisted Python objects."""

import json
import os
import pickle
import struct
from pathlib import Path

import pytest

from gateway import output_files
from gateway.output_files import read_output_file


def dump(root, value, *, protocol=pickle.HIGHEST_PROTOCOL, name="result"):
    path = root / "run" / "op" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pickle.dumps(value, protocol=protocol))
    return path


@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
@pytest.mark.parametrize(
    "value", [{"items": [0, False, None, "中文"]}, "# Markdown", 0, False, None]
)
def test_dagster_pickle_scalars_and_containers(tmp_path, protocol, value):
    path = dump(tmp_path, value, protocol=protocol)
    assert read_output_file(str(path), root=tmp_path) == value


def test_json_files_and_tuple_arrays(tmp_path):
    path = tmp_path / "result.json"
    path.write_text(json.dumps({"rows": 2, "ok": False}))
    assert read_output_file(path, root=tmp_path) == {"rows": 2, "ok": False}
    assert read_output_file(dump(tmp_path, (1, "a")), root=tmp_path) == [1, "a"]


def test_env_root_is_read_each_time(tmp_path, monkeypatch):
    first, second = tmp_path / "first", tmp_path / "second"
    path = dump(first, "first")
    monkeypatch.setenv("RUN_OUTPUT_STORAGE_ROOT", str(first))
    assert read_output_file(path) == "first"
    monkeypatch.setenv("RUN_OUTPUT_STORAGE_ROOT", str(second))
    second.mkdir()
    assert read_output_file(path) is None
    assert read_output_file(dump(second, "second")) == "second"
    monkeypatch.setenv("RUN_OUTPUT_STORAGE_ROOT", "")
    assert read_output_file(path) is None


def test_no_mount_missing_directory_and_outside_paths_return_null(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = dump(tmp_path / "outside", "unrelated")
    assert read_output_file(outside, root=root) is None
    assert read_output_file(root / "missing", root=root) is None
    assert read_output_file("relative/result", root=root) is None
    assert read_output_file(root, root=root) is None
    assert read_output_file(outside, root=tmp_path / "missing-root") is None
    assert (
        read_output_file(root / ".." / "outside" / "run" / "op" / "result", root=root)
        is None
    )


def test_file_and_directory_symlinks_are_not_followed(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = dump(tmp_path / "outside", "unrelated")
    linked_file = root / "linked-file"
    linked_file.symlink_to(outside)
    assert read_output_file(linked_file, root=root) is None
    linked_directory = root / "linked-directory"
    linked_directory.symlink_to(outside.parent, target_is_directory=True)
    assert read_output_file(linked_directory / outside.name, root=root) is None
    inside = dump(root, "inside")
    (root / "inside-link").symlink_to(inside)
    assert read_output_file(root / "inside-link", root=root) is None


def test_storage_root_itself_may_be_configured_through_a_symlink(tmp_path):
    real = tmp_path / "real"
    path = dump(real, "value")
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    assert read_output_file(alias / path.relative_to(real), root=alias) == "value"


def test_permissions_failure_is_returned_as_null(tmp_path, monkeypatch):
    path = dump(tmp_path, "value")
    original_open = output_files.os.open

    def denied(filename, flags, *args, **kwargs):
        if filename == path.name:
            raise PermissionError("fixture denies output")
        return original_open(filename, flags, *args, **kwargs)

    monkeypatch.setattr(output_files.os, "open", denied)
    assert read_output_file(path, root=tmp_path) is None


def test_nonregular_files_do_not_block(tmp_path):
    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)
    assert read_output_file(fifo, root=tmp_path) is None


@pytest.mark.parametrize(
    "payload", [b"", b"not a pickle", b"{broken json", b"\x80\x05broken", b"1 2"]
)
def test_malformed_data_returns_null(tmp_path, payload):
    path = tmp_path / "bad"
    path.write_bytes(payload)
    assert read_output_file(path, root=tmp_path) is None


def test_trailing_pickle_data_is_not_accepted(tmp_path):
    path = dump(tmp_path, "first")
    path.write_bytes(path.read_bytes() + pickle.dumps("second"))
    assert read_output_file(path, root=tmp_path) is None


class UnsupportedOutput:
    pass


def test_custom_python_classes_and_non_json_types_are_null(tmp_path):
    for index, value in enumerate(
        (UnsupportedOutput(), b"bytes", {1, 2}, {1: "integer-key"})
    ):
        assert (
            read_output_file(dump(tmp_path, value, name=str(index)), root=tmp_path)
            is None
        )


def _write_marker(path):
    Path(path).write_text("must never execute")
    return {"unexpected": True}


class MaliciousOutput:
    def __init__(self, marker):
        self.marker = marker

    def __reduce__(self):
        return _write_marker, (str(self.marker),)


def test_executable_pickle_is_never_executed(tmp_path):
    marker = tmp_path / "executed"
    path = dump(tmp_path, MaliciousOutput(marker))
    assert read_output_file(path, root=tmp_path) is None
    assert not marker.exists()


def test_extreme_pickle_memo_index_is_rejected_before_allocation(tmp_path, monkeypatch):
    path = tmp_path / "memo-bomb"
    path.write_bytes(b"\x80\x04Nr" + struct.pack("<I", 2**32 - 1) + b".")

    def unexpected_unpickler(*args, **kwargs):
        raise AssertionError("Large memo must be rejected during opcode inspection")

    monkeypatch.setattr(output_files, "_RestrictedUnpickler", unexpected_unpickler)
    assert read_output_file(path, root=tmp_path) is None


def test_platform_without_secure_file_open_support_returns_null(tmp_path, monkeypatch):
    path = dump(tmp_path, "value")

    def unsupported(*args, **kwargs):
        raise NotImplementedError("Directory file descriptors unavailable")

    monkeypatch.setattr(output_files, "_read_beneath_root", unsupported)
    assert read_output_file(path, root=tmp_path) is None


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), {"nested": float("-inf")}]
)
def test_nonfinite_pickle_values_return_null(tmp_path, value):
    assert read_output_file(dump(tmp_path, value), root=tmp_path) is None


def test_nonfinite_json_returns_null(tmp_path):
    path = tmp_path / "result"
    path.write_text('{"value":NaN}')
    assert read_output_file(path, root=tmp_path) is None


def test_file_and_json_size_limits_are_independent(tmp_path):
    path = dump(tmp_path, "x" * 100)
    assert read_output_file(path, root=tmp_path, max_bytes=8) is None
    assert read_output_file(path, root=tmp_path, max_value_bytes=101) is None
    assert read_output_file(path, root=tmp_path, max_value_bytes=102) == "x" * 100
    path = dump(tmp_path, "中" * 10)
    assert read_output_file(path, root=tmp_path, max_value_bytes=32) == "中" * 10
    assert read_output_file(path, root=tmp_path, max_value_bytes=31) is None


def test_deep_cyclic_and_expanding_alias_values_are_bounded(tmp_path):
    cyclic = []
    cyclic.append(cyclic)
    assert read_output_file(dump(tmp_path, cyclic), root=tmp_path) is None
    deep = 0
    for _ in range(65):
        deep = [deep]
    assert read_output_file(dump(tmp_path, deep), root=tmp_path) is None
    expanding = [0]
    for _ in range(40):
        expanding = [expanding, expanding]
    assert (
        read_output_file(dump(tmp_path, expanding), root=tmp_path, max_value_bytes=1024)
        is None
    )
