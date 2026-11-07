"""Read mounted Dagster output files as bounded JSON-compatible values."""

import io
import json
import math
import os
import pickle
import pickletools
import stat
from pathlib import Path

DEFAULT_STORAGE_ROOT = "/opt/dagster/dagster_home/storage"
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_VALUE_BYTES = 16 * 1024 * 1024
MAX_JSON_DEPTH = 64
MAX_PICKLE_MEMO_ENTRIES = 1_000_000

# Dagster's filesystem IO manager pickles objects. Basic containers and scalar
# values need no constructors, imported classes, persistent IDs or buffers.
_ALLOWED_PICKLE_OPCODES = frozenset(
    [
        "MARK",
        "STOP",
        "POP",
        "POP_MARK",
        "DUP",
        "FLOAT",
        "INT",
        "BININT",
        "BININT1",
        "LONG",
        "BININT2",
        "NONE",
        "STRING",
        "BINSTRING",
        "SHORT_BINSTRING",
        "UNICODE",
        "BINUNICODE",
        "APPEND",
        "DICT",
        "EMPTY_DICT",
        "APPENDS",
        "GET",
        "BINGET",
        "LONG_BINGET",
        "LIST",
        "EMPTY_LIST",
        "PUT",
        "BINPUT",
        "LONG_BINPUT",
        "SETITEM",
        "TUPLE",
        "EMPTY_TUPLE",
        "SETITEMS",
        "BINFLOAT",
        "PROTO",
        "TUPLE1",
        "TUPLE2",
        "TUPLE3",
        "NEWTRUE",
        "NEWFALSE",
        "LONG1",
        "LONG4",
        "SHORT_BINUNICODE",
        "BINUNICODE8",
        "BINBYTES8",
        "EMPTY_SET",
        "ADDITEMS",
        "FROZENSET",
        "MEMOIZE",
        "FRAME",
        "BYTEARRAY8",
        "BINBYTES",
        "SHORT_BINBYTES",
    ]
)


class _RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        raise pickle.UnpicklingError("Output requires an unsupported Python class")

    def persistent_load(self, pid):
        raise pickle.UnpicklingError("Persistent output references are unsupported")


def _restricted_pickle(data):
    next_memo_index = 0
    for opcode, argument, _position in pickletools.genops(data):
        if opcode.name not in _ALLOWED_PICKLE_OPCODES:
            raise pickle.UnpicklingError(
                "Output requires an unsupported pickle operation"
            )
        if opcode.name in {"PUT", "BINPUT", "LONG_BINPUT"}:
            if not 0 <= argument < MAX_PICKLE_MEMO_ENTRIES:
                raise pickle.UnpicklingError("Output pickle memo is too large")
            next_memo_index = max(next_memo_index, argument + 1)
        elif opcode.name == "MEMOIZE":
            if next_memo_index >= MAX_PICKLE_MEMO_ENTRIES:
                raise pickle.UnpicklingError("Output pickle memo is too large")
            next_memo_index += 1
    stream = io.BytesIO(data)
    value = _RestrictedUnpickler(stream).load()
    if stream.read(1):
        raise pickle.UnpicklingError("Unexpected trailing output data")
    return value


def _finite_constant(value):
    raise ValueError("Non-finite output values are unsupported")


def _decode(data):
    try:
        return json.loads(data.decode("utf-8"), parse_constant=_finite_constant)
    except (UnicodeError, ValueError, RecursionError):
        return _restricted_pickle(data)


def _json_compatible(value, maximum):
    """Validate built-ins and charge encoded bytes before expanding aliases."""
    remaining = maximum
    active = set()

    def charge(size):
        nonlocal remaining
        remaining -= size
        if remaining < 0:
            raise ValueError("Output exceeds the JSON value limit")

    def encoded_size(item):
        return len(
            json.dumps(
                item, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode("utf-8")
        )

    def convert(item, depth):
        kind = type(item)
        if item is None or kind in {bool, int, float, str}:
            if kind is float and not math.isfinite(item):
                raise ValueError("Non-finite output value")
            charge(encoded_size(item))
            return item
        if kind not in {list, tuple, dict}:
            raise ValueError("Output is not JSON-compatible")
        if depth >= MAX_JSON_DEPTH or id(item) in active:
            raise ValueError("Output is cyclic or too deeply nested")
        charge(2 + max(0, len(item) - 1))
        active.add(id(item))
        try:
            if kind is dict:
                result = {}
                for key, child in item.items():
                    if type(key) is not str:
                        raise ValueError("Output object keys must be strings")
                    charge(encoded_size(key) + 1)
                    result[key] = convert(child, depth + 1)
                return result
            return [convert(child, depth + 1) for child in item]
        finally:
            active.remove(id(item))

    return convert(value, 0)


def _read_beneath_root(path, root, maximum):
    candidate = Path(path)
    if not candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("Output path must remain beneath the storage root")
    configured_root = Path(root).expanduser().absolute()
    canonical_root = configured_root.resolve(strict=True)
    try:
        relative = candidate.relative_to(configured_root)
    except ValueError:
        relative = candidate.relative_to(canonical_root)
    if not relative.parts:
        raise ValueError("Output path is not a file")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    directory_fd = os.open(canonical_root, directory_flags)
    file_fd = None
    try:
        for part in relative.parts[:-1]:
            next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        file_fd = os.open(relative.parts[-1], file_flags, dir_fd=directory_fd)
        info = os.fstat(file_fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise ValueError("Output is not a bounded regular file")
        with os.fdopen(file_fd, "rb") as stream:
            file_fd = None
            data = stream.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError("Output grew beyond the file size limit")
        return data
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(directory_fd)


def read_output_file(
    path,
    *,
    root=None,
    max_bytes=MAX_FILE_BYTES,
    max_value_bytes=MAX_VALUE_BYTES,
):
    """Return a mounted JSON/pickle value, or ``None`` when it cannot be read.

    ``root`` defaults to RUN_OUTPUT_STORAGE_ROOT, read on each call. Only basic
    built-in JSON-compatible pickle values are supported; imported classes and
    executable pickle operations are rejected. Tuples become JSON arrays.
    """
    try:
        if (
            type(max_bytes) is not int
            or type(max_value_bytes) is not int
            or max_bytes <= 0
            or max_value_bytes <= 0
        ):
            return None
        configured = (
            os.getenv("RUN_OUTPUT_STORAGE_ROOT", DEFAULT_STORAGE_ROOT)
            if root is None
            else root
        )
        if not configured:
            return None
        data = _read_beneath_root(path, configured, max_bytes)
        return _json_compatible(_decode(data), max_value_bytes)
    except (
        OSError,
        ValueError,
        TypeError,
        EOFError,
        pickle.UnpicklingError,
        OverflowError,
        RecursionError,
        AttributeError,
        NotImplementedError,
    ):
        return None
