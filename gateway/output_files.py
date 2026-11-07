"""Read mounted Dagster output files as bounded JSON-compatible values."""

import datetime as dt
import io
import json
import math
import os
import pickle
import pickletools
import stat
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_STORAGE_ROOT = "/opt/dagster/dagster_home/storage"
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_VALUE_BYTES = 16 * 1024 * 1024
MAX_JSON_DEPTH = 64
MAX_PICKLE_MEMO_ENTRIES = 1_000_000

# Dagster's filesystem IO manager uses pickle protocol 4. Only the constructors
# below may be resolved; extensions, arbitrary classes and object state stay blocked.
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
        "GLOBAL",
        "STACK_GLOBAL",
        "REDUCE",
    ]
)


def _packed_state(data, size):
    if type(data) is not bytes or len(data) != size:
        raise pickle.UnpicklingError("Invalid date/time state")
    return data


def _timezone_value(value):
    if value is not None and type(value) not in {dt.timezone, ZoneInfo}:
        raise pickle.UnpicklingError("Unsupported date/time zone")
    return value


def _restore_datetime(data, timezone=None):
    data = _packed_state(data, 10)
    return dt.datetime(
        int.from_bytes(data[:2]),
        data[2] & 0x7F,
        data[3],
        data[4],
        data[5],
        data[6],
        int.from_bytes(data[7:]),
        tzinfo=_timezone_value(timezone),
        fold=data[2] >> 7,
    )


def _restore_date(data):
    data = _packed_state(data, 4)
    return dt.date(int.from_bytes(data[:2]), data[2], data[3])


def _restore_time(data, timezone=None):
    data = _packed_state(data, 6)
    return dt.time(
        data[0] & 0x7F,
        data[1],
        data[2],
        int.from_bytes(data[3:]),
        tzinfo=_timezone_value(timezone),
        fold=data[0] >> 7,
    )


def _restore_timedelta(days, seconds, microseconds):
    # Only canonical pickle state is needed to restore fixed-offset time zones.
    if (
        any(type(value) is not int for value in (days, seconds, microseconds))
        or not -999999999 <= days <= 999999999
        or not 0 <= seconds < 86400
        or not 0 <= microseconds < 1000000
    ):
        raise pickle.UnpicklingError("Invalid time zone offset")
    return dt.timedelta(days=days, seconds=seconds, microseconds=microseconds)


def _restore_timezone(offset, name=None):
    if type(offset) is not dt.timedelta or (name is not None and type(name) is not str):
        raise pickle.UnpicklingError("Invalid fixed time zone")
    return dt.timezone(offset) if name is None else dt.timezone(offset, name)


def _restore_zoneinfo(key):
    if (
        type(key) is not str
        or len(key) > 255
        or any(part in {"", ".", ".."} for part in key.split("/"))
        or "\\" in key
        or "\0" in key
    ):
        raise pickle.UnpicklingError("Invalid IANA time zone key")
    try:
        return ZoneInfo(key)
    except ZoneInfoNotFoundError:
        raise pickle.UnpicklingError("Unknown IANA time zone") from None


def _restore_zoneinfo_pickle(key, from_cache):
    if type(from_cache) is not int or from_cache not in {0, 1}:
        raise pickle.UnpicklingError("Invalid IANA time zone pickle state")
    # Cache identity has no effect on the JSON value or the offset at this instant.
    return _restore_zoneinfo(key)


def _zoneinfo_getattr(target, name):
    # ZoneInfo pickles encode getattr(ZoneInfo, "_unpickle"). Never expose the
    # builtin getattr or any class attributes to a file-controlled callable chain.
    if target is not _restore_zoneinfo or type(name) is not str or name != "_unpickle":
        raise pickle.UnpicklingError("Unsupported pickle attribute lookup")
    return _restore_zoneinfo_pickle


_PICKLE_CONSTRUCTORS = {
    ("datetime", "datetime"): _restore_datetime,
    ("datetime", "date"): _restore_date,
    ("datetime", "time"): _restore_time,
    ("datetime", "timedelta"): _restore_timedelta,
    ("datetime", "timezone"): _restore_timezone,
    ("zoneinfo", "ZoneInfo"): _restore_zoneinfo,
    ("builtins", "getattr"): _zoneinfo_getattr,
}


class _RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        constructor = _PICKLE_CONSTRUCTORS.get((module, name))
        if constructor is None:
            raise pickle.UnpicklingError("Output requires an unsupported Python class")
        return constructor

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
    """Convert known date/time types and bound JSON before expanding aliases."""
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
        if kind in {dt.datetime, dt.date, dt.time}:
            item = item.isoformat()
            kind = str
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
    JSON-compatible pickle values and standard date/time values are supported.
    Date/time values become ISO strings; tuples become arrays. Only narrowly
    validated date/time constructors are allowed, never arbitrary Python classes.
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
