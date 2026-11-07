"""Optional per-caller API keys; tokens are never used as caller identifiers."""

import json
import re
import secrets
from dataclasses import dataclass, field

SCOPES = frozenset({"read", "submit", "cancel", "admin"})


@dataclass(frozen=True)
class Principal:
    name: str
    scopes: frozenset[str]


@dataclass(frozen=True)
class APIKey:
    name: str
    token: str = field(repr=False)
    scopes: frozenset[str] = field(default_factory=frozenset)


def parse_api_keys(raw: str | None) -> tuple[APIKey, ...]:
    if raw is None:
        return ()
    try:
        values = json.loads(raw)
        if not isinstance(values, list) or not values:
            raise ValueError
        keys = []
        names, tokens = set(), set()
        for item in values:
            if not isinstance(item, dict) or set(item) != {"name", "token", "scopes"}:
                raise ValueError
            name, token, scopes = item["name"], item["token"], item["scopes"]
            if (
                not isinstance(name, str)
                or not re.fullmatch(r"[A-Za-z0-9._-]{1,100}", name)
                or name == "legacy"
                or not isinstance(token, str)
                or not re.fullmatch(r"[!-~]+", token)
                or not isinstance(scopes, list)
                or not all(isinstance(s, str) for s in scopes)
                or not scopes
                or not set(scopes) <= SCOPES
                or name in names
                or token in tokens
            ):
                raise ValueError
            names.add(name)
            tokens.add(token)
            keys.append(APIKey(name, token, frozenset(scopes)))
        return tuple(keys)
    except (ValueError, TypeError, KeyError):
        raise ValueError(
            "APP_API_KEYS_JSON must contain unique name/token entries with read, submit, cancel or admin scopes"
        ) from None


def authenticate(
    token: str, keys: tuple[APIKey, ...], legacy_token: str | None
) -> Principal | None:
    encoded = token.encode("utf-8")
    if legacy_token and secrets.compare_digest(encoded, legacy_token.encode("utf-8")):
        return Principal("legacy", SCOPES)
    for key in keys:
        if secrets.compare_digest(encoded, key.token.encode("utf-8")):
            return Principal(key.name, key.scopes)
    return None
