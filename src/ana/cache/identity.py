"""Canonical request normalization and cache fingerprints.

The request fingerprint is the identity of the *data* a request needs. It
deliberately excludes the experiment name: the name is a namespace, and two
requests with identical fields must share fragments and stores.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from dataclasses import is_dataclass
from datetime import date
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Mapping


CACHE_SCHEMA_VERSION = 2
CACHE_SOURCE = "ana"


def request_fingerprint(requests: Mapping[str, object]) -> str:
    """Return the stable fingerprint of a set of named typed requests."""
    identity = {
        "schema": CACHE_SCHEMA_VERSION,
        "source": CACHE_SOURCE,
        "requests": {
            request_name: {
                "type": type(request).__qualname__,
                "fields": normalize_dataclass(request),
            }
            for request_name, request in sorted(requests.items())
        },
    }
    return _digest(identity)


def experiment_cache_key(name: str, requests: Mapping[str, object]) -> str:
    """Return the legacy name-and-request key.

    Kept for callers written before namespaces; new code uses
    :func:`request_fingerprint`.
    """
    identity = {
        "schema": 1,
        "source": CACHE_SOURCE,
        "name": name,
        "requests": {
            request_name: {
                "type": type(request).__qualname__,
                "fields": normalize_dataclass(request),
            }
            for request_name, request in sorted(requests.items())
        },
    }
    return _digest(identity)


def normalize_dataclass(value: object) -> dict[str, object]:
    """Convert a request dataclass into JSON-compatible identity data."""
    if not is_dataclass(value):
        raise TypeError("Cache identity requires a dataclass request.")
    return {
        field.name: normalize_value(getattr(value, field.name))
        for field in fields(value)
    }


def normalize_value(value: object) -> object:
    """Normalize nested request values for canonical JSON serialization."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "digest"):
        return {"type": type(value).__qualname__, "sha256": value.digest}
    if is_dataclass(value):
        return normalize_dataclass(value)
    if isinstance(value, dict):
        return {
            str(key): normalize_value(item)
            for key, item in sorted(value.items(), key=str)
        }
    if isinstance(value, (tuple, list)):
        return [normalize_value(item) for item in value]
    return value


def _digest(identity: object) -> str:
    encoded = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
