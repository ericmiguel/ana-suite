"""Atomic incremental station cache: inventory, metadata, and observations."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import threading
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from ana.models import Station


if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass
class StationMeta:
    """Mutable state used to schedule one station and variable."""

    code: str
    variable: str
    status: str = "unknown"
    known_data_start: str | None = None
    known_data_end: str | None = None
    checked_ranges: list[tuple[str, str, str]] = field(default_factory=list)
    last_checked: str | None = None
    consecutive_errors: int = 0

    @classmethod
    def create(cls, code: str, variable: str) -> StationMeta:
        """Create state for a station that has not been queried."""
        return cls(code=code, variable=variable)

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-compatible representation."""
        return {
            "code": self.code,
            "variable": self.variable,
            "status": self.status,
            "known_data_start": self.known_data_start,
            "known_data_end": self.known_data_end,
            "checked_ranges": [list(item) for item in self.checked_ranges],
            "last_checked": self.last_checked,
            "consecutive_errors": self.consecutive_errors,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> StationMeta:
        """Build state from a persisted payload."""
        raw_ranges = payload.get("checked_ranges", [])
        ranges: list[tuple[str, str, str]] = []
        if isinstance(raw_ranges, list):
            ranges = [
                (item[0], item[1], item[2])
                for item in raw_ranges
                if isinstance(item, list)
                and len(item) == 3
                and all(isinstance(value, str) for value in item)
            ]
        raw_errors = payload.get("consecutive_errors", 0)
        consecutive_errors = raw_errors if isinstance(raw_errors, int) else 0
        return cls(
            code=str(payload["code"]),
            variable=str(payload["variable"]),
            status=str(payload.get("status", "unknown")),
            known_data_start=_optional_string(payload.get("known_data_start")),
            known_data_end=_optional_string(payload.get("known_data_end")),
            checked_ranges=ranges,
            last_checked=_optional_string(payload.get("last_checked")),
            consecutive_errors=consecutive_errors,
        )


class StationCache:
    """Atomic inventory, metadata, and observation persistence."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    @property
    def inventory_file(self) -> Path:
        """Return the cached inventory path."""
        return self.path / "inventory.json"

    @property
    def active_inventory_file(self) -> Path:
        """Return the cached active telemetry inventory path."""
        return self.path / "active_inventory.json"

    def inventory_age_days(self, *, active: bool = False) -> float | None:
        """Return inventory age in days, or ``None`` when absent."""
        path = self.active_inventory_file if active else self.inventory_file
        if not path.is_file():
            return None
        modified = dt.datetime.fromtimestamp(path.stat().st_mtime)
        return (dt.datetime.now() - modified).total_seconds() / 86400

    def load_inventory(self, *, active: bool = False) -> list[Station] | None:
        """Load the cached station inventory."""
        path = self.active_inventory_file if active else self.inventory_file
        if not path.is_file():
            return None
        payload = json.loads(path.read_text())
        return [Station(**item) for item in payload]

    def save_inventory(self, stations: list[Station], *, active: bool = False) -> None:
        """Atomically save the station inventory."""
        self.path.mkdir(parents=True, exist_ok=True)
        _atomic_text(
            self.active_inventory_file if active else self.inventory_file,
            json.dumps([dataclasses.asdict(item) for item in stations], indent=2),
        )

    def _meta_file(self, variable: str, code: str) -> Path:
        return self.path / "meta" / variable / f"{code}.json"

    def load_meta(self, variable: str, code: str) -> StationMeta | None:
        """Load state for one station and variable."""
        path = self._meta_file(variable, code)
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text())
            return StationMeta.from_payload(payload)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    def save_meta(self, meta: StationMeta) -> None:
        """Atomically save station state."""
        path = self._meta_file(meta.variable, meta.code)
        _atomic_text(path, json.dumps(meta.to_payload(), indent=2))

    def _data_file(self, variable: str, code: str) -> Path:
        return self.path / "observations" / variable / f"{code}.parquet"

    def load_data(self, variable: str, code: str) -> pl.DataFrame | None:
        """Load one station's cached observations."""
        path = self._data_file(variable, code)
        return pl.read_parquet(path) if path.is_file() else None

    def merge_data(self, variable: str, code: str, frame: pl.DataFrame) -> None:
        """Merge observations, with the newest response winning per timestamp."""
        path = self._data_file(variable, code)
        existing = self.load_data(variable, code)
        combined = frame if existing is None else pl.concat([existing, frame])
        combined = combined.unique(subset=["datetime"], keep="last").sort("datetime")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.part")
        combined.write_parquet(temporary)
        temporary.replace(path)

    def data_paths(self, variable: str, codes: set[str]) -> list[Path]:
        """Return cached station files for a variable and code set."""
        return sorted(
            self._data_file(variable, code)
            for code in codes
            if self._data_file(variable, code).is_file()
        )


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.part")
    temporary.write_text(content)
    temporary.replace(path)


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None
