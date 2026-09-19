"""Namespace manifest describing the stores a data namespace holds."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass
class StoreRecord:
    """One materialized store and the request that produced it."""

    fingerprint: str
    requests: dict[str, object]
    coverage: dict[str, object]
    provenance: dict[str, str]
    built_at: str
    last_used_at: str

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-compatible representation."""
        return {
            "fingerprint": self.fingerprint,
            "requests": self.requests,
            "coverage": self.coverage,
            "provenance": self.provenance,
            "built_at": self.built_at,
            "last_used_at": self.last_used_at,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> StoreRecord:
        """Build a record from a persisted payload."""
        return cls(
            fingerprint=str(payload["fingerprint"]),
            requests=dict(payload.get("requests", {})),  # type: ignore[arg-type]
            coverage=dict(payload.get("coverage", {})),  # type: ignore[arg-type]
            provenance={
                str(key): str(value)
                for key, value in dict(payload.get("provenance", {})).items()  # type: ignore[arg-type]
            },
            built_at=str(payload.get("built_at", "")),
            last_used_at=str(payload.get("last_used_at", "")),
        )


@dataclass
class ExperimentManifest:
    """The source of truth for what a namespace currently holds."""

    schema: int
    source: str
    experiment: str
    current: str | None = None
    stores: dict[str, StoreRecord] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-compatible representation."""
        return {
            "schema": self.schema,
            "source": self.source,
            "experiment": self.experiment,
            "current": self.current,
            "stores": {
                key: record.to_payload() for key, record in sorted(self.stores.items())
            },
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ExperimentManifest:
        """Build a manifest from a persisted payload."""
        raw_stores = payload.get("stores", {})
        stores = {
            str(key): StoreRecord.from_payload(value)  # type: ignore[arg-type]
            for key, value in dict(raw_stores).items()  # type: ignore[arg-type]
        }
        current = payload.get("current")
        return cls(
            schema=int(payload["schema"]),  # type: ignore[arg-type]
            source=str(payload["source"]),
            experiment=str(payload["experiment"]),
            current=str(current) if current is not None else None,
            stores=stores,
        )
