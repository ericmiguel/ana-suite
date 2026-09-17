"""Typed events emitted by the ANA download and store pipelines."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, kw_only=True)
class RequestPlanned:
    """One named request was selected for collection."""

    name: str
    stations: int


@dataclass(frozen=True, kw_only=True)
class StationResolved:
    """One station finished its current incremental collection step."""

    request: str
    station: str
    path: Path | None


@dataclass(frozen=True, kw_only=True)
class StorePlanned:
    """A Parquet store write was planned."""

    items: int


@dataclass(frozen=True, kw_only=True)
class ItemWritten:
    """One store item was written."""

    description: str


type PipelineEvent = RequestPlanned | StationResolved | StorePlanned | ItemWritten
type PipelineListener = Callable[[PipelineEvent], None]
