"""Shared fake ANA provider for offline tests."""

from datetime import datetime

import polars as pl

from ana import FetchResult
from ana import Station


class FakeProvider:
    """Small deterministic provider that records every requested interval."""

    def __init__(self, stations: list[Station]) -> None:
        self.stations = stations
        self.calls: list[tuple[str, str, str, str]] = []

    def fetch_inventory(self) -> list[Station]:
        return list(self.stations)

    def fetch_series(
        self, station: Station, start: str, end: str, variable: str
    ) -> tuple[FetchResult, pl.DataFrame | None]:
        self.calls.append((station.code, start, end, variable))
        return FetchResult.HAS_DATA, pl.DataFrame(
            {
                "datetime": [datetime.fromisoformat(f"{start}T00:00:00")],
                "value": [1.5],
            }
        )
